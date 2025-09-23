"""Processing of completed queries."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import UTC, datetime
from typing import assert_never

from faststream.kafka import KafkaBroker
from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ..config import config
from ..events import (
    Events,
    QservFailureEvent,
    QservProtocol,
    QueryAbortEvent,
    QueryFailureEvent,
    QuerySuccessEvent,
)
from ..exceptions import QservApiError, QservApiSqlError, UploadWebError
from ..models.kafka import (
    JobError,
    JobErrorCode,
    JobResultConfig,
    JobResultInfo,
    JobResultType,
    JobStatus,
)
from ..models.qserv import AsyncQueryPhase
from ..models.state import Query, RunningQuery
from ..models.votable import UploadStats
from ..storage.qserv import QservClient
from ..storage.rate import RateLimitStore
from ..storage.state import QueryStateStore
from ..storage.votable import OutputFormat, VOTableWriter

__all__ = ["ResultProcessor"]


class ResultProcessor:
    """Process the results of a completed query.

    Parameters
    ----------
    qserv_client
        Client to talk to the Qserv REST API.
    state_store
        Storage for query state.
    votable_writer
        Writer for VOTable output.
    kafka_broker
        Broker to use to publish status messages.
    rate_limit_store
        Storage for rate limiting.
    events
        Metrics events publishers.
    logger
        Logger to use.
    """

    def __init__(
        self,
        *,
        qserv_client: QservClient,
        state_store: QueryStateStore,
        votable_writer: VOTableWriter,
        kafka_broker: KafkaBroker,
        rate_limit_store: RateLimitStore,
        events: Events,
        logger: BoundLogger,
    ) -> None:
        self._qserv = qserv_client
        self._state = state_store
        self._votable = votable_writer
        self._kafka = kafka_broker
        self._rate_store = rate_limit_store
        self._events = events
        self._logger = logger

    def build_executing_status(self, query: RunningQuery) -> JobStatus:
        """Build the status for a query that's still executing.

        Parameters
        ----------
        query
            Metadata about the query.

        Returns
        -------
        JobStatus
            Job status to report to Kafka.
        """
        self._logger.debug("Query is executing", **query.to_logging_context())
        return JobStatus(
            job_id=query.job.job_id,
            execution_id=str(query.status.query_id),
            timestamp=query.status.last_update or datetime.now(tz=UTC),
            status=ExecutionPhase.EXECUTING,
            query_info=query.to_job_query_info(),
            metadata=query.job.to_job_metadata(),
        )

    async def build_query_status(
        self, query: Query, *, initial: bool = False
    ) -> JobStatus:
        """Retrieve query status and convert it to a job status update.

        If the query has already completed, retrieve the results if successful
        and return an appropriate final status. Otherwise, return a status
        message indicating that the job has started executing.

        Parameters
        ----------
        query
            Metadata about the query without the Qserv status.
        initial
            Whether this is the first status call immediately after starting
            the job.

        Returns
        -------
        JobStatus
            Job status to report to Kafka.
        """
        logger = self._logger.bind(**query.to_logging_context())

        # Get the current query status.
        try:
            status = await self._qserv.get_query_status(query.query_id)
        except QservApiError as e:
            logger.exception("Unable to get job status", error=str(e))
            update = JobStatus(
                job_id=query.job.job_id,
                execution_id=str(query.query_id),
                timestamp=datetime.now(tz=UTC),
                status=ExecutionPhase.ERROR,
                error=e.to_job_error(),
                metadata=query.job.to_job_metadata(),
            )
            await self._delete_query_data(query, logger)
            return update
        full_query = RunningQuery.from_query(query, status)

        # Based on the status, process the results.
        match status.status:
            case AsyncQueryPhase.ABORTED:
                result = await self._build_aborted_status(full_query)
            case AsyncQueryPhase.EXECUTING:
                if initial:
                    await self._state.store_query(full_query)
                else:
                    await self._state.update_status(query.query_id, status)
                return self.build_executing_status(full_query)
            case AsyncQueryPhase.COMPLETED:
                result = await self._build_completed_status(
                    full_query, initial=initial
                )
            case AsyncQueryPhase.FAILED | AsyncQueryPhase.FAILED_LR:
                result = await self._build_failed_status(full_query)
            case _:  # pragma: no cover
                raise ValueError(f"Unknown phase {status.status}")

        # Query was completed, either successfully or unsuccessfully. Delete
        # any state storage needed for it, update rate limits, and return the
        # resulting status.
        await self._delete_query_data(query, logger)
        return result

    async def publish_status(self, status: JobStatus) -> None:
        """Publish a status update to Kafka.

        Parameters
        ----------
        status
            Status update to publish.
        """
        await self._kafka.publish(
            status.model_dump(mode="json"),
            config.job_status_topic,
            headers={"Content-Type": "application/json"},
        )

    async def _build_aborted_status(self, query: RunningQuery) -> JobStatus:
        """Construct the status for an aborted job.

        Parameters
        ----------
        query
            Metadata about query.

        Returns
        -------
        JobStatus
            Status for the query.
        """
        self._logger.info("Job aborted", **query.to_logging_context())
        timestamp = query.status.last_update or datetime.now(tz=UTC)
        event = QueryAbortEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            elapsed=timestamp - query.start,
        )
        await self._events.query_abort.publish(event)
        return JobStatus(
            job_id=query.job.job_id,
            execution_id=str(query.status.query_id),
            timestamp=query.status.last_update or datetime.now(tz=UTC),
            status=ExecutionPhase.ABORTED,
            query_info=query.to_job_query_info(finished=True),
            metadata=query.job.to_job_metadata(),
        )

    async def _build_completed_status(
        self,
        query: RunningQuery,
        *,
        initial: bool = False,
    ) -> JobStatus:
        """Retrieve results and construct status for a completed job.

        This method is responsible for retrieving the results from Qserv,
        encoding them, and uploading the resulting VOTable to the provided
        URL, as well as constructing the status response.

        Parameters
        ----------
        query
            Metadata about the query.
        initial
            Whether this is the initial invocation, immediately after creating
            the job.

        Returns
        -------
        JobStatus
            Status for the query.
        """
        logger = self._logger.bind(**query.to_logging_context())
        logger.debug("Processing job completion")

        # Retrieve and upload the results.
        try:
            stats = await self._upload_results_with_retry(query, logger)
        except (QservApiError, UploadWebError, TimeoutError) as e:
            return await self._build_exception_status(query, e)

        # Can be removed and created made non-optional after the upgrade is
        # fully deployed.
        if not query.created:
            query.created = query.start

        # Send a metrics event for the job completion and log it.
        now = datetime.now(tz=UTC)
        qserv_end = query.status.last_update or now
        qserv_elapsed = qserv_end - query.status.query_begin
        qserv_elapsed_sec = int(qserv_elapsed.total_seconds())
        if qserv_elapsed.total_seconds() > 0:
            qserv_rate = query.status.collected_bytes / qserv_elapsed_sec
        else:
            qserv_rate = None
        submit_elapsed = query.created - query.start
        event = QuerySuccessEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            elapsed=now - query.start,
            qserv_elapsed=qserv_elapsed,
            result_elapsed=stats.elapsed,
            submit_elapsed=submit_elapsed,
            rows=stats.rows,
            qserv_size=query.status.collected_bytes,
            encoded_size=stats.data_bytes,
            result_size=stats.total_bytes,
            rate=stats.data_bytes / (now - query.start).total_seconds(),
            qserv_rate=qserv_rate,
            result_rate=stats.data_bytes / stats.elapsed.total_seconds(),
            upload_tables=len(query.job.upload_tables),
            immediate=initial,
        )
        await self._events.query_success.publish(event)
        logger.info(
            "Job complete and results uploaded",
            rows=stats.rows,
            encoded_size=stats.data_bytes,
            total_size=stats.total_bytes,
            elapsed=(now - query.start).total_seconds(),
            qserv_elapsed=qserv_elapsed_sec,
            result_elapsed=stats.elapsed.total_seconds(),
            submit_elapsed=submit_elapsed.total_seconds(),
        )

        # Delete the results.
        try:
            await self._qserv.delete_result(query.query_id)
        except QservApiError:
            logger.exception("Cannot delete results")

        # Return the resulting status.
        return JobStatus(
            job_id=query.job.job_id,
            execution_id=str(query.query_id),
            timestamp=datetime.now(tz=UTC),
            status=ExecutionPhase.COMPLETED,
            query_info=query.to_job_query_info(finished=True),
            result_info=JobResultInfo(
                total_rows=stats.rows,
                result_location=query.job.result_location,
                format=query.job.result_format.format,
            ),
            metadata=query.job.to_job_metadata(),
        )

    async def _build_exception_status(
        self,
        query: RunningQuery,
        exc: QservApiError | UploadWebError | TimeoutError,
    ) -> JobStatus:
        """Construct the job status for an exception.

        Parameters
        ----------
        query
            Query metadata.
        exc
            Exception that caused the job to fail.

        Returns
        -------
        JobStatus
            Status for the query.
        """
        logger = self._logger.bind(**query.to_logging_context())
        now = datetime.now(tz=UTC)
        elapsed = now - query.start
        elapsed_seconds = elapsed.total_seconds()

        # Analyze the exception.
        match exc:
            case QservApiError() | UploadWebError():
                if isinstance(exc, UploadWebError):
                    msg = "Unable to upload results"
                else:
                    msg = "Unable to retrieve results"
                logger.exception(msg, error=str(exc), elapsed=elapsed_seconds)
                error = exc.to_job_error()
            case TimeoutError():
                logger.exception(
                    "Retrieving and uploading results timed out",
                    elapsed=elapsed_seconds,
                    timeout=config.result_timeout.total_seconds(),
                )
                error = JobError(
                    code=JobErrorCode.result_timeout,
                    message="Retrieving and uploading results timed out",
                )
            case _:
                assert_never(exc)

        # Send a metrics event for the failure.
        event = QueryFailureEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            error=error.code,
            elapsed=elapsed,
        )
        await self._events.query_failure.publish(event)

        # Return the job status to send to Kafka.
        return JobStatus(
            job_id=query.job.job_id,
            execution_id=str(query.query_id),
            timestamp=now,
            status=ExecutionPhase.ERROR,
            error=error,
            metadata=query.job.to_job_metadata(),
        )

    async def _build_failed_status(self, query: RunningQuery) -> JobStatus:
        """Build the status for a failed job.

        Currently, Qserv has no way of reporting an error, so we have to
        synthesize an error.

        Parameters
        ----------
        query
            Query metadata.

        Returns
        -------
        JobStatus
            Status for the query.
        """
        metadata = query.job.to_job_metadata()
        if query.status.status == AsyncQueryPhase.FAILED_LR:
            msg = "Query failed in backend because results were too large"
            code = JobErrorCode.backend_results_too_large
            error = (
                "Query results are too large to return; please narrow your"
                " query and try again"
            )
        else:
            msg = "Backend reported query failure"
            code = JobErrorCode.backend_error
            error = "Query failed in backend"
        if query.status.error:
            error = f"{error}: {query.status.error}"
        self._logger.warning(
            msg,
            **query.to_logging_context(),
            query=metadata.model_dump(mode="json", exclude_none=True),
            status=query.status.model_dump(mode="json", exclude_none=True),
        )
        event = QueryFailureEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            error=code,
            elapsed=datetime.now(tz=UTC) - query.start,
        )
        await self._events.query_failure.publish(event)
        return JobStatus(
            job_id=query.job.job_id,
            execution_id=str(query.status.query_id),
            timestamp=query.status.last_update or datetime.now(tz=UTC),
            status=ExecutionPhase.ERROR,
            query_info=query.to_job_query_info(finished=True),
            error=JobError(code=code, message=error),
            metadata=metadata,
        )

    async def _delete_query_data(
        self, query: Query, logger: BoundLogger
    ) -> None:
        """Delete stored information for the query.

        Remove the query from the Qserv Kafka bridge state storage and delete
        any uploaded temporary tables from Qserv.

        Parameters
        ----------
        query
            Query metadata.
        logger
            Logger to use.
        """
        await self._state.delete_query(query.query_id)
        await self._rate_store.end_query(query.job.owner)

        # Delete any temporary databases.
        databases_to_delete = {t.database for t in query.job.upload_tables}
        for database in databases_to_delete:
            try:
                await self._qserv.delete_database(database)
            except QservApiError as e:
                logger.exception(
                    "Unable to delete temporary database, orphaning it",
                    error=str(e),
                    database_name=database,
                )

    async def _upload_results(self, query: RunningQuery) -> UploadStats:
        """Retrieve and upload the results.

        Parameters
        ----------
        query
            Query metadata.

        Returns
        -------
        UploadStats
            Statistics about the upload.

        Raises
        ------
        QservApiSqlError
            Raised if there was a failure retrieving the results via the
            SQL database connection.
        UploadWebError
            Raised if there was a failure to upload the results.
        TimeoutError
            Raised if the processing and upload did not complete within the
            configured timeout.
        """
        result_start = datetime.now(tz=UTC)
        results = self._qserv.get_query_results_gen(query.query_id)
        timeout = config.result_timeout.total_seconds()

        output_format = self._determine_output_format(query.job.result_format)

        async with asyncio.timeout(timeout):
            size = await self._votable.store(
                query.job.result_url,
                query.job.result_format,
                results,
                format_type=output_format,
                maxrec=query.job.maxrec,
            )
            return UploadStats(
                elapsed=datetime.now(tz=UTC) - result_start, **asdict(size)
            )

    def _determine_output_format(
        self, result_config: JobResultConfig
    ) -> OutputFormat:
        """Determine output format from job configuration."""
        if result_config.format.type == JobResultType.Parquet:
            return OutputFormat.Parquet
        else:
            return OutputFormat.VOTable

    async def _upload_results_with_retry(
        self, query: RunningQuery, logger: BoundLogger
    ) -> UploadStats:
        """Retrieve and upload the results, with retries.

        Retry the attempt to retrieve and upload the results on SQL or HTTP
        error to work around flaky connections to Qserv (and the occasional
        GCS hiccup). This cannot use the retry logic at the storage level
        since the SQL call and the HTTP call have to be coordinated.

        Parameters
        ----------
        query
            Query metadata.
        logger
            Logger to use.

        Returns
        -------
        UploadStats
            Statistics about the upload.

        Raises
        ------
        QservApiSqlError
            Raised if there was a failure retrieving the results via the
            SQL database connection.
        UploadWebError
            Raised if there was a failure to upload the results.
        TimeoutError
            Raised if the processing and upload did not complete within the
            configured timeout.
        """
        for _ in range(1, config.qserv_retry_count):
            try:
                return await self._upload_results(query)
            except (QservApiSqlError, UploadWebError) as e:
                delay = config.qserv_retry_delay.total_seconds()
                if isinstance(e, QservApiSqlError):
                    msg = f"SQL call to Qserv failed, retrying after {delay}s"
                    event = QservFailureEvent(protocol=QservProtocol.SQL)
                    await self._events.qserv_failure.publish(event)
                else:
                    msg = f"Upload of results failed, retrying after {delay}s"
                logger.exception(msg)
                await asyncio.sleep(delay)

        # Fell through, so failed max_tries - 1 times. Try one more time,
        # re-raising the exception.
        try:
            return await self._upload_results(query)
        except QservApiSqlError:
            event = QservFailureEvent(protocol=QservProtocol.SQL)
            await self._events.qserv_failure.publish(event)
            raise
