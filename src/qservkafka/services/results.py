"""Processing of completed queries."""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import override

from faststream.kafka import KafkaBroker
from safir.sentry import report_exception
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ..config import config
from ..events import (
    BigQueryFailureEvent,
    BigQuerySuccessEvent,
    Events,
    QservFailureEvent,
    QservProtocol,
    QservSuccessEvent,
    QueryAbortEvent,
    QueryFailureEvent,
    QuerySuccessEvent,
)
from ..exceptions import (
    BackendApiError,
    BackendApiTransientError,
    UploadWebError,
)
from ..models.kafka import JobError, JobErrorCode, JobResultInfo, JobStatus
from ..models.query import AsyncQueryPhase
from ..models.state import Query, RunningQuery
from ..models.votable import UploadStats
from ..storage.backend import DatabaseBackend
from ..storage.rate import RateLimitStore
from ..storage.state import QueryStateStore
from ..storage.votable import VOTableWriter

__all__ = [
    "BigQueryResultProcessor",
    "QservResultProcessor",
    "ResultProcessor",
]


class ResultProcessor(ABC):
    """Process the results of a completed query.

    Parameters
    ----------
    backend
        Database backend client (Qserv, BigQuery, etc.).
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
    slack_client
        Client to send errors to Slack
    logger
        Logger to use.
    """

    def __init__(
        self,
        *,
        backend: DatabaseBackend,
        state_store: QueryStateStore,
        votable_writer: VOTableWriter,
        kafka_broker: KafkaBroker,
        rate_limit_store: RateLimitStore,
        events: Events,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self._backend = backend
        self._state = state_store
        self._votable = votable_writer
        self._kafka = kafka_broker
        self._rate_store = rate_limit_store
        self._events = events
        self._slack_client = slack_client
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
            execution_id=str(query.query_id),
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
            status = await self._backend.get_query_status(query.query_id)
        except BackendApiError as e:
            await report_exception(e, slack_client=self._slack_client)
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
            case AsyncQueryPhase.FAILED:
                result = await self._build_failed_status(full_query)

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

    @abstractmethod
    async def publish_success_event(
        self,
        *,
        query: RunningQuery,
        stats: UploadStats,
        elapsed: timedelta,
        backend_elapsed: timedelta,
        backend_rate: float | None,
        delete_elapsed: timedelta | None,
        initial: bool,
    ) -> QuerySuccessEvent:
        """Publish backend-specific success event.

        Parameters
        ----------
        query
            Query metadata.
        stats
            Upload statistics.
        elapsed
            Total elapsed time.
        backend_elapsed
            Time spent in backend.
        backend_rate
            Backend processing rate (bytes/sec).
        delete_elapsed
            Time spent deleting results.
        initial
            Whether this was immediate completion.

        Returns
        -------
        QuerySuccessEvent
            The published event for logging.
        """

    @abstractmethod
    async def _publish_backend_failure_event(self) -> None:
        """Publish backend-specific failure event during retry."""

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
            execution_id=str(query.query_id),
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

        This method is responsible for retrieving the results from the backend,
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
        except (BackendApiError, UploadWebError, TimeoutError) as e:
            return await self._build_exception_status(query, e)

        # Delete the results if configured to do so.
        delete_elapsed = None
        if config.qserv_delete_queries:
            delete_start = datetime.now(tz=UTC)
            try:
                await self._backend.delete_result(query.query_id)
                delete_elapsed = datetime.now(tz=UTC) - delete_start
            except BackendApiError as e:
                delete_elapsed = None
                await report_exception(e, slack_client=self._slack_client)
                logger.exception("Cannot delete results")
            delete_elapsed = datetime.now(tz=UTC) - delete_start

        # Send a metrics event for the job completion and log it.
        now = datetime.now(tz=UTC)
        backend_end = query.status.last_update or now
        backend_elapsed = backend_end - query.status.query_begin
        backend_elapsed_sec = backend_elapsed.total_seconds()
        if backend_elapsed_sec > 0:
            backend_rate = query.status.collected_bytes / backend_elapsed_sec
        else:
            backend_rate = None
        elapsed = now - (query.queued or query.start)
        event = await self.publish_success_event(
            query=query,
            stats=stats,
            elapsed=elapsed,
            backend_elapsed=backend_elapsed,
            backend_rate=backend_rate,
            delete_elapsed=delete_elapsed,
            initial=initial,
        )
        logger.info(
            "Job complete and results uploaded", **event.to_logging_context()
        )

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
        exc: BackendApiError | UploadWebError | TimeoutError,
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
            case BackendApiError() | UploadWebError():
                if isinstance(exc, UploadWebError):
                    msg = "Unable to upload results"
                else:
                    msg = "Unable to retrieve results"
                await report_exception(exc, slack_client=self._slack_client)
                logger.exception(msg, error=str(exc), elapsed=elapsed_seconds)
                error = exc.to_job_error()
            case TimeoutError():
                await report_exception(exc, slack_client=self._slack_client)
                logger.exception(
                    "Retrieving and uploading results timed out",
                    elapsed=elapsed_seconds,
                    timeout=config.result_timeout.total_seconds(),
                )
                error = JobError(
                    code=JobErrorCode.result_timeout,
                    message="Retrieving and uploading results timed out",
                )

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
        if query.status.results_too_large:
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
            execution_id=str(query.query_id),
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

        Remove the query from the Kafka bridge state storage and delete
        any uploaded temporary tables.

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
                await self._backend.delete_database(database)
            except BackendApiError as e:
                await report_exception(e, slack_client=self._slack_client)
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
        BackendApiTransientError
            Raised if there was a transient failure retrieving results from
            the backend.
        UploadWebError
            Raised if there was a failure to upload the results.
        TimeoutError
            Raised if the processing and upload did not complete within the
            configured timeout.
        """
        result_start = datetime.now(tz=UTC)
        results = self._backend.get_query_results_gen(query.query_id)
        timeout = config.result_timeout.total_seconds()

        async with asyncio.timeout(timeout):
            size = await self._votable.store(
                query.job.result_url,
                query.job.result_format,
                results,
                maxrec=query.job.maxrec,
            )
            return UploadStats(
                elapsed=datetime.now(tz=UTC) - result_start, **asdict(size)
            )

    async def _upload_results_with_retry(
        self, query: RunningQuery, logger: BoundLogger
    ) -> UploadStats:
        """Retrieve and upload the results, with retries.

        Retry the attempt to retrieve and upload the results on SQL or HTTP
        error to work around flaky connections to backend (and the occasional
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
        BackendApiTransientError
            Raised if there was a transient failure retrieving results from
            the backend.
        UploadWebError
            Raised if there was a failure to upload the results.
        TimeoutError
            Raised if the processing and upload did not complete within the
            configured timeout.
        """
        for _ in range(1, config.backend_retry_count):
            try:
                return await self._upload_results(query)
            except (BackendApiTransientError, UploadWebError) as e:
                delay = config.backend_retry_delay.total_seconds()
                if isinstance(e, BackendApiTransientError):
                    await self._publish_backend_failure_event()
                    msg = f"Backend call failed, retrying after {delay}s"
                else:
                    msg = f"Upload of results failed, retrying after {delay}s"

                # We don't want to notify Sentry or Slack about exceptions
                # here because we are going to retry.
                logger.exception(msg)
                await asyncio.sleep(delay)

        # Fell through, so failed max_tries - 1 times. Try one more time,
        # re-raising the exception.
        try:
            return await self._upload_results(query)
        except BackendApiTransientError:
            await self._publish_backend_failure_event()
            raise


class QservResultProcessor(ResultProcessor):
    """Result processor for Qserv backend.

    Publishes Qserv-specific metrics events.
    """

    @override
    async def publish_success_event(
        self,
        *,
        query: RunningQuery,
        stats: UploadStats,
        elapsed: timedelta,
        backend_elapsed: timedelta,
        backend_rate: float | None,
        delete_elapsed: timedelta | None,
        initial: bool,
    ) -> QservSuccessEvent:
        event = QservSuccessEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            elapsed=elapsed,
            kafka_elapsed=query.start - query.queued if query.queued else None,
            backend_elapsed=backend_elapsed,
            result_elapsed=stats.elapsed,
            submit_elapsed=query.created - query.start,
            delete_elapsed=delete_elapsed,
            rows=stats.rows,
            backend_size=query.status.collected_bytes,
            encoded_size=stats.data_bytes,
            result_size=stats.total_bytes,
            rate=stats.data_bytes / elapsed.total_seconds(),
            backend_rate=backend_rate,
            result_rate=stats.data_bytes / stats.elapsed.total_seconds(),
            upload_tables=len(query.job.upload_tables),
            immediate=initial,
        )
        await self._events.qserv_success.publish(event)
        return event

    @override
    async def _publish_backend_failure_event(self) -> None:
        event = QservFailureEvent(protocol=QservProtocol.SQL)
        await self._events.qserv_failure.publish(event)


class BigQueryResultProcessor(ResultProcessor):
    """Result processor for BigQuery backend.

    Publishes BigQuery-specific metrics events.
    """

    @override
    async def publish_success_event(
        self,
        *,
        query: RunningQuery,
        stats: UploadStats,
        elapsed: timedelta,
        backend_elapsed: timedelta,
        backend_rate: float | None,
        delete_elapsed: timedelta | None,
        initial: bool,
    ) -> BigQuerySuccessEvent:
        event = BigQuerySuccessEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            elapsed=elapsed,
            kafka_elapsed=query.start - query.queued if query.queued else None,
            backend_elapsed=backend_elapsed,
            result_elapsed=stats.elapsed,
            submit_elapsed=query.created - query.start,
            delete_elapsed=delete_elapsed,
            rows=stats.rows,
            backend_size=query.status.collected_bytes,
            backend_rate=backend_rate,
            encoded_size=stats.data_bytes,
            result_size=stats.total_bytes,
            rate=stats.data_bytes / elapsed.total_seconds(),
            result_rate=stats.data_bytes / stats.elapsed.total_seconds(),
            upload_tables=len(query.job.upload_tables),
            immediate=initial,
        )
        await self._events.bigquery_success.publish(event)
        return event

    @override
    async def _publish_backend_failure_event(self) -> None:
        event = BigQueryFailureEvent()
        await self._events.bigquery_failure.publish(event)
