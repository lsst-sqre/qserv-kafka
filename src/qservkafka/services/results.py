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
    JobQueryInfo,
    JobResultInfo,
    JobRun,
    JobStatus,
)
from ..models.qserv import AsyncQueryPhase, AsyncQueryStatus
from ..models.votable import UploadStats
from ..storage.qserv import QservClient
from ..storage.state import QueryStateStore
from ..storage.votable import VOTableWriter

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
        events: Events,
        logger: BoundLogger,
    ) -> None:
        self._qserv = qserv_client
        self._state = state_store
        self._votable = votable_writer
        self._kafka = kafka_broker
        self._events = events
        self._logger = logger

    def build_executing_status(
        self, job: JobRun, status: AsyncQueryStatus
    ) -> JobStatus:
        """Build the status for a query that's still executing.

        Parameters
        ----------
        job
            Original query request.
        status
            Status response from Qserv.

        Returns
        -------
        JobStatus
            Job status to report to Kafka.
        """
        self._logger.debug(
            "Query is executing",
            job_id=job.job_id,
            qserv_id=str(status.query_id),
            username=job.owner,
        )
        return JobStatus(
            job_id=job.job_id,
            execution_id=str(status.query_id),
            timestamp=status.last_update or datetime.now(tz=UTC),
            status=ExecutionPhase.EXECUTING,
            query_info=JobQueryInfo.from_query_status(status),
            metadata=job.to_job_metadata(),
        )

    async def build_query_status(
        self,
        query_id: int,
        job: JobRun,
        start: datetime,
        *,
        initial: bool = False,
    ) -> JobStatus:
        """Retrieve query status and convert it to a job status update.

        If the query has already completed, retrieve the results if successful
        and return an appropriate final status. Otherwise, return a status
        message indicating that the job has started executing.

        Parameters
        ----------
        query_id
            Qserv query ID.
        job
            Initial query request.
        start
            Start time of the query.
        initial
            Whether this is the first status call immediately after starting
            the job.

        Returns
        -------
        JobStatus
            Job status to report to Kafka.
        """
        logger = self._logger.bind(
            job_id=job.job_id, qserv_id=str(query_id), username=job.owner
        )
        try:
            status = await self._qserv.get_query_status(query_id)
        except QservApiError as e:
            logger.exception("Unable to get job status", error=str(e))
            update = JobStatus(
                job_id=job.job_id,
                execution_id=str(query_id),
                timestamp=datetime.now(tz=UTC),
                status=ExecutionPhase.ERROR,
                error=e.to_job_error(),
                metadata=job.to_job_metadata(),
            )
            await self._state.delete_query(query_id)
            return update
        match status.status:
            case AsyncQueryPhase.ABORTED:
                result = await self._build_aborted_status(job, status, start)
            case AsyncQueryPhase.EXECUTING:
                if initial:
                    await self._state.store_query(
                        query_id, job, status, start=start
                    )
                else:
                    await self._state.update_status(query_id, status)
                return self.build_executing_status(job, status)
            case AsyncQueryPhase.COMPLETED:
                result = await self._build_completed_status(
                    job, status, start, initial=initial
                )
            case AsyncQueryPhase.FAILED:
                result = await self._build_failed_status(job, status, start)
            case _:  # pragma: no cover
                raise ValueError(f"Unknown phase {status.status}")
        await self._state.delete_query(query_id)
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

    async def _build_aborted_status(
        self, job: JobRun, status: AsyncQueryStatus, start: datetime
    ) -> JobStatus:
        """Construct the status for an aborted job.

        Parameters
        ----------
        job
            Original query request.
        status
            Status from Qserv.
        start
            When the query was started.

        Returns
        -------
        JobStatus
            Status for the query.
        """
        self._logger.info(
            "Job aborted",
            job_id=job.job_id,
            qserv_id=str(status.query_id),
            username=job.owner,
            total_chunks=status.total_chunks,
            completed_chunks=status.completed_chunks,
            result_bytes=status.collected_bytes,
        )
        timestamp = status.last_update or datetime.now(tz=UTC)
        event = QueryAbortEvent(username=job.owner, elapsed=timestamp - start)
        await self._events.query_abort.publish(event)
        return JobStatus(
            job_id=job.job_id,
            execution_id=str(status.query_id),
            timestamp=status.last_update or datetime.now(tz=UTC),
            status=ExecutionPhase.ABORTED,
            query_info=JobQueryInfo.from_query_status(status),
            metadata=job.to_job_metadata(),
        )

    async def _build_completed_status(
        self,
        job: JobRun,
        status: AsyncQueryStatus,
        start: datetime,
        *,
        initial: bool = False,
    ) -> JobStatus:
        """Retrieve results and construct status for a completed job.

        This method is responsible for retrieving the results from Qserv,
        encoding them, and uploading the resulting VOTable to the provided
        URL, as well as constructing the status response.

        Parameters
        ----------
        job
            Original query request.
        status
            Status from Qserv.
        start
            When the query was started.
        initial
            Whether this is the initial invocation, immediately after creating
            the job.

        Returns
        -------
        JobStatus
            Status for the query.
        """
        query_id = status.query_id
        logger = self._logger.bind(
            job_id=job.job_id, qserv_id=str(query_id), username=job.owner
        )
        logger.debug("Processing job completion")

        # Retrieve and upload the results.
        try:
            stats = await self._upload_results_with_retry(
                query_id, job, start, logger=logger
            )
        except (QservApiError, UploadWebError, TimeoutError) as e:
            return await self._build_exception_status(
                query_id, job, e, start=start
            )

        # Send a metrics event for the job completion and log it.
        now = datetime.now(tz=UTC)
        qserv_end = status.last_update or now
        qserv_elapsed = qserv_end - status.query_begin
        if qserv_elapsed.total_seconds() > 0:
            qserv_rate = status.collected_bytes / qserv_elapsed.total_seconds()
        else:
            qserv_rate = None
        event = QuerySuccessEvent(
            username=job.owner,
            elapsed=now - start,
            qserv_elapsed=qserv_elapsed,
            result_elapsed=stats.elapsed,
            rows=stats.rows,
            qserv_size=status.collected_bytes,
            encoded_size=stats.data_bytes,
            result_size=stats.total_bytes,
            rate=stats.data_bytes / (now - start).total_seconds(),
            qserv_rate=qserv_rate,
            result_rate=stats.data_bytes / stats.elapsed.total_seconds(),
            upload_tables=len(job.upload_tables),
            immediate=initial,
        )
        await self._events.query_success.publish(event)
        logger.info(
            "Job complete and results uploaded",
            rows=stats.rows,
            data_size=stats.data_bytes,
            total_size=stats.total_bytes,
            elapsed=stats.elapsed.total_seconds(),
        )

        # Delete the results.
        try:
            await self._qserv.delete_result(query_id)
        except QservApiError:
            logger.exception("Cannot delete results")

        # Return the resulting status.
        return JobStatus(
            job_id=job.job_id,
            execution_id=str(query_id),
            timestamp=status.last_update or datetime.now(tz=UTC),
            status=ExecutionPhase.COMPLETED,
            query_info=JobQueryInfo.from_query_status(status),
            result_info=JobResultInfo(
                total_rows=stats.rows,
                result_location=job.result_location,
                format=job.result_format.format,
            ),
            metadata=job.to_job_metadata(),
        )

    async def _build_exception_status(
        self,
        query_id: int,
        job: JobRun,
        exc: QservApiError | UploadWebError | TimeoutError,
        *,
        start: datetime,
    ) -> JobStatus:
        """Construct the job status for an exception.

        Parameters
        ----------
        query_id
            Qserv query ID.
        job
            Original query request.
        exc
            Exception that caused the job to fail.
        start
            When the query was started.

        Returns
        -------
        JobStatus
            Status for the query.
        """
        logger = self._logger.bind(
            job_id=job.job_id, qserv_id=str(query_id), username=job.owner
        )
        now = datetime.now(tz=UTC)
        elapsed = now - start
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
            username=job.owner, error=error.code, elapsed=elapsed
        )
        await self._events.query_failure.publish(event)

        # Return the job status to send to Kafka.
        return JobStatus(
            job_id=job.job_id,
            execution_id=str(query_id),
            timestamp=now,
            status=ExecutionPhase.ERROR,
            error=error,
            metadata=job.to_job_metadata(),
        )

    async def _build_failed_status(
        self, job: JobRun, status: AsyncQueryStatus, start: datetime
    ) -> JobStatus:
        """Build the status for a failed job.

        Currently, Qserv has no way of reporting an error, so we have to
        synthesize an error.

        Parameters
        ----------
        job
            Original query request.
        status
            Status from Qserv.
        start
            When the query was started.

        Returns
        -------
        JobStatus
            Status for the query.
        """
        metadata = job.to_job_metadata()
        self._logger.warning(
            "Backend reported query failure",
            job_id=job.job_id,
            qserv_id=str(status.query_id),
            username=job.owner,
            query=metadata.model_dump(mode="json", exclude_none=True),
            status=status.model_dump(mode="json", exclude_none=True),
            total_chunks=status.total_chunks,
            completed_chunks=status.completed_chunks,
            result_bytes=status.collected_bytes,
        )
        event = QueryFailureEvent(
            username=job.owner,
            error=JobErrorCode.backend_error,
            elapsed=datetime.now(tz=UTC) - start,
        )
        await self._events.query_failure.publish(event)
        return JobStatus(
            job_id=job.job_id,
            execution_id=str(status.query_id),
            timestamp=status.last_update or datetime.now(tz=UTC),
            status=ExecutionPhase.ERROR,
            query_info=JobQueryInfo.from_query_status(status),
            error=JobError(
                code=JobErrorCode.backend_error,
                message="Query failed in backend",
            ),
            metadata=metadata,
        )

    async def _upload_results(self, query_id: int, job: JobRun) -> UploadStats:
        """Retrieve and upload the results.

        Parameters
        ----------
        query_id
            Qserv query ID.
        job
            Original query request.

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
        results = self._qserv.get_query_results_gen(query_id)
        timeout = config.result_timeout.total_seconds()
        async with asyncio.timeout(timeout):
            size = await self._votable.store(
                job.result_url, job.result_format, results, maxrec=job.maxrec
            )
            return UploadStats(
                elapsed=datetime.now(tz=UTC) - result_start, **asdict(size)
            )

    async def _upload_results_with_retry(
        self,
        query_id: int,
        job: JobRun,
        start: datetime,
        *,
        logger: BoundLogger,
    ) -> UploadStats:
        """Retrieve and upload the results, with retries.

        Retry the attempt to retrieve and upload the results on SQL or HTTP
        error to work around flaky connections to Qserv (and the occasional
        GCS hiccup). This cannot use the retry logic at the storage level
        since the SQL call and the HTTP call have to be coordinated.

        Parameters
        ----------
        query_id
            Qserv query ID.
        job
            Original query request.
        start
            When the query was started.
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
                return await self._upload_results(query_id, job)
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
            return await self._upload_results(query_id, job)
        except QservApiSqlError:
            event = QservFailureEvent(protocol=QservProtocol.SQL)
            await self._events.qserv_failure.publish(event)
            raise
