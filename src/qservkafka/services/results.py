"""Processing of completed queries."""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import override

from faststream.kafka import KafkaBroker
from safir.arq import ArqQueue
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
    QueryError,
    UploadTimeoutError,
    UploadWebError,
)
from ..models.kafka import JobError, JobErrorCode, JobRun, JobStatus
from ..models.query import AsyncQueryPhase
from ..models.state import RunningQuery, StartedQuery
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
    arq_queue
        Shared client used to dispatch jobs to arq workers.
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
        arq_queue: ArqQueue,
        events: Events,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self._backend = backend
        self._state = state_store
        self._votable = votable_writer
        self._kafka = kafka_broker
        self._rate_store = rate_limit_store
        self._arq = arq_queue
        self._events = events
        self._slack_client = slack_client
        self._logger = logger

    async def delete_query_data(self, query: StartedQuery) -> None:
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
        logger = self._logger.bind(**query.to_logging_context())
        logger.debug("Cleaning up query data")
        await self._state.delete_query(query.query_id)
        await self._rate_store.end_query(query.job.owner)
        await self.delete_uploaded_databases(query.job)

    async def delete_uploaded_databases(
        self,
        job: JobRun,
        databases: set[str] | None = None,
    ) -> None:
        """Delete any temporary databases created for uploaded tables.

        Parameters
        ----------
        job
            Job metadata.
        databases
            If given, the databases to delete. Otherwise, all databases in the
            list of tables to upload will be deleted.
        """
        logger = self._logger.bind(**job.to_logging_context())
        if databases is None:
            databases = {t.database for t in job.upload_tables}
        logger.debug(
            "Deleting upload databases",
            upload_table_count=len(job.upload_tables),
            upload_table_types=[type(t).__name__ for t in job.upload_tables],
            databases=list(databases),
        )
        for database in databases:
            try:
                await self._backend.delete_database(database)
                logger.debug("Deleted upload database", database_name=database)
            except BackendApiError as e:
                await report_exception(e, slack_client=self._slack_client)
                logger.exception(
                    "Unable to delete temporary database, orphaning it",
                    error=str(e),
                    database_name=database,
                )

    async def get_running_query(self, query: StartedQuery) -> RunningQuery:
        """Retrieve the query status and construct a running query record.

        Parameters
        ----------
        query
            Metadata about the query, possibly without the backend status.

        Returns
        -------
        RunningQuery
            Query enhanced with backend status.
        """
        try:
            status = await self._backend.get_query_status(query.query_id)
        except BackendApiError as e:
            await report_exception(e, slack_client=self._slack_client)
            logger = self._logger.bind(**query.to_logging_context())
            logger.exception("Unable to get job status", error=str(e))
            raise
        return RunningQuery.from_started_query(query, status)

    async def handle_query_exception(
        self, query: StartedQuery, exc: QueryError
    ) -> None:
        """Handle a query that started but failed due to an exception.

        Cleans up the job and publishes an appropriate status message to
        Kafka.

        Parameters
        ----------
        query
            Failed query.
        exc
            Exception provoking the query failure.
        """
        error = exc.to_job_error()
        status = JobStatus.from_error(query.job, error, query.query_id)
        await self.publish_status(status)
        await self.delete_query_data(query)

    async def process_query(self, query: RunningQuery) -> JobStatus:
        """Convert the query status to a Kafka update.

        If the query has already completed, retrieve the results if successful
        and return an appropriate final status. Otherwise, return a status
        message indicating that the job is executing.

        Parameters
        ----------
        query
            Metadata about the query without the Qserv status.

        Returns
        -------
        JobStatus
            Job status to report to Kafka.
        """
        match query.status.status:
            case AsyncQueryPhase.ABORTED:
                return await self._build_aborted_status(query)
            case AsyncQueryPhase.EXECUTING:
                logger = self._logger.bind(**query.to_logging_context())
                logger.debug("Query is executing")
                return query.to_job_status()
            case AsyncQueryPhase.COMPLETED:
                return await self._build_completed_status(query)
            case AsyncQueryPhase.FAILED:
                return await self._build_failed_status(query)

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
        return query.to_job_status(ExecutionPhase.ABORTED)

    async def _build_completed_status(self, query: RunningQuery) -> JobStatus:
        """Retrieve results and construct status for a completed job.

        This method is responsible for retrieving the results from the backend,
        encoding them, and uploading the resulting VOTable to the provided
        URL, as well as constructing the status response.

        Parameters
        ----------
        query
            Metadata about the query.

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
        except QueryError as e:
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
        )
        logger.info(
            "Job complete and results uploaded", **event.to_logging_context()
        )

        # Return the resulting status.
        return query.to_completed_job_status(stats)

    async def _build_exception_status(
        self, query: RunningQuery, exc: QueryError
    ) -> JobStatus:
        """Construct the job status for an exception.

        This method may only be called from inside an exception handler.

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
        logger = self._logger.bind(
            **query.to_logging_context(), **exc.to_logging_context()
        )
        now = datetime.now(tz=UTC)
        elapsed = now - query.start

        # Analyze the exception. _build_exception_status is only called inside
        # an exception handler, so suppress the Ruff diagnostics since Ruff
        # has no way of knowing that.
        await report_exception(exc, slack_client=self._slack_client)
        logger.exception(exc.description)  # noqa: LOG004
        error = exc.to_job_error()

        # Send a metrics event for the failure.
        event = QueryFailureEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            error=error.code,
            elapsed=elapsed,
        )
        await self._events.query_failure.publish(event)

        # Return the job status to send to Kafka.
        return JobStatus.from_error(query.job, error, query.query_id)

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
        self._logger.warning(msg, **query.to_logging_context())
        event = QueryFailureEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            error=code,
            elapsed=datetime.now(tz=UTC) - query.start,
        )
        await self._events.query_failure.publish(event)
        job_error = JobError(code=code, message=error)
        return query.to_job_status(ExecutionPhase.ERROR, job_error)

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
        start = datetime.now(tz=UTC)
        results = self._backend.get_query_results_gen(query.query_id)
        timeout = config.result_timeout.total_seconds()
        try:
            async with asyncio.timeout(timeout):
                size = await self._votable.store(
                    query.job.result_url,
                    query.job.result_format,
                    results,
                    maxrec=query.job.maxrec,
                )
            elapsed = datetime.now(tz=UTC) - start
            return UploadStats(elapsed=elapsed, **asdict(size))
        except TimeoutError as e:
            elapsed = datetime.now(tz=UTC) - start
            raise UploadTimeoutError(elapsed) from e

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
    ) -> QservSuccessEvent:
        event = QservSuccessEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            elapsed=elapsed,
            kafka_elapsed=query.start - query.queued if query.queued else None,
            qserv_elapsed=backend_elapsed,
            result_elapsed=stats.elapsed,
            submit_elapsed=query.created - query.start,
            delete_elapsed=delete_elapsed,
            rows=stats.rows,
            qserv_size=query.status.collected_bytes,
            encoded_size=stats.data_bytes,
            result_size=stats.total_bytes,
            rate=stats.data_bytes / elapsed.total_seconds(),
            qserv_rate=backend_rate,
            result_rate=stats.data_bytes / stats.elapsed.total_seconds(),
            upload_tables=len(query.job.upload_tables),
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
    ) -> BigQuerySuccessEvent:
        event = BigQuerySuccessEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            elapsed=elapsed,
            kafka_elapsed=query.start - query.queued if query.queued else None,
            bigquery_elapsed=backend_elapsed,
            result_elapsed=stats.elapsed,
            submit_elapsed=query.created - query.start,
            delete_elapsed=delete_elapsed,
            rows=stats.rows,
            bigquery_size=query.status.collected_bytes,
            bigquery_rate=backend_rate,
            encoded_size=stats.data_bytes,
            result_size=stats.total_bytes,
            rate=stats.data_bytes / elapsed.total_seconds(),
            result_rate=stats.data_bytes / stats.elapsed.total_seconds(),
            upload_tables=len(query.job.upload_tables),
        )
        await self._events.bigquery_success.publish(event)
        return event

    @override
    async def _publish_backend_failure_event(self) -> None:
        event = BigQueryFailureEvent()
        await self._events.bigquery_failure.publish(event)
