"""Query job status publisher."""

from datetime import UTC, datetime

from faststream.kafka.publisher import DefaultPublisher
from rubin.gafaelfawr import GafaelfawrTapQuota
from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ..config import BackendType, config
from ..events import (
    BigQuerySuccessEvent,
    Events,
    QservSuccessEvent,
    QueryAbortEvent,
    QueryFailureEvent,
)
from ..exceptions import QueryError
from ..models.kafka import JobError, JobErrorCode, JobRun, JobStatus
from ..models.state import Query, RunningQuery, StartedQuery
from ..models.votable import UploadStats

__all__ = ["StatusPublisher"]


class StatusPublisher:
    """Publish query job status to Kafka.

    This class is responsible for sending events, log messages, and
    `~qservkafka.models.kafka.JobStatus` messages to Kafka for all of the
    other services.

    Parameters
    ----------
    kafka_publisher
        Broker to use to publish status messages.
    events
        Metrics events publishers.
    logger
        Logger to use.
    """

    def __init__(
        self,
        kafka_publisher: DefaultPublisher,
        events: Events,
        logger: BoundLogger,
    ) -> None:
        self._kafka = kafka_publisher
        self._events = events
        self._logger = logger

        self._success_event_class: type[
            BigQuerySuccessEvent | QservSuccessEvent
        ]
        match config.backend:
            case BackendType.QSERV:
                self._success_event_class = QservSuccessEvent
            case BackendType.BIGQUERY:
                self._success_event_class = BigQuerySuccessEvent

    async def publish_aborted(self, query: RunningQuery) -> None:
        """Publish status for an aborted job.

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
        await self._publish_status(query.to_job_status(ExecutionPhase.ABORTED))
        event = QueryAbortEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            elapsed=query.elapsed,
        )
        await self._events.query_abort.publish(event)

    async def publish_completed(
        self, query: RunningQuery, stats: UploadStats
    ) -> None:
        """Publish status for a completed query.

        The results of the query must be uploaded before calling this method.

        Parameters
        ----------
        query
            Completed query.
        stats
            Statistics about the uploaded results.
        """
        await self._publish_status(query.to_completed_job_status(stats))
        logger = self._logger.bind(**query.to_logging_context())
        now = datetime.now(tz=UTC)
        backend_end = query.status.last_update or now
        backend_elapsed = backend_end - query.status.query_begin
        backend_elapsed_sec = backend_elapsed.total_seconds()
        if backend_elapsed_sec > 0:
            backend_rate = query.status.collected_bytes / backend_elapsed_sec
        else:
            backend_rate = None
        elapsed = now - (query.queued or query.start)
        event = self._success_event_class(
            job_id=query.job.job_id,
            username=query.job.owner,
            elapsed=elapsed,
            kafka_elapsed=query.start - query.queued if query.queued else None,
            result_elapsed=stats.elapsed,
            submit_elapsed=query.created - query.start,
            rows=stats.rows,
            encoded_size=stats.data_bytes,
            result_size=stats.total_bytes,
            rate=stats.data_bytes / elapsed.total_seconds(),
            result_rate=stats.data_bytes / stats.elapsed.total_seconds(),
            upload_tables=len(query.job.upload_tables),
            backend_elapsed=backend_elapsed,
            backend_size=query.status.collected_bytes,
            backend_rate=backend_rate,
        )
        await self._events.query_success.publish(event)
        logger = logger.bind(**event.to_logging_context())
        logger.info("Job complete and results uploaded")

    async def publish_exception(
        self, query: StartedQuery, exc: QueryError
    ) -> None:
        """Publish status for a query that started and then failed.

        Unlike the other ``publish_*`` methods, the caller is responsible for
        logging the exception, since the logging call should happen within the
        exception handler.

        Parameters
        ----------
        query
            Failed query.
        exc
            Exception provoking the query failure.
        """
        error = exc.to_job_error()
        status = JobStatus.from_error(query.job, error, query.query_id)
        await self._publish_status(status)
        event = QueryFailureEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            error=error.code,
            elapsed=query.elapsed,
        )
        await self._events.query_failure.publish(event)

    async def publish_executing(self, query: RunningQuery) -> None:
        """Publish status for a query that is executing.

        The status will be changed to executing if it is completed to handle
        the case of an immediately-completing query. In this case, initial
        status returned by the backend will be completed. We can't report a
        completed status to Kafka until we have uploaded the results, but we
        want to publish an immediate status update so that the TAP server
        knows the query is in progress. Override the status to executing for
        this case. The proper completed status will then be published by the
        result worker when the results have been uploaded.

        Parameters
        ----------
        query
            Executing query.
        """
        logger = self._logger.bind(**query.to_logging_context())
        logger.debug("Query is executing")
        status = query.to_job_status()
        if status.status == ExecutionPhase.COMPLETED:
            status.status = ExecutionPhase.EXECUTING
        await self._publish_status(status)

    async def publish_failed(self, query: RunningQuery) -> None:
        """Publish status for a failed query.

        Parameters
        ----------
        query
            Query metadata.
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
        job_error = JobError(code=code, message=error)
        status = query.to_job_status(ExecutionPhase.ERROR, job_error)
        await self._publish_status(status)
        event = QueryFailureEvent(
            job_id=query.job.job_id,
            username=query.job.owner,
            error=code,
            elapsed=query.elapsed,
        )
        await self._events.query_failure.publish(event)

    async def publish_invalid_request(self, job: JobRun, message: str) -> None:
        """Publish status for an invalid request.

        Parameters
        ----------
        job
            Initial query request.
        message
            Error message.
        """
        self._logger.warning(message, **job.to_logging_context())
        error = JobError(code=JobErrorCode.invalid_request, message=message)
        await self._publish_status(JobStatus.from_error(job, error))

    async def publish_quota_exceeded(
        self, job: JobRun, running: int, quota: GafaelfawrTapQuota
    ) -> None:
        """Publish status for a request that exceeds the user's quota.

        Parameters
        ----------
        job
            Initial query request.
        running
            Number of running queries.
        quota
            Maximum allowed number of concurrent running queries.
        """
        self._logger.info(
            "Query rejected due to quota",
            quota=quota.model_dump(mode="json"),
            running=running,
            **job.to_logging_context(),
        )
        error = JobError.for_quota_exceeded(running, quota)
        await self._publish_status(JobStatus.from_error(job, error))

    async def publish_start_exception(
        self, query: Query, exc: QueryError
    ) -> None:
        """Publish status for a query that failed to start due to an exception.

        Parameters
        ----------
        query
            Failed query.
        exc
            Exception provoking the query failure.
        """
        status = JobStatus.from_error(query.job, exc.to_job_error())
        await self._publish_status(status)

    async def _publish_status(self, status: JobStatus) -> None:
        """Publish a status update to Kafka.

        Parameters
        ----------
        status
            Status update to publish.
        """
        await self._kafka.publish(
            status.model_dump(mode="json"),
            headers={"Content-Type": "application/json"},
        )
