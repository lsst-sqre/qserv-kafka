"""Service to create new queries."""

from datetime import UTC, datetime

from safir.arq import ArqQueue
from safir.sentry import report_exception
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger

from ..config import config
from ..events import Events, TemporaryTableUploadEvent
from ..exceptions import (
    BackendApiError,
    BackendApiFailedError,
    TableUploadWebError,
)
from ..models.kafka import (
    JobCancel,
    JobError,
    JobErrorCode,
    JobResultSerialization,
    JobResultType,
    JobRun,
    JobStatus,
)
from ..models.query import AsyncQueryPhase
from ..models.state import Query, StartedQuery
from ..storage.backend import DatabaseBackend
from ..storage.gafaelfawr import GafaelfawrStorage
from ..storage.rate import RateLimitStore
from ..storage.state import QueryStateStore
from .results import ResultProcessor

__all__ = ["QueryService"]


class QueryService:
    """Start or cancel queries.

    Parameters
    ----------
    backend
        Database backend client (Qserv, BigQuery, etc.).
    state_store
        Storage for query state.
    result_processor
        Service to process completed queries.
    rate_limit_store
        Storage for rate limiting.
    gafaelfawr_storage
        Storage for quota information.
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
        result_processor: ResultProcessor,
        rate_limit_store: RateLimitStore,
        gafaelfawr_storage: GafaelfawrStorage,
        arq_queue: ArqQueue,
        events: Events,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self._backend = backend
        self._state = state_store
        self._results = result_processor
        self._rate_store = rate_limit_store
        self._gafaelfawr = gafaelfawr_storage
        self._arq_queue = arq_queue
        self._events = events
        self._slack_client = slack_client
        self._logger = logger

    async def cancel_query(self, message: JobCancel) -> JobStatus | None:
        """Cancel a running query.

        Parameters
        ----------
        message
            Request to cancel the query.

        Returns
        -------
        JobStatus or None
            New status of job, or `None` if there is no update or if the
            cancel message is invalid.
        """
        logger = self._logger.bind(
            job_id=message.job_id, username=message.owner
        )
        query_id = message.execution_id
        logger = logger.bind(backend_id=query_id)
        query = await self._state.get_query(query_id)
        if not query:
            logger.warning("Cannot cancel unknown or completed job")
            return None

        # Cancel the query. If this fails, check to see if it only failed
        # because the job finished and, if so, quietly do nothing and let
        # normal result processing pick up the completion since it's too late
        # to cancel.
        #
        # There's not much we can do with exceptions other than log them,
        # since we don't have a way of returning a cancelation error to the
        # TAP server, so we send a status update matching the last known
        # status in that case.
        try:
            await self._backend.cancel_query(query_id)
        except BackendApiError as e:
            try:
                status = await self._backend.get_query_status(query_id)
                if status.status != AsyncQueryPhase.EXECUTING:
                    return None
            except BackendApiError:
                pass
            await report_exception(e, self._slack_client)
            logger.exception("Failed to cancel query", error=str(e))
            return None

        # Return an appropriate status update for the job's current status.
        return await self._results.build_query_status(query)

    async def handle_cancel(self, message: JobCancel) -> None:
        """Handle an incoming cancel request.

        Cancel the running query if necessary and publish any status update.

        Parameters
        ----------
        message
            Request to cancel the query.
        """
        status = await self.cancel_query(message)
        if status:
            await self._results.publish_status(status)

    async def handle_query(
        self, job: JobRun, kafka_start: datetime | None
    ) -> None:
        """Handle an incoming request to run a query.

        Start the query if possible and publish the status of the query as a
        Kafka message. Jobs with table uploads are dispatched to an arq
        worker instead, since an upload can take a long time and would
        otherwise block consumption of the job-run topic.

        Parameters
        ----------
        job
            Query job to start.
        kafka_start
            Time at which the Kafka message for the job was queued.
        """
        if job.upload_tables:
            await self._arq_queue.enqueue(
                "handle_upload_job",
                job,
                kafka_start,
                _queue_name=config.upload_queue,
            )
            return
        status = await self.start_query(job, kafka_start)
        await self._results.publish_status(status)

    async def start_query(
        self, job: JobRun, kafka_start: datetime | None = None
    ) -> JobStatus:
        """Start a new query and return its initial status.

        Parameters
        ----------
        job
            Query job to start.
        kafka_start
            Time at which the Kafka message for the job was queued.

        Returns
        -------
        JobStatus
            Initial status of the job.
        """
        result_type = job.result_format.format.type
        serialization = job.result_format.format.serialization
        if result_type == JobResultType.VOTable:
            if not serialization:
                msg = "VOTable format requires serialization"
                return self._build_invalid_request_status(job, msg)
            if serialization != JobResultSerialization.BINARY2:
                msg = f"{serialization} serialization not supported"
                return self._build_invalid_request_status(job, msg)
        for column in job.result_format.column_types:
            if not column.is_string() and column.arraysize is not None:
                m = "arraysize only supported for char and unicodeChar fields"
                return self._build_invalid_request_status(job, m)

        # Increment the user's running queries and make sure they have space
        # to start a new query.
        quota = await self._gafaelfawr.get_user_quota(job.owner)
        count = await self._rate_store.start_query(job.owner)
        if quota and quota.concurrent < count:
            await self._rate_store.end_query(job.owner)
            return self._build_quota_status(job, count - 1, quota.concurrent)

        # Create the query and set up the logger for it.
        query = Query(queued=kafka_start, job=job, start=datetime.now(tz=UTC))
        logger = self._logger.bind(
            quota=quota.to_logging_context() if quota else None,
            running=count,
            **query.to_logging_context(),
        )

        # Start the query.
        try:
            started_query = await self._start_query_internal(query, logger)
        except (BackendApiError, TableUploadWebError) as e:
            await self._rate_store.end_query(job.owner)
            return JobStatus.from_error(query.job, e.to_job_error())
        except Exception:
            await self._rate_store.end_query(job.owner)
            raise

        # Update the query status and return the status message to send.
        return await self._results.build_query_status(
            started_query, initial=True
        )

    def _build_invalid_request_status(
        self, job: JobRun, message: str
    ) -> JobStatus:
        """Build a status reply for an invalid request.

        Parameters
        ----------
        job
            Initial query request.
        message
            Error message.

        Returns
        -------
        JobStatus
            Job status to report to Kafka.
        """
        self._logger.warning(message, **job.to_logging_context())
        error = JobError(code=JobErrorCode.invalid_request, message=message)
        return JobStatus.from_error(job, error)

    def _build_quota_status(
        self, job: JobRun, running: int, quota: int
    ) -> JobStatus:
        """Build a status reply for an over-quota request.

        Parameters
        ----------
        job
            Initial query request.
        running
            Number of running queries.
        quota
            Maximum allowed number of concurrent running queries.

        Returns
        -------
        JobStatus
            Job status to report to Kafka.
        """
        self._logger.info(
            "Query rejected due to quota",
            quota={"concurrent": quota},
            running=running,
            **job.to_logging_context(),
        )
        error = JobError.for_quota_exceeded(running, quota)
        return JobStatus.from_error(job, error)

    async def _start_query_internal(
        self, query: Query, logger: BoundLogger
    ) -> StartedQuery:
        """Start a query by dispatching it to the backend.

        Parameters
        ----------
        query
            Query to start.
        logger
            Logger to use for any messages.

        Returns
        -------
        StartedQuery
            Started query with backend job ID.
        """
        # Upload any tables.
        uploaded = set()
        try:
            for upload in query.job.upload_tables:
                stats = await self._backend.upload_table(upload)
                uploaded.add(upload.database)
                logger.info("Uploaded table", table_name=upload.table_name)
                event = TemporaryTableUploadEvent(
                    job_id=query.job.job_id,
                    username=query.job.owner,
                    size=stats.size,
                    elapsed=stats.elapsed,
                )
                await self._events.temporary_table.publish(event)
        except (BackendApiError, TableUploadWebError) as e:
            await self._rate_store.end_query(query.job.owner)
            if isinstance(e, TableUploadWebError):
                msg = "Unable to retrieve table to upload"
            else:
                msg = "Unable to upload table"
            await report_exception(e, self._slack_client)
            logger.exception(msg, error=str(e))
            await self._results.delete_uploaded_databases(query.job, uploaded)
            raise

        # Start the query.
        query_id = None
        try:
            query_id = await self._backend.submit_query(query.job)
        except BackendApiError as e:
            await self._rate_store.end_query(query.job.owner)
            if isinstance(e, BackendApiFailedError):
                logger = logger.bind(**e.to_logging_context())
                logger.info("Query rejected by backend")
            else:
                await report_exception(e, self._slack_client)
                logger.exception("Unable to start query", error=str(e))
            await self._results.delete_uploaded_databases(query.job)
            raise

        # Return the corresponding StartedQuery object.
        logger.info("Started query", backend_id=query_id)
        return StartedQuery.from_query(query, query_id)
