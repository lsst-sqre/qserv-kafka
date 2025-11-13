"""Service to create new queries."""

from __future__ import annotations

from datetime import UTC, datetime

from safir.sentry import report_exception
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ..events import Events, TemporaryTableUploadEvent
from ..exceptions import (
    QservApiError,
    QservApiFailedError,
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
from ..models.qserv import AsyncQueryPhase
from ..models.state import Query
from ..storage.gafaelfawr import GafaelfawrClient
from ..storage.qserv import QservClient
from ..storage.rate import RateLimitStore
from ..storage.state import QueryStateStore
from .results import ResultProcessor

__all__ = ["QueryService"]


class QueryService:
    """Start or cancel queries.

    Parameters
    ----------
    qserv_client
        Client to talk to the Qserv REST API.
    state_store
        Storage for query state.
    result_processor
        Service to process completed queries.
    rate_limit_store
        Storage for rate limiting.
    gafaelfawr_client
        Client for quota information.
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
        qserv_client: QservClient,
        state_store: QueryStateStore,
        result_processor: ResultProcessor,
        rate_limit_store: RateLimitStore,
        gafaelfawr_client: GafaelfawrClient,
        events: Events,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self._qserv = qserv_client
        self._state = state_store
        self._results = result_processor
        self._rate_store = rate_limit_store
        self._gafaelfawr = gafaelfawr_client
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
        try:
            query_id = int(message.execution_id)
        except Exception as e:
            await report_exception(e, self._slack_client)
            logger.exception("Invalid exectionID in cancel message")
            return None
        logger = logger.bind(qserv_id=str(query_id))
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
            await self._qserv.cancel_query(query_id)
        except QservApiError as e:
            try:
                status = await self._qserv.get_query_status(query_id)
                if status.status != AsyncQueryPhase.EXECUTING:
                    return None
            except QservApiError:
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

    async def handle_query(self, job: JobRun) -> None:
        """Handle an incoming request to run a query.

        Start the query if possible and publish the status of the query as a
        Kafka message.

        Parameters
        ----------
        job
            Query job to start.
        """
        status = await self.start_query(job)
        await self._results.publish_status(status)

    async def start_query(self, job: JobRun) -> JobStatus:
        """Start a new query and return its initial status.

        Parameters
        ----------
        job
            Query job to start.

        Returns
        -------
        JobStatus
            Initial status of the job.
        """
        metadata = job.to_job_metadata()
        query_for_logging = metadata.model_dump(mode="json", exclude_none=True)

        # Check that the job request is supported.
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

        # Set up the logger for the query.
        logger = self._logger.bind(
            job_id=job.job_id,
            username=job.owner,
            quota=quota,
            running=count,
            query=query_for_logging,
        )

        # Start the query.
        try:
            return await self._start_query_internal(job, logger)
        except Exception:
            await self._rate_store.end_query(job.owner)
            raise

    def _build_invalid_request_status(
        self, job: JobRun, error: str
    ) -> JobStatus:
        """Build a status reply for an invalid request.

        Parameters
        ----------
        job
            Initial query request.
        error
            Error message.

        Returns
        -------
        JobStatus
            Job status to report to Kafka.
        """
        metadata = job.to_job_metadata()
        self._logger.warning(
            error,
            job_id=job.job_id,
            username=job.owner,
            query=metadata.model_dump(mode="json", exclude_none=True),
        )
        return JobStatus(
            job_id=job.job_id,
            timestamp=datetime.now(tz=UTC),
            status=ExecutionPhase.ERROR,
            error=JobError(code=JobErrorCode.invalid_request, message=error),
            metadata=metadata,
        )

    def _build_quota_status(
        self, job: JobRun, count: int, quota: int
    ) -> JobStatus:
        """Build a status reply for an over-quota request.

        Parameters
        ----------
        job
            Initial query request.
        count
            Number of running queries.
        quota
            Maximum allowed number of concurrent running queries.

        Returns
        -------
        JobStatus
            Job status to report to Kafka.
        """
        metadata = job.to_job_metadata()
        self._logger.info(
            "Query rejected due to quota",
            job_id=job.job_id,
            username=job.owner,
            quota=quota,
            running=count,
            query=metadata.model_dump(mode="json", exclude_none=True),
        )
        error = (
            f"Maximum running queries reached ({count} running, maximum"
            f" {quota}); wait for a query to finish or cancel one of your"
            " running queries"
        )
        return JobStatus(
            job_id=job.job_id,
            timestamp=datetime.now(tz=UTC),
            status=ExecutionPhase.ERROR,
            error=JobError(code=JobErrorCode.quota_exceeded, message=error),
            metadata=metadata,
        )

    async def _start_query_internal(
        self, job: JobRun, logger: BoundLogger
    ) -> JobStatus:
        """Start a query after verification and rate limiting.

        Parameters
        ----------
        job
            Query job to start.
        logger
            Logger to use for any messages.

        Returns
        -------
        JobStatus
            Initial status of the job.
        """
        start = datetime.now(tz=UTC)
        metadata = job.to_job_metadata()

        # Upload any tables.
        try:
            for upload in job.upload_tables:
                stats = await self._qserv.upload_table(upload)
                logger.info("Uploaded table", table_name=upload.table_name)
                event = TemporaryTableUploadEvent(
                    job_id=job.job_id,
                    username=job.owner,
                    size=stats.size,
                    elapsed=stats.elapsed,
                )
                await self._events.temporary_table.publish(event)
        except (QservApiError, TableUploadWebError) as e:
            await self._rate_store.end_query(job.owner)
            if isinstance(e, TableUploadWebError):
                msg = "Unable to retrieve table to upload"
            else:
                msg = "Unable to upload table"
            await report_exception(e, self._slack_client)
            logger.exception(msg, error=str(e))
            return JobStatus(
                job_id=job.job_id,
                execution_id=None,
                timestamp=datetime.now(tz=UTC),
                status=ExecutionPhase.ERROR,
                error=e.to_job_error(),
                metadata=metadata,
            )

        # Start the query.
        query_id = None
        try:
            query_id = await self._qserv.submit_query(job)
        except QservApiError as e:
            await self._rate_store.end_query(job.owner)
            if isinstance(e, QservApiFailedError):
                msg = "Query rejected by Qserv"
                logger.info(msg, error=str(e), detail=e.detail)
            else:
                await report_exception(e, self._slack_client)
                logger.exception("Unable to start query", error=str(e))
            return JobStatus(
                job_id=job.job_id,
                execution_id=str(query_id) if query_id else None,
                timestamp=datetime.now(tz=UTC),
                status=ExecutionPhase.ERROR,
                error=e.to_job_error(),
                metadata=metadata,
            )
        created = datetime.now(tz=UTC)
        logger.info("Started query", qserv_id=str(query_id))

        # Analyze the initial status and return it.
        query = Query(query_id=query_id, job=job, start=start, created=created)
        return await self._results.build_query_status(query, initial=True)
