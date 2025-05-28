"""Service to create new queries."""

from __future__ import annotations

from datetime import UTC, datetime

from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ..events import Events
from ..exceptions import QservApiError, TableUploadWebError
from ..models.kafka import (
    JobCancel,
    JobError,
    JobErrorCode,
    JobResultSerialization,
    JobRun,
    JobStatus,
)
from ..models.votable import VOTablePrimitive
from ..storage.qserv import QservClient
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
        result_processor: ResultProcessor,
        events: Events,
        logger: BoundLogger,
    ) -> None:
        self._qserv = qserv_client
        self._state = state_store
        self._results = result_processor
        self._events = events
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
        except Exception:
            logger.exception("Invalid exectionID in cancel message")
            return None
        logger = logger.bind(qserv_id=str(query_id))
        query = await self._state.get_query(query_id)
        if not query:
            logger.warning("Cannot cancel unknown or completed job")
            return None

        # Cancel the query. There's not much we can do with exceptions other
        # than log them, since we don't have a way of returning a cancelation
        # error to the TAP server, so we send a status update matching the
        # last known status in that case.
        try:
            await self._qserv.cancel_query(query_id)
        except QservApiError as e:
            logger.exception("Failed to cancel query", error=str(e))
            return None

        # Return an appropriate status update for the job's current status.
        return await self._results.build_query_status(
            query_id,
            query.job,
            query.start or datetime.now(tz=UTC),
        )

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
        logger = self._logger.bind(
            job_id=job.job_id, username=job.owner, query=query_for_logging
        )
        start = datetime.now(tz=UTC)

        # Check that the job request is supported.
        serialization = job.result_format.format.serialization
        if serialization != JobResultSerialization.BINARY2:
            msg = f"{serialization} serialization not supported"
            return self._build_invalid_request_status(job, msg)
        for column in job.result_format.column_types:
            is_char = column.datatype == VOTablePrimitive.char
            if not is_char and column.arraysize is not None:
                msg = "arraysize only supported for char fields"
                return self._build_invalid_request_status(job, msg)

        # Upload any tables.
        try:
            for upload in job.upload_tables:
                await self._qserv.upload_table(upload)
                logger.info("Uploaded table", table_name=upload.table_name)
        except (QservApiError, TableUploadWebError) as e:
            if isinstance(e, TableUploadWebError):
                msg = "Unable to retrieve table to upload"
            else:
                msg = "Unable to upload table"
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
            logger.exception("Unable to start query", error=str(e))
            return JobStatus(
                job_id=job.job_id,
                execution_id=str(query_id) if query_id else None,
                timestamp=datetime.now(tz=UTC),
                status=ExecutionPhase.ERROR,
                error=e.to_job_error(),
                metadata=metadata,
            )
        logger.info("Started query", qserv_id=str(query_id))

        # Analyze the initial status and return it.
        return await self._results.build_query_status(
            query_id, job, start, initial=True
        )

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
