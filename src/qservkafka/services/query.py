"""Service to create new queries."""

from __future__ import annotations

from datetime import UTC, datetime

from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ..exceptions import QservApiError
from ..models.kafka import (
    JobError,
    JobErrorCode,
    JobQueryInfo,
    JobRun,
    JobStatus,
)
from ..models.qserv import AsyncQueryPhase
from ..storage.qserv import QservClient
from ..storage.state import QueryStateStore

__all__ = ["QueryService"]


class QueryService:
    """Start new queries.

    Parameters
    ----------
    qserv_client
        Client to talk to the Qserv REST API.
    state_store
        Storage for query state.
    logger
        Logger to use.
    """

    def __init__(
        self,
        qserv_client: QservClient,
        state_store: QueryStateStore,
        logger: BoundLogger,
    ) -> None:
        self._rest = qserv_client
        self._state = state_store
        self._logger = logger

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
        query_id = None

        # Start the query.
        self._logger.info(
            "Starting query",
            query=metadata.model_dump(mode="json", exclude_none=True),
        )
        try:
            query_id = await self._rest.submit_query(job)
            status = await self._rest.get_query_status(query_id)
        except QservApiError as e:
            self._logger.exception("Unable to start job", error=str(e))
            return JobStatus(
                job_id=job.job_id,
                execution_id=str(query_id) if query_id else None,
                timestamp=datetime.now(tz=UTC),
                status=ExecutionPhase.ERROR,
                error=e.to_job_error(),
                metadata=metadata,
            )

        # Analyze the initial status and store the query if it successfully
        # went into executing.
        error = None
        if status.status == AsyncQueryPhase.FAILED:
            self._logger.warning(
                "Backend reported query failure",
                query=metadata.model_dump(mode="json", exclude_none=True),
            )
            error = JobError(
                code=JobErrorCode.backend_error,
                message="Query failed in backend",
            )
        elif status.status == AsyncQueryPhase.EXECUTING:
            await self._state.add_query(query_id, job, status)

        # Return the status message to send to Kafka.
        return JobStatus(
            job_id=job.job_id,
            execution_id=str(query_id),
            timestamp=status.last_update or datetime.now(tz=UTC),
            status=status.status.to_execution_phase(),
            query_info=JobQueryInfo.from_query_status(status),
            error=error,
            metadata=metadata,
        )
