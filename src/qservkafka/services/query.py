"""Service to create new queries."""

from __future__ import annotations

from datetime import UTC, datetime

from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ..exceptions import QservApiError, UploadWebError
from ..models.kafka import (
    JobError,
    JobErrorCode,
    JobQueryInfo,
    JobResultInfo,
    JobResultSerialization,
    JobRun,
    JobStatus,
)
from ..models.qserv import AsyncQueryPhase, AsyncQueryStatus
from ..storage.qserv import QservClient
from ..storage.state import QueryStateStore
from ..storage.votable import VOTableWriter

__all__ = ["QueryService"]


class QueryService:
    """Start new queries.

    Parameters
    ----------
    qserv_client
        Client to talk to the Qserv REST API.
    state_store
        Storage for query state.
    votable_writer
        Writer for VOTable output.
    logger
        Logger to use.
    """

    def __init__(
        self,
        *,
        qserv_client: QservClient,
        state_store: QueryStateStore,
        votable_writer: VOTableWriter,
        logger: BoundLogger,
    ) -> None:
        self._qserv = qserv_client
        self._state = state_store
        self._votable = votable_writer
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

        # Check that the job request is supported.
        serialization = job.result_format.format.serialization
        if serialization != JobResultSerialization.BINARY2:
            return JobStatus(
                job_id=job.job_id,
                timestamp=datetime.now(tz=UTC),
                status=ExecutionPhase.ERROR,
                error=JobError(
                    code=JobErrorCode.invalid_request,
                    message=f"{serialization} serialization not supported",
                ),
                metadata=metadata,
            )

        # Start the query.
        self._logger.info(
            "Starting query",
            query=metadata.model_dump(mode="json", exclude_none=True),
        )
        query_id = None
        try:
            query_id = await self._qserv.submit_query(job)
            status = await self._qserv.get_query_status(query_id)
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
        return await self._build_initial_status(query_id, job, status)

    async def _build_initial_status(
        self, query_id: int, job: JobRun, status: AsyncQueryStatus
    ) -> JobStatus:
        """Build the initial status response for a just-created query.

        If the query has already completed, retrieve the results if successful
        and return an appropriate final status. Otherwise, return a status
        message indicating that the job has started executing.

        Parameters
        ----------
        query_id
            Qserv query ID.
        job
            Initial query request.
        status
            Initial status response from Qserv.

        Returns
        -------
        JobStatus
            Initial job status to report to Kafka.
        """
        metadata = job.to_job_metadata()
        error = None
        result_info = None

        # Check the status of the job according to Qserv.
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
        elif status.status == AsyncQueryPhase.COMPLETED:
            results = self._qserv.get_query_results_gen(query_id)
            try:
                total_rows = await self._votable.store(
                    job.result_url, job.result_format, results
                )
            except UploadWebError as e:
                msg = "Unable to upload results"
                self._logger.exception(msg, error=str(e))
                return JobStatus(
                    job_id=job.job_id,
                    execution_id=str(query_id),
                    timestamp=datetime.now(tz=UTC),
                    status=ExecutionPhase.ERROR,
                    error=e.to_job_error(),
                    metadata=job.to_job_metadata(),
                )
            result_info = JobResultInfo(
                total_rows=total_rows,
                result_location=job.result_location,
                format=job.result_format.format,
            )

        # Return the job status message for Kafka.
        return JobStatus(
            job_id=job.job_id,
            execution_id=str(query_id),
            timestamp=status.last_update or datetime.now(tz=UTC),
            status=status.status.to_execution_phase(),
            query_info=JobQueryInfo.from_query_status(status),
            result_info=result_info,
            error=error,
            metadata=metadata,
        )
