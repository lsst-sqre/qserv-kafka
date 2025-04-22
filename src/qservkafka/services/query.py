"""Service to create new queries."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from faststream.kafka import KafkaBroker
from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ..config import config
from ..exceptions import QservApiError, UploadWebError
from ..models.kafka import (
    JobCancel,
    JobError,
    JobErrorCode,
    JobQueryInfo,
    JobResultInfo,
    JobResultSerialization,
    JobRun,
    JobStatus,
)
from ..models.qserv import AsyncQueryPhase, AsyncQueryStatus
from ..models.votable import VOTablePrimitive
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
    kafka_broker
        Broker to use to publish status messages.
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
        logger: BoundLogger,
    ) -> None:
        self._qserv = qserv_client
        self._state = state_store
        self._votable = votable_writer
        self._kafka = kafka_broker
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
        logger = logger.bind(qserv_id=query_id)
        query = await self._state.get_query(query_id)
        if not query:
            logger.warning("Cannot cancel unknown job")
            return None

        # Cancel the query. There's not much we can do with exceptions other
        # than log them, since we don't have a way of returning a cancelation
        # error to the TAP server, so we send a status update matching the
        # last known status in that case.
        try:
            await self._qserv.cancel_query(query_id)
            status = await self._qserv.get_query_status(query_id)
        except QservApiError as e:
            logger.exception("Failed to cancel query", error=str(e))
            return None

        # Return an appropriate status update for the job's current status.
        return await self._build_status(query_id, query.job, status)

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
        logger = self._logger.bind(job_id=job.job_id, username=job.owner)
        metadata = job.to_job_metadata()

        # Check that the job request is supported.
        serialization = job.result_format.format.serialization
        if serialization != JobResultSerialization.BINARY2:
            msg = f"{serialization} serialization not supported"
            return self._build_invalid_request_error(job, msg)
        for column in job.result_format.column_types:
            is_char = column.datatype == VOTablePrimitive.char
            if not is_char and column.arraysize is not None:
                msg = "arraysize only supported for char fields"
                return self._build_invalid_request_error(job, msg)

        # Start the query.
        logger.info(
            "Starting query",
            query=metadata.model_dump(mode="json", exclude_none=True),
        )
        query_id = None
        try:
            query_id = await self._qserv.submit_query(job)
            status = await self._qserv.get_query_status(query_id)
        except QservApiError as e:
            logger.exception("Unable to start job", error=str(e))
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
        result = await self._build_status(query_id, job, status)
        if result.status == ExecutionPhase.EXECUTING:
            await self._state.add_query(query_id, job, status)
        return result

    async def _build_status(
        self, query_id: int, job: JobRun, status: AsyncQueryStatus
    ) -> JobStatus:
        """Build the status response for a query.

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
            Status response from Qserv.

        Returns
        -------
        JobStatus
            Job status to report to Kafka.
        """
        logger = self._logger.bind(
            job_id=job.job_id, qserv_id=query_id, username=job.owner
        )
        metadata = job.to_job_metadata()
        error = None
        result_info = None

        # Check the status of the job according to Qserv.
        if status.status == AsyncQueryPhase.FAILED:
            logger.warning(
                "Backend reported query failure",
                query=metadata.model_dump(mode="json", exclude_none=True),
            )
            error = JobError(
                code=JobErrorCode.backend_error,
                message="Query failed in backend",
            )
        elif status.status == AsyncQueryPhase.COMPLETED:
            results = self._qserv.get_query_results_gen(query_id)
            start = datetime.now(tz=UTC)
            timeout = config.shutdown_timeout.total_seconds()
            try:
                async with asyncio.timeout(timeout):
                    total_rows = await self._votable.store(
                        job.result_url, job.result_format, results
                    )
            except (QservApiError, UploadWebError) as e:
                if isinstance(e, UploadWebError):
                    msg = "Unable to upload results"
                else:
                    msg = "Unable to retrieve results"
                logger.exception(msg, error=str(e))
                return JobStatus(
                    job_id=job.job_id,
                    execution_id=str(query_id),
                    timestamp=datetime.now(tz=UTC),
                    status=ExecutionPhase.ERROR,
                    error=e.to_job_error(),
                    metadata=job.to_job_metadata(),
                )
            except TimeoutError:
                elapsed = datetime.now(tz=UTC) - start
                logger.exception(
                    "Retrieving results timed out",
                    elapsed=elapsed.total_seconds,
                    timeout=timeout,
                )
                status.status = AsyncQueryPhase.EXECUTING
                await self._state.add_query(query_id, job, status)
            else:
                result_info = JobResultInfo(
                    total_rows=total_rows,
                    result_location=job.result_location,
                    format=job.result_format.format,
                )
                logger.info("Job complete and results uploaded")

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

    def _build_invalid_request_error(
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
