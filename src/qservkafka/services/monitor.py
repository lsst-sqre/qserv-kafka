"""Service to monitor the status of running queries."""

from __future__ import annotations

from datetime import UTC, datetime

from faststream.kafka import KafkaBroker
from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ..config import config
from ..exceptions import QservApiError, UploadWebError
from ..models.kafka import (
    JobError,
    JobErrorCode,
    JobQueryInfo,
    JobResultInfo,
    JobRun,
    JobStatus,
)
from ..models.qserv import AsyncQueryPhase, AsyncQueryStatus
from ..storage.qserv import QservClient
from ..storage.state import QueryStateStore
from ..storage.votable import VOTableWriter

__all__ = ["QueryMonitor"]


class QueryMonitor:
    """Service to monitor queries and send Kafka messages for updates.

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

    async def check_status(self) -> None:
        """Check the status of running queries and report updates to Kafka."""
        known_queries = await self._state.get_active_queries()
        if not known_queries:
            return
        running = await self._qserv.list_running_queries()
        for query_id in known_queries:
            query = await self._state.get_query(query_id)
            if not query:
                continue
            job = query.job
            if query_id in running:
                status = running[query_id]
                if query.status != status:
                    await self._send_status(job, status)
                    await self._state.update_status(query_id, job, status)
            else:
                await self._handle_finished_query(query_id, job)

    async def _handle_finished_query(self, query_id: int, job: JobRun) -> None:
        """Sent event for a completed query.

        Parameters
        ----------
        query_id
           Qserv ID of completed query.
        """
        logger = self._logger.bind(
            job_id=job.job_id, qserv_id=query_id, username=job.owner
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
            await self._publish_status(update)
            await self._state.delete_query(query_id)
            return

        match status.status:
            case AsyncQueryPhase.EXECUTING:
                logger.error(
                    "Job executing but not in process list",
                    job_id=job.job_id,
                    username=job.owner,
                    status=status.model_dump(mode="json"),
                )
                # Do nothing and hope that the job either finishes or shows up
                # in the process list the next time through.
            case AsyncQueryPhase.COMPLETED:
                await self._send_completed(query_id, job, status)
                await self._state.delete_query(query_id)
            case AsyncQueryPhase.FAILED:
                await self._send_failed(job, status)
                await self._state.delete_query(query_id)
            case AsyncQueryPhase.ABORTED:
                await self._send_aborted(job, status)
                await self._state.delete_query(query_id)
            case _:  # pragma: no cover
                raise ValueError(f"Unknown phase {status.status}")

    async def _publish_status(self, status: JobStatus) -> None:
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

    async def _send_aborted(
        self, job: JobRun, status: AsyncQueryStatus
    ) -> None:
        """Send a status update for an aborted job.

        Parameters
        ----------
        job
            Original query request.
        status
            Status of the job.
        """
        self._logger.info(
            "Job aborted",
            job_id=job.job_id,
            qserv_id=status.query_id,
            username=job.owner,
        )
        update = JobStatus(
            job_id=job.job_id,
            execution_id=str(status.query_id),
            timestamp=status.last_update or datetime.now(tz=UTC),
            status=ExecutionPhase.ABORTED,
            query_info=JobQueryInfo.from_query_status(status),
            metadata=job.to_job_metadata(),
        )
        await self._publish_status(update)

    async def _send_completed(
        self, query_id: int, job: JobRun, status: AsyncQueryStatus
    ) -> None:
        """Send a status update for a completed job.

        Parameters
        ----------
        query_id
            Qserv query ID.
        job
            Original query request.
        status
            Status of the job.
        """
        logger = self._logger.bind(
            job_id=job.job_id, qserv_id=query_id, username=job.owner
        )
        logger.debug("Processing job completion")

        # Retrieve and upload the results.
        results = self._qserv.get_query_results_gen(query_id)
        try:
            total_rows = await self._votable.store(
                job.result_url, job.result_format, results
            )
        except UploadWebError as e:
            self._logger.exception("Unable to upload results", error=str(e))
            update = JobStatus(
                job_id=job.job_id,
                execution_id=str(query_id),
                timestamp=datetime.now(tz=UTC),
                status=ExecutionPhase.ERROR,
                error=e.to_job_error(),
                metadata=job.to_job_metadata(),
            )
            await self._publish_status(update)
            return
        logger.info("Job complete and results uploaded")

        # Send the Kafka message indicating job completion.
        update = JobStatus(
            job_id=job.job_id,
            execution_id=str(status.query_id),
            timestamp=status.last_update or datetime.now(tz=UTC),
            status=ExecutionPhase.COMPLETED,
            query_info=JobQueryInfo.from_query_status(status),
            result_info=JobResultInfo(
                total_rows=total_rows,
                result_location=job.result_location,
                format=job.result_format.format,
            ),
            metadata=job.to_job_metadata(),
        )
        await self._publish_status(update)

    async def _send_failed(
        self, job: JobRun, status: AsyncQueryStatus
    ) -> None:
        """Send a status update for a failed job.

        Currently, Qserv has no way of reporting an error, so we have to
        synthesize an error.

        Parameters
        ----------
        job
            Original query request.
        status
            Status of the job.
        """
        metadata = job.to_job_metadata()
        self._logger.warning(
            "Backend reported query failure",
            job_id=job.job_id,
            query=metadata.model_dump(mode="json", exclude_none=True),
            status=status.model_dump(mode="json", exclude_none=True),
        )
        update = JobStatus(
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
        await self._publish_status(update)

    async def _send_status(
        self, job: JobRun, status: AsyncQueryStatus
    ) -> None:
        """Send a status update for a job that's still executing.

        Parameters
        ----------
        job
            Original query request.
        status
            Status of the job.
        """
        self._logger.debug(
            "Sending job status update",
            job_id=job.job_id,
            qserv_id=status.query_id,
            username=job.owner,
        )
        update = JobStatus(
            job_id=job.job_id,
            execution_id=str(status.query_id),
            timestamp=status.last_update or datetime.now(tz=UTC),
            status=ExecutionPhase.EXECUTING,
            query_info=JobQueryInfo.from_query_status(status),
            metadata=job.to_job_metadata(),
        )
        await self._publish_status(update)
