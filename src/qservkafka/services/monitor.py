"""Service to monitor the status of running queries."""

from __future__ import annotations

from datetime import UTC, datetime

from aiojobs import Scheduler
from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ..exceptions import QservApiError
from ..models.kafka import JobRun, JobStatus
from ..storage.qserv import QservClient
from ..storage.state import QueryStateStore
from .query import QueryService

__all__ = ["QueryMonitor"]


class QueryMonitor:
    """Service to monitor queries and send Kafka messages for updates.

    Parameters
    ----------
    query_service
        Query service used to process queries.
    qserv_client
        Client to talk to the Qserv REST API.
    state_store
        Storage for query state.
    logger
        Logger to use.
    """

    def __init__(
        self,
        *,
        query_service: QueryService,
        qserv_client: QservClient,
        state_store: QueryStateStore,
        logger: BoundLogger,
    ) -> None:
        self._query = query_service
        self._qserv = qserv_client
        self._state = state_store
        self._logger = logger

        # Completed jobs that are currently being processed, so that we don't
        # process them twice.
        self._in_progress: set[int] = set()

    async def check_status(self, scheduler: Scheduler) -> None:
        """Check the status of running queries and report updates to Kafka.

        Parameters
        ----------
        scheduler
            Job scheduler to handle background tasks that process completed
            queries. This allows multiple completed queries to be processed
            simultaneously using the MySQL client connection pool.
        """
        active_queries = await self._state.get_active_queries()
        queries_to_process = active_queries - self._in_progress
        if not queries_to_process:
            return
        running = await self._qserv.list_running_queries()
        for query_id in queries_to_process:
            query = await self._state.get_query(query_id)
            if not query:
                continue
            job = query.job
            if query_id in running:
                status = running[query_id]
                if query.status != status:
                    update = await self._query.build_status(job, status)
                    await self._query.publish_status(update)
                    await self._state.update_status(query_id, job, status)
            elif query_id not in self._in_progress:
                self._in_progress.add(query_id)
                coro = self._handle_finished_query(query_id, job)
                await scheduler.spawn(coro)

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
            await self._query.publish_status(update)
            await self._state.delete_query(query_id)
            self._in_progress.remove(query_id)
            return

        # Analyze the status and retrieve and upload the results if needed.
        update = await self._query.build_status(job, status)

        # Publish the results.
        await self._query.publish_status(update)

        # Clean up the handled query if it is no longer executing.
        if update.status != ExecutionPhase.EXECUTING:
            await self._state.delete_query(query_id)
        self._in_progress.remove(query_id)
