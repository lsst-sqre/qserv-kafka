"""Service to monitor the status of running queries."""

from __future__ import annotations

from aiojobs import Scheduler
from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ..storage.qserv import QservClient
from ..storage.state import QueryStateStore
from .results import ResultProcessor

__all__ = ["QueryMonitor"]


class QueryMonitor:
    """Service to monitor queries and send Kafka messages for updates.

    Parameters
    ----------
    result_processor
        Service used to process results.
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
        result_processor: ResultProcessor,
        qserv_client: QservClient,
        state_store: QueryStateStore,
        logger: BoundLogger,
    ) -> None:
        self._results = result_processor
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
                if query.status == status:
                    continue
                update = await self._results.build_query_status(query_id, job)
                await self._results.publish_status(update)
                await self._state.store_query(query_id, job, status)
            elif query_id not in self._in_progress:
                self._in_progress.add(query_id)
                coro = self.handle_finished_query(query_id)
                await scheduler.spawn(coro)

    async def handle_finished_query(self, query_id: int) -> None:
        """Process results and send event for a completed query.

        Parameters
        ----------
        query_id
           Qserv ID of completed query.
        """
        query = await self._state.get_query(query_id)
        if not query:
            return

        # Analyze the status and retrieve and upload the results if needed.
        update = await self._results.build_query_status(query_id, query.job)

        # Publish the status update.
        await self._results.publish_status(update)

        # Clean up the handled query if it is no longer executing.
        if update.status != ExecutionPhase.EXECUTING:
            await self._state.delete_query(query_id)
        self._in_progress.remove(query_id)
