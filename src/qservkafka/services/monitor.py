"""Service to monitor the status of running queries."""

from __future__ import annotations

from safir.arq import ArqQueue
from structlog.stdlib import BoundLogger

from ..events import Events
from ..models.qserv import AsyncQueryPhase
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
    arq_queue
        Queue to which to dispatch result processing requests.
    state_store
        Storage for query state.
    events
        Metrics events publishers.
    logger
        Logger to use.
    """

    def __init__(
        self,
        *,
        result_processor: ResultProcessor,
        qserv_client: QservClient,
        arq_queue: ArqQueue,
        state_store: QueryStateStore,
        events: Events,
        logger: BoundLogger,
    ) -> None:
        self._results = result_processor
        self._qserv = qserv_client
        self._arq = arq_queue
        self._state = state_store
        self._events = events
        self._logger = logger

    async def check_status(self) -> None:
        """Check status of running queries and report updates to Kafka."""
        active_queries = await self._state.get_active_queries()
        queries_to_process = active_queries
        if not queries_to_process:
            return
        running = await self._qserv.list_running_queries()
        for query_id in queries_to_process:
            query = await self._state.get_query(query_id)
            if not query or query.result_queued:
                continue
            job = query.job

            # Send updates to executing queries directly from the background
            # monitoring task for faster updates, but make sure we do not
            # accidentally process finished queries from the monitor task. We
            # otherwise might send duplicate updates. Ensure all finished
            # queries are dispatched to arq.
            #
            # Do not use build_query_status inside the first block, since it
            # will re-retrieve the status from Qserv and may at that point
            # find that it is completed.
            is_running = (
                query_id in running
                and running[query_id].status == AsyncQueryPhase.EXECUTING
            )
            if is_running:
                status = running[query_id]
                if query.status == status:
                    continue
                update = self._results.build_executing_status(job, status)
                await self._results.publish_status(update)
                await self._state.update_status(query_id, status)
            else:
                await self._arq.enqueue("handle_finished_query", query_id)
                await self._state.mark_queued_query(query_id)
                self._logger.debug(
                    "Dispatched finished query to worker",
                    job_id=job.job_id,
                    qserv_id=query_id,
                    username=job.owner,
                )
