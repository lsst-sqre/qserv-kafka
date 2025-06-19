"""Service to monitor the status of running queries."""

from __future__ import annotations

from safir.arq import ArqQueue
from structlog.stdlib import BoundLogger

from ..events import Events
from ..models.kafka import JobStatus
from ..models.qserv import AsyncQueryPhase, AsyncQueryStatus
from ..models.state import Query
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
        if not active_queries:
            return
        running = await self._qserv.list_running_queries()
        for query_id in active_queries:
            query = await self._state.get_query(query_id)
            if not query:
                continue
            update = await self.check_query(query, running.get(query_id))
            if update:
                await self._results.publish_status(update)

    async def check_query(
        self, query: Query, status: AsyncQueryStatus | None
    ) -> JobStatus | None:
        """Check the status of one Qserv query.

        If the query is complete, dispatch it to the result worker. Otherwise,
        construct a status update for posting to Kafka, if warranted.

        Parameters
        ----------
        query
            Running query information.
        status
            Qserv status from the running process list, if the query appeared
            there, or `None` otherwise.

        Returns
        -------
        JobStatus or None
            The status of the query, for posting to Kafka, or `None` if no
            update is warranted.
        """
        query_id = query.query_id
        job = query.job
        logger = self._logger.bind(
            job_id=job.job_id, qserv_id=str(query_id), username=job.owner
        )
        if query.result_queued:
            logger.debug("Skipping already queued query")
            return None

        # Send updates to executing queries directly from the background
        # monitoring task for faster updates, but make sure we do not
        # accidentally process finished queries from the monitor task. We
        # otherwise might send duplicate updates. Ensure all finished queries
        # are dispatched to arq.
        #
        # Do not use build_query_status inside the first block, since it will
        # re-retrieve the status from Qserv and may at that point find that it
        # is completed.
        if status and status.status == AsyncQueryPhase.EXECUTING:
            if query.status == status:
                logger.debug("Running query has not changed state")
                return None
            update = self._results.build_executing_status(job, status)
            await self._state.update_status(query_id, status)
            return update
        else:
            await self._arq.enqueue("handle_finished_query", query_id)
            await self._state.mark_queued_query(query_id)
            self._logger.debug("Dispatched finished query to worker")
            return None
