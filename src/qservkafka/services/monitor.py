"""Service to monitor the status of running queries."""

from collections import Counter

from safir.arq import ArqQueue
from structlog.stdlib import BoundLogger

from ..events import Events
from ..models.kafka import JobStatus
from ..models.query import AsyncQueryPhase, ProcessStatus
from ..models.state import RunningQuery
from ..storage.backend import DatabaseBackend
from ..storage.rate import RateLimitStore
from ..storage.state import QueryStateStore
from .results import ResultProcessor

__all__ = ["QueryMonitor"]


class QueryMonitor:
    """Service to monitor queries and send Kafka messages for updates.

    Parameters
    ----------
    result_processor
        Service used to process results.
    backend
        Database backend client (Qserv, BigQuery, etc.).
    arq_queue
        Queue to which to dispatch result processing requests.
    state_store
        Storage for query state.
    rate_limit_store
        Storage for rate limits.
    events
        Metrics events publishers.
    logger
        Logger to use.
    """

    def __init__(
        self,
        *,
        result_processor: ResultProcessor,
        backend: DatabaseBackend,
        arq_queue: ArqQueue,
        state_store: QueryStateStore,
        rate_limit_store: RateLimitStore,
        events: Events,
        logger: BoundLogger,
    ) -> None:
        self._results = result_processor
        self._backend = backend
        self._arq = arq_queue
        self._state = state_store
        self._rate_store = rate_limit_store
        self._events = events
        self._logger = logger

    async def check_status(self) -> None:
        """Check status of running queries and report updates to Kafka."""
        active_queries = await self._state.get_active_queries()
        if not active_queries:
            return
        running = await self._backend.list_running_queries()
        for query_id in active_queries:
            query = await self._state.get_query(query_id)
            if not query:
                continue
            update = await self.check_query(query, running.get(query_id))
            if update:
                await self._results.publish_status(update)

    async def check_query(
        self, query: RunningQuery, status: ProcessStatus | None
    ) -> JobStatus | None:
        """Check the status of one backend query.

        If the query is complete, dispatch it to the result worker. Otherwise,
        construct a status update for posting to Kafka, if warranted.

        Parameters
        ----------
        query
            Running query information.
        status
            Backend status from the running process list, if the query appeared
            there, or `None` otherwise.

        Returns
        -------
        JobStatus or None
            The status of the query, for posting to Kafka, or `None` if no
            update is warranted.
        """
        logger = self._logger.bind(**query.to_logging_context())
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
        # re-retrieve the status from the backend and may at that point find
        # that it is completed.
        if status and status.status == AsyncQueryPhase.EXECUTING:
            if not query.status.is_different_than(status):
                logger.debug("Running query has not changed state")
                return None
            query.status.update_from(status)
            update = self._results.build_executing_status(query)
            await self._state.update_status(query.query_id, query.status)
            logger = self._logger.bind(**query.to_logging_context())
            logger.debug("Sending status update for running query")
            return update
        else:
            await self._arq.enqueue("handle_finished_query", query.query_id)
            await self._state.mark_queued_query(query.query_id)
            logger.info("Dispatched finished query to worker")
            return None

    async def reconcile_rate_limits(self) -> None:
        """Reconcile rate limits against currently running queries."""
        active_queries = await self._state.get_active_queries()
        counts: Counter[str] = Counter()
        for backend_id in active_queries:
            query = await self._state.get_query(backend_id)
            if query:
                counts[query.job.owner] += 1
        await self._rate_store.reconcile_query_counts(counts)
