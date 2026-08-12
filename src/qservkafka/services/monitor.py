"""Service to monitor the status of running queries."""

import asyncio
from collections import Counter

from safir.arq import ArqQueue
from structlog.stdlib import BoundLogger

from ..config import config
from ..events import BigQueryExecutingEvent, Events, QservExecutingEvent
from ..exceptions import QueryError
from ..models.query import AsyncQueryPhase, ProcessStatus
from ..models.state import RunningQuery
from ..storage.backend import BackendType, DatabaseBackend
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

        # Build a list of query check tasks to run.
        queries_executing = 0
        tasks = []
        for query_id in active_queries:
            query = await self._state.get_query(query_id)
            if not query:
                continue
            status = running.get(query_id)
            if status and status.status == AsyncQueryPhase.EXECUTING:
                queries_executing += 1
            tasks.append(self.check_query(query, status))

        # Check the status of all the currently running queries in parallel.
        await asyncio.gather(*tasks)

        # Post a metric for how many queries are in flight. This counts only
        # queries that are both still executing and that this bridge instance
        # is aware of, since the same backend may be shared by multiple
        # bridges and each should only count its own queries.
        match config.backend:
            case BackendType.BIGQUERY:
                bq_event = BigQueryExecutingEvent(count=queries_executing)
                await self._events.bigquery_executing.publish(bq_event)
            case BackendType.QSERV:
                qs_event = QservExecutingEvent(count=queries_executing)
                await self._events.qserv_executing.publish(qs_event)

    async def check_query(
        self, query: RunningQuery, status: ProcessStatus | None
    ) -> None:
        """Check the status of one backend query.

        If the query is complete, dispatch it to the result worker. Otherwise,
        send a status update for posting to Kafka, if warranted.

        Parameters
        ----------
        query
            Running query information.
        status
            Backend status from the running process list, if the query
            appeared there, or `None` otherwise. If `None`, the job will be
            assumed to be complete.
        """
        logger = self._logger.bind(**query.to_logging_context())
        if query.result_queued:
            logger.debug("Skipping already queued query")
            return

        # Send updates to executing queries directly from the background
        # monitoring task for faster updates, but dispatch any completed
        # queries to a result worker.
        if status and status.status == AsyncQueryPhase.EXECUTING:
            if not query.status.is_different_than(status):
                logger.debug("Running query has not changed state")
                return
            query.status.update_from(status)
            await self._state.update_status(query.query_id, query.status)
            logger.debug("Sending status update for running query")
            await self._results.publish_status(query.to_job_status())
        else:
            try:
                query = await self._results.get_running_query(query)
            except QueryError as e:
                await self._results.handle_query_exception(query, e)
                return
            await self._state.store_query(query)
            if query.status.status == AsyncQueryPhase.EXECUTING:
                await self._results.publish_status(query.to_job_status())
                return
            await self._arq.enqueue("handle_finished_query", query.query_id)
            await self._state.mark_queued_query(query.query_id)
            logger.info("Dispatched finished query to worker")

    async def reconcile_rate_limits(self) -> None:
        """Reconcile rate limits against currently running queries."""
        active_queries = await self._state.get_active_queries()
        counts: Counter[str] = Counter()
        for backend_id in active_queries:
            query = await self._state.get_query(backend_id)
            if query:
                counts[query.job.owner] += 1
        await self._rate_store.reconcile_query_counts(counts)
