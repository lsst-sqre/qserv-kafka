"""State tracking for queries in progress."""

from safir.redis import PydanticRedisStorage
from structlog.stdlib import BoundLogger

from ..constants import MAXIMUM_QUERY_LIFETIME
from ..models.query import QueryStatus
from ..models.state import RunningQuery

__all__ = ["QueryStateStore"]


class QueryStateStore:
    """Tracks backend queries in progress.

    Currently, this uses a simple in-memory store that is wiped whenever the
    Qserv Kafka bridge is restarted. Eventually, it will use Redis so that the
    service can recover from restarts.

    Parameters
    ----------
    storage
        Underlying Redis storage for query state.
    logger
        Logger to use.
    """

    def __init__(
        self, storage: PydanticRedisStorage[RunningQuery], logger: BoundLogger
    ) -> None:
        self._storage = storage
        self._logger = logger

    async def delete_query(self, query_id: str) -> None:
        """Delete a query from storage.

        Should be called after the query results have been stored (if
        applicable) and just before sending the Kafka message giving the
        result.

        Parameters
        ----------
        query_id
            Backend query ID.
        """
        await self._storage.delete(query_id)

    async def get_active_queries(self) -> set[str]:
        """Get the IDs of all active queries.

        Returns
        -------
        set of str
            All queries we believe are currently active.
        """
        return {k async for k in self._storage.scan("*")}

    async def get_query(self, query_id: str) -> RunningQuery | None:
        """Get the original job request for a given query.

        Parameters
        ----------
        query_id
            Backend query ID.

        Returns
        -------
        job or None
            Original job request, or `None` if no such job was found.
        """
        return await self._storage.get(query_id)

    async def mark_queued_query(self, query_id: str) -> None:
        """Mark a query as queued.

        Parameters
        ----------
        query_id
            Backend query ID.
        """
        query = await self.get_query(query_id)
        if query:
            query.result_queued = True
            lifetime = int(MAXIMUM_QUERY_LIFETIME.total_seconds())
            await self._storage.store(
                query_id, query, lifetime, exclude_defaults=True
            )

    async def store_query(
        self, query: RunningQuery, *, result_queued: bool = False
    ) -> None:
        """Add or update a record for an in-progress query.

        Parameters
        ----------
        query
            Query to store.
        result_queued
            Whether this query has been dispatched to arq for result
            processing.
        """
        lifetime = int(MAXIMUM_QUERY_LIFETIME.total_seconds())
        await self._storage.store(
            query.query_id, query, lifetime, exclude_defaults=True
        )

    async def update_status(self, query_id: str, status: QueryStatus) -> None:
        """Update the status of a query.

        Parameters
        ----------
        query_id
            Backend query ID.
        status
            Backend query status.
        """
        query = await self.get_query(query_id)
        if query:
            query.result_queued = False
            query.status.update_from(status.to_process_status())
            lifetime = int(MAXIMUM_QUERY_LIFETIME.total_seconds())
            await self._storage.store(
                query_id, query, lifetime, exclude_defaults=True
            )
