"""State tracking for queries in progress."""

from __future__ import annotations

from safir.redis import PydanticRedisStorage
from structlog.stdlib import BoundLogger

from ..models.kafka import JobRun
from ..models.qserv import AsyncQueryStatus
from ..models.state import Query

__all__ = ["QueryStateStore"]


class QueryStateStore:
    """Tracks Qserv queries in progress.

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
        self, storage: PydanticRedisStorage[Query], logger: BoundLogger
    ) -> None:
        self._storage = storage
        self._logger = logger

    async def add_query(
        self, query_id: int, job: JobRun, status: AsyncQueryStatus
    ) -> None:
        """Add a record for a newly-created query.

        Parameters
        ----------
        query_id
            Qserv query ID.
        job
            Original job request.
        status
            Initial query status.
        """
        query = await self._storage.get(str(query_id))
        if query:
            msg = "Duplicate query ID, replacing old query record"
            self._logger.error(msg, query_id=query_id)
        query = Query(query_id=query_id, job=job, status=status)
        await self._storage.store(str(query_id), query)

    async def delete_query(self, query_id: int) -> None:
        """Delete a query from storage.

        Should be called after the query results have been stored (if
        applicable) and just before sending the Kafka message giving the
        result.

        Parameters
        ----------
        query_id
            Qserv query ID.
        """
        await self._storage.delete(str(query_id))

    async def get_active_queries(self) -> list[int]:
        """Get the IDs of all active queries.

        Returns
        -------
        list of int
            All queries we believe are currently active.
        """
        return [int(k) async for k in self._storage.scan("*")]

    async def get_query(self, query_id: int) -> Query | None:
        """Get the original job request for a given query.

        Parameters
        ----------
        query_id
            Qserv query ID.

        Returns
        -------
        job or None
            Original job request, or `None` if no such job was found.
        """
        return await self._storage.get(str(query_id))

    async def update_status(
        self, query_id: int, job: JobRun, status: AsyncQueryStatus
    ) -> None:
        """Add a record for a newly-created query.

        Parameters
        ----------
        query_id
            Qserv query ID.
        job
            Original job request.
        status
            Initial query status.
        """
        query = await self.get_query(query_id)
        if query:
            query.status = status
        else:
            # This case shouldn't happen but we may as well handle it.
            query = Query(query_id=query_id, job=job, status=status)
        await self._storage.store(str(query_id), query)
