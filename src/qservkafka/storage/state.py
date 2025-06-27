"""State tracking for queries in progress."""

from __future__ import annotations

from datetime import datetime

from safir.redis import PydanticRedisStorage
from structlog.stdlib import BoundLogger

from ..constants import MAXIMUM_QUERY_LIFETIME
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

    async def get_active_queries(self) -> set[int]:
        """Get the IDs of all active queries.

        Returns
        -------
        set of int
            All queries we believe are currently active.
        """
        return {int(k) async for k in self._storage.scan("*")}

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

    async def mark_queued_query(self, query_id: int) -> None:
        """Mark a query as queued.

        Parameters
        ----------
        query_id
            Qserv query ID.
        """
        query = await self.get_query(query_id)
        if query:
            query.result_queued = True
            lifetime = int(MAXIMUM_QUERY_LIFETIME.total_seconds())
            await self._storage.store(str(query_id), query, lifetime)

    async def store_query(
        self,
        query_id: int,
        job: JobRun,
        status: AsyncQueryStatus | None,
        *,
        start: datetime,
        result_queued: bool = False,
    ) -> None:
        """Add or update a record for an in-progress query.

        Parameters
        ----------
        query_id
            Qserv query ID.
        job
            Original job request.
        status
            Query status.
        start
            When the query was received.
        result_queued
            Whether this query has been dispatched to arq for result
            processing.
        """
        query = Query(
            query_id=query_id,
            job=job,
            status=status,
            start=start,
            result_queued=result_queued,
        )
        lifetime = int(MAXIMUM_QUERY_LIFETIME.total_seconds())
        await self._storage.store(
            str(query_id), query, lifetime, exclude_defaults=True
        )

    async def update_status(
        self, query_id: int, status: AsyncQueryStatus
    ) -> None:
        """Update the status of a query.

        Parameters
        ----------
        query_id
            Qserv query ID.
        status
            Qserv query status.
        """
        query = await self.get_query(query_id)
        if query:
            query.result_queued = False
            query.status = status
            lifetime = int(MAXIMUM_QUERY_LIFETIME.total_seconds())
            await self._storage.store(
                str(query_id), query, lifetime, exclude_defaults=True
            )
