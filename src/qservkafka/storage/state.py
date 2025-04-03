"""State tracking for queries in progress."""

from __future__ import annotations

from copy import copy

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
    """

    def __init__(self, logger: BoundLogger) -> None:
        self._logger = logger

        # Map from Qserv query ID to query metadata.
        self._queries: dict[int, Query] = {}

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
        if query_id in self._queries:
            msg = "Duplicate query ID, replacing old query record"
            self._logger.error(msg, query_id=query_id)
        self._queries[query_id] = Query(
            query_id=query_id, job=job, status=status
        )

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
        if query_id in self._queries:
            del self._queries[query_id]

    async def get_active_queries(self) -> dict[int, Query]:
        """Get the IDs of all active queries.

        Returns
        -------
        set of int
            All queries we believe are currently active.
        """
        return copy(self._queries)

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
        return self._queries.get(query_id)

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
        if query_id in self._queries:
            self._queries[query_id].status = status
        else:
            # This case shouldn't happen but we may as well handle it.
            self._queries[query_id] = Query(
                query_id=query_id, job=job, status=status
            )
