"""State tracking for queries in progress."""

from __future__ import annotations

from structlog.stdlib import BoundLogger

from ..models.kafka import JobRun
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

    def add_query(self, query_id: int, job: JobRun) -> None:
        """Add a record for a newly-created query.

        Parameters
        ----------
        query_id
            Qserv query ID.
        job
            Original job request.
        """
        if query_id in self._queries:
            msg = "Duplicate query ID, replacing old query record"
            self._logger.error(msg, query_id=query_id)
        self._queries[query_id] = Query(query_id=query_id, job=job)

    def delete_query(self, query_id: int) -> None:
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

    def get_active_queries(self) -> set[int]:
        """Get the IDs of all active queries.

        Returns
        -------
        set of int
            All queries we believe are currently active.
        """
        return set(self._queries.keys())

    def get_query(self, query_id: int) -> Query | None:
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
