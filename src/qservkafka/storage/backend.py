"""Abstract interface for database backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Sequence
from typing import Any

from ..events import QueryApiFailureEvent
from ..models.kafka import JobRun, JobTableUpload
from ..models.qserv import TableUploadStats
from ..models.query import ProcessStatus, QueryStatus

__all__ = ["DatabaseBackend"]


class DatabaseBackend(ABC):
    """Abstract interface for database query backends.

    This interface defines the operations that all database backends
    must implement to integrate with the bridge application.

    All backends must be able to:
    - Submit queries and track their execution
    - Monitor query status (and optionally progress)
    - Retrieve query results
    - Optionally delete results

    Query IDs are represented as strings.
    """

    @abstractmethod
    async def cancel_query(self, query_id: str) -> None:
        """Cancel a running query.

        Parameters
        ----------
        query_id
            ID of the query to cancel.

        Raises
        ------
        Exception
            Error if cancel fails.
        """

    @abstractmethod
    async def delete_database(self, database: str) -> None:
        """Delete a temporary database and all its contents.

        Parameters
        ----------
        database
            Name of the database to delete.

        Raises
        ------
        Exception
           Error if deletion fails.

        Notes
        -----
        Backends may implement this as a no-op.
        """

    @abstractmethod
    async def delete_result(self, query_id: str) -> None:
        """Delete the results of a query.

        Parameters
        ----------
        query_id
            ID of the query whose results should be deleted.

        Raises
        ------
        Exception
            Error if delete fails.

        Notes
        -----
        Backends that automatically expire results (like BigQuery)
        may implement this as a no-op.
        """

    @abstractmethod
    async def get_query_results_gen(
        self, query_id: str
    ) -> AsyncGenerator[Sequence[Any]]:
        """Stream the results of a completed query.

        Parameters
        ----------
        query_id
            ID of the query.

        Yields
        ------
        Sequence
            Individual rows as sequences of column values (Row or tuple).

        Raises
        ------
        Exception
            Error if result retrieval fails.
        """
        yield  # type: ignore[misc]

    @abstractmethod
    async def get_query_status(self, query_id: str) -> QueryStatus:
        """Get the current status of a query.

        Parameters
        ----------
        query_id
            ID of the query.

        Returns
        -------
        QueryStatus
            Current status of the query (QservQueryStatus or
            BigQueryQueryStatus depending on the backend).

        Raises
        ------
        Exception
            Error if status retrieval fails.
        """

    @abstractmethod
    async def list_running_queries(self) -> dict[str, ProcessStatus]:
        """List all currently running queries.

        This is used by the monitoring service to detect status changes.

        Returns
        -------
        dict of ProcessStatus
            Mapping from query ID to process status for all running queries.

        Raises
        ------
        Exception
            Error if listing fails.

        Notes
        -----
        Some backends may not support listing all running
        queries. In such cases, implementations should return an empty dict
        and rely on status polling of known queries instead.
        """

    @abstractmethod
    def result_api_failure_event(self) -> QueryApiFailureEvent:
        """Return an appropriate API failure event for result retrieval.

        Different backends use different metrics events, possibly with
        parameters, for a failure in retrieving results. This event cannot be
        easily logged from inside the result iterator and therefore must be
        logged by the results service, but it is backend-agnostic. It
        therefore calls this method to retrieve the appropriate event to log.

        Returns
        -------
        QueryApiFailureEvent
            Appropriate event to log for a backend failure while retrieving
            results.
        """

    @abstractmethod
    async def submit_query(self, job: JobRun) -> str:
        """Submit a query to the backend for execution.

        Parameters
        ----------
        job
            Query job run request from the user via Kafka.

        Returns
        -------
        str
            ID for the submitted query.

        Raises
        ------
        Exception
            Error if the query submit fails.
        """

    @abstractmethod
    async def upload_table(self, upload: JobTableUpload) -> TableUploadStats:
        """Upload a temporary table for use in queries.

        Parameters
        ----------
        upload
            Table upload specification.

        Returns
        -------
        TableUploadStats
            Statistics about the uploaded table.

        Raises
        ------
        NotImplementedError
            If the backend does not support table uploads.
        Exception
            Error if the upload fails.

        Notes
        -----
        Backends that don't support this operation should raise
        the appropriate exception.
        """
