"""Abstract interface for database backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from enum import StrEnum
from typing import Any

from ..models.kafka import JobRun, JobTableUpload
from ..models.progress import ProgressMetrics
from ..models.qserv import TableUploadStats
from ..models.query import AsyncQueryPhase, ProcessStatus, QueryStatus

__all__ = [
    "BackendProcessStatus",
    "BackendQueryStatus",
    "BackendType",
    "DatabaseBackend",
]


class BackendType(StrEnum):
    """Supported database backend types."""

    QSERV = "QSERV"
    BIGQUERY = "BIGQUERY"


class BackendQueryStatus:
    """Query status information.

    This is an abstraction over the status models for the various backends
    (Qserv, BigQuery..) and provides a common interface for the service
    layer.

    Attributes
    ----------
    query_id
        Backend-specific query identifier
    phase
        Current execution phase (EXECUTING, COMPLETED, FAILED, ABORTED)
    error
        Error message if the query failed
    progress
        Progress information
    query_begin
        When the query started executing
    last_update
        When the status was last updated
    collected_bytes
        Total size of results collected (bytes)
    final_rows
        Number of rows in the final result
    """

    def __init__(
        self,
        *,
        query_id: str,
        phase: str,
        error: str | None = None,
        progress: ProgressMetrics | None = None,
        query_begin: Any = None,
        last_update: Any = None,
        collected_bytes: int = 0,
        final_rows: int | None = None,
    ) -> None:
        self.query_id = query_id
        self.phase = phase
        self.error = error
        self.progress = progress
        self.query_begin = query_begin
        self.last_update = last_update
        self.collected_bytes = collected_bytes
        self.final_rows = final_rows

    def to_query_status(self) -> QueryStatus:
        """Convert to QueryStatus for internal use.

        Returns
        -------
        QueryStatus
            Internal query status representation.
        """
        return QueryStatus(
            query_id=self.query_id,
            status=AsyncQueryPhase(self.phase),
            query_begin=self.query_begin,
            last_update=self.last_update,
            error=self.error,
            collected_bytes=self.collected_bytes,
            final_rows=self.final_rows,
            progress=self.progress,
        )


class BackendProcessStatus:
    """Backend process status for a running query.

    This is a subset of BackendQueryStatus used for monitoring
    running queries without retrieving full query details.

    Attributes
    ----------
    query_id
        Backend-specific query identifier
    status
        Current execution phase
    progress
        Backend-specific progress information
    query_begin
        When the query started executing
    last_update
        When the status was last updated
    """

    def __init__(
        self,
        *,
        query_id: str,
        status: str,
        progress: ProgressMetrics | None = None,
        query_begin: Any = None,
        last_update: Any = None,
    ) -> None:
        self.query_id = query_id
        self.status = status
        self.progress = progress
        self.query_begin = query_begin
        self.last_update = last_update

    def to_process_status(self) -> ProcessStatus:
        """Convert to ProcessStatus for monitoring.

        Returns
        -------
        ProcessStatus
            Process status for monitoring.
        """
        return ProcessStatus(
            status=AsyncQueryPhase(self.status),
            last_update=self.last_update,
            progress=self.progress,
        )


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
    async def get_query_status(self, query_id: str) -> BackendQueryStatus:
        """Get the current status of a query.

        Parameters
        ----------
        query_id
            ID of the query.

        Returns
        -------
        BackendQueryStatus
            Current status of the query

        Raises
        ------
        Exception
            Error if status retrieval fails.
        """

    @abstractmethod
    async def list_running_queries(self) -> dict[str, BackendProcessStatus]:
        """List all currently running queries.

        This is used by the monitoring service to detect status changes.

        Returns
        -------
        dict of BackendProcessStatus
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
    async def get_query_results_gen(
        self, query_id: str
    ) -> AsyncGenerator[tuple[Any, ...]]:
        """Stream the results of a completed query.

        Parameters
        ----------
        query_id
            ID of the query.

        Yields
        ------
        tuple
            Individual rows as tuples of column values.

        Raises
        ------
        Exception
            Error if result retrieval fails.
        """
        yield  # type: ignore[misc]

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
