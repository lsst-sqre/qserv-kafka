"""Models for query status and execution."""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Annotated, Any, Literal, override

from pydantic import BaseModel, Field
from safir.pydantic import UtcDatetime

from .progress import ByteProgress, ChunkProgress, ProgressMetrics

__all__ = [
    "AsyncQueryPhase",
    "BigQueryQueryStatus",
    "ProcessStatus",
    "QservQueryStatus",
    "QueryStatus",
]


class AsyncQueryPhase(StrEnum):
    """Possible status values for a query from the backend's perspective.

    These are generic phases shared by all backends.
    Backend-specific failure reasons can be captured for each backend
    separately.
    """

    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


class QueryStatusBase(BaseModel, ABC):
    """Base class for query status models."""

    query_id: Annotated[str, Field(title="Backend query ID")]

    status: Annotated[AsyncQueryPhase, Field(title="Execution phase")]

    query_begin: Annotated[UtcDatetime, Field(title="Query start time")]

    last_update: Annotated[
        UtcDatetime | None, Field(title="Last status update")
    ] = None

    error: Annotated[str | None, Field(title="Error message")] = None

    collected_bytes: Annotated[
        int, Field(title="Bytes collected by backend")
    ] = 0

    final_rows: Annotated[int | None, Field(title="Final row count")] = None

    query: Annotated[str | None, Field(title="Query text")] = None

    results_too_large: Annotated[
        bool,
        Field(
            title="Results too large",
            description="Query failed because results exceeded size limit",
        ),
    ] = False

    def to_process_status(self) -> ProcessStatus:
        """Extract minimal process status for monitoring.

        Returns
        -------
        ProcessStatus
            Minimal status containing only monitoring-relevant fields.
        """
        return ProcessStatus(
            status=self.status,
            last_update=self.last_update,
            progress=self.progress,
        )

    def is_different_than(self, other: ProcessStatus) -> bool:
        """Check if new process status represents a meaningful change.

        Parameters
        ----------
        other
            New process status to compare against.

        Returns
        -------
        bool
            True if the statuses differ meaningfully.
        """
        current = self.to_process_status()
        return current.is_different_than(other)

    def update_from(self, other: ProcessStatus) -> None:
        """Update this query status from new process status.

        This updates only the monitoring-relevant fields (status, last_update,
        progress) without retrieving the full query status from the backend.

        Parameters
        ----------
        other
            New process status to update from.
        """
        self.status = other.status
        self.last_update = other.last_update
        self.update_progress_from(other.progress)

    @abstractmethod
    def update_progress_from(self, progress: ProgressMetrics | None) -> None:
        """Update progress from a `ProcessStatus`."""

    @property
    @abstractmethod
    def progress(self) -> ProgressMetrics | None:
        """Backend-specific progress."""

    @abstractmethod
    def to_logging_context(self) -> dict[str, Any]:
        """Get backend-specific fields for logging context.

        Returns
        -------
        dict
            Dictionary of field names to values for logging.
        """


class QservQueryStatus(QueryStatusBase):
    """Query status for Qserv backend.

    Includes Qserv specific info (e.g. chunk-based progress and czar
    information).
    """

    backend_type: Annotated[
        Literal["Qserv"], Field(title="Backend type discriminator")
    ]

    chunk_progress: Annotated[
        ChunkProgress | None,
        Field(title="Chunk-based progress"),
    ] = None

    czar_id: Annotated[int | None, Field(title="Qserv czar ID")] = None

    czar_type: Annotated[str | None, Field(title="Qserv czar type")] = None

    @override
    @property
    def progress(self) -> ChunkProgress | None:
        """Chunk-based progress for Qserv queries."""
        return self.chunk_progress

    @override
    def update_progress_from(self, progress: ProgressMetrics | None) -> None:
        """Update chunk progress from `ProcessStatus`."""
        if isinstance(progress, ChunkProgress):
            self.chunk_progress = progress

    def get_completion_percentage(self) -> float | None:
        """Get completion percentage from chunk progress.

        Returns
        -------
        float or None
            Completion percentage (0-100), or None if no progress available.
        """
        if self.chunk_progress:
            return self.chunk_progress.completion_percentage()
        return None

    @override
    def to_logging_context(self) -> dict[str, Any]:
        """Get Qserv-specific fields for logging context."""
        result: dict[str, Any] = {}
        if self.chunk_progress:
            result["total_chunks"] = self.chunk_progress.total_chunks
            result["completed_chunks"] = self.chunk_progress.completed_chunks
        return result


class BigQueryQueryStatus(QueryStatusBase):
    """Query status for BigQuery backend.

    Includes BigQuery specific to info (e.g. byte progress).
    """

    backend_type: Annotated[
        Literal["BigQuery"], Field(title="Backend type discriminator")
    ]

    byte_progress: Annotated[
        ByteProgress | None,
        Field(title="Byte-based progress"),
    ] = None

    @override
    @property
    def progress(self) -> ByteProgress | None:
        """Byte-based progress for BigQuery queries."""
        return self.byte_progress

    @override
    def update_progress_from(self, progress: ProgressMetrics | None) -> None:
        """Update byte progress from ProcessStatus."""
        if isinstance(progress, ByteProgress):
            self.byte_progress = progress

    @override
    def to_logging_context(self) -> dict[str, Any]:
        """Get BigQuery-specific fields for logging context."""
        result: dict[str, Any] = {}
        if self.byte_progress:
            result["bytes_processed"] = self.byte_progress.bytes_processed
            result["bytes_processed_human"] = (
                self.byte_progress.to_human_readable()
            )
            if self.byte_progress.bytes_billed is not None:
                result["bytes_billed"] = self.byte_progress.bytes_billed
            result["cached"] = self.byte_progress.cached
        return result


type QueryStatus = Annotated[
    QservQueryStatus | BigQueryQueryStatus,
    Field(discriminator="backend_type"),
]


class ProcessStatus(BaseModel):
    """Minimal status info for monitoring queries.

    Used to check if a query's status has changed.

    Attributes
    ----------
    status
        Current execution phase
    last_update
        When the status was last updated
    progress
        Backend-specific progress information
    """

    status: Annotated[AsyncQueryPhase, Field(title="Execution phase")]

    last_update: Annotated[
        UtcDatetime | None, Field(title="Last status update")
    ] = None

    progress: Annotated[
        ProgressMetrics | None,
        Field(title="Backend-specific progress"),
    ] = None

    def is_different_than(self, other: ProcessStatus) -> bool:
        """Check if this status represents a meaningful change.

        Parameters
        ----------
        other
            Another status to compare against

        Returns
        -------
        bool
            True if the statuses differ in a meaningful way
        """
        if self.status != other.status:
            return True
        if self.last_update != other.last_update:
            return True

        if self.progress is None and other.progress is None:
            return False

        if self.progress is None or other.progress is None:
            return True

        return self.progress.is_different_than(other.progress)
