"""Models for query status and execution."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field
from safir.pydantic import UtcDatetime

from .progress import ByteProgress, ChunkProgress, ProgressMetrics

__all__ = [
    "AsyncQueryPhase",
    "ProcessStatus",
    "QueryStatus",
]


class AsyncQueryPhase(StrEnum):
    """Possible status values for a query from the backend's perspective."""

    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    FAILED_LR = "FAILED_LR"
    ABORTED = "ABORTED"


class QueryStatus(BaseModel):
    """Query status for internal storage.

    Attributes
    ----------
    query_id
        Backend query identifier as a string (numeric for QServ, UUID for
        BigQuery)
    status
        Current execution phase
    query_begin
        When the query started executing in the backend
    last_update
        When the status was last updated
    error
        Error message if the query failed
    collected_bytes
        Total bytes collected/processed by the backend
    final_rows
        Number of rows in the final result (once completed)
    progress
        Backend-specific progress (ChunkProgress for QServ, ByteProgress for
        BigQuery, None if not available)
    query
        Optional query text for debugging
    czar_id
        QServ-specific czar ID (None for other backends)
    czar_type
        QServ-specific czar type (None for other backends)
    """

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

    progress: Annotated[
        ProgressMetrics | None,
        Field(
            title="Backend-specific progress",
            description=(
                "Progress information specific to the backend. "
                "ChunkProgress for QServ, ByteProgress for BigQuery, "
                "or None if not available."
            ),
        ),
    ] = None

    query: Annotated[str | None, Field(title="Query text")] = None

    czar_id: Annotated[
        int | None, Field(title="QServ czar ID (QServ only)")
    ] = None

    czar_type: Annotated[
        str | None, Field(title="QServ czar type (QServ only)")
    ] = None

    def has_chunk_progress(self) -> bool:
        """Check if this query uses chunk-based progress (QServ)."""
        return isinstance(self.progress, ChunkProgress)

    def has_byte_progress(self) -> bool:
        """Check if this query uses byte-based progress (BigQuery)."""
        return isinstance(self.progress, ByteProgress)

    def get_completion_percentage(self) -> float | None:
        """Get completion percentage if progress type supports it.

        Returns
        -------
        float or None
            Completion percentage (0-100) for chunk-based progress, or None
            if progress type doesn't support percentage calculation.
        """
        if isinstance(self.progress, ChunkProgress):
            return self.progress.completion_percentage()
        return None

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
        self.progress = other.progress


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

        # Progress must be the same type
        if type(self.progress) is not type(other.progress):
            return True

        # Compare progress based on type
        if isinstance(self.progress, ChunkProgress) and isinstance(
            other.progress, ChunkProgress
        ):
            return (
                self.progress.total_chunks != other.progress.total_chunks
                or self.progress.completed_chunks
                != other.progress.completed_chunks
            )

        if isinstance(self.progress, ByteProgress) and isinstance(
            other.progress, ByteProgress
        ):
            return (
                self.progress.bytes_processed != other.progress.bytes_processed
            )

        return False
