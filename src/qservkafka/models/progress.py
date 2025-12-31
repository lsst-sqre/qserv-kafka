"""Data models for query progress."""

from __future__ import annotations

from typing import Annotated

import humanize
from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "ByteProgress",
    "ChunkProgress",
    "ProgressMetrics",
]


class ChunkProgress(BaseModel):
    """Qserv-style chunk-based progress reporting.

    Qserv divides queries across data chunks and reports progress as the
    number of chunks completed out of the total number of chunks.

    Attributes
    ----------
    total_chunks
        Total number of data chunks that must be processed
    completed_chunks
        Number of chunks that have been processed so far
    """

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    total_chunks: Annotated[
        int,
        Field(
            title="Total chunks",
            description="Total query chunks",
            alias="totalChunks",
        ),
    ]

    completed_chunks: Annotated[
        int,
        Field(
            title="Completed chunks",
            description="Completed query chunks",
            alias="completedChunks",
        ),
    ]

    def completion_percentage(self) -> float:
        """Calculate completion percentage.

        Returns
        -------
        float
            Percentage complete (0.0 to 100.0)
        """
        if self.total_chunks == 0:
            return 0.0
        return (self.completed_chunks / self.total_chunks) * 100.0

    def is_different_than(self, other: ProgressMetrics) -> bool:
        """Check if progress has changed.

        Parameters
        ----------
        other
            Another progress to compare against.

        Returns
        -------
        bool
            True if types differ or progress values differ.
        """
        if not isinstance(other, ChunkProgress):
            return True
        return (
            self.total_chunks != other.total_chunks
            or self.completed_chunks != other.completed_chunks
        )


class ByteProgress(BaseModel):
    """BigQuery-style byte-based progress reporting.

    BigQuery reports progress in terms of data processed (bytes scanned)
    and may indicate whether results were served from cache.

    Attributes
    ----------
    bytes_processed
        Number of bytes processed by the query
    bytes_billed
        Number of bytes that will be billed (may differ from processed
        if results are cached)
    cached
        Whether the query results were served from cache
    """

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    bytes_processed: Annotated[
        int,
        Field(
            title="Bytes processed",
            description="Bytes scanned during query execution",
            alias="bytesProcessed",
        ),
    ]

    bytes_billed: Annotated[
        int | None,
        Field(
            title="Bytes billed",
            description="Bytes that will be billed for this query",
            alias="bytesBilled",
        ),
    ] = None

    cached: Annotated[
        bool,
        Field(
            title="Cached",
            description="Whether results were served from cache",
        ),
    ] = False

    def to_human_readable(self) -> str:
        return humanize.naturalsize(self.bytes_processed, binary=True)

    def is_different_than(self, other: ProgressMetrics) -> bool:
        """Check if progress has changed.

        Parameters
        ----------
        other
            Another progress to compare against.

        Returns
        -------
        bool
            True if types differ or bytes processed differs.
        """
        if not isinstance(other, ByteProgress):
            return True
        return self.bytes_processed != other.bytes_processed


# Progress metrics type (either chunk-based or byte-based)
type ProgressMetrics = ChunkProgress | ByteProgress
