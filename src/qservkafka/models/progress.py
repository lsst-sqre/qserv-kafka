"""Data models for query progress."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

__all__ = [
    "ByteProgress",
    "ChunkProgress",
    "ProgressMetrics",
]


class ChunkProgress(BaseModel):
    """QServ-style chunk-based progress reporting.

    QServ divides queries across data chunks and reports progress as the
    number of chunks completed out of the total number of chunks.

    Attributes
    ----------
    total_chunks
        Total number of data chunks that must be processed
    completed_chunks
        Number of chunks that have been processed so far
    """

    total_chunks: Annotated[
        int, Field(title="Total chunks", description="Total query chunks")
    ]

    completed_chunks: Annotated[
        int,
        Field(title="Completed chunks", description="Completed query chunks"),
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

    bytes_processed: Annotated[
        int,
        Field(
            title="Bytes processed",
            description="Bytes scanned during query execution",
        ),
    ]

    bytes_billed: Annotated[
        int | None,
        Field(
            title="Bytes billed",
            description="Bytes that will be billed for this query",
        ),
    ] = None

    cached: Annotated[
        bool,
        Field(
            title="Cached",
            description="Whether results were served from cache",
        ),
    ] = False

    def to_gb(self) -> float:
        """Convert bytes processed to gigabytes.

        Returns
        -------
        float
            Bytes processed in gigabytes
        """
        return self.bytes_processed / (1024**3)

    def to_tb(self) -> float:
        """Convert bytes processed to terabytes.

        Returns
        -------
        float
            Bytes processed in terabytes
        """
        return self.bytes_processed / (1024**4)


# Progress metrics type (either chunk-based or byte-based)
ProgressMetrics = ChunkProgress | ByteProgress
