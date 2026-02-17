"""Models used for talking to Qserv."""

from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from safir.pydantic import UtcDatetime

from .progress import ChunkProgress
from .query import AsyncQueryPhase, QservQueryStatus

__all__ = [
    "AsyncSubmitRequest",
    "AsyncSubmitResponse",
    "BaseResponse",
    "QservAsyncStatusData",
    "QservQueryPhase",
    "QservStatusResponse",
    "TableUploadStats",
]


class QservQueryPhase(StrEnum):
    """Qserv-specific query status values.

    These are the status values returned by the Qserv API. They are
    translated to generic `AsyncQueryPhase` values for use in the service
    layer.
    """

    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    FAILED_LR = "FAILED_LR"
    ABORTED = "ABORTED"

    def to_generic_phase(self) -> AsyncQueryPhase:
        """Translate to generic query phase.

        Returns
        -------
        AsyncQueryPhase
            The generic phase corresponding to this Qserv-specific phase.
            FAILED_LR is translated to FAILED.
        """
        if self == QservQueryPhase.FAILED_LR:
            return AsyncQueryPhase.FAILED
        return AsyncQueryPhase(self.value)


class BaseResponse(BaseModel):
    """Parameters included in all responses from the Qserv REST API."""

    success: Annotated[int, Field(title="Success code")]

    error: Annotated[str | None, Field(title="Error message")] = None

    error_ext: Annotated[
        dict[str, Any] | None, Field(title="Extra error details")
    ] = None

    warning: Annotated[str | None, Field(title="Warning message")] = None

    def is_success(self) -> bool:
        """Whether the request was successful."""
        return bool(self.success)


class QservAsyncStatusData(BaseModel):
    """Qserv async query status data from REST API.

    This model is used to parse Qserv's API responses and is designed for use
    only within `QservClient`.
    """

    model_config = ConfigDict(validate_by_name=True)

    query_id: Annotated[int, Field(title="ID", validation_alias="queryId")]

    status: Annotated[
        QservQueryPhase, Field(title="Status", validation_alias="status")
    ]

    total_chunks: Annotated[
        int, Field(title="Total query chunks", validation_alias="totalChunks")
    ]

    completed_chunks: Annotated[
        int,
        Field(
            title="Completed query chunks", validation_alias="completedChunks"
        ),
        BeforeValidator(lambda v: 0 if v is None else v),
    ]

    query_begin: Annotated[
        UtcDatetime,
        Field(title="Query start time", validation_alias="queryBeginEpoch"),
    ]

    last_update: Annotated[
        UtcDatetime | None,
        Field(title="Last status update", validation_alias="lastUpdateEpoch"),
        BeforeValidator(
            lambda u: None if isinstance(u, int) and u == 0 else u
        ),
    ] = None

    query: Annotated[str | None, Field(title="Query text")] = None

    error: Annotated[
        str | None,
        Field(title="Error for failed query"),
        BeforeValidator(lambda v: v or None),
    ] = None

    czar_id: Annotated[
        int | None,
        Field(title="Qserv czar processing query", validation_alias="czarId"),
    ] = None

    czar_type: Annotated[
        str | None,
        Field(
            title="Type of Qserv czar processing query",
            validation_alias="czarType",
        ),
    ] = None

    collected_bytes: Annotated[
        int,
        Field(
            title="Size of results",
            description="Size of results collected so far in bytes",
            validation_alias="collectedBytes",
        ),
        BeforeValidator(lambda v: 0 if v is None else v),
    ] = 0

    final_rows: Annotated[
        int | None,
        Field(title="Rows in result", validation_alias="finalRows"),
    ] = None

    def to_query_status(self) -> QservQueryStatus:
        """Convert to query status model.

        Returns
        -------
        QservQueryStatus
            Query status for Qserv backend.
        """
        qserv_phase = QservQueryPhase(self.status)
        generic_phase = qserv_phase.to_generic_phase()
        results_too_large = qserv_phase == QservQueryPhase.FAILED_LR

        return QservQueryStatus(
            backend_type="Qserv",
            query_id=str(self.query_id),
            status=generic_phase,
            query_begin=self.query_begin,
            last_update=self.last_update,
            error=self.error,
            collected_bytes=self.collected_bytes,
            final_rows=self.final_rows,
            chunk_progress=ChunkProgress(
                total_chunks=self.total_chunks,
                completed_chunks=self.completed_chunks,
            ),
            czar_id=self.czar_id,
            czar_type=self.czar_type,
            results_too_large=results_too_large,
        )


class QservStatusResponse(BaseResponse):
    """Response to a Qserv async query status request.

    Uses `QservAsyncStatusData` to parse Qserv's API responses.
    """

    status: Annotated[QservAsyncStatusData, Field(title="Async query status")]


class AsyncSubmitRequest(BaseModel):
    """Request parameters to create an async job."""

    query: Annotated[str, Field(title="Query to run")]

    database: Annotated[
        str | None, Field(title="Default database for query")
    ] = None


class AsyncSubmitResponse(BaseResponse):
    """Response from creating an async job."""

    model_config = ConfigDict(validate_by_name=True)

    query_id: Annotated[
        int,
        Field(
            title="Query ID",
            serialization_alias="queryId",
            validation_alias="queryId",
        ),
    ]


@dataclass
class TableUploadStats:
    """Statistics from a table upload to Qserv."""

    size: int
    """Size of the uploaded table in CSV format (bytes)."""

    elapsed: timedelta
    """Time required to upload the table to Qserv.

    Does not include the time required to retrieve the table source and SQL
    from GCS before uploading.
    """
