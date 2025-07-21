"""Models used for talking to Qserv."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from safir.pydantic import UtcDatetime

__all__ = [
    "AsyncQueryPhase",
    "AsyncQueryStatus",
    "AsyncStatusResponse",
    "AsyncSubmitRequest",
    "AsyncSubmitResponse",
    "BaseResponse",
    "TableUploadStats",
]


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


class AsyncQueryPhase(StrEnum):
    """Possible status values for the query from Qserv's perspective."""

    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


class AsyncQueryStatus(BaseModel):
    """Status information for a query."""

    model_config = ConfigDict(validate_by_name=True)

    query_id: Annotated[int, Field(title="ID", validation_alias="queryId")]

    status: Annotated[AsyncQueryPhase, Field(title="Status")]

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

    collected_bytes: Annotated[
        int,
        Field(
            title="Size of results",
            description="Size of results collected so far in bytes",
            validation_alias="collectedBytes",
        ),
        BeforeValidator(lambda v: 0 if v is None else v),
    ] = 0

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


class AsyncStatusResponse(BaseResponse):
    """Response to an async query status request."""

    status: Annotated[AsyncQueryStatus, Field(title="Async query status")]


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
