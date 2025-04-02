"""Models used for talking to Qserv."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from safir.pydantic import UtcDatetime
from vo_models.uws.types import ExecutionPhase

__all__ = [
    "AsyncQueryPhase",
    "AsyncQueryStatus",
    "AsyncStatusResponse",
    "AsyncSubmitRequest",
    "AsyncSubmitResponse",
    "BaseResponse",
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

    def to_execution_phase(self) -> ExecutionPhase:
        match self.value:
            case "EXECUTING":
                return ExecutionPhase.EXECUTING
            case "COMPLETED":
                return ExecutionPhase.COMPLETED
            case "FAILED":
                return ExecutionPhase.ERROR
            case "ABORTED":
                return ExecutionPhase.ABORTED
            case _:
                raise ValueError(f"Unknown phase {self.value}")


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


class AsyncStatusResponse(BaseResponse):
    """Response to an async query status request."""

    status: Annotated[AsyncQueryStatus, Field(title="Async query status")]


class AsyncSubmitRequest(BaseModel):
    """Request parameters to create an async job."""

    query: Annotated[str, Field(title="Query to run")]

    database: Annotated[str | None, Field(title="Default database for query")]


class AsyncSubmitResponse(BaseResponse):
    """Response from creating an async job."""

    query_id: Annotated[int, Field(title="Query ID")]
