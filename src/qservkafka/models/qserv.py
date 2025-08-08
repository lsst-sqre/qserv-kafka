"""Models used for talking to Qserv."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from safir.pydantic import UtcDatetime

__all__ = [
    "AsyncProcessStatus",
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
    FAILED_LR = "FAILED_LR"
    ABORTED = "ABORTED"


class AsyncProcessStatus(BaseModel):
    """Process status for a query.

    This is the subset of information that we retrieve about running queries,
    as opposed to the full query status in `AsyncQueryStatus`. It is used to
    determine if we need to send a status update message to Kafka and to
    determine if the query is finished.
    """

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

    def is_different_than(self, new: AsyncProcessStatus) -> bool:
        """Whether a new process status represents a change worth an update."""
        return (
            self.status != new.status
            or self.total_chunks != new.total_chunks
            or self.completed_chunks != new.completed_chunks
            or self.last_update != new.last_update
        )

    def update_from(self, new: AsyncProcessStatus) -> None:
        """Copy updated information from a new process status.

        This avoids the need to make another Qserv REST API call to return the
        full query status when the only change is forward progress on a
        running query.
        """
        self.total_chunks = new.total_chunks
        self.completed_chunks = new.completed_chunks
        self.last_update = new.last_update


class AsyncQueryStatus(AsyncProcessStatus):
    """Status information for a query.

    This includes the additional fields that are only available via the REST
    API.
    """

    query: Annotated[str | None, Field(title="Query text")] = None

    error: Annotated[
        str | None,
        Field(title="Error for failed query"),
        BeforeValidator(lambda v: v if v else None),
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
        int | None, Field(title="Rows in result", validation_alias="finalRows")
    ] = None

    def to_process_status(self) -> AsyncProcessStatus:
        """Convert to an `AsyncProcessStatus`.

        Used primarily by the test suite to compare a full `AsyncQueryStatus`
        to an `AsyncProcessStatus`.
        """
        return AsyncProcessStatus(
            query_id=self.query_id,
            status=self.status,
            total_chunks=self.total_chunks,
            completed_chunks=self.completed_chunks,
            query_begin=self.query_begin,
            last_update=self.last_update,
        )


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
