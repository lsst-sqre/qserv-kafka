"""Models used for talking to Qserv."""

from dataclasses import dataclass
from datetime import timedelta
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from safir.pydantic import UtcDatetime

from .query import AsyncQueryPhase

__all__ = [
    "AsyncQueryPhase",
    "AsyncSubmitRequest",
    "AsyncSubmitResponse",
    "BaseResponse",
    "QservAsyncStatusData",
    "QservStatusResponse",
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


class QservAsyncStatusData(BaseModel):
    """QServ async query status data from REST API.

    This model is used to parse QServ's API responses and is designed for use
    only within QservClient.
    """

    model_config = ConfigDict(validate_by_name=True)

    query_id: Annotated[int, Field(validation_alias="queryId")]

    status: Annotated[str, Field(validation_alias="status")]

    total_chunks: Annotated[int, Field(validation_alias="totalChunks")]

    completed_chunks: Annotated[
        int,
        Field(validation_alias="completedChunks"),
        BeforeValidator(lambda v: 0 if v is None else v),
    ]

    query_begin: Annotated[
        UtcDatetime, Field(validation_alias="queryBeginEpoch")
    ]

    last_update: Annotated[
        UtcDatetime | None,
        Field(validation_alias="lastUpdateEpoch"),
        BeforeValidator(
            lambda u: None if isinstance(u, int) and u == 0 else u
        ),
    ] = None

    query: Annotated[str | None, Field(title="Query text")] = None

    error: Annotated[
        str | None,
        Field(title="Error for failed query"),
        BeforeValidator(lambda v: v if v else None),
    ] = None

    czar_id: Annotated[
        int | None, Field(title="Backend czar ID", validation_alias="czarId")
    ] = None

    czar_type: Annotated[
        str | None,
        Field(title="Backend czar type", validation_alias="czarType"),
    ] = None

    collected_bytes: Annotated[
        int,
        Field(
            title="Result bytes collected",
            validation_alias="collectedBytes",
        ),
        BeforeValidator(lambda v: 0 if v is None else v),
    ] = 0

    final_rows: Annotated[
        int | None,
        Field(title="Final row count", validation_alias="finalRows"),
    ] = None


class QservStatusResponse(BaseResponse):
    """Response to a QServ async query status request.

    Replacement for AsyncStatusResponse that uses
    QservAsyncStatusData for parsing QServ's API responses.
    """

    status: Annotated[QservAsyncStatusData, Field(title="Query status")]


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
