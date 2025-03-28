"""Models for Kafka messages."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    HttpUrl,
    PlainSerializer,
)
from safir.pydantic import SecondsTimedelta
from vo_models.uws.types import ExecutionPhase

type DatetimeMillis = Annotated[
    datetime,
    BeforeValidator(
        lambda t: t
        if not isinstance(t, float)
        else datetime.fromtimestamp(t / 1000, tz=UTC)
    ),
    PlainSerializer(lambda t: int(t.timestamp() * 1000), return_type=int),
]
"""Type for timestamps, which are represented in Kafka in milliseconds."""

__all__ = [
    "JobError",
    "JobErrorCode",
    "JobMetadata",
    "JobQueryInfo",
    "JobResultEnvelope",
    "JobResultFormat",
    "JobResultInfo",
    "JobResultSerialization",
    "JobResultType",
    "JobRun",
    "JobStatus",
]


class JobResultEnvelope(BaseModel):
    """VOTable envelope for job results."""

    model_config = ConfigDict(validate_by_name=True)

    header: Annotated[
        str,
        Field(
            title="Result XML header",
            description="VOTable XML header for the results",
        ),
    ]

    footer: Annotated[
        str,
        Field(
            title="Result XML footer",
            description="VOTable XML footer for the results",
        ),
    ]


class JobResultType(StrEnum):
    """Possible types for the output format of results."""

    votable = "votable"


class JobResultSerialization(StrEnum):
    """Possible serializations of the output format of results."""

    TABLEDATA = "TABLEDATA"
    BINARY2 = "BINARY2"


class JobResultFormat(BaseModel):
    """Result format for job results."""

    model_config = ConfigDict(validate_by_name=True)

    type: Annotated[
        JobResultType,
        Field(
            title="Output format for result",
            description="Format in which to write the output",
        ),
    ]

    serialization: Annotated[
        JobResultSerialization | None,
        Field(
            title="Serialization of result",
            description="Serialization format of the result",
        ),
    ] = None

    base_url: Annotated[
        str | None,
        Field(
            title="Base URL for access_url",
            description="Base URL for rewriting access_url column values",
            validation_alias="baseUrl",
        ),
    ] = None

    envelope: Annotated[
        JobResultEnvelope,
        Field(
            title="XML envelope", description="XML envelope for the results"
        ),
    ]


class JobRun(BaseModel):
    """Kafka message requesting execution of a TAP query."""

    model_config = ConfigDict(validate_by_name=True)

    job_id: Annotated[
        str,
        Field(
            title="UWS job ID",
            description="Identifier of job in the TAP server's UWS database",
            validation_alias="jobID",
        ),
    ]

    owner: Annotated[
        str,
        Field(
            title="Username of owner",
            description="Username of the user who generated the query",
            validation_alias="ownerID",
        ),
    ]

    query: Annotated[
        str,
        Field(
            title="Query to run",
            description="TAP query converted to MySQL-compatible SQL",
        ),
    ]

    database: Annotated[
        str | None,
        Field(
            title="Database to query",
            description="Database to query if not specified in the query",
        ),
    ] = None

    result_url: Annotated[
        HttpUrl,
        Field(
            title="Results URL",
            description="Signed URL at which to store the results",
            validation_alias="resultDestination",
        ),
    ]

    result_format: Annotated[
        JobResultFormat,
        Field(
            title="Format of result",
            description="Formatting instructions for writing the result",
            validation_alias="resultFormat",
        ),
    ]

    timeout: Annotated[
        SecondsTimedelta | None,
        Field(
            title="Query timeout",
            description="Optional timeout in seconds for query execution",
        ),
    ] = None


class JobQueryInfo(BaseModel):
    """Information about the status of an executing query."""

    model_config = ConfigDict(serialize_by_alias=True)

    start_time: Annotated[
        DatetimeMillis,
        Field(
            title="Start time",
            description="When the job started executing",
            serialization_alias="startTime",
        ),
    ]

    end_time: Annotated[
        DatetimeMillis | None,
        Field(
            title="Completion time",
            description="When the job completed",
            serialization_alias="endTime",
        ),
    ] = None

    total_chunks: Annotated[
        int,
        Field(
            title="Total work units",
            description="Total work units required for the query",
            serialization_alias="totalChunks",
        ),
    ]

    completed_chunks: Annotated[
        int,
        Field(
            title="Completed work units",
            description="Work units completed so far",
            serialization_alias="completedChunks",
        ),
    ]


class JobResultInfo(BaseModel):
    """Result of a query."""

    model_config = ConfigDict(serialize_by_alias=True)

    total_rows: Annotated[
        int,
        Field(
            title="Output rows",
            description="Total number of rows in the result",
            serialization_alias="totalRows",
        ),
    ]

    format: Annotated[JobResultType, Field(title="Format of result")] = (
        JobResultType.votable
    )

    serialization: Annotated[
        JobResultSerialization | None,
        Field(
            title="Serialization of result",
            description="Serialization format of the result",
        ),
    ] = None


class JobErrorCode(StrEnum):
    """Possible error codes for failures."""

    backend_error = "backend_error"


class JobError(BaseModel):
    """Error from a query."""

    model_config = ConfigDict(serialize_by_alias=True)

    code: Annotated[
        JobErrorCode,
        Field(title="Error code", serialization_alias="errorCode"),
    ]

    message: Annotated[
        str,
        Field(
            title="Error message",
            description="Human-readable error message",
            serialization_alias="errorMessage",
        ),
    ]


class JobMetadata(BaseModel):
    """Metadata about a query."""

    model_config = ConfigDict(serialize_by_alias=True)

    query: Annotated[
        str,
        Field(
            title="Query to run",
            description="TAP query converted to MySQL-compatible SQL",
        ),
    ]

    database: Annotated[
        str | None,
        Field(
            title="Database to query",
            description="Database to query if not specified in the query",
        ),
    ] = None


class JobStatus(BaseModel):
    """Status of a TAP query."""

    model_config = ConfigDict(serialize_by_alias=True)

    job_id: Annotated[
        str,
        Field(
            title="UWS job ID",
            description="Identifier of job in the TAP server's UWS database",
            serialization_alias="jobID",
        ),
    ]

    execution_id: Annotated[
        str,
        Field(
            title="Backend execution ID",
            description="Identifier of the running query in the backend",
            serialization_alias="executionID",
        ),
    ]

    timestamp: Annotated[
        DatetimeMillis,
        Field(
            title="Timestamp of update",
            description="When this update was published",
        ),
    ]

    status: Annotated[
        ExecutionPhase,
        Field(
            title="Current status",
            description="Status of the job as of this update",
        ),
    ]

    query_info: Annotated[
        JobQueryInfo,
        Field(title="Query information", serialization_alias="queryInfo"),
    ]

    result_info: Annotated[
        JobResultInfo | None,
        Field(
            title="Job result",
            description="Result of the job if it has completed",
            serialization_alias="resultInfo",
        ),
    ] = None

    error: Annotated[
        JobError | None,
        Field(
            title="Job error",
            description="Error for the job if the job failed",
            serialization_alias="errorInfo",
        ),
    ] = None

    metadata: Annotated[JobMetadata, Field(title="Job metadata")]
