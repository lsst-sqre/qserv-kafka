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

from .votable import VOTableArraySize, VOTablePrimitive

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
    "JobCancel",
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


class JobCancel(BaseModel):
    """Request to cancel a running query."""

    model_config = ConfigDict(validate_by_name=True)

    job_id: Annotated[
        str,
        Field(
            title="UWS job ID",
            description="Identifier of job in the TAP server's UWS database",
            validation_alias="jobID",
        ),
    ]

    execution_id: Annotated[
        str,
        Field(
            title="Backend execution ID",
            description="Identifier of the running query in the backend",
            validation_alias="executionID",
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

    footer_overflow: Annotated[
        str,
        Field(
            title="Result XML overflow footer",
            description=(
                "VOTable XML footer to use for results that overflow a MAXREC"
                " setting"
            ),
            validation_alias="footerOverflow",
        ),
    ]


class JobResultType(StrEnum):
    """Possible types for the output format of results."""

    VOTable = "VOTable"


class JobResultSerialization(StrEnum):
    """Possible serializations of the output format of results."""

    TABLEDATA = "TABLEDATA"
    BINARY2 = "BINARY2"


class JobResultFormat(BaseModel):
    """Format of the result of a query."""

    type: Annotated[
        JobResultType,
        Field(
            title="Output format for result",
            description="Format in which to write the output",
        ),
    ]

    serialization: Annotated[
        JobResultSerialization,
        Field(
            title="Serialization of result",
            description="Serialization format of the result",
        ),
    ]


class JobResultColumnType(BaseModel):
    """Type information for a single output column."""

    model_config = ConfigDict(validate_by_name=True)

    name: Annotated[str, Field(title="Column name")]

    datatype: Annotated[VOTablePrimitive, Field(title="Primitive type")]

    arraysize: Annotated[
        VOTableArraySize | None, Field(title="Array size")
    ] = None

    requires_url_rewrite: Annotated[
        bool,
        Field(
            title="Whether to rewrite value",
            description=(
                "If true, this column contains a URL that needs to be"
                " rewritten using base URL information"
            ),
            validation_alias="requiresUrlRewrite",
        ),
    ] = False

    def is_string(self) -> bool:
        """Check whether the underlying data type is a string."""
        return self.datatype.is_string()


class JobResultConfig(BaseModel):
    """Configuration for job result."""

    model_config = ConfigDict(validate_by_name=True)

    format: Annotated[JobResultFormat, Field(title="Output format for result")]

    envelope: Annotated[
        JobResultEnvelope,
        Field(
            title="XML envelope", description="XML envelope for the results"
        ),
    ]

    column_types: Annotated[
        list[JobResultColumnType],
        Field(
            title="Type information",
            description="Types of output columns, in column order",
            validation_alias="columnTypes",
        ),
    ]

    base_url: Annotated[
        str | None,
        Field(
            title="Base URL for access_url",
            description="Base URL for rewriting access_url column values",
            validation_alias="baseUrl",
        ),
    ] = None


class JobTableUpload(BaseModel):
    """Table to upload for a query."""

    model_config = ConfigDict(validate_by_name=True)

    table_name: Annotated[
        str,
        Field(
            title="Name of table",
            description=(
                "Name of the table in Qserv. Must start with user_<username>."
            ),
            pattern=r"user_[^.]+\.[^.]+$",
            validation_alias="tableName",
        ),
    ]

    source_url: Annotated[
        str,
        Field(
            title="URL of data",
            description="URL to a CSV file of table data",
            validation_alias="sourceUrl",
        ),
    ]

    schema_url: Annotated[
        str,
        Field(
            title="URL of schema",
            description=(
                "URL to a JSON file specifying the table schema. This must"
                " be in the format expected by Qserv."
            ),
            validation_alias="schemaUrl",
        ),
    ]

    @property
    def database(self) -> str:
        """Name of the database."""
        return self.table_name.split(".", 1)[0]

    @property
    def table(self) -> str:
        """Name of the table."""
        return self.table_name.split(".", 1)[1]


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

    maxrec: Annotated[
        int | None,
        Field(
            title="Maximum records",
            description=(
                "Truncate and report overflow if the query returns more than"
                " this number of rows"
            ),
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

    result_location: Annotated[
        str | None,
        Field(
            title="User-facing location of results",
            description=(
                "Not used by the bridge, just copied into the status message"
                " sent when the job is complete"
            ),
            validation_alias="resultLocation",
        ),
    ] = None

    result_format: Annotated[
        JobResultConfig,
        Field(
            title="Format of result",
            description="Formatting instructions for writing the result",
            validation_alias="resultFormat",
        ),
    ]

    upload_tables: Annotated[
        list[JobTableUpload],
        Field(
            title="Upload tables",
            description="Temporary tables to create while running this job",
            validation_alias="uploadTables",
        ),
    ] = []

    timeout: Annotated[
        SecondsTimedelta | None,
        Field(
            title="Query timeout",
            description="Optional timeout in seconds for query execution",
        ),
    ] = None

    def to_job_metadata(self) -> JobMetadata:
        """Convert to the job metadata used in status responses."""
        return JobMetadata(query=self.query, database=self.database)


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

    result_location: Annotated[
        str | None,
        Field(
            title="User-facing URL of results",
            description="Copied from the job request, not used by the bridge",
            serialization_alias="resultLocation",
        ),
    ] = None

    format: Annotated[JobResultFormat, Field(title="Format of result")]


class JobErrorCode(StrEnum):
    """Possible error codes for failures."""

    backend_error = "backend_error"
    backend_internal_error = "backend_internal_error"
    backend_request_error = "backend_request_error"
    backend_results_too_large = "backend_results_too_large"
    backend_sql_error = "backend_sql_error"
    invalid_request = "invalid_request"
    quota_exceeded = "quota_exceeded"
    result_timeout = "result_timeout"
    upload_failed = "upload_failed"
    table_read = "table_read"


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
        str | None,
        Field(
            title="Backend execution ID",
            description="Identifier of the running query in the backend",
            serialization_alias="executionID",
        ),
    ] = None

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
        JobQueryInfo | None,
        Field(title="Query information", serialization_alias="queryInfo"),
    ] = None

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
