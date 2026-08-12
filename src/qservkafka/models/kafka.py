"""Models for Kafka messages.

The models starting with ``Job`` are used to serialize and deserialize Kafka
messages. Internally, they are converted or wrapped in models starting with
``Query``.
"""

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, Self, override

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Discriminator,
    Field,
    HttpUrl,
    PlainSerializer,
    Tag,
)
from safir.pydantic import SecondsTimedelta
from vo_models.uws.types import ExecutionPhase

from .progress import ProgressMetrics
from .query import QueryStatus
from .votable import VOTableArraySize, VOTablePrimitive

type DatetimeMillis = Annotated[
    datetime,
    BeforeValidator(
        lambda t: (
            t
            if not isinstance(t, float)
            else datetime.fromtimestamp(t / 1000, tz=UTC)
        )
    ),
    PlainSerializer(lambda t: int(t.timestamp() * 1000), return_type=int),
]
"""Type for timestamps, which are represented in Kafka in milliseconds."""

__all__ = [
    "DependentTableUpload",
    "DirectorTableUpload",
    "JobCancel",
    "JobError",
    "JobErrorCode",
    "JobMetadata",
    "JobQueryInfo",
    "JobResultColumnType",
    "JobResultConfig",
    "JobResultEnvelope",
    "JobResultFormat",
    "JobResultInfo",
    "JobResultSerialization",
    "JobResultType",
    "JobRun",
    "JobStatus",
    "JobTableUpload",
    "ReplicatedTableUpload",
    "UploadTableBase",
    "UploadTablePartitionType",
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

    def to_logging_context(self) -> dict[str, Any]:
        """Convert job status details to a logging context."""
        return {
            "job_id": self.job_id,
            "username": self.owner,
            "backend_id": self.execution_id,
        }


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
    Parquet = "Parquet"


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
        JobResultSerialization | None,
        Field(
            title="Serialization of result",
            description="Serialization format of the result "
            "(only applies to VOTable output)",
        ),
    ] = None


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
                "If true, this column contains a URL to the DataLink service"
                " that needs to be rewritten based on service discovery"
            ),
            validation_alias="requiresUrlRewrite",
        ),
    ] = False

    @property
    def type_description(self) -> str:
        """Data type as a human-readable string."""
        result = self.datatype.name
        if self.arraysize:
            result += "(" + self.arraysize.to_string() + ")"
        return result

    def is_string(self) -> bool:
        """Check whether the underlying data type is a string."""
        return self.datatype.is_string()

    def is_array(self) -> bool | None:
        """Determine if the column is an array.

        Returns
        -------
        bool
            True if the column is an array otherwise False.
        """
        if not self.arraysize:
            return False
        if self.arraysize.variable:
            return True
        return self.arraysize.limit is not None and self.arraysize.limit > 1


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


class UploadTablePartitionType(StrEnum):
    """Partition strategy for a TAP_UPLOAD table in Qserv."""

    REPLICATED = "replicated"
    DIRECTOR = "director"
    DEPENDENT = "dependent"


def _partition_type_discriminator(v: Any) -> str:
    """Union discriminator for upload table partitioning type."""
    if isinstance(v, dict):
        val = v.get("partitionType", v.get("partition_type"))
    else:
        val = getattr(v, "partition_type", None)
    if val is None:
        return UploadTablePartitionType.REPLICATED
    return str(val)


class UploadTableBase(BaseModel, ABC):
    """Common fields for all upload table classes."""

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

    @abstractmethod
    def to_ingest_fields(self) -> dict[str, str | int]:
        """Qserv ingest API fields specific to this partition strategy."""


class ReplicatedTableUpload(UploadTableBase):
    """Upload table fully replicated across all Qserv nodes."""

    partition_type: Annotated[
        Literal[UploadTablePartitionType.REPLICATED] | None,
        Field(validation_alias="partitionType"),
    ] = None

    @override
    def to_ingest_fields(self) -> dict[str, str | int]:
        return {}


class DirectorTableUpload(UploadTableBase):
    """Upload table spatially partitioned by RA/Dec (Qserv director table)."""

    partition_type: Annotated[
        Literal[UploadTablePartitionType.DIRECTOR],
        Field(validation_alias="partitionType"),
    ]

    longitude_col_name: Annotated[
        str,
        Field(
            title="Longitude column name", validation_alias="longitudeColName"
        ),
    ]

    latitude_col_name: Annotated[
        str,
        Field(
            title="Latitude column name", validation_alias="latitudeColName"
        ),
    ]

    @override
    def to_ingest_fields(self) -> dict[str, str | int]:
        return {
            "is_partitioned": 1,
            "is_director": 1,
            "longitude_col_name": self.longitude_col_name,
            "latitude_col_name": self.latitude_col_name,
        }


class DependentTableUpload(UploadTableBase):
    """Upload table partitioned by FK reference to a director table."""

    partition_type: Annotated[
        Literal[UploadTablePartitionType.DEPENDENT],
        Field(validation_alias="partitionType"),
    ]

    id_col_name: Annotated[
        str,
        Field(title="ID column name", validation_alias="idColName"),
    ]

    ref_director_database: Annotated[
        str,
        Field(
            title="Reference director database",
            validation_alias="refDirectorDatabase",
        ),
    ]

    ref_director_table: Annotated[
        str,
        Field(
            title="Reference director table",
            validation_alias="refDirectorTable",
        ),
    ]

    ref_director_id_col_name: Annotated[
        str,
        Field(
            title="Reference director ID column name",
            validation_alias="refDirectorIdColName",
        ),
    ]

    @override
    def to_ingest_fields(self) -> dict[str, str | int]:
        return {
            "is_partitioned": 1,
            "is_director": 0,
            "id_col_name": self.id_col_name,
            "ref_director_database": self.ref_director_database,
            "ref_director_table": self.ref_director_table,
            "ref_director_id_col_name": self.ref_director_id_col_name,
        }


JobTableUpload = Annotated[
    Annotated[ReplicatedTableUpload, Tag(UploadTablePartitionType.REPLICATED)]
    | Annotated[DirectorTableUpload, Tag(UploadTablePartitionType.DIRECTOR)]
    | Annotated[DependentTableUpload, Tag(UploadTablePartitionType.DEPENDENT)],
    Discriminator(_partition_type_discriminator),
]


class JobMetadata(BaseModel):
    """Metadata about a query."""

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

    def to_logging_context(self) -> dict[str, Any]:
        """Convert job status details to a logging context."""
        metadata = self.to_job_metadata()
        return {
            "job_id": self.job_id,
            "username": self.owner,
            "query": metadata.model_dump(mode="json", exclude_none=True),
        }

    def to_job_metadata(self) -> JobMetadata:
        """Convert to the job metadata used in status responses."""
        return JobMetadata(query=self.query, database=self.database)


class JobQueryInfo(BaseModel):
    """Information about the status of an executing query."""

    model_config = ConfigDict(serialize_by_alias=True, validate_by_name=True)

    start_time: Annotated[
        DatetimeMillis,
        Field(
            title="Start time",
            description="When the job started executing",
            serialization_alias="startTime",
            validation_alias="startTime",
        ),
    ]

    end_time: Annotated[
        DatetimeMillis | None,
        Field(
            title="Completion time",
            description="When the job completed",
            serialization_alias="endTime",
            validation_alias="endTime",
        ),
    ] = None

    progress: Annotated[
        ProgressMetrics | None,
        Field(
            title="Query progress",
            description="Backend-specific progress information",
        ),
    ] = None


class JobResultInfo(BaseModel):
    """Result of a query."""

    model_config = ConfigDict(serialize_by_alias=True, validate_by_name=True)

    total_rows: Annotated[
        int,
        Field(
            title="Output rows",
            description="Total number of rows in the result",
            serialization_alias="totalRows",
            validation_alias="totalRows",
        ),
    ]

    result_location: Annotated[
        str | None,
        Field(
            title="User-facing URL of results",
            description="Copied from the job request, not used by the bridge",
            serialization_alias="resultLocation",
            validation_alias="resultLocation",
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
    encoding_error = "encoding_error"
    invalid_request = "invalid_request"
    quota_exceeded = "quota_exceeded"
    result_timeout = "result_timeout"
    upload_failed = "upload_failed"
    table_read = "table_read"


class JobError(BaseModel):
    """Error from a query."""

    model_config = ConfigDict(serialize_by_alias=True, validate_by_name=True)

    code: Annotated[
        JobErrorCode,
        Field(
            title="Error code",
            serialization_alias="errorCode",
            validation_alias="errorCode",
        ),
    ]

    message: Annotated[
        str,
        Field(
            title="Error message",
            description="Human-readable error message",
            serialization_alias="errorMessage",
            validation_alias="errorMessage",
        ),
    ]

    @classmethod
    def for_quota_exceeded(cls, used: int, quota: int) -> Self:
        """Generate an error object for a quota exceeded error.

        Parameters
        ----------
        used
            Number of running queries.
        quota
            Maximum allowed number of concurrent running queries.

        Returns
        -------
        JobError
            Corresponding error component of a Kafka job status message.
        """
        error = (
            f"Maximum running queries reached ({used} running, maximum"
            f" {quota}); wait for a query to finish or cancel one of your"
            " running queries"
        )
        return cls(code=JobErrorCode.quota_exceeded, message=error)


class JobStatus(BaseModel):
    """Status of a TAP query."""

    model_config = ConfigDict(serialize_by_alias=True, validate_by_name=True)

    job_id: Annotated[
        str,
        Field(
            title="UWS job ID",
            description="Identifier of job in the TAP server's UWS database",
            serialization_alias="jobID",
            validation_alias="jobID",
        ),
    ]

    execution_id: Annotated[
        str | None,
        Field(
            title="Backend execution ID",
            description="Identifier of the running query in the backend",
            serialization_alias="executionID",
            validation_alias="executionID",
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
        Field(
            title="Query information",
            serialization_alias="queryInfo",
            validation_alias="queryInfo",
        ),
    ] = None

    result_info: Annotated[
        JobResultInfo | None,
        Field(
            title="Job result",
            description="Result of the job if it has completed",
            serialization_alias="resultInfo",
            validation_alias="resultInfo",
        ),
    ] = None

    error: Annotated[
        JobError | None,
        Field(
            title="Job error",
            description="Error for the job if the job failed",
            serialization_alias="errorInfo",
            validation_alias="errorInfo",
        ),
    ] = None

    metadata: Annotated[JobMetadata, Field(title="Job metadata")]

    @classmethod
    def from_abort(
        cls, job: JobRun, status: QueryStatus, start: datetime
    ) -> Self:
        """Construct from an underlying `QueryStatus`.

        Parameters
        ----------
        job
            Job for which to create a status message.
        status
            Underlying query status.
        start
            Start time of the query.

        Returns
        -------
        JobStatus
            Job status message for Kafka.
        """
        return cls(
            job_id=job.job_id,
            execution_id=status.query_id,
            timestamp=status.last_update or datetime.now(tz=UTC),
            status=ExecutionPhase.EXECUTING,
            query_info=JobQueryInfo(
                start_time=start, progress=status.progress
            ),
            metadata=job.to_job_metadata(),
        )

    @classmethod
    def from_error(
        cls, job: JobRun, error: JobError, execution_id: str | None = None
    ) -> Self:
        """Construct from an underlying `JobError`.

        Parameters
        ----------
        job
            Job for which to create a status message.
        error
            Error to report.
        execution_id
            Backend execution ID, if one is known.

        Returns
        -------
        JobStatus
            Job status message for Kafka.
        """
        return cls(
            job_id=job.job_id,
            execution_id=execution_id,
            timestamp=datetime.now(tz=UTC),
            status=ExecutionPhase.ERROR,
            error=error,
            metadata=job.to_job_metadata(),
        )
