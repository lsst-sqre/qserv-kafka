"""Models for tracking the state of running queries."""

from datetime import UTC, datetime
from typing import Annotated, Any, Self, override

from pydantic import BaseModel, Field, model_validator
from safir.datetime import format_datetime_for_logging
from vo_models.uws.types import ExecutionPhase

from .kafka import JobError, JobQueryInfo, JobResultInfo, JobRun, JobStatus
from .query import AsyncQueryPhase, QueryStatus
from .votable import UploadStats

__all__ = [
    "Query",
    "RunningQuery",
    "StartedQuery",
]


class Query(BaseModel):
    """Represents a started query with no backend status."""

    queued: Annotated[
        datetime | None, Field(title="Kafka queue time of query")
    ] = None

    start: Annotated[datetime, Field(title="Receipt time of query")]

    job: Annotated[JobRun, Field(title="Full job request")]

    def to_logging_context(self) -> dict[str, str | float]:
        """Convert to variables for a structlog logging context."""
        result = self.job.to_logging_context()
        if self.queued:
            result["queued"] = format_datetime_for_logging(self.queued)
        result["start_time"] = format_datetime_for_logging(self.start)
        return result


class StartedQuery(Query):
    """Represents a query that has been dispatched to the backend."""

    query_id: Annotated[str, Field(title="ID of query")]

    created: Annotated[datetime, Field(title="Creation time of query")]

    @classmethod
    def from_query(cls, query: Query, query_id: str) -> Self:
        """Convert a new query to a started query.

        Parameters
        ----------
        query
            New query.
        query_id
            Backend query ID.

        Returns
        -------
        StartedQuery
            Query state.
        """
        return cls(
            query_id=query_id,
            queued=query.queued,
            start=query.start,
            created=datetime.now(tz=UTC),
            job=query.job,
        )

    def has_results(self) -> bool:
        """Whether this query has results that may need to be deleted."""
        return False

    @override
    def to_logging_context(self) -> dict[str, str | float]:
        results = super().to_logging_context()
        results["backend_id"] = self.query_id
        return results


class RunningQuery(StartedQuery):
    """Represents a running query with a known status."""

    status: Annotated[QueryStatus, Field(title="Last known status")]

    result_queued: Annotated[
        bool, Field(title="Whether queued for result procesing")
    ]

    @model_validator(mode="before")
    @classmethod
    def _migrate_old_schema(cls, data: Any) -> Any:
        """Accept data stored by versions prior to 4.3.0.
        The backend abstractions changed in 4.3.0, so the query_id and status
        fields may be in different formats.
        This validator converts the old formats to the new ones.
        """
        if not isinstance(data, dict):
            return data
        if isinstance(data.get("query_id"), int):
            data["query_id"] = str(data["query_id"])
        status = data.get("status")
        if isinstance(status, dict) and "backend_type" not in status:
            status["backend_type"] = "Qserv"
            if isinstance(status.get("query_id"), int):
                status["query_id"] = str(status["query_id"])
            total = status.pop("total_chunks", 0) or 0
            completed = status.pop("completed_chunks", 0) or 0
            status["chunk_progress"] = {
                "total_chunks": total,
                "completed_chunks": completed,
            }
        return data

    @classmethod
    def from_started_query(
        cls, query: StartedQuery, status: QueryStatus
    ) -> Self:
        """Convert a started query to full query state by recording status.

        Parameters
        ----------
        query
            Query with no status.
        status
            Initial status of query.

        Returns
        -------
        Query
            Query state.
        """
        return cls(
            query_id=query.query_id,
            queued=query.queued,
            start=query.start,
            created=query.created,
            job=query.job,
            status=status,
            result_queued=False,
        )

    @override
    def has_results(self) -> bool:
        """Whether this query has results that may need to be deleted."""
        return self.status.status == AsyncQueryPhase.COMPLETED

    def to_completed_job_status(self, stats: UploadStats) -> JobStatus:
        """Construct a Kafka job status message for a completed query."""
        return JobStatus(
            job_id=self.job.job_id,
            execution_id=self.query_id,
            timestamp=datetime.now(tz=UTC),
            status=ExecutionPhase.COMPLETED,
            query_info=self.to_job_query_info(finished=True),
            result_info=JobResultInfo(
                total_rows=stats.rows,
                result_location=self.job.result_location,
                format=self.job.result_format.format,
            ),
            metadata=self.job.to_job_metadata(),
        )

    def to_job_query_info(self, *, finished: bool = False) -> JobQueryInfo:
        """Build job query information based on query status.

        Parameters
        ----------
        finished
            Whether the query is finished and therefore the end time should be
            set to now.

        Returns
        -------
        JobQueryInfo
            Corresponding query information.
        """
        return JobQueryInfo(
            start_time=self.start,
            progress=self.status.progress,
            end_time=datetime.now(tz=UTC) if finished else None,
        )

    def to_job_status(
        self,
        status: ExecutionPhase = ExecutionPhase.EXECUTING,
        error: JobError | None = None,
    ) -> JobStatus:
        """Construct a Kafka job status message from the query."""
        finished = status != ExecutionPhase.EXECUTING
        return JobStatus(
            job_id=self.job.job_id,
            execution_id=self.query_id,
            timestamp=self.status.last_update or datetime.now(tz=UTC),
            status=status,
            query_info=self.to_job_query_info(finished=finished),
            error=error,
            metadata=self.job.to_job_metadata(),
        )

    @override
    def to_logging_context(self) -> dict[str, Any]:
        result = super().to_logging_context()
        result.update(self.status.to_logging_context())
        return result
