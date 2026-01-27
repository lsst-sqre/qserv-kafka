"""Models for tracking the state of running queries."""

from datetime import UTC, datetime
from typing import Annotated, Any, Self, override

from pydantic import BaseModel, Field
from safir.datetime import format_datetime_for_logging

from .kafka import JobQueryInfo, JobRun
from .query import QueryStatus

__all__ = [
    "Query",
    "RunningQuery",
]


class Query(BaseModel):
    """Represents a started query with no backend status."""

    query_id: Annotated[str, Field(title="ID of query")]

    queued: Annotated[
        datetime | None, Field(title="Kafka queue time of query")
    ] = None

    start: Annotated[datetime, Field(title="Receipt time of query")]

    created: Annotated[datetime, Field(title="Creation time of query")]

    job: Annotated[JobRun, Field(title="Full job request")]

    def to_logging_context(self) -> dict[str, str | float]:
        """Convert to variables for a structlog logging context."""
        result: dict[str, str | float] = {
            "job_id": self.job.job_id,
            "backend_id": self.query_id,
            "username": self.job.owner,
            "start_time": format_datetime_for_logging(self.start),
        }
        if self.queued:
            result["queued"] = format_datetime_for_logging(self.queued)
        return result


class RunningQuery(Query):
    """Represents a running query with a known status."""

    status: Annotated[QueryStatus, Field(title="Last known status")]

    result_queued: Annotated[
        bool, Field(title="Whether queued for result procesing")
    ]

    @classmethod
    def from_query(cls, query: Query, status: QueryStatus) -> Self:
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

    @override
    def to_logging_context(self) -> dict[str, Any]:
        result = super().to_logging_context()
        result.update(self.status.to_logging_context())
        if self.status.collected_bytes:
            result["backend_size"] = self.status.collected_bytes
        return result
