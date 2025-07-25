"""Models for tracking the state of running queries."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Self

from pydantic import BaseModel, Field

from .kafka import JobRun
from .qserv import AsyncQueryStatus

__all__ = [
    "Query",
    "RunningQuery",
]


class Query(BaseModel):
    """Represents a started Qserv query with no Qserv status."""

    query_id: Annotated[int, Field(title="Qserv ID of query")]

    start: Annotated[datetime, Field(title="Receipt time of query")]

    job: Annotated[JobRun, Field(title="Full job request")]


class RunningQuery(Query):
    """Represents a running Qserv query with a known status."""

    status: Annotated[AsyncQueryStatus, Field(title="Last known status")]

    result_queued: Annotated[
        bool, Field(title="Whether queued for result procesing")
    ]

    @classmethod
    def from_query(cls, query: Query, status: AsyncQueryStatus) -> Self:
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
            start=query.start,
            job=query.job,
            status=status,
            result_queued=False,
        )
