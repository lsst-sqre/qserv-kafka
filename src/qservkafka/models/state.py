"""Models for tracking the state of running queries."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field

from .kafka import JobRun
from .qserv import AsyncQueryStatus

__all__ = ["Query"]


class Query(BaseModel):
    """Represents a running Qserv query."""

    query_id: Annotated[int, Field(title="Qserv ID of query")]

    start: Annotated[datetime | None, Field(title="Receipt time of query")] = (
        None
    )

    job: Annotated[JobRun, Field(title="Full job request")]

    status: Annotated[
        AsyncQueryStatus | None, Field(title="Last known status")
    ]

    result_queued: Annotated[
        bool, Field(title="Whether queued for result procesing")
    ]
