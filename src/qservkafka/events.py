"""Metrics events implementation for the Qserv Kafka bridge."""

from __future__ import annotations

from datetime import timedelta

from pydantic import Field
from safir.dependencies.metrics import EventMaker
from safir.metrics import EventManager, EventPayload

from .models.kafka import JobErrorCode

__all__ = [
    "Events",
    "QueryAbortEvent",
    "QueryFailureEvent",
    "QuerySuccessEvent",
    "TemporaryTableUploadEvent",
]


class BaseQueryEvent(EventPayload):
    """Common fields in all query events."""

    username: str = Field(
        ..., title="Username", description="Username of authenticated user"
    )


class QuerySuccessEvent(BaseQueryEvent):
    """Successful end-to-end completion of a query."""

    elapsed: timedelta = Field(
        ...,
        title="Query time (seconds)",
        description=(
            "Time elapsed from initial receipt of the Kafka request to upload"
            " of results and sending of the completion Kafka message"
        ),
    )

    qserv_elapsed: timedelta = Field(
        ...,
        title="Qserv processing time (seconds)",
        description="How long it took for Qserv to process the query",
    )

    result_elapsed: timedelta = Field(
        ...,
        title="Result processing time (seconds)",
        description=(
            "How long it took to retrieve, encode, and upload the results"
        ),
    )

    rows: int = Field(
        ..., title="Row count", description="Number of rows in the output"
    )

    qserv_size: int = Field(
        ...,
        title="Data size in Qserv",
        description="Reported result size from Qserv in bytes",
    )

    encoded_size: int = Field(
        ...,
        title="Encoded data size",
        description="Encoded data size in bytes",
    )

    result_size: int = Field(
        ...,
        title="Result size",
        description="Total size of result VOTable including XML wrapper",
    )

    rate: float = Field(
        ...,
        title="Query rate",
        description="Encoded data bytes per second for the whole query",
    )

    qserv_rate: float | None = Field(
        ...,
        title="Qserv result rate",
        description=(
            "Qserv data bytes per second for Qserv query, or null if the"
            " query completed too quickly to determine a meaningful rate"
        ),
    )

    result_rate: float = Field(
        ...,
        title="Processing rate",
        description="Encoded data bytes per second for result processing",
    )

    upload_tables: int = Field(
        ...,
        title="Count of uploaded tables",
        description="Number of uploaded tables provided as part of the query",
    )

    immediate: bool = Field(
        False,
        title="Query finished immediately",
        description=(
            "Whether the query finished so quickly that it was processed"
            " entirely by the frontend"
        ),
    )


class QueryAbortEvent(BaseQueryEvent):
    """Query aborted (by user or by administrator)."""

    elapsed: timedelta = Field(
        ...,
        title="Query time (seconds)",
        description=(
            "Time elapsed from initial receipt of the Kafka request for the"
            " query to completion of the aborting of the query"
        ),
    )


class QueryFailureEvent(BaseQueryEvent):
    """Query failed for some reason.

    This does not include Kafka messages rejected for syntax errors, only
    queries that got as far as attempting to start the query in Qserv.
    """

    error: JobErrorCode = Field(..., title="Error code")

    elapsed: timedelta = Field(
        ...,
        title="Query time (seconds)",
        description=(
            "Time elapsed from initial receipt of the Kafka request for the"
            " query to the eventual failure of the query"
        ),
    )


class TemporaryTableUploadEvent(BaseQueryEvent):
    """Table uploaded for a query."""

    size: int = Field(
        ...,
        title="Size of table",
        description="Size of the CSV file holding the table data",
    )


class Events(EventMaker):
    """Event publishers for all possible events, used by workers and frontend.

    Attributes
    ----------
    query_success
        Successful query execution.
    query_failure
        Failed query.
    query_abort
        Aborted query.
    """

    async def initialize(self, manager: EventManager) -> None:
        self.query_success = await manager.create_publisher(
            "query_success", QuerySuccessEvent
        )
        self.query_failure = await manager.create_publisher(
            "query_failure", QueryFailureEvent
        )
        self.query_abort = await manager.create_publisher(
            "query_abort", QueryAbortEvent
        )
        self.temporary_table = await manager.create_publisher(
            "temporary_table", TemporaryTableUploadEvent
        )
