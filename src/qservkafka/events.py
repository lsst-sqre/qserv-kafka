"""Metrics events implementation for the Qserv Kafka bridge."""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum
from typing import override

from pydantic import Field
from safir.dependencies.metrics import EventMaker
from safir.metrics import EventManager, EventPayload

from .models.kafka import JobErrorCode

__all__ = [
    "Events",
    "QservFailureEvent",
    "QservProtocol",
    "QueryAbortEvent",
    "QueryFailureEvent",
    "QuerySuccessEvent",
    "TemporaryTableUploadEvent",
]


class QservProtocol(StrEnum):
    """Protocol of Qserv API used."""

    HTTP = "HTTP"
    SQL = "SQL"


class QservFailureEvent(EventPayload):
    """Unexpected failure sending a Qserv API request.

    This event will be logged for each low-level API failure (either HTTP or
    SQL).
    """

    protocol: QservProtocol = Field(..., title="Protocol of Qserv API")


class BaseQueryEvent(EventPayload):
    """Common fields in all query events."""

    job_id: str = Field(
        title="UWS job ID",
        description="Identifier of job in the TAP server's UWS database",
    )

    username: str = Field(
        ..., title="Username", description="Username of authenticated user"
    )


class QuerySuccessEvent(BaseQueryEvent):
    """Successful end-to-end completion of a query."""

    elapsed: timedelta = Field(
        ...,
        title="Query time",
        description=(
            "Time elapsed from initial receipt of the Kafka request to upload"
            " of results and sending of the completion Kafka message"
        ),
    )

    kafka_elapsed: timedelta | None = Field(
        None,
        title="Kafka processing delay",
        description="Time from Kafka message queuing to start of processing",
    )

    qserv_elapsed: timedelta = Field(
        ...,
        title="Qserv processing time",
        description="How long it took for Qserv to process the query",
    )

    result_elapsed: timedelta = Field(
        ...,
        title="Result processing time",
        description=(
            "How long it took to retrieve, encode, and upload the results"
        ),
    )

    submit_elapsed: timedelta = Field(
        ...,
        title="Job submission time",
        description=(
            "How long it took from receipt of the Kafka message to successful"
            " creation of the query job in Qserv"
        ),
    )

    delete_elapsed: timedelta | None = Field(
        None,
        title="Job deletion time",
        description=(
            "How long it took to delete the query from Qserv after successful"
            " completion"
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
        description="Encoded data size, after base64 encoding, in bytes",
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

    elapsed: timedelta = Field(
        ...,
        title="Upload time (seconds)",
        description=(
            "Time required to upload the table to Qserv. This does not include"
            " the time required to read the table from GCS."
        ),
    )


class Events(EventMaker):
    """Event publishers for all possible events, used by workers and frontend.

    Attributes
    ----------
    qserv_failure
        Qserv API call failed with a protocol error.
    query_success
        Successful query execution.
    query_failure
        Failed query.
    query_abort
        Aborted query.
    """

    @override
    async def initialize(self, manager: EventManager) -> None:
        self.qserv_failure = await manager.create_publisher(
            "qserv_failure", QservFailureEvent
        )
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
