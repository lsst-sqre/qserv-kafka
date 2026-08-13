"""arq queue worker to start queries that include table uploads."""

from typing import Any

from ...factory import Factory
from ...models.state import Query

__all__ = ["start_query"]


async def start_query(ctx: dict[Any, Any], query: Query) -> None:
    """Start a query that includes table uploads.

    Parameters
    ----------
    ctx
        arq context.
    job
        Query job to start.
    kafka_start
        Time at which the Kafka message for the job was queued.
    """
    factory: Factory = ctx["factory"]

    query_service = factory.create_query_service()
    await query_service.start_query(query)
