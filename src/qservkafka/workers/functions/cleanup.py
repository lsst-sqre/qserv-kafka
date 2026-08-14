"""arq queue worker to delete query data."""

from typing import Any

from ...factory import Factory
from ...models.state import StartedQuery

__all__ = ["cleanup_query"]


async def cleanup_query(ctx: dict[Any, Any], query: StartedQuery) -> None:
    """Clean up the data associated with a query.

    Parameters
    ----------
    ctx
        arq context.
    query
        Query to clean up.
    """
    factory: Factory = ctx["factory"]

    query_service = factory.create_query_service()
    await query_service.delete_query_data(query)
