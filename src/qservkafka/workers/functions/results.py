"""arq queue worker to process completed queries."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import async_scoped_session

from ...factory import Factory


async def handle_finished_query(ctx: dict[Any, Any], query_id: int) -> None:
    """Process a completed query.

    Parameters
    ----------
    ctx
        arq context.
    query_id
        Qserv query ID of completed query.
    """
    factory: Factory = ctx["factory"]
    session: async_scoped_session = ctx["session"]
    state = factory.query_state_store

    query = await state.get_query(query_id)
    if not query:
        return
    processor = factory.create_result_processor()
    try:
        status = await processor.build_query_status(query_id, query.job)
    finally:
        await session.remove()
    await processor.publish_status(status)
