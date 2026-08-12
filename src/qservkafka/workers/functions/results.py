"""arq queue worker to process completed queries."""

from typing import Any

from structlog.stdlib import BoundLogger

from ...factory import Factory


async def handle_finished_query(ctx: dict[Any, Any], query_id: str) -> None:
    """Process a completed query.

    Parameters
    ----------
    ctx
        arq context.
    query_id
        Backend query ID of completed query.
    """
    factory: Factory = ctx["factory"]
    logger: BoundLogger = ctx["logger"]
    state = factory.create_query_state_store()

    query = await state.get_query(query_id)
    if not query:
        logger.warning("Query state not found, skipping", query_id=query_id)
        return
    processor = factory.create_result_processor()
    status = await processor.process_query(query)
    await processor.publish_status(status)
    await processor.delete_query_data(query)
