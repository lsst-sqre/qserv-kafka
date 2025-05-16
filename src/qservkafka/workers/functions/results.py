"""arq queue worker to process completed queries."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import async_scoped_session
from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

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
    logger: BoundLogger = ctx["logger"]
    state = factory.query_state_store

    query = await state.get_query(query_id)
    if not query:
        return
    processor = factory.create_result_processor()
    try:
        status = await processor.build_query_status(
            query_id, query.job, query.start or datetime.now(tz=UTC)
        )
    finally:
        await session.remove()
    if status.status == ExecutionPhase.EXECUTING:
        logger.warning(
            "Apparently completed job still executing",
            job_id=query.job.job_id,
            qserv_id=str(query_id),
            username=query.job.owner,
            status=status.model_dump(mode="json", exclude_none=True),
        )
    await processor.publish_status(status)
