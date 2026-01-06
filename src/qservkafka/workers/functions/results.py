"""arq queue worker to process completed queries."""

from typing import Any

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
    logger: BoundLogger = ctx["logger"]
    state = factory.create_query_state_store()

    query = await state.get_query(query_id)
    if not query:
        return
    processor = factory.create_result_processor()
    status = await processor.build_query_status(query)
    if status.status == ExecutionPhase.EXECUTING:
        logger.warning(
            "Apparently completed job still executing",
            job_id=query.job.job_id,
            qserv_id=str(query.query_id),
            username=query.job.owner,
            status=status.model_dump(mode="json", exclude_none=True),
        )
    await processor.publish_status(status)
