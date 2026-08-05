"""arq queue worker to process completed queries."""

from typing import Any

from arq import Retry
from safir.sentry import report_exception
from structlog.stdlib import BoundLogger
from vo_models.uws.types import ExecutionPhase

from ...config import config
from ...factory import Factory


async def handle_finished_query(ctx: dict[Any, Any], query_id: str) -> None:
    """Process a completed query.

    Publishes the final status to Kafka as soon as the query is done,
    before cleaning up the backend resources. This way users don't have
    to wait for that cleanup to get their results. If cleanup fails, the job
    retries via arq. On retry the query's state (``result_published``)
    tells us to skip straight to cleanup.

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

    needs_result_cleanup = query.needs_result_cleanup
    if not query.result_published:
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
            return
        await processor.publish_status(status)
        needs_result_cleanup = status.status == ExecutionPhase.COMPLETED
        await state.mark_published(
            query.query_id, needs_result_cleanup=needs_result_cleanup
        )

    try:
        await processor.cleanup_query(
            query, logger, needs_result_cleanup=needs_result_cleanup
        )
    except Exception as e:
        await report_exception(e, slack_client=factory.slack_client)
        logger.exception("Cleanup failed for finished query, will retry")
        raise Retry(defer=config.backend_retry_delay) from e
