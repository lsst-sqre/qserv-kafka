"""Configuration for arq queue workers."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any, ClassVar

from safir.database import create_async_session
from safir.logging import configure_logging
from safir.metrics import make_on_job_start
from structlog import get_logger

from ..config import config
from ..constants import ARQ_TIMEOUT_GRACE
from ..factory import Factory, ProcessContext
from .functions.results import handle_finished_query


async def startup(ctx: dict[Any, Any]) -> None:
    """Set up the shared context for the worker functions.

    Parameters
    ----------
    ctx
        Worker context.
    """
    configure_logging(
        profile=config.profile,
        log_level=config.log_level,
        name="qservkafka",
        add_timestamp=True,
    )
    logger = get_logger("qservkafka").bind(worker_instance=uuid.uuid4().hex)

    # Allow the test suite to override the process context to, for example,
    # provide mock metrics event publishers that are accessible to the test.
    if "context" in ctx:
        context = ctx["context"]
    else:
        context = await ProcessContext.create()
    session = await create_async_session(context.engine)
    factory = Factory(context, session, logger)

    ctx.update(context.arq_context_additions)
    ctx["context"] = context
    ctx["session"] = session
    ctx["factory"] = factory
    ctx["logger"] = logger


async def shutdown(ctx: dict[Any, Any]) -> None:
    """Shut down the shared context for worker functions.

    Parameters
    ----------
    ctx
        Worker context.
    """
    context: ProcessContext = ctx["context"]
    await context.aclose()


class WorkerSettings:
    """Configuration for the arq worker."""

    functions: ClassVar[list[Callable]] = [handle_finished_query]
    job_completion_wait = int(
        (config.result_timeout + ARQ_TIMEOUT_GRACE).total_seconds()
    )
    job_timeout = config.result_timeout + ARQ_TIMEOUT_GRACE
    max_jobs = config.max_worker_jobs
    on_startup = startup
    on_shutdown = shutdown
    on_job_start = make_on_job_start(config.arq_queue)
    redis_settings = config.arq_redis_settings
