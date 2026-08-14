"""Configuration for arq queue workers."""

import uuid
from collections.abc import Callable
from typing import Any, ClassVar

from arq import func
from arq.worker import Function
from safir.logging import configure_logging
from safir.metrics.arq import initialize_arq_metrics, make_on_job_start
from safir.sentry import initialize_sentry
from sentry_sdk.integrations.logging import LoggingIntegration
from structlog.stdlib import get_logger

from .. import __version__
from ..config import config
from ..factory import ProcessContext, build_process_context
from .functions.cleanup import cleanup_query
from .functions.results import finish_query
from .functions.upload import start_query

__all__ = ["FastWorkerSettings", "SlowWorkerSettings"]


async def startup(ctx: dict[Any, Any]) -> None:
    """Set up the shared context for the worker functions.

    Parameters
    ----------
    ctx
        Worker context.
    """
    initialize_sentry(
        release=__version__, disabled_integrations=[LoggingIntegration]
    )
    configure_logging(
        profile=config.log_profile,
        log_level=config.log_level,
        name="qservkafka",
        add_timestamp=True,
    )
    logger = get_logger("qservkafka").bind(worker_instance=uuid.uuid4().hex)

    # Allow the test suite to override the process context to, for example,
    # provide mock metrics event publishers that are accessible to the test.
    context = ctx.get("context")
    if not context:
        context = await build_process_context(worker_max_jobs=ctx["max_jobs"])
        await context.connect()
    factory = context.build_factory(logger)

    # Metrics initialization must be done exactly once. If not done at all,
    # the on_job_start function fails; if done more than once, Safir's metrics
    # manager complains about duplicate registered metrics. This is not an
    # issue during normal arq operations, but the test suite runs the worker
    # multiple times in burst mode, which calls on_startup each time.
    if not ctx.get("metrics_initialized"):
        await initialize_arq_metrics(context.event_manager, ctx)
        ctx["metrics_initialized"] = True

    ctx["context"] = context
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


class FastWorkerSettings:
    """Configuration for the arq worker for I/O-intensive jobs.

    Runs on a separate queue so that a slow result upload can't starve user
    table uploads or cleanup jobs.
    """

    functions: ClassVar[list[Callable | Function]] = [
        func(cleanup_query, timeout=config.api_worker_timeout),
        finish_query,
        func(start_query, timeout=config.upload_worker_timeout),
    ]
    queue_name = config.arq_fast_queue
    redis_settings = config.arq_redis_settings
    on_startup = startup
    on_shutdown = shutdown
    on_job_start = make_on_job_start(config.arq_fast_queue)
    job_completion_wait = int(config.result_worker_timeout.total_seconds())
    max_jobs = config.arq_fast_max_jobs
    job_timeout = config.result_worker_timeout
    ctx: ClassVar[dict[str, int]] = {"max_jobs": config.arq_fast_max_jobs}


class SlowWorkerSettings:
    """Configuration for the arq worker for CPU-intensive jobs."""

    functions: ClassVar[list[Callable]] = [finish_query]
    queue_name = config.arq_slow_queue
    redis_settings = config.arq_redis_settings
    on_startup = startup
    on_shutdown = shutdown
    on_job_start = make_on_job_start(config.arq_slow_queue)
    job_completion_wait = int(config.result_worker_timeout.total_seconds())
    max_jobs = config.arq_slow_max_jobs
    job_timeout = config.result_worker_timeout
    ctx: ClassVar[dict[str, int]] = {"max_jobs": config.arq_slow_max_jobs}
