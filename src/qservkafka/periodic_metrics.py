"""Function to publish metrics, meant to be called periodically."""

import asyncio

from safir.metrics import publish_queue_stats

from qservkafka.factory import ProcessContext

from .config import config


def publish_periodic_metrics() -> None:
    """Publish queue statistics events.

    This should be run on a schedule.
    """

    async def publish() -> None:
        context = await ProcessContext.create()
        try:
            arq_events = context.arq_events
            await publish_queue_stats(
                queue=config.arq_queue,
                arq_events=arq_events,
                redis_settings=config.arq_redis_settings,
            )
        finally:
            await context.aclose()

    asyncio.run(publish())
