"""Helpers for publishing periodic metrics."""

from safir.metrics import ArqEvents, publish_queue_stats

from .config import config


async def publish_arq_metrics() -> None:
    """Publish metrics, meant to be executed periodically."""
    manager = config.metrics.make_manager()
    try:
        await manager.initialize()
        arq_events = ArqEvents()
        await arq_events.initialize(manager)
        await publish_queue_stats(
            queue=config.arq_queue,
            arq_events=arq_events,
            redis_settings=config.arq_redis_settings,
        )
    finally:
        await manager.aclose()
