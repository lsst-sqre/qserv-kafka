"""Helpers for publishing periodic metrics."""

from safir.metrics.arq import ArqEvents, publish_queue_stats

from .config import config

__all__ = ["publish_arq_metrics"]


async def publish_arq_metrics() -> None:
    """Publish metrics, meant to be executed periodically."""
    manager = config.metrics.make_manager()
    try:
        await manager.initialize()
        arq_events = ArqEvents()
        settings = config.arq_redis_settings
        await arq_events.initialize(manager)
        await publish_queue_stats(config.arq_slow_queue, settings, arq_events)
        await publish_queue_stats(config.arq_fast_queue, settings, arq_events)
    finally:
        await manager.aclose()
