"""The FastStream Kafka router.

Notes
-----
This module holds kafka_router in a separate module to avoid circular imports
with the `~qservkafka.dependencies.context.context_dependency`.
"""

from __future__ import annotations

from faststream.kafka.fastapi import KafkaRouter
from structlog import get_logger

from .config import config

__all__ = ["kafka_router"]


kafka_router = KafkaRouter(
    **config.kafka.to_faststream_params(), logger=get_logger("qservkafka")
)
"""Faststream router for incoming Kafka requests."""
