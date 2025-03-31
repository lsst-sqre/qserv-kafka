"""Per-request context."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from faststream.kafka.fastapi import KafkaMessage
from structlog import get_logger
from structlog.stdlib import BoundLogger

__all__ = [
    "ConsumerContext",
    "ContextDependency",
    "context_dependency",
]


@dataclass(kw_only=True, slots=True)
class ConsumerContext:
    """Context for a Kafka consumer."""

    logger: BoundLogger
    """Logger for the consumer."""

    def rebind_logger(self, **values: Any) -> None:
        """Add the given values to the logging context.

        Parameters
        ----------
        **values
            Additional values that should be added to the logging context.
        """
        self.logger = self.logger.bind(**values)


class ContextDependency:
    """Provide per-message context as a dependency for a FastStream consumer.

    Each message handler class gets a `ConsumerContext`. To save overhead, the
    portions of the context that are shared by all requests are collected into
    the single process-global `~unfurlbot.factory.ProcessContext` and reused
    with each request.
    """

    async def __call__(self, message: KafkaMessage) -> ConsumerContext:
        """Create a per-request context."""
        # The underlying Kafka messages can either be a single message or a
        # tuple of messages. Since we only are using them to extract some
        # metadata for logging purposes, use the first message if there are
        # several.
        record = message.raw_message
        if isinstance(record, Sequence):
            record = record[0]

        # Add the Kafka context to the logger
        logger = get_logger("qservkafka")
        kafka_context = {
            "topic": record.topic,
            "offset": record.offset,
            "partition": record.partition,
        }
        logger = logger.bind(kafka=kafka_context)

        return ConsumerContext(logger=logger)


context_dependency = ContextDependency()
"""Dependency to create the per-request context."""
