"""Per-request context."""

from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, override

from aiokafka import ConsumerRecord
from faststream.message import StreamMessage
from faststream.middlewares import BaseMiddleware
from structlog import get_logger
from structlog.stdlib import BoundLogger

from ..factory import Factory, ProcessContext, build_process_context

__all__ = [
    "ConsumerContext",
    "ContextDependency",
    "MessageContextMiddleware",
    "context_dependency",
]


type RawMessage = ConsumerRecord | tuple[ConsumerRecord, ...]
"""Type of a raw message, which may contain a tuple of records."""


_current_message: ContextVar[StreamMessage[Any] | None] = ContextVar(
    "qserv_kafka_current_message", default=None
)
"""The FastStream message being consumed on the current task, if any.

Set by `MessageContextMiddleware` for the duration of each message and read
by `ConsumerContextDependency`.
"""


class MessageContextMiddleware(BaseMiddleware[Any, Any]):
    """Expose the message being consumed to FastAPI-style dependencies.

    FastStream stores the current message in its own context repository, which
    ``faststream_fastapi.Context("message")`` used to read. Since faststream
    0.7.5, an application-level ``FastDependsConfig`` merged into a broker
    wraps the broker's context in a ``ContextRepoComposition`` and scopes the
    per-message values inside that composition, while faststream-fastapi
    (1.3.1) still hands its ``Context()`` dependencies the application-level
    ``ContextRepo``, which no longer sees ``message`` and resolves it to
    ``EMPTY``.

    This middleware sidesteps that plumbing: the subscriber hands middlewares
    the parsed message directly, so it is published on a
    `contextvars.ContextVar` that the handler's dependencies, running on the
    same task, can read.
    """

    @override
    async def consume_scope(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:
        """Publish the message to the current task while it is handled."""
        token = _current_message.set(msg)
        try:
            return await call_next(msg)
        finally:
            _current_message.reset(token)


@dataclass(kw_only=True, slots=True)
class ConsumerContext:
    """Context for a Kafka consumer."""

    factory: Factory
    """The component factory."""

    message_timestamp: datetime
    """Timestamp of the message, extracted from the Kafka header."""

    logger: BoundLogger
    """Logger for the consumer."""


class ContextDependency:
    """Provide per-message context as a dependency for a FastStream consumer.

    Each message handler class gets a `ConsumerContext`. To save overhead, the
    portions of the context that are shared by all requests are collected into
    the single process-global `~qservkafka.factory.ProcessContext` and reused
    with each request.

    The message itself comes from `MessageContextMiddleware`, which must be
    registered on the broker, rather than from a FastStream ``Context``
    parameter (see the middleware for why).
    """

    def __init__(self) -> None:
        self._context: ProcessContext | None = None

    async def __call__(self) -> ConsumerContext:
        """Create a per-request context."""
        message = _current_message.get()
        if message is None:
            msg = (
                "No message is being consumed on this task; is"
                " MessageContextMiddleware registered on the broker?"
            )
            raise RuntimeError(msg)
        record: RawMessage = message.raw_message

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

        # Return the per-message context.
        timestamp = datetime.fromtimestamp(record.timestamp / 1000, tz=UTC)
        return ConsumerContext(
            logger=logger,
            factory=self.create_factory(logger),
            message_timestamp=timestamp,
        )

    async def aclose(self) -> None:
        """Clean up the per-process singletons."""
        if self._context:
            await self._context.aclose()
        self._context = None

    def create_factory(self, logger: BoundLogger | None = None) -> Factory:
        """Create a new factory.

        This is used for background processing, so it is separate from the
        work inside ``__call__``, which assumes that there is a Kafka message
        to which the bridge is reacting.

        Parameters
        ----------
        logger
            Logger to use. If not given, the default logger will be used.

        Returns
        -------
        Factory
            Newly-constructed factory.
        """
        if not self._context:
            raise RuntimeError("Context dependency not initialized")
        if not logger:
            logger = get_logger("qservkafka")
        return self._context.build_factory(logger)

    async def initialize(self) -> None:
        """Initialize the process-wide shared context."""
        self._context = await build_process_context()
        await self._context.connect()


context_dependency = ContextDependency()
"""Dependency to create the per-request context."""
