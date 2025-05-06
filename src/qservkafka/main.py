"""The main application factory for the Qserv Kafka bridge."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from importlib.metadata import metadata, version

from fastapi import FastAPI
from faststream.kafka import KafkaBroker
from faststream.kafka.fastapi import KafkaRouter
from faststream.kafka.publisher.asyncapi import AsyncAPIDefaultPublisher
from safir.logging import configure_logging, configure_uvicorn_logging
from structlog import get_logger

from .background import BackgroundTaskManager
from .config import config
from .dependencies.context import context_dependency
from .handlers.internal import internal_router
from .handlers.kafka import register_kafka_handlers

__all__ = ["create_app"]


def create_app(
    kafka_router: KafkaRouter | None = None,
    kafka_broker: KafkaBroker | None = None,
    status_publisher: AsyncAPIDefaultPublisher | None = None,
) -> FastAPI:
    """Create the FastAPI application.

    This is a function rather than using a global variable (as is more typical
    for FastAPI) so that the test suite can configure Kafka before creating
    the Kafka router.

    Parameters
    ----------
    kafka_router
        Use the provided Kafka router instead of creating a new one if this
        parameter is not `None`. Only used by the test suite.
    kafka_broker
        Use the provided Kafka broker for background jobs instead of creating
        a new one if this parameter is not `None`. Only used by the test
        suite.
    status_publisher
        Use the provided publisher for job status messages instead of creating
        a new one if this parameter is not `None`. Only used by the test
        suite.
    """
    configure_logging(
        profile=config.profile,
        log_level=config.log_level,
        name="qservkafka",
        add_timestamp=True,
    )
    configure_uvicorn_logging(config.log_level)
    logger = get_logger("qservkafka")

    # Create the Kafka router if one was not provided.
    if not kafka_router:
        kafka_params = config.kafka.to_faststream_params()
        kafka_router = KafkaRouter(**kafka_params, logger=logger)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        await context_dependency.initialize(kafka_broker)
        logger = get_logger("qservkafka")
        factory = context_dependency.create_factory()
        monitor = await factory.create_query_monitor()
        background = BackgroundTaskManager(monitor, logger)
        await background.start()
        logger.info("Qserv Kafka bridge started")

        yield

        await background.stop()
        await context_dependency.aclose()

    app = FastAPI(
        title="qserv-kafka",
        description=metadata("qserv-kafka")["Summary"],
        version=version("qserv-kafka"),
        lifespan=lifespan,
    )

    # Attach the routers.
    app.include_router(internal_router)
    register_kafka_handlers(kafka_router, status_publisher)
    app.include_router(kafka_router)

    return app
