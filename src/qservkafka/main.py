"""The main application factory for the Qserv Kafka bridge."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from importlib.metadata import metadata, version

from fastapi import FastAPI
from faststream.kafka.fastapi import KafkaRouter
from safir.kafka import FastStreamErrorHandler
from safir.logging import configure_logging, configure_uvicorn_logging
from safir.sentry import initialize_sentry
from safir.slack.webhook import SlackRouteErrorHandler
from sentry_sdk.integrations.logging import LoggingIntegration
from structlog.stdlib import get_logger

from . import __version__
from .config import config
from .dependencies.context import context_dependency
from .handlers.internal import internal_router
from .handlers.kafka import register_kafka_handlers

__all__ = ["create_app"]


def create_app() -> FastAPI:
    """Create the FastAPI application.

    This is a function rather than using a global variable (as is more typical
    for FastAPI) so that the test suite can configure Kafka before creating
    the Kafka router.
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
    configure_uvicorn_logging(config.log_level)
    logger = get_logger("qservkafka")

    # Create the Kafka router if one was not provided.
    faststream_error_handler = FastStreamErrorHandler()
    kafka_params = config.kafka.to_faststream_params()
    kafka_router = KafkaRouter(
        middlewares=[faststream_error_handler.make_middleware()],
        **kafka_params,
        logger=logger,
    )

    # Configure Slack alerts.
    if config.slack.enabled:
        if config.slack.webhook is None:
            msg = "Slack: if enabled is true, then webhook must be set"
            raise RuntimeError(msg)
        SlackRouteErrorHandler.initialize(
            config.slack.webhook, "qserv-kafka", logger
        )
        faststream_error_handler.initialize_slack(
            config.slack.webhook, "qserv-kafka", logger
        )
        logger.debug("Initialized Slack webhook")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        await context_dependency.initialize()
        logger = get_logger("qservkafka")
        factory = context_dependency.create_factory()
        background = await factory.create_background_task_manager()
        await background.start()
        logger.info("Qserv Kafka bridge started")

        # Ensure that everything is shut down cleanly even if the application
        # dies with an exception. This shouldn't matter for normal usage, but
        # it ensures that test suite failures don't cause errors in all
        # subsequent tests because we have stray objects attached to the wrong
        # event loop.
        try:
            yield
        finally:
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
    register_kafka_handlers(kafka_router)
    app.include_router(kafka_router)

    return app
