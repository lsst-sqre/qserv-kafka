"""The main application factory for the Qserv Kafka bridge.

Notes
-----
Be aware that, following the normal pattern for FastAPI services, the app is
constructed when this module is loaded and is not deferred until a function is
called.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from importlib.metadata import metadata, version

from fastapi import FastAPI
from safir.logging import configure_logging, configure_uvicorn_logging
from structlog import get_logger

from .config import config
from .handlers.internal import internal_router
from .handlers.kafka import kafka_router

__all__ = ["app"]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Set up and tear down the application."""
    logger = get_logger("qservkafka")
    logger.info("Qserv Kafka bridge started")
    yield


configure_logging(
    profile=config.profile,
    log_level=config.log_level,
    name="qservkafka",
)
configure_uvicorn_logging(config.log_level)

app = FastAPI(
    title="qserv-kafka",
    description=metadata("qserv-kafka")["Summary"],
    version=version("qserv-kafka"),
    lifespan=lifespan,
)
"""The main FastAPI application for qserv-kafka."""

# Attach the routers.
app.include_router(internal_router)
app.include_router(kafka_router)
