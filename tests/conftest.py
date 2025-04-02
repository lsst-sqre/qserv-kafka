"""Test fixtures for qserv-kafka tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import aclosing

import pytest
import pytest_asyncio
import respx
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from faststream.kafka import KafkaBroker, TestKafkaBroker
from httpx import ASGITransport, AsyncClient
from structlog import get_logger

from qservkafka import main
from qservkafka.config import config
from qservkafka.factory import Factory, ProcessContext
from qservkafka.handlers.kafka import kafka_router

from .support.qserv import MockQserv, register_mock_qserv


@pytest_asyncio.fixture
async def app(
    kafka_broker: KafkaBroker, mock_qserv: MockQserv
) -> AsyncGenerator[FastAPI]:
    """Return a configured test application.

    Wraps the application in a lifespan manager so that startup and shutdown
    events are sent during test execution.
    """
    async with LifespanManager(main.app):
        yield main.app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncGenerator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(
        base_url="https://example.com/", transport=ASGITransport(app=app)
    ) as client:
        yield client


@pytest_asyncio.fixture
async def factory(mock_qserv: MockQserv) -> AsyncGenerator[Factory]:
    """Provide a component factory for tests that don't require the app."""
    context = await ProcessContext.from_config()
    async with aclosing(context):
        yield Factory(context, get_logger("qservkafka"))


@pytest_asyncio.fixture
async def kafka_broker() -> AsyncGenerator[KafkaBroker]:
    """Provide a Kafka producer pointing to the test Kafka."""
    async with TestKafkaBroker(kafka_router.broker) as broker:
        yield broker


@pytest.fixture
def mock_qserv(respx_mock: respx.Router) -> MockQserv:
    """Mock the Qserv REST API."""
    return register_mock_qserv(respx_mock, str(config.qserv_rest_url))
