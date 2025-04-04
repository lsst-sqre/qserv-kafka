"""Test fixtures for qserv-kafka tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from contextlib import aclosing

import pytest
import pytest_asyncio
import respx
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from faststream.kafka import KafkaBroker, TestKafkaBroker
from httpx import ASGITransport, AsyncClient
from pydantic import MySQLDsn, SecretStr
from safir.database import create_async_session, create_database_engine
from sqlalchemy.ext.asyncio import AsyncEngine
from structlog import get_logger
from testcontainers.mysql import MySqlContainer

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
async def engine(
    mysql: MySqlContainer, monkeypatch: pytest.MonkeyPatch
) -> AsyncGenerator[AsyncEngine]:
    """Construct a SQLAlchemy engine for the test database."""
    url = MySQLDsn(mysql.get_connection_url())
    password = SecretStr("INSECURE-PASSWORD")
    monkeypatch.setattr(config, "qserv_database_password", password)
    monkeypatch.setattr(config, "qserv_database_url", url)
    engine = create_database_engine(str(url), config.qserv_database_password)
    logger = get_logger("qservkafka")
    await MockQserv.initialize(engine, logger)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def factory(
    mock_qserv: MockQserv, engine: AsyncEngine
) -> AsyncGenerator[Factory]:
    """Provide a component factory for tests that don't require the app."""
    context = await ProcessContext.from_config()
    async with aclosing(context):
        logger = get_logger("qservkafka")
        session = await create_async_session(engine, logger)
        yield Factory(context, session, logger)


@pytest_asyncio.fixture
async def kafka_broker() -> AsyncGenerator[KafkaBroker]:
    """Provide a Kafka producer pointing to the test Kafka."""
    async with TestKafkaBroker(kafka_router.broker) as broker:
        yield broker


@pytest_asyncio.fixture
async def mock_qserv(
    respx_mock: respx.Router, engine: AsyncEngine
) -> AsyncGenerator[MockQserv]:
    """Mock the Qserv REST API."""
    url = str(config.qserv_rest_url)
    async with register_mock_qserv(respx_mock, url, engine) as mock_qserv:
        yield mock_qserv


@pytest.fixture(scope="session")
def mysql() -> Generator[MySqlContainer]:
    """Start a MySQL database container for testing."""
    with MySqlContainer(
        dialect="asyncmy", username="qserv", password="INSECURE-PASSWORD"
    ) as mysql:
        yield mysql
