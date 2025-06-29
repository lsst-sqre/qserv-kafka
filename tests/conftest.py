"""Test fixtures for qserv-kafka tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from contextlib import aclosing
from datetime import timedelta

import pytest
import pytest_asyncio
import respx
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from faststream.kafka import KafkaBroker, TestKafkaBroker
from faststream.kafka.fastapi import KafkaRouter
from faststream.kafka.publisher.asyncapi import AsyncAPIDefaultPublisher
from httpx import ASGITransport, AsyncClient
from pydantic import MySQLDsn, RedisDsn, SecretStr
from safir.database import create_async_session, create_database_engine
from safir.logging import LogLevel, Profile, configure_logging
from sqlalchemy.ext.asyncio import AsyncEngine
from structlog import get_logger
from structlog.stdlib import BoundLogger
from testcontainers.mysql import MySqlContainer
from testcontainers.redis import RedisContainer

from qservkafka.background import BackgroundTaskManager
from qservkafka.config import config
from qservkafka.factory import Factory, ProcessContext
from qservkafka.main import create_app

from .support.qserv import MockQserv, register_mock_qserv


@pytest_asyncio.fixture
async def app(
    *,
    kafka_router: KafkaRouter,
    kafka_broker: KafkaBroker,
    status_publisher: AsyncAPIDefaultPublisher,
    mock_qserv: MockQserv,
    redis: RedisContainer,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[FastAPI]:
    """Return a configured test application.

    Wraps the application in a lifespan manager so that startup and shutdown
    events are sent during test execution.
    """
    redis_host = redis.get_container_host_ip()
    redis_port = redis.get_exposed_port(6379)
    redis_url = RedisDsn(f"redis://{redis_host}:{redis_port}/0")
    monkeypatch.setattr(config, "redis_url", redis_url)
    app = create_app(kafka_router, kafka_broker, status_publisher)
    async with LifespanManager(app):
        yield app


@pytest_asyncio.fixture
async def background(
    factory: Factory, logger: BoundLogger
) -> AsyncGenerator[BackgroundTaskManager]:
    """Create and start the background task manager."""
    monitor = await factory.create_query_monitor()
    background = BackgroundTaskManager(monitor, logger)
    await background.start()
    yield background
    await background.stop()


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncGenerator[AsyncClient]:
    """Return an ``httpx.AsyncClient`` configured to talk to the test app."""
    async with AsyncClient(
        base_url="https://example.com/", transport=ASGITransport(app=app)
    ) as client:
        yield client


@pytest_asyncio.fixture
async def engine(
    mysql: MySqlContainer, monkeypatch: pytest.MonkeyPatch, logger: BoundLogger
) -> AsyncGenerator[AsyncEngine]:
    """Construct a SQLAlchemy engine for the test database."""
    url = MySQLDsn(mysql.get_connection_url())
    password = SecretStr("INSECURE-PASSWORD")
    monkeypatch.setattr(config, "qserv_database_password", password)
    monkeypatch.setattr(config, "qserv_database_url", url)
    engine = create_database_engine(str(url), config.qserv_database_password)
    await MockQserv.initialize(engine, logger)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def factory(
    *,
    mock_qserv: MockQserv,
    engine: AsyncEngine,
    redis: RedisContainer,
    monkeypatch: pytest.MonkeyPatch,
    logger: BoundLogger,
) -> AsyncGenerator[Factory]:
    """Provide a component factory for tests that don't require the app."""
    redis_host = redis.get_container_host_ip()
    redis_port = redis.get_exposed_port(6379)
    redis_url = RedisDsn(f"redis://{redis_host}:{redis_port}/0")
    monkeypatch.setattr(config, "redis_url", redis_url)
    kafka_broker = KafkaBroker(**config.kafka.to_faststream_params())
    async with TestKafkaBroker(kafka_broker) as mock_broker:
        context = await ProcessContext.create(mock_broker)
        async with aclosing(context):
            session = await create_async_session(engine, logger)
            yield Factory(context, session, logger)
    await kafka_broker.close()


@pytest.fixture
def kafka_router(logger: BoundLogger) -> KafkaRouter:
    """Kafka router used for testing, broken out so that it can be mocked."""
    kafka_params = config.kafka.to_faststream_params()
    return KafkaRouter(**kafka_params, logger=logger)


@pytest.fixture
def logger() -> BoundLogger:
    configure_logging(
        profile=Profile.production,
        log_level=LogLevel.DEBUG,
        name="qservkafka",
        add_timestamp=True,
    )
    return get_logger("qservkafka")


@pytest.fixture
def status_publisher(
    kafka_router: KafkaRouter, kafka_broker: KafkaBroker
) -> AsyncAPIDefaultPublisher:
    """Create a mocked Kafka publisher for status messages."""
    return kafka_router.publisher(config.job_status_topic)


@pytest_asyncio.fixture
async def kafka_broker(
    kafka_router: KafkaRouter,
) -> AsyncGenerator[KafkaBroker]:
    """Provide a Kafka producer pointing to the test Kafka."""
    async with TestKafkaBroker(kafka_router.broker) as broker:
        yield broker


@pytest_asyncio.fixture(ids=["good"], params=[False])
async def mock_qserv(
    *,
    respx_mock: respx.Router,
    engine: AsyncEngine,
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[MockQserv]:
    """Mock the Qserv REST API.

    This mock is designed for pytest indirect parameterization. Tests that
    want to use flaky web services that fail ever other time should inject a
    parameter of `True`.

    Examples
    --------
    Add the following mark before tests that should repeat the test with a
    flaky web service.

    .. code-block:: python

       @pytest.mark.parametrize(
           "mock_qserv", [False, True], ids=["good", "flaky"], indirect=True
       )
    """
    if request.param:
        delay = timedelta(milliseconds=10)
        monkeypatch.setattr(config, "qserv_retry_delay", delay)
    url = str(config.qserv_rest_url)
    async with register_mock_qserv(
        respx_mock, url, engine, flaky=request.param
    ) as mock_qserv:
        yield mock_qserv


@pytest.fixture(scope="session")
def mysql() -> Generator[MySqlContainer]:
    """Start a MySQL database container for testing."""
    assert config.qserv_database_password
    password = config.qserv_database_password.get_secret_value()
    with MySqlContainer(
        dialect="asyncmy", username="qserv", password=password
    ) as mysql:
        yield mysql


@pytest.fixture
def redis(redis_container: RedisContainer) -> RedisContainer:
    """Wrap the session fixture to clear data before each test."""
    redis_client = redis_container.get_client()
    for key in redis_client.scan_iter("query:*"):
        redis_client.delete(key)
    return redis_container


@pytest.fixture(scope="session")
def redis_container() -> Generator[RedisContainer]:
    """Start a Redis container for testing."""
    assert config.redis_password
    password = config.redis_password.get_secret_value()
    with RedisContainer(password=password) as redis:
        yield redis
