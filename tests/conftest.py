"""Test fixtures for qserv-kafka tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator, Iterator
from contextlib import aclosing
from datetime import timedelta

import pytest
import pytest_asyncio
import respx
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from faststream.kafka import KafkaBroker, TestKafkaBroker
from httpx import ASGITransport, AsyncClient
from pydantic import MySQLDsn, RedisDsn, SecretStr
from safir.arq import ArqMode
from safir.database import create_async_session, create_database_engine
from safir.kafka import KafkaConnectionSettings, SecurityProtocol
from safir.logging import configure_logging
from safir.testing.containers import FullKafkaContainer
from sqlalchemy.ext.asyncio import AsyncEngine
from structlog import get_logger
from structlog.stdlib import BoundLogger
from testcontainers.core.network import Network
from testcontainers.mysql import MySqlContainer
from testcontainers.redis import RedisContainer

from qservkafka.config import config
from qservkafka.factory import Factory, ProcessContext
from qservkafka.main import create_app

from .support.gafaelfawr import MockGafaelfawr, register_mock_gafaelfawr
from .support.qserv import MockQserv, register_mock_qserv


@pytest_asyncio.fixture
async def app(
    *,
    kafka_connection_settings: KafkaConnectionSettings,
    mock_qserv: MockQserv,
    redis: RedisContainer,
    monkeypatch: pytest.MonkeyPatch,
) -> FastAPI:
    redis_host = redis.get_container_host_ip()
    redis_port = redis.get_exposed_port(6379)
    redis_url = RedisDsn(f"redis://{redis_host}:{redis_port}/0")
    poll_interval = timedelta(milliseconds=100)
    monkeypatch.setattr(config, "arq_mode", ArqMode.production)
    monkeypatch.setattr(config, "redis_url", redis_url)
    monkeypatch.setattr(config, "kafka", kafka_connection_settings)
    monkeypatch.setattr(config, "qserv_poll_interval", poll_interval)
    return create_app()


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
    """Provide a component factory for tests.

    This fixture doesn't require the app and doesn't start an actual Kafka
    instance. Only use this for unit tests without a real Kafka.
    """
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
    await kafka_broker.stop()


@pytest.fixture(scope="session")
def global_kafka_container(
    kafka_docker_network: Network,
) -> Iterator[FullKafkaContainer]:
    container = FullKafkaContainer()
    container.with_network(kafka_docker_network)
    container.with_network_aliases("kafka")
    with container as kafka:
        yield kafka


@pytest_asyncio.fixture
async def kafka_broker(
    kafka_connection_settings: KafkaConnectionSettings,
) -> AsyncGenerator[KafkaBroker]:
    broker = KafkaBroker(
        **kafka_connection_settings.to_faststream_params(),
        client_id="pytest-broker",
    )
    await broker.start()
    yield broker
    await broker.stop()


@pytest.fixture
def kafka_container(
    global_kafka_container: FullKafkaContainer,
) -> FullKafkaContainer:
    global_kafka_container.reset()
    return global_kafka_container


@pytest.fixture
def kafka_connection_settings(
    kafka_container: FullKafkaContainer, monkeypatch: pytest.MonkeyPatch
) -> KafkaConnectionSettings:
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS")
    monkeypatch.delenv("KAFKA_SECURITY_PROTOCOL")
    return KafkaConnectionSettings(
        bootstrap_servers=kafka_container.get_bootstrap_server(),
        security_protocol=SecurityProtocol.PLAINTEXT,
    )


@pytest.fixture(scope="session")
def kafka_docker_network() -> Iterator[Network]:
    with Network() as network:
        yield network


@pytest_asyncio.fixture
async def kafka_status_consumer(
    kafka_connection_settings: KafkaConnectionSettings,
) -> AsyncGenerator[AIOKafkaConsumer]:
    consumer = AIOKafkaConsumer(
        config.job_status_topic,
        **kafka_connection_settings.to_aiokafka_params(),
        client_id="pytest-consumer",
    )
    await consumer.start()
    yield consumer
    await consumer.stop()


@pytest.fixture
def logger() -> BoundLogger:
    configure_logging(
        profile=config.profile,
        log_level=config.log_level,
        name="qservkafka",
        add_timestamp=True,
    )
    return get_logger("qservkafka")


@pytest.fixture(autouse=True)
def mock_gafaelfawr(respx_mock: respx.Router) -> MockGafaelfawr:
    base_url = str(config.gafaelfawr_base_url)
    return register_mock_gafaelfawr(respx_mock, base_url)


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
    for key in redis_client.scan_iter("rate:*"):
        redis_client.delete(key)
    return redis_container


@pytest.fixture(scope="session")
def redis_container() -> Generator[RedisContainer]:
    """Start a Redis container for testing."""
    assert config.redis_password
    password = config.redis_password.get_secret_value()
    with RedisContainer(password=password) as redis:
        yield redis
