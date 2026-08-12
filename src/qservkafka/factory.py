"""Create Qserv Kafka bridge components."""

import ssl
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Self, override

from faststream.kafka import KafkaBroker
from httpx import AsyncClient, Limits
from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from rubin.repertoire import DiscoveryClient
from safir.arq import ArqMode, ArqQueue, MockArqQueue, RedisArqQueue
from safir.database import create_database_engine
from safir.dependencies.http_client import http_client_dependency
from safir.metrics import EventManager
from safir.redis import PydanticRedisStorage
from safir.slack.webhook import SlackWebhookClient
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from structlog.stdlib import BoundLogger, get_logger

from qservkafka.background import BackgroundTaskManager

from .config import BackendType, config
from .constants import (
    REDIS_BACKOFF_MAX,
    REDIS_BACKOFF_START,
    REDIS_POOL_TIMEOUT,
    REDIS_RETRIES,
    REDIS_TIMEOUT,
)
from .events import Events
from .models.state import RunningQuery
from .services.monitor import QueryMonitor
from .services.query import QueryService
from .services.results import ResultProcessor
from .storage.backend import DatabaseBackend
from .storage.bigquery import BigQueryClient
from .storage.gafaelfawr import GafaelfawrStorage
from .storage.qserv import QservClient
from .storage.rate import RateLimitStore
from .storage.state import QueryStateStore
from .storage.votable import VOTableWriter

__all__ = [
    "Factory",
    "ProcessContext",
    "QservProcessContext",
    "build_process_context",
]


@dataclass(kw_only=True, slots=True)
class ProcessContext(metaclass=ABCMeta):
    """Per-process application context.

    This object caches all of the per-process singletons that can be reused
    for every incoming message and only need to be recreated if the
    application configuration changes.
    """

    http_client: AsyncClient
    """HTTP client used for talking to the backend."""

    redis: Redis
    """Connection pool for state-tracking Redis."""

    arq_queue: ArqQueue
    """Queue to which to dispatch work to arq workers."""

    kafka_broker: KafkaBroker
    """Kafka broker to use for publishing messages from background jobs."""

    slack_client: SlackWebhookClient | None
    """Client for sending Slack error notifications."""

    discovery_client: DiscoveryClient
    """Shared service discovery client."""

    gafaelfawr: GafaelfawrStorage
    """Shared caching Gafaelfawr storage."""

    event_manager: EventManager
    """Manager for publishing metrics events."""

    events: Events
    """Event publishers for metrics events."""

    @classmethod
    async def build_shared_context(
        cls, kafka_broker: KafkaBroker | None = None
    ) -> dict[str, Any]:
        """Create shared `ProcessContext` attributes.

        This method should only be called by subclasses as part of their
        `create` method. It creates all the shared data for a process context
        and returns it as a dictionary suiltable for providing keyword
        parameters to a class constructor.

        Parameters
        ----------
        kafka_broker
            If not `None`, use this Kafka broker instead of making a new one.

        Returns
        -------
        dict
            Shared context for the process.
        """
        logger = get_logger("qservkafka")

        # Create a Redis client pool with exponential backoff and the state
        # store that stores job state in Redis.
        redis_client = cls._create_redis_client()

        # Create the arq queues with their underlying Redis clients.
        arq_queue = await cls._create_arq_queue()

        # Create a Kafka broker used for background tasks. This needs to be a
        # separate broker from the one used by handlers, since the one used by
        # handlers will be shut down when SIGTERM is retrieved, thus
        # preventing the Qserv Kafka bridge from completing any result
        # processing that is still running.
        if not kafka_broker:
            kafka_broker = KafkaBroker(
                client_id="qserv-kafka", **config.kafka.to_faststream_params()
            )
        await kafka_broker.connect()

        # Create the HTTP, Slack, service discovery, and Gafaelfawr clients,
        # all using the same HTTP connection pool.
        slack_client = cls._create_slack_client(logger)
        http_client = await http_client_dependency()
        discovery_client = DiscoveryClient(http_client, logger=logger)
        gafaelfawr = GafaelfawrStorage(
            http_client=http_client,
            discovery_client=discovery_client,
            slack_client=slack_client,
            logger=logger,
        )

        # Create the events manager and publishers.
        event_manager = config.metrics.make_manager(kafka_broker=kafka_broker)
        await event_manager.initialize()
        events = Events()
        await events.initialize(event_manager)

        # Create and return the process context.
        return {
            "http_client": http_client,
            "redis": redis_client,
            "arq_queue": arq_queue,
            "discovery_client": discovery_client,
            "kafka_broker": kafka_broker,
            "gafaelfawr": gafaelfawr,
            "event_manager": event_manager,
            "events": events,
            "slack_client": slack_client,
        }

    @staticmethod
    async def _create_arq_queue() -> ArqQueue:
        """Create the queue used to dispatch work to arq workers."""
        if config.arq_mode == ArqMode.production:
            settings = config.arq_redis_settings
            return await RedisArqQueue.initialize(
                settings, default_queue_name=config.arq_queue
            )
        else:
            return MockArqQueue()

    @staticmethod
    def _create_redis_client() -> Redis:
        """Create the Redis client pool with exponential backoff."""
        redis_password = config.redis_password.get_secret_value()
        backoff = ExponentialBackoff(
            base=REDIS_BACKOFF_START, cap=REDIS_BACKOFF_MAX
        )
        redis_pool = BlockingConnectionPool.from_url(
            str(config.redis_url),
            password=redis_password,
            max_connections=config.redis_max_connections,
            retry=Retry(backoff, REDIS_RETRIES),
            retry_on_timeout=True,
            socket_keepalive=True,
            socket_timeout=REDIS_TIMEOUT,
            timeout=REDIS_POOL_TIMEOUT,
        )
        return Redis.from_pool(redis_pool)

    @staticmethod
    def _create_slack_client(logger: BoundLogger) -> SlackWebhookClient | None:
        """Create the Slack client for error notifications."""
        if not config.slack.enabled:
            return None
        if config.slack.webhook is None:
            msg = "Slack: if enabled is true, then webhook must be set"
            raise RuntimeError(msg)
        return SlackWebhookClient(
            hook_url=config.slack.webhook,
            application="qserv-kafka",
            logger=logger,
        )

    @classmethod
    async def create(cls, kafka_broker: KafkaBroker | None = None) -> Self:
        """Create a new process context from a database engine.

        Parameters
        ----------
        kafka_broker
            If not `None`, use this Kafka broker instead of making a new one.

        Returns
        -------
        ProcessContext
            Shared context for a Qserv Kafka bridge process.
        """
        shared = await cls.build_shared_context(kafka_broker)
        return cls(**shared)

    async def aclose(self) -> None:
        """Clean up a process context.

        Called during shutdown, or before recreating the process context using
        a different configuration.
        """
        await self.redis.aclose()
        await self.arq_queue.aclose()
        await self.kafka_broker.stop()
        await self.event_manager.aclose()

    @abstractmethod
    def build_factory(self, logger: BoundLogger) -> Factory:
        """Construct an appropriate factory for this process context.

        Parameters
        ----------
        logger
            Logger to use.

        Returns
        -------
        Factory
            An appropriate factory for the configured backend.
        """


@dataclass(kw_only=True, slots=True)
class BigQueryProcessContext(ProcessContext):
    """Per-process application context for BigQuery.

    This object caches all of the per-process singletons that can be reused
    for every incoming message and only need to be recreated if the
    application configuration changes.
    """

    @override
    def build_factory(self, logger: BoundLogger) -> Factory:
        return BigQueryFactory(self, logger)


@dataclass(kw_only=True, slots=True)
class QservProcessContext(ProcessContext):
    """Per-process application context for Qserv.

    This object caches all of the per-process singletons that can be reused
    for every incoming message and only need to be recreated if the
    application configuration changes.
    """

    engine: AsyncEngine
    """Database engine."""

    sessionmaker: async_sessionmaker
    """Factory for database sessions."""

    qserv_http_client: AsyncClient
    """HTTP client for talking to Qserv."""

    @override
    @classmethod
    async def create(
        cls,
        kafka_broker: KafkaBroker | None = None,
        *,
        qserv_database_pool_size: int | None = None,
    ) -> Self:
        """Create a new process context from a database engine.

        Parameters
        ----------
        kafka_broker
            If not `None`, use this Kafka broker instead of making a new one.
        qserv_database_pool_size
            If not `None`, override the default database pool size. This is
            used by result workers, since they only need one connection per
            worker job.

        Returns
        -------
        ProcessContext
            Shared context for a Qserv Kafka bridge process.
        """
        if config.qserv_database_url is None:
            msg = "qserv_database_url is required for Qserv backend"
            raise ValueError(msg)
        shared = await cls.build_shared_context(kafka_broker)

        # Qserv uses a self-signed certificate with no known certificate
        # chain. We do not use TLS to validate the identity of the server.
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Qserv uses a self-signed certificate and has configuration settings
        # for maximum simultaneous connections.
        qserv_http_client = AsyncClient(
            timeout=config.backend_api_timeout.total_seconds(),
            limits=Limits(max_connections=config.qserv_rest_max_connections),
            verify=False,  # noqa: S501
        )

        # Create the database engine and sessionmaker.
        pool_size = qserv_database_pool_size or config.qserv_database_pool_size
        connect_args = {
            "ssl": ssl_context,
            "connect_timeout": config.qserv_database_connect_timeout,
            "read_timeout": config.qserv_database_read_timeout,
        }
        engine = create_database_engine(
            str(config.qserv_database_url),
            config.qserv_database_password,
            connect_args=connect_args,
            max_overflow=config.qserv_database_overflow,
            pool_size=pool_size,
        )
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)

        # Construct the Qserv-specific process context.
        return cls(
            engine=engine,
            sessionmaker=sessionmaker,
            qserv_http_client=qserv_http_client,
            **shared,
        )

    @override
    async def aclose(self) -> None:
        await super().aclose()
        await self.engine.dispose()
        await self.qserv_http_client.aclose()

    @override
    def build_factory(self, logger: BoundLogger) -> Factory:
        return QservFactory(self, logger)


async def build_process_context(
    kafka_broker: KafkaBroker | None = None,
    *,
    worker_max_jobs: int | None = None,
) -> ProcessContext:
    """Construct a new process context of the appropriate type.

    Handles determining the backend type from the configuration and creating
    an appropriate process context for either a backend worker or the frontend
    service.

    Parameters
    ----------
    kafka_broker
        If not `None`, use this Kafka broker instead of making a new one.
    worker_max_jobs
        Maximum number of worker jobs if creating a process context for an
        arq worker, or `None` if creating the frontend context.
    """
    match config.backend:
        case BackendType.BIGQUERY:
            return await ProcessContext.create(kafka_broker)
        case BackendType.QSERV:
            if worker_max_jobs is not None:
                return await QservProcessContext.create(
                    kafka_broker, qserv_database_pool_size=worker_max_jobs
                )
            else:
                return await QservProcessContext.create(kafka_broker)


class Factory(metaclass=ABCMeta):
    """Build bridge components.

    Uses the contents of a `ProcessContext` to construct the components of the
    application on demand. There are specialized versions of this class for
    each backend.

    Parameters
    ----------
    context
        Shared process context.
    logger
        Logger to use for errors.
    """

    def __init__(self, context: ProcessContext, logger: BoundLogger) -> None:
        self._context = context
        self._logger = logger

    @property
    def events(self) -> Events:
        """Global shared metrics events publishers, used by the test suite."""
        return self._context.events

    @property
    def gafaelfawr(self) -> GafaelfawrStorage:
        """Global shared caching Gafaelfawr client."""
        return self._context.gafaelfawr

    @abstractmethod
    def create_backend_client(self) -> DatabaseBackend:
        """Create a client for the configured database backend.

        Returns
        -------
        DatabaseBackend
            Client for the database backend.
        """

    async def create_background_task_manager(self) -> BackgroundTaskManager:
        """Create the background task manager to monitor Qserv jobs.

        Returns
        -------
        BackgroundTaskManager
            Manager for periodically checking the status of Qserv jobs.
        """
        monitor = await self.create_query_monitor()
        return BackgroundTaskManager(
            monitor, self._context.slack_client, self._logger
        )

    def create_query_state_store(self) -> QueryStateStore:
        """Create the storage client for query state.

        Returns
        -------
        QueryStateStore
            Client for query state.
        """
        redis_storage = PydanticRedisStorage(
            datatype=RunningQuery,
            redis=self._context.redis,
            key_prefix="query:",
        )
        return QueryStateStore(redis_storage, self._logger)

    async def create_query_monitor(self) -> QueryMonitor:
        """Create the singleton monitor for query status.

        This is run as a background task.

        Returns
        -------
        QueryMonitor
            New service to monitor query status.
        """
        return QueryMonitor(
            result_processor=self.create_result_processor(),
            backend=self.create_backend_client(),
            arq_queue=self._context.arq_queue,
            state_store=self.create_query_state_store(),
            rate_limit_store=self.create_rate_limit_store(),
            events=self._context.events,
            logger=self._logger,
        )

    def create_query_service(self) -> QueryService:
        """Create a new service for starting queries.

        Returns
        -------
        QueryService
            New service to start queries.
        """
        return QueryService(
            backend=self.create_backend_client(),
            state_store=self.create_query_state_store(),
            result_processor=self.create_result_processor(),
            rate_limit_store=self.create_rate_limit_store(),
            gafaelfawr_storage=self.gafaelfawr,
            arq_queue=self._context.arq_queue,
            events=self._context.events,
            slack_client=self._context.slack_client,
            logger=self._logger,
        )

    def create_result_processor(self) -> ResultProcessor:
        """Create a new service for processing results.

        Returns
        -------
        ResultProcessor
            New service to process a completed query.
        """
        return ResultProcessor(
            backend=self.create_backend_client(),
            state_store=self.create_query_state_store(),
            votable_writer=VOTableWriter(
                self._context.http_client,
                self._context.discovery_client,
                self._logger,
            ),
            kafka_broker=self._context.kafka_broker,
            rate_limit_store=self.create_rate_limit_store(),
            arq_queue=self._context.arq_queue,
            events=self._context.events,
            slack_client=self._context.slack_client,
            logger=self._logger,
        )

    def create_rate_limit_store(self) -> RateLimitStore:
        """Create a new storage client for rate limiting.

        Returns
        -------
        RateLimitStore
            Storage for rate limit information.
        """
        return RateLimitStore(self._context.redis)


class BigQueryFactory(Factory):
    """Build BigQuery Kafka bridge components.

    Uses the contents of a `ProcessContext` to construct the components of the
    application on demand.

    Parameters
    ----------
    context
        Shared process context.
    logger
        Logger to use for errors.
    """

    @override
    def create_backend_client(self) -> DatabaseBackend:
        return BigQueryClient(
            project=config.bigquery_project,
            location=config.bigquery_location,
            http_client=self._context.http_client,
            events=self._context.events,
            slack_client=self._context.slack_client,
            logger=self._logger,
        )


class QservFactory(Factory):
    """Build Qserv Kafka bridge components.

    Uses the contents of a `QservProcessContext` to construct the components
    of the application on demand.

    Parameters
    ----------
    context
        Shared process context.
    logger
        Logger to use for errors.
    """

    def __init__(
        self, context: QservProcessContext, logger: BoundLogger
    ) -> None:
        self._context: QservProcessContext = context
        self._logger = logger

    @override
    def create_backend_client(self) -> DatabaseBackend:
        return QservClient(
            sessionmaker=self._context.sessionmaker,
            http_client=self._context.qserv_http_client,
            events=self._context.events,
            slack_client=self._context.slack_client,
            logger=self._logger,
        )
