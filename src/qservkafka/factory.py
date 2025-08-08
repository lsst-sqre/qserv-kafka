"""Create Qserv Kafka bridge components."""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from typing import Self

from faststream.kafka import KafkaBroker
from httpx import AsyncClient, Limits
from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from safir.arq import ArqMode, ArqQueue, MockArqQueue, RedisArqQueue
from safir.database import create_database_engine
from safir.metrics import EventManager
from safir.redis import PydanticRedisStorage
from sqlalchemy.ext.asyncio import AsyncEngine, async_scoped_session
from structlog import get_logger
from structlog.stdlib import BoundLogger

from .config import config
from .constants import (
    REDIS_BACKOFF_MAX,
    REDIS_BACKOFF_START,
    REDIS_POOL_SIZE,
    REDIS_POOL_TIMEOUT,
    REDIS_RETRIES,
    REDIS_TIMEOUT,
)
from .events import Events
from .models.state import RunningQuery
from .services.monitor import QueryMonitor
from .services.query import QueryService
from .services.results import ResultProcessor
from .storage.gafaelfawr import GafaelfawrClient
from .storage.qserv import QservClient
from .storage.rate import RateLimitStore
from .storage.state import QueryStateStore
from .storage.votable import VOTableWriter

__all__ = ["Factory", "ProcessContext"]


@dataclass(kw_only=True, slots=True)
class ProcessContext:
    """Per-process application context.

    This object caches all of the per-process singletons that can be reused
    for every incoming message and only need to be recreated if the
    application configuration changes.
    """

    http_client: AsyncClient
    """Shared HTTP client."""

    engine: AsyncEngine
    """Database engine."""

    kafka_broker: KafkaBroker
    """Kafka broker to use for publishing messages from background jobs."""

    event_manager: EventManager
    """Manager for publishing metrics events."""

    gafaelfawr_client: GafaelfawrClient
    """Shared caching Gafaelfawr client."""

    events: Events
    """Event publishers for metrics events."""

    redis: Redis
    """Connection pool for state-tracking Redis."""

    @classmethod
    async def create(
        cls,
        kafka_broker: KafkaBroker | None = None,
    ) -> Self:
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
        # Qserv currently uses a self-signed certificate.
        limits = Limits(max_connections=config.qserv_rest_max_connections)
        http_client = AsyncClient(
            timeout=config.qserv_rest_timeout.total_seconds(),
            limits=limits,
            verify=False,  # noqa: S501
        )

        # Qserv uses a self-signed certificate with no known certificate
        # chain. We do not use TLS to validate the identity of the server.
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Create a Redis client pool with exponential backoff and the state
        # store that stores job state in Redis.
        redis_password = config.redis_password.get_secret_value()
        backoff = ExponentialBackoff(
            base=REDIS_BACKOFF_START, cap=REDIS_BACKOFF_MAX
        )
        redis_pool = BlockingConnectionPool.from_url(
            str(config.redis_url),
            password=redis_password,
            max_connections=REDIS_POOL_SIZE,
            retry=Retry(backoff, REDIS_RETRIES),
            retry_on_timeout=True,
            socket_keepalive=True,
            socket_timeout=REDIS_TIMEOUT,
            timeout=REDIS_POOL_TIMEOUT,
        )
        redis_client = Redis.from_pool(redis_pool)

        # Create the database engine.
        engine = create_database_engine(
            str(config.qserv_database_url),
            config.qserv_database_password,
            connect_args={"ssl": ssl_context},
            max_overflow=config.qserv_database_overflow,
            pool_size=config.qserv_database_pool_size,
        )

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

        # Create an event manager for posting metrics events.
        event_manager = config.metrics.make_manager(kafka_broker=kafka_broker)
        await event_manager.initialize()
        events = Events()
        await events.initialize(event_manager)

        # Create a shared caching Gafaelfawr client.
        logger = get_logger("qservkafka")
        gafaelfawr_client = GafaelfawrClient(http_client, logger)

        # Return the newly-constructed context.
        return cls(
            http_client=http_client,
            engine=engine,
            kafka_broker=kafka_broker,
            event_manager=event_manager,
            gafaelfawr_client=gafaelfawr_client,
            events=events,
            redis=redis_client,
        )

    async def aclose(self) -> None:
        """Clean up a process context.

        Called during shutdown, or before recreating the process context using
        a different configuration.
        """
        await self.event_manager.aclose()
        await self.kafka_broker.stop()
        await self.redis.aclose()
        await self.engine.dispose()
        await self.http_client.aclose()


class Factory:
    """Build Qserv Kafka bridge components.

    Uses the contents of a `ProcessContext` to construct the components of the
    application on demand.

    Parameters
    ----------
    context
        Shared process context.
    session
        Database session.
    logger
        Logger to use for errors.
    """

    def __init__(
        self,
        context: ProcessContext,
        session: async_scoped_session,
        logger: BoundLogger,
    ) -> None:
        self._context = context
        self._session = session
        self._logger = logger

    @property
    def events(self) -> Events:
        """Global shared metrics events publishers, used by the test suite."""
        return self._context.events

    @property
    def gafaelfawr_client(self) -> GafaelfawrClient:
        """Global shared caching Gafaelfawr client."""
        return self._context.gafaelfawr_client

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

    def create_qserv_client(self) -> QservClient:
        """Create a client for talking to the Qserv API.

        Returns
        -------
        QservClient
            Client for the Qserv API.
        """
        return QservClient(
            session=self._session,
            http_client=self._context.http_client,
            events=self._context.events,
            logger=self._logger,
        )

    async def create_query_monitor(self) -> QueryMonitor:
        """Create the singleton monitor for query status.

        This is run as a background task.

        Returns
        -------
        QueryMonitor
            New service to monitor query status.
        """
        if config.arq_mode == ArqMode.production:
            settings = config.arq_redis_settings
            arq_queue: ArqQueue = await RedisArqQueue.initialize(
                settings, default_queue_name=config.arq_queue
            )
        else:
            arq_queue = MockArqQueue()
        return QueryMonitor(
            result_processor=self.create_result_processor(),
            qserv_client=self.create_qserv_client(),
            arq_queue=arq_queue,
            state_store=self.create_query_state_store(),
            rate_limit_store=self.create_rate_limit_store(),
            events=self._context.events,
            logger=self._logger,
        )

    def create_query_service(self) -> QueryService:
        """Create a new service for starting queries.

        Parameters
        ----------
        arq_queue
            arq queue to which to dispatch result handling jobs.

        Returns
        -------
        QueryService
            New service to start queries.
        """
        return QueryService(
            qserv_client=self.create_qserv_client(),
            state_store=self.create_query_state_store(),
            result_processor=self.create_result_processor(),
            rate_limit_store=self.create_rate_limit_store(),
            gafaelfawr_client=self.gafaelfawr_client,
            events=self._context.events,
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
            qserv_client=self.create_qserv_client(),
            state_store=self.create_query_state_store(),
            votable_writer=VOTableWriter(
                self._context.http_client, self._logger
            ),
            kafka_broker=self._context.kafka_broker,
            rate_limit_store=self.create_rate_limit_store(),
            events=self._context.events,
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
