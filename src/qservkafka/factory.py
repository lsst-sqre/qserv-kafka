"""Create Qserv Kafka bridge components."""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from typing import Self

from httpx import AsyncClient
from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from safir.database import create_async_session, create_database_engine
from safir.redis import PydanticRedisStorage
from sqlalchemy.ext.asyncio import AsyncEngine, async_scoped_session
from structlog import get_logger
from structlog.stdlib import BoundLogger

from .background import BackgroundTaskManager
from .config import config
from .constants import (
    REDIS_BACKOFF_MAX,
    REDIS_BACKOFF_START,
    REDIS_POOL_SIZE,
    REDIS_POOL_TIMEOUT,
    REDIS_RETRIES,
    REDIS_TIMEOUT,
)
from .kafkarouters import kafka_router
from .models.state import Query
from .services.monitor import QueryMonitor
from .services.query import QueryService
from .storage.qserv import QservClient
from .storage.state import QueryStateStore
from .storage.votable import VOTableWriter

__all__ = ["Factory", "ProcessContext"]


@dataclass(frozen=True, kw_only=True, slots=True)
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

    session: async_scoped_session
    """Session used by background jobs."""

    redis: Redis
    """Connection pool for state-tracking Redis."""

    state: QueryStateStore
    """Storage for running query state.

    This will eventually move out of the process context once the state has
    been moved into Qserv.
    """

    background: BackgroundTaskManager
    """Manager for background tasks."""

    @classmethod
    async def from_config(cls) -> Self:
        """Create a new process context from a database engine.

        Returns
        -------
        ProcessContext
            Shared context for a Qserv Kafka bridge process.
        """
        logger = get_logger("qservkafka")

        # Qserv currently uses a self-signed certificate.
        http_client = AsyncClient(timeout=30, verify=False)  # noqa: S501

        # Qserv uses a self-signed certificate with no known certificate
        # chain. We do not use TLS to validate the identity of the server.
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Create a Redis client pool with exponential backoff.
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
        redis_storage = PydanticRedisStorage(
            datatype=Query, redis=redis_client, key_prefix="query:"
        )

        state_store = QueryStateStore(redis_storage, logger)
        engine = create_database_engine(
            str(config.qserv_database_url),
            config.qserv_database_password,
            connect_args={"ssl": ssl_context},
        )
        session = await create_async_session(engine)
        qserv_client = QservClient(session, http_client, logger)
        votable_writer = VOTableWriter(http_client, logger)
        query_service = QueryService(
            qserv_client=qserv_client,
            state_store=state_store,
            votable_writer=votable_writer,
            kafka_broker=kafka_router.broker,
            logger=logger,
        )
        monitor = QueryMonitor(
            query_service=query_service,
            qserv_client=QservClient(session, http_client, logger),
            state_store=state_store,
            logger=logger,
        )
        background = BackgroundTaskManager(monitor, logger)

        return cls(
            http_client=http_client,
            engine=engine,
            session=session,
            redis=redis_client,
            state=state_store,
            background=background,
        )

    async def aclose(self) -> None:
        """Clean up a process context.

        Called during shutdown, or before recreating the process context using
        a different configuration.
        """
        await self.stop()
        await self.session.remove()
        await self.redis.aclose()
        await self.engine.dispose()
        await self.http_client.aclose()

    async def start(self) -> None:
        """Start the background tasks running."""
        await self.background.start()

    async def stop(self) -> None:
        """Stop the background tasks if they are running."""
        await self.background.stop()


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
        self._background_services_started = False

    def create_query_service(self) -> QueryService:
        """Create a new service for starting queries.

        Returns
        -------
        QueryService
            New service to start queries.
        """
        return QueryService(
            qserv_client=QservClient(
                self._session, self._context.http_client, self._logger
            ),
            state_store=self._context.state,
            votable_writer=VOTableWriter(
                self._context.http_client, self._logger
            ),
            kafka_broker=kafka_router.broker,
            logger=self._logger,
        )

    def set_logger(self, logger: BoundLogger) -> None:
        """Replace the internal logger.

        Used by the context dependency to update the logger for all
        newly-created components when it's rebound with additional context.

        Parameters
        ----------
        logger
            New logger.
        """
        self._logger = logger
