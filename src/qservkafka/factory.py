"""Create Qserv Kafka bridge components."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from faststream.kafka.fastapi import KafkaRouter
from httpx import AsyncClient
from safir.database import create_async_session, create_database_engine
from sqlalchemy.ext.asyncio import AsyncEngine, async_scoped_session
from structlog import get_logger
from structlog.stdlib import BoundLogger

from .background import BackgroundTaskManager
from .config import config
from .services.monitor import QueryMonitor
from .services.query import QueryService
from .storage.qserv import QservClient
from .storage.state import QueryStateStore
from .storage.votable import VOTableWriter

kafka_router = KafkaRouter(
    **config.kafka.to_faststream_params(), logger=get_logger("qservkafka")
)
"""Faststream router for incoming Kafka requests."""

__all__ = ["Factory", "ProcessContext", "kafka_router"]


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
        http_client = AsyncClient(verify=False)  # noqa: S501

        state_store = QueryStateStore(logger)
        engine = create_database_engine(
            str(config.qserv_database_url), config.qserv_database_password
        )
        session = await create_async_session(engine)
        monitor = QueryMonitor(
            qserv_client=QservClient(session, http_client, logger),
            state_store=state_store,
            votable_writer=VOTableWriter(http_client, logger),
            kafka_broker=kafka_router.broker,
            logger=logger,
        )
        background = BackgroundTaskManager(monitor, logger)

        return cls(
            http_client=http_client,
            engine=engine,
            session=session,
            state=state_store,
            background=background,
        )

    async def aclose(self) -> None:
        """Clean up a process context.

        Called during shutdown, or before recreating the process context using
        a different configuration.
        """
        await self.background.stop()
        await self.session.remove()
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
            votable_writer=VOTableWriter(
                self._context.http_client, self._logger
            ),
            state_store=self._context.state,
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

    async def start_background_services(self) -> None:
        """Start global background services managed by the process context.

        These are normally started by initializing the context dependency when
        running as a FastAPI app, but the test suite may want the background
        processes running while testing with only a factory.

        Only used by the test suite.
        """
        await self._context.start()
        self._background_services_started = True

    async def stop_background_services(self) -> None:
        """Stop global background services managed by the process context.

        These are normally stopped when closing down the global context, but
        the test suite may want to stop and start them independently.

        Only used by the test suite.
        """
        if self._background_services_started:
            await self._context.stop()
        self._background_services_started = False
