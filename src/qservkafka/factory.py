"""Create Qserv Kafka bridge components."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from httpx import AsyncClient
from structlog.stdlib import BoundLogger

from .services.query import QueryService
from .storage.rest import QservRestClient
from .storage.state import QueryStateStore

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

    @classmethod
    async def from_config(cls) -> Self:
        """Create a new process context from the configuration.

        Returns
        -------
        ProcessContext
            Shared context for a Qserv Kafka bridge process.
        """
        # Qserv currently uses a self-signed certificate.
        http_client = AsyncClient(verify=False)  # noqa: S501
        return cls(http_client=http_client)

    async def aclose(self) -> None:
        """Clean up a process context.

        Called during shutdown, or before recreating the process context using
        a different configuration.
        """
        await self.http_client.aclose()


class Factory:
    """Build Qserv Kafka bridge components.

    Uses the contents of a `ProcessContext` to construct the components of the
    application on demand.

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

    def create_query_service(self) -> QueryService:
        """Create a new service for starting queries.

        Returns
        -------
        QueryService
            New service to start queries.
        """
        rest_store = QservRestClient(self._context.http_client, self._logger)
        state_store = QueryStateStore(self._logger)
        return QueryService(rest_store, state_store, self._logger)

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
