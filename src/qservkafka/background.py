"""Background task management."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from aiojobs import Scheduler
from structlog.stdlib import BoundLogger

from .config import config
from .services.monitor import QueryMonitor

__all__ = ["BackgroundTaskManager"]


class BackgroundTaskManager:
    """Manage Qserv Kafka bridge background tasks.

    While the Qserv Kafka bridge is running, it needs to periodically check
    the status of running jobs in Qserv and send status updates for any that
    have changed. This class manages that background task and its schedule.
    It only does the task management; all of the work of these tasks is done
    by methods on the underlying service objects.

    This class is created during startup and tracked as part of the
    `~controller.factory.ProcessContext`.

    Parameters
    ----------
    monitor
        Query monitor.
    logger
        Logger to use.
    """

    def __init__(self, monitor: QueryMonitor, logger: BoundLogger) -> None:
        self._monitor = monitor
        self._logger = logger
        self._scheduler: Scheduler | None = None

    async def start(self) -> None:
        """Start all background tasks.

        Intended to be called during Qserv Kafka bridge startup.
        """
        if self._scheduler:
            msg = "Background tasks already running, cannot start"
            self._logger.warning(msg)
            return
        self._scheduler = Scheduler()
        coros = [
            self._loop(
                self._monitor.check_status,
                config.qserv_poll_interval,
                "polling query status",
            )
        ]
        self._logger.info("Starting background tasks")
        for coro in coros:
            await self._scheduler.spawn(coro)

        # Give all of the newly-spawned background tasks a chance to start.
        await asyncio.sleep(0)

    async def stop(self) -> None:
        """Stop the background tasks."""
        if not self._scheduler:
            msg = "Background tasks were already stopped"
            self._logger.warning(msg)
            return
        self._logger.info("Stopping background tasks")
        await self._scheduler.close()
        self._scheduler = None

    async def _loop(
        self,
        call: Callable[[], Awaitable[None]],
        interval: timedelta,
        description: str,
    ) -> None:
        """Wrap a coroutine in a periodic scheduling loop.

        The provided coroutine is run on every interval. This method always
        delays by the interval first before running the coroutine for the
        first time.

        Parameters
        ----------
        call
            Async function to run repeatedly.
        interval
            Scheduling interval to use. This is how long to wait between each
            run, **not** the interval between the start of each run.
        description
            Description of the background task for error reporting.
        """
        await asyncio.sleep(interval.total_seconds())
        while True:
            start = datetime.now(tz=UTC)
            try:
                await call()
            except Exception:
                # On failure, log the exception but otherwise continue as
                # normal, including the delay. This will provide some time for
                # whatever the problem was to be resolved.
                elapsed = datetime.now(tz=UTC) - start
                msg = f"Uncaught exception {description}"
                self._logger.exception(msg, delay=elapsed.total_seconds)
            await asyncio.sleep(interval.total_seconds())
