"""Background task management."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta

from aiojobs import Scheduler
from safir.sentry import report_exception
from safir.slack.webhook import SlackWebhookClient
from structlog.stdlib import BoundLogger

from .config import config
from .constants import RATE_LIMIT_RECONCILE_INTERVAL
from .services.monitor import QueryMonitor

__all__ = ["BackgroundTaskManager"]


class BackgroundTaskManager:
    """Manage Qserv Kafka bridge background tasks.

    While the Qserv Kafka bridge is running, it needs to periodically check
    the status of running jobs in Qserv and send status updates for any that
    have changed, and periodically reconcile the quota information in Redis.
    This class manages those background tasks and their schedules. It only
    does the task management; all of the work of these tasks is done by
    methods on the underlying service objects.

    Parameters
    ----------
    monitor
        Query monitor.
    slack_client
        Client to send errors to Slack
    logger
        Logger to use.
    """

    def __init__(
        self,
        monitor: QueryMonitor,
        slack_client: SlackWebhookClient | None,
        logger: BoundLogger,
    ) -> None:
        self._monitor = monitor
        self._slack_client = slack_client
        self._logger = logger
        self._closing = asyncio.Event()
        self._scheduler: Scheduler | None = None

    async def start(self) -> None:
        """Start all background tasks.

        Normally called during Qserv Kafka bridge startup from the lifespan
        hook. The caller is responsible for calling `stop` during shutdown.
        """
        if self._scheduler:
            msg = "Background tasks already running, cannot start"
            self._logger.warning(msg)
            return
        self._scheduler = Scheduler()

        # Start the background tasks.
        coros = [
            self._loop(
                self._monitor.check_status,
                config.qserv_poll_interval,
                "polling query status",
            ),
            self._loop(
                self._monitor.reconcile_rate_limits,
                RATE_LIMIT_RECONCILE_INTERVAL,
                "reconciling rate limits",
            ),
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
        self._closing.set()
        timeout = config.result_timeout.total_seconds() + 1.0
        await self._scheduler.wait_and_close(timeout=timeout)
        self._scheduler = None
        self._closing = asyncio.Event()

    async def _loop(
        self,
        call: Callable[[], Awaitable[None]],
        interval: timedelta,
        description: str,
    ) -> None:
        """Wrap a coroutine in a periodic scheduling loop.

        The provided coroutine is run on every interval. Each execution runs
        to completion; shutdown, handled by setting the ``self._closing``
        event, is checked after the execution of the coroutine and during the
        delay before it is run again.

        Parameters
        ----------
        call
            Async function to run repeatedly.
        interval
            Scheduling interval to use. This is how long to wait between each
            run, **not** the interval between the start of each run.
        description
            Description of the background task for error reporting.

        Notes
        -----
        A previous version of this method instead cancelled the individual
        background tasks during `stop` using `~aiojobs.Job.close` and
        protected the execution of the underlying periodic task with
        `~aiojobs.Scheduler.shield` so that it would still run to completion.
        This works, but with Python 3.13.5 and aiojobs 1.4.0 it causes a
        memory leak, apparently of data associated with each periodic task.

        Because of that bug, this implementation instead uses an
        `asyncio.Event` to signal shutdown, uses `asyncio.wait_for` on that
        event variable to implement the delay between executions, and avoids
        calling `~aiojobs.Scheduler.close` or `aiojobs.Job.close`, instead
        waiting for the jobs to finish on their own after the event variable
        is set. This appears to eliminate that cause of a memory leak.
        """
        delay = interval.total_seconds()
        while not self._closing.is_set():
            start = datetime.now(tz=UTC)
            try:
                await call()
            except Exception as e:
                # On failure, log the exception but otherwise continue as
                # normal, including the delay. This will provide some time for
                # whatever the problem was to be resolved.
                elapsed = datetime.now(tz=UTC) - start
                await report_exception(e, slack_client=self._slack_client)
                msg = f"Uncaught exception {description}"
                self._logger.exception(msg, delay=elapsed.total_seconds())
            with suppress(TimeoutError):
                await asyncio.wait_for(self._closing.wait(), delay)
