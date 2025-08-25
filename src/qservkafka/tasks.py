"""Simple asyncio task manager for query tasks.

This is roughly equivalent to the functionality of aiojobs except that it
requires a periodic background task reap all of the completed tasks
explicitly. The original attempt at starting queries in short-lived tasks used
aiojobs, but aiojobs leaks memory like a sieve with this use case (at least in
version 1.4.0).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Coroutine

from structlog.stdlib import BoundLogger

__all__ = ["TaskManager"]


class TaskManager:
    """Manage a queue of background tasks.

    Start background tasks up to a concurrency limit, and then queue them for
    later execution once tasks have been drained. Do not attempt to
    automatically detect completion of tasks; instead, provide a
    ``reap_tasks`` method that should be called periodically and waits for all
    current running tasks to complete, and then starts a new batch of tasks.

    This class only works with tasks that return `None`. It is narrowly
    designed for the specific needs of the Qserv Kafka bridge.

    Parameters
    ----------
    logger
        Logger to use.
    concurrency_limit
        Maximum number of tasks to run at a time.
    """

    def __init__(
        self, logger: BoundLogger, *, concurrency_limit: int = 100
    ) -> None:
        self._logger = logger
        self._limit = concurrency_limit

        self._tasks: list[Awaitable[None]] = []
        self._queue: list[Coroutine[None, None, None]] = []

    async def reap_tasks(self) -> int:
        """Wait for all running tasks and start queued tasks if needed.

        Returns
        -------
        int
            Number of tasks reaped. This is used primarily so that
            `reap_tasks` can be called repeatedly on shutdown until all tasks
            are drained.
        """
        if not self._tasks:
            return 0
        tasks = self._tasks
        self._tasks = []
        reaped = 0
        for task in tasks:
            try:
                await task
            except Exception:
                self._logger.exception("Creating query failed")
            reaped += 1
            if self._queue:
                coro = self._queue.pop(0)
                self._tasks.append(asyncio.create_task(coro))
        return reaped

    def start(self, coro: Coroutine[None, None, None]) -> None:
        """Start a new task, or queue it if the concurrency limit is reached.

        Parameters
        ----------
        coro
            Coroutine to start.
        """
        if len(self._tasks) < self._limit:
            self._tasks.append(asyncio.create_task(coro))
        else:
            self._queue.append(coro)
