"""Test support functions for arq queuing."""

import asyncio
import inspect
from copy import copy
from datetime import timedelta
from typing import Any

from arq import Worker
from safir.metrics.arq import initialize_arq_metrics

from qservkafka.config import config
from qservkafka.factory import ProcessContext
from qservkafka.workers.main import UploadWorkerSettings, WorkerSettings

__all__ = ["ArqWorkers"]


class ArqWorkers:
    """Container to manage qserv-kafka arq workers during tests.

    Parameters
    ----------
    context
        Process context to use, if given.
    """

    def __init__(self, context: ProcessContext) -> None:
        self._context = context
        self._metrics_initialized = False
        self._fast_worker = self._create_worker(UploadWorkerSettings)
        self._slow_worker = self._create_worker(WorkerSettings)

        self._fast_count = 0
        self._slow_count = 0

    async def run_workers(
        self,
        expected: int,
        *,
        only_fast: bool = False,
        only_slow: bool = False,
        timeout: timedelta = timedelta(seconds=5),
    ) -> int:
        """Run the arq workers until the given number have completed.

        There is an unpredictable delay in dispatching the job to the user
        through Redis, so poll both workers until at least the given expected
        number of workers have run and return the total number.

        Parameters
        ----------
        expected
            How many total workers should complete.
        only_fast
            Only run the fast queue.
        only_slow
            Only run the slow queue.
        timeout
            How long to wait for the given number of workers to complete.

        Returns
        -------
        int
            Number of jobs processed by the run that found at least one.

        Raises
        ------
        TimeoutError
            Raised if no job was processed within the timeout.
        """
        assert not (only_fast and only_slow)

        # This is an ugly hack to work around the assumption the Safir arq
        # metrics makes that each worker runs in a separate process. If each
        # worker initializes metrics separately, the metrics library complains
        # about double initializations; if neither does, it complains about
        # missing initializations. Initialize manually, once, and copy the
        # resulting data into both worker contexts.
        if not self._metrics_initialized:
            ctx: dict[str, Any] = {}
            await initialize_arq_metrics(self._context.event_manager, ctx)
            self._fast_worker.ctx.update(ctx)
            self._slow_worker.ctx.update(ctx)
            self._metrics_initialized = True

        # Now, run the workers as needed.
        async with asyncio.timeout(timeout.total_seconds()):
            count = 0
            while True:
                if not only_slow:
                    fast_count = await self._fast_worker.run_check()
                    count += fast_count - self._fast_count
                    self._fast_count = fast_count
                if not only_fast:
                    slow_count = await self._slow_worker.run_check()
                    count += slow_count - self._slow_count
                    self._slow_count = slow_count
                if count >= expected:
                    return count
                await asyncio.sleep(0.01)

    def _create_worker(self, settings: type[Any] = WorkerSettings) -> Worker:
        """Create an arq worker to run queued jobs.

        Parameters
        ----------
        settings
            arq worker settings class to use.

        Returns
        -------
        Worker
            arq worker.
        """
        ctx = copy(settings.ctx)
        ctx["context"] = self._context
        ctx["metrics_initialized"] = True
        settings.redis_settings = config.arq_redis_settings
        valid = set(inspect.signature(Worker).parameters.keys())
        params = {k: v for k, v in vars(settings).items() if k in valid}
        params.pop("ctx", None)
        return Worker(burst=True, ctx=ctx, **params)
