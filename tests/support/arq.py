"""Test support functions for arq queuing."""

import asyncio
import inspect
from copy import copy
from datetime import timedelta
from typing import Any

from arq import Worker

from qservkafka.config import config
from qservkafka.factory import Factory, ProcessContext
from qservkafka.workers.main import WorkerSettings

__all__ = [
    "create_arq_worker",
    "run_worker_until_processed",
    "wait_for_dispatch",
]


def create_arq_worker(
    context: ProcessContext | None = None,
    *,
    settings: type[Any] = WorkerSettings,
) -> Worker:
    """Create an arq worker to run queued jobs.

    Parameters
    ----------
    context
        Process context to use, if given.
    settings
        arq worker settings class to use (default `WorkerSettings`).

    Returns
    -------
    Worker
        arq worker.
    """
    ctx = copy(settings.ctx)
    if context:
        ctx["context"] = context
    settings.redis_settings = config.arq_redis_settings
    worker_args = set(inspect.signature(Worker).parameters.keys())
    return Worker(
        burst=True,
        ctx=ctx,
        **{
            k: v
            for k, v in vars(settings).items()
            if k in worker_args and k != "ctx"
        },
    )


async def run_worker_until_processed(
    worker: Worker,
    *,
    timeout: timedelta = timedelta(seconds=5),
) -> int:
    """Run an arq worker repeatedly until it processes a job,
    to handle the delay during upload jobs between the Kafka consumer
    enqueuing a job and it becoming visible to the worker.

    Parameters
    ----------
    worker
        Worker to run.
    timeout
        How long to wait for a job to appear before giving up.

    Returns
    -------
    int
        Number of jobs processed by the run that found at least one.

    Raises
    ------
    TimeoutError
        Raised if no job was processed within the timeout.
    """
    async with asyncio.timeout(timeout.total_seconds()):
        while True:
            count = await worker.run_check()
            if count:
                return count
            await asyncio.sleep(0.05)


async def wait_for_dispatch(
    factory: Factory,
    query_id: int,
    *,
    timeout: timedelta = timedelta(seconds=1),
) -> None:
    """Wait for a job to be queued for the result worker.

    Parameters
    ----------
    factory
        Component factory to use.
    query_id
        Qserv query ID.
    timeout
        How long to wait for the dispatch before giving up.

    Raises
    ------
    TimeoutError
        Raised if it takes more than the timeout interval for the job to be
        dispatched to the backend worker.
    """
    state_store = factory.create_query_state_store()

    # Use polling of Redis, since subscribing to key updates in Redis is
    # complicated enough that I don't feel like writing all that code.
    poll_delay = config.backend_poll_interval.total_seconds() / 2
    async with asyncio.timeout(timeout.total_seconds()):
        while True:
            query = await state_store.get_query(str(query_id))
            assert query
            if query.result_queued:
                return
            await asyncio.sleep(poll_delay)
