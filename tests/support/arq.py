"""Test support functions for arq queuing."""

import inspect

from arq import Worker

from qservkafka.config import config
from qservkafka.factory import ProcessContext
from qservkafka.workers.main import WorkerSettings

__all__ = ["create_arq_worker"]


def create_arq_worker(context: ProcessContext | None = None) -> Worker:
    """Create an arq worker to run queued jobs.

    Parameters
    ----------
    context
        Process context to use, if given.

    Returns
    -------
    Worker
        arq worker.
    """
    ctx = {}
    if context:
        ctx["context"] = context
    WorkerSettings.redis_settings = config.arq_redis_settings
    worker_args = set(inspect.signature(Worker).parameters.keys())
    return Worker(
        burst=True,
        ctx=ctx,
        **{k: v for k, v in vars(WorkerSettings).items() if k in worker_args},
    )
