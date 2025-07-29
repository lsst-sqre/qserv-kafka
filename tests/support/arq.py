"""Test support functions for arq queuing."""

from __future__ import annotations

import inspect

from arq import Worker

from qservkafka.config import config
from qservkafka.factory import ProcessContext
from qservkafka.workers.main import WorkerSettings

__all__ = ["run_arq_jobs"]


async def run_arq_jobs(context: ProcessContext | None = None) -> int:
    """Run any queued arq jobs.

    Returns
    -------
    int
        Number of jobs run.
    """
    ctx = {}
    if context:
        ctx["context"] = context
    WorkerSettings.redis_settings = config.arq_redis_settings
    worker_args = set(inspect.signature(Worker).parameters.keys())
    worker = Worker(
        burst=True,
        ctx=ctx,
        **{k: v for k, v in vars(WorkerSettings).items() if k in worker_args},
    )
    return await worker.run_check()
