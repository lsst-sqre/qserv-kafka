"""Test support for driving queriess."""

from datetime import datetime

from vo_models.uws.types import ExecutionPhase

from qservkafka.factory import Factory
from qservkafka.models.kafka import JobRun, JobStatus
from qservkafka.models.state import Query

from .kafka import read_status_message

__all__ = ["start_and_complete_immediate"]


async def start_and_complete_immediate(
    factory: Factory,
    job: JobRun,
    *,
    kafka_start: datetime | None = None,
) -> JobStatus:
    """Start a query that completes immediately and wait for its result.

    Queries found already complete on their first status check are
    dispatched to the result worker, so this also simulates that worker
    running by calling the result processor directly while avoiding the arq
    queue and workers, since there is no easy way to patch the arq workers to
    not use Kafka.

    Parameters
    ----------
    factory
        Component factory to use.
    job
        Job to start.
    kafka_start
        Time at which the Kafka message for the job was queued.

    Returns
    -------
    JobStatus
        The completed status of the job.
    """
    query_service = factory.create_query_service()
    state_store = factory.create_query_state_store()

    await query_service.start_query(Query(job=job, queued=kafka_start))
    status = read_status_message(factory)
    assert status.status == ExecutionPhase.EXECUTING
    assert status.execution_id
    query = await state_store.get_query(status.execution_id)
    assert query

    await query_service.update_query(query)
    await query_service.delete_query_data(query)
    return read_status_message(factory)
