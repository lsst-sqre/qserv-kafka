"""Test support for driving queriess."""

from datetime import datetime

from vo_models.uws.types import ExecutionPhase

from qservkafka.factory import Factory
from qservkafka.models.kafka import JobRun, JobStatus
from qservkafka.services.query import QueryService

__all__ = ["start_and_complete_immediate"]


async def start_and_complete_immediate(
    query_service: QueryService,
    factory: Factory,
    job: JobRun,
    *,
    kafka_start: datetime | None = None,
) -> JobStatus:
    """Start a query that completes immediately and wait for its result.

    Queries found already complete on their first status check are
    dispatched to the result worker, so this also simulates that worker
    running by calling the result processor directly, just like
    ``handle_finished_query`` does.

    Parameters
    ----------
    query_service
        Query service to start the job with.
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
    initial_status = await query_service.start_query(job, kafka_start)
    assert initial_status.status == ExecutionPhase.EXECUTING
    assert initial_status.execution_id

    state_store = factory.create_query_state_store()
    query = await state_store.get_query(initial_status.execution_id)
    assert query

    result_processor = factory.create_result_processor()
    return await result_processor.handle_completed_query(query)
