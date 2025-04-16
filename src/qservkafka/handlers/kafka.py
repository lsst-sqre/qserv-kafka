"""Kafka router and consumers.

The `kafka_router` symbol must be imported from this module, not from its true
source at `~qservkafka.kafkarouters.kafka_router`, to ensure that the router
is properly configured with its subscribers and publishers. Otherwise, the
router will have no subscribers and no publishers and will thus do nothing.
"""

from typing import Annotated

from fastapi import Depends

from ..config import config
from ..dependencies.context import ConsumerContext, context_dependency
from ..kafkarouters import kafka_router
from ..models.kafka import JobRun, JobStatus

publisher = kafka_router.publisher(config.job_status_topic)
"""Publisher for status messages, defined separately for testing."""

__all__ = ["kafka_router", "publisher"]


@kafka_router.subscriber(
    config.job_run_topic, group_id=config.consumer_group_id
)
@publisher
async def job_run(
    message: JobRun,
    context: Annotated[ConsumerContext, Depends(context_dependency)],
) -> JobStatus:
    context.rebind_logger(job_id=message.job_id, username=message.owner)
    query_service = context.factory.create_query_service()
    return await query_service.start_query(message)
