"""Kafka router and consumers."""

from typing import Annotated

from fastapi import Depends
from faststream.kafka.fastapi import KafkaRouter
from structlog import get_logger

from ..config import config
from ..dependencies.context import ConsumerContext, context_dependency
from ..models.kafka import JobRun, JobStatus

kafka_router = KafkaRouter(
    **config.kafka.to_faststream_params(), logger=get_logger("qservkafka")
)
"""Faststream router for incoming Kafka requests."""

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
