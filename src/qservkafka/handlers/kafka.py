"""Kafka router and consumers."""

from typing import Annotated

from fastapi import Depends
from faststream.kafka.fastapi import KafkaRouter
from structlog import get_logger

from ..config import config
from ..dependencies.context import ConsumerContext, context_dependency
from ..models.kafka import JobRun

kafka_router = KafkaRouter(
    **config.kafka.to_faststream_params(), logger=get_logger("qservkafka")
)
"""Faststream router for incoming Kafka requests."""

__all__ = ["kafka_router"]


@kafka_router.subscriber(
    config.job_run_topic, group_id=config.consumer_group_id
)
async def job_run(
    message: JobRun,
    context: Annotated[ConsumerContext, Depends(context_dependency)],
) -> None:
    context.rebind_logger(job_id=message.job_id, username=message.owner)
    context.logger.debug(
        "Received job run request",
        job=message.model_dump(mode="json", exclude_none=True),
    )
