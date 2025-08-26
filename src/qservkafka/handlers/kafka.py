"""Kafka router and consumers.

The `kafka_router` symbol must be imported from this module, not from its true
source at `~qservkafka.kafkarouters.kafka_router`, to ensure that the router
is properly configured with its subscribers and publishers. Otherwise, the
router will have no subscribers and no publishers and will thus do nothing.
"""

import asyncio
from typing import Annotated

from fastapi import Depends
from faststream.kafka.fastapi import KafkaRouter

from ..config import config
from ..dependencies.context import ConsumerContext, context_dependency
from ..models.kafka import JobCancel, JobRun

__all__ = ["register_kafka_handlers"]


async def job_run(
    messages: list[JobRun],
    context: Annotated[ConsumerContext, Depends(context_dependency)],
) -> None:
    query_service = context.factory.create_query_service()
    jobs = [query_service.handle_query(m) for m in messages]
    await asyncio.gather(*jobs)


async def job_cancel(
    message: JobCancel,
    context: Annotated[ConsumerContext, Depends(context_dependency)],
) -> None:
    query_service = context.factory.create_query_service()
    await query_service.handle_cancel(message)


def register_kafka_handlers(kafka_router: KafkaRouter) -> None:
    """Register the Kafka message handlers with the router.

    This is done dynamically via a function instead of statically with
    decorators to allow the Kafka router to be constructed after the test
    suite has set up the Kafka configuration.

    Parameters
    ----------
    kafka_router
        Kafka router to register handlers with.
    """
    kafka_router.subscriber(
        config.job_run_topic,
        auto_offset_reset="earliest",
        batch=True,
        group_id=config.consumer_group_id,
        max_records=config.job_run_batch_size,
    )(job_run)
    kafka_router.subscriber(
        config.job_cancel_topic,
        auto_offset_reset="earliest",
        group_id=config.consumer_group_id,
    )(job_cancel)
