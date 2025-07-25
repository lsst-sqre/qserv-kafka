"""Kafka router and consumers.

The `kafka_router` symbol must be imported from this module, not from its true
source at `~qservkafka.kafkarouters.kafka_router`, to ensure that the router
is properly configured with its subscribers and publishers. Otherwise, the
router will have no subscribers and no publishers and will thus do nothing.
"""

from typing import Annotated

from fastapi import Depends
from faststream.kafka.fastapi import KafkaRouter

from ..config import config
from ..dependencies.context import ConsumerContext, context_dependency
from ..models.kafka import JobCancel, JobRun, JobStatus

__all__ = ["register_kafka_handlers"]


async def job_run(
    message: JobRun,
    context: Annotated[ConsumerContext, Depends(context_dependency)],
) -> JobStatus:
    query_service = context.factory.create_query_service()
    return await query_service.start_query(message)


async def job_cancel(
    message: JobCancel,
    context: Annotated[ConsumerContext, Depends(context_dependency)],
) -> None:
    query_service = context.factory.create_query_service()
    result = await query_service.cancel_query(message)
    if result:
        processor = context.factory.create_result_processor()
        await processor.publish_status(result)


def register_kafka_handlers(kafka_router: KafkaRouter) -> None:
    """Register the Kafka message handlers with the router.

    This is done dynamically via a function instead of statically with
    decorators to allow the Kafka router to be constructed after the test
    suite has set up the Kafka configuration.

    Parameters
    ----------
    kafka_router
        Kafka router to register handlers with.
    status_publisher
        If not `None`, use this publisher to handle the return value of query
        processing. This is only used by the test suite.
    """
    status_publisher = kafka_router.publisher(config.job_status_topic)
    kafka_router.subscriber(
        config.job_run_topic,
        auto_offset_reset="earliest",
        group_id=config.consumer_group_id,
    )(status_publisher(job_run))
    kafka_router.subscriber(
        config.job_cancel_topic,
        auto_offset_reset="earliest",
        group_id=config.consumer_group_id,
    )(job_cancel)
