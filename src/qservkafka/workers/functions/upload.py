"""arq queue worker to start queries that include table uploads."""

from datetime import datetime
from typing import Any

from ...factory import Factory
from ...models.kafka import JobRun

__all__ = ["handle_upload_job"]


async def handle_upload_job(
    ctx: dict[Any, Any], job: JobRun, kafka_start: datetime | None
) -> None:
    """Start a query that includes table uploads.

    Parameters
    ----------
    ctx
        arq context.
    job
        Query job to start.
    kafka_start
        Time at which the Kafka message for the job was queued.
    """
    factory: Factory = ctx["factory"]
    query_service = factory.create_query_service()
    result_processor = factory.create_result_processor()
    status = await query_service.start_query(job, kafka_start)
    await result_processor.publish_status(status)
