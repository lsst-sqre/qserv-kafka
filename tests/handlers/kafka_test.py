"""Tests for the Kafka message handlers."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from faststream.kafka import KafkaBroker
from faststream.types import SendableTable

from qservkafka.config import config
from qservkafka.models.kafka import JobRun

from ..support.logging import parse_log


@pytest.mark.asyncio
async def test_job_run(
    app: FastAPI,
    kafka_broker: KafkaBroker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    job: SendableTable = {
        "query": "SELECT TOP 10 * FROM table",
        "database": "dp1",
        "jobID": "uws123",
        "ownerID": "username",
        "resultDestination": "https://gcs.example.com/upload",
        "resultFormat": {
            "type": "votable",
            "serialization": "BINARY2",
            "envelope": {
                "header": (
                    '<VOTable xmlns="http://www.ivoa.net/xml/VOTable/v1.3'
                    ' version="1.3"><RESOURCE type="results"><TABLE>'
                    '<FIELD ID="col_0" arraysize="*" datatype="char"'
                    ' name="col1"/>'
                ),
                "footer": "</TABLE></RESOURCE></VOTable>",
            },
        },
    }
    job_model = JobRun.model_validate(job)

    caplog.clear()
    await kafka_broker.publish(job, config.job_run_topic)
    assert [m for m in parse_log(caplog) if m.get("job_id")] == [
        {
            "event": "Received job run request",
            "job": job_model.model_dump(mode="json", exclude_none=True),
            "job_id": "uws123",
            "kafka": {
                "offset": 0,
                "partition": 0,
                "topic": config.job_run_topic,
            },
            "severity": "debug",
            "username": "username",
        }
    ]
