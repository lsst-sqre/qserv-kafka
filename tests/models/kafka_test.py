"""Tests for the Kafka models."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from vo_models.uws.types import ExecutionPhase

from qservkafka.models.kafka import (
    JobError,
    JobErrorCode,
    JobMetadata,
    JobQueryInfo,
    JobResultInfo,
    JobResultSerialization,
    JobResultType,
    JobRun,
    JobStatus,
)


def test_job_run() -> None:
    job = JobRun.model_validate(
        {
            "query": "SELECT TOP 10 * FROM table",
            "database": "dp1",
            "jobID": "uws123",
            "ownerID": "me",
            "resultDestination": (
                "https://bucket/results_uws123.xml?X-Goog-Signature=a82c76..."
            ),
            "resultFormat": {
                "type": "votable",
                "serialization": "BINARY2",
                "envelope": {
                    "header": (
                        '<VOTable xmlns="http://www.ivoa.net/xml/VOTable'
                        '/v1.3" version="1.3"><RESOURCE type="results">'
                        '<TABLE><FIELD ID="col_0" arraysize="*" '
                        'datatype="char" name="col1"/>'
                    ),
                    "footer": "</TABLE></RESOURCE></VOTable>",
                },
            },
        }
    )
    assert job.owner == "me"
    assert job.result_format.serialization == JobResultSerialization.BINARY2
    assert job.result_format.envelope.footer == "</TABLE></RESOURCE></VOTable>"


def test_job_status() -> None:
    now = datetime.now(tz=UTC)
    start = now - timedelta(hours=1)
    end = now - timedelta(seconds=5)

    # Success.
    status = JobStatus(
        job_id="uws-123",
        execution_id="123",
        timestamp=now,
        status=ExecutionPhase.COMPLETED,
        query_info=JobQueryInfo(
            start_time=start,
            end_time=end,
            total_chunks=167,
            completed_chunks=167,
        ),
        result_info=JobResultInfo(
            total_rows=1000,
            format=JobResultType.votable,
            serialization=JobResultSerialization.BINARY2,
        ),
        metadata=JobMetadata(
            query="SELECT TOP 10 * FROM table", database="dp1"
        ),
    )
    assert status.model_dump(mode="json", exclude_none=True) == {
        "jobID": "uws-123",
        "executionID": "123",
        "timestamp": int(now.timestamp() * 1000),
        "status": "COMPLETED",
        "queryInfo": {
            "startTime": int(start.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000),
            "totalChunks": 167,
            "completedChunks": 167,
        },
        "resultInfo": {
            "totalRows": 1000,
            "format": "votable",
            "serialization": "BINARY2",
        },
        "metadata": {"query": "SELECT TOP 10 * FROM table", "database": "dp1"},
    }

    # Failure with error.
    status = JobStatus(
        job_id="uws-123",
        execution_id="123",
        timestamp=now,
        status=ExecutionPhase.ERROR,
        query_info=JobQueryInfo(
            start_time=start,
            end_time=end,
            total_chunks=3,
            completed_chunks=1,
        ),
        error=JobError(
            code=JobErrorCode.backend_error, message="Syntax Error at line 1"
        ),
        metadata=JobMetadata(
            query="SELECT TOP 10 * FROM dp1.Table", database="dp1"
        ),
    )
    assert status.model_dump(mode="json", exclude_none=True) == {
        "jobID": "uws-123",
        "executionID": "123",
        "timestamp": int(now.timestamp() * 1000),
        "status": "ERROR",
        "queryInfo": {
            "startTime": int(start.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000),
            "totalChunks": 3,
            "completedChunks": 1,
        },
        "errorInfo": {
            "errorCode": "backend_error",
            "errorMessage": "Syntax Error at line 1",
        },
        "metadata": {
            "query": "SELECT TOP 10 * FROM dp1.Table",
            "database": "dp1",
        },
    }
