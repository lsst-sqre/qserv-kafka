"""Utilities for reading test data."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from qservkafka.models.kafka import JobCancel, JobRun, JobStatus
from qservkafka.models.qserv import QservAsyncStatusData

from ..support.constants import ANY_DATETIME, ANY_OPTIONAL_DATETIME

__all__ = [
    "read_test_data",
    "read_test_job_run",
    "read_test_job_status",
    "read_test_job_status_json",
    "read_test_json",
    "read_test_qserv_status",
]


def read_test_data(filename: str) -> str:
    """Read an input data file and return its contents.

    Parameters
    ----------
    config
        Configuration from which to read data (the name of one of the
        directories under :file:`tests/data`).
    filename
        File to read.

    Returns
    -------
    str
        Contents of the file.
    """
    return (Path(__file__).parent.parent / "data" / filename).read_text()


def read_test_json(filename: str) -> Any:
    """Read test data as JSON and return its decoded form.

    Any ``<ANY>`` strings in the JSON are converted to `unittest.mock.ANY`
    after being read in.

    Parameters
    ----------
    filename
        File to read relative to the test data directory, without any
        ``.json`` suffix. Must be in JSON format.
    mock

    Returns
    -------
    typing.Any
        Parsed contents of the file.
    """
    path = Path(__file__).parent.parent / "data" / (filename + ".json")
    with path.open("r") as f:
        return json.load(f)


def read_test_job_cancel(filename: str) -> JobCancel:
    """Read test data parsed as a Kafka message to cancel a query.

    Parameters
    ----------
    filename
        File to read relative to the test cancel directory, without the
        ``.json`` suffix.

    Returns
    -------
    JobCancel
        Parsed contents of the file.
    """
    return JobCancel.model_validate(read_test_json(f"cancel/{filename}"))


def read_test_job_run(filename: str) -> JobRun:
    """Read test data parsed as a Kafka message to run a query.

    Parameters
    ----------
    filename
        File to read relative to the test jobs directory, without the
        ``.json`` suffix.

    Returns
    -------
    JobRun
        Parsed contents of the file.
    """
    return JobRun.model_validate(read_test_json(f"jobs/{filename}"))


def read_test_job_run_json(filename: str) -> JobRun:
    """Read test data parsed as JSON to run a query.

    Parameters
    ----------
    filename
        File to read relative to the test jobs directory, without the
        ``.json`` suffix.

    Returns
    -------
    JobRun
        Parsed contents of the file.
    """
    return read_test_json(f"jobs/{filename}")


def read_test_job_status(filename: str) -> JobStatus:
    """Read test data parsed as a Kafka job status message.

    Parameters
    ----------
    filename
        File to read relative to the test status directory, without the
        ``.json`` suffix.

    Returns
    -------
    JobStatus
        Parsed contents of the file.
    """
    result = JobStatus.model_validate(read_test_json(f"status/{filename}"))
    result.timestamp = ANY_DATETIME
    if result.query_info:
        result.query_info.start_time = ANY_DATETIME
        if result.query_info.end_time:
            result.query_info.end_time = ANY_OPTIONAL_DATETIME
    return result


def read_test_job_status_json(filename: str) -> dict[str, Any]:
    """Read JSON for a Kafka job status message.

    Parameters
    ----------
    filename
        File to read relative to the test status directory, without the
        ``.json`` suffix.

    Returns
    -------
    JobStatus
        Parsed contents of the file.
    """
    model = JobStatus.model_validate(read_test_json(f"status/{filename}"))
    result = model.model_dump(mode="json")
    result["timestamp"] = ANY_DATETIME
    if result.get("queryInfo"):
        result["queryInfo"]["startTime"] = ANY_DATETIME
        if result["queryInfo"].get("endTime"):
            result["queryInfo"]["endTime"] = ANY_DATETIME
    return result


def read_test_qserv_status(
    filename: str,
    *,
    query_id: int | None = None,
    query_begin: datetime | None = None,
    last_update: datetime | None = None,
) -> QservAsyncStatusData:
    """Read the result of q Qserv query status API call.

    Parameters
    ----------
    filename
        File to read relative to the test :file:`qserv` directory, without the
        ``.json`` suffix.
    query_id
        Override the ``execution_id`` number.
    query_begin
        Override the ``query_begin`` timestamp.
    last_update
        Override the ``last_update`` timestamp.

    Returns
    -------
    QservAsyncStatusData
        Parsed contents of the file.
    """
    model_data = read_test_json(f"qserv/{filename}")
    model = QservAsyncStatusData.model_validate(model_data)
    if query_id:
        model.query_id = query_id
    if query_begin:
        model.query_begin = query_begin.replace(microsecond=0)
    if last_update:
        model.last_update = last_update.replace(microsecond=0)
    return model
