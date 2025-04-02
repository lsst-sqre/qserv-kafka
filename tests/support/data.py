"""Utilities for reading test data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import ANY

from qservkafka.models.kafka import JobRun, JobStatus

__all__ = [
    "read_test_job_run",
    "read_test_job_status",
    "read_test_json",
]


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


def read_test_job_run(filename: str) -> JobRun:
    """Read test data parsed as a Kafka message to run a query.

    Parameters
    ----------
    filename
        File to read relative to the test data directory, without the
        ``.json`` suffix.

    Returns
    -------
    JobRun
        Parsed contents of the file.
    """
    return JobRun.model_validate(read_test_json(filename))


def read_test_job_status(
    filename: str, *, mock_timestamps: bool = True
) -> JobStatus:
    """Read test data parsed as a Kafka job status message.

    Parameters
    ----------
    filename
        File to read relative to the test data directory, without the
        ``.json`` suffix.
    mock_timestamps
        Whether to mock out the timestamps so they'll match any time.

    Returns
    -------
    JobStatus
        Parsed contents of the file.
    """
    result = JobStatus.model_validate(read_test_json(filename))
    if mock_timestamps:
        result.timestamp = ANY
        if result.query_info:
            result.query_info.start_time = ANY
            if result.query_info.end_time:
                result.query_info.end_time = ANY
    return result
