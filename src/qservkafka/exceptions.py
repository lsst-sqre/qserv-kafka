"""Custom exceptions for the Qserv Kafka bridge."""

from __future__ import annotations

from typing import ClassVar, override

from safir.slack.blockkit import (
    SlackCodeBlock,
    SlackException,
    SlackMessage,
    SlackTextField,
    SlackWebException,
)

from .models.kafka import JobError, JobErrorCode
from .models.qserv import BaseResponse

__all__ = [
    "QservApiError",
    "QservApiFailedError",
    "QservApiProtocolError",
    "QservApiWebError",
]


class QservApiError(SlackException):
    """Base class for failures talking to the Qserv API."""

    error: ClassVar[JobErrorCode] = JobErrorCode.backend_error

    def to_job_error(self) -> JobError:
        """Convert to a `~qservkafka.models.kafka.JobError` for reporting.

        Returns
        -------
        JobError
            Corresponding job error.
        """
        return JobError(code=self.error, message=str(self))


class QservApiFailedError(QservApiError):
    """A Qserv API request returned failure.

    Parameters
    ----------
    url
        URL of request that failed.
    error
        Response from Qserv.
    """

    def __init__(self, url: str, error: BaseResponse) -> None:
        if error.error:
            msg = f"Qserv request failed: {error.error}"
        else:
            msg = "Qserv request failed without an error"
        super().__init__(msg)
        self.url = url
        self.detail = str(error.error_ext) if error.error_ext else None

    @override
    def to_job_error(self) -> JobError:
        msg = f"{self!s}\n\n{self.detail}" if self.detail else str(self)
        return JobError(code=self.error, message=msg)

    @override
    def to_slack(self) -> SlackMessage:
        """Format the exception for Slack reporting.

        Returns
        -------
        SlackMessage
            Message suitable for sending to Slack.
        """
        result = super().to_slack()
        result.fields.append(SlackTextField(heading="URL", text=self.url))
        if self.detail:
            block = SlackCodeBlock(heading="Error details", code=self.detail)
            result.blocks.append(block)
        return result


class QservApiProtocolError(QservApiError):
    """A Qserv REST API returned unexpected results.

    Parameters
    ----------
    url
        URL of request that failed.
    error
        Error message.
    """

    error = JobErrorCode.backend_internal_error

    def __init__(self, url: str, error: str) -> None:
        super().__init__(f"Qserv request failed: {error}")
        self.url = url

    @override
    def to_slack(self) -> SlackMessage:
        """Format the exception for Slack reporting.

        Returns
        -------
        SlackMessage
            Message suitable for sending to Slack.
        """
        result = super().to_slack()
        result.fields.append(SlackTextField(heading="URL", text=self.url))
        return result


class QservApiWebError(SlackWebException, QservApiError):
    """A web request to Qserv failed at the HTTP protocol level."""

    error = JobErrorCode.backend_request_error
