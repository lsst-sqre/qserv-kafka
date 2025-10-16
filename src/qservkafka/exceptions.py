"""Custom exceptions for the Qserv Kafka bridge."""

from __future__ import annotations

from typing import ClassVar, Self, override

from safir.slack.blockkit import (
    SlackCodeBlock,
    SlackException,
    SlackMessage,
    SlackTextField,
    SlackWebException,
)
from safir.slack.sentry import SentryEventInfo
from sqlalchemy.exc import SQLAlchemyError

from .models.kafka import JobError, JobErrorCode
from .models.qserv import BaseResponse

__all__ = [
    "QservApiError",
    "QservApiFailedError",
    "QservApiProtocolError",
    "QservApiSqlError",
    "QservApiWebError",
    "QueryError",
    "TableUploadWebError",
    "UploadWebError",
]


class QueryError(SlackException):
    """Base class for reportable query errors."""

    error: ClassVar[JobErrorCode] = JobErrorCode.backend_error

    def to_job_error(self) -> JobError:
        """Convert to a `~qservkafka.models.kafka.JobError` for reporting.

        Returns
        -------
        JobError
            Corresponding job error.
        """
        return JobError(code=self.error, message=str(self))


class QservApiError(QueryError):
    """Base class for failures talking to the Qserv API."""


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

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        info.tags["url"] = self.url
        if self.detail:
            info.attachments["Error details"] = self.detail
        return info


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

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        info.tags["url"] = self.url
        return info


class QservApiSqlError(QservApiError):
    """A SQL request to Qserv failed unexpectedly."""

    error = JobErrorCode.backend_sql_error

    @classmethod
    def from_exception(cls, exc: SQLAlchemyError) -> Self:
        """Create the exception from a SQLAlchemy exception.

        Parameters
        ----------
        exc
            The underlying SQLAlchemy exception.

        Returns
        -------
        QservApiSqlError
            Newly-created exception.
        """
        if str(exc):
            msg = f"{type(exc).__name__}: {exc!s}"
        else:
            msg = type(exc).__name__
        return cls(f"SQL query error: {msg}")


class QservApiWebError(SlackWebException, QservApiError):
    """A web request to Qserv failed at the HTTP protocol level."""

    error = JobErrorCode.backend_request_error


class TableUploadWebError(SlackWebException, QueryError):
    """Retrieving an uploaded table failed."""

    error = JobErrorCode.table_read


class UploadWebError(SlackWebException, QueryError):
    """Upload of the query results failed."""

    error = JobErrorCode.upload_failed
