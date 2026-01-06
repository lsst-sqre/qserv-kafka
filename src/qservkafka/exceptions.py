"""Custom exceptions for the Qserv Kafka bridge."""

from typing import ClassVar, Self, override

from safir.slack.blockkit import (
    SlackCodeBlock,
    SlackException,
    SlackMessage,
    SlackTextBlock,
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
    method
        Method that failed.
    url
        URL of request that failed.
    error
        Response from Qserv.

    Attributes
    ----------
    details
        Supplemental error details from Qserv.
    error
        Qesrv error.
    method
        Method that failed.
    url
        URL of request that failed.
    """

    def __init__(self, method: str, url: str, error: BaseResponse) -> None:
        super().__init__("Qserv request failed")
        self.method = method
        self.url = url
        self.qserv_error = error.error
        self.detail = str(error.error_ext) if error.error_ext else None

    @override
    def to_job_error(self) -> JobError:
        if self.qserv_error:
            msg = f"{self!s}: {self.qserv_error}"
        else:
            msg = str(self)
        return JobError(code=self.error, message=msg)

    @override
    def to_slack(self) -> SlackMessage:
        result = super().to_slack()
        text = f"{self.method} {self.url}"
        result.blocks.append(SlackTextBlock(heading="URL", text=text))
        if self.qserv_error:
            block = SlackCodeBlock(heading="Error", code=self.qserv_error)
            result.blocks.append(block)
        if self.detail:
            block = SlackCodeBlock(heading="Error details", code=self.detail)
            result.blocks.append(block)
        return result

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        info.tags["method"] = self.method
        info.tags["url"] = self.url
        if self.error or self.detail:
            context = {}
            if self.qserv_error:
                context["error"] = self.qserv_error
            if self.detail:
                context["error_details"] = self.detail
            info.contexts["qserv_error"] = context
        return info


class QservApiProtocolError(QservApiError):
    """A Qserv REST API returned unexpected results.

    Parameters
    ----------
    method
        Method that failed.
    url
        URL of request that failed.
    error
        Error message.

    Attributes
    ----------
    method
        Method that failed.
    url
        URL of request that failed.
    """

    error = JobErrorCode.backend_internal_error

    def __init__(self, method: str, url: str, error: str) -> None:
        super().__init__(f"Qserv request failed: {error}")
        self.method = method
        self.url = url

    @override
    def to_slack(self) -> SlackMessage:
        result = super().to_slack()
        text = f"{self.method} {self.url}"
        result.blocks.append(SlackTextBlock(heading="URL", text=text))
        return result

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        info.tags["method"] = self.method
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
