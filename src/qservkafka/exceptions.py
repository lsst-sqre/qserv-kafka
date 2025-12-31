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
    "BackendApiError",
    "BackendApiFailedError",
    "BackendApiProtocolError",
    "BackendApiSqlError",
    "BackendApiSqlError",
    "BackendApiWebError",
    "BackendNotImplementedError",
    "BigQueryApiError",
    "BigQueryApiFailedError",
    "BigQueryApiProtocolError",
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


class BackendApiError(QueryError):
    """Base class for failures talking to any database backend API.

    This is the generic exception that service layer code should catch.
    Specific backends (QServ, BigQuery) raise subclasses of this.
    """


class BackendApiFailedError(BackendApiError):
    """A backend API request returned a failure status.

    This is a generic base class, backend-specific subclasses should provide
    additional context.
    """


class BackendApiProtocolError(BackendApiError):
    """A backend API returned unexpected results.

    This indicates a protocol error while communicating
    with the backend.
    """

    error = JobErrorCode.backend_internal_error


class BackendApiSqlError(BackendApiError):
    """A SQL request to a backend failed unexpectedly.

    This is used by SQL-based backends (like QServ) to wrap SQLAlchemy
    errors.
    """

    error = JobErrorCode.backend_sql_error


class BackendApiWebError(SlackWebException, BackendApiError):
    """A web request to a backend failed at the HTTP level.

    This wraps HTTP errors when communicating with a backend REST API.
    """

    error = JobErrorCode.backend_request_error


class BackendNotImplementedError(BackendApiError):
    """Feature not implemented by this backend.

    This is raised when a backend doesn't support a particular feature.
    """

    error = JobErrorCode.backend_error


class QservApiError(BackendApiError):
    """Base class for failures talking to the Qserv API.

    Deprecated: Use BackendApiError for generic handling.
    """


class BigQueryApiError(BackendApiError):
    """Base class for failures talking to the BigQuery API."""


class BigQueryApiFailedError(BigQueryApiError, BackendApiFailedError):
    """A BigQuery API request returned a failure status.

    This represents a query that BigQuery rejected.

    Parameters
    ----------
    method
        Method that failed.
    project
        GCP project ID.
    error
        Error message from BigQuery.

    Attributes
    ----------
    method
        Method that failed.
    project
        GCP project ID.
    error_message
        Error message from BigQuery.
    """

    def __init__(self, method: str, project: str, error: str) -> None:
        super().__init__(f"BigQuery request failed: {error}")
        self.method = method
        self.project = project
        self.error_message = error

    @override
    def to_slack(self) -> SlackMessage:
        result = super().to_slack()
        text = f"{self.method} in project {self.project}"
        result.blocks.append(SlackTextBlock(heading="Request", text=text))
        if self.error_message:
            block = SlackCodeBlock(heading="Error", code=self.error_message)
            result.blocks.append(block)
        return result

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        info.tags["method"] = self.method
        info.tags["project"] = self.project
        if self.error_message:
            info.contexts["bigquery_error"] = {"error": self.error_message}
        return info


class BigQueryApiProtocolError(BigQueryApiError, BackendApiProtocolError):
    """A BigQuery API call failed due to protocol or unexpected errors.

    This indicates an unexpected error when communicating with BigQuery.

    Parameters
    ----------
    method
        Method that failed.
    project
        GCP project ID.
    error
        Error message.

    Attributes
    ----------
    method
        Method that failed.
    project
        GCP project ID.
    """

    error = JobErrorCode.backend_internal_error

    def __init__(self, method: str, project: str, error: str) -> None:
        super().__init__(f"BigQuery protocol error: {error}")
        self.method = method
        self.project = project

    @override
    def to_slack(self) -> SlackMessage:
        result = super().to_slack()
        text = f"{self.method} in project {self.project}"
        result.blocks.append(SlackTextBlock(heading="Request", text=text))
        return result

    @override
    def to_sentry(self) -> SentryEventInfo:
        info = super().to_sentry()
        info.tags["method"] = self.method
        info.tags["project"] = self.project
        return info


class QservApiFailedError(QservApiError, BackendApiFailedError):
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
    detail
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


class QservApiProtocolError(QservApiError, BackendApiProtocolError):
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


class QservApiSqlError(QservApiError, BackendApiSqlError):
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


class QservApiWebError(QservApiError, BackendApiWebError):
    """A web request to Qserv failed at the HTTP protocol level."""

    error = JobErrorCode.backend_request_error


class TableUploadWebError(SlackWebException, QueryError):
    """Retrieving an uploaded table failed."""

    error = JobErrorCode.table_read


class UploadWebError(SlackWebException, QueryError):
    """Upload of the query results failed."""

    error = JobErrorCode.upload_failed
