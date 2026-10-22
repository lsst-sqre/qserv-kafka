"""Internal models for quota information."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from rubin.gafaelfawr import GafaelfawrTapQuota
from safir.datetime import format_datetime_for_logging

__all__ = ["RateLimitStatus"]


@dataclass
class RateLimitStatus:
    """Current status of a user's quota."""

    allowed: bool
    """Whether a query is allowed or denied."""

    running: int
    """Number of running queries."""

    bytes_billed_reset: datetime | None = None
    """Time when the user's bytes billed quota resets, if known.

    If this is set, the request was rejected due to exceeded bytes billed
    quota. If not, it was rejected due to the number of running queries.
    Replace this with an enum if more quota cases are added in the future.
    """

    quota: GafaelfawrTapQuota | None = None
    """User's quota information, if known."""

    def to_logging_context(self) -> dict[str, Any]:
        """Convert relevant information to a dictionary for logging."""
        result: dict[str, Any] = {"running": self.running}
        if self.bytes_billed_reset:
            quota_reset = format_datetime_for_logging(self.bytes_billed_reset)
            result["quota_reset"] = quota_reset
        if self.quota:
            result["quota"] = self.quota.to_logging_context()
        return result
