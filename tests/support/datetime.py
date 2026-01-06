"""Utility functions for tests involving dates."""

from datetime import UTC, datetime, timedelta

__all__ = [
    "assert_approximately_now",
    "milliseconds_to_timestamp",
]


def assert_approximately_now(time: datetime | None) -> None:
    """Assert that a datetime is at most five seconds older than now."""
    assert time
    now = datetime.now(tz=UTC)
    assert now - timedelta(seconds=5) <= time <= now


def milliseconds_to_timestamp(milliseconds: int) -> datetime:
    """Convert from milliseconds since epoch to a `~datetime.datetime`."""
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)
