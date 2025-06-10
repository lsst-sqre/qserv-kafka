"""Utility functions for tests involving dates."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

__all__ = ["assert_approximately_now"]


def assert_approximately_now(time: datetime | None) -> None:
    """Assert that a datetime is at most five seconds older than now."""
    assert time
    now = datetime.now(tz=UTC)
    assert now - timedelta(seconds=5) <= time <= now
