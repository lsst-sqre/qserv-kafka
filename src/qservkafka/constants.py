"""Constants that (currently) do not need to be configurable."""

from __future__ import annotations

__all__ = [
    "REDIS_BACKOFF_MAX",
    "REDIS_BACKOFF_START",
    "REDIS_POOL_SIZE",
    "REDIS_POOL_TIMEOUT",
    "REDIS_RETRIES",
    "REDIS_TIMEOUT",
]

REDIS_BACKOFF_MAX = 1.0
"""Maximum delay (in seconds) to wait after a Redis failure."""

REDIS_BACKOFF_START = 0.2
"""How long (in seconds) to initially wait after a Redis failure.

Exponential backoff will be used for subsequent retries, up to
`REDIS_BACKOFF_MAX` total delay.
"""

REDIS_POOL_SIZE = 5
"""Size of the ephemeral Redis connection pool (without rate limiting.)"""

REDIS_POOL_TIMEOUT = 30
"""Seconds to wait for a connection from the pool before giving up."""

REDIS_RETRIES = 10
"""How many times to try to connect to Redis before giving up."""

REDIS_TIMEOUT = 5
"""Timeout in seconds for a Redis network operation (including connecting)."""
