"""Models for the Gafaelfawr API.

These modules will be removed in favor of a PyPI Gafaelfawr client once one is
available.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

__all__ = ["GafaelfawrQuota", "GafaelfawrUserInfo"]


class GafaelfawrTapQuota(BaseModel):
    """TAP quota information for a user."""

    concurrent: int = Field(..., title="Concurrent queries")


class GafaelfawrQuota(BaseModel):
    """Quota information for a user.

    This manually copies only the fields from the Gafaelfawr Quota model
    that we care about in the Qserv Kafka bridge.
    """

    tap: dict[str, GafaelfawrTapQuota] = Field({}, title="TAP quotas")


class GafaelfawrUserInfo(BaseModel):
    """Metadata about a user.

    This manually copies only the fields from the Gafaelfawr UserInfo model
    that we care about in the Qserv Kafka bridge.
    """

    quota: GafaelfawrQuota | None = Field(None, title="Quota")
