"""Common constants for tests."""

from datetime import datetime
from typing import cast
from unittest.mock import ANY

ANY_DATETIME: datetime = cast("datetime", ANY)
ANY_OPTIONAL_DATETIME: datetime | None = cast("datetime | None", ANY)
ANY_STRING: str = cast("str", ANY)
ANY_INT: int = cast("int", ANY)
