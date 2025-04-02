"""Models for VOTables."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, Field, PlainValidator

__all__ = [
    "VOTableArraySize",
    "VOTablePrimitive",
    "VOTableSize",
]


class VOTableSize(BaseModel):
    """Parsed VOTable array size."""

    limit: Annotated[int | None, Field(title="Maximum length")]

    variable: Annotated[bool, Field(title="Whether length can vary")]


def _validate_array_size(arraysize: Any) -> VOTableSize:
    """Verify that a VOTable arraysize is in a valid format."""
    if isinstance(arraysize, VOTableSize):
        return arraysize
    if not isinstance(arraysize, str):
        msg = f"Invalid arraysize type {type(arraysize).__name__}"
        raise ValueError(msg)  # noqa: TRY004 (Pydantic requirement)
    variable = False
    if arraysize.endswith("*"):
        variable = True
        arraysize = arraysize.removesuffix("*")
    limit = int(arraysize) if arraysize else None
    return VOTableSize(limit=limit, variable=variable)


type VOTableArraySize = Annotated[
    VOTableSize, PlainValidator(_validate_array_size)
]


class VOTablePrimitive(StrEnum):
    """VOTable primitive types supported by the Qserv Kafka bridge."""

    boolean = "boolean"
    char = "char"
    double = "double"
    float = "float"
    int = "int"
    long = "long"
