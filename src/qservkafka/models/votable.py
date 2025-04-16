"""Models for VOTables."""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Self

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


class VOTablePrimitive(Enum):
    """VOTable primitive types supported by the Qserv Kafka bridge.

    Each enum value is also associated with the `struct.pack` format that is
    used when encoding that type into BINARY2, assuming that no ``arraysize``
    is set for that type.
    """

    boolean = ("boolean", "1s")
    char = ("char", "1s")
    double = ("double", ">d")
    float = ("float", ">f")
    int = ("int", ">l")
    long = ("long", ">q")

    def __new__(cls, *args: Any) -> Self:
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(self, _: str, pack: str) -> None:
        self._pack = pack

    @property
    def pack(self) -> str:
        return self._pack
