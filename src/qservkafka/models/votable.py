"""Models for VOTables."""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Annotated, Any, Self

from pydantic import BaseModel, Field, PlainSerializer, PlainValidator

__all__ = [
    "EncodedSize",
    "UploadStats",
    "VOTableArraySize",
    "VOTablePrimitive",
    "VOTableSize",
]


@dataclass
class EncodedSize:
    """Size of the result VOTable."""

    rows: int
    """Number of rows in the table."""

    data_bytes: int
    """Size of the encoded data in bytes, not including the XML wrapper."""

    total_bytes: int
    """Total size of the result in bytes, including the XML wrapper."""


@dataclass
class UploadStats(EncodedSize):
    """Statistics about the uploaded result VOTable."""

    elapsed: timedelta
    """Time required to process and upload the results."""


class VOTableSize(BaseModel):
    """Parsed VOTable array size."""

    limit: Annotated[int | None, Field(title="Maximum length")]

    variable: Annotated[bool, Field(title="Whether length can vary")]

    def to_string(self) -> str:
        """Convert to string form as seen in VOTables."""
        result = str(self.limit) if self.limit is not None else ""
        if self.variable:
            result += "*"
        return result


def _validate_array_size(arraysize: Any) -> VOTableSize:
    """Convert the string representation of `VOTableSize` to an object."""
    if isinstance(arraysize, VOTableSize):
        return arraysize
    elif isinstance(arraysize, dict):
        return VOTableSize(**arraysize)
    elif not isinstance(arraysize, str):
        msg = f"Invalid arraysize type {type(arraysize).__name__}"
        raise ValueError(msg)  # noqa: TRY004 (Pydantic requirement)
    variable = False
    if arraysize.endswith("*"):
        variable = True
        arraysize = arraysize.removesuffix("*")
    limit = int(arraysize) if arraysize else None
    return VOTableSize(limit=limit, variable=variable)


type VOTableArraySize = Annotated[
    VOTableSize,
    PlainSerializer(lambda v: v.to_string(), return_type=str),
    PlainValidator(_validate_array_size),
]


class VOTablePrimitive(Enum):
    """VOTable primitive types supported by the Qserv Kafka bridge.

    Each enum value is also associated with the `struct.pack` format that is
    used when encoding that type into BINARY2, assuming that no ``arraysize``
    is set for that type.
    """

    boolean = ("boolean", "1s")
    char = ("char", "c")
    unicode_char = ("unicodeChar", "2s")
    double = ("double", ">d")
    float = ("float", ">f")
    short = ("short", ">h")
    int = ("int", ">l")
    long = ("long", ">q")

    def __new__(cls, *args: Any) -> Self:
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(self, _: str, pack_format: str) -> None:
        self._pack_format = pack_format

    def pack(self, value: Any) -> bytes:
        """Serialize a value of the corresponding data type to BINARY2.

        Parameters
        ----------
        value
            Value to serialize.

        Returns
        -------
        bytes
            Binary (non-base64) serialized form of the data.
        """
        match self.value:
            case "boolean":
                value = b"\0" if value is None else b"T" if value else b"F"
                return struct.pack(self._pack_format, value)
            case "char":
                if not isinstance(value, bytes):
                    value = str(value).encode()
                return struct.pack(
                    self._pack_format, value[:1] if value else b"\x00"
                )
            case "unicodeChar":
                if not isinstance(value, bytes):
                    value = str(value).encode("utf-16-be")
                return struct.pack(self._pack_format, value)
            case "double" | "float":
                value = math.nan if value is None else float(value)
                return struct.pack(self._pack_format, value)
            case "short" | "int" | "long":
                value = 0 if value is None else int(value)
                return struct.pack(self._pack_format, value)
            case _:  # pragma: no cover
                raise ValueError(f"Unknown type {self.value}")

    def is_string(self) -> bool:
        """Check if the primitive type is a string type."""
        return self in (VOTablePrimitive.char, VOTablePrimitive.unicode_char)
