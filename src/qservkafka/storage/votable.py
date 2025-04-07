"""Writer for BINARY2-encoded VOTables."""

from __future__ import annotations

import math
import struct
from binascii import b2a_base64
from collections.abc import AsyncGenerator
from typing import Any, assert_never

from bitstring import BitArray
from httpx import AsyncClient, HTTPError
from pydantic import HttpUrl
from sqlalchemy import Row
from structlog.stdlib import BoundLogger

from ..exceptions import UploadWebError
from ..models.kafka import JobResultColumnType, JobResultConfig
from ..models.votable import VOTablePrimitive

_BASE64_LINE_LENGTH = 64
"""Maximum length of a base64-encoded line in BINARY2 output."""

__all__ = ["VOTableEncoder", "VOTableWriter"]


class VOTableEncoder:
    """Streaming encoder for the BINARY2 serialization format of VOTable.

    A new encoder should be used for every result set, since it keeps internal
    state in order to count the number of output rows.

    Parameters
    ----------
    config
        Configuration for the output format. Includes the header, footer, and
        type information. The type information must exactly match the columns
        of the results. This is not checked.
    """

    def __init__(self, config: JobResultConfig) -> None:
        self._config = config
        self._total_rows: int = 0

    @property
    def total_rows(self) -> int:
        """Total number of rows encoded."""
        return self._total_rows

    async def encode(
        self, results: AsyncGenerator[Row[Any] | tuple[Any]]
    ) -> AsyncGenerator[bytes]:
        """Encode results into a VOTable with BINARY2 encoding.

        Parameters
        ----------
        results
            Async generator that yields one result row at a time.

        Yields
        ------
        bytes
            Encoded data suitable for writing to a file or sending via an HTTP
            ``PUT`` request.
        """
        yield self._config.envelope.header.encode()
        binary2 = self._generate_binary2(self._config.column_types, results)
        encoded = self._base64_encode(binary2)
        try:
            async for output in encoded:
                yield output
        finally:
            await encoded.aclose()
        yield self._config.envelope.footer.encode()

    async def _base64_encode(
        self, data: AsyncGenerator[bytes]
    ) -> AsyncGenerator[bytes]:
        """Encode a stream in base64.

        Parameters
        ----------
        data
            An async generator producing the data to be encoded.

        Yields
        ------
        bytes
            A blob of base64-encoded data.
        """
        input_line_length = _BASE64_LINE_LENGTH * 3 // 4
        buf = bytearray()
        try:
            async for blob in data:
                buf += blob
                if len(buf) >= input_line_length:
                    view = memoryview(buf)
                    yield b2a_base64(view[:input_line_length])
                    view.release()
                    buf = buf[input_line_length:]
            yield b2a_base64(buf)
        finally:
            await data.aclose()

    def _encode_row(
        self, types: list[JobResultColumnType], row: Row[Any] | tuple[Any]
    ) -> bytes:
        """Encode a single row of output in BINARY2.

        Parameters
        ----------
        types
            Data types of the columns.
        row
            Result row.

        Returns
        -------
        bytes
            Encoded results in raw binary format, not base64-encoded.
        """
        nulls = BitArray(length=len(types))
        output = bytearray()
        for i, column in enumerate(types):
            arraysize = column.arraysize
            datatype = column.datatype
            value = row[i]
            if value is None:
                nulls.set(True, i)
            match datatype:
                case VOTablePrimitive.boolean:
                    value = False if value is None else bool(value)
                    output += struct.pack(datatype.pack, value)
                case VOTablePrimitive.char:
                    value = ("" if value is None else str(value)).encode()
                    if arraysize and arraysize.variable:
                        if arraysize.limit:
                            value = value[: arraysize.limit]
                        rule = ">I" + str(len(value)) + "s"
                        output += struct.pack(rule, len(value), value)
                    elif arraysize and arraysize.limit:
                        rule = str(arraysize.limit) + "s"
                        output += struct.pack(rule, value)
                    else:
                        output += struct.pack(datatype.pack, value)
                case VOTablePrimitive.double | VOTablePrimitive.float:
                    value = math.nan if value is None else float(value)
                    output += struct.pack(datatype.pack, value)
                case VOTablePrimitive.int | VOTablePrimitive.long:
                    value = 0 if value is None else int(value)
                    output += struct.pack(datatype.pack, value)
                case _ as unreachable:
                    assert_never(unreachable)
        return nulls.tobytes() + output

    async def _generate_binary2(
        self,
        types: list[JobResultColumnType],
        results: AsyncGenerator[Row[Any] | tuple[Any]],
    ) -> AsyncGenerator[bytes]:
        """Generate the binary output that goes into BINARY2 encoding.

        This output needs to be base64-encoded and then wrapped in the XML
        envelope to make a valid VOTable.

        Parameters
        ----------
        types
            Column types. This must exactly match the structure of each row
            yielded by the ``results`` parameter. This is not checked.
        results
            Async generator that yields one result row at a time.

        Yields
        ------
        bytes
            A blob of binary data that needs to be base64-encoded.
        """
        try:
            async for row in results:
                encoded = self._encode_row(types, row)
                yield encoded
                self._total_rows += 1
        finally:
            await results.aclose()


class VOTableWriter:
    """Streaming BINARY2 VOTable writer.

    Supports streaming encoding into a (subset of) the VOTable BINARY2 format
    given a generator of data rows and a set of type definitions. This custom
    encoder is used in preference to, e.g., the encoder provided by astropy
    because it can encode one result row at a time without holding the full
    data in memory.

    Parameters
    ----------
    http_client
        HTTP client to use for uploads.
    logger
        Logger to use.
    """

    def __init__(self, http_client: AsyncClient, logger: BoundLogger) -> None:
        self._client = http_client
        self._logger = logger

    async def store(
        self,
        url: str | HttpUrl,
        config: JobResultConfig,
        results: AsyncGenerator[Row[Any]],
    ) -> int:
        """Store the encoded VOTable via an HTTP PUT.

        Parameters
        ----------
        url
            URL to which to upload the encoded result.
        config
            Configuration for the output format. Includes the header, footer,
            and type information. The type information must exactly match the
            columns of the results. This is not checked.
        results
            Async generator that yields one result row at a time.

        Returns
        -------
        int
            Total number of rows encoded.

        Raises
        ------
        UploadWebError
            Raised if there was a failure to upload the results.
        """
        encoder = VOTableEncoder(config)
        generator = encoder.encode(results)
        try:
            mime_type = "application/x-votable+xml; serialization=binary2"
            r = await self._client.put(
                str(url),
                headers={"Content-Type": mime_type},
                content=generator,
            )
            r.raise_for_status()
        except HTTPError as e:
            raise UploadWebError.from_exception(e) from e
        finally:
            await generator.aclose()
        return encoder.total_rows
