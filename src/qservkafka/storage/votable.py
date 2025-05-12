"""Writer for BINARY2-encoded VOTables."""

from __future__ import annotations

import struct
from binascii import b2a_base64
from collections.abc import AsyncGenerator
from io import BytesIO
from typing import Any
from urllib.parse import urlparse

from bitstring import BitArray
from httpx import AsyncClient, HTTPError
from pydantic import HttpUrl
from sqlalchemy import Row
from structlog.stdlib import BoundLogger

from ..config import config
from ..constants import UPLOAD_BUFFER_SIZE
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
    logger
        Logger to use.
    """

    def __init__(self, config: JobResultConfig, logger: BoundLogger) -> None:
        self._config = config
        self._logger = logger
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
        buf = BytesIO()
        try:
            async for output in encoded:
                buf.write(output)
                if buf.tell() >= UPLOAD_BUFFER_SIZE:
                    buf.truncate()
                    yield buf.getvalue()
                    buf.seek(0)
            if buf.tell() > 0:
                buf.truncate()
                yield buf.getbuffer()
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
                while len(buf) >= input_line_length:
                    view = memoryview(buf)
                    yield b2a_base64(view[:input_line_length])
                    view.release()
                    buf = buf[input_line_length:]
            yield b2a_base64(buf)
        finally:
            await data.aclose()

    def _encode_char_column(
        self, column: JobResultColumnType, value_raw: Any
    ) -> bytes:
        """Encode a column of type ``char``.

        Most of the complex encoding handling applies to char columns.

        Parameters
        ----------
        column
            Column type definition.
        value_raw
            Value for that column.

        Returns
        -------
        bytes
            Serialized representation of the column.
        """
        value_str = "" if value_raw is None else str(value_raw)
        if value_str and column.requires_url_rewrite:
            try:
                base_url = urlparse(str(config.rewrite_base_url))
                url = urlparse(value_str)
                value_str = url._replace(netloc=base_url.netloc).geturl()
            except Exception as e:
                self._logger.warning(
                    "Unable to rewrite URL", column=column.name, error=str(e)
                )
        value = value_str.encode()
        if column.arraysize and column.arraysize.variable:
            if column.arraysize.limit:
                value = value[: column.arraysize.limit]
            rule = ">I" + str(len(value)) + "s"
            return struct.pack(rule, len(value), value)
        elif column.arraysize and column.arraysize.limit:
            rule = str(column.arraysize.limit) + "s"
            return struct.pack(rule, value)
        else:
            return column.datatype.pack(value)

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
            datatype = column.datatype
            value = row[i]
            if value is None:
                nulls.set(True, i)
            if datatype == VOTablePrimitive.char:
                output += self._encode_char_column(column, value)
            else:
                output += datatype.pack(value)
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
                if self._total_rows % 100000 == 0:
                    self._logger.debug(f"Processed {self._total_rows} rows")
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
        encoder = VOTableEncoder(config, self._logger)
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
