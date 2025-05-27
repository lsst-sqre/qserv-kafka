"""Writer for BINARY2-encoded VOTables."""

from __future__ import annotations

import struct
from binascii import b2a_base64
from collections.abc import AsyncGenerator
from datetime import datetime
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
from ..models.votable import EncodedSize, VOTablePrimitive

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

        self._encoded_size: int = 0
        self._total_rows: int = 0
        self._wrapper_size: int = 0

    @property
    def encoded_size(self) -> int:
        """Size of the encoded data without the VOTable wrapper."""
        return self._encoded_size

    @property
    def total_rows(self) -> int:
        """Total number of rows encoded."""
        return self._total_rows

    @property
    def total_size(self) -> int:
        """Total size of the output VOTable, including header and footer."""
        return self._encoded_size + self._wrapper_size

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
        header = self._config.envelope.header.encode()
        self._wrapper_size += len(header)
        yield header
        encoded = BytesIO()
        input_line_length = _BASE64_LINE_LENGTH * 3 // 4
        output_lines = UPLOAD_BUFFER_SIZE // (_BASE64_LINE_LENGTH + 1)
        threshold = input_line_length * output_lines
        try:
            async for row in results:
                encoded_row = self._encode_row(self._config.column_types, row)
                self._total_rows += 1
                self._encoded_size += len(encoded_row)
                encoded.write(encoded_row)
                if self._total_rows % 100000 == 0:
                    self._logger.debug(f"Processed {self._total_rows} rows")
                if encoded.tell() >= threshold:
                    encoded.truncate()
                    yield self._base64_encode_bytes(encoded)
            encoded.truncate()
            yield self._base64_encode_bytes(encoded, last=True)
        finally:
            encoded.close()
            await results.aclose()
        footer = self._config.envelope.footer.encode()
        self._wrapper_size += len(footer)
        yield footer

    def _base64_encode_bytes(
        self, binary: BytesIO, *, last: bool = False
    ) -> bytes:
        """Encode the provided `io.BytesIO` object into base64.

        Break up the base64 encoding with newlines to match the output of our
        modified version of the CADC TAP server, just in case that helps some
        clients that might not want to deal with long lines.

        Parameters
        ----------
        binary
            Buffer object containing BINARY2-encoded rows without base64
            encoding. This object is modified in place, removing all of the
            encoded data and resetting the buffer to contain only the data
            that was not included in the output.
        last
            If `True`, this is the end of the data, so the last portion of
            the buffer should be encoded into a partial line rather than
            preserved.

        Returns
        -------
        bytes
            Base64-encoded chunk.
        """
        input_line_length = _BASE64_LINE_LENGTH * 3 // 4
        available = binary.tell()
        view = binary.getbuffer()
        offset = 0
        output = bytearray()
        while available > offset + input_line_length:
            output += b2a_base64(view[offset : offset + input_line_length])
            offset += input_line_length
        if last:
            output += b2a_base64(view[offset:])
            view.release()
        else:
            leftover = view[offset:].tobytes()
            view.release()
            binary.seek(0)
            binary.write(leftover)
        return output

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
        if isinstance(value_raw, datetime):
            millisecond = value_raw.microsecond // 1000

            # Blindly assume that the timestamp is already in UTC (or TAI,
            # which we are not yet handling correctly with TIMESYS). We could
            # check whether the datetime is zone-aware and, if so, convert to
            # UTC, but the database really shouldn't have times in civil time
            # zones in it.
            #
            # This f-string approach is slightly faster than using strftime
            # and separately appending milliseconds. Unfortunately, we can't
            # use isoformat because we don't want to append a time zone.
            #
            # The VOTable format says that astronomical times must be in UTC
            # and not use any time zone suffix, and civil times must be in UTC
            # but may use a Z suffix. Since we have no way of knowing whether
            # a given column is an astronomical or civil time, the only safe
            # approach seems to be to leave off the time zone suffix.
            value_str = (
                f"{value_raw.year:04d}-{value_raw.month:02d}"
                f"-{value_raw.day:02d}T{value_raw.hour:02d}"
                f":{value_raw.minute:02d}:{value_raw.second:02d}"
                f".{millisecond:03d}"
            )
        else:
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
    ) -> EncodedSize:
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
        EncodedSize
            Size of the output VOTable.

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
        return EncodedSize(
            rows=encoder.total_rows,
            data_bytes=encoder.encoded_size,
            total_bytes=encoder.total_size,
        )
