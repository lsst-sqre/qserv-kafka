"""Writer for BINARY2-encoded VOTables and Parquet files."""

from __future__ import annotations

import asyncio
import gc
import struct
from binascii import b2a_base64
from collections.abc import AsyncGenerator, Callable
from datetime import datetime
from enum import Enum
from io import BytesIO
from typing import Any
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
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


__all__ = [
    "OutputFormat",
    "VOParquetEncoder",
    "VOTableEncoder",
    "VOTableWriter",
]

pa.set_memory_pool(pa.system_memory_pool())


class OutputFormat(str, Enum):
    """Supported output formats."""

    VOTable = "votable"
    Parquet = "parquet"


class StreamingAdapter:
    """Streaming adapter that serves as a buffer between for streaming Parquet
    pyarrow's ParquetWriter and an async generator that yields bytes.

    Adapted from the StackOverflow example to work with PyArrow ParquetWriter.
    https://stackoverflow.com/questions/64791558/
    create-parquet-files-from-stream-in-python-in-memory-efficient-manner
    """

    def __init__(self) -> None:
        self.buffer = BytesIO()
        self.written = 0
        self.closed = False

    def flush_buffer(self) -> bytes:
        """Flush the buffer and return bytes for yielding."""
        b = self.buffer.getvalue()
        self.buffer.seek(0)
        self.buffer.truncate(0)
        return b

    def write(self, b: bytes) -> None:
        """Write bytes to the buffer."""
        self.buffer.write(b)
        self.written += len(b)

    def tell(self) -> int:
        """Return total bytes written."""
        return self.written

    def close(self) -> None:
        """Mark buffer as closed."""
        self.closed = True
        if self.buffer:
            self.buffer.seek(0)
            self.buffer.truncate(0)
            self.buffer.close()
        self.written = 0
        gc.collect()

    def flush(self) -> None:
        """Flush method required by some writers."""


def _rewrite_url(value_str: str, column_name: str, logger: BoundLogger) -> str:
    """Shared URL rewriting logic."""
    try:
        base_url = urlparse(str(config.rewrite_base_url))
        url = urlparse(value_str)
        return url._replace(netloc=base_url.netloc).geturl()
    except Exception as e:
        logger.warning(
            "Unable to rewrite URL", column=column_name, error=str(e)
        )
        return value_str


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
        self,
        results: AsyncGenerator[Row[Any] | tuple[Any]],
        *,
        maxrec: int | None = None,
    ) -> AsyncGenerator[bytes]:
        """Encode results into a VOTable with BINARY2 encoding.

        Parameters
        ----------
        results
            Async generator that yields one result row at a time.
        maxrec
            Maximum record limit, if not `None`.

        Yields
        ------
        bytes
            Encoded data suitable for writing to a file or sending via an HTTP
            ``PUT`` request.
        """
        header = self._config.envelope.header.encode()
        self._wrapper_size += len(header)
        yield header

        # Encode the result data.
        encoded = BytesIO()
        input_line_length = _BASE64_LINE_LENGTH * 3 // 4
        output_lines = UPLOAD_BUFFER_SIZE // (_BASE64_LINE_LENGTH + 1)
        threshold = input_line_length * output_lines
        overflow = False
        try:
            async for row in results:
                if maxrec is not None and self._total_rows == maxrec:
                    overflow = True
                    break
                encoded_row = self._encode_row(self._config.column_types, row)
                self._total_rows += 1
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
            await asyncio.shield(results.aclose())

        # Add the footer, which varies if the results overflowed.
        if overflow:
            footer = self._config.envelope.footer_overflow.encode()
        else:
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
        self._encoded_size += len(output)
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
            value_str = _rewrite_url(value_str, column.name, self._logger)
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
            elif datatype == VOTablePrimitive.unicode_char:
                output += self._encode_unicode_char_column(
                    column=column, value_raw=value
                )
            else:
                output += datatype.pack(value)

        return nulls.tobytes() + output

    def _encode_unicode_char_column(
        self,
        column: JobResultColumnType,
        value_raw: Any,
    ) -> bytes:
        """Encode a column of type ``unicodeChar``.

        In the BINARY2 encoding, ``arraysize`` in the column specification
        gives the number of UCS-2 characters, each of which are two bytes, not
        the number of Uniocde characters. Characters that require surrogate
        pairs will therefore take more than one slot and may cause additional
        truncation of the string.

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
            value_str = (
                f"{value_raw.year:04d}-{value_raw.month:02d}"
                f"-{value_raw.day:02d}T{value_raw.hour:02d}"
                f":{value_raw.minute:02d}:{value_raw.second:02d}"
                f".{millisecond:03d}"
            )
        else:
            value_str = "" if value_raw is None else str(value_raw)

        # Rewrite URLs in the string if necessary.
        if value_str and column.requires_url_rewrite:
            try:
                base_url = urlparse(str(config.rewrite_base_url))
                url = urlparse(value_str)
                value_str = url._replace(netloc=base_url.netloc).geturl()
            except Exception as e:
                logger = self._logger.bind(column=column.name)
                logger.warning("Unable to rewrite URL", error=str(e))

        # Encode the string. Technically this is supposed to be UCS-2, but
        # Python doesn't support that natively and it's not clear what to do
        # with characters that aren't representable in UCS-2. Use UTF-16
        # instead and hope the client can cope.
        value = value_str.encode("utf-16-be")

        # Encoding varies depending on whether the string is fixed-width or
        # variable, or has no arraysize (indicating it should be encoded as a
        # single UCS-2 character).
        if column.arraysize and column.arraysize.variable:
            limit = column.arraysize.limit
            return self._encode_unicode_variable_array(value, limit)
        elif column.arraysize and column.arraysize.limit:
            limit = column.arraysize.limit
            return self._encode_unicode_fixed_array(value, limit)
        else:
            return column.datatype.pack(value)

    def _encode_unicode_fixed_array(self, value: bytes, limit: int) -> bytes:
        """Encode fixed-length unicodeChar array.

        Parameters
        ----------
        value
            Encoded byte representation of the string.
        limit
            Exact number of characters the field

        Returns
        -------
        bytes
            Serialized representation of the fixed unicodeChar array.
        """
        max_bytes = limit * 2
        if len(value) > max_bytes:
            value = self._truncate_utf16(value, max_bytes)
        rule = str(max_bytes) + "s"
        return struct.pack(rule, value)

    def _encode_unicode_variable_array(
        self, value: bytes, limit: int | None
    ) -> bytes:
        """Encode variable-length unicodeChar array.

        Parameters
        ----------
        value
            UTF-16-BE encoded bytes.
        limit
            Maximum number of characters allowed, or None

        Returns
        -------
        bytes
            Serialized representation of the variable-length unicodeChar array.
        """
        if limit is not None:
            max_bytes = limit * 2
            if len(value) > max_bytes:
                value = self._truncate_utf16(value, max_bytes)
        char_count = len(value) // 2
        rule = ">I" + str(len(value)) + "s"
        return struct.pack(rule, char_count, value)

    def _truncate_utf16(self, value: bytes, max_bytes: int) -> bytes:
        """Truncate UTF-16-BE bytes without breaking surrogate pairs.

        Parameters
        ----------
        value
            UTF-16-BE encoded bytes to truncate.
        max_bytes
            Maximum number of bytes to keep.

        Returns
        -------
        bytes
            Truncated bytes.
        """
        max_bytes_even = max_bytes & ~1
        truncated = value[:max_bytes_even]
        if len(truncated) >= 2:
            last_word = struct.unpack(">H", truncated[-2:])[0]
            if 0xD800 <= last_word <= 0xDBFF:
                truncated = truncated[:-2]
        return truncated


class VOParquetEncoder:
    """VOParquet encoder for generating VOParquet data with embedded VOTable
    metadata as described in the specification:
    https://www.ivoa.net/documents/Notes/VOParquet/.

    This encoder uses PyArrow to write Parquet data directly to an
    in-memory buffer, which is then streamed to the caller.

    Parameters
    ----------
    config
        Configuration for the output format. Includes the header, footer, and
        type information. The type information must exactly match the columns
        of the results. This is not checked.
    logger
        Logger to use.
    overflow
        If `True`, use the overflow footer in the embedded VOTable metadata.
    """

    def __init__(
        self,
        config: JobResultConfig,
        logger: BoundLogger,
        *,
        overflow: bool = False,
    ) -> None:
        self._config = config
        self._logger = logger
        self._predetermined_overflow = overflow

        self._total_rows: int = 0
        self._encoded_size: int = 0
        self._batch_size = 2000
        self._column_names = [col.name for col in config.column_types]
        self._column_processors = self._build_column_processors()
        self._arrow_schema = self._build_arrow_schema()
        self._schema_with_metadata = self._build_schema_with_metadata(
            overflow=self._predetermined_overflow
        )

    @property
    def total_rows(self) -> int:
        return self._total_rows

    @property
    def encoded_size(self) -> int:
        return self._encoded_size

    async def encode(
        self,
        results: AsyncGenerator[Row[Any] | tuple[Any]],
        *,
        maxrec: int | None = None,
    ) -> AsyncGenerator[bytes]:
        """Encode results to VOParquet encoding.

        Parameters
        ----------
        results
            Async generator that yields one result row at a time.
        maxrec
            Maximum record limit, if not `None`.

        Yields
        ------
        bytes
            Encoded data suitable for writing to a file or sending via an HTTP
            ``PUT`` request.
        """
        buffer = StreamingAdapter()
        writer = pq.ParquetWriter(
            buffer,
            self._schema_with_metadata,
            compression="snappy",
            use_dictionary=False,
        )

        current_batch = []
        batch = None
        try:
            async for row in results:
                if maxrec is not None and self._total_rows >= maxrec:
                    break

                processed_row = self._process_row_for_parquet(row)
                current_batch.append(processed_row)
                self._total_rows += 1

                if len(current_batch) >= self._batch_size:
                    batch = self._batch_to_arrow_record_batch(current_batch)
                    current_batch.clear()

                    writer.write_batch(
                        batch=batch, row_group_size=self._batch_size
                    )
                    buffered_bytes = buffer.flush_buffer()

                    if buffered_bytes:
                        yield buffered_bytes

                    if self._total_rows % 100000 == 0:
                        self._logger.info(
                            f"Processed {self._total_rows:,} rows"
                        )

                    await asyncio.sleep(0)

            if current_batch:
                batch = self._batch_to_arrow_record_batch(current_batch)
                writer.write_batch(batch)

            writer.close()
            final_bytes = buffer.flush_buffer()

            if final_bytes:
                yield final_bytes

            self._encoded_size = buffer.written

            # Aggressively clean up references to help with garbage collection.
            # Otherwise the memory usage seems to stay high for a while.
            del writer
            del batch
            del current_batch
            del self._column_processors
            del self._arrow_schema
            gc.collect()
            pa.default_memory_pool().release_unused()

        finally:
            await asyncio.shield(results.aclose())

    def _batch_to_arrow_record_batch(
        self, batch_data: list[dict[str, Any]]
    ) -> pa.RecordBatch:
        """Convert batch data to Arrow RecordBatch for streaming.

        Parameters
        ----------
        batch_data
            List of processed row dictionaries.

        Returns
        -------
        pa.RecordBatch
            Arrow RecordBatch containing the batch data.

        """
        if not batch_data:
            return pa.record_batch([], schema=self._arrow_schema)

        arrays = []
        for i, col_name in enumerate(self._column_names):
            column_data = [row[col_name] for row in batch_data]
            expected_type = self._arrow_schema.field(i).type

            try:
                array = pa.array(column_data, type=expected_type)
                arrays.append(array)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                string_data = [
                    str(x) if x is not None else None for x in column_data
                ]
                arrays.append(pa.array(string_data))

        return pa.RecordBatch.from_arrays(arrays, names=self._column_names)

    def _build_schema_with_metadata(self, *, overflow: bool) -> pa.Schema:
        """Build Arrow schema with VOParquet metadata.

        Parameters
        ----------
        overflow
            If True use the overflow footer in the embedded VOTable metadata.

        Returns
        -------
        pa.Schema
            Arrow schema with embedded VOTable metadata.
        """
        if overflow:
            footer = self._config.envelope.footer_overflow.encode().decode()
        else:
            footer = self._config.envelope.footer.encode().decode()

        votable_xml = self._config.envelope.header.encode().decode() + footer

        metadata = {
            "IVOA.VOTable-Parquet.version": "1.0",
            "IVOA.VOTable-Parquet.content": votable_xml,
        }

        return self._arrow_schema.with_metadata(metadata)

    def _build_arrow_schema(self) -> pa.Schema:
        """Build Arrow schema with proper type mapping.

        Returns
        -------
        pa.Schema
            Arrow schema corresponding to the VOTable column types.

        """
        fields = []

        for col in self._config.column_types:
            arrow_type = self._map_votable_to_arrow_type(col)
            fields.append(pa.field(col.name, arrow_type))

        return pa.schema(fields)

    def _is_array_column(self, col: JobResultColumnType) -> bool:
        """Determine if a column is an array.

        Parameters
        ----------
        col
            VOTable column type.

        Returns
        -------
        bool
            True if the column is an array otherwise False.
        """
        if not col.arraysize:
            return False
        if hasattr(col.arraysize, "variable") and col.arraysize.variable:
            return True
        if hasattr(col.arraysize, "limit") and col.arraysize.limit:
            return col.arraysize.limit > 1
        return False

    def _map_votable_to_arrow_type(
        self, col: JobResultColumnType
    ) -> pa.DataType:
        """Map VOTable column type to Arrow type.

        Parameters
        ----------
        col
            VOTable column type.

        Returns
        -------
        pa.DataType
            Corresponding Arrow data type.
        """
        base_type_map = {
            VOTablePrimitive.boolean: pa.bool_(),
            VOTablePrimitive.short: pa.int16(),
            VOTablePrimitive.int: pa.int32(),
            VOTablePrimitive.long: pa.int64(),
            VOTablePrimitive.float: pa.float32(),
            VOTablePrimitive.double: pa.float64(),
            VOTablePrimitive.char: pa.string(),
            VOTablePrimitive.unicode_char: pa.string(),
        }

        base_type = base_type_map.get(col.datatype, pa.string())

        if self._is_array_column(col):
            return pa.list_(base_type)
        return base_type

    def _build_column_processors(self) -> list[Callable[[Any], Any]]:
        """Pre-build column processing functions.

        Returns
        -------
        list[Callable[[Any], Any]]
            List of processing functions for each column.
        """
        processors = []
        for col in self._config.column_types:
            if col.datatype == VOTablePrimitive.boolean:
                processors.append(self._make_boolean_converter())
            elif self._is_array_column(col):
                processors.append(self._make_array_converter(col))
            elif col.datatype in (
                VOTablePrimitive.char,
                VOTablePrimitive.unicode_char,
            ):
                processors.append(self._make_string_processor(col))
            else:
                processors.append(self._make_identity_processor())
        return processors

    def _make_identity_processor(self) -> Callable[[Any], Any]:
        """Create identity processor that returns the input value unchanged.

        Returns
        -------
        Callable[[Any], Any]
            Function that returns the input value unchanged.
        """
        return lambda x: x

    def _make_string_processor(
        self, col: JobResultColumnType
    ) -> Callable[[Any], Any]:
        """Create string processor (Handle char and unicodeChars).

        Parameters
        ----------
        col
            Column definition.

        Returns
        -------
        Callable[[Any], Any]
            Function that processes string values.
        """

        def process_string(value: Any) -> str | None:
            if value is None:
                return None

            if isinstance(value, datetime):
                millisecond = value.microsecond // 1000
                value_str = (
                    f"{value.year:04d}-{value.month:02d}"
                    f"-{value.day:02d}T{value.hour:02d}"
                    f":{value.minute:02d}:{value.second:02d}"
                    f".{millisecond:03d}"
                )
            else:
                value_str = str(value)

            # Apply URL rewriting
            if value_str and getattr(col, "requires_url_rewrite", False):
                value_str = _rewrite_url(value_str, col.name, self._logger)

            # Apply length limit if specified
            if col.arraysize and hasattr(col.arraysize, "limit"):
                limit = getattr(col.arraysize, "limit", None)
                if limit is not None:
                    value_str = value_str[:limit]

            return value_str

        return process_string

    def _make_array_converter(
        self, col: JobResultColumnType
    ) -> Callable[[Any], Any]:
        """Create array converter function.

        Parameters
        ----------
        col
            Column type for the array.

        Returns
        -------
        Callable[[Any], Any]
            Function that converts values to properly formatted arrays.
        """
        limit = None
        if col.arraysize and hasattr(col.arraysize, "limit"):
            limit = getattr(col.arraysize, "limit", None)

        def convert_array(value: Any) -> list[Any]:
            if value is None:
                return []
            result = value if isinstance(value, list) else [value]

            if limit is not None and len(result) > limit:
                result = result[:limit]

            return result

        return convert_array

    def _make_boolean_converter(self) -> Callable[[Any], Any]:
        """Create boolean converter function.

        Returns
        -------
        Callable[[Any], Any]
            Function that converts values to boolean
        """

        def convert_boolean(value: Any) -> bool | None:
            if value is None:
                return None

            if isinstance(value, str):
                lower_val = value.lower().strip()
                return lower_val in ("true", "1", "t", "yes", "y", "on")

            return bool(value)

        return convert_boolean

    def _process_row_for_parquet(
        self, row: Row[Any] | tuple[Any]
    ) -> dict[str, Any]:
        """Process row for the VOParquet format.

        Parameters
        ----------
        row
            Input row from the result set.

        Returns
        -------
        dict[str, Any]
            Processed row as a dictionary suitable for Parquet encoding.
        """
        processed: dict[str, Any] = {}

        for i, col_name in enumerate(self._column_names):
            value = row[i]

            if value is None:
                processed[col_name] = None
            else:
                processor = self._column_processors[i]
                processed[col_name] = processor(value)

        return processed


class VOTableWriter:
    """Writer supporting both VOTable BINARY2 and VOParquet formats.

    Both formats embed complete VOTable metadata - BINARY2 as wrapper XML,
    Parquet as embedded metadata following VOParquet 1.0 specification.
    """

    def __init__(self, http_client: AsyncClient, logger: BoundLogger) -> None:
        self._client = http_client
        self._logger = logger

    async def store(
        self,
        url: str | HttpUrl,
        config: JobResultConfig,
        results: AsyncGenerator[Row[Any]],
        format_type: OutputFormat = OutputFormat.VOTable,
        *,
        maxrec: int | None = None,
        overflow: bool = False,
    ) -> EncodedSize:
        """Store the encoded data in the specified format via HTTP PUT."""
        if format_type == OutputFormat.VOTable:
            return await self._store_votable(
                url, config, results, maxrec=maxrec
            )
        elif format_type == OutputFormat.Parquet:
            return await self._store_parquet(
                url, config, results, maxrec=maxrec, overflow=overflow
            )
        else:
            raise ValueError(f"Unsupported format: {format_type}")

    async def _store_votable(
        self,
        url: str | HttpUrl,
        config: JobResultConfig,
        results: AsyncGenerator[Row[Any]],
        maxrec: int | None = None,
    ) -> EncodedSize:
        """Store as VOTable BINARY2 format."""
        encoder = VOTableEncoder(config, self._logger)
        generator = encoder.encode(results, maxrec=maxrec)
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

    async def _store_parquet(
        self,
        url: str | HttpUrl,
        config: JobResultConfig,
        results: AsyncGenerator[Row[Any]],
        maxrec: int | None = None,
        *,
        overflow: bool = False,
    ) -> EncodedSize:
        """Store as VOParquet (Parquet with embedded VOTable metadata)."""
        encoder = VOParquetEncoder(config, self._logger, overflow=overflow)
        generator = encoder.encode(results, maxrec=maxrec)
        try:
            r = await self._client.put(
                str(url),
                headers={"Content-Type": "application/octet-stream"},
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
            total_bytes=encoder.encoded_size,
        )
