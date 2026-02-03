"""Writer for BINARY2-encoded VOTables and Parquet files."""

import asyncio
import struct
from abc import ABCMeta, abstractmethod
from binascii import b2a_base64
from collections.abc import AsyncGenerator, Sequence
from datetime import datetime
from io import BytesIO
from typing import Any, override
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from bitstring import BitArray
from httpx import AsyncClient, HTTPError
from pydantic import HttpUrl
from rubin.repertoire import DiscoveryClient
from structlog.stdlib import BoundLogger

from ..config import config as global_config
from ..constants import UPLOAD_BUFFER_SIZE
from ..exceptions import UploadWebError
from ..models.kafka import JobResultColumnType, JobResultConfig, JobResultType
from ..models.votable import EncodedSize, VOTablePrimitive

_BASE64_LINE_LENGTH = 64
"""Maximum length of a base64-encoded line in BINARY2 output."""

__all__ = [
    "Binary2Encoder",
    "VOParquetEncoder",
    "VOTableEncoder",
    "VOTableWriter",
]

# Set up the pyarrow memory pool.
pa.set_memory_pool(pa.system_memory_pool())


class StreamingAdapter:
    """Streaming adapter for Parquet encoding.

    This class serves as a buffer between pyarrow's ParquetWriter and an async
    generator that yields bytes. Adapted from the `StackOverflow example
    <https://stackoverflow.com/questions/64791558/>`__ to work with PyArrow
    ParquetWriter.
    """

    def __init__(self) -> None:
        self._buffer = BytesIO()
        self._written = 0
        self._closed = False

    def flush_buffer(self) -> bytes:
        """Flush the buffer and return bytes for yielding."""
        b = self._buffer.getvalue()
        self._buffer.seek(0)
        self._buffer.truncate(0)
        return b

    def write(self, b: bytes) -> None:
        """Write bytes to the buffer."""
        self._buffer.write(b)
        self._written += len(b)

    @property
    def written(self) -> int:
        """Total bytes written to the buffer."""
        return self._written

    @property
    def closed(self) -> bool:
        """Whether the buffer is closed."""
        return self._closed

    def tell(self) -> int:
        """Return total bytes written."""
        return self._written

    def close(self) -> None:
        """Mark buffer as closed."""
        self._closed = True
        if self._buffer:
            self._buffer.seek(0)
            self._buffer.truncate(0)
            self._buffer.close()
        self._written = 0

    def flush(self) -> None:
        """Flush method required by some writers."""


class VOTableEncoder(metaclass=ABCMeta):
    """Generic interface to VOTable encoding.

    An appropriate subclass of this class is created for each result to
    encode.

    Parameters
    ----------
    config
        Configuration for the output format. Includes the header, footer, and
        type information. The type information must exactly match the columns
        of the results. This is not checked.
    discovery_client
        Service discovery client.
    logger
        Logger to use.
    """

    def __init__(
        self,
        config: JobResultConfig,
        discovery_client: DiscoveryClient,
        logger: BoundLogger,
    ) -> None:
        self._config = config
        self._discovery = discovery_client
        self._logger = logger

        self._encoded_size = 0
        self._total_rows = 0

    @property
    def encoded_size(self) -> int:
        """Size of the encoded data without the VOTable wrapper."""
        return self._encoded_size

    @property
    def total_rows(self) -> int:
        """Total number of rows encoded."""
        return self._total_rows

    @property
    @abstractmethod
    def total_size(self) -> int:
        """Total size of the output VOTable, including header and footer."""

    @abstractmethod
    async def encode(
        self,
        results: AsyncGenerator[Sequence[Any]],
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
        yield b""

    async def rewrite_datalink_url(
        self, value_str: str, column_name: str
    ) -> str:
        """Rewrite a DataLink URL to point to the local DataLink server.

        Parameters
        ----------
        value_str
            Raw value from the TAP results.
        column_name
            Name of the column for error reporting.

        Returns
        -------
        str
            Rewritten version of the URL using the base URL for the local
            DataLink links service and the query string from the original
            URL.
        """
        try:
            datalink_url = await self._discovery.url_for_internal(
                "datalink", version="datalink-links-1.1"
            )
            if not datalink_url:
                self._logger.warning("No DataLink service found in Repertoire")
                return value_str
            base_url = urlparse(datalink_url)
            url = urlparse(value_str)
            return base_url._replace(query=url.query).geturl()
        except Exception as e:
            logger = self._logger.bind(column=column_name)
            logger.warning("Unable to rewrite URL", error=str(e))
            return value_str


class Binary2Encoder(VOTableEncoder):
    """Streaming encoder for the BINARY2 serialization format of VOTable.

    A new encoder should be used for every result set, since it keeps internal
    state in order to count the number of output rows.

    Parameters
    ----------
    config
        Configuration for the output format. Includes the header, footer, and
        type information. The type information must exactly match the columns
        of the results. This is not checked.
    discovery_client
        Service discovery client.
    logger
        Logger to use.
    """

    def __init__(
        self,
        config: JobResultConfig,
        discovery_client: DiscoveryClient,
        logger: BoundLogger,
    ) -> None:
        super().__init__(config, discovery_client, logger)
        self._wrapper_size = 0

    @property
    @override
    def total_size(self) -> int:
        return self._encoded_size + self._wrapper_size

    @override
    async def encode(
        self,
        results: AsyncGenerator[Sequence[Any]],
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
                encoded_row = await self._encode_row(
                    self._config.column_types, row
                )
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

    async def _encode_char_column(
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
            value_str = await self.rewrite_datalink_url(value_str, column.name)
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

    async def _encode_row(
        self, types: list[JobResultColumnType], row: Sequence[Any]
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
                output += await self._encode_char_column(column, value)
            elif datatype == VOTablePrimitive.unicode_char:
                output += await self._encode_unicode_char_column(
                    column=column, value_raw=value
                )
            else:
                output += datatype.pack(value)

        return nulls.tobytes() + output

    async def _encode_unicode_char_column(
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
            value_str = await self.rewrite_datalink_url(value_str, column.name)

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


class VOParquetEncoder(VOTableEncoder):
    """VOParquet encoder.

    Generates VOParquet data with embedded VOTable metadata as described in
    the `IVOA specification
    <https://www.ivoa.net/documents/Notes/VOParquet/>`__.

    This encoder uses PyArrow to write Parquet data directly to an in-memory
    buffer, which is then streamed to the caller.

    Parameters
    ----------
    config
        Configuration for the output format. Includes the header, footer, and
        type information. The type information must exactly match the columns
        of the results. This is not checked.
    discovery_client
        Service discovery client.
    logger
        Logger to use.
    overflow
        If `True`, use the overflow footer in the embedded VOTable metadata.
    """

    def __init__(
        self,
        config: JobResultConfig,
        discovery_client: DiscoveryClient,
        logger: BoundLogger,
        *,
        overflow: bool = False,
    ) -> None:
        super().__init__(config, discovery_client, logger)
        self._predetermined_overflow = overflow

        self._batch_size = global_config.parquet_batch_size
        self._column_names = [col.name for col in config.column_types]
        self._arrow_schema = self._build_arrow_schema()
        self._schema_with_metadata = self._build_schema_with_metadata(
            overflow=self._predetermined_overflow
        )

    @property
    @override
    def total_size(self) -> int:
        """Total size of the output, same as encoded_size."""
        return self._encoded_size

    @override
    async def encode(
        self,
        results: AsyncGenerator[Sequence[Any]],
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

                processed_row = await self._process_row_for_parquet(row)
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

            if current_batch:
                batch = self._batch_to_arrow_record_batch(current_batch)
                writer.write_batch(batch)

            writer.close()
            final_bytes = buffer.flush_buffer()

            if final_bytes:
                yield final_bytes

            self._encoded_size = buffer.written
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
            array = pa.array(column_data, type=expected_type)
            arrays.append(array)

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
        base_type = col.datatype.arrow_type

        if col.is_array():
            return pa.list_(base_type)
        return base_type

    async def _process_row_for_parquet(
        self, row: Sequence[Any]
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
            column = self._config.column_types[i]

            if value is None:
                processed[col_name] = None
            elif column.datatype == VOTablePrimitive.boolean:
                processed[col_name] = bool(value)
            elif column.is_array():
                result = value if isinstance(value, list) else [value]
                limit = column.arraysize.limit if column.arraysize else None
                if limit is not None and len(result) > limit:
                    result = result[:limit]
                processed[col_name] = result
            elif column.datatype in (
                VOTablePrimitive.char,
                VOTablePrimitive.unicode_char,
            ):
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
                if value_str and column.requires_url_rewrite:
                    value_str = await self.rewrite_datalink_url(
                        value_str, column.name
                    )

                # Apply length limit if specified
                if column.arraysize and column.arraysize.limit is not None:
                    value_str = value_str[: column.arraysize.limit]

                processed[col_name] = value_str
            else:
                processed[col_name] = value

        return processed


class VOTableWriter:
    """Writer supporting both VOTable BINARY2 and VOParquet formats.

    Both formats embed complete VOTable metadata: BINARY2 as wrapper XML,
    Parquet as embedded metadata following VOParquet 1.0 specification.

    Parameters
    ----------
    http_client
        Client used to upload the results.
    discovery_client
        Service discovery client.
    logger
        Logger to use.
    """

    def __init__(
        self,
        http_client: AsyncClient,
        discovery_client: DiscoveryClient,
        logger: BoundLogger,
    ) -> None:
        self._client = http_client
        self._discovery = discovery_client
        self._logger = logger

    async def store(
        self,
        url: str | HttpUrl,
        config: JobResultConfig,
        results: AsyncGenerator[Sequence[Any]],
        *,
        maxrec: int | None = None,
        overflow: bool = False,
    ) -> EncodedSize:
        """Store the encoded VOTable or VOParquet result via an HTTP PUT.

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
        maxrec
            Maximum record limit, if not `None`.
        overflow
            If `True`, use the overflow footer in the embedded VOTable
            metadata.

        Returns
        -------
        EncodedSize
            Size of the output VOTable.

        Raises
        ------
        UploadWebError
            Raised if there was a failure to upload the results.
        """
        match config.format.type:
            case JobResultType.Parquet:
                encoder: VOTableEncoder = VOParquetEncoder(
                    config,
                    self._discovery,
                    self._logger,
                    overflow=overflow,
                )
                mime_type = "application/vnd.apache.parquet"
            case JobResultType.VOTable:
                encoder = Binary2Encoder(config, self._discovery, self._logger)
                mime_type = "application/x-votable+xml; serialization=binary2"

        generator = encoder.encode(results, maxrec=maxrec)
        try:
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
