"""Utilities for reading test data."""

from datetime import datetime

from safir.testing.data import Data

from qservkafka.models.kafka import JobStatus
from qservkafka.models.qserv import QservAsyncStatusData

__all__ = ["QservKafkaData"]


class QservKafkaData(Data):
    """Manage test data for the qserv-kafka bridge."""

    def assert_job_status_matches(
        self, seen: JobStatus, path: str, *, execution_id: str | None = None
    ) -> None:
        """Raise an assertion if the job status model doesn't match.

        Parameters
        ----------
        seen
            Expected job status.
        path
            Path relative to :file:`tests/data` of the expected output. A
            ``.json`` extension will be added automatically.
        execution_id
            If provided, override the expected execution ID with the given
            value. If this is specified, do not overwrite the expected data
            even if data updating is enabled.

        Raises
        ------
        AssertionError
            Raised if the data doesn't match.
        """
        if self._update and execution_id is None:
            self.write_pydantic(seen, path, exclude_defaults=True)
        seen_json = seen.model_dump(mode="json", exclude_defaults=True)
        expected_json = self.read_json(path)
        if execution_id is not None:
            expected_json["executionID"] = execution_id
        assert seen_json == expected_json

    def read_qserv_status(
        self,
        path: str,
        *,
        query_id: int | None = None,
        query_begin: datetime | None = None,
        last_update: datetime | None = None,
    ) -> QservAsyncStatusData:
        """Read the result of q Qserv query status API call.

        Parameters
        ----------
        path
            Path relative to :file:`tests/data` of the expected output. A
            ``.json`` extension will be added automatically.
        query_id
            Override the ``execution_id`` field.
        query_begin
            Override the ``query_begin`` timestamp. Any microsecond value
            will be dropped since Qserv doesn't include microseconds.
        last_update
            Override the ``last_update`` timestamp. Any microsecond value
            will be dropped since Qserv doesn't include microseconds.

        Returns
        -------
        QservAsyncStatusData
            Parsed contents of the file.
        """
        model = self.read_pydantic(QservAsyncStatusData, path)
        if query_id is not None:
            model.query_id = query_id
        if query_begin:
            model.query_begin = query_begin.replace(microsecond=0)
        if last_update:
            model.last_update = last_update.replace(microsecond=0)
        return model
