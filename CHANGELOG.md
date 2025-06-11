# Change log

qserv-kafka is versioned with [semver](https://semver.org/).
Dependencies are updated to the latest available version during each release, and aren't noted here.

Find changes for the upcoming release in the project's [changelog.d directory](https://github.com/lsst-sqre/qserv-kafka/tree/main/changelog.d/).

<!-- scriv-insert-here -->

<a id='changelog-0.6.0'></a>
## 0.6.0 (2025-06-11)

### Backwards-incompatible changes

- Request version 41 of the Qserv REST API instead of version 39.

### New features

- Correctly handle queries with `MAXREC` set by truncating the results at at `MAXREC` and using the overflow XML footer rather than the regular XML footer if truncation was necessary.
- Explicitly delete results from Qserv after they have been successfully retrieved.
- Retry HTTP requests to Qserv, and to retrieve user table uploads, up to three times on HTTP request failure, pausing for one second between requests. This will hopefully work around ongoing network instability.
- Retry SQL requests to Qserv and result uploads up to three times, pausing for one second between attempts. This will hopefully work around ongoing network instability.
- Allow disabling the sending of the expected Qserv REST API version on each request. This allows the Qserv Kafka bridge to work with newer Qserv REST API versions without modification, provided that the new REST API is backwards-compatible.

### Other changes

- Add `qserv_size` and `qserv_rate` to the metrics events for success, tracking the Qserv-reported result size and the bytes per second rate of the Qserv execution.
- Add more metadata from Qserv to the log messages for successful and failed queries.

<a id='changelog-0.5.0'></a>
## 0.5.0 (2025-05-30)

### New features

- Add support for table uploads to Qserv before running a query. In this initial version, the table and schema are retrieved into memory rather than streaming them, and no attempt is made to delete the uploaded table after the query completes.

### Bug fixes

- Correctly update the query state in the result worker if Qserv unexpectedly reports it as still running to ensure that it will be checked again and not simply dropped.
- When result processing fails, close the streaming response from MySQL before attempting to roll back the transaction, hopefully suppressing otherwise unintelligible SQLAlchemy errors about packet sequences.
- Allow completed chunks as returned from the Qserv REST API to be `None` and treat that the same as 0.

<a id='changelog-0.4.0'></a>
## 0.4.0 (2025-05-28)

### New features

- Add support for the `short` VOTable data type.

### Bug fixes

- Catch more SQLAlchemy errors during result processing, hopefully closing an error case where result processing failed without sending an error reply or removing the query from Redis.
- When formatting `char` fields in VOTable results, convert column `datetime` types to the ISO 8601 string representation required by the IVOA DALI standard. Previously, such fields were serialized using the Python `str` value of `datetime`, which does not match the expected IVOA format.

<a id='changelog-0.3.0'></a>
## 0.3.0 (2025-05-16)

### New features

- Add metrics events on query success, failure, and abort. Include elapsed time for Qserv, result processing, and the entire query, row count, encoded size, and processing rates by result size for the whole query and just result processing.

### Bug fixes

- Log the Qserv query ID as a string, since one apparently cannot search for numbers in Google's Log Explorer.

### Other changes

- Improve debug logging to include more metadata and provide more specifics when starting a new query.

<a id='changelog-0.2.0'></a>
## 0.2.0 (2025-05-12)

### New features

- Add new configuration options to change the maximum number of connections and the timeout for requests to the Qserv REST API.
- Add new configuration options to change the steady-state connection pool size and the maximum simultaneous connections limit for the Qserv MySQL API.
- Processing of results was, under load, consuming the full CPU available to the Qserv Kafka bridge in a single pod deployment. Add an arq queue and move result processing into arq workers so that they can be horizontally scaled. For simplicitly, keep a single frontend pod responsible for creating jobs, monitoring query status, and telling the arq workers when a query is ready for processing.

### Bug fixes

- Forget about queries and drop them from Redis after one day as a last fallback to clean out stranded data in Redis.
- Use a buffering strategy in the VOTable BINARY2 encoder to simplify the async generator stack and avoid creating as many small objects that have to be garbage-collected. Accumulate roughly 64KB of encoded data before writing to the upload URL to reduce the number of small writes. These changes produced roughly a 2x performance improvement in encoding.

### Other changes

- Add the elapsed time to the log message sent when result processing is finished.
- Add a debug message after each 100,000 rows procesed during result processing.

<a id='changelog-0.1.0'></a>
## 0.1.0 (2024-12-12)

Initial release with support for running queries, retrieving results, and cancelling queries.
This release does not support `MAXREC` or table upload.
