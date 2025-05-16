# Change log

qserv-kafka is versioned with [semver](https://semver.org/).
Dependencies are updated to the latest available version during each release, and aren't noted here.

Find changes for the upcoming release in the project's [changelog.d directory](https://github.com/lsst-sqre/qserv-kafka/tree/main/changelog.d/).

<!-- scriv-insert-here -->

<a id='changelog-0.3.0'></a>
## 0.3.0 (2025-05-16)

### New features

- Add metrics events on query success, failure, and abort. Include elapsed time for Qserv, result processing, and the entire query, row count, encoded size, and processing rates by result size for the whole query and just result processing.

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
