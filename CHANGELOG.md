# Change log

qserv-kafka is versioned with [semver](https://semver.org/).
Dependencies are updated to the latest available version during each release, and aren't noted here.

Find changes for the upcoming release in the project's [changelog.d directory](https://github.com/lsst-sqre/qserv-kafka/tree/main/changelog.d/).

<!-- scriv-insert-here -->

<a id='changelog-4.0.1'></a>
## 4.0.1 (2025-12-03)

### Bug fixes

- Use updated Safir so that app metrics won't break the app in rare situations if the underlying Kafka infrastructure is down.

<a id='changelog-4.0.0'></a>
## 4.0.0 (2025-12-02)

### Backwards-incompatible changes

- Request version 51 of the Qserv REST API instead of version 49. This only applies if the Qserv Kafka bridge is configured to send versions in REST API requests.

### New features

- Publish multi-platform images that support both linux/amd64 and linux/arm64.

### Bug fixes

- Suppress error reporting if canceling a query fails because the query already completed or failed.
- Improve formatting of error messages by making more room for the URL, adding the method, moving the (sometimes long) Qserv error message into a text block, and suppressing the (not useful to the user in practice) error details in the error message sent back to the user.

<a id='changelog-3.4.0'></a>
## 3.4.0 (2025-10-30)

### New features

- Add support for reporting errors directly to Slack.

### Bug fixes

- Don't report retriable Qserv disconnects as errors

<a id='changelog-3.3.0'></a>
## 3.3.0 (2025-10-16)

### New features

- Add support for reporting errors to Sentry.

<a id='changelog-3.2.0'></a>
## 3.2.0 (2025-10-06)

### New features

- Add support for an output format of VOParquet, which generates a Parquet file with VOTable metadata according to the current draft IVOA standard.

<a id='changelog-3.1.0'></a>
## 3.1.0 (2025-08-27)

### New features

- Accept batches of query jobs up to a configurable batch size and send them to Qserv in parallel to increase throughput. The size of the batch is controlled by the new configuration setting `job_run_batch_size`. The Qserv Kafka bridge previously was not keeping up with large numbers of small, wide queries.
- Add a new configuration setting, `job_run_max_bytes`, to increase the size of a batch the bridge will read from Kafka at one time.
- Add a new configuration setting, `redis_max_connections`, to tune the size of the Redis connection pool.

### Bug fixes

- Update collation for user tables to `utf8mb4_uca1400_ai_ci`, which uses a newer version of the Unicode standard.
- Change the default for `max_worker_jobs` to 2, matching the Phalanx configuration. Because worker jobs are CPU-bound, there's rarely a reason to increase the number of jobs per worker higher than this.
- Changed the defaults for the Qserv database connection pool and REST connection pool to align with the default for the message batch size.
- Limit the Qserv database connection pool size in remote workers to the number of simultaneous jobs that worker is willing to handle, rather than using the full pool size of the frontend (which is massive overkill for a typical worker).

### Other changes

- Add `submit_elapsed` to successful query metrics, which tracks the elapsed time from the receipt of the query to successfully starting the query in Qserv.

<a id='changelog-3.0.0'></a>
## 3.0.0 (2025-08-22)

### Backwards-incompatible changes

- Set character set to `utf8mb4` and collation to `utf8mb4_unicode_520_ci` when creating user-uploaded tables in Qserv. This requires version 49 of the Qserv REST API and allows the full range of valid UTF-8 characters and forces a matching collation, instead of using the non-matching collation default in API version 49.
- Request version 49 of the Qserv REST API instead of version 47. This only applies if the Qserv Kafka bridge is configured to send versions in REST API requests.

<a id='changelog-2.0.0'></a>
## 2.0.0 (2025-08-08)

### Backwards-incompatible changes

- Request version 47 of the Qserv REST API instead of version 43. This only applies if the Qserv Kafka bridge is configured to send versions in REST API requests.

### New features

- Add support for the Qserv `FAILED_LR` status, which indicates that a query failed because the results were too large. Return a hopefully user-understandable error message in that case that asks the user to narrow their query.
- Read the error message for a query that failed in Qserv and include that error in the error message sent back to the user.

### Other changes

- Include more status information from Qserv in log messages.

<a id='changelog-1.4.0'></a>
## 1.4.0 (2025-08-07)

### Other changes

- Update the arq metrics support to be compatible with the latest Safir release.

<a id='changelog-1.3.0'></a>
## 1.3.0 (2025-08-06)

### New features

- When cleaning up after completed queries that included user table uploads, delete the entire database rather than only the table. The TAP server side now specifies a separate user database per query with uploaded tables. Deleting the entire database simplifies error handling in Qserv.
- At the start of every worker job to process query results, publish an [ArqQueueJobEvent](https://safir.lsst.io/user-guide/arq.html#per-job-metrics) with a `time_in_queue` field. This will allow us to monitor whether waiting for a result worker is causing query delays.
- Add a command-line interface for publishing an [ArqQueueStatsEvent](https://safir.lsst.io/api/safir.metrics.ArqQueueStatsEvent.html#safir.metrics.ArqQueueStatsEvent). This is called periodically from a Kubernetes `CronJob`.

<a id='changelog-1.2.0'></a>
## 1.2.0 (2025-07-29)

### New features

- Add support for encoding `unicodeChar` fields in results, with correct truncation before surrogate pairs if the value is (in violation of the VOTable specification) UTF-16. Currently, this will only arise for queries with user table uploads where the user specified that a field had the `unicodeChar` type. This type is deprecated; better Unicode support will be added in the future based on the outcome of ongoing IVOA discussions.

### Bug fixes

- Fix the `encoded_size` field of success metrics events to track the result size after base64 encoding, not before.
- Avoid the use of `aiojobs.Scheduler.shield` in background tasks since it appears to cause a memory leak.

<a id='changelog-1.1.0'></a>
## 1.1.0 (2025-07-21)

### New features

- Add support for retrieving a user's TAP quota information from Gafaelfawr and imposing a limit on the number of concurrent queries a user is allowed to run. Queries in excess of their quota will be rejected with an error.

### Bug fixes

- Increase termination grace period for arq workers and shield closing the database query results. This hopefully will fix an issue where arq's attempt to cancel a timed-out task converted the cancellation exception to a SQL exception, which triggered a retry of the query in one worker while simultaneously processing the query in a second worker.

### Other changes

- Add the TAP query ID to all metrics events.
- Include the elapsed time required to upload the table to Qserv in metrics events for successful user table uploads. This does not include the time required to retrieve the table from its source URLs before upload.

<a id='changelog-1.0.1'></a>
## 1.0.1 (2025-06-27)

### Bug fixes

- Fix query start and end times in status responses. Previously, the start time and, for canceled or completed queries, the end time were taken directly from the Qserv query information. This includes only time spent by Qserv, which results in an inaccurate picture of the query execution time in the TAP UWS table. Instead measure query timings from the time the query was received by the bridge to when it completes uploading of the results, or is aware of a failure.

<a id='changelog-1.0.0'></a>
## 1.0.0 (2025-06-19)

### New features

- Delete temporary table uploads from Qserv after completion of the query.
- Allow configuration of the number of retries for Qserv API network failures and the delay between retries. The defaults are 3 and one second, respectively.

<a id='changelog-0.7.0'></a>
## 0.7.0 (2025-06-17)

### Backwards-incompatible changes

- Request version 43 of the Qserv REST API instead of version 41.
- When sending the API version to Qserv's REST API, always send it as a query parameter and never in the request body. The Qserv version 43 API supports this and it's simpler and more consistent.

### New features

- Optionally support HTTP Basic Authentication for Qserv REST API calls.

### Bug fixes

- Fix retries of SQL connection failures when getting the list of active queries.

### Other changes

- Log metrics events for protocol errors when talking to Qserv with separate events for each failed attempt, rather than one event with a count of retries per eventually-successful API call.

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
