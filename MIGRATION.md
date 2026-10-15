# Migrating from the Go implementation

The Rust implementation is a drop-in replacement of the Go implementation of Ministream:

- the **web API** is the same (routes, payloads, responses, error codes),
- the **configuration files** are the same (every template of `config-templates` works as is),
- the **data files** of the JSON file storage and the **MySQL tables** written by the Go implementation are read
  as is,
- the **command lines** are the same (`ministream -config config.yaml`, `ministream -version`,
  `generatepasswords -digest sha512 -iterations 1000 -count 10`).

Below are the differences: mostly bugs of the Go implementation which are fixed, and a few improvements.


## Storage

### JSON files

- **Index files.** The Go stream writer wrote index rows of 20 bytes while the Go reader (and the Go index rebuild)
  used rows of 32 bytes: seeking by message id or timestamp did not work until the index was rebuilt with the API.
  The Rust writer writes 32 bytes rows. At startup, an index which is not consistent with its data file (for
  example an index written by the Go writer) is rebuilt automatically.
- **Crash recovery.** An incomplete last line of a data file (crash while writing) is removed at startup. Records
  which cannot be decoded are skipped by the index rebuild instead of failing it.
- **Stream information file** (`stream.json`) is written atomically. The Go implementation did not truncate the file
  before writing, a shorter content left trailing garbage.
- **Properties** set or updated with the API are saved immediately (the Go implementation saved them only with the
  next records).
- Readers never return a record being written (the Go reader could lose a partially read line).

### MySQL

- The Go provider used inconsistent table names (the index used `stream_<uuid with dashes>` whatever the configured
  prefix, the deletion dropped a table with dashes), and the index statistics referenced a `creation_date` column
  which does not exist: seeking and index rebuilds failed. Fixed.
- Dates are sent to MySQL as date values. The Go provider sent RFC 3339 strings, which MySQL rejects for a date in
  UTC (suffix `Z`): on a server running in UTC (ex: a docker container) no stream could be created, and after a
  restart every write into an existing stream failed (the records accepted by the API were never saved).
- A failed write is rolled back (the Go provider left the transaction open: it kept its locks on the stream table,
  then the index rebuild and the deletion of the stream hung).
- Records are inserted with multi-rows `INSERT` statements (much faster).
- New tables use `DATETIME(6)` columns (sub-second timestamps, no year 2038 limit). Tables created by the Go
  implementation are still supported.
- The DSN can also be an URL (`mysql://...`). With the Go driver format, the `tls` parameter is supported
  (`true`, `skip-verify`, `preferred`, `false`), the other Go driver parameters are ignored.
- `maxIdleConns` is ignored (the connection pool manages idle connections by itself).
- Properties set or updated with the API are saved immediately (the Go implementation never saved them).

### In memory

- `maxSize` is enforced (it was not implemented). An empty `maxSize` means unlimited (the Go implementation
  refused to start).

### Ingestion

- The records still waiting in the ingestion channel are written when a stream stops (the Go implementation lost
  them).
- When the storage keeps failing, the buffered records are dropped after 5 attempts (the Go implementation kept
  them forever, and retried the records already written, writing duplicates).
- Successive records are written in a single batch even when `bulkFlushFrequency` is 0.
- `sizeInBytes` counts the size of the JSON records (the Go implementation counted the size of the Go debug
  representation of the records).


## Records iterators

- The position of an iterator is set when the iterator is created: an invalid position (ex: unknown message id) is
  reported by the creation request (status 400, code 1014) instead of failing every "get records" request.
- `LAST_MESSAGE` returns the last record (the in memory provider of the Go implementation returned nothing), and
  `AFTER_LAST_MESSAGE` does not skip the next record anymore (in memory provider).
- `AT_TIMESTAMP` with a timestamp after the last record waits for the next records instead of failing.
  `AFTER_MESSAGE_ID` with message id 0 starts at the first record.
- `lastRecordIdRead` is the id of the last record read (the in memory provider of the Go implementation returned an
  index starting at 0), and it is not reset to 0 when no record is read.
- jq filters work on every storage provider (with the in memory and MySQL providers, the Go implementation returned
  empty objects). A jq runtime error on a record counts as an error (`countErrors`) instead of returning the error as a
  record.
- Long polling (`maxWaitTimeSeconds`) returns as soon as new records are written (the Go implementation checked
  every second). Closing an iterator interrupts a pending long polling request.
- `GET .../records` on an unknown iterator returns status 400 with code 1031 (status 500 with code 1020 before).
- The "get iterator stats" response also contains the statistics of the iterator (`stats`).
- `maxAllowedIteratorsPerStream` is enforced exactly (one more iterator was allowed).
- New option `streams.iteratorIdleTimeoutInSeconds`: iterators not used for this duration are closed automatically
  (0, the default, means never).
- `maxMessagePerGetOperation: 0` means the default value (10000).
- The `timestamp` of an `AT_TIMESTAMP` iterator accepts any RFC 3339 date (fractional seconds and time offsets).


## Security

- **ABAC rules are enforced on the routes of a stream.** The Go implementation only applied the ABAC rules to the
  lists of streams: a rule like `rule_demo` of the templates granted its actions on every stream. Now the ABAC
  expression is evaluated on `{"streamUUID": "...", "properties": {...}}` of the stream of the route.
  An ABAC expression that yields no output denies the access (it granted it).
- The secrets file may contain comments: keys starting with `_` are ignored. The Go implementation could not start
  with the secured templates because of the `_WARNING` key of their `secrets.json`.
- Logging in with an unknown user returns 403 "wrong credentials" (it returned 500).
  `GET /api/v1/user/login` when RBAC is disabled returns code 1213 (1202 before).
- When the authentication is disabled, the credentials are not loaded (a missing credentials file prevented the
  Go implementation from starting).
- HTTP credentials: `{accountid}` in the URL is replaced by the account id, `cacheDurationInSec: 0` means the
  credentials are loaded for every login (they were never loaded).
- The `account` claim of the JWT is `webserver.jwt.accountId`, or the account id when it is empty, so that a token
  cannot be used on another server instance (the Go implementation used an empty value).
  `tokenExpireInMinutes: 0` means 60 minutes.
- The secret API key and passwords are compared in constant time, random keys use a cryptographically secure
  generator, and password hashing runs outside of the request threads.
- The rate limiter of the account and user routes uses `rateLimiter.routeAccount` (the Go implementation used
  `routeStream`).


## Web API

- `GET /api/v1/streams/?jq=...` (list of streams filtered by a jq expression) works: it deadlocked the Go
  implementation.
- The routes `/api/v1/stream`, `/api/v1/streams`, `/api/v1/users` and `/api/v1/account` also work without the
  trailing slash.
- Request bodies are parsed as JSON whatever the content type (the Go implementation required
  `application/json`). The JSON lines content type may have parameters (`application/x-ndjson; charset=utf-8`),
  and blank lines are ignored.
- `PUT .../records` with an empty list returns status 400 (500 before). `PUT .../record` with an invalid body
  returns code 1007 (1002 before).
- A batch id (`x-ministream-batch-id`) of a failed request can be sent again (`PUT .../record` kept it).
- Lists of streams and users are sorted (by creation date and by name).
- JSON object keys are sorted (ex: the record envelope is `{"d": ..., "i": ..., "m": ...}`), and dates are UTC.
- Some error details differ (they are produced by different JSON parsers).
- `/monitor` serves its own page (JSON with the header `Accept: application/json`), `/metrics` serves ministream
  and process metrics in the Prometheus format.
- The swagger UI (`/docs/index.html`) is loaded from a CDN.


## Server

- The program exits with status 1 on error (it exited with 0).
- `SIGHUP` restarts the server and reloads the configuration on Unix; Ctrl+C stops the server on every platform.
- Pending requests are given 10 seconds to complete when the server stops.
- A listen address can be `:8080` (every interface) or a host name.
