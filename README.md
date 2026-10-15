# Ministream (Rust)

[![license](https://img.shields.io/github/license/nbigot/ministream-rs)](./LICENSE)

<p align="center">
  <img alt="Ministream" title="Ministream" src="docs/content/assets/img/ministream.logo.png">
</p>

## Overview

Ministream is an open-source real-time data stream service, well suited for event driven architectures.

Ministream is simple and straightforward: it runs on a single server and has no dependency on third parties.
Records are pushed and pulled with simple HTTP requests, and filtered or transformed on the fly with [jq](https://jqlang.github.io/jq/) expressions.
It is served by its own HTTP(S) server and stores data in memory, in JSON lines files or in MySQL.
Ministream easily fits in a standalone docker container, and provides a complete web API to manage the server.

This is the Rust implementation of Ministream, a rewrite of the [Go implementation](https://github.com/nbigot/ministream).
It keeps the same web API, the same configuration files and the same data files: see [MIGRATION.md](MIGRATION.md)
for the (few) differences and the bugs fixed along the way.


## Quick start

### Download a release

The [releases](https://github.com/nbigot/ministream-rs/releases) provide static binaries for Linux (x86_64, arm64,
armv7) and Windows (x86_64), with the configuration templates:

```sh
$ tar -xzf ministream-rs_<version>_linux_amd64.tar.gz
$ cd ministream-rs_<version>_linux_amd64
$ ./ministream -config config-templates/minimal-inmemory/config/config.yaml
```

### Build and run from the source code

Requirements: a [Rust toolchain](https://rustup.rs/) (1.80 or newer).

```sh
$ cargo build --release
$ ./target/release/ministream -config config-templates/minimal-inmemory/config/config.yaml
```

Or simply:

```sh
$ cargo run --release -- -config config-templates/minimal-inmemory/config/config.yaml
```

Set the version reported by the server at build time:

```sh
$ MINISTREAM_VERSION="$(git describe --tags --abbrev=0)" cargo build --release
```

The MySQL storage provider is enabled by default (cargo feature `mysql`), build a leaner binary without it with:

```sh
$ cargo build --release --no-default-features
```

### Docker

The image of each release is published for linux/amd64, linux/arm64 and linux/arm/v7:

```sh
$ docker run --name ministream -it -p 8080:8080 ghcr.io/nbigot/ministream-rs:latest
```

Or build it from the source code:

```sh
$ docker build -t nbigot/ministream-rs .
$ docker run --name ministream -it -p 8080:8080 nbigot/ministream-rs
```

The image uses the configuration `config-templates/docker` (JSON file storage in `/app/data/storage`).


## Ministream quick tips

Let's assume Ministream is running and listening on the tcp port 8080.

```sh
$ curl http://localhost:8080
Welcome to ministream!

$ curl http://localhost:8080/api/v1/utils/ping
ok

$ curl http://localhost:8080/livez
OK

# create a stream
$ curl -X POST http://localhost:8080/api/v1/stream/ -d '{"properties": {"name": "orders"}}'
{"uuid":"4ce589e2-b483-467b-8b59-758b339801db", ...}

# put records (a JSON array, or JSON lines with the content type application/x-ndjson)
$ curl -X PUT http://localhost:8080/api/v1/stream/4ce589e2-b483-467b-8b59-758b339801db/records \
    -d '[{"item": "book", "price": 12}, {"item": "pen", "price": 2}]'
{"status":"success","streamUUID":"4ce589e2-...","duration":0,"count":2,"messageIds":[1,2]}

# create a records iterator (with an optional jq filter and long polling)
$ curl -X POST http://localhost:8080/api/v1/stream/4ce589e2-b483-467b-8b59-758b339801db/iterator \
    -d '{"iteratorType": "FIRST_MESSAGE", "jqFilter": "select(.m.price > 5) | .m", "maxWaitTimeSeconds": 20}'
{"status":"success","message":"Stream iterator created","streamUUID":"4ce589e2-...","streamIteratorUUID":"0b9e..."}

# get the records
$ curl http://localhost:8080/api/v1/stream/4ce589e2-b483-467b-8b59-758b339801db/iterator/0b9e.../records?maxRecords=100
{"status":"success","count":1,"remain":false,"lastRecordIdRead":2,"records":[{"item":"book","price":12}], ...}
```

Records are returned in an envelope `{"i": <message id>, "d": <creation date>, "m": <record>}` (the jq filter applies
on this envelope).


## Web API

The documentation of the API is served by the server at `/docs/index.html` when `webserver.swagger.enable` is true
(the OpenAPI document is [docs/swagger.json](docs/swagger.json)).

| Method | Route | RBAC action |
|---|---|---|
| GET | `/api/v1/streams/?jq=<filter>` | ListStreams |
| GET | `/api/v1/streams/properties?jq=<filter>` | ListStreamsProperties |
| POST | `/api/v1/stream/` | CreateStream |
| GET | `/api/v1/stream/{streamuuid}` | GetStreamDescription |
| DELETE | `/api/v1/stream/{streamuuid}` | DeleteStream |
| GET | `/api/v1/stream/{streamuuid}/properties` | GetStreamProperties |
| POST | `/api/v1/stream/{streamuuid}/properties` | SetStreamProperties |
| PATCH | `/api/v1/stream/{streamuuid}/properties` | UpdateStreamProperties |
| PUT | `/api/v1/stream/{streamuuid}/record` | PutRecord |
| PUT | `/api/v1/stream/{streamuuid}/records` | PutRecords |
| POST | `/api/v1/stream/{streamuuid}/iterator` | CreateRecordsIterator |
| GET | `/api/v1/stream/{streamuuid}/iterator/{streamiteratoruuid}/records?maxRecords=<n>` | GetRecords |
| GET | `/api/v1/stream/{streamuuid}/iterator/{streamiteratoruuid}/stats` | GetRecordsIteratorStats |
| DELETE | `/api/v1/stream/{streamuuid}/iterator/{streamiteratoruuid}` | CloseRecordsIterator |
| POST | `/api/v1/stream/{streamuuid}/index/rebuild` | RebuildIndex |
| GET | `/api/v1/account/` | GetAccount |
| GET | `/api/v1/account/validate` (header `API-KEY`) | |
| GET | `/api/v1/account/login` (header `API-KEY`) | |
| GET | `/api/v1/user/login` (headers `ACCESS-KEY-ID`, `SECRET-ACCESS-KEY`) | |
| GET | `/api/v1/users/` | ListUsers |
| POST | `/api/v1/admin/server/shutdown` | ShutdownServer |
| POST | `/api/v1/admin/server/restart` | RestartServer |
| POST | `/api/v1/admin/jwt/revoke` | JWTRevokeAll |
| POST | `/api/v1/utils/pbkdf2` | |
| GET | `/api/v1/utils/ping` | |
| GET | `/livez`, `/readyz` | |
| GET | `/monitor` (when enabled) | |
| GET | `/metrics` (Prometheus, when enabled) | |

Iterator types: `FIRST_MESSAGE`, `LAST_MESSAGE`, `AFTER_LAST_MESSAGE`, `AT_MESSAGE_ID` and `AFTER_MESSAGE_ID`
(with `messageId`), `AT_TIMESTAMP` (with `timestamp`, RFC 3339).

A batch of records can be sent with the header `x-ministream-batch-id`: a batch id already received during the last
5 minutes is rejected, so that a client can safely retry a request.


## Configuration

Ministream is configured with a single YAML file, see the templates of the [config-templates](config-templates)
directory:

| Template | Storage | Security |
|---|---|---|
| `minimal-inmemory` | in memory | none |
| `minimal-jsonfilestorage` | JSON files | none |
| `minimal-mysql-storage` | MySQL | none |
| `secured-inmemory` | in memory | authentication, JWT, RBAC |
| `secured-jsonfilestorage` | JSON files | authentication, JWT, RBAC |
| `docker` | JSON files | none |

Pay attention to the directory paths of the configuration files. The configuration file is reloaded when the server
restarts (API `POST /api/v1/admin/server/restart`, or signal `SIGHUP` on Unix).

Main sections:

- `storage`: storage provider (`type`: `InMemory`, `JSONFile` or `MySQL`) and its own logger,
- `streams`: ingestion buffering (`bulkFlushFrequency` in seconds, `bulkMaxSize`, `channelBufferSize`), limits
  (`maxAllowedStreams`, `maxAllowedIteratorsPerStream`, `maxMessagePerGetOperation`) and
  `iteratorIdleTimeoutInSeconds` (iterators not used for this duration are closed, 0 means never),
- `account`: account name, id and secret API key (random values are generated and logged when empty),
- `auth`: authentication of the users (`FILE` or `HTTP` credentials),
- `rbac`: role based access control rules file,
- `auditlog`: logs of the access decisions,
- `webserver`: HTTP/HTTPS listeners, CORS, rate limiter, JWT, access logs, monitor, metrics and swagger,
- `logger`: the application logger (zap compatible configuration: level, json/console encoding, outputs, ...).

### MySQL

The data source name `storage.mysql.dsn` is either an URL (`mysql://user:password@host:3306/ministream`) or a DSN
in the format of the Go MySQL driver (`user:password@tcp(host:3306)/ministream?tls=skip-verify`).
When the value starts with `$` it is the name of an environment variable holding the DSN.

Start a MySQL server for testing:

```sh
$ docker run --name mysql-ministream -e MYSQL_ROOT_PASSWORD=my-secret-pw -e MYSQL_DATABASE=ministream -p 3306:3306 -d mysql:8
```


## Security

- **Authentication**: users credentials are PBKDF2 hashes stored in a JSON file (or returned by an HTTP endpoint).
  Generate hashes with the `generatepasswords` tool:

  ```sh
  $ cargo run --release --bin generatepasswords -- -digest sha512 -iterations 10000 -password mysecretpassword
  ```

- **JWT**: `GET /api/v1/account/login` with the secret API key returns a super user token,
  `GET /api/v1/user/login` with the user credentials returns a user token.
  Then send the token in the header `Authorization: Bearer <token>`.
- **RBAC**: users have roles, roles have rules, rules grant actions.
  A rule may have an ABAC jq expression evaluated on the stream (`{"streamUUID": "...", "properties": {...}}`),
  or on the stream properties to filter the lists of streams.
- **Rate limiter**, **CORS** and **HTTPS** can be enabled in the `webserver` section.

Read [config-templates/SECURITY_NOTICE.md](config-templates/SECURITY_NOTICE.md): the credentials and certificates of
the templates are examples only.


## Development

```sh
$ cargo test                    # unit and integration tests
$ cargo clippy --all-targets    # linter
$ cargo fmt                     # code formatting
```

The MySQL integration test runs when the environment variable `MINISTREAM_TEST_MYSQL_DSN` is set:

```sh
$ MINISTREAM_TEST_MYSQL_DSN="root:my-secret-pw@tcp(127.0.0.1:3306)/ministream" cargo test --test api_mysql
```

### Releasing

1. Set the new version in `Cargo.toml` (`cargo check` updates `Cargo.lock`), and merge it into `main`.
2. Push the tag of this version: `git tag v1.2.3 && git push origin v1.2.3`.

The workflow `.github/workflows/release.yml` then builds the binaries and publishes them in a GitHub release,
and publishes the docker image `ghcr.io/nbigot/ministream-rs:<version>` (and `latest`). The tag must match the
version of `Cargo.toml`.

### Source code layout

| Path | Content |
|---|---|
| `src/main.rs` | the server program |
| `src/bin/generatepasswords.rs` | the password hashes generator |
| `src/config.rs` | configuration file |
| `src/service.rs` | lifecycle of the streams |
| `src/stream/` | streams, ingestion, records iterators |
| `src/storage/` | storage providers (in memory, JSON files, MySQL) |
| `src/auth/`, `src/rbac.rs` | authentication and access control |
| `src/web/` | web API: router, handlers, JWT, RBAC middleware, rate limiter, server |
| `tests/` | integration tests of the web API |


## License

This software is licensed under the [MIT](./LICENSE) license.
