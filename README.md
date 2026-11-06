# PocketBase with PostgresSQL

**Features**

- ✅ PostgresSQL (v17+) support
- ✅ Support horizontal scaling
- ✅ Realtime events works normally with horizontal scaling
- ✅ 100% test case pass rate across all unit and integration tests
- ✅ Fully compatible with upstream PocketBase v0.41.1 SDKs/Docs
- ✅ Versioning: `v<upstream>-pg` for upstream syncs, `v<upstream>-pg.<N>` for fork-specific PostgreSQL patches
**Demo App**

To demonstrate the horizontal scaling and realtime capabilities, I have deployed a **realtime chat demo app** on two different PocketBase instances.

- Instance 1: [pocketbase-chat-01.mimimiao.com](https://pocketbase-chat-01.mimimiao.com)
- Instance 2: [pocketbase-chat-02.mimimiao.com](https://pocketbase-chat-02.mimimiao.com)

![Realtime Chat Demo](./.github/chat-page-demo.png)

**Get Started**

Everything is the same as the original PocketBase, except that an additional environment variable `POSTGRES_URL` is required.

```sh
export POSTGRES_URL=postgres://user:pass@127.0.0.1:5432/postgres?sslmode=disable
./pocketbase serve
```

See: [pocketbase/pocketbase](https://ganigeorgiev.com/pocketbase/pocketbase) (upstream moved off GitHub in v0.41.0; the GitHub repository is archived)

**Use as a Go framework**

Since `v0.41.1-pg` the fork uses the upstream module path `ganigeorgiev.com/pocketbase/pocketbase`. Import `ganigeorgiev.com/pocketbase/pocketbase/...` in your code and replace the module and its `dbx` dependency in your `go.mod`. Go ignores the `replace` lines of dependencies, so your app needs both:

```
require ganigeorgiev.com/pocketbase/pocketbase v0.41.1

replace ganigeorgiev.com/pocketbase/pocketbase => github.com/DrDejaVuNG/pocketbase v0.41.1-pg

replace ganigeorgiev.com/pocketbase/dbx => github.com/DrDejaVuNG/dbx v1.12.1-0.20261105154651-66e6fcf5a301
```

Upgrade from `v0.40.x-pg`: in your `.go` files change `github.com/pocketbase/pocketbase`, `github.com/pocketbase/dbx`, `github.com/pocketbase/ozzo-validation/v4` and `github.com/pocketbase/tygoja` to `ganigeorgiev.com/pocketbase/...` and `github.com/ganigeorgiev/fexpr` to `ganigeorgiev.com/fexpr`, change the `replace` lines as above, then run `go mod tidy`.

**Deploy with Docker**

1. Start PostgresSQL:

   ```sh
   docker run -d \
     --name postgres \
     -p 5432:5432 \
     -e POSTGRES_USER=user \
     -e POSTGRES_PASSWORD=pass \
     -v ./pg_data:/var/lib/postgresql \
     postgres:18-alpine
   ```

2. Start PocketBase (Don't use @latest tag, use a specific version in production)

   ```sh
   docker run -d \
     --network=host \
     --name pocketbase \
     -v ./pb_data:/data \
     -e PB_HTTP_ADDR=127.0.0.1:8090 \
     -e PB_DATA_DIR="/data" \
     -e POSTGRES_URL="postgres://user:pass@127.0.0.1:5432/postgres?sslmode=disable" \
     ghcr.io/fondoger/pocketbase:latest
   ```

3. Get admin password reset link
   ```sh
   docker logs -f pocketbase
   ```

**Available Environment Variables**
| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `POSTGRES_URL` | PostgresSQL connection URL (required: `serve` and the other commands fail at startup when it is not set; also `--postgresUrl`) | required |
| `POSTGRES_DATA_DB` | Database name for the PocketBase data | `pb-data` |
| `POSTGRES_AUX_DB` | Database name for the PocketBase logs data | `pb-auxiliary` |
| `PB_DATA_DIR` | Directory to store the PocketBase data | `./pb_data` |
| `PB_PUBLIC_DIR` | Directory to store the public files | `./pb_public` |
| `PB_HOOKS_DIR` | Directory to store the custom hooks | `./pb_hooks` |
| `PB_REALTIME_BRIDGE` | Enable/Disable the realtime bridge. Disable it if you don't need horizontal scaling or don't need realtime feature. | `true` |
| `PB_HTTP_ADDR` | TCP address to listen for the HTTP server | `127.0.0.1:8090` if no domain specified |
| `PB_HTTPS_ADDR` | TCP address to listen for the HTTPS server | - |
| `PB_PATH_PREFIX` | URL path prefix for the HTTP server (Useful when reuse same domain for diffrent sites behind nginx) | - |
| `PB_ALLOWED_ORIGINS` | Comma separated list of allowed CORS origins | `*` (all origins) |
| `PB_DATA_MAX_OPEN_CONNS` / `PB_DATA_MAX_IDLE_CONNS` | Data DB pool size per node (keep `nodes × (data + aux)` below PostgresSQL `max_connections` minus reserved; data ≥ 4 with the realtime bridge; keep idle = open, a smaller idle pool reconnects on every request under load) | `20` / `20` |
| `PB_AUX_MAX_OPEN_CONNS` / `PB_AUX_MAX_IDLE_CONNS` | Logs DB pool size per node | `4` / `4` |
| `PB_TX_SERIALIZE` | Serialize top-level `RunInTransaction` calls across all nodes with an advisory lock (prevents lost updates, like SQLite's single writer). Set `false` only if your code locks rows itself. | `true` |
| `PB_REALTIME_PING_SECONDS` | SSE keepalive interval for proxies with idle timeouts (`0` disables) | `30` |
| `PB_HEALTH_LOG_SUCCESS` | Log successful `/api/health` requests | `false` |

**Limitations**

- Local file system is not synced across multiple instances.
  > You need to add a S3 storage account if you are deploying multiple instances and need the file upload feature.
- The built-in SQLite Backup feature is not supported
  > PostgresSQL have many mature and stable backup solutions. Eg: `pg_dump`, `docker-pg-backup`, `postgres-backup-s3`.
- The `update` (self-update) command is not available: the upstream command installs the upstream SQLite release. Deploy a new binary or Docker image instead.
- Connect to PostgresSQL directly or through a pooler in session mode. Transaction-mode poolers (PgBouncer, RDS Proxy) break the realtime bridge (`LISTEN`) and the migration locks.
- Rate limits, OTP guess limits and resend throttles are kept per node; with N nodes behind a load balancer the effective limit is N times the configured one.
- Realtime bridge messages changed format on 2026-11-04; during a rolling upgrade from an older build, realtime events between old and new nodes are not delivered.
- Transactions (record deletes, batch requests, `RunInTransaction`) are serialized across all nodes while `PB_TX_SERIALIZE` is on (the default), similar to SQLite's single writer. Keep them short: a 3-node test cluster handled about 250-700 transactions per second in total.
- Add an index to relation fields that other records reference with cascade delete (or that are unset on delete): each delete looks up the referencing records inside its serialized transaction.
- Record creates and updates lock the related records they reference (`FOR KEY SHARE`, like a foreign key check) until they commit, so a concurrent delete of a related record can't leave a dangling relation: the delete waits, or the write fails with the relation validation error (400). `SaveNoValidate` writes are not checked.
- Empty date fields are stored as `NULL` (upstream SQLite stores `''`), and filters and sorts follow the PostgreSQL `NULL` rules everywhere (API, rules, Go queries, views): `due < X` and `due !~ '2024'` don't match empty dates, and empty dates sort last ascending and first descending (`sort=-due`). Use `due = ''` / `due != ''` to include or exclude them explicitly.
- Statement descriptions are cached per connection (pgx `cache_describe`; set `default_query_exec_mode` in `POSTGRES_URL` to change it, e.g. `describe_exec`). Schema changes made through PocketBase (migrations, the dashboard, `app.Save(collection)`, other nodes through the realtime bridge) clear the caches. After changing tables with another tool (psql, an external migration tool), a transaction that reads the changed table can fail once per pooled connection until the first stale statement error resets the pool (requests outside transactions are retried). Code that changes tables directly can call `core.ResetStatementCaches()` afterwards.
- Multi-node production scenarios: see `examples/prodtest/README.md`.

**Disclaimer**

I don't want to create another fork of PocketBase, but I really need PostgresSQL support for my project. The eventual goal is to merge this code back into the main PocketBase repository.

If you want to contribute, please first go to [pocketbase/pocketbase](https://ganigeorgiev.com/pocketbase/pocketbase). This repository only handles the bugs related to PostgresSQL.

---

## Original README.md file

<p align="center">
    <a href="https://pocketbase.io" target="_blank" rel="noopener">
        <img src="https://i.imgur.com/aCBbjKx.png" alt="PocketBase - open source backend in 1 file" />
    </a>
</p>

<p align="center">
    <a href="https://pkg.go.dev/ganigeorgiev.com/pocketbase/pocketbase" target="_blank" rel="noopener"><img src="https://godoc.org/ganigeorgiev.com/pocketbase/pocketbase?status.svg" alt="Go package documentation" /></a>
</p>

[PocketBase](https://pocketbase.io) is an open source Go backend that includes:

- embedded database (_SQLite_) with **realtime subscriptions**
- built-in **files and users management**
- convenient **Superuser dashboard UI**
- and simple **REST-ish API**

**For documentation and examples, please visit https://pocketbase.io/docs .**

> **NB!** Please keep in mind that PocketBase is still under active development
> and full backward compatibility is not guaranteed before reaching v1.0.0.


## API SDK clients

The easiest way to interact with the PocketBase Web APIs is to use one of the official SDK clients:

- **JavaScript - [pocketbase/js-sdk](https://ganigeorgiev.com/pocketbase/js-sdk)** (_Browser, Node.js, React Native_)
- **Dart - [pocketbase/dart-sdk](https://ganigeorgiev.com/pocketbase/dart-sdk)** (_Web, Mobile, Desktop, CLI_)

You could also check the recommendations in https://pocketbase.io/docs/how-to-use/.


## Overview

### Use as standalone app

You could download the prebuilt executable for your platform from the [Releases page](https://ganigeorgiev.com/pocketbase/pocketbase/releases).
Once downloaded, extract the archive and run `./pocketbase serve` in the extracted directory.

The prebuilt executables are based on the [`examples/base/main.go` file](https://ganigeorgiev.com/pocketbase/pocketbase/files/examples/base/main.go) and comes with the JS VM plugin enabled by default which allows to extend PocketBase with JavaScript (_for more details please refer to [Extend with JavaScript](https://pocketbase.io/docs/js-overview/)_).

### Use as a Go framework/toolkit

PocketBase is distributed as a regular Go library package which allows you to build
your own custom app specific business logic and still have a single portable executable at the end.

Here is a minimal example:

0. [Install Go 1.27+](https://go.dev/doc/install) (_if you haven't already_)

1. Create a new project directory with the following `main.go` file inside it:
    ```go
    package main

    import (
        "log"

        "ganigeorgiev.com/pocketbase/pocketbase"
        "ganigeorgiev.com/pocketbase/pocketbase/core"
    )

    func main() {
        app := pocketbase.New()

        app.OnServe().BindFunc(func(se *core.ServeEvent) error {
            // registers new "GET /hello" route
            se.Router.GET("/hello", func(re *core.RequestEvent) error {
                return re.String(200, "Hello world!")
            })

            return se.Next()
        })

        if err := app.Start(); err != nil {
            log.Fatal(err)
        }
    }
    ```

2. To init the dependencies, run `go mod init myapp && go mod tidy`.

3. To start the application, run `go run main.go serve`.

4. To build a statically linked executable, you can run `CGO_ENABLED=0 go build` and then start the created executable with `./myapp serve`.

_For more details please refer to [Extend with Go](https://pocketbase.io/docs/go-overview/)._

### Building and running the repo main.go example

To build the minimal standalone executable, like the prebuilt ones in the releases page, you can simply run `go build` inside the `examples/base` directory:

0. [Install Go 1.27+](https://go.dev/doc/install) (_if you haven't already_)
1. Clone/download the repo
2. Navigate to `examples/base`
3. Run `CGO_ENABLED=0 go build` to build a binary for your current environment
   _(or to target other platforms use `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build`; see https://go.dev/doc/install/source#environment)_
4. Start the created executable by running `./base serve`.

Note that the supported build targets by the pure Go SQLite driver at the moment are:

| GOOS    | GOARCH  |
|---------|---------|
| darwin  | amd64   |
| darwin  | arm64   |
| freebsd | 386     |
| freebsd | amd64   |
| freebsd | arm     |
| freebsd | arm64   |
| linux   | 386     |
| linux   | amd64   |
| linux   | arm     |
| linux   | arm64   |
| linux   | loong64 |
| linux   | ppc64le |
| linux   | riscv64 |
| linux   | s390x   |
| netbsd  | amd64   |
| openbsd | amd64   |
| openbsd | arm64   |
| windows | 386     |
| windows | amd64   |
| windows | arm64   |

### Testing

PocketBase comes with mixed bag of unit and integration tests.
To run them, use the standard `go test` command:

```sh
go test ./...
```

Check also the [Testing guide](http://pocketbase.io/docs/testing) to learn how to write your own custom application tests.


## Security

If you discover a security vulnerability within PocketBase, please submit a [private bug](https://ganigeorgiev.com/pocketbase/discussions/new/Bugs?private=1) OR send an e-mail to **support at pocketbase.io**.

You could find more details in the project [Security policy](https://ganigeorgiev.com/pocketbase/pocketbase/files/SECURITY.md).


## Contributing

PocketBase is free and open source project licensed under the [MIT License](https://ganigeorgiev.com/pocketbase/pocketbase/files/LICENSE.md).

You are free to do whatever you want with it, even offering it as a paid service
_(if you decide to do the latter, please make it clear that your service is not affiliated with the PocketBase project to avoid people wrongly submitting issues related to your service in the project's tracker)_.

You could help me by:

- [Report bugs and security issues](https://ganigeorgiev.com/pocketbase/discussions?category=Bugs)
- [Suggesting new features](https://ganigeorgiev.com/pocketbase/discussions?category=Feature+requests)
- [Help other people in the discussions](https://ganigeorgiev.com/pocketbase/discussions)
