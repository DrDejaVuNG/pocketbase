# prodtest — multi-node production scenarios

A PocketBase backend built on this PostgreSQL fork plus a scenario suite that
runs it the way production does: several nodes on one PostgreSQL, behind a load
balancer without sticky sessions.

## The backend (`main.go`, `routes.go`, `pbmigrations/`)

A small multi-tenant app, created by a regular app migration on first start:

| Collection | Purpose |
|---|---|
| `orgs`, `users` (auth, OTP on) | tenants; users belong to one org (`org`, `role`) |
| `posts` | rule-scoped to the user's org; text/editor/select/multi-select/number/bool/json/date/relation/autodate fields, partial unique index on `slug` |
| `comments` | rule joins `post.org`; cascade delete with `posts` |
| `big_docs` | large records (NOTIFY payload limit) |
| `accounts`, `counters` | superuser-only; transactional read-modify-write routes |
| `mail_outbox` | captured OTP mails (no SMTP needed) |
| `post_stats` | view collection (aggregate per org) |

Custom routes: `GET /api/prodtest/node` (node identity), and superuser-only
`POST /api/prodtest/counters/{name}/incr`, `POST /api/prodtest/transfer`
(both `RunInTransaction` read-modify-write), `POST /api/prodtest/collections/{name}/rules`
(changes rules with `app.Save`, like hooks/cron/CLI code), `GET /api/prodtest/dbstats`
(connection pool statistics of the node). `PRODTEST_PPROF_ADDR` starts a pprof
listener on that address (load investigations only, never public).

Run it on its own:

```sh
POSTGRES_URL='postgres://USER:PASSWORD@127.0.0.1:5432/postgres?sslmode=disable' \
  go run ./examples/prodtest serve
```

## The scenarios (`*_test.go`, build tag `prodtest`)

```sh
cd examples/prodtest
go test -tags prodtest -count=1 -v -timeout 30m .                      # everything (~10 min)
go test -tags prodtest -count=1 -v -run 'TestRealtime/LargeRecord' .   # one scenario
go test -tags prodtest -count=1 -v -short .                            # skip the ~90s pause/crash scenarios
```

The suite builds the backend, starts a throwaway `postgres:18-alpine` container
(random localhost port, a regular non-superuser app role like on managed
PostgreSQL), and starts 1-3 nodes per cluster. A shared 3-node cluster serves
the non-destructive groups; scenarios that stop nodes or disturb PostgreSQL get
their own cluster.

| Group | Covers |
|---|---|
| `TestStartup` | concurrent cold start on missing databases; rolling restart under write load |
| `TestRealtime` | SSE delivery across nodes, subscribe through another node, rules and hidden fields on remote nodes, no secrets in NOTIFY payloads, `\|\|` filters, records over 8 KB, auth changes, disconnect cleanup, fan-out burst |
| `TestPropagation` | collection/rule/settings changes (Web API and `app.Save`) reaching every node; schema change window |
| `TestSchemaChange` | tables changed while every node serves (psql `ALTER TABLE`, collection changes through the API): no failed lists, views, creates, batches or transactions with the default statement cache |
| `TestConcurrency` | unique index collisions, duplicate signup, transactional counters and transfers, OTP single use, batch atomicity, cascade delete vs create |
| `TestQuery` | filter/sort parity with SQLite PocketBase on production column types, pagination/expand/fields, view collection, numeric and `:each` rules, `geoDistance`, multi-row inserts with default ids |
| `TestResilience` | LISTEN connections killed, PostgreSQL restart, paused node rejoining, crashed node cleanup, graceful shutdown cleanup |
| `TestCapacity` | default pools against a 97-connection budget, latency profile, pgx query exec mode cost (default `cache_describe` vs `describe_exec` vs `cache_statement`) |
| `TestPerf` | throughput benchmark, runs only with `PRODTEST_PERF=1` (see below) |

Environment:

| Variable | Effect |
|---|---|
| `PRODTEST_POSTGRES_URL` | use an existing server (admin URL) instead of Docker; destructive scenarios (restart, role limits) are skipped |
| `PRODTEST_POSTGRES_IMAGE` | container image (default `postgres:18-alpine`) |
| `PRODTEST_KEEP` | keep node logs and `postgres.log` (printed path) even when all scenarios pass |
| `PRODTEST_BIN` | run against a prebuilt backend binary instead of building this directory (compare two builds of the fork) |
| `PRODTEST_POSTGRES_DURABLE` | keep PostgreSQL's default WAL durability (the container otherwise runs with `fsync=off`); use it for write throughput numbers |
| `PRODTEST_PERF` | run `TestPerf` |

Node logs are kept in a temp directory when a scenario fails.

## Throughput benchmark (`perf_test.go`)

```sh
PRODTEST_PERF=1 PRODTEST_POSTGRES_DURABLE=1 go test -tags prodtest -count=1 -run 'TestPerf$' -v .
```

A dedicated 3-node cluster with 20k posts and 20k comments in one org, 48
clients, 6 s per workload. One `PERF` line per workload: req/s, p50/p99,
errors, SQL statements and DB time per request (`pg_stat_statements`), commits,
advisory lock waiters, new PostgreSQL sessions and pool waits. Knobs
(`PERF_NODE_ENV`, `PERF_WORKERS`, `PERF_WORKLOADS`, `PERF_EXEC_MODE`,
`PERF_SETUP_SQL`, `PERF_TOP`, `PERF_PPROF`, ...) are listed in `perf_test.go`.
Throughput varies by ±25% between runs on a shared machine: compare builds with
interleaved runs (`PRODTEST_BIN`) and prefer the per-request DB time.
