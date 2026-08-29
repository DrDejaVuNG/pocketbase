# PocketBase with PostgreSQL — Project Overview

PocketBase with PostgreSQL is a production-grade fork of [PocketBase](https://pocketbase.io) that replaces the embedded SQLite database with native **PostgreSQL (v17+)** support, horizontal scaling across multiple nodes, and distributed realtime events via PostgreSQL `LISTEN/NOTIFY`.

The codebase is kept synchronized with upstream `pocketbase/pocketbase` releases (currently tracking **v0.40.1**).

### Versioning Strategy
- Releases tracking upstream releases directly: `v<upstream_version>` (e.g. `v0.40.1`).
- Fork-specific PostgreSQL bug fixes and patches: `v<upstream_version>-hotfix<N>` (e.g. `v0.40.1-hotfix1`).
  This avoids colliding with future official PocketBase releases while keeping upstream lineage explicit.
---

## Tech Stack

| Component | Technology | Description |
|---|---|---|
| **Language & Runtime** | Go 1.27+ (`encoding/json/v2`) | Single portable binary backend framework |
| **Primary Database** | PostgreSQL 17+ | Drop-in replacement for SQLite via `pgx/v5` and `DrDejaVuNG/dbx` |
| **DB Abstraction** | `github.com/DrDejaVuNG/dbx` | Fork of `pocketbase/dbx` with Postgres dialect & text-PK `RETURNING` support |
| **Realtime Scaling** | PostgreSQL `LISTEN/NOTIFY` | Horizontal event broadcasting across distributed instances (`apis/realtime_bridge.go`) |
| **Admin Dashboard** | Svelte 5 + Vite | Prebuilt embedded UI in `ui/dist` with dark mode, SQL console, and theming |
| **JS VM Engine** | `goja` | Embedded ECMAScript 5.1+ runtime for `pb_hooks` scripting |

---

## Architecture & PostgreSQL Integration

### 1. Database Connection & Lifecycle
- Managed via `core/db_connect.go` and `core/base.go`.
- Configured with `POSTGRES_URL` (e.g. `postgres://user:pass@127.0.0.1:5432/postgres?sslmode=disable`).
- Automatically creates `pb-data` and `pb-auxiliary` databases on initial startup if they do not already exist.
- Connection pooling is shared across concurrent and nonconcurrent operations since PostgreSQL natively supports concurrent writes.

### 2. DDL & Schema Synchronization
- Handled through `core/db_table.go` and `core/collection_record_table_sync.go`.
- Table metadata and column information query PostgreSQL `information_schema.columns` and `information_schema.table_constraints`.
- Index introspection queries `pg_indexes` and `pg_constraint` (excluding primary key constraints).
- View dependencies are tracked via `findDependentViews` in `core/view.go` to safely drop and recreate dependent views in topological order when schema changes occur.

### 3. SQLite-Equivalent Functions & Collation
Installed automatically in `migrations/postgres_functions.go` on startup:
- `hex(data bytea)` -> `encode(data, 'hex')`
- `randomblob(length integer)` -> `gen_random_bytes(length)` via `pgcrypto`
- `uuid_generate_v7()` -> Version 7 UUID generator using microsecond timestamp + random entropy
- `json_valid(text)` -> JSON validation helper returning boolean
- `json_query_or_null(anyelement, text)` -> Safe JSON path query with null fallback
- `total(anyelement)` -> Polymorphic aggregate matching SQLite's `total()` (returns `0.0` for empty inputs)
- `strftime(format, [timevalue, modifiers...])` -> Full SQLite-compatible date/time formatting function with modifiers (`start of month/year`, `±N years/months/days`, `weekday N`, `unixepoch`, etc.)
- `"nocase"` ICU collation -> Undetermined case-insensitive collation matching SQLite's default behavior

### 4. Realtime Scaling Bridge
- Implemented in `apis/realtime_bridge.go` and `apis/realtime_bridgedclient.go`.
- Instances listen on PostgreSQL channels using `LISTEN` / `NOTIFY`.
- Record creates, updates, and deletes are broadcasted across all nodes so connected SSE clients receive instant updates regardless of which instance processed the HTTP write.

---

## Key Differences from Upstream SQLite

1. **Backups**: The built-in SQLite `VACUUM INTO` backup command is guarded with a clean error message. Use `pg_dump` / `pg_dumpall` or containerized backup tools for PostgreSQL backups.
2. **Environment Variables**:
   - `POSTGRES_URL` (required) — PostgreSQL connection URL.
   - `POSTGRES_DATA_DB` (default: `pb-data`) — Main data database name.
   - `POSTGRES_AUX_DB` (default: `pb-auxiliary`) — Auxiliary/logs database name.
   - `PB_REALTIME_BRIDGE` (default: `true`) — Toggle distributed realtime bridge.
   - `PB_PATH_PREFIX` — Optional URL path prefix for multi-site reverse proxy setups.

---

## Project Structure

```
pocketbase/
├── apis/                    # HTTP REST and SSE endpoints
│   ├── realtime_bridge.go   # PostgreSQL LISTEN/NOTIFY scaling bridge
│   └── sql.go               # Admin SQL console endpoint
├── cmd/                     # CLI commands (superuser, migrate, serve)
├── core/                    # Core PocketBase application runtime
│   ├── db_connect.go        # PostgreSQL connection bootstrap
│   ├── db_table.go          # Schema introspection via information_schema
│   ├── view.go              # View validation and dependency resolution
│   └── notify_watcher.go    # Local process state synchronization
├── migrations/              # System and database migration scripts
│   └── postgres_functions.go# SQLite-compatibility SQL functions for Postgres
├── tests/                   # Test helpers and mock applications
│   ├── data/                # Seed dumps (data.pg-dump.sql, auxiliary.pg-dump.sql)
│   └── dynamic_stubs.go     # Dynamic test fixture generators
├── tools/                   # Utility packages
│   ├── dbutils/             # Schema/index parsing and json helpers
│   └── search/              # Filter, sort, and token evaluation
└── ui/                      # Dashboard Admin UI (Svelte 5 + Vite)
    └── dist/                # Precompiled static assets embedded in binary
```

---

## Testing & CI

- Unit and integration tests run against PostgreSQL 17/18 via `go test ./...`.
- 100% test pass rate maintained across all 36 Go packages in the repository.
- GitHub Actions CI workflow in `.github/workflows/release.yaml` provisions a native PostgreSQL service container for automated testing.
