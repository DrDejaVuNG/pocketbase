//go:build !no_default_driver

package core

import (
	"errors"
	"fmt"
	"net/url"

	"ganigeorgiev.com/pocketbase/dbx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

func DefaultDBConnect(dbPath string) (*dbx.DB, error) {
	// Note: the busy_timeout pragma must be first because
	// the connection needs to be set to block on busy before WAL mode
	// is set in case it hasn't been already set by another connection.
	pragmas := "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=journal_size_limit(200000000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=temp_store(MEMORY)&_pragma=cache_size(-32000)&_defensive=1"

	db, err := dbx.Open("sqlite", dbPath+pragmas)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Sample Connection String: "postgres://<username>:<password>@127.0.0.1:<port>"
//
// An invalid connection string doesn't panic but results in a DBConnectFunc
// that returns the validation error (without the connection string credentials).
func PostgresDBConnectFunc(connectionString string) DBConnectFunc {
	url, err := url.Parse(connectionString)
	/* PostgreSQL (removed):
	// panicked even for commands that don't connect (e.g. --help) and the
	// url.Error message contains the raw connection string with the password
	if err != nil {
		panic(fmt.Errorf("invalid connection string: %s", err))
	}
	if url.Scheme != "postgres" && url.Scheme != "postgresql" {
		panic(fmt.Errorf("invalid connection string scheme: [%s], must be [postgres] or [postgresql]", url.Scheme))
	}
	*/
	if err != nil {
		return failedDBConnect(errors.New("invalid Postgres connection string: malformed URL"))
	}
	if url.Scheme != "postgres" && url.Scheme != "postgresql" {
		return failedDBConnect(fmt.Errorf("invalid Postgres connection string scheme: [%s], must be [postgres] or [postgresql]", url.Scheme))
	}
	// Prefer no client-side statement cache. pgx's default
	// QueryExecModeCacheStatement caches a prepared statement's result
	// description per connection; when a schema change (e.g. a migration that
	// adds a column to an existing collection) alters a table the cache still
	// describes, the next execution of that statement fails with "cached plan
	// must not change result type" (SQLSTATE 0A000). QueryExecModeDescribeExec
	// re-prepares and re-describes on every execution, so DDL can never leave a
	// stale result description behind, while still sending typed parameters
	// over the extended protocol. Callers can override via
	// default_query_exec_mode on the connection string.
	if !url.Query().Has("default_query_exec_mode") {
		q := url.Query()
		q.Set("default_query_exec_mode", "describe_exec")
		url.RawQuery = q.Encode()
	}

	return func(dbName string) (*dbx.DB, error) {
		fmt.Println("Connecting to DB:", dbName)
		// clone url and replace the db name
		urlClone := *url
		urlClone.Path = dbName
		db, err := dbx.MustOpen("pgx", urlClone.String())
		/* SQLite:
		if err != nil && regexp.MustCompile(`database ".+" does not exist`).MatchString(err.Error()) {
		*/
		// PostgreSQL: detect the missing database by its SQLSTATE
		// (the error message text depends on the server lc_messages)
		if isPgErrorCode(err, pgCodeInvalidCatalogName) {
			fmt.Println("Database not found, creating:", dbName)
			if err := createDatabase(connectionString, dbName); err != nil {
				return nil, fmt.Errorf("Failed to create database [%s]: %s, please create it manually", dbName, err)
			}
			fmt.Println("Database created, reconnecting:", dbName)
			db, err = dbx.MustOpen("pgx", urlClone.String())
		}
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Postgres: %s", err)
		}

		return db, nil
	}
}

// failedDBConnect returns a DBConnectFunc that always fails with err.
func failedDBConnect(err error) DBConnectFunc {
	return func(dbName string) (*dbx.DB, error) {
		return nil, err
	}
}

const (
	pgCodeUniqueViolation    = "23505"
	pgCodeInvalidCatalogName = "3D000"
	pgCodeDuplicateDatabase  = "42P04"
)

// isPgErrorCode reports whether err wraps a PostgreSQL error with one of the specified SQLSTATE codes.
func isPgErrorCode(err error, codes ...string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}

	for _, code := range codes {
		if pgErr.Code == code {
			return true
		}
	}

	return false
}

// createDatabase creates the dbName database (if it doesn't exist already).
//
// Several nodes may start at the same time against a database that doesn't
// exist yet, so losing the CREATE DATABASE race to another node
// (42P04 or 23505 on pg_database_datname_index) is treated as success.
func createDatabase(connectionString string, dbName string) error {
	initDB, err := dbx.MustOpen("pgx", connectionString)
	if err != nil {
		return err
	}
	defer initDB.Close()

	_, err = initDB.NewQuery("CREATE DATABASE " + pgx.Identifier{dbName}.Sanitize()).Execute()
	if err != nil && !isPgErrorCode(err, pgCodeDuplicateDatabase, pgCodeUniqueViolation) {
		return err
	}

	return nil
}
