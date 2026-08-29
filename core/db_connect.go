//go:build !no_default_driver

package core

import (
	"fmt"
	"net/url"
	"regexp"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pocketbase/dbx"
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
func PostgresDBConnectFunc(connectionString string) DBConnectFunc {
	url, err := url.Parse(connectionString)
	if err != nil {
		panic(fmt.Errorf("invalid connection string: %s", err))
	}
	if url.Scheme != "postgres" && url.Scheme != "postgresql" {
		panic(fmt.Errorf("invalid connection string scheme: [%s], must be [postgres] or [postgresql]", url.Scheme))
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
		if err != nil && regexp.MustCompile(`database ".+" does not exist`).MatchString(err.Error()) {
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

func createDatabase(connectionString string, dbName string) error {
	initDB, err := dbx.MustOpen("pgx", connectionString)
	if err != nil {
		return err
	}
	_, err = initDB.NewQuery(fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)).Execute()
	if err != nil {
		return err
	}
	return nil
}
