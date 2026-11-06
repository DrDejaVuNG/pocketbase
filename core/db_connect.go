//go:build !no_default_driver

package core

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"

	"ganigeorgiev.com/pocketbase/dbx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
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
// An empty or invalid connection string doesn't panic but results in a DBConnectFunc
// that returns the validation error (without the connection string credentials).
func PostgresDBConnectFunc(connectionString string) DBConnectFunc {
	if connectionString == "" {
		return failedDBConnect(errors.New("missing Postgres connection string: set the POSTGRES_URL env variable, the --postgresUrl flag or BaseAppConfig.PostgresURL"))
	}

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
	// PostgreSQL: pgx's QueryExecModeCacheDescribe keeps the statement
	// descriptions per connection, so after its first execution a statement
	// takes a single round trip (QueryExecModeDescribeExec, the previous
	// default, describes every statement first: about 20-30% more CPU per
	// cheap request, measured 2026-11-06). The descriptions that a schema
	// change makes stale are handled by statementCacheConn (see
	// statementCacheGeneration). Callers can override the mode via
	// default_query_exec_mode on the connection string.
	if !url.Query().Has("default_query_exec_mode") {
		q := url.Query()
		q.Set("default_query_exec_mode", "cache_describe")
		url.RawQuery = q.Encode()
	}

	return func(dbName string) (*dbx.DB, error) {
		fmt.Println("Connecting to DB:", dbName)
		// clone url and replace the db name
		urlClone := *url
		urlClone.Path = dbName
		db, err := openPostgres(urlClone.String())
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
			db, err = openPostgres(urlClone.String())
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
	pgCodeUniqueViolation     = "23505"
	pgCodeInvalidCatalogName  = "3D000"
	pgCodeDuplicateDatabase   = "42P04"
	pgCodeFeatureNotSupported = "0A000" // eg. "cached plan must not change result type"
	pgCodeProtocolViolation   = "08P01" // eg. "bind message has N result formats but query has M columns"
)

// openPostgres opens and pings a pgx database/sql pool.
//
// With a caching query exec mode the connections keep their statement
// caches consistent with the schema (see statementCacheGeneration).
func openPostgres(connectionString string) (*dbx.DB, error) {
	config, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}

	var connector driver.Connector
	switch config.DefaultQueryExecMode {
	case pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeCacheStatement:
		connector = statementCacheConnector{stdlib.GetConnector(*config,
			stdlib.OptionAfterConnect(initStatementCacheGeneration),
			stdlib.OptionResetSession(resetStaleStatementCache),
		)}
	default:
		connector = stdlib.GetConnector(*config)
	}

	sqlDB := sql.OpenDB(connector)
	if err := sqlDB.Ping(); err != nil {
		sqlDB.Close()
		return nil, err
	}

	return dbx.NewFromDB(sqlDB, "pgx"), nil
}

// statementCacheGenerationKey is the pgconn custom data key of the
// statementCacheGeneration that the connection's statement cache matches
// (missing = the cache must be cleared before the next use).
const statementCacheGenerationKey = "pb.statementCacheGeneration"

func initStatementCacheGeneration(_ context.Context, conn *pgx.Conn) error {
	conn.PgConn().CustomData()[statementCacheGenerationKey] = statementCacheGeneration.Load()
	return nil
}

// resetStaleStatementCache clears the statement cache of a pooled connection
// before it is used again if a schema change happened since it was cached.
func resetStaleStatementCache(ctx context.Context, conn *pgx.Conn) error {
	generation := statementCacheGeneration.Load()
	data := conn.PgConn().CustomData()
	if cached, ok := data[statementCacheGenerationKey].(uint64); ok && cached == generation {
		return nil
	}
	if err := conn.DeallocateAll(ctx); err != nil {
		return driver.ErrBadConn // discard the connection
	}
	data[statementCacheGenerationKey] = generation
	return nil
}

// statementCacheConnector creates the connections of a pool that uses a
// caching query exec mode.
type statementCacheConnector struct {
	driver.Connector
}

func (c statementCacheConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	stdlibConn, ok := conn.(*stdlib.Conn)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("unexpected pgx database/sql connection type %T", conn)
	}

	return &statementCacheConn{pgxStdlibConn: stdlibConn}, nil
}

// pgxStdlibConn gives the embedded connection field a name that doesn't
// hide its Conn() method (the field of an embedded *stdlib.Conn is named Conn).
type pgxStdlibConn = stdlib.Conn

// statementCacheConn is a pgx database/sql connection that keeps its
// statement cache consistent with the schema (see statementCacheGeneration).
//
// The embedded *stdlib.Conn provides the other driver interfaces
// and Conn() for the raw pgx connection (eg. the realtime bridge LISTEN).
type statementCacheConn struct {
	*pgxStdlibConn

	// schemaChanged is set when a schema change ran in the current transaction
	schemaChanged bool
}

// the driver interfaces that database/sql and the realtime bridge use
var (
	_ driver.QueryerContext         = (*statementCacheConn)(nil)
	_ driver.ExecerContext          = (*statementCacheConn)(nil)
	_ driver.ConnBeginTx            = (*statementCacheConn)(nil)
	_ driver.ConnPrepareContext     = (*statementCacheConn)(nil)
	_ driver.SessionResetter        = (*statementCacheConn)(nil)
	_ driver.Pinger                 = (*statementCacheConn)(nil)
	_ driver.NamedValueChecker      = (*statementCacheConn)(nil)
	_ interface{ Conn() *pgx.Conn } = (*statementCacheConn)(nil)
)

func (c *statementCacheConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.pgxStdlibConn.QueryContext(ctx, query, args)
	if err != nil && c.handleStaleStatement(ctx, err) {
		rows, err = c.pgxStdlibConn.QueryContext(ctx, query, args)
	}
	return rows, err
}

func (c *statementCacheConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.pgxStdlibConn.ExecContext(ctx, query, args)
	if err != nil && c.handleStaleStatement(ctx, err) {
		result, err = c.pgxStdlibConn.ExecContext(ctx, query, args)
	}
	if err != nil {
		return result, err
	}

	switch {
	case isSchemaChange(query):
		// the next statements on this connection must see the new schema,
		// also in the same transaction (eg. a migration that reads a table
		// before and after changing it)
		c.clearCache(ctx)
		if c.inTransaction() {
			c.schemaChanged = true // the other connections are reset when it ends
		} else {
			ResetStatementCaches()
		}
	case c.schemaChanged && isRollbackStatement(query):
		// a rollback to a savepoint can undo the transaction's schema change
		c.clearCache(ctx)
	}

	return result, nil
}

func (c *statementCacheConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.pgxStdlibConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &statementCacheTx{Tx: tx, conn: c}, nil
}

// handleStaleStatement reports whether a statement that failed with err can
// run once more.
//
// The errors of a statement description cached before a schema change made by
// another connection (matched by SQLSTATE because the message text depends on
// the server lc_messages) clear the connection's cache and make the other
// connections clear theirs. Outside a transaction the failed statement had no
// effect, so it can run again with a new description.
func (c *statementCacheConn) handleStaleStatement(ctx context.Context, err error) bool {
	if !isPgErrorCode(err, pgCodeFeatureNotSupported, pgCodeProtocolViolation) {
		return false
	}

	ResetStatementCaches()

	if c.inTransaction() {
		// the failed transaction accepts only a rollback;
		// the cache is cleared before the connection is used again
		delete(c.Conn().PgConn().CustomData(), statementCacheGenerationKey)
		return false
	}

	return c.clearCache(ctx)
}

// clearCache clears the statement cache of the connection and reports
// whether it succeeded (on failure it is cleared before the next use).
func (c *statementCacheConn) clearCache(ctx context.Context) bool {
	if err := c.Conn().DeallocateAll(ctx); err != nil {
		delete(c.Conn().PgConn().CustomData(), statementCacheGenerationKey)
		return false
	}
	return true
}

func (c *statementCacheConn) inTransaction() bool {
	return c.Conn().PgConn().TxStatus() != 'I'
}

// statementCacheTx makes the other connections clear their statement caches
// when a transaction with a schema change ends (also on rollback because the
// connection itself cached statements of the rolled back schema).
type statementCacheTx struct {
	driver.Tx
	conn *statementCacheConn
}

func (t *statementCacheTx) Commit() error {
	defer t.end()
	return t.Tx.Commit()
}

func (t *statementCacheTx) Rollback() error {
	defer t.end()
	return t.Tx.Rollback()
}

func (t *statementCacheTx) end() {
	if t.conn.schemaChanged {
		t.conn.schemaChanged = false
		ResetStatementCaches()
	}
}

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
