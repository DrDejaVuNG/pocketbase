package core_test

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"testing"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tests"
	"github.com/jackc/pgx/v5"
)

// The default query exec mode (cache_describe) caches the statement
// descriptions per pooled connection; a table change must never make the
// cached statements fail.

const stmtCacheSelect = `SELECT * FROM stmt_cache_test WHERE id = $1`

func newStmtCacheTable(t *testing.T, app *tests.TestApp) *sql.DB {
	t.Helper()

	if _, err := app.DB().NewQuery(`CREATE TABLE stmt_cache_test (id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '')`).Execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := app.DB().NewQuery(`INSERT INTO stmt_cache_test (id, name) VALUES ('a', 'A')`).Execute(); err != nil {
		t.Fatal(err)
	}

	return app.NonconcurrentDB().(*dbx.DB).DB()
}

// alterFromOtherConnection changes the table through a connection outside
// the app pools (like another instance or psql would).
func alterFromOtherConnection(t *testing.T, app *tests.TestApp, query string) {
	t.Helper()

	u, err := url.Parse(tests.PostgresURL)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = app.PostgresDataDB()

	conn, err := pgx.Connect(context.Background(), u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(context.Background(), query); err != nil {
		t.Fatal(err)
	}
}

// withConns checks out n pool connections at the same time
// (the same ones on every call while the pool has n idle connections)
// and runs fn on each.
func withConns(t *testing.T, db *sql.DB, n int, fn func(conn *sql.Conn) error) {
	t.Helper()

	conns := make([]*sql.Conn, n)
	for i := range conns {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conns[i] = conn
	}

	for i, conn := range conns {
		if err := fn(conn); err != nil {
			t.Fatalf("connection %d: %v", i, err)
		}
	}
}

// selectColumns runs stmtCacheSelect and returns the number of its columns.
func selectColumns(q interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}) (int, error) {
	rows, err := q.QueryContext(context.Background(), stmtCacheSelect, "a")
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	for rows.Next() {
	}
	return len(columns), rows.Err()
}

// updateReturning runs an UPDATE with a result through ExecContext.
func updateReturning(conn *sql.Conn) error {
	_, err := conn.ExecContext(context.Background(), `UPDATE stmt_cache_test SET name = $1 WHERE id = $2 RETURNING *`, "B", "a")
	return err
}

func TestStatementCacheTableChangedByOtherConnection(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	db := newStmtCacheTable(t, app)

	const conns = 5
	withConns(t, db, conns, func(conn *sql.Conn) error {
		if _, err := selectColumns(conn); err != nil {
			return err
		}
		return updateReturning(conn)
	})

	alterFromOtherConnection(t, app, `ALTER TABLE stmt_cache_test ADD COLUMN extra TEXT NOT NULL DEFAULT ''`)

	// statements outside a transaction run again after the stale error
	withConns(t, db, conns, func(conn *sql.Conn) error {
		n, err := selectColumns(conn)
		if err != nil {
			return err
		}
		if n != 3 {
			return errors.New("expected the new column in the result")
		}
		return updateReturning(conn)
	})

	alterFromOtherConnection(t, app, `ALTER TABLE stmt_cache_test DROP COLUMN extra`)

	// after a reported schema change (eg. from the realtime bridge)
	// the transactions don't fail either
	core.ResetStatementCaches()
	withConns(t, db, conns, func(conn *sql.Conn) error {
		tx, err := conn.BeginTx(context.Background(), nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if n, err := selectColumns(tx); err != nil || n != 2 {
			return errors.Join(err, errors.New("expected the 2 table columns"))
		}
		return tx.Commit()
	})
}

func TestStatementCacheSchemaChangeInTransaction(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	db := newStmtCacheTable(t, app)

	const conns = 5
	withConns(t, db, conns, func(conn *sql.Conn) error {
		_, err := selectColumns(conn)
		return err
	})

	withConns(t, db, 1, func(conn *sql.Conn) error {
		tx, err := conn.BeginTx(context.Background(), nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// a migration that reads a table before and after changing it
		if _, err := selectColumns(tx); err != nil {
			return err
		}
		if _, err := tx.Exec(`ALTER TABLE stmt_cache_test ADD COLUMN extra TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
		if n, err := selectColumns(tx); err != nil || n != 3 {
			return errors.Join(err, errors.New("expected the new column after ALTER"))
		}

		// a rolled back savepoint undoes the schema change
		for _, q := range []string{`SAVEPOINT sp1`, `ALTER TABLE stmt_cache_test ADD COLUMN extra2 TEXT`} {
			if _, err := tx.Exec(q); err != nil {
				return err
			}
		}
		if n, err := selectColumns(tx); err != nil || n != 4 {
			return errors.Join(err, errors.New("expected 4 columns in the savepoint"))
		}
		if _, err := tx.Exec(`ROLLBACK TO SAVEPOINT sp1`); err != nil {
			return err
		}
		if n, err := selectColumns(tx); err != nil || n != 3 {
			return errors.Join(err, errors.New("expected 3 columns after the savepoint rollback"))
		}

		return tx.Commit()
	})

	// the other connections cached the statement before the committed change
	withConns(t, db, conns, func(conn *sql.Conn) error {
		tx, err := conn.BeginTx(context.Background(), nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if n, err := selectColumns(tx); err != nil || n != 3 {
			return errors.Join(err, errors.New("expected 3 columns in the other connections"))
		}
		return tx.Commit()
	})
}

func TestStatementCacheCollectionChangeInTransaction(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection := core.NewBaseCollection("stmt_cache_records")
	collection.Fields.Add(&core.TextField{Name: "title"})
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}
	record := core.NewRecord(collection)
	record.Set("title", "a")
	if err := app.Save(record); err != nil {
		t.Fatal(err)
	}

	err := app.RunInTransaction(func(txApp core.App) error {
		if _, err := txApp.FindRecordById(collection.Id, record.Id); err != nil {
			return err
		}

		// a nested change that is rolled back
		nestedErr := txApp.RunInTransaction(func(nestedApp core.App) error {
			c, err := nestedApp.FindCollectionByNameOrId(collection.Id)
			if err != nil {
				return err
			}
			c.Fields.Add(&core.TextField{Name: "discarded"})
			if err := nestedApp.Save(c); err != nil {
				return err
			}
			if _, err := nestedApp.FindRecordById(collection.Id, record.Id); err != nil {
				return err
			}
			return errors.New("rollback")
		})
		if nestedErr == nil || nestedErr.Error() != "rollback" {
			return errors.Join(errors.New("expected the nested rollback error"), nestedErr)
		}

		c, err := txApp.FindCollectionByNameOrId(collection.Id)
		if err != nil {
			return err
		}
		c.Fields.Add(&core.TextField{Name: "subtitle"})
		if err := txApp.Save(c); err != nil {
			return err
		}

		found, err := txApp.FindRecordById(collection.Id, record.Id)
		if err != nil {
			return err
		}
		if _, ok := found.FieldsData()["subtitle"]; !ok {
			return errors.New("expected the new field in the record")
		}
		if _, ok := found.FieldsData()["discarded"]; ok {
			return errors.New("expected no rolled back field in the record")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
