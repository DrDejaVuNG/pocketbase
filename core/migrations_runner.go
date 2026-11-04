package core

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/tools/osutils"
	"github.com/spf13/cast"
)

var AppMigrations MigrationsList
var SystemMigrations MigrationsList

const DefaultMigrationsTable = "_migrations"

// MigrationsRunner defines a simple struct for managing the execution of db migrations.
type MigrationsRunner struct {
	app            App
	tableName      string
	migrationsList MigrationsList
	inited         bool

	// PostgreSQL: lockHeld indicates that the caller already holds
	// the cross-node migrations lock (see withMigrationsLock).
	lockHeld bool
}

// NewMigrationsRunner creates and initializes a new db migrations MigrationsRunner instance.
func NewMigrationsRunner(app App, migrationsList MigrationsList) *MigrationsRunner {
	return &MigrationsRunner{
		app:            app,
		migrationsList: migrationsList,
		tableName:      DefaultMigrationsTable,
	}
}

// Run interactively executes the current runner with the provided args.
//
// The following commands are supported:
// - up           - applies all migrations
// - down [n]     - reverts the last n (default 1) applied migrations
// - history-sync - syncs the migrations table with the runner's migrations list
func (r *MigrationsRunner) Run(args ...string) error {
	if err := r.withLock(r.initMigrationsTable); err != nil {
		return err
	}

	cmd := "up"
	if len(args) > 0 {
		cmd = args[0]
	}

	switch cmd {
	case "up":
		applied, err := r.Up()
		if err != nil {
			return err
		}

		if len(applied) == 0 {
			color.Green("No new migrations to apply.")
		} else {
			for _, file := range applied {
				color.Green("Applied %s", file)
			}
		}

		return nil
	case "down":
		toRevertCount := 1
		if len(args) > 1 {
			toRevertCount = cast.ToInt(args[1])
			if toRevertCount < 0 {
				// revert all applied migrations
				toRevertCount = len(r.migrationsList.Items())
			}
		}

		names, err := r.lastAppliedMigrations(toRevertCount)
		if err != nil {
			return err
		}

		confirm := osutils.YesNoPrompt(fmt.Sprintf(
			"\n%v\nDo you really want to revert the last %d applied migration(s)?",
			strings.Join(names, "\n"),
			toRevertCount,
		), false)
		if !confirm {
			fmt.Println("The command has been cancelled")
			return nil
		}

		reverted, err := r.Down(toRevertCount)
		if err != nil {
			return err
		}

		if len(reverted) == 0 {
			color.Green("No migrations to revert.")
		} else {
			for _, file := range reverted {
				color.Green("Reverted %s", file)
			}
		}

		return nil
	case "history-sync":
		if err := r.RemoveMissingAppliedMigrations(); err != nil {
			return err
		}

		color.Green("The %s table was synced with the available migrations.", r.tableName)
		return nil
	default:
		return fmt.Errorf("unsupported command: %q", cmd)
	}
}

// Up executes all unapplied migrations for the provided runner.
//
// On success returns list with the applied migrations file names.
func (r *MigrationsRunner) Up() ([]string, error) {
	var applied []string

	// PostgreSQL: several nodes may run the same migrations at the same time
	// (e.g. on first boot or when deploying new migrations) so the applied
	// migrations are checked and executed while holding the migrations lock
	err := r.withLock(func() error {
		var err error
		applied, err = r.up()
		return err
	})
	if err != nil {
		return nil, err
	}

	return applied, nil
}

func (r *MigrationsRunner) up() ([]string, error) {
	if err := r.initMigrationsTable(); err != nil {
		return nil, err
	}

	applied := []string{}

	err := r.app.AuxRunInTransaction(func(txApp App) error {
		return txApp.RunInTransaction(func(txApp App) error {
			for _, m := range r.migrationsList.Items() {
				// applied migrations check
				if r.isMigrationApplied(txApp, m.File) {
					if m.ReapplyCondition == nil {
						continue // no need to reapply
					}

					shouldReapply, err := m.ReapplyCondition(txApp, r, m.File)
					if err != nil {
						return err
					}
					if !shouldReapply {
						continue
					}

					// clear previous history stored entry
					// (it will be recreated after successful execution)
					r.saveRevertedMigration(txApp, m.File)
				}

				// ignore empty Up action
				if m.Up != nil {
					if err := m.Up(txApp); err != nil {
						return fmt.Errorf("failed to apply migration %s: %w", m.File, err)
					}
				}

				if err := r.saveAppliedMigration(txApp, m.File); err != nil {
					return fmt.Errorf("failed to save applied migration info for %s: %w", m.File, err)
				}

				applied = append(applied, m.File)
			}

			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return applied, nil
}

// Down reverts the last `toRevertCount` applied migrations
// (in the order they were applied).
//
// On success returns list with the reverted migrations file names.
func (r *MigrationsRunner) Down(toRevertCount int) ([]string, error) {
	var reverted []string

	// PostgreSQL: see Up()
	err := r.withLock(func() error {
		var err error
		reverted, err = r.down(toRevertCount)
		return err
	})
	if err != nil {
		return nil, err
	}

	return reverted, nil
}

func (r *MigrationsRunner) down(toRevertCount int) ([]string, error) {
	if err := r.initMigrationsTable(); err != nil {
		return nil, err
	}

	reverted := make([]string, 0, toRevertCount)

	names, appliedErr := r.lastAppliedMigrations(toRevertCount)
	if appliedErr != nil {
		return nil, appliedErr
	}

	err := r.app.AuxRunInTransaction(func(txApp App) error {
		return txApp.RunInTransaction(func(txApp App) error {
			for _, name := range names {
				for _, m := range r.migrationsList.Items() {
					if m.File != name {
						continue
					}

					// revert limit reached
					if toRevertCount-len(reverted) <= 0 {
						return nil
					}

					// ignore empty Down action
					if m.Down != nil {
						if err := m.Down(txApp); err != nil {
							return fmt.Errorf("failed to revert migration %s: %w", m.File, err)
						}
					}

					if err := r.saveRevertedMigration(txApp, m.File); err != nil {
						return fmt.Errorf("failed to save reverted migration info for %s: %w", m.File, err)
					}

					reverted = append(reverted, m.File)
				}
			}
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	return reverted, nil
}

// RemoveMissingAppliedMigrations removes the db entries of all applied migrations
// that are not listed in the runner's migrations list.
func (r *MigrationsRunner) RemoveMissingAppliedMigrations() error {
	loadedMigrations := r.migrationsList.Items()

	names := make([]any, len(loadedMigrations))
	for i, migration := range loadedMigrations {
		names[i] = migration.File
	}

	_, err := r.app.DB().Delete(r.tableName, dbx.Not(dbx.HashExp{
		"file": names,
	})).Execute()

	return err
}

func (r *MigrationsRunner) initMigrationsTable() error {
	if r.inited {
		return nil // already inited
	}

	/* SQLite:
	rawQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS {{%s}} (file VARCHAR(255) PRIMARY KEY NOT NULL, applied INTEGER NOT NULL)",
		r.tableName,
	)
	*/
	// PostgreSQL:
	// the column `applied` is a int64 so we have to use BIGINT
	rawQuery := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS {{%s}} (file VARCHAR(255) PRIMARY KEY NOT NULL, applied BIGINT NOT NULL)`,
		r.tableName,
	)

	_, err := r.app.DB().NewQuery(rawQuery).Execute()

	if err == nil {
		r.inited = true
	}

	return err
}

func (r *MigrationsRunner) isMigrationApplied(txApp App, file string) bool {
	var exists int

	err := txApp.DB().Select("(1)").
		From(r.tableName).
		Where(dbx.HashExp{"file": file}).
		Limit(1).
		Row(&exists)

	return err == nil && exists > 0
}

func (r *MigrationsRunner) saveAppliedMigration(txApp App, file string) error {
	_, err := txApp.DB().Insert(r.tableName, dbx.Params{
		"file":    file,
		"applied": time.Now().UnixMicro(),
	}).Execute()

	return err
}

func (r *MigrationsRunner) saveRevertedMigration(txApp App, file string) error {
	_, err := txApp.DB().Delete(r.tableName, dbx.HashExp{"file": file}).Execute()

	return err
}

func (r *MigrationsRunner) lastAppliedMigrations(limit int) ([]string, error) {
	var files = make([]string, 0, limit)

	loadedMigrations := r.migrationsList.Items()

	names := make([]any, len(loadedMigrations))
	for i, migration := range loadedMigrations {
		names[i] = migration.File
	}

	err := r.app.DB().Select("file").
		From(r.tableName).
		Where(dbx.Not(dbx.HashExp{"applied": nil})).
		AndWhere(dbx.HashExp{"file": names}).
		// unify microseconds and seconds applied time for backward compatibility
		OrderBy("substr(applied||'0000000000000000', 0, 17) DESC").
		AndOrderBy("file DESC").
		Limit(int64(limit)).
		Column(&files)

	if err != nil {
		return nil, err
	}

	return files, nil
}

// PostgreSQL:
// Session level advisory lock keys serializing the migrations (and the
// initial settings insert, see BaseApp.Bootstrap) across all nodes sharing
// the same databases.
//
// The data and auxiliary keys are distinct so that the two locks don't block
// each other when both names point to the same physical database. They are
// also distinct from the transaction level keys used by the system migrations.
const (
	migrationsLockKeyData int64 = 723390692
	migrationsLockKeyAux  int64 = 723390693
)

func (r *MigrationsRunner) withLock(fn func() error) error {
	if r.lockHeld {
		return fn()
	}

	return withMigrationsLock(r.app, fn)
}

// withMigrationsLock executes fn while holding a session level
// pg_advisory_lock on a dedicated connection from the data db pool
// and another one from the auxiliary db pool.
//
// Since session level advisory locks are not reentrant across connections,
// fn must not call withMigrationsLock again (use MigrationsRunner.lockHeld).
//
// No lock is acquired for a transactional app
// (the caller is responsible to serialize the transaction).
func withMigrationsLock(app App, fn func() error) error {
	ctx := context.Background()

	locks := []struct {
		builder dbx.Builder
		key     int64
	}{
		// always acquired in the same order to avoid deadlocks between nodes
		{app.ConcurrentDB(), migrationsLockKeyData},
		{app.AuxConcurrentDB(), migrationsLockKeyAux},
	}

	for _, l := range locks {
		db, ok := l.builder.(*dbx.DB)
		if !ok {
			continue // transaction or not bootstrapped
		}

		conn, err := pgAdvisoryLock(ctx, db.DB(), l.key)
		if err != nil {
			return fmt.Errorf("failed to acquire the migrations lock: %w", err)
		}
		defer pgAdvisoryUnlock(ctx, conn, l.key)
	}

	return fn()
}

// pgAdvisoryLock waits for and acquires the session level advisory lock key
// on a dedicated pool connection that is returned to the caller.
func pgAdvisoryLock(ctx context.Context, db *sql.DB, key int64) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", key); err != nil {
		discardConn(conn)
		return nil, err
	}

	return conn, nil
}

// pgAdvisoryUnlock releases the session level advisory lock key
// previously acquired with pgAdvisoryLock.
func pgAdvisoryUnlock(ctx context.Context, conn *sql.Conn, key int64) {
	var unlocked bool

	err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", key).Scan(&unlocked)
	if err != nil || !unlocked {
		// never return a connection that may still hold the lock to the pool
		// (closing the session releases all of its advisory locks)
		discardConn(conn)
		return
	}

	conn.Close()
}

// discardConn closes the underlying driver connection instead of returning it to the pool.
func discardConn(conn *sql.Conn) {
	_ = conn.Raw(func(any) error {
		return driver.ErrBadConn
	})
	_ = conn.Close()
}
