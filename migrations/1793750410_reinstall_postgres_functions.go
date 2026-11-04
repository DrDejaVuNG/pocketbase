package migrations

import (
	"fmt"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

// PostgreSQL:
// The SQLite equivalent functions were installed only once (and not at all
// if any uuid_generate_v7 function existed), so fixed definitions never
// reached existing databases. This migration re-runs the idempotent
// installation batch to deliver:
//   - randomblob() as VOLATILE instead of IMMUTABLE (the same "random"
//     value for every row of a statement -> duplicated default ids)
//   - strftime_impl() and total() for databases initialized before they were added
//
// Changed function definitions should be delivered with a new migration like this one.
func init() {
	core.SystemMigrations.Register(func(txApp core.App) error {
		return reinstallSQLiteEquivalentFunctions(txApp)
	}, nil)
}

func reinstallSQLiteEquivalentFunctions(txApp core.App) error {
	// when the data and the auxiliary db are the same physical database
	// the functions must be replaced only once because the two still open
	// migration transactions would otherwise block each other
	sameDB, err := isSameDatabase(txApp.DB(), txApp.AuxDB())
	if err != nil {
		return err
	}

	if !sameDB {
		if err := createSQLiteEquivalentFunctions(txApp.AuxDB(), advisoryLockKeyAux); err != nil {
			return fmt.Errorf("aux createSQLiteEquivalentFunctions error: %w", err)
		}
	}

	if err := createSQLiteEquivalentFunctions(txApp.DB(), advisoryLockKeyData); err != nil {
		return fmt.Errorf("createSQLiteEquivalentFunctions error: %w", err)
	}

	return nil
}

// isSameDatabase reports whether auxDB is connected to the same physical
// database as the (dedicated transaction) connection of dataDB.
func isSameDatabase(dataDB dbx.Builder, auxDB dbx.Builder) (bool, error) {
	var dataPid int64
	if err := dataDB.NewQuery("SELECT pg_backend_pid()").Row(&dataPid); err != nil {
		return false, err
	}

	var same bool
	err := auxDB.NewQuery("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = {:pid} AND datname = current_database())").
		Bind(dbx.Params{"pid": dataPid}).
		Row(&same)

	return same, err
}
