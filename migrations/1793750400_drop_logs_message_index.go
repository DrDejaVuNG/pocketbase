package migrations

import (
	"github.com/pocketbase/pocketbase/core"
)

// PostgreSQL:
// The B-tree idx_logs_message index (see 1640988000_aux_init.go) rejects
// index rows larger than 2704 bytes (SQLSTATE 54000), so a single long
// incompressible log message (request uris are logged up to several KB)
// failed its insert. The index is also unusable for the logs UI message
// search because it is translated to LIKE '%...%'.
func init() {
	core.SystemMigrations.Register(func(txApp core.App) error {
		_, err := txApp.AuxDB().NewQuery("DROP INDEX IF EXISTS {{idx_logs_message}}").Execute()
		return err
	}, nil)
}
