package migrations_test

import (
	"testing"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
)

func findSystemMigration(t *testing.T, file string) *core.Migration {
	t.Helper()

	for _, m := range core.SystemMigrations.Items() {
		if m.File == file {
			return m
		}
	}

	t.Fatalf("Missing system migration %q", file)

	return nil
}

func isColumnNullable(t *testing.T, app core.App, table, column string) bool {
	t.Helper()

	var isNullable string
	err := app.DB().NewQuery(`
		SELECT is_nullable FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = {:table} AND column_name = {:column}
	`).Bind(dbx.Params{"table": table, "column": column}).Row(&isNullable)
	if err != nil {
		t.Fatal(err)
	}

	return isNullable == "YES"
}

func TestNullableAutodateColumnsMigration(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection := core.NewBaseCollection("legacy_autodate")
	collection.Fields.Add(
		&core.AutodateField{Name: "created", OnCreate: true},
		&core.AutodateField{Name: "touched", OnUpdate: true},
	)
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}

	// simulate columns created by an older version
	for _, col := range []string{"created", "touched"} {
		_, err := app.DB().NewQuery("ALTER TABLE {{legacy_autodate}} ALTER COLUMN [[" + col + "]] SET NOT NULL").Execute()
		if err != nil {
			t.Fatal(err)
		}
		if isColumnNullable(t, app, "legacy_autodate", col) {
			t.Fatalf("Expected %q to be NOT NULL before the migration", col)
		}
	}

	m := findSystemMigration(t, "1778900000_nullable_autodate_columns.go")
	if err := app.RunInTransaction(m.Up); err != nil {
		t.Fatal(err)
	}

	for _, col := range []string{"created", "touched"} {
		if !isColumnNullable(t, app, "legacy_autodate", col) {
			t.Fatalf("Expected %q to be nullable after the migration", col)
		}
	}

	record := core.NewRecord(collection)
	if err := app.Save(record); err != nil {
		t.Fatalf("Expected the record create to succeed, got %v", err)
	}
}
