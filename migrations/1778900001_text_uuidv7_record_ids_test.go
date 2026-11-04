package migrations_test

import (
	"testing"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
)

func TestTextUUIDv7RecordIdsMigration(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection := core.NewBaseCollection("legacy_uuid")
	idField := collection.Fields.GetByName(core.FieldNameId).(*core.TextField)
	idField.Pattern = "<uuidv7>"
	idField.AutogeneratePattern = "<uuidv7>"
	idField.Min = 36
	idField.Max = 36
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}

	record := core.NewRecord(collection)
	if err := app.Save(record); err != nil {
		t.Fatal(err)
	}

	// simulate a table created by an older version (incl. a dependent view)
	for _, q := range []string{
		`ALTER TABLE "legacy_uuid" ALTER COLUMN "id" DROP DEFAULT, ALTER COLUMN "id" TYPE UUID USING "id"::uuid, ALTER COLUMN "id" SET DEFAULT uuid_generate_v7()`,
		`CREATE VIEW "legacy_uuid_view" AS SELECT id FROM legacy_uuid`,
	} {
		if _, err := app.DB().NewQuery(q).Execute(); err != nil {
			t.Fatal(err)
		}
	}

	m := findSystemMigration(t, "1778900001_text_uuidv7_record_ids.go")
	if err := app.RunInTransaction(m.Up); err != nil {
		t.Fatal(err)
	}

	var dataType string
	err := app.DB().NewQuery(`
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'legacy_uuid' AND column_name = 'id'
	`).Row(&dataType)
	if err != nil {
		t.Fatal(err)
	}
	if dataType != "text" {
		t.Fatalf("Expected text id column, got %q", dataType)
	}

	var viewId string
	err = app.DB().NewQuery(`SELECT id FROM "legacy_uuid_view" WHERE id = {:id}`).
		Bind(dbx.Params{"id": record.Id}).
		Row(&viewId)
	if err != nil {
		t.Fatalf("Expected the dependent view to be restored: %v", err)
	}
}
