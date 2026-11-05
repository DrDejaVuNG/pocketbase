package core_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tests"
)

func TestTextFieldBaseMethods(t *testing.T) {
	testFieldBaseMethods(t, core.FieldTypeText)
}

func TestTextFieldColumnType(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	f := &core.TextField{}

	expected := "TEXT DEFAULT '' NOT NULL"

	if v := f.ColumnType(app); v != expected {
		t.Fatalf("Expected\n%q\ngot\n%q", expected, v)
	}
}

// PostgreSQL: the <uuidv7> ids must be stored as TEXT because the relation
// fields store the referenced ids as TEXT.
func TestTextFieldUUIDv7PrimaryKey(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	pkField := &core.TextField{PrimaryKey: true, Pattern: "<uuidv7>"}
	expectedColumnType := "TEXT PRIMARY KEY DEFAULT (uuid_generate_v7()::text) NOT NULL"
	if v := pkField.ColumnType(app); v != expectedColumnType {
		t.Fatalf("Expected column type\n%q\ngot\n%q", expectedColumnType, v)
	}

	uuidCol := core.NewBaseCollection("rv_uuid")
	idField := uuidCol.Fields.GetByName("id").(*core.TextField)
	idField.Pattern = "<uuidv7>"
	idField.AutogeneratePattern = "<uuidv7>"
	idField.Min = 36
	idField.Max = 36
	uuidCol.Fields.Add(&core.TextField{Name: "name"})
	if err := app.Save(uuidCol); err != nil {
		t.Fatal(err)
	}

	refCol := core.NewBaseCollection("rv_uuid_ref")
	refCol.Fields.Add(
		&core.TextField{Name: "label"},
		&core.RelationField{Name: "u", CollectionId: uuidCol.Id, MaxSelect: 1},
	)
	if err := app.Save(refCol); err != nil {
		t.Fatal(err)
	}

	t.Run("id validation", func(t *testing.T) {
		scenarios := []struct {
			id          string
			expectError bool
		}{
			{"0190a6a8-7f2d-7c3a-9b1e-3f6c2a1b4d5e", false},
			{"0190A6A8-7F2D-7C3A-9B1E-3F6C2A1B4D5E", true},   // uppercase
			{"{0190a6a8-7f2d-7c3a-9b1e-3f6c2a1b4d5e}", true}, // braces
			{"0190a6a87f2d7c3a9b1e3f6c2a1b4d5e", true},       // no hyphens
			{"0190a6a8-7f2d-4c3a-9b1e-3f6c2a1b4d5e", true},   // v4
			{"abc", true},
		}

		for _, s := range scenarios {
			record := core.NewRecord(uuidCol)
			record.Id = s.id
			err := app.Validate(record)
			if hasErr := err != nil; hasErr != s.expectError {
				t.Fatalf("[%s] Expected hasErr %v, got %v (%v)", s.id, s.expectError, hasErr, err)
			}
		}
	})

	first := core.NewRecord(uuidCol)
	first.Set("name", "first")
	if err := app.Save(first); err != nil {
		t.Fatal(err)
	}

	ref := core.NewRecord(refCol)
	ref.Set("label", "ref1")
	ref.Set("u", first.Id)
	if err := app.Save(ref); err != nil {
		t.Fatal(err)
	}

	assertRelationFilters(t, app, first.Id, ref.Id)

	t.Run("legacy UUID column conversion", func(t *testing.T) {
		view := core.NewViewCollection("rv_uuid_view")
		view.ViewQuery = "select id, name from rv_uuid"
		if err := app.Save(view); err != nil {
			t.Fatal(err)
		}

		// simulate a table created by an older version
		for _, q := range []string{
			`DROP VIEW "rv_uuid_view"`,
			`ALTER TABLE "rv_uuid" ALTER COLUMN "id" DROP DEFAULT, ALTER COLUMN "id" TYPE UUID USING "id"::uuid, ALTER COLUMN "id" SET DEFAULT uuid_generate_v7()`,
			`CREATE VIEW "rv_uuid_view" AS select id, name from rv_uuid`,
		} {
			if _, err := app.DB().NewQuery(q).Execute(); err != nil {
				t.Fatal(err)
			}
		}
		if dataType := idColumnType(t, app, "rv_uuid"); dataType != "uuid" {
			t.Fatalf("Expected uuid id column before the conversion, got %q", dataType)
		}

		if err := app.SyncRecordTableSchema(uuidCol, uuidCol); err != nil {
			t.Fatal(err)
		}

		if dataType := idColumnType(t, app, "rv_uuid"); dataType != "text" {
			t.Fatalf("Expected text id column after the conversion, got %q", dataType)
		}

		if _, err := app.FindRecordById("rv_uuid_view", first.Id); err != nil {
			t.Fatalf("Expected the dependent view to be restored: %v", err)
		}

		assertRelationFilters(t, app, first.Id, ref.Id)

		// the default must still produce uuid ids
		var defaultId string
		err := app.DB().NewQuery(`INSERT INTO "rv_uuid" ("name") VALUES ('raw') RETURNING "id"`).Row(&defaultId)
		if err != nil {
			t.Fatal(err)
		}
		if len(defaultId) != 36 {
			t.Fatalf("Expected uuid default id, got %q", defaultId)
		}
	})
}

func idColumnType(t *testing.T, app core.App, table string) string {
	t.Helper()

	var dataType string
	err := app.DB().NewQuery(`
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = {:table} AND column_name = 'id'
	`).Bind(dbx.Params{"table": table}).Row(&dataType)
	if err != nil {
		t.Fatal(err)
	}

	return dataType
}

func assertRelationFilters(t *testing.T, app core.App, uuidId string, refId string) {
	t.Helper()

	refs, err := app.FindRecordsByFilter("rv_uuid_ref", "u.name = 'first'", "", 0, 0)
	if err != nil {
		t.Fatalf("Relation filter failed: %v", err)
	}
	if len(refs) != 1 || refs[0].Id != refId {
		t.Fatalf("Expected 1 ref record, got %v", refs)
	}

	uuids, err := app.FindRecordsByFilter("rv_uuid", "rv_uuid_ref_via_u.label ?= 'ref1'", "", 0, 0)
	if err != nil {
		t.Fatalf("Back-relation filter failed: %v", err)
	}
	if len(uuids) != 1 || uuids[0].Id != uuidId {
		t.Fatalf("Expected 1 uuid record, got %v", uuids)
	}

	none, err := app.FindRecordsByFilter("rv_uuid", "id = 'abc'", "", 0, 0)
	if err != nil {
		t.Fatalf("Non-uuid id filter failed: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("Expected no records, got %v", none)
	}
}

func TestTextFieldPrepareValue(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	f := &core.TextField{}
	record := core.NewRecord(core.NewBaseCollection("test"))

	scenarios := []struct {
		raw      any
		expected string
	}{
		{"", ""},
		{"test", "test"},
		{false, "false"},
		{true, "true"},
		{123.456, "123.456"},
	}

	for i, s := range scenarios {
		t.Run(fmt.Sprintf("%d_%#v", i, s.raw), func(t *testing.T) {
			v, err := f.PrepareValue(record, s.raw)
			if err != nil {
				t.Fatal(err)
			}

			vStr, ok := v.(string)
			if !ok {
				t.Fatalf("Expected string instance, got %T", v)
			}

			if vStr != s.expected {
				t.Fatalf("Expected %q, got %q", s.expected, v)
			}
		})
	}
}

func TestTextFieldValidateValue(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection, err := app.FindCollectionByNameOrId("demo1")
	if err != nil {
		t.Fatal(err)
	}

	existingRecord, err := app.FindFirstRecordByFilter(collection, "id != ''")
	if err != nil {
		t.Fatal(err)
	}

	scenarios := []struct {
		name        string
		field       *core.TextField
		record      func() *core.Record
		expectError bool
	}{
		{
			"invalid raw value",
			&core.TextField{Name: "test"},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", 123)
				return record
			},
			true,
		},
		{
			"zero field value (not required)",
			&core.TextField{Name: "test", Pattern: `\d+`, Min: 10, Max: 100}, // other fields validators should be ignored
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "")
				return record
			},
			false,
		},
		{
			"zero field value (required)",
			&core.TextField{Name: "test", Required: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "")
				return record
			},
			true,
		},
		{
			"non-zero field value (required)",
			&core.TextField{Name: "test", Required: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "abc")
				return record
			},
			false,
		},
		{
			"special forbidden character / (non-primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: false},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "abc/")
				return record
			},
			false,
		},
		{
			"special forbidden character \\ (non-primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: false},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "abc\\")
				return record
			},
			false,
		},
		{
			"special forbidden character . (non-primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: false},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "abc.")
				return record
			},
			false,
		},
		{
			"special forbidden character ' ' (non-primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: false},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "ab c")
				return record
			},
			false,
		},
		{
			"special forbidden character * (non-primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: false},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "abc*")
				return record
			},
			false,
		},
		{
			"special forbidden character / (primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "abc/")
				return record
			},
			true,
		},
		{
			"special forbidden character \\ (primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "abc\\")
				return record
			},
			true,
		},
		{
			"special forbidden character . (primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "abc.")
				return record
			},
			true,
		},
		{
			"special forbidden character ' ' (primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "ab c")
				return record
			},
			true,
		},
		{
			"special forbidden character * (primaryKey; used in the realtime events too)",
			&core.TextField{Name: "test", PrimaryKey: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "abc*")
				return record
			},
			true,
		},
		{
			"reserved pk literal (non-primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: false},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "aUx")
				return record
			},
			false,
		},
		{
			"reserved pk literal (primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "aUx")
				return record
			},
			true,
		},
		{
			"reserved pk literal (non-exact match, primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "aUx-")
				return record
			},
			false,
		},
		{
			"zero field value (primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "")
				return record
			},
			true,
		},
		{
			"non-zero field value (primaryKey)",
			&core.TextField{Name: "test", PrimaryKey: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "abcd")
				return record
			},
			false,
		},
		{
			"case-insensitive duplicated primary key check",
			&core.TextField{Name: "test", PrimaryKey: true},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", strings.ToUpper(existingRecord.Id))
				return record
			},
			true,
		},
		{
			"< min",
			&core.TextField{Name: "test", Min: 4},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "абв") // multi-byte
				return record
			},
			true,
		},
		{
			">= min",
			&core.TextField{Name: "test", Min: 3},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "абв") // multi-byte
				return record
			},
			false,
		},
		{
			"> default max",
			&core.TextField{Name: "test"},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", strings.Repeat("a", 5001))
				return record
			},
			true,
		},
		{
			"<= default max",
			&core.TextField{Name: "test"},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", strings.Repeat("a", 500))
				return record
			},
			false,
		},
		{
			"> max",
			&core.TextField{Name: "test", Max: 2},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "абв") // multi-byte
				return record
			},
			true,
		},
		{
			"<= max",
			&core.TextField{Name: "test", Min: 3},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "абв") // multi-byte
				return record
			},
			false,
		},
		{
			"mismatched pattern",
			&core.TextField{Name: "test", Pattern: `\d+`},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "abc")
				return record
			},
			true,
		},
		{
			"matched pattern",
			&core.TextField{Name: "test", Pattern: `\d+`},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "123")
				return record
			},
			false,
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			err := s.field.ValidateValue(context.Background(), app, s.record())

			hasErr := err != nil
			if hasErr != s.expectError {
				t.Fatalf("Expected hasErr %v, got %v (%v)", s.expectError, hasErr, err)
			}
		})
	}
}

func TestTextFieldValidateSettings(t *testing.T) {
	testDefaultFieldIdValidation(t, core.FieldTypeText)
	testDefaultFieldNameValidation(t, core.FieldTypeText)
	testDefaultFieldHelpValidation[core.TextField](t)

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	scenarios := []struct {
		name         string
		field        func() *core.TextField
		expectErrors []string
	}{
		{
			"zero minimal",
			func() *core.TextField {
				return &core.TextField{
					Id:   "test",
					Name: "test",
				}
			},
			[]string{},
		},
		{
			"primaryKey without required",
			func() *core.TextField {
				return &core.TextField{
					Id:         "test",
					Name:       "id",
					PrimaryKey: true,
					Pattern:    `\d+`,
				}
			},
			[]string{"required"},
		},
		{
			"primaryKey without pattern",
			func() *core.TextField {
				return &core.TextField{
					Id:         "test",
					Name:       "id",
					PrimaryKey: true,
					Required:   true,
				}
			},
			[]string{"pattern"},
		},
		{
			"primaryKey with hidden",
			func() *core.TextField {
				return &core.TextField{
					Id:         "test",
					Name:       "id",
					Required:   true,
					PrimaryKey: true,
					Hidden:     true,
					Pattern:    `\d+`,
				}
			},
			[]string{"hidden"},
		},
		{
			"primaryKey with name != id",
			func() *core.TextField {
				return &core.TextField{
					Id:         "test",
					Name:       "test",
					PrimaryKey: true,
					Required:   true,
					Pattern:    `\d+`,
				}
			},
			[]string{"name"},
		},
		{
			"multiple primaryKey fields",
			func() *core.TextField {
				return &core.TextField{
					Id:         "test2",
					Name:       "id",
					PrimaryKey: true,
					Pattern:    `\d+`,
					Required:   true,
				}
			},
			[]string{"primaryKey"},
		},
		{
			"invalid pattern",
			func() *core.TextField {
				return &core.TextField{
					Id:      "test2",
					Name:    "id",
					Pattern: `(invalid`,
				}
			},
			[]string{"pattern"},
		},
		{
			"valid pattern",
			func() *core.TextField {
				return &core.TextField{
					Id:      "test2",
					Name:    "id",
					Pattern: `\d+`,
				}
			},
			[]string{},
		},
		{
			"invalid autogeneratePattern",
			func() *core.TextField {
				return &core.TextField{
					Id:                  "test2",
					Name:                "id",
					AutogeneratePattern: `(invalid`,
				}
			},
			[]string{"autogeneratePattern"},
		},
		{
			"valid autogeneratePattern",
			func() *core.TextField {
				return &core.TextField{
					Id:                  "test2",
					Name:                "id",
					AutogeneratePattern: `[a-z]+`,
				}
			},
			[]string{},
		},
		{
			"conflicting pattern and autogeneratePattern",
			func() *core.TextField {
				return &core.TextField{
					Id:                  "test2",
					Name:                "id",
					Pattern:             `\d+`,
					AutogeneratePattern: `[a-z]+`,
				}
			},
			[]string{"autogeneratePattern"},
		},
		{
			"Max > safe json int",
			func() *core.TextField {
				return &core.TextField{
					Id:   "test",
					Name: "test",
					Max:  1 << 53,
				}
			},
			[]string{"max"},
		},
		{
			"Max < 0",
			func() *core.TextField {
				return &core.TextField{
					Id:   "test",
					Name: "test",
					Max:  -1,
				}
			},
			[]string{"max"},
		},
		{
			"Min > safe json int",
			func() *core.TextField {
				return &core.TextField{
					Id:   "test",
					Name: "test",
					Min:  1 << 53,
				}
			},
			[]string{"min"},
		},
		{
			"Min < 0",
			func() *core.TextField {
				return &core.TextField{
					Id:   "test",
					Name: "test",
					Min:  -1,
				}
			},
			[]string{"min"},
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			field := s.field()

			collection := core.NewBaseCollection("test_collection")
			collection.Fields.GetByName("id").SetId("test") // set a dummy known id so that it can be replaced
			collection.Fields.Add(field)

			errs := field.ValidateSettings(context.Background(), app, collection)

			tests.TestValidationErrors(t, errs, s.expectErrors)
		})
	}
}

func TestTextFieldAutogenerate(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection := core.NewBaseCollection("test_collection")

	scenarios := []struct {
		name       string
		actionName string
		field      *core.TextField
		record     func() *core.Record
		expected   string
	}{
		{
			"non-matching action",
			core.InterceptorActionUpdate,
			&core.TextField{Name: "test", AutogeneratePattern: "abc"},
			func() *core.Record {
				return core.NewRecord(collection)
			},
			"",
		},
		{
			"matching action (create)",
			core.InterceptorActionCreate,
			&core.TextField{Name: "test", AutogeneratePattern: "abc"},
			func() *core.Record {
				return core.NewRecord(collection)
			},
			"abc",
		},
		{
			"matching action (validate)",
			core.InterceptorActionValidate,
			&core.TextField{Name: "test", AutogeneratePattern: "abc"},
			func() *core.Record {
				return core.NewRecord(collection)
			},
			"abc",
		},
		{
			"existing non-zero value",
			core.InterceptorActionCreate,
			&core.TextField{Name: "test", AutogeneratePattern: "abc"},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.SetRaw("test", "123")
				return record
			},
			"123",
		},
		{
			"non-new record",
			core.InterceptorActionValidate,
			&core.TextField{Name: "test", AutogeneratePattern: "abc"},
			func() *core.Record {
				record := core.NewRecord(collection)
				record.Id = "test"
				record.PostScan()
				return record
			},
			"",
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			actionCalls := 0
			record := s.record()

			err := s.field.Intercept(context.Background(), app, record, s.actionName, func() error {
				actionCalls++
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}

			if actionCalls != 1 {
				t.Fatalf("Expected actionCalls %d, got %d", 1, actionCalls)
			}

			v := record.GetString(s.field.GetName())
			if v != s.expected {
				t.Fatalf("Expected value %q, got %q", s.expected, v)
			}
		})
	}
}

func TestTextFieldFindSetter(t *testing.T) {
	scenarios := []struct {
		name      string
		key       string
		value     any
		field     *core.TextField
		hasSetter bool
		expected  string
	}{
		{
			"no match",
			"example",
			"abc",
			&core.TextField{Name: "test", AutogeneratePattern: "test"},
			false,
			"",
		},
		{
			"exact match",
			"test",
			"abc",
			&core.TextField{Name: "test", AutogeneratePattern: "test"},
			true,
			"abc",
		},
		{
			"autogenerate modifier",
			"test:autogenerate",
			"abc",
			&core.TextField{Name: "test", AutogeneratePattern: "test"},
			true,
			"abctest",
		},
		{
			"autogenerate modifier without AutogeneratePattern option",
			"test:autogenerate",
			"abc",
			&core.TextField{Name: "test"},
			true,
			"abc",
		},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			collection := core.NewBaseCollection("test_collection")
			collection.Fields.Add(s.field)

			setter := s.field.FindSetter(s.key)

			hasSetter := setter != nil
			if hasSetter != s.hasSetter {
				t.Fatalf("Expected hasSetter %v, got %v", s.hasSetter, hasSetter)
			}

			if !hasSetter {
				return
			}

			record := core.NewRecord(collection)

			setter(record, s.value)

			result := record.GetString(s.field.Name)

			if result != s.expected {
				t.Fatalf("Expected %q, got %q", s.expected, result)
			}
		})
	}
}
