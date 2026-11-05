package core_test

import (
	"errors"
	"testing"

	validation "ganigeorgiev.com/pocketbase/ozzo-validation/v4"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tests"
)

// A relation target that is deleted after the record validation (eg. by a
// concurrent cascade delete on another node) must fail the write with the
// relation validation error instead of storing a reference to a missing record.
func TestRecordSaveRelationTargetDeletedAfterValidation(t *testing.T) {
	t.Parallel()

	app, err := tests.NewTestApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Cleanup()

	targets := core.NewBaseCollection("rg_targets")
	targets.Fields.Add(&core.TextField{Name: "name"})
	if err := app.Save(targets); err != nil {
		t.Fatal(err)
	}

	refs := core.NewBaseCollection("rg_refs")
	refs.Fields.Add(
		&core.RelationField{Name: "one", CollectionId: targets.Id, MaxSelect: 1},
		&core.RelationField{Name: "many", CollectionId: targets.Id, MaxSelect: 5},
	)
	if err := app.Save(refs); err != nil {
		t.Fatal(err)
	}

	newTarget := func() *core.Record {
		r := core.NewRecord(targets)
		r.Set("name", "target")
		if err := app.Save(r); err != nil {
			t.Fatal(err)
		}
		return r
	}

	// deletes the target between the validation and the db write
	var deleteBeforeWrite *core.Record
	deleteTarget := func(e *core.RecordEvent) error {
		if deleteBeforeWrite != nil {
			if err := e.App.Delete(deleteBeforeWrite); err != nil {
				return err
			}
			deleteBeforeWrite = nil
		}
		return e.Next()
	}
	app.OnRecordCreateExecute(refs.Name).BindFunc(deleteTarget)
	app.OnRecordUpdateExecute(refs.Name).BindFunc(deleteTarget)

	assertMissingRelError := func(t *testing.T, err error, field string) {
		t.Helper()

		var errs validation.Errors
		if !errors.As(err, &errs) {
			t.Fatalf("Expected validation errors, got %v", err)
		}

		var fieldErr validation.Error
		if !errors.As(errs[field], &fieldErr) || fieldErr.Code() != "validation_missing_rel_records" {
			t.Fatalf("Expected %q validation_missing_rel_records error, got %v", field, errs)
		}
	}

	countRefs := func(t *testing.T) int {
		t.Helper()

		total, err := app.CountRecords(refs)
		if err != nil {
			t.Fatal(err)
		}
		return int(total)
	}

	t.Run("create with single relation", func(t *testing.T) {
		target := newTarget()
		before := countRefs(t)

		record := core.NewRecord(refs)
		record.Set("one", target.Id)
		deleteBeforeWrite = target

		assertMissingRelError(t, app.Save(record), "one")

		if after := countRefs(t); after != before {
			t.Fatalf("Expected no new record, got %d -> %d", before, after)
		}
	})

	t.Run("create with multiple relation", func(t *testing.T) {
		kept, deleted := newTarget(), newTarget()

		record := core.NewRecord(refs)
		record.Set("many", []string{kept.Id, deleted.Id})
		deleteBeforeWrite = deleted

		assertMissingRelError(t, app.Save(record), "many")
	})

	t.Run("update to a deleted relation", func(t *testing.T) {
		current, deleted := newTarget(), newTarget()

		record := core.NewRecord(refs)
		record.Set("one", current.Id)
		if err := app.Save(record); err != nil {
			t.Fatal(err)
		}

		record.Set("one", deleted.Id)
		deleteBeforeWrite = deleted

		assertMissingRelError(t, app.Save(record), "one")

		stored, err := app.FindRecordById(refs, record.Id)
		if err != nil {
			t.Fatal(err)
		}
		if v := stored.GetString("one"); v != current.Id {
			t.Fatalf("Expected the stored relation %q, got %q", current.Id, v)
		}
	})

	t.Run("existing relations still save", func(t *testing.T) {
		a, b := newTarget(), newTarget()

		record := core.NewRecord(refs)
		record.Set("one", a.Id)
		record.Set("many", []string{a.Id, b.Id})
		if err := app.Save(record); err != nil {
			t.Fatal(err)
		}

		record.Set("one", b.Id)
		if err := app.Save(record); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("SaveNoValidate keeps storing unchecked relations", func(t *testing.T) {
		record := core.NewRecord(refs)
		record.Set("one", "missing_target_")
		if err := app.SaveNoValidate(record); err != nil {
			t.Fatalf("Expected SaveNoValidate to store the unchecked relation, got %v", err)
		}
	})
}
