package core_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
)

func TestRunInTransaction(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	t.Run("failed nested transaction", func(t *testing.T) {
		app.RunInTransaction(func(txApp core.App) error {
			superuser, _ := txApp.FindAuthRecordByEmail(core.CollectionNameSuperusers, "test@example.com")

			return txApp.RunInTransaction(func(tx2Dao core.App) error {
				if err := tx2Dao.Delete(superuser); err != nil {
					t.Fatal(err)
				}
				return errors.New("test error")
			})
		})

		// superuser should still exist
		superuser, _ := app.FindAuthRecordByEmail(core.CollectionNameSuperusers, "test@example.com")
		if superuser == nil {
			t.Fatal("Expected superuser test@example.com to not be deleted")
		}
	})

	t.Run("successful nested transaction", func(t *testing.T) {
		app.RunInTransaction(func(txApp core.App) error {
			superuser, _ := txApp.FindAuthRecordByEmail(core.CollectionNameSuperusers, "test@example.com")

			return txApp.RunInTransaction(func(tx2Dao core.App) error {
				return tx2Dao.Delete(superuser)
			})
		})

		// superuser should have been deleted
		superuser, _ := app.FindAuthRecordByEmail(core.CollectionNameSuperusers, "test@example.com")
		if superuser != nil {
			t.Fatalf("Expected superuser test@example.com to be deleted, found %v", superuser)
		}
	})
}

func TestTransactionHooksCallsOnFailure(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	createHookCalls := 0
	updateHookCalls := 0
	deleteHookCalls := 0
	afterCreateHookCalls := 0
	afterUpdateHookCalls := 0
	afterDeleteHookCalls := 0

	app.OnModelCreate().BindFunc(func(e *core.ModelEvent) error {
		createHookCalls++
		return e.Next()
	})

	app.OnModelUpdate().BindFunc(func(e *core.ModelEvent) error {
		updateHookCalls++
		return e.Next()
	})

	app.OnModelDelete().BindFunc(func(e *core.ModelEvent) error {
		deleteHookCalls++
		return e.Next()
	})

	app.OnModelAfterCreateSuccess().BindFunc(func(e *core.ModelEvent) error {
		afterCreateHookCalls++
		return e.Next()
	})

	app.OnModelAfterUpdateSuccess().BindFunc(func(e *core.ModelEvent) error {
		afterUpdateHookCalls++
		return e.Next()
	})

	app.OnModelAfterDeleteSuccess().BindFunc(func(e *core.ModelEvent) error {
		afterDeleteHookCalls++
		return e.Next()
	})

	existingModel, _ := app.FindAuthRecordByEmail(core.CollectionNameSuperusers, "test@example.com")

	app.RunInTransaction(func(txApp1 core.App) error {
		return txApp1.RunInTransaction(func(txApp2 core.App) error {
			// test create
			// ---
			newModel := core.NewRecord(existingModel.Collection())
			newModel.SetEmail("test_new1@example.com")
			newModel.SetPassword("1234567890")
			if err := txApp2.Save(newModel); err != nil {
				t.Fatal(err)
			}

			// test update (twice)
			// ---
			if err := txApp2.Save(existingModel); err != nil {
				t.Fatal(err)
			}
			if err := txApp2.Save(existingModel); err != nil {
				t.Fatal(err)
			}

			// test delete
			// ---
			if err := txApp2.Delete(newModel); err != nil {
				t.Fatal(err)
			}

			return errors.New("test_tx_error")
		})
	})

	if createHookCalls != 1 {
		t.Errorf("Expected createHookCalls to be called 1 time, got %d", createHookCalls)
	}
	if updateHookCalls != 2 {
		t.Errorf("Expected updateHookCalls to be called 2 times, got %d", updateHookCalls)
	}
	if deleteHookCalls != 1 {
		t.Errorf("Expected deleteHookCalls to be called 1 time, got %d", deleteHookCalls)
	}
	if afterCreateHookCalls != 0 {
		t.Errorf("Expected afterCreateHookCalls to be called 0 times, got %d", afterCreateHookCalls)
	}
	if afterUpdateHookCalls != 0 {
		t.Errorf("Expected afterUpdateHookCalls to be called 0 times, got %d", afterUpdateHookCalls)
	}
	if afterDeleteHookCalls != 0 {
		t.Errorf("Expected afterDeleteHookCalls to be called 0 times, got %d", afterDeleteHookCalls)
	}
}

func TestTransactionHooksCallsOnSuccess(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	createHookCalls := 0
	updateHookCalls := 0
	deleteHookCalls := 0
	afterCreateHookCalls := 0
	afterUpdateHookCalls := 0
	afterDeleteHookCalls := 0

	app.OnModelCreate().BindFunc(func(e *core.ModelEvent) error {
		createHookCalls++
		return e.Next()
	})

	app.OnModelUpdate().BindFunc(func(e *core.ModelEvent) error {
		updateHookCalls++
		return e.Next()
	})

	app.OnModelDelete().BindFunc(func(e *core.ModelEvent) error {
		deleteHookCalls++
		return e.Next()
	})

	app.OnModelAfterCreateSuccess().BindFunc(func(e *core.ModelEvent) error {
		if e.App.IsTransactional() {
			t.Fatal("Expected e.App to be non-transactional")
		}

		afterCreateHookCalls++
		return e.Next()
	})

	app.OnModelAfterUpdateSuccess().BindFunc(func(e *core.ModelEvent) error {
		if e.App.IsTransactional() {
			t.Fatal("Expected e.App to be non-transactional")
		}

		afterUpdateHookCalls++
		return e.Next()
	})

	app.OnModelAfterDeleteSuccess().BindFunc(func(e *core.ModelEvent) error {
		if e.App.IsTransactional() {
			t.Fatal("Expected e.App to be non-transactional")
		}

		afterDeleteHookCalls++
		return e.Next()
	})

	existingModel, _ := app.FindAuthRecordByEmail(core.CollectionNameSuperusers, "test@example.com")

	app.RunInTransaction(func(txApp1 core.App) error {
		return txApp1.RunInTransaction(func(txApp2 core.App) error {
			// test create
			// ---
			newModel := core.NewRecord(existingModel.Collection())
			newModel.SetEmail("test_new1@example.com")
			newModel.SetPassword("1234567890")
			if err := txApp2.Save(newModel); err != nil {
				t.Fatal(err)
			}

			// test update (twice)
			// ---
			if err := txApp2.Save(existingModel); err != nil {
				t.Fatal(err)
			}
			if err := txApp2.Save(existingModel); err != nil {
				t.Fatal(err)
			}

			// test delete
			// ---
			if err := txApp2.Delete(newModel); err != nil {
				t.Fatal(err)
			}

			return nil
		})
	})

	if createHookCalls != 1 {
		t.Errorf("Expected createHookCalls to be called 1 time, got %d", createHookCalls)
	}
	if updateHookCalls != 2 {
		t.Errorf("Expected updateHookCalls to be called 2 times, got %d", updateHookCalls)
	}
	if deleteHookCalls != 1 {
		t.Errorf("Expected deleteHookCalls to be called 1 time, got %d", deleteHookCalls)
	}
	if afterCreateHookCalls != 1 {
		t.Errorf("Expected afterCreateHookCalls to be called 1 time, got %d", afterCreateHookCalls)
	}
	if afterUpdateHookCalls != 2 {
		t.Errorf("Expected afterUpdateHookCalls to be called 2 times, got %d", afterUpdateHookCalls)
	}
	if afterDeleteHookCalls != 1 {
		t.Errorf("Expected afterDeleteHookCalls to be called 1 time, got %d", afterDeleteHookCalls)
	}
}

func TestTransactionFromInnerCreateHook(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	app.OnRecordCreateExecute("demo2").BindFunc(func(e *core.RecordEvent) error {
		originalApp := e.App
		return e.App.RunInTransaction(func(txApp core.App) error {
			e.App = txApp
			defer func() {
				e.App = originalApp
			}()

			nextErr := e.Next()

			return nextErr
		})
	})

	app.OnRecordAfterCreateSuccess("demo2").BindFunc(func(e *core.RecordEvent) error {
		if e.App.IsTransactional() {
			t.Fatal("Expected e.App to be non-transactional")
		}

		// perform a db query with the app instance to ensure that it is still valid
		_, err := e.App.FindFirstRecordByFilter("demo2", "1=1")
		if err != nil {
			t.Fatalf("Failed to perform a db query after tx success: %v", err)
		}

		return e.Next()
	})

	collection, err := app.FindCollectionByNameOrId("demo2")
	if err != nil {
		t.Fatal(err)
	}

	record := core.NewRecord(collection)

	record.Set("title", "test_inner_tx")

	if err = app.Save(record); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	expectedHookCalls := map[string]int{
		"OnRecordCreateExecute":      1,
		"OnRecordAfterCreateSuccess": 1,
	}
	for k, total := range expectedHookCalls {
		if found, ok := app.EventCalls[k]; !ok || total != found {
			t.Fatalf("Expected %q %d calls, got %d", k, total, found)
		}
	}
}

func TestTransactionFromInnerUpdateHook(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	app.OnRecordUpdateExecute("demo2").BindFunc(func(e *core.RecordEvent) error {
		originalApp := e.App
		return e.App.RunInTransaction(func(txApp core.App) error {
			e.App = txApp
			defer func() {
				e.App = originalApp
			}()

			nextErr := e.Next()

			return nextErr
		})
	})

	app.OnRecordAfterUpdateSuccess("demo2").BindFunc(func(e *core.RecordEvent) error {
		if e.App.IsTransactional() {
			t.Fatal("Expected e.App to be non-transactional")
		}

		// perform a db query with the app instance to ensure that it is still valid
		_, err := e.App.FindFirstRecordByFilter("demo2", "1=1")
		if err != nil {
			t.Fatalf("Failed to perform a db query after tx success: %v", err)
		}

		return e.Next()
	})

	existingModel, err := app.FindFirstRecordByFilter("demo2", "1=1")
	if err != nil {
		t.Fatal(err)
	}

	if err = app.Save(existingModel); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	expectedHookCalls := map[string]int{
		"OnRecordUpdateExecute":      1,
		"OnRecordAfterUpdateSuccess": 1,
	}
	for k, total := range expectedHookCalls {
		if found, ok := app.EventCalls[k]; !ok || total != found {
			t.Fatalf("Expected %q %d calls, got %d", k, total, found)
		}
	}
}

func TestTransactionFromInnerDeleteHook(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	app.OnRecordDeleteExecute("demo2").BindFunc(func(e *core.RecordEvent) error {
		originalApp := e.App
		return e.App.RunInTransaction(func(txApp core.App) error {
			e.App = txApp
			defer func() {
				e.App = originalApp
			}()

			nextErr := e.Next()

			return nextErr
		})
	})

	app.OnRecordAfterDeleteSuccess("demo2").BindFunc(func(e *core.RecordEvent) error {
		if e.App.IsTransactional() {
			t.Fatal("Expected e.App to be non-transactional")
		}

		// perform a db query with the app instance to ensure that it is still valid
		_, err := e.App.FindFirstRecordByFilter("demo2", "1=1")
		if err != nil {
			t.Fatalf("Failed to perform a db query after tx success: %v", err)
		}

		return e.Next()
	})

	existingModel, err := app.FindFirstRecordByFilter("demo2", "1=1")
	if err != nil {
		t.Fatal(err)
	}

	if err = app.Delete(existingModel); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	expectedHookCalls := map[string]int{
		"OnRecordDeleteExecute":      1,
		"OnRecordAfterDeleteSuccess": 1,
	}
	for k, total := range expectedHookCalls {
		if found, ok := app.EventCalls[k]; !ok || total != found {
			t.Fatalf("Expected %q %d calls, got %d", k, total, found)
		}
	}
}

func TestRunInTransactionFailedNestedKeepsOuterTxUsable(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection, err := app.FindCollectionByNameOrId("demo2")
	if err != nil {
		t.Fatal(err)
	}

	scenarios := []struct {
		name   string
		aux    bool
		nested func(txApp core.App) error
	}{
		{
			"returned statement error",
			false,
			func(txApp core.App) error {
				superuser, err := txApp.FindAuthRecordByEmail(core.CollectionNameSuperusers, "test@example.com")
				if err != nil {
					t.Fatal(err)
				}
				if err := txApp.Delete(superuser); err != nil {
					t.Fatal(err)
				}
				_, err = txApp.DB().NewQuery("SELECT 1/0").Execute()
				return err
			},
		},
		{
			"swallowed statement error",
			false,
			func(txApp core.App) error {
				txApp.DB().NewQuery("SELECT 1/0").Execute()
				return nil
			},
		},
		{
			"aux returned statement error",
			true,
			func(txApp core.App) error {
				_, err := txApp.AuxDB().NewQuery("SELECT 1/0").Execute()
				return err
			},
		},
	}

	for i, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			title := fmt.Sprintf("outer_after_nested_failure_%d", i)

			run := app.RunInTransaction
			if s.aux {
				run = app.AuxRunInTransaction
			}

			outerErr := run(func(txApp core.App) error {
				nestedRun := txApp.RunInTransaction
				if s.aux {
					nestedRun = txApp.AuxRunInTransaction
				}

				if err := nestedRun(s.nested); err == nil {
					t.Fatal("Expected the nested call to fail")
				}

				// the outer tx must remain usable
				if s.aux {
					total := 0
					if err := txApp.AuxDB().NewQuery("SELECT count(*) FROM {{_logs}}").Row(&total); err != nil {
						return fmt.Errorf("aux outer query failed: %w", err)
					}
				}

				if _, err := txApp.FindAuthRecordByEmail(core.CollectionNameSuperusers, "test@example.com"); err != nil {
					return fmt.Errorf("outer query failed: %w", err)
				}

				record := core.NewRecord(collection)
				record.Set("title", title)
				return txApp.Save(record)
			})
			if outerErr != nil {
				t.Fatalf("Expected the outer tx to succeed, got %v", outerErr)
			}

			if _, err := app.FindFirstRecordByData("demo2", "title", title); err != nil {
				t.Fatalf("Expected the outer tx record to be persisted: %v", err)
			}

			// the nested changes must be rolled back
			if _, err := app.FindAuthRecordByEmail(core.CollectionNameSuperusers, "test@example.com"); err != nil {
				t.Fatalf("Expected the nested delete to be rolled back: %v", err)
			}
		})
	}
}

func TestRunInTransactionNestedAfterFuncs(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection, err := app.FindCollectionByNameOrId("demo2")
	if err != nil {
		t.Fatal(err)
	}

	var successTitles, errorTitles []string
	var errorHookTxs []bool

	app.OnRecordAfterCreateSuccess("demo2").BindFunc(func(e *core.RecordEvent) error {
		if e.App.IsTransactional() {
			t.Errorf("[%s] Expected the after success e.App to be non-transactional", e.Record.GetString("title"))
		}
		if _, err := e.App.FindRecordById("demo2", e.Record.Id); err != nil {
			t.Errorf("[%s] Expected the record to be persisted: %v", e.Record.GetString("title"), err)
		}
		successTitles = append(successTitles, e.Record.GetString("title"))
		return e.Next()
	})

	app.OnRecordAfterCreateError("demo2").BindFunc(func(e *core.RecordErrorEvent) error {
		// perform a query with the provided app to ensure that it is still valid
		if _, err := e.App.FindFirstRecordByFilter("demo2", "1=1"); err != nil {
			t.Errorf("[%s] Failed to query with the after error e.App: %v", e.Record.GetString("title"), err)
		}
		errorTitles = append(errorTitles, e.Record.GetString("title"))
		errorHookTxs = append(errorHookTxs, e.App.IsTransactional())
		return e.Next()
	})

	newRecord := func(title string) *core.Record {
		r := core.NewRecord(collection)
		r.Set("title", title)
		return r
	}

	t.Run("failed nested", func(t *testing.T) {
		successTitles, errorTitles, errorHookTxs = nil, nil, nil

		nestedErr := errors.New("nested_error")
		var completeErrs []error

		outerErr := app.RunInTransaction(func(txApp core.App) error {
			err := txApp.RunInTransaction(func(nestedApp core.App) error {
				nestedApp.TxInfo().OnComplete(func(txErr error) error {
					completeErrs = append(completeErrs, txErr)
					return nil
				})
				if err := nestedApp.Save(newRecord("nested_failed")); err != nil {
					t.Fatal(err)
				}
				return nestedErr
			})
			if !errors.Is(err, nestedErr) {
				t.Fatalf("Expected nested error %v, got %v", nestedErr, err)
			}

			// must be invoked on the nested rollback (the outer tx is still active)
			if len(completeErrs) != 1 || !errors.Is(completeErrs[0], nestedErr) {
				t.Fatalf("Expected OnComplete to be called once with the nested error, got %v", completeErrs)
			}
			if len(errorTitles) != 1 || !errorHookTxs[0] {
				t.Fatalf("Expected 1 after error hook call with transactional app, got %v (%v)", errorTitles, errorHookTxs)
			}

			return txApp.Save(newRecord("outer_ok"))
		})
		if outerErr != nil {
			t.Fatal(outerErr)
		}

		if len(completeErrs) != 1 {
			t.Fatalf("Expected OnComplete to not be called again on the outer commit, got %v", completeErrs)
		}
		if fmt.Sprint(successTitles) != "[outer_ok]" {
			t.Fatalf("Expected only the outer record success hook, got %v", successTitles)
		}
		if fmt.Sprint(errorTitles) != "[nested_failed]" {
			t.Fatalf("Expected only the nested record error hook, got %v", errorTitles)
		}
		if _, err := app.FindFirstRecordByData("demo2", "title", "nested_failed"); err == nil {
			t.Fatal("Expected the nested record to be rolled back")
		}
	})

	t.Run("successful nested", func(t *testing.T) {
		successTitles, errorTitles, errorHookTxs = nil, nil, nil

		var completeErrs []error

		outerErr := app.RunInTransaction(func(txApp core.App) error {
			err := txApp.RunInTransaction(func(nestedApp core.App) error {
				nestedApp.TxInfo().OnComplete(func(txErr error) error {
					completeErrs = append(completeErrs, txErr)
					return nil
				})
				return nestedApp.Save(newRecord("nested_ok"))
			})
			if err != nil {
				t.Fatal(err)
			}

			if len(completeErrs) != 0 || len(successTitles) != 0 {
				t.Fatalf("Expected the after funcs to be deferred until the outer commit, got %v %v", completeErrs, successTitles)
			}

			return nil
		})
		if outerErr != nil {
			t.Fatal(outerErr)
		}

		if len(completeErrs) != 1 || completeErrs[0] != nil {
			t.Fatalf("Expected OnComplete to be called once with nil error, got %v", completeErrs)
		}
		if fmt.Sprint(successTitles) != "[nested_ok]" || len(errorTitles) != 0 {
			t.Fatalf("Expected the nested record success hook, got %v %v", successTitles, errorTitles)
		}
	})

	t.Run("successful nested with failed outer", func(t *testing.T) {
		successTitles, errorTitles, errorHookTxs = nil, nil, nil

		outerErrValue := errors.New("outer_error")
		var completeErrs []error

		outerErr := app.RunInTransaction(func(txApp core.App) error {
			err := txApp.RunInTransaction(func(nestedApp core.App) error {
				nestedApp.TxInfo().OnComplete(func(txErr error) error {
					completeErrs = append(completeErrs, txErr)
					return nil
				})
				return nestedApp.Save(newRecord("nested_ok_outer_failed"))
			})
			if err != nil {
				t.Fatal(err)
			}
			return outerErrValue
		})
		if !errors.Is(outerErr, outerErrValue) {
			t.Fatalf("Expected outer error, got %v", outerErr)
		}

		if len(completeErrs) != 1 || !errors.Is(completeErrs[0], outerErrValue) {
			t.Fatalf("Expected OnComplete to be called once with the outer error, got %v", completeErrs)
		}
		if len(successTitles) != 0 || fmt.Sprint(errorTitles) != "[nested_ok_outer_failed]" || errorHookTxs[0] {
			t.Fatalf("Expected only a non-transactional error hook call, got %v %v %v", successTitles, errorTitles, errorHookTxs)
		}
	})

	t.Run("panic in nested", func(t *testing.T) {
		successTitles, errorTitles, errorHookTxs = nil, nil, nil

		outerErr := app.RunInTransaction(func(txApp core.App) error {
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("Expected the nested panic to be rethrown")
					}
				}()
				txApp.RunInTransaction(func(nestedApp core.App) error {
					if err := nestedApp.Save(newRecord("nested_panic")); err != nil {
						t.Fatal(err)
					}
					panic("nested_panic")
				})
			}()

			return txApp.Save(newRecord("outer_after_panic"))
		})
		if outerErr != nil {
			t.Fatal(outerErr)
		}

		if fmt.Sprint(successTitles) != "[outer_after_panic]" {
			t.Fatalf("Expected only the outer record success hook, got %v", successTitles)
		}
		if _, err := app.FindFirstRecordByData("demo2", "title", "nested_panic"); err == nil {
			t.Fatal("Expected the nested record to be rolled back")
		}
	})
}

func TestRunInTransactionDeepNesting(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection, err := app.FindCollectionByNameOrId("demo2")
	if err != nil {
		t.Fatal(err)
	}

	var successTitles []string
	app.OnRecordAfterCreateSuccess("demo2").BindFunc(func(e *core.RecordEvent) error {
		if e.App.IsTransactional() {
			t.Errorf("[%s] Expected the after success e.App to be non-transactional", e.Record.GetString("title"))
		}
		successTitles = append(successTitles, e.Record.GetString("title"))
		return e.Next()
	})

	save := func(txApp core.App, title string) error {
		r := core.NewRecord(collection)
		r.Set("title", title)
		return txApp.Save(r)
	}

	exists := func(title string) bool {
		_, err := app.FindFirstRecordByData("demo2", "title", title)
		return err == nil
	}

	t.Run("all levels succeed", func(t *testing.T) {
		successTitles = nil

		var levels func(txApp core.App, depth int) error
		levels = func(txApp core.App, depth int) error {
			if err := save(txApp, fmt.Sprintf("deep_ok_%d", depth)); err != nil {
				return err
			}
			if depth == 4 {
				return nil
			}
			return txApp.RunInTransaction(func(nestedApp core.App) error {
				return levels(nestedApp, depth+1)
			})
		}

		if err := app.RunInTransaction(func(txApp core.App) error { return levels(txApp, 0) }); err != nil {
			t.Fatal(err)
		}

		if len(successTitles) != 5 {
			t.Fatalf("Expected 5 after success hook calls, got %v", successTitles)
		}
		for i := 0; i <= 4; i++ {
			if !exists(fmt.Sprintf("deep_ok_%d", i)) {
				t.Fatalf("Expected deep_ok_%d to be persisted", i)
			}
		}
	})

	t.Run("middle level fails after successful inner level", func(t *testing.T) {
		successTitles = nil

		middleErr := errors.New("middle_error")
		var innerCompleteErrs []error

		err := app.RunInTransaction(func(txApp0 core.App) error {
			err := txApp0.RunInTransaction(func(txApp1 core.App) error {
				if err := save(txApp1, "deep_l1"); err != nil {
					return err
				}

				err := txApp1.RunInTransaction(func(txApp2 core.App) error {
					txApp2.TxInfo().OnComplete(func(txErr error) error {
						innerCompleteErrs = append(innerCompleteErrs, txErr)
						return nil
					})
					return save(txApp2, "deep_l2")
				})
				if err != nil {
					return err
				}

				// sibling nested call that fails at the same level
				txApp1.RunInTransaction(func(txApp2 core.App) error {
					_, err := txApp2.DB().NewQuery("SELECT 1/0").Execute()
					return err
				})

				if err := save(txApp1, "deep_l1_after_sibling"); err != nil {
					return err
				}

				return middleErr
			})
			if !errors.Is(err, middleErr) {
				t.Fatalf("Expected middle error, got %v", err)
			}

			if len(innerCompleteErrs) != 1 || !errors.Is(innerCompleteErrs[0], middleErr) {
				t.Fatalf("Expected the inner OnComplete to be called with the middle error, got %v", innerCompleteErrs)
			}

			return save(txApp0, "deep_l0")
		})
		if err != nil {
			t.Fatal(err)
		}

		if len(innerCompleteErrs) != 1 {
			t.Fatalf("Expected the inner OnComplete to not be called again, got %v", innerCompleteErrs)
		}
		if fmt.Sprint(successTitles) != "[deep_l0]" {
			t.Fatalf("Expected only the level 0 success hook, got %v", successTitles)
		}
		if !exists("deep_l0") {
			t.Fatal("Expected deep_l0 to be persisted")
		}
		for _, title := range []string{"deep_l1", "deep_l2", "deep_l1_after_sibling"} {
			if exists(title) {
				t.Fatalf("Expected %s to be rolled back", title)
			}
		}
	})
}

func TestRunInTransactionConcurrentReadModifyWrite(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection := core.NewBaseCollection("tx_counters")
	collection.Fields.Add(&core.NumberField{Name: "n"})
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}

	counter := core.NewRecord(collection)
	counter.Set("n", 0)
	if err := app.Save(counter); err != nil {
		t.Fatal(err)
	}

	const workers = 20

	var successes atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			err := app.RunInTransaction(func(txApp core.App) error {
				r, err := txApp.FindRecordById(collection, counter.Id)
				if err != nil {
					return err
				}
				time.Sleep(5 * time.Millisecond) // simulate some work between the read and the write
				r.Set("n", r.GetInt("n")+1)
				return txApp.Save(r)
			})
			if err == nil {
				successes.Add(1)
			}
		})
	}
	wg.Wait()

	r, err := app.FindRecordById(collection, counter.Id)
	if err != nil {
		t.Fatal(err)
	}

	if successes.Load() == 0 {
		t.Fatal("Expected at least 1 successful increment")
	}
	if n := r.GetInt("n"); int64(n) != successes.Load() {
		t.Fatalf("Lost updates: %d successful increments but the counter is %d", successes.Load(), n)
	}
}

func TestRunInTransactionSerializationLock(t *testing.T) {
	// note: not parallel because of t.Setenv

	// counts the transaction serialization advisory locks held by the current db session
	countLocks := func(t *testing.T, builder dbx.Builder) int {
		var total int
		err := builder.NewQuery(`
			SELECT count(*) FROM pg_locks
			WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted
			AND classid = 0 AND objid = 723390700 AND objsubid = 1
		`).Row(&total)
		if err != nil {
			t.Fatal(err)
		}
		return total
	}

	scenarios := []struct {
		name          string
		env           string
		config        bool
		expectedLocks int
	}{
		{"default", "", false, 1},
		{"PB_TX_SERIALIZE=true", "true", false, 1},
		{"PB_TX_SERIALIZE=false", "false", false, 0},
		{"DisableTxSerialization config", "", true, 0},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			t.Setenv("PB_TX_SERIALIZE", s.env)

			app, err := tests.NewTestAppWithConfig(core.BaseAppConfig{DisableTxSerialization: s.config})
			if err != nil {
				t.Fatal(err)
			}
			defer app.Cleanup()

			err = app.RunInTransaction(func(txApp core.App) error {
				if total := countLocks(t, txApp.DB()); total != s.expectedLocks {
					t.Fatalf("Expected %d top-level tx locks, got %d", s.expectedLocks, total)
				}

				// nested calls reuse the outer tx lock
				return txApp.RunInTransaction(func(nestedApp core.App) error {
					if total := countLocks(t, nestedApp.DB()); total != s.expectedLocks {
						t.Fatalf("Expected %d nested tx locks, got %d", s.expectedLocks, total)
					}
					return nil
				})
			})
			if err != nil {
				t.Fatal(err)
			}

			// the aux db transactions are never serialized
			err = app.AuxRunInTransaction(func(txApp core.App) error {
				if total := countLocks(t, txApp.AuxDB()); total != 0 {
					t.Fatalf("Expected no aux tx locks, got %d", total)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
