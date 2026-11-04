package core

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pocketbase/dbx"
)

// txSerializationLockKey is the PostgreSQL advisory lock key used to
// serialize the top-level regular database transactions (see [BaseApp.RunInTransaction]).
//
// note: keep it different from the other advisory lock keys used by the app.
const txSerializationLockKey int64 = 723390700

// RunInTransaction wraps fn into a transaction for the regular app database.
//
// It is safe to nest RunInTransaction calls as long as you use the callback's txApp.
//
// PostgreSQL: nested calls run inside a SAVEPOINT (on error only the nested
// changes are rolled back and the outer transaction remains usable).
//
// PostgreSQL: to preserve the SQLite single writer semantic (and prevent lost updates
// on read-modify-write inside fn), the top-level transactions are serialized
// cluster-wide (for all nodes sharing the same database) with a transaction level
// advisory lock (pg_advisory_xact_lock) acquired as their first statement.
// This could be disabled with BaseAppConfig.DisableTxSerialization
// (or the PB_TX_SERIALIZE=false env variable).
//
// Note that calling the root app RunInTransaction (not the callback's txApp) from
// inside another transaction waits for the lock held by the outer transaction
// (self-deadlock until the query timeout), similar to upstream SQLite.
func (app *BaseApp) RunInTransaction(fn func(txApp App) error) error {
	return app.runInTransaction(app.NonconcurrentDB(), fn, false)
}

// AuxRunInTransaction wraps fn into a transaction for the auxiliary app database.
//
// It is safe to nest RunInTransaction calls as long as you use the callback's txApp.
func (app *BaseApp) AuxRunInTransaction(fn func(txApp App) error) error {
	return app.runInTransaction(app.AuxNonconcurrentDB(), fn, true)
}

func (app *BaseApp) runInTransaction(db dbx.Builder, fn func(txApp App) error, isForAuxDB bool) error {
	switch txOrDB := db.(type) {
	case *dbx.Tx:
		/* SQLite:
		// run as part of the already existing transaction
		return fn(app)
		*/
		// PostgreSQL: any failed statement aborts the whole transaction (25P02),
		// so the nested call runs inside a SAVEPOINT that is rolled back on error,
		// keeping the outer transaction usable (similar to SQLite where a failed
		// statement doesn't abort the transaction).
		return app.runInSavepoint(txOrDB, fn, isForAuxDB)
	case *dbx.DB:
		var txApp *BaseApp
		txErr := txOrDB.Transactional(func(tx *dbx.Tx) error {
			// PostgreSQL: serialize the regular db transactions
			// similar to the SQLite single writer connection
			if !isForAuxDB && !app.config.DisableTxSerialization {
				_, err := tx.NewQuery("SELECT pg_advisory_xact_lock({:key})").Bind(dbx.Params{"key": txSerializationLockKey}).Execute()
				if err != nil {
					return fmt.Errorf("failed to acquire the transaction lock: %w", err)
				}
			}

			txApp = app.createTxApp(tx, isForAuxDB)
			return fn(txApp)
		})

		// execute all after event calls on transaction complete
		if txApp != nil && txApp.txInfo != nil {
			afterFuncErr := txApp.txInfo.runAfterFuncs(txErr)
			if afterFuncErr != nil {
				return errors.Join(txErr, afterFuncErr)
			}
		}

		return txErr
	default:
		return errors.New("failed to start transaction (unknown db type)")
	}
}

// savepointCounter is used to generate unique savepoint names.
var savepointCounter atomic.Uint64

// runInSavepoint executes fn as part of the already existing tx
// wrapped in a SAVEPOINT.
//
// On fn error (or panic) the changes are rolled back to the savepoint,
// the outer transaction remains usable and the after funcs registered
// during the nested call are invoked immediately with the nested error.
//
// On success the savepoint is released and the after funcs registered during
// the nested call are deferred to the outer transaction completion.
func (app *BaseApp) runInSavepoint(tx *dbx.Tx, fn func(txApp App) error, isForAuxDB bool) error {
	name := "pb_sp_" + strconv.FormatUint(savepointCounter.Add(1), 10)

	if _, err := tx.NewQuery("SAVEPOINT " + name).Execute(); err != nil {
		return fmt.Errorf("failed to create savepoint: %w", err)
	}

	rollbackToSavepoint := func() error {
		if _, err := tx.NewQuery("ROLLBACK TO SAVEPOINT " + name).Execute(); err != nil {
			return fmt.Errorf("failed to rollback to savepoint: %w", err)
		}
		if _, err := tx.NewQuery("RELEASE SAVEPOINT " + name).Execute(); err != nil {
			return fmt.Errorf("failed to release savepoint: %w", err)
		}
		return nil
	}

	// the nested txApp has its own TxAppInfo so that the after funcs
	// registered during the nested call could be handled separately
	// (until merged into the outer TxAppInfo its parent is the current tx app)
	nestedApp := app.createTxApp(tx, isForAuxDB)

	// similar to dbx.Transactional, rollback and rethrow on panic
	// (the after funcs are not invoked)
	completed := false
	defer func() {
		if !completed {
			rollbackToSavepoint()
		}
	}()

	txErr := fn(nestedApp)
	if txErr == nil {
		if _, err := tx.NewQuery("RELEASE SAVEPOINT " + name).Execute(); err != nil {
			txErr = fmt.Errorf("failed to release savepoint: %w", err)
		}
	}

	if txErr != nil {
		if err := rollbackToSavepoint(); err != nil {
			txErr = errors.Join(txErr, err)
		}
		completed = true

		// the nested changes were discarded so there is no need to wait the outer tx
		if afterFuncErr := nestedApp.txInfo.runAfterFuncs(txErr); afterFuncErr != nil {
			return errors.Join(txErr, afterFuncErr)
		}

		return txErr
	}

	completed = true

	nestedApp.txInfo.mergeInto(app.txInfo)

	return nil
}

// createTxApp shallow clones the current app and assigns a new tx state.
func (app *BaseApp) createTxApp(tx *dbx.Tx, isForAuxDB bool) *BaseApp {
	clone := *app

	if isForAuxDB {
		clone.auxConcurrentDB = tx
		clone.auxNonconcurrentDB = tx
	} else {
		clone.concurrentDB = tx
		clone.nonconcurrentDB = tx
	}

	clone.txInfo = &TxAppInfo{
		parent:     app,
		isForAuxDB: isForAuxDB,
	}

	return &clone
}

// TxAppInfo represents an active transaction context associated to an existing app instance.
type TxAppInfo struct {
	parent     *BaseApp
	afterFuncs []func(txErr error) error
	merged     []*TxAppInfo // successfully completed nested (savepoint) tx infos
	mu         sync.Mutex
	isForAuxDB bool
}

// OnComplete registers the provided callback that will be invoked
// once the related transaction ends (either completes successfully or rollbacked with an error).
//
// The callback receives the transaction error (if any) as its argument.
// Any additional errors returned by the OnComplete callbacks will be
// joined together with txErr when returning the final transaction result.
func (a *TxAppInfo) OnComplete(fn func(txErr error) error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.afterFuncs = append(a.afterFuncs, fn)
}

// note: can be called only once because TxAppInfo is cleared
func (a *TxAppInfo) runAfterFuncs(txErr error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var errs []error

	for _, call := range a.afterFuncs {
		if err := call(txErr); err != nil {
			errs = append(errs, err)
		}
	}

	a.afterFuncs = nil

	if len(errs) > 0 {
		return fmt.Errorf("transaction afterFunc errors: %w", errors.Join(errs...))
	}

	return nil
}

// mergeInto moves the registered after funcs of a successfully completed
// nested transaction into its outer transaction so that they are invoked
// once the outer transaction completes.
func (a *TxAppInfo) mergeInto(outer *TxAppInfo) {
	a.mu.Lock()
	defer a.mu.Unlock()

	outer.mu.Lock()
	defer outer.mu.Unlock()

	// the registered callbacks lazily access the parent app of the TxAppInfo
	// they were registered to (aka. the app instance to use after the transaction completes)
	// so rebind it (incl. the previously merged deeper levels) to the outer one
	a.parent = outer.parent
	for _, m := range a.merged {
		m.parent = outer.parent
	}

	outer.merged = append(outer.merged, a)
	outer.merged = append(outer.merged, a.merged...)
	outer.afterFuncs = append(outer.afterFuncs, a.afterFuncs...)

	a.merged = nil
	a.afterFuncs = nil
}
