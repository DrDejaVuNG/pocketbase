package core

// PostgreSQL:
// runBestEffort executes fn whose error is expected to be only logged or ignored by the caller.
//
// Unlike SQLite, any failed statement in PostgreSQL aborts the whole transaction
// (25P02 for every following statement), so when app is part of a transaction
// fn is executed as nested transaction (SAVEPOINT) that is rolled back on error,
// leaving the outer transaction usable.
//
// When app is not transactional, fn is executed directly to avoid the extra roundtrips.
func runBestEffort(app App, fn func(txApp App) error) error {
	if app.IsTransactional() {
		return app.RunInTransaction(fn)
	}

	return fn(app)
}
