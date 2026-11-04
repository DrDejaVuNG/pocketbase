package core

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pocketbase/dbx"
)

// newTestDatabaseNames returns the connection url and the data and auxiliary
// database names that will be dropped on test cleanup.
//
// If createEmpty is false, the databases don't exist yet.
func newTestDatabaseNames(t *testing.T, createEmpty bool) (pgURL string, dataDB string, auxDB string) {
	t.Helper()

	base, cleanup := NewBaseAppForTest(BaseAppConfig{DataDir: t.TempDir()})
	t.Cleanup(cleanup)

	adminDB, err := dbx.MustOpen("pgx", base.config.PostgresURL)
	if err != nil {
		t.Fatal(err)
	}

	names := []string{base.config.PostgresDataDB, base.config.PostgresAuxDB}

	dropDatabases := func() {
		for _, name := range names {
			_, err := adminDB.NewQuery("DROP DATABASE IF EXISTS " + pgx.Identifier{name}.Sanitize() + " WITH (FORCE)").Execute()
			if err != nil {
				t.Errorf("failed to drop test database %q: %v", name, err)
			}
		}
	}

	if !createEmpty {
		dropDatabases()
	}

	t.Cleanup(func() {
		dropDatabases()
		adminDB.Close()
	})

	return base.config.PostgresURL, names[0], names[1]
}

func TestBaseAppBootstrapConcurrent(t *testing.T) {
	t.Run("not existing databases", func(t *testing.T) {
		testBaseAppBootstrapConcurrent(t, false)
	})

	t.Run("existing empty databases", func(t *testing.T) {
		testBaseAppBootstrapConcurrent(t, true)
	})
}

func testBaseAppBootstrapConcurrent(t *testing.T, createEmpty bool) {
	pgURL, dataDB, auxDB := newTestDatabaseNames(t, createEmpty)

	const totalApps = 4

	apps := make([]*BaseApp, totalApps)
	for i := range apps {
		apps[i] = NewBaseApp(BaseAppConfig{
			DataDir:        filepath.Join(t.TempDir(), "pb_data"),
			PostgresURL:    pgURL,
			PostgresDataDB: dataDB,
			PostgresAuxDB:  auxDB,
		})
	}
	t.Cleanup(func() {
		for _, app := range apps {
			app.ClearBootstrap()
		}
	})

	// simulate N nodes starting at the same time against the same new databases
	// (CREATE DATABASE, system migrations and initial settings insert races)
	errs := make([]error, totalApps)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, app := range apps {
		wg.Go(func() {
			<-start
			errs[i] = app.Bootstrap()
		})
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("[app %d] bootstrap failed: %v", i, err)
		}
	}
	if t.Failed() {
		return
	}

	var totalApplied int
	err := apps[0].DB().NewQuery("SELECT COUNT(*) FROM {{_migrations}}").Row(&totalApplied)
	if err != nil {
		t.Fatal(err)
	}
	if expected := len(SystemMigrations.Items()); totalApplied != expected {
		t.Fatalf("Expected %d applied system migrations, got %d", expected, totalApplied)
	}

	var totalSettings int
	err = apps[0].DB().Select("COUNT(*)").From((&Param{}).TableName()).Where(dbx.HashExp{"id": paramsKeySettings}).Row(&totalSettings)
	if err != nil {
		t.Fatal(err)
	}
	if totalSettings != 1 {
		t.Fatalf("Expected 1 settings param, got %d", totalSettings)
	}

	// the other nodes must be able to apply the (already applied) migrations again
	for i, app := range apps {
		if err := app.RunAllMigrations(); err != nil {
			t.Fatalf("[app %d] RunAllMigrations failed: %v", i, err)
		}
	}
}

func TestMigrationsLockSameDataAndAuxDatabase(t *testing.T) {
	pgURL, dataDB, _ := newTestDatabaseNames(t, true)

	// data and auxiliary db pointing to the same physical database
	newApp := func() *BaseApp {
		app := NewBaseApp(BaseAppConfig{
			DataDir:        t.TempDir(),
			PostgresURL:    pgURL,
			PostgresDataDB: dataDB,
			PostgresAuxDB:  dataDB,
		})
		if err := app.initDataDB(); err != nil {
			t.Fatal(err)
		}
		if err := app.initAuxDB(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			app.concurrentDB.(*dbx.DB).Close()
			app.auxConcurrentDB.(*dbx.DB).Close()
		})
		return app
	}

	app1 := newApp()
	app2 := newApp()

	app1Locked := make(chan struct{})
	app1Release := make(chan struct{})
	app1Done := make(chan error, 1)
	go func() {
		app1Done <- withMigrationsLock(app1, func() error {
			close(app1Locked)
			<-app1Release
			return nil
		})
	}()

	select {
	case <-app1Locked:
	case <-time.After(10 * time.Second):
		t.Fatal("the data and aux locks of the same database blocked each other")
	}

	app2Locked := make(chan struct{})
	app2Done := make(chan error, 1)
	go func() {
		app2Done <- withMigrationsLock(app2, func() error {
			close(app2Locked)
			return nil
		})
	}()

	select {
	case <-app2Locked:
		t.Fatal("app2 acquired the migrations lock while app1 was holding it")
	case <-time.After(300 * time.Millisecond):
	}

	close(app1Release)
	if err := <-app1Done; err != nil {
		t.Fatal(err)
	}

	select {
	case <-app2Locked:
	case <-time.After(10 * time.Second):
		t.Fatal("app2 didn't acquire the migrations lock after app1 released it")
	}
	if err := <-app2Done; err != nil {
		t.Fatal(err)
	}
}
