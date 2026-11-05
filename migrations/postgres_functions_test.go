package migrations_test

import (
	"os"
	"os/exec"
	"testing"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tests"
)

func TestRandomblobIsVolatile(t *testing.T) {
	t.Parallel()

	// existing databases (the fixtures were initialized with the old definitions)
	app, err := tests.NewTestApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Cleanup()

	dbs := map[string]dbx.Builder{"data": app.DB(), "aux": app.AuxDB()}
	for name, db := range dbs {
		var volatility string
		err := db.NewQuery("SELECT provolatile FROM pg_proc WHERE proname = 'randomblob'").Row(&volatility)
		if err != nil {
			t.Fatalf("[%s] %v", name, err)
		}
		if volatility != "v" {
			t.Fatalf("[%s] Expected randomblob to be VOLATILE, got %q", name, volatility)
		}

		var distinct int
		err = db.NewQuery("SELECT COUNT(DISTINCT lower(hex(randomblob(7)))) FROM generate_series(1, 5)").Row(&distinct)
		if err != nil {
			t.Fatalf("[%s] %v", name, err)
		}
		if distinct != 5 {
			t.Fatalf("[%s] Expected 5 distinct random values, got %d", name, distinct)
		}
	}

	// multi-row INSERT relying on the default record id
	collection := core.NewBaseCollection("test_multi_row_ids")
	collection.Fields.Add(&core.TextField{Name: "title"})
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}

	_, err = app.DB().NewQuery("INSERT INTO {{test_multi_row_ids}} ([[title]]) VALUES ('a'), ('b'), ('c')").Execute()
	if err != nil {
		t.Fatalf("Multi-row insert with default ids failed: %v", err)
	}

	var total int
	err = app.DB().NewQuery("SELECT COUNT(DISTINCT [[id]]) FROM {{test_multi_row_ids}}").Row(&total)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("Expected 3 distinct ids, got %d", total)
	}
}

func TestFunctionsInstalledWithForeignUuidGenerateV7(t *testing.T) {
	t.Parallel()

	app, cleanup := core.NewBaseAppForTest(core.BaseAppConfig{DataDir: t.TempDir()})
	defer cleanup()

	// e.g. created by the pg_uuidv7 extension in another schema
	for _, dbName := range []string{app.PostgresDataDB(), app.PostgresAuxDB()} {
		cmd := exec.Command("psql", "-h", "localhost", "-p", "5432", "-U", "user", "-d", dbName, "-v", "ON_ERROR_STOP=1", "-c",
			`CREATE SCHEMA ext; CREATE FUNCTION ext.uuid_generate_v7() RETURNS uuid LANGUAGE sql AS 'SELECT gen_random_uuid()';`,
		)
		// test-only credentials (see core.NewBaseAppForTest)
		cmd.Env = append(os.Environ(), "PGPASSWORD=pass")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("[%s] failed to create the foreign function: %v\n%s", dbName, err, out)
		}
	}

	if err := app.Bootstrap(); err != nil {
		t.Fatalf("Bootstrap failed: %v", err)
	}
	defer app.ClearBootstrap()

	for name, db := range map[string]dbx.Builder{"data": app.DB(), "aux": app.AuxDB()} {
		var total int
		err := db.NewQuery("SELECT COUNT(*) FROM pg_proc WHERE proname IN ('randomblob', 'strftime_impl') AND pronamespace = 'public'::regnamespace").Row(&total)
		if err != nil {
			t.Fatalf("[%s] %v", name, err)
		}
		if total != 2 {
			t.Fatalf("[%s] Expected the randomblob and strftime_impl functions to be installed, got %d", name, total)
		}
	}
}
