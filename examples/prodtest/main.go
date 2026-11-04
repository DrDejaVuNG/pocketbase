// Command prodtest is a PocketBase backend built on the PostgreSQL fork that
// models a small multi-tenant app (orgs, users, posts, comments, accounts).
//
// It is the system under test for the multi-node production scenarios in this
// directory (see README.md). It can also be run on its own:
//
//	POSTGRES_URL=postgres://... go run ./examples/prodtest serve
package main

import (
	"log"
	"os"

	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"

	_ "github.com/pocketbase/pocketbase/examples/prodtest/pbmigrations"
)

func main() {
	app := pocketbase.New()

	bindMailCapture(app)

	app.OnServe().BindFunc(func(se *core.ServeEvent) error {
		bindRoutes(se, os.Getenv("PRODTEST_NODE"))
		return se.Next()
	})

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}
