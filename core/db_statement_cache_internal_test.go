package core

import "testing"

// A missed schema change inside a transaction leaves the other pooled
// connections with stale statement descriptions after the commit.
func TestIsSchemaChange(t *testing.T) {
	t.Parallel()

	scenarios := []struct {
		query    string
		expected bool
	}{
		{`ALTER TABLE "posts" ADD COLUMN "x" TEXT`, true},
		{`alter table posts drop column x`, true},
		{"  \n\tCREATE INDEX idx ON posts (x)", true},
		{`DROP VIEW IF EXISTS "post_stats"`, true},
		{"-- migration\nALTER TABLE posts ADD COLUMN x TEXT", true},
		{"/* generated */ CREATE TABLE t (id TEXT)", true},
		{"/* a */ -- b\n/* c */ DROP TABLE t", true},
		{`UPDATE "_params" SET "value" = $1 WHERE "id" = $2; ALTER TABLE t ADD COLUMN x TEXT`, true},
		{"CREATE\nTABLE t (id TEXT)", true},
		{"CREATE(", true}, // keyword followed by a non-identifier character

		{`SELECT "created" FROM "posts"`, false},
		{`INSERT INTO "posts" ("created") VALUES ($1)`, false},
		{`UPDATE "posts" SET "altered" = $1`, false},
		{"CREATED", false},
		{"dropped_rows", false},
		{"ALTER_x", false},
		{"-- ALTER TABLE t ADD COLUMN x TEXT\nSELECT 1", false},
		{"/* DROP TABLE t */ SELECT 1", false},
		{"/* unterminated DROP TABLE t", false},
		{"-- unterminated line comment", false},
		{"", false},
		{"SAVEPOINT pb_sp_1", false},
	}

	for _, s := range scenarios {
		t.Run(s.query, func(t *testing.T) {
			if got := isSchemaChange(s.query); got != s.expected {
				t.Fatalf("Expected %v, got %v", s.expected, got)
			}
		})
	}
}

func TestIsRollbackStatement(t *testing.T) {
	t.Parallel()

	scenarios := []struct {
		query    string
		expected bool
	}{
		{"ROLLBACK TO SAVEPOINT pb_sp_1", true},
		{"rollback", true},
		{"  -- undo\nROLLBACK TO SAVEPOINT pb_sp_2", true},
		{"RELEASE SAVEPOINT pb_sp_1", false},
		{"ROLLBACKS", false},
		{`SELECT "rollback" FROM t`, false},
	}

	for _, s := range scenarios {
		t.Run(s.query, func(t *testing.T) {
			if got := isRollbackStatement(s.query); got != s.expected {
				t.Fatalf("Expected %v, got %v", s.expected, got)
			}
		})
	}
}
