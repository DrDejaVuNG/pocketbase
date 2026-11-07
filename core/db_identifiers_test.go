package core_test

import (
	"slices"
	"testing"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tests"
)

// PostgreSQL: raw SQL written for SQLite with unquoted camelCase names and a
// bare user column (see dbx.DB.QuoteIdentifiers).
func TestRawSQLIdentifierQuoting(t *testing.T) {
	t.Parallel()

	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	collection := core.NewBaseCollection("camelPosts")
	collection.Fields.Add(
		&core.TextField{Name: "authorId"},
		&core.BoolField{Name: "isPublished"},
		&core.NumberField{Name: "likeCount"},
		&core.TextField{Name: "user"},
	)
	if err := app.Save(collection); err != nil {
		t.Fatal(err)
	}
	for _, data := range []map[string]any{
		{"authorId": "a1", "isPublished": true, "likeCount": 3, "user": "u1"},
		{"authorId": "a1", "isPublished": false, "likeCount": 5, "user": "u2"},
		{"authorId": "a2", "isPublished": true, "likeCount": 1, "user": "u2"},
	} {
		record := core.NewRecord(collection)
		record.Load(data)
		if err := app.Save(record); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("NewExp", func(t *testing.T) {
		total, err := app.CountRecords(collection, dbx.NewExp("authorId = {:id} AND isPublished = true", dbx.Params{"id": "a1"}))
		if err != nil {
			t.Fatal(err)
		}
		if total != 1 {
			t.Fatalf("Expected 1 record, got %d", total)
		}
	})

	t.Run("Select and OrderBy with parentheses", func(t *testing.T) {
		var users []string
		err := app.DB().Select("user").From("camelPosts").OrderBy("(likeCount * 2) DESC").Column(&users)
		if err != nil {
			t.Fatal(err)
		}
		if expected := []string{"u2", "u1", "u2"}; !slices.Equal(users, expected) {
			t.Fatalf("Expected users %v, got %v", expected, users)
		}

		var sum int
		if err := app.DB().Select("COALESCE(SUM(likeCount), 0)::int").From("camelPosts").Row(&sum); err != nil {
			t.Fatal(err)
		}
		if sum != 9 {
			t.Fatalf("Expected sum 9, got %d", sum)
		}
	})

	t.Run("raw NewQuery with a mixed-case alias", func(t *testing.T) {
		var rows []struct {
			AuthorId   string `db:"authorId"`
			TotalLikes int    `db:"totalLikes"`
		}
		err := app.DB().NewQuery(`
			SELECT authorId, SUM(likeCount)::int AS totalLikes FROM camelPosts
			GROUP BY authorId ORDER BY authorId
		`).All(&rows)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 2 || rows[0].AuthorId != "a1" || rows[0].TotalLikes != 8 || rows[1].AuthorId != "a2" || rows[1].TotalLikes != 1 {
			t.Fatalf("Expected [{a1 8} {a2 1}], got %v", rows)
		}
	})

	t.Run("bare user column", func(t *testing.T) {
		// without quoting PostgreSQL reads user as CURRENT_USER and matches no row
		total, err := app.CountRecords(collection, dbx.NewExp("user = {:user}", dbx.Params{"user": "u2"}))
		if err != nil {
			t.Fatal(err)
		}
		if total != 2 {
			t.Fatalf("Expected 2 records, got %d", total)
		}
	})

	t.Run("view collection with a mixed-case alias", func(t *testing.T) {
		view := core.NewViewCollection("camelPostsView")
		view.ViewQuery = "SELECT id, authorId AS ownerId FROM camelPosts WHERE isPublished = true"
		if err := app.Save(view); err != nil {
			t.Fatal(err)
		}

		records, err := app.FindAllRecords(view, dbx.NewExp("ownerId = {:owner}", dbx.Params{"owner": "a2"}))
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 1 || records[0].GetString("ownerId") != "a2" {
			t.Fatalf("Expected 1 record with ownerId a2, got %v", records)
		}
	})
}

func TestRawSQLIdentifierQuotingConfig(t *testing.T) {
	scenarios := []struct {
		name     string
		env      string
		config   bool
		expected string
	}{
		{"default", "", false, "fooBar"},
		{"PB_PG_QUOTE_IDENTIFIERS=true", "true", false, "fooBar"},
		{"PB_PG_QUOTE_IDENTIFIERS=false", "false", false, "foobar"},
		{"DisableIdentifierQuoting config", "", true, "foobar"},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			t.Setenv("PB_PG_QUOTE_IDENTIFIERS", s.env)

			app, err := tests.NewTestAppWithConfig(core.BaseAppConfig{DisableIdentifierQuoting: s.config})
			if err != nil {
				t.Fatal(err)
			}
			defer app.Cleanup()

			for name, db := range map[string]dbx.Builder{"data": app.DB(), "aux": app.AuxDB()} {
				rows, err := db.NewQuery("SELECT 1 AS fooBar").Rows()
				if err != nil {
					t.Fatal(err)
				}
				columns, err := rows.Columns()
				rows.Close()
				if err != nil {
					t.Fatal(err)
				}
				if len(columns) != 1 || columns[0] != s.expected {
					t.Fatalf("[%s] Expected column %q, got %v", name, s.expected, columns)
				}
			}
		})
	}
}
