package search

import (
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/pocketbase/dbx"
)

// mapFieldResolver resolves the registered field names to raw SQL identifiers.
type mapFieldResolver map[string]string

func (m mapFieldResolver) UpdateQuery(query *dbx.SelectQuery) error {
	return nil
}

func (m mapFieldResolver) Resolve(field string) (*ResolverResult, error) {
	identifier, ok := m[field]
	if !ok {
		return nil, errors.New("unknown field " + field)
	}

	return &ResolverResult{Identifier: identifier}, nil
}

// TestFilterDataExecPostgresTypes executes filter expressions
// against a table with the production column types.
func TestFilterDataExecPostgresTypes(t *testing.T) {
	t.Parallel()

	db, cleanup := NewTestDBX()
	defer cleanup()

	_, err := db.NewQuery(`CREATE TABLE qf_items (
		name   TEXT DEFAULT '' NOT NULL,
		amount NUMERIC DEFAULT 0 NOT NULL,
		year   TEXT DEFAULT '' NOT NULL,
		meta   JSONB DEFAULT NULL,
		loc    JSONB DEFAULT '{"lon":0,"lat":0}' NOT NULL
	)`).Execute()
	if err != nil {
		t.Fatal(err)
	}

	currentYear := strconv.Itoa(time.Now().UTC().Year())

	rows := []dbx.Params{
		{"name": "Hello World", "amount": 10, "year": currentYear, "meta": `{"sku":"AB-1","level":5}`, "loc": `{"lon":23.3219,"lat":42.6977}`},
		{"name": "hello again", "amount": 0.5, "year": "2000", "meta": `{"sku":"CD-2","level":1}`, "loc": `{"lon":24.7453,"lat":42.1354}`},
		{"name": "Other", "amount": 100, "year": "2000", "meta": `{"level":"n/a"}`},
	}
	for _, row := range rows {
		if _, err := db.Insert("qf_items", row).Execute(); err != nil {
			t.Fatal(err)
		}
	}

	resolver := mapFieldResolver{
		"name":     "[[qf_items.name]]",
		"amount":   "[[qf_items.amount]]",
		"year":     "[[qf_items.year]]",
		"meta.sku": "([[qf_items.meta]]->'sku')::jsonb",
		"loc.lon":  "([[qf_items.loc]]->'lon')::jsonb",
		"loc.lat":  "([[qf_items.loc]]->'lat')::jsonb",
		"meta.lvl": "([[qf_items.meta]]->'level')::jsonb",
	}

	scenarios := []struct {
		filter FilterData
		params dbx.Params
		expect []string
	}{
		// LIKE must be case-insensitive (similar to SQLite)
		{"name ~ 'hello'", nil, []string{"Hello World", "hello again"}},
		{"name !~ 'HELLO'", nil, []string{"Other"}},
		{"name ?~ 'WORLD'", nil, []string{"Hello World"}},
		{"name ~ 'HELLO%'", nil, []string{"Hello World", "hello again"}},

		// LIKE on a jsonb value must compare the unquoted text
		{"meta.sku ~ 'AB%'", nil, []string{"Hello World"}},
		{"meta.sku !~ 'AB%'", nil, []string{"hello again"}},
		{"meta.sku ~ '-1'", nil, []string{"Hello World"}},

		// text params compared with number literals
		// (previously inferred as int4 -> "invalid input syntax for type integer")
		{"{:v} > 1", dbx.Params{"v": "1.5"}, []string{"Hello World", "hello again", "Other"}},
		{"{:v} > 2", dbx.Params{"v": "1.5"}, []string{}},
		{"{:v} = 1.5", dbx.Params{"v": "1.5"}, []string{"Hello World", "hello again", "Other"}},
		{"{:v} > 0", dbx.Params{"v": "abc"}, []string{"Hello World", "hello again", "Other"}},
		{"{:v} = 0", dbx.Params{"v": "abc"}, []string{}},
		{"amount > 0.5 && amount < 100", nil, []string{"Hello World"}},

		// numeric macros
		{"year = @year", nil, []string{"Hello World"}},
		{"@year > 2000", nil, []string{"Hello World", "hello again", "Other"}},
		{"@year < 2000", nil, []string{}},
		{"@month >= 1 && @month <= 12", nil, []string{"Hello World", "hello again", "Other"}},

		// geoDistance with jsonb arguments
		{"geoDistance(loc.lon, loc.lat, 23.32, 42.69) < 25", nil, []string{"Hello World"}},
		{"geoDistance(loc.lon, loc.lat, 23.32, 42.69) < 200", nil, []string{"Hello World", "hello again"}},
		{"geoDistance(meta.lvl, meta.lvl, 0, 0) < 0", nil, []string{}}, // non-numeric jsonb shouldn't fail the query
	}

	for _, s := range scenarios {
		t.Run(string(s.filter), func(t *testing.T) {
			var params []dbx.Params
			if s.params != nil {
				params = append(params, s.params)
			}

			expr, err := s.filter.BuildExpr(resolver, params...)
			if err != nil {
				t.Fatal(err)
			}

			names := []string{}
			err = db.Select("name").From("qf_items").AndWhere(expr).OrderBy("name ASC").Column(&names)
			if err != nil {
				t.Fatal(err)
			}

			expect := slices.Clone(s.expect)
			slices.Sort(expect)
			slices.Sort(names)

			if !slices.Equal(names, expect) {
				t.Fatalf("Expected %v, got %v", expect, names)
			}
		})
	}
}
