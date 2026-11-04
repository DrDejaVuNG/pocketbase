package issues

import (
	"testing"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketbase/pocketbase/tools/search"
)

// https://github.com/fondoger/pocketbase/issues/56
func TestIssue56_FilterByJsonField(t *testing.T) {
	app, _ := tests.NewTestApp()
	defer app.Cleanup()

	// Create a collection with a JSON field
	collection := core.NewBaseCollection("test_issue_56")
	collection.Fields.Add(&core.JSONField{
		Name: "json_column",
	})
	err := app.Save(collection)
	if err != nil {
		t.Fatal(err)
	}
	resolver := core.NewRecordFieldResolver(app, collection, nil, false)

	// Insert a record with a specific date
	record := core.NewRecord(collection)
	record.Set("json_column", map[string]interface{}{"strValue": "Hello, World!", "numValue": 42})
	err = app.Save(record)
	if err != nil {
		t.Fatal(err)
	}
	record2 := core.NewRecord(collection)
	record2.Set("json_column", map[string]interface{}{"strValue": "Another Record", "numValue": 100})
	err = app.Save(record2)
	if err != nil {
		t.Fatal(err)
	}

	scenarios := []struct {
		filter      string
		expectCount int
	}{
		{`json_column.numValue = 42`, 1},
		{`json_column.numValue > 50`, 1},
		{`json_column.strValue = "Hello, World!"`, 1},
		{`json_column.strValue != "Hello, World!"`, 1},
		// like operator is the only one that threw errors originally
		{`json_column.strValue ~ 'ello, Worl'`, 1},
		// the LIKE operand is the unquoted JSON string
		{`json_column.strValue ~ 'Hello%'`, 1},
		{`json_column.strValue !~ 'Hello%'`, 1},
	}

	for _, scenario := range scenarios {
		whereExpr, _ := search.FilterData(scenario.filter).BuildExpr(resolver)
		query := app.RecordQuery(collection).Select("COUNT(*) as count")
		_ = resolver.UpdateQuery(query)
		var count int
		err = query.AndWhere(whereExpr).Row(&count)
		if err != nil {
			t.Fatalf("%s: %v", scenario.filter, err)
		}

		if count != scenario.expectCount {
			t.Fatalf("%s: expected %d record, got %d", scenario.filter, scenario.expectCount, count)
		}
	}
}
