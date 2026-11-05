package core_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketbase/pocketbase/tools/search"
	"github.com/pocketbase/pocketbase/tools/security"
	"github.com/pocketbase/pocketbase/tools/types"
)

// qfFixture holds collections created with the production column types
// (NUMERIC, BOOLEAN, JSONB, TIMESTAMP) instead of the text based test fixtures.
type qfFixture struct {
	app      core.App
	authors  *core.Collection
	posts    *core.Collection
	comments *core.Collection
	view     *core.Collection
	ids      map[string]string // name/title -> id
}

func newQFFixture(t *testing.T) (*qfFixture, func()) {
	t.Helper()

	app, err := tests.NewTestApp()
	if err != nil {
		t.Fatal(err)
	}

	f := &qfFixture{app: app, ids: map[string]string{}}

	f.authors = core.NewBaseCollection("qf_authors")
	f.authors.ListRule = types.Pointer("verified = true")
	f.authors.Fields.Add(
		&core.TextField{Name: "name"},
		&core.BoolField{Name: "verified"},
	)
	if err := app.Save(f.authors); err != nil {
		t.Fatal(err)
	}

	f.posts = core.NewBaseCollection("qf_posts")
	f.posts.ListRule = types.Pointer("author.verified = true")
	f.posts.Fields.Add(
		&core.TextField{Name: "title"},
		&core.NumberField{Name: "amount"},
		&core.BoolField{Name: "featured"},
		&core.SelectField{Name: "tags", MaxSelect: 5, Values: []string{"go", "pg", "web", "user", "admin", "sys_x"}},
		&core.JSONField{Name: "meta"},
		&core.GeoPointField{Name: "loc"},
		&core.RelationField{Name: "author", CollectionId: f.authors.Id, MaxSelect: 1},
		&core.RelationField{Name: "owners", CollectionId: f.authors.Id, MaxSelect: 5},
		&core.AutodateField{Name: "created", OnCreate: true},
	)
	if err := app.Save(f.posts); err != nil {
		t.Fatal(err)
	}

	f.comments = core.NewBaseCollection("qf_comments")
	f.comments.ListRule = types.Pointer("post.author.verified = true")
	f.comments.Fields.Add(
		&core.RelationField{Name: "post", CollectionId: f.posts.Id, MaxSelect: 1},
		&core.TextField{Name: "body"},
	)
	if err := app.Save(f.comments); err != nil {
		t.Fatal(err)
	}

	f.view = core.NewViewCollection("qf_view")
	f.view.ListRule = types.Pointer("")
	f.view.ViewQuery = "SELECT id, title, json_build_array(title) AS j, author FROM qf_posts"
	if err := app.Save(f.view); err != nil {
		t.Fatal(err)
	}

	for _, a := range []struct {
		name     string
		verified bool
	}{{"Ann", true}, {"Zed", true}, {"Bob", false}} {
		r := core.NewRecord(f.authors)
		r.Set("name", a.name)
		r.Set("verified", a.verified)
		if err := app.Save(r); err != nil {
			t.Fatal(err)
		}
		f.ids[a.name] = r.Id
	}

	for _, p := range []struct {
		title  string
		amount float64
		tags   []string
		meta   any
		loc    types.GeoPoint
		author string
		owners []string
	}{
		{"Hello World", 10, []string{"go", "pg"}, map[string]any{"level": 5, "name": "alpha"}, types.GeoPoint{Lon: 23.3219, Lat: 42.6977}, "Zed", []string{"Ann", "Zed"}},
		{"hello again", 0.5, []string{"go"}, map[string]any{"level": 1, "sku": "AB-1"}, types.GeoPoint{Lon: 24.7453, Lat: 42.1354}, "Ann", []string{"Ann"}},
		{"Other", 100, []string{}, map[string]any{"level": 3, "sku": "CD-2"}, types.GeoPoint{}, "Bob", []string{"Bob", "Zed"}},
	} {
		r := core.NewRecord(f.posts)
		r.Set("title", p.title)
		r.Set("amount", p.amount)
		r.Set("tags", p.tags)
		r.Set("meta", p.meta)
		r.Set("loc", p.loc)
		r.Set("author", f.ids[p.author])
		owners := make([]string, 0, len(p.owners))
		for _, o := range p.owners {
			owners = append(owners, f.ids[o])
		}
		r.Set("owners", owners)
		if err := app.Save(r); err != nil {
			t.Fatal(err)
		}
		f.ids[p.title] = r.Id
	}

	for _, c := range []struct{ post, body string }{{"Hello World", "Great post"}, {"hello again", "meh"}} {
		r := core.NewRecord(f.comments)
		r.Set("post", f.ids[c.post])
		r.Set("body", c.body)
		if err := app.Save(r); err != nil {
			t.Fatal(err)
		}
	}

	return f, app.Cleanup
}

// list mimics the records list API (ListRule + client filter/sort + count).
func (f *qfFixture) list(c *core.Collection, superuser bool, filter, sort string) ([]string, int, error) {
	query := f.app.RecordQuery(c)

	resolver := core.NewRecordFieldResolver(f.app, c, &core.RequestInfo{}, true)

	if !superuser && c.ListRule != nil && *c.ListRule != "" {
		expr, err := search.FilterData(*c.ListRule).BuildExpr(resolver)
		if err != nil {
			return nil, 0, err
		}
		query.AndWhere(expr)
	}

	resolver.SetAllowHiddenFields(superuser)

	provider := search.NewProvider(resolver).Query(query)
	if !c.IsView() {
		provider.CountCol("ctid")
	}
	if filter != "" {
		provider.AddFilter(search.FilterData(filter))
	}
	if sort != "" {
		provider.Sort(search.ParseSortFromString(sort))
	}

	records := []*core.Record{}
	result, err := provider.Exec(&records)
	if err != nil {
		return nil, 0, err
	}

	titles := make([]string, 0, len(records))
	for _, r := range records {
		titles = append(titles, r.GetString("title")+r.GetString("body"))
	}

	return titles, result.TotalItems, nil
}

func sortedStrings(s []string) []string {
	s = slices.Clone(s)
	slices.Sort(s)
	return s
}

func TestRecordFieldResolverExecFilters(t *testing.T) {
	t.Parallel()

	f, cleanup := newQFFixture(t)
	defer cleanup()

	scenarios := []struct {
		collection *core.Collection
		superuser  bool
		filter     string
		expected   []string
	}{
		// case-insensitive LIKE (similar to SQLite)
		{f.posts, true, "title ~ 'hello'", []string{"Hello World", "hello again"}},
		{f.posts, true, "title !~ 'HELLO'", []string{"Other"}},
		{f.posts, true, "qf_comments_via_post.body ?~ 'GREAT'", []string{"Hello World"}},
		{f.posts, true, "@collection.qf_comments.post ?= id && @collection.qf_comments.body ?~ 'great'", []string{"Hello World"}},
		{f.posts, false, "qf_comments_via_post.body ?~ 'GREAT'", []string{"Hello World"}},

		// LIKE on JSON paths compares the unquoted text value
		{f.posts, true, "meta.sku ~ 'AB%'", []string{"hello again"}},
		{f.posts, true, "meta.sku !~ 'AB%'", []string{"Other"}},
		{f.posts, true, "meta.name ~ 'alp'", []string{"Hello World"}},
		{f.posts, true, "author.name !~ 'Z%'", []string{"hello again", "Other"}},

		// multi-value JSONB fields compared with plain strings (SQLite compares the raw json text)
		{f.posts, true, "tags ?= 'pg'", []string{}},
		{f.posts, true, "tags = 'go'", []string{}},
		{f.posts, true, "tags != 'go'", []string{"Hello World", "hello again", "Other"}},
		// (the above and below plain multi-value results were verified against upstream v0.40.6 on SQLite)
		{f.posts, true, "tags ?!= 'pg'", []string{"Hello World", "hello again", "Other"}},
		{f.posts, true, "owners = '" + f.ids["Ann"] + "'", []string{}},
		{f.posts, true, "owners ?= '" + f.ids["Ann"] + "'", []string{}},
		{f.posts, true, "owners != '" + f.ids["Ann"] + "'", []string{"Hello World", "hello again", "Other"}},
		{f.posts, true, "owners ~ '" + f.ids["Ann"] + "'", []string{"Hello World", "hello again"}},
		{f.posts, true, "owners.id ?= '" + f.ids["Ann"] + "'", []string{"Hello World", "hello again"}},
		{f.posts, true, "owners.id = '" + f.ids["Ann"] + "'", []string{"hello again"}},
		{f.posts, true, "owners:each ?= '" + f.ids["Ann"] + "'", []string{"Hello World", "hello again"}},
		{f.posts, true, "owners = 'x'", []string{}},
		{f.posts, true, "owners ?= 'x'", []string{}},
		{f.posts, true, "tags ~ 'pg'", []string{"Hello World"}},
		{f.posts, true, "tags = '[\"go\"]'", []string{"hello again"}},
		{f.posts, true, "tags = ''", []string{}},
		{f.posts, true, "tags:each ?= 'pg'", []string{"Hello World"}},
		{f.posts, true, "tags:length = 0", []string{"Other"}},

		// geoDistance with geoPoint field arguments
		{f.posts, true, "geoDistance(loc.lon, loc.lat, 23.32, 42.69) < 25", []string{"Hello World"}},
		{f.posts, true, "geoDistance(loc.lon, loc.lat, 23.32, 42.69) < 200", []string{"Hello World", "hello again"}},

		// numeric and macro comparisons
		{f.posts, true, "amount > 0.5 && amount <= 10", []string{"Hello World"}},
		{f.posts, true, "strftime('%Y', created) = @year", []string{"Hello World", "hello again", "Other"}},
		{f.posts, true, "title = @year", []string{}},
		{f.posts, true, "@month > 0", []string{"Hello World", "hello again", "Other"}},

		// view with a json column + relation filter (no DISTINCT over json)
		{f.view, true, "author.name = 'Zed'", []string{"Hello World"}},
		{f.view, false, "author.name ?~ 'a'", []string{"hello again"}},
	}

	for _, s := range scenarios {
		t.Run(s.collection.Name+"_"+s.filter, func(t *testing.T) {
			titles, total, err := f.list(s.collection, s.superuser, s.filter, "")
			if err != nil {
				t.Fatal(err)
			}

			if !slices.Equal(sortedStrings(titles), sortedStrings(s.expected)) {
				t.Fatalf("Expected %v, got %v", s.expected, titles)
			}

			if total != len(s.expected) {
				t.Fatalf("Expected totalItems %d, got %d", len(s.expected), total)
			}
		})
	}
}

func TestRecordFieldResolverExecSorts(t *testing.T) {
	t.Parallel()

	f, cleanup := newQFFixture(t)
	defer cleanup()

	scenarios := []struct {
		collection *core.Collection
		superuser  bool
		filter     string
		sort       string
		expected   []string // nil = only check the items count
		total      int
	}{
		// sorting by joined/computed expressions with ListRule joins
		{f.posts, false, "", "author.name", []string{"hello again", "Hello World"}, 2},
		{f.posts, false, "", "-author.name", []string{"Hello World", "hello again"}, 2},
		{f.posts, false, "", "@random", nil, 2},
		{f.posts, false, "", "meta.level", []string{"hello again", "Hello World"}, 2},
		{f.posts, false, "", "-meta.level", []string{"Hello World", "hello again"}, 2},
		{f.posts, false, "", "title:lower", []string{"hello again", "Hello World"}, 2},
		{f.posts, false, "", "-@rowid", nil, 2},
		{f.posts, false, "", "-owners.name,title", []string{"Hello World", "hello again"}, 2},
		{f.posts, false, "", "-featured,amount", []string{"hello again", "Hello World"}, 2},

		// multi-relation joins must not duplicate the rows or the count
		{f.posts, true, "owners.id != ''", "amount", []string{"hello again", "Hello World", "Other"}, 3},
		{f.posts, true, "owners.name ?= 'Zed'", "-owners.name,-amount", []string{"Other", "Hello World"}, 2},
		{f.posts, true, "owners.name ?!= 'x'", "@random", nil, 3},
		{f.posts, true, "", "author.name", []string{"hello again", "Other", "Hello World"}, 3},

		// collection with a nested ListRule join
		{f.comments, false, "", "@random", nil, 2},
		{f.comments, false, "", "-post.amount", []string{"Great post", "meh"}, 2},
		{f.comments, false, "", "post.author.name", []string{"meh", "Great post"}, 2},

		// view with a json column
		{f.view, true, "author.name != ''", "-author.name,title", []string{"Hello World", "Other", "hello again"}, 3},
		{f.view, true, "author.verified = true", "-author.name", []string{"Hello World", "hello again"}, 2},
		{f.view, true, "author.verified = true", "j,@random", nil, 2},
		{f.view, false, "", "j", nil, 3},
	}

	for _, s := range scenarios {
		t.Run(s.collection.Name+"_"+s.filter+"_"+s.sort, func(t *testing.T) {
			titles, total, err := f.list(s.collection, s.superuser, s.filter, s.sort)
			if err != nil {
				t.Fatal(err)
			}

			if total != s.total || len(titles) != s.total {
				t.Fatalf("Expected %d items (total %d), got %d (total %d): %v", s.total, s.total, len(titles), total, titles)
			}

			if s.expected != nil && !slices.Equal(titles, s.expected) {
				t.Fatalf("Expected %v, got %v", s.expected, titles)
			}
		})
	}
}

func TestRecordFieldResolverExecRequestRules(t *testing.T) {
	t.Parallel()

	f, cleanup := newQFFixture(t)
	defer cleanup()

	record, err := f.app.FindRecordById(f.posts, f.ids["Hello World"])
	if err != nil {
		t.Fatal(err)
	}

	scenarios := []struct {
		name     string
		rule     string
		info     *core.RequestInfo
		expected bool
	}{
		// numeric body params must not be truncated to int4
		{"amount -0.5", "@request.body.amount >= 0 && @request.body.amount <= 100", &core.RequestInfo{Body: map[string]any{"amount": -0.5}}, false},
		{"amount 100.9", "@request.body.amount >= 0 && @request.body.amount <= 100", &core.RequestInfo{Body: map[string]any{"amount": 100.9}}, false},
		{"amount 101", "@request.body.amount >= 0 && @request.body.amount <= 100", &core.RequestInfo{Body: map[string]any{"amount": 101.0}}, false},
		{"amount 0", "@request.body.amount >= 0 && @request.body.amount <= 100", &core.RequestInfo{Body: map[string]any{"amount": 0.0}}, true},
		{"amount 50.5", "@request.body.amount >= 0 && @request.body.amount <= 100", &core.RequestInfo{Body: map[string]any{"amount": 50.5}}, true},
		{"amount 100", "@request.body.amount >= 0 && @request.body.amount <= 100", &core.RequestInfo{Body: map[string]any{"amount": 100.0}}, true},
		{"amount multipart string", "@request.body.amount > 0.5", &core.RequestInfo{Body: map[string]any{"amount": "0.75"}}, true},
		{"qty 3.5 = 3", "@request.body.qty = 3", &core.RequestInfo{Body: map[string]any{"qty": 3.5}}, false},
		{"amount vs column", "@request.body.amount = amount", &core.RequestInfo{Body: map[string]any{"amount": 10.0}}, true},
		{"amount vs column (fraction)", "@request.body.amount < amount", &core.RequestInfo{Body: map[string]any{"amount": 9.9}}, true},
		{"int body value", "@request.body.n > 1.5", &core.RequestInfo{Body: map[string]any{"n": 2}}, true},
		{"bool body value vs number", "@request.body.flag = 1", &core.RequestInfo{Body: map[string]any{"flag": true}}, true},
		{"bool body value vs bool", "@request.body.flag = false", &core.RequestInfo{Body: map[string]any{"flag": true}}, false},
		{"numeric body value like", "@request.body.n ~ '5.5'", &core.RequestInfo{Body: map[string]any{"n": 15.5}}, true},

		// query/headers numbers are strings
		{"query fraction", "@request.query.page > 0", &core.RequestInfo{Query: map[string]string{"page": "1.5"}}, true},
		{"query fraction (lt)", "@request.query.page < 1", &core.RequestInfo{Query: map[string]string{"page": "1.5"}}, false},
		{"query non-numeric", "@request.query.page > 0", &core.RequestInfo{Query: map[string]string{"page": "abc"}}, true},
		{"header fraction", "@request.headers.x_num > 5", &core.RequestInfo{Headers: map[string]string{"x_num": "7.25"}}, true},
		{"header fraction equal", "@request.headers.x_num = 7.25", &core.RequestInfo{Headers: map[string]string{"x_num": "7.25"}}, true},

		// body :each must expand to text elements
		{"each user", "@request.body.tags:each != 'admin' && @request.body.tags:each !~ 'sys_%'", &core.RequestInfo{Body: map[string]any{"tags": []string{"user"}}}, true},
		{"each user,web", "@request.body.tags:each != 'admin' && @request.body.tags:each !~ 'sys_%'", &core.RequestInfo{Body: map[string]any{"tags": []string{"user", "web"}}}, true},
		{"each admin", "@request.body.tags:each != 'admin' && @request.body.tags:each !~ 'sys_%'", &core.RequestInfo{Body: map[string]any{"tags": []string{"admin"}}}, false},
		{"each user,sys_x", "@request.body.tags:each != 'admin' && @request.body.tags:each !~ 'sys_%'", &core.RequestInfo{Body: map[string]any{"tags": []string{"user", "sys_x"}}}, false},
		{"each any eq", "@request.body.tags:each ?= 'pg'", &core.RequestInfo{Body: map[string]any{"tags": []string{"go", "pg"}}}, true},
		{"each any eq (missing)", "@request.body.tags:each ?= 'web'", &core.RequestInfo{Body: map[string]any{"tags": []string{"go", "pg"}}}, false},
		{"each all eq", "@request.body.tags:each = 'go'", &core.RequestInfo{Body: map[string]any{"tags": []string{"go"}}}, true},
		{"each like", "@request.body.tags:each ~ 'g%'", &core.RequestInfo{Body: map[string]any{"tags": []string{"go", "gx"}}}, true},
		{"each vs relation column", "@request.body.owners:each ?= author", &core.RequestInfo{Body: map[string]any{"owners": []string{f.ids["Ann"], f.ids["Zed"]}}}, true},
		{"each vs relation column (missing)", "@request.body.owners:each ?= author", &core.RequestInfo{Body: map[string]any{"owners": []string{f.ids["Ann"]}}}, false},

		// :changed with multi-value and json fields
		{"tags unchanged", "@request.body.tags:changed = false", &core.RequestInfo{Body: map[string]any{"tags": []string{"go", "pg"}}}, true},
		{"tags changed", "@request.body.tags:changed = true", &core.RequestInfo{Body: map[string]any{"tags": []string{"go"}}}, true},

		// macros
		{"year", "strftime('%Y', created) = @year", &core.RequestInfo{}, true},
		{"title year", "title = @year", &core.RequestInfo{}, false},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			result, err := f.app.CanAccessRecord(record, s.info, &s.rule)
			if err != nil {
				t.Fatal(err)
			}

			if result != s.expected {
				t.Fatalf("Expected %v, got %v", s.expected, result)
			}
		})
	}
}

// ensure that the fixture uses the production column types
func TestRecordFieldResolverExecFixtureColumnTypes(t *testing.T) {
	t.Parallel()

	f, cleanup := newQFFixture(t)
	defer cleanup()

	rows := []struct {
		Column string `db:"column_name"`
		Type   string `db:"data_type"`
	}{}
	err := f.app.DB().NewQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'qf_posts'").All(&rows)
	if err != nil {
		t.Fatal(err)
	}

	types := map[string]string{}
	for _, r := range rows {
		types[r.Column] = strings.ToLower(r.Type)
	}

	expected := map[string]string{
		"amount":   "numeric",
		"featured": "boolean",
		"tags":     "jsonb",
		"owners":   "jsonb",
		"meta":     "jsonb",
		"loc":      "jsonb",
	}
	for col, typ := range expected {
		if types[col] != typ {
			t.Errorf("Expected column %q to be %q, got %q", col, typ, types[col])
		}
	}
}

// "@request.auth.*" single value fields are resolved as query params and
// columns of the same type are compared directly (PostgreSQL only); the
// results must stay the same as with the auth join and to_jsonb() comparisons,
// including the NULL values of missing relations.
func TestRecordFieldResolverExecAuthFieldsAndColumnComparisons(t *testing.T) {
	t.Parallel()

	app, err := tests.NewTestApp()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Cleanup()

	orgs := core.NewBaseCollection("ac_orgs")
	orgs.Fields.Add(&core.TextField{Name: "name"})
	if err := app.Save(orgs); err != nil {
		t.Fatal(err)
	}

	members := core.NewAuthCollection("ac_members")
	members.Fields.Add(
		&core.RelationField{Name: "org", CollectionId: orgs.Id, MaxSelect: 1},
		&core.TextField{Name: "secret", Hidden: true},
	)
	if err := app.Save(members); err != nil {
		t.Fatal(err)
	}

	docs := core.NewBaseCollection("ac_docs")
	docs.Fields.Add(
		&core.TextField{Name: "title"},
		&core.RelationField{Name: "org", CollectionId: orgs.Id, MaxSelect: 1},
		&core.RelationField{Name: "owner", CollectionId: orgs.Id, MaxSelect: 1},
	)
	if err := app.Save(docs); err != nil {
		t.Fatal(err)
	}

	newRecord := func(c *core.Collection, data map[string]any) *core.Record {
		r := core.NewRecord(c)
		r.Load(data)
		if c.IsAuth() {
			r.SetEmail(security.RandomString(10) + "@example.com")
			r.SetPassword("1234567890")
		}
		if err := app.Save(r); err != nil {
			t.Fatal(err)
		}
		return r
	}

	orgA := newRecord(orgs, map[string]any{"name": "a"})
	orgB := newRecord(orgs, map[string]any{"name": "b"})
	withOrg := newRecord(members, map[string]any{"org": orgA.Id, "secret": "s1"})
	withoutOrg := newRecord(members, map[string]any{})
	newRecord(docs, map[string]any{"title": "a", "org": orgA.Id, "owner": orgB.Id})
	newRecord(docs, map[string]any{"title": "x", "org": orgB.Id, "owner": orgB.Id})
	newRecord(docs, map[string]any{"title": "c"}) // missing org and owner (NULL joined columns)

	scenarios := []struct {
		name        string
		auth        *core.Record
		allowHidden bool
		filter      string
		expected    []string // nil = error
	}{
		{"auth relation", withOrg, true, "org = @request.auth.org", []string{"a"}},
		{"empty auth relation", withoutOrg, true, "org = @request.auth.org", []string{"c"}},
		{"auth relation path (join)", withOrg, true, "org.name = @request.auth.org.name", []string{"a"}},
		{"hidden auth field in a rule", withOrg, true, "@request.auth.secret = 's1'", []string{"a", "c", "x"}},
		{"hidden auth field in a client filter", withOrg, false, "@request.auth.secret = 's1'", nil},
		{"joined columns", withOrg, true, "org.name = owner.name", []string{"c", "x"}},
		{"joined columns (not equal)", withOrg, true, "org.name != owner.name", []string{"a"}},
		{"main vs joined column", withOrg, true, "title = org.name", []string{"a"}},
		{"main vs joined column (not equal)", withOrg, true, "title != org.name", []string{"c", "x"}},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			resolver := core.NewRecordFieldResolver(app, docs, &core.RequestInfo{Auth: s.auth}, s.allowHidden)

			provider := search.NewProvider(resolver).
				Query(app.RecordQuery(docs)).
				CountCol("ctid").
				AddFilter(search.FilterData(s.filter))

			records := []*core.Record{}
			result, err := provider.Exec(&records)
			if s.expected == nil {
				if err == nil {
					t.Fatalf("Expected error, got %d records", len(records))
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}

			titles := make([]string, 0, len(records))
			for _, r := range records {
				titles = append(titles, r.GetString("title"))
			}
			if got := sortedStrings(titles); !slices.Equal(got, s.expected) {
				t.Fatalf("Expected %v, got %v", s.expected, got)
			}
			if result.TotalItems != len(s.expected) {
				t.Fatalf("Expected total %d, got %d", len(s.expected), result.TotalItems)
			}
		})
	}
}
