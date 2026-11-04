//go:build prodtest

package main

import (
	"net/url"
	"slices"
	"sort"
	"strings"
	"testing"
)

type seedPost struct {
	title, slug, status, body string
	tags                      []string
	views                     int
	rating                    float64
	featured                  bool
	meta                      any
	publishedAt               string
}

// querySeed is a small dataset whose filter results are known from the
// SQLite-based upstream PocketBase semantics.
var querySeed = []seedPost{
	{title: "Hello World", slug: "hello-world", status: "published", body: "<p>Intro</p>", tags: []string{"go", "pg"}, views: 120, rating: 4.5, featured: true, meta: map[string]any{"level": 5, "name": "alpha", "flag": true}, publishedAt: "2024-03-01 10:00:00.000Z"},
	{title: "hello again", status: "draft", tags: []string{"go"}, views: 3, rating: 1.25, meta: map[string]any{"level": 1, "name": "beta", "sku": "AB-1"}},
	{title: "100% Coverage", slug: "coverage", status: "published", tags: []string{"ops"}, views: 50, rating: 3, meta: map[string]any{"level": 7, "sku": "CD-2"}, publishedAt: "2025-01-01 00:00:00.000Z"},
	{title: "snake_case tips", slug: "snake", status: "archived", views: 0, rating: 0},
	{title: "snakeXcase", status: "published", tags: []string{"web", "sql", "pg"}, views: 9, rating: 2.5, featured: true, meta: map[string]any{"level": 3}, publishedAt: "2023-07-15 08:30:00.000Z"},
	{title: "Ünïcödé Straße", status: "draft", tags: []string{"web"}, views: 12, rating: 5, meta: map[string]any{"level": 12, "note": nil}},
}

type queryCase struct {
	filter string
	want   []string // expected titles (any order)
}

var queryCases = []queryCase{
	{`title = 'Hello World'`, []string{"Hello World"}},
	{`title ~ 'hello'`, []string{"Hello World", "hello again"}}, // LIKE is ASCII case-insensitive in SQLite
	{`title !~ 'HELLO'`, []string{"100% Coverage", "snake_case tips", "snakeXcase", "Ünïcödé Straße"}},
	{`title ~ 'snake_case'`, []string{"snake_case tips"}}, // "_" is escaped
	{`title ~ '100%'`, []string{"100% Coverage"}},         // explicit wildcard
	{`title:lower = 'hello world'`, []string{"Hello World"}},
	{`body ~ 'Intro'`, []string{"Hello World"}},
	{`views > 10 && rating >= 2.5`, []string{"Hello World", "100% Coverage", "Ünïcödé Straße"}},
	{`views = 12`, []string{"Ünïcödé Straße"}},
	{`views = '12'`, []string{"Ünïcödé Straße"}},
	{`rating < 2`, []string{"hello again", "snake_case tips"}},
	{`featured = true`, []string{"Hello World", "snakeXcase"}},
	{`featured != true`, []string{"hello again", "100% Coverage", "snake_case tips", "Ünïcödé Straße"}},
	{`(featured = true || views > 100) && status = 'published'`, []string{"Hello World", "snakeXcase"}},
	{`slug = ''`, []string{"hello again", "snakeXcase", "Ünïcödé Straße"}},
	{`tags ?= 'pg'`, []string{"Hello World", "snakeXcase"}},
	{`tags ?= 'go' && tags ?= 'pg'`, []string{"Hello World"}},
	{`tags:length > 1`, []string{"Hello World", "snakeXcase"}},
	{`tags:length = 0`, []string{"snake_case tips"}},
	{`tags ~ 'sql'`, []string{"snakeXcase"}},
	{`meta.level > 3`, []string{"Hello World", "100% Coverage", "Ünïcödé Straße"}},
	{`meta.name = 'alpha'`, []string{"Hello World"}},
	{`meta.flag = true`, []string{"Hello World"}},
	{`meta.sku ~ 'AB%'`, []string{"hello again"}},
	{`meta.level = null`, []string{"snake_case tips"}},
	{`publishedAt = ''`, []string{"hello again", "snake_case tips", "Ünïcödé Straße"}},
	{`publishedAt != ''`, []string{"Hello World", "100% Coverage", "snakeXcase"}},
	{`publishedAt > '2024-01-01 00:00:00.000Z'`, []string{"Hello World", "100% Coverage"}},
	{`publishedAt < '2024-01-01 00:00:00.000Z'`, []string{"snakeXcase", "hello again", "snake_case tips", "Ünïcödé Straße"}}, // '' < any date
	{`strftime('%Y', publishedAt) = '2025'`, []string{"100% Coverage"}},
	{`created >= @todayStart`, nil}, // nil = all seeded posts
	{`author.name = 'Quinn'`, nil},
	{`author.org.name ~ 'Org Q'`, nil},
	{`@collection.comments.post ?= id && @collection.comments.body ?~ 'great'`, []string{"Hello World"}},
	{`comments_via_post.body ?~ 'GREAT'`, []string{"Hello World"}},
}

// TestQuery checks filter/sort semantics against a schema created through
// the app (production column types: NUMERIC, JSONB, TIMESTAMP, BOOLEAN).
func TestQuery(t *testing.T) {
	f := shared(t)

	orgQ, err := f.createRecord("orgs", map[string]any{"name": "Org Q " + f.run})
	if err != nil {
		t.Fatal(err)
	}
	quinn, err := f.createUser("quinn", orgQ)
	if err != nil {
		t.Fatal(err)
	}
	mustCall(t, f.node(0), f.suToken, "PATCH", "/api/collections/users/records/"+quinn.ID,
		map[string]any{"name": "Quinn"}, 200)

	ids := map[string]string{}
	allTitles := []string{}
	for i, p := range querySeed {
		body := map[string]any{
			"title": p.title, "slug": p.slug, "status": p.status, "body": p.body,
			"tags": p.tags, "views": p.views, "rating": p.rating, "featured": p.featured,
			"meta": p.meta, "publishedAt": p.publishedAt, "org": orgQ, "author": quinn.ID,
		}
		r := mustCall(t, f.node(i%3), quinn.Token, "POST", "/api/collections/posts/records", body, 200)
		var rec struct{ ID string }
		r.decode(t, &rec)
		ids[p.title] = rec.ID
		allTitles = append(allTitles, p.title)
	}
	mustCall(t, f.node(1), quinn.Token, "POST", "/api/collections/comments/records",
		map[string]any{"post": ids["Hello World"], "author": quinn.ID, "body": "Great post"}, 200)

	list := func(params url.Values) apiResp {
		params.Set("perPage", "100")
		r, err := call(f.node(0), quinn.Token, "GET", "/api/collections/posts/records?"+params.Encode(), nil)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	titlesOf := func(t *testing.T, r apiResp) []string {
		var res struct {
			Items []struct{ Title string } `json:"items"`
		}
		r.decode(t, &res)
		out := []string{}
		for _, it := range res.Items {
			out = append(out, it.Title)
		}
		return out
	}

	t.Run("Filters", func(t *testing.T) {
		for _, tc := range queryCases {
			want := tc.want
			if want == nil {
				want = allTitles
			}
			r := list(url.Values{"filter": {tc.filter}})
			if r.Status != 200 {
				t.Errorf("%s\n    -> %s", tc.filter, r)
				continue
			}
			got := titlesOf(t, r)
			if !sameSet(got, want) {
				t.Errorf("%s\n    got  %q\n    want %q", tc.filter, sorted(got), sorted(want))
			}
		}
	})

	t.Run("Sorts", func(t *testing.T) {
		cases := []struct {
			sort  string
			first string // expected first title ("" = only check the status)
		}{
			{"-views,title", "Hello World"},
			{"title", "100% Coverage"},
			{"-publishedAt", "100% Coverage"}, // empty dates sort first ascending in SQLite, so last descending
			{"author.name,-views", "Hello World"},
			{"@random", ""},
			{"-@rowid", ""},
			{"meta.level", ""},
			{"title:lower", ""},
		}
		for _, tc := range cases {
			r := list(url.Values{"sort": {tc.sort}})
			if r.Status != 200 {
				t.Errorf("sort=%s -> %s", tc.sort, r)
				continue
			}
			got := titlesOf(t, r)
			if len(got) != len(allTitles) {
				t.Errorf("sort=%s returned %d items, want %d", tc.sort, len(got), len(allTitles))
				continue
			}
			if tc.first != "" && got[0] != tc.first {
				t.Errorf("sort=%s: first %q, want %q (order %q)", tc.sort, got[0], tc.first, got)
			}
		}

		// sorting a collection whose list rule joins another table
		r, err := call(f.node(0), quinn.Token, "GET", "/api/collections/comments/records?sort=@random", nil)
		if err != nil || r.Status != 200 {
			t.Errorf("comments sort=@random (rule with a join): %v %s", err, r)
		}
		r, err = call(f.node(0), quinn.Token, "GET", "/api/collections/comments/records?sort=-post.views", nil)
		if err != nil || r.Status != 200 {
			t.Errorf("comments sort=-post.views: %v %s", err, r)
		}
	})

	t.Run("PaginationAndProjection", func(t *testing.T) {
		r := list(url.Values{"sort": {"-views,title"}, "fields": {"id,title"}, "expand": {"author"}})
		if r.Status != 200 {
			t.Fatal(r)
		}
		if strings.Contains(string(r.Body), `"views"`) {
			t.Errorf("fields=id,title returned other fields")
		}

		r = mustCall(t, f.node(1), quinn.Token, "GET",
			"/api/collections/posts/records?sort=-views,title&perPage=2&page=2&expand=author,org", nil, 200)
		var page struct {
			TotalItems int
			Items      []struct {
				Title  string
				Expand map[string]map[string]any
			}
		}
		r.decode(t, &page)
		if page.TotalItems != len(allTitles) {
			t.Errorf("totalItems %d, want %d", page.TotalItems, len(allTitles))
		}
		if len(page.Items) != 2 || page.Items[0].Title != "Ünïcödé Straße" {
			t.Errorf("page 2 = %+v", page.Items)
		}
		for _, it := range page.Items {
			if it.Expand["author"]["name"] != "Quinn" || it.Expand["org"]["id"] != orgQ {
				t.Errorf("expand missing on %q: %v", it.Title, it.Expand)
			}
		}

		r = mustCall(t, f.node(2), quinn.Token, "GET", "/api/collections/posts/records?skipTotal=1", nil, 200)
		if !strings.Contains(string(r.Body), `"totalItems":-1`) {
			t.Errorf("skipTotal=1 did not skip the count: %s", truncate(string(r.Body), 200))
		}
	})

	t.Run("ViewCollection", func(t *testing.T) {
		r := mustCall(t, f.node(2), quinn.Token, "GET", "/api/collections/post_stats/records", nil, 200)
		var res struct {
			Items []struct {
				ID         string  `json:"id"`
				Total      float64 `json:"total"`
				TotalViews float64 `json:"totalViews"`
			}
		}
		r.decode(t, &res)
		if len(res.Items) != 1 || res.Items[0].ID != orgQ || int(res.Items[0].Total) != len(querySeed) || int(res.Items[0].TotalViews) != 194 {
			t.Errorf("post_stats = %+v", res.Items)
		}
	})

	t.Run("DefaultIdsMultiRowInsert", func(t *testing.T) {
		// SQL migrations and the superuser SQL console insert several rows
		// in one statement and rely on the id column default.
		err := pg.exec(f.c.dataDB, `INSERT INTO accounts (owner, balance) SELECT 'bulk-' || g, g FROM generate_series(1, 3) g`)
		if err != nil {
			t.Fatalf("multi-row insert with default ids: %v", err)
		}
	})

	t.Run("NumericRuleBoundaries", func(t *testing.T) {
		name := "payments_" + randHex(3)
		f.createCollection(t, map[string]any{
			"name":       name,
			"type":       "base",
			"createRule": "@request.body.amount >= 0 && @request.body.amount <= 100",
			"fields":     []map[string]any{{"name": "amount", "type": "number"}},
		})
		cases := map[float64]int{-0.5: 400, 100.9: 400, 0: 200, 50.5: 200, 100: 200, 101: 400}
		for amount, want := range cases {
			r, err := call(f.node(1), "", "POST", "/api/collections/"+name+"/records", map[string]any{"amount": amount})
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != want {
				t.Errorf("amount %v: want %d, got %s", amount, want, r)
			}
		}
	})

	t.Run("BodyEachRule", func(t *testing.T) {
		name := "tagged_" + randHex(3)
		f.createCollection(t, map[string]any{
			"name":       name,
			"type":       "base",
			"createRule": "@request.body.tags:each != 'admin' && @request.body.tags:each !~ 'sys_%'",
			"fields": []map[string]any{{
				"name": "tags", "type": "select", "maxSelect": 4,
				"values": []string{"user", "admin", "sys_x", "editor"},
			}},
		})
		cases := []struct {
			tags []string
			want int
		}{
			{[]string{"user"}, 200},
			{[]string{"user", "editor"}, 200},
			{[]string{"admin"}, 400},
			{[]string{"user", "sys_x"}, 400},
		}
		for _, tc := range cases {
			r, err := call(f.node(2), "", "POST", "/api/collections/"+name+"/records", map[string]any{"tags": tc.tags})
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != tc.want {
				t.Errorf("tags %v: want %d, got %s", tc.tags, tc.want, r)
			}
		}
	})

	t.Run("GeoDistance", func(t *testing.T) {
		name := "places_" + randHex(3)
		f.createCollection(t, map[string]any{
			"name":       name,
			"type":       "base",
			"listRule":   "",
			"createRule": "",
			"fields":     []map[string]any{{"name": "name", "type": "text"}, {"name": "loc", "type": "geoPoint"}},
		})
		for _, p := range []map[string]any{
			{"name": "Sofia", "loc": map[string]any{"lon": 23.3219, "lat": 42.6977}},
			{"name": "Plovdiv", "loc": map[string]any{"lon": 24.7453, "lat": 42.1354}},
		} {
			mustCall(t, f.node(1), "", "POST", "/api/collections/"+name+"/records", p, 200)
		}
		filter := url.QueryEscape("geoDistance(loc.lon, loc.lat, 23.32, 42.69) < 25")
		r := mustCall(t, f.node(2), "", "GET", "/api/collections/"+name+"/records?filter="+filter, nil, 200)
		var res struct{ TotalItems int }
		r.decode(t, &res)
		if res.TotalItems != 1 {
			t.Errorf("geoDistance matched %d places, want 1", res.TotalItems)
		}
	})
}

func sorted(s []string) []string {
	c := slices.Clone(s)
	sort.Strings(c)
	return c
}

func sameSet(a, b []string) bool {
	return slices.Equal(sorted(a), sorted(b))
}
