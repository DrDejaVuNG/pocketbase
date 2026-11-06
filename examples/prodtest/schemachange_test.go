//go:build prodtest

package main

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestSchemaChange covers tables that change while every node keeps serving
// (a rolling deploy whose new build migrates, dashboard edits, app.Save of a
// collection). The pooled connections cache statement descriptions (default
// query exec mode cache_describe); a statement cached before the change must
// not fail the requests.
func TestSchemaChange(t *testing.T) {
	const nodes, poolSize = 2, 20 // poolSize: default PB_DATA_MAX_OPEN_CONNS

	f := dedicated(t, "schema", nodes)
	seedPosts(t, f, f.orgA, f.alice.ID, 200)
	ids, err := pgStrings(f.c.dataDB, `SELECT id FROM posts WHERE org = $1 LIMIT 200`, f.orgA)
	if err != nil || len(ids) == 0 {
		t.Fatalf("seeded posts: %v (%d)", err, len(ids))
	}
	counter := "schema-" + randHex(3)
	if _, err := f.createRecord("counters", map[string]any{"name": counter, "n": 0}); err != nil {
		t.Fatal(err)
	}

	ops := []struct {
		name string
		do   func(base string, i int) (apiResp, error)
	}{
		{"list", func(base string, i int) (apiResp, error) {
			return call(base, f.alice.Token, "GET", "/api/collections/posts/records?perPage=20&sort=-created", nil)
		}},
		{"view", func(base string, i int) (apiResp, error) {
			return call(base, f.alice.Token, "GET", "/api/collections/posts/records/"+ids[i%len(ids)], nil)
		}},
		{"create", func(base string, i int) (apiResp, error) {
			return call(base, f.alice.Token, "POST", "/api/collections/posts/records", postData("schema", f.orgA, f.alice))
		}},
		{"batch", func(base string, i int) (apiResp, error) { // transaction, inserts only
			req := map[string]any{"method": "POST", "url": "/api/collections/posts/records", "body": postData("schema", f.orgA, f.alice)}
			return call(base, f.suToken, "POST", "/api/batch", map[string]any{"requests": []any{req, req}})
		}},
		{"counterTx", func(base string, i int) (apiResp, error) { // transaction that reads the table
			return call(base, f.suToken, "POST", "/api/prodtest/counters/"+counter+"/incr", nil)
		}},
	}

	// round sends every op from 40 clients per node and returns the failed
	// requests per op (with one sample response each)
	round := func() (map[string]int, map[string]string) {
		const workers, iterations = 40, 5
		failed := map[string]int{}
		samples := map[string]string{}
		var mu sync.Mutex
		var wg sync.WaitGroup
		for n := range nodes {
			for w := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for it := range iterations {
						for _, op := range ops {
							r, err := op.do(f.node(n), w*iterations+it)
							if err == nil && r.ok() {
								continue
							}
							mu.Lock()
							failed[op.name]++
							if samples[op.name] == "" {
								samples[op.name] = fmt.Sprintf("node%d %s %v", n+1, truncate(r.String(), 160), err)
							}
							mu.Unlock()
						}
					}
				}()
			}
		}
		wg.Wait()
		return failed, samples
	}

	requireClean := func(t *testing.T, label string, failed map[string]int, samples map[string]string, skip ...string) {
		t.Helper()
		names := make([]string, 0, len(failed))
		for name := range failed {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if !strings.Contains(strings.Join(skip, ","), name) {
				t.Errorf("%s: %d failed %s requests (e.g. %s)", label, failed[name], name, samples[name])
			}
		}
	}

	// fill the statement caches of the pooled connections
	failed, samples := round()
	requireClean(t, "warm up", failed, samples)

	t.Run("TableChangedOutsidePocketBase", func(t *testing.T) {
		// like `migrate up` from another process or psql: no instance reports it
		for _, table := range []string{"posts", "counters"} {
			if err := pg.exec(f.c.dataDB, `ALTER TABLE `+table+` ADD COLUMN "external" TEXT NOT NULL DEFAULT ''`); err != nil {
				t.Fatal(err)
			}
		}

		failed, samples := round()
		// a statement outside a transaction runs again with a new description
		requireClean(t, "after the change", failed, samples, "counterTx")
		// a transaction that reads the table fails at most once per pooled
		// connection that cached the statement (documented limit)
		if failed["counterTx"] > nodes*poolSize {
			t.Errorf("%d failed transactions, want at most one per pooled connection (%d)", failed["counterTx"], nodes*poolSize)
		}
		t.Logf("transactions that read the changed table: %d failed (limit %d)", failed["counterTx"], nodes*poolSize)

		failed, samples = round()
		requireClean(t, "next round", failed, samples)
	})

	t.Run("CollectionChangedThroughAPI", func(t *testing.T) {
		for _, c := range []struct{ collection, field string }{{"posts", "subtitle"}, {"counters", "note"}} {
			r := mustCall(t, f.node(0), f.suToken, "GET", "/api/collections/"+c.collection, nil, 200)
			var col struct {
				Fields []map[string]any `json:"fields"`
			}
			r.decode(t, &col)
			fields := append(col.Fields, map[string]any{"name": c.field, "type": "text"})
			mustCall(t, f.node(0), f.suToken, "PATCH", "/api/collections/"+c.collection, map[string]any{"fields": fields}, 200)
		}

		// node2 applied the bridge notification once it serves the new fields
		if _, err := eventually(propagationTimeout, 50*time.Millisecond, func() error {
			for _, check := range []struct{ path, token, key string }{
				{"/api/collections/posts/records/" + ids[0], f.alice.Token, `"subtitle"`},
				{"/api/collections/counters/records?filter=" + url.QueryEscape("name='"+counter+"'"), f.suToken, `"note"`},
			} {
				r, err := call(f.node(1), check.token, "GET", check.path, nil)
				if err != nil || !r.ok() || !strings.Contains(string(r.Body), check.key) {
					return fmt.Errorf("%s not served yet: %s %v", check.key, r, err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// the instances reset their statement caches, also for transactions
		failed, samples := round()
		requireClean(t, "after the change", failed, samples)
	})
}
