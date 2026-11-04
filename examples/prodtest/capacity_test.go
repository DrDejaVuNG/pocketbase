//go:build prodtest

package main

import (
	"fmt"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type loadResult struct {
	requests  int
	errors    int
	status    map[int]int
	sample    string
	latencies []time.Duration
	elapsed   time.Duration
}

func (r *loadResult) percentile(p float64) time.Duration {
	if len(r.latencies) == 0 {
		return 0
	}
	s := slices.Clone(r.latencies)
	slices.Sort(s)
	return s[int(float64(len(s)-1)*p)]
}

func (r *loadResult) String() string {
	return fmt.Sprintf("%d req in %s (%.0f req/s), %d errors %v, p50 %s p95 %s p99 %s",
		r.requests, r.elapsed.Round(time.Millisecond), float64(r.requests)/r.elapsed.Seconds(),
		r.errors, r.status, r.percentile(0.50).Round(time.Microsecond),
		r.percentile(0.95).Round(time.Microsecond), r.percentile(0.99).Round(time.Microsecond))
}

// runLoad runs workers concurrently for d; op returns the response of one request.
func runLoad(workers int, d time.Duration, op func(worker, i int) (apiResp, error)) *loadResult {
	res := &loadResult{status: map[int]int{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	deadline := time.Now().Add(d)
	start := time.Now()
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				t0 := time.Now()
				r, err := op(w, i)
				lat := time.Since(t0)
				mu.Lock()
				res.requests++
				res.latencies = append(res.latencies, lat)
				if err != nil || !r.ok() {
					res.errors++
					if err != nil {
						res.status[-1]++
						res.sample = err.Error()
					} else {
						res.status[r.Status]++
						res.sample = r.String()
					}
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	res.elapsed = time.Since(start)
	return res
}

// seedPosts inserts n posts for the org directly in SQL (load data only).
// Ids are explicit: the column default is covered by TestQuery/DefaultIdsMultiRowInsert.
func seedPosts(t *testing.T, f *fixture, org, author string, n int) {
	t.Helper()
	err := pg.exec(f.c.dataDB, `
		INSERT INTO posts (id, title, slug, status, tags, views, rating, featured, meta, org, author)
		SELECT 'l' || substr(md5($4 || g::text), 1, 14), 'load post ' || g, 'load-' || $4 || '-' || g,
			(ARRAY['draft','published','archived'])[1 + g % 3],
			'["go","pg"]'::jsonb, g % 1000, (g % 50) / 10.0, g % 7 = 0,
			jsonb_build_object('level', g % 20), $1, $2
		FROM generate_series(1, $3::int) g`, org, author, n, randHex(4))
	if err != nil {
		t.Fatal(err)
	}
}

// TestCapacity covers pool sizing and throughput.
func TestCapacity(t *testing.T) {
	t.Run("ConnectionBudget", func(t *testing.T) {
		// PostgreSQL defaults to max_connections=100 (97 for regular roles).
		// 3 nodes with the default pool sizes under a burst must not fail.
		if !pg.destructive() {
			t.Skip("needs the harness-managed PostgreSQL container")
		}
		f := dedicated(t, "conns", 3)
		seedPosts(t, f, f.orgA, f.alice.ID, 20_000)

		if err := pg.exec("postgres", fmt.Sprintf(`ALTER ROLE %s CONNECTION LIMIT 97`, pg.appRole)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			pg.exec("postgres", fmt.Sprintf(`ALTER ROLE %s CONNECTION LIMIT -1`, pg.appRole))
		})

		var peak atomic.Int64
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				case <-time.After(100 * time.Millisecond):
				}
				n, err := pg.queryInt("postgres", `SELECT COUNT(*) FROM pg_stat_activity WHERE usename = $1`, pg.appRole)
				if err == nil && n > peak.Load() {
					peak.Store(n)
				}
			}
		}()

		q := url.Values{"filter": {"title ~ '9' && views > 10"}, "sort": {"-views"}, "perPage": {"50"}}.Encode()
		res := runLoad(240, 10*time.Second, func(w, i int) (apiResp, error) {
			return call(f.node(w%3), f.alice.Token, "GET", "/api/collections/posts/records?"+q, nil)
		})
		close(stop)
		<-done

		t.Logf("burst with a 97 connection budget: %s; peak backends %d", res, peak.Load())
		if res.errors > 0 {
			t.Errorf("%d requests failed (e.g. %s)", res.errors, res.sample)
		}
	})

	t.Run("LatencyProfile", func(t *testing.T) {
		f := shared(t)
		seedPosts(t, f, f.orgB, f.carol.ID, 10_000)

		q := url.Values{"filter": {"status = 'published' && views > 500"}, "sort": {"-created"}, "perPage": {"30"}}.Encode()
		res := runLoad(32, 8*time.Second, func(w, i int) (apiResp, error) {
			base := f.node(w % 3)
			switch i % 4 {
			case 0:
				return call(base, f.carol.Token, "POST", "/api/collections/posts/records",
					postData("lat", f.orgB, f.carol, "views", i))
			default:
				return call(base, f.carol.Token, "GET", "/api/collections/posts/records?"+q, nil)
			}
		})
		t.Logf("mixed load (75%% list / 25%% create, 32 workers, 3 nodes): %s", res)
		if res.errors > 0 {
			t.Errorf("%d requests failed (e.g. %s)", res.errors, res.sample)
		}
	})

	t.Run("QueryExecModeCost", func(t *testing.T) {
		// default_query_exec_mode=describe_exec (fork default) vs pgx's
		// cache_statement on the same read workload, one node each.
		measure := func(prefix string, extraEnv ...string) *loadResult {
			f := dedicated(t, prefix, 1, extraEnv...)
			seedPosts(t, f, f.orgA, f.alice.ID, 10_000)
			q := url.Values{"filter": {"status = 'published' && views > 500"}, "sort": {"-created"}, "perPage": {"30"}}.Encode()
			// warm up
			runLoad(8, time.Second, func(w, i int) (apiResp, error) {
				return call(f.node(0), f.alice.Token, "GET", "/api/collections/posts/records?"+q, nil)
			})
			return runLoad(16, 6*time.Second, func(w, i int) (apiResp, error) {
				return call(f.node(0), f.alice.Token, "GET", "/api/collections/posts/records?"+q, nil)
			})
		}

		cached := *pg.appURL
		qs := cached.Query()
		qs.Set("default_query_exec_mode", "cache_statement")
		cached.RawQuery = qs.Encode()

		describe := measure("descexec")
		cache := measure("cachestmt", "POSTGRES_URL="+cached.String())
		t.Logf("describe_exec:   %s", describe)
		t.Logf("cache_statement: %s", cache)
		if describe.errors > 0 || cache.errors > 0 {
			t.Errorf("errors: describe_exec %d (%s), cache_statement %d (%s)",
				describe.errors, describe.sample, cache.errors, cache.sample)
		}
	})
}
