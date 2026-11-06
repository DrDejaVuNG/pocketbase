//go:build prodtest

package main

// TestPerf is a throughput benchmark (not part of the scenario suite): it seeds
// 20k posts and 20k comments in one org and runs every workload (list, list
// without total, list with a relation rule, view, create, update, delete,
// transactional counter, 3-record batch, mixed) against a dedicated cluster,
// printing one "PERF" line per workload: req/s, p50/p99 latency, errors, SQL
// statements and DB time per request (pg_stat_statements), CPU per request of
// the nodes (appCPU) and of the PostgreSQL container (pgCPU), commits,
// advisory lock waiters, new PostgreSQL sessions and pool waits. On a shared
// machine req/s varies between runs; the CPU per request is the stable number.
//
// It runs only with PRODTEST_PERF=1. Compare builds with PRODTEST_BIN and
// PRODTEST_POSTGRES_DURABLE=1 (production-like fsync settings), eg.:
//
//	PRODTEST_PERF=1 PRODTEST_POSTGRES_DURABLE=1 go test -tags prodtest -count=1 -run 'TestPerf$' -v .
//
// PERF_LABEL      label printed with every result line
// PERF_NODE_ENV   ";"-separated extra env for every node (eg. "PB_TX_SERIALIZE=false")
// PERF_NODES      node count (default 3)
// PERF_WORKERS    concurrent clients (default 48)
// PERF_DURATION   seconds per workload (default 6)
// PERF_WORKLOADS  ","-separated subset of the workloads
// PERF_EXEC_MODE  pgx default_query_exec_mode for the nodes (eg. "cache_statement")
// PERF_URL_PARAMS extra connection URL params for the nodes (eg. "plan_cache_mode=force_custom_plan")
// PERF_SETUP_SQL  extra SQL executed after seeding (eg. "CREATE INDEX ...")
// PERF_TOP        print the most frequent statements ("full" = untruncated SQL)
// PERF_PPROF      file prefix; CPU-profiles node1 during every workload

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

type perfOp func(worker, i int) (apiResp, error)

func TestPerf(t *testing.T) {
	if os.Getenv("PRODTEST_PERF") == "" {
		t.Skip("throughput benchmark; set PRODTEST_PERF=1 to run it")
	}

	label := os.Getenv("PERF_LABEL")
	nodes := envInt("PERF_NODES", 3)
	workers := envInt("PERF_WORKERS", 48)
	dur := time.Duration(envInt("PERF_DURATION", 6)) * time.Second
	var nodeEnv []string
	if v := os.Getenv("PERF_NODE_ENV"); v != "" {
		nodeEnv = strings.Split(v, ";")
	}
	only := map[string]bool{}
	for _, w := range strings.Split(os.Getenv("PERF_WORKLOADS"), ",") {
		if w != "" {
			only[w] = true
		}
	}

	if mode, params := os.Getenv("PERF_EXEC_MODE"), os.Getenv("PERF_URL_PARAMS"); mode != "" || params != "" {
		u := *pg.appURL
		q := u.Query()
		if mode != "" {
			q.Set("default_query_exec_mode", mode)
		}
		extra, err := url.ParseQuery(params)
		if err != nil {
			t.Fatalf("PERF_URL_PARAMS: %v", err)
		}
		for k, v := range extra {
			q[k] = v
		}
		u.RawQuery = q.Encode()
		nodeEnv = append(nodeEnv, "POSTGRES_URL="+u.String())
	}

	f := dedicated(t, "perf", nodes, nodeEnv...)

	pprofAddr := ""
	if os.Getenv("PERF_PPROF") != "" {
		pprofAddr = freeAddr()
		n := f.c.nodes[0]
		n.stop()
		n.env = append(n.env, "PRODTEST_PPROF_ADDR="+pprofAddr)
		if err := n.start(); err != nil {
			t.Fatal(err)
		}
		if err := n.waitHealthy(60 * time.Second); err != nil {
			t.Fatal(err)
		}
	}

	// ---- seed ----
	seedPosts(t, f, f.orgA, f.alice.ID, 20_000)
	salt := randHex(4)
	if err := pg.exec(f.c.dataDB, `
		WITH p AS (SELECT array_agg(id) a FROM (SELECT id FROM posts WHERE org = $1 LIMIT 1000) s)
		INSERT INTO comments (id, post, author, body)
		SELECT 'c' || substr(md5($3 || g::text), 1, 14), p.a[1 + g % 1000], $2, 'comment ' || g
		FROM generate_series(1, 20000) g, p`, f.orgA, f.bob.ID, salt); err != nil {
		t.Fatal(err)
	}
	postIDs, err := pgStrings(f.c.dataDB, `SELECT id FROM posts WHERE org = $1 LIMIT 5000`, f.orgA)
	if err != nil {
		t.Fatal(err)
	}
	// per worker deletable posts
	delIDs := make([][]string, workers)
	all := []string{}
	for w := range workers {
		for i := range 600 {
			id := fmt.Sprintf("d%03d%010d", w, i)
			delIDs[w] = append(delIDs[w], id)
			all = append(all, id)
		}
	}
	if err := pg.exec(f.c.dataDB, `
		INSERT INTO posts (id, title, org, author) SELECT x, 'del', $2, $3 FROM unnest($1::text[]) x`,
		all, f.orgA, f.alice.ID); err != nil {
		t.Fatal(err)
	}
	for w := range workers {
		if _, err := f.createRecord("counters", map[string]any{"name": fmt.Sprintf("perf-%d", w), "n": 0}); err != nil {
			t.Fatal(err)
		}
	}
	if s := os.Getenv("PERF_SETUP_SQL"); s != "" {
		if err := pg.exec(f.c.dataDB, s); err != nil {
			t.Fatal(err)
		}
	}
	pg.exec(f.c.dataDB, `ANALYZE`)

	base := func(w int) string { return f.node(w % nodes) }
	listQ := url.Values{"filter": {"status = 'published'"}, "sort": {"-created"}, "perPage": {"30"}}.Encode()
	var delPos = make([]atomic.Int64, workers)

	workloads := []struct {
		name string
		op   perfOp
	}{
		{"list", func(w, i int) (apiResp, error) {
			return call(base(w), f.alice.Token, "GET", "/api/collections/posts/records?"+listQ, nil)
		}},
		{"listSkipTotal", func(w, i int) (apiResp, error) {
			return call(base(w), f.alice.Token, "GET", "/api/collections/posts/records?skipTotal=1&"+listQ, nil)
		}},
		{"listJoin", func(w, i int) (apiResp, error) {
			return call(base(w), f.bob.Token, "GET", "/api/collections/comments/records?sort=-created&perPage=30", nil)
		}},
		{"view", func(w, i int) (apiResp, error) {
			return call(base(w), f.alice.Token, "GET", "/api/collections/posts/records/"+postIDs[rand.IntN(len(postIDs))], nil)
		}},
		{"create", func(w, i int) (apiResp, error) {
			return call(base(w), f.alice.Token, "POST", "/api/collections/posts/records", postData("perf", f.orgA, f.alice))
		}},
		{"update", func(w, i int) (apiResp, error) {
			id := postIDs[(w*97+i)%len(postIDs)]
			return call(base(w), f.alice.Token, "PATCH", "/api/collections/posts/records/"+id, map[string]any{"views": i})
		}},
		{"delete", func(w, i int) (apiResp, error) {
			k := delPos[w].Add(1) - 1
			if int(k) >= len(delIDs[w]) {
				return apiResp{Status: 599, Body: []byte("out of ids")}, nil
			}
			return call(base(w), f.suToken, "DELETE", "/api/collections/posts/records/"+delIDs[w][k], nil)
		}},
		{"txIncr", func(w, i int) (apiResp, error) {
			return call(base(w), f.suToken, "POST", fmt.Sprintf("/api/prodtest/counters/perf-%d/incr", w), nil)
		}},
		{"batch3", func(w, i int) (apiResp, error) {
			req := map[string]any{"method": "POST", "url": "/api/collections/accounts/records", "body": map[string]any{"owner": "perf", "balance": 1}}
			return call(base(w), f.suToken, "POST", "/api/batch", map[string]any{"requests": []any{req, req, req}})
		}},
		{"mixed", func(w, i int) (apiResp, error) {
			switch i % 10 {
			case 0:
				k := delPos[w].Add(1) - 1
				if int(k) >= len(delIDs[w]) {
					return call(base(w), f.alice.Token, "GET", "/api/collections/posts/records?"+listQ, nil)
				}
				return call(base(w), f.suToken, "DELETE", "/api/collections/posts/records/"+delIDs[w][k], nil)
			case 1, 2:
				return call(base(w), f.alice.Token, "POST", "/api/collections/posts/records", postData("perf", f.orgA, f.alice))
			default:
				return call(base(w), f.alice.Token, "GET", "/api/collections/posts/records?"+listQ, nil)
			}
		}},
	}

	for _, wl := range workloads {
		if len(only) > 0 && !only[wl.name] {
			continue
		}
		runLoad(workers, time.Second, wl.op) // warm up
		statsBefore := f.dbStats(nodes)
		appCPU0, pgCPU0 := f.c.appCPU(), pgCPU()
		pg.exec("postgres", `SELECT pg_stat_statements_reset()`)
		commits0, _ := pg.queryInt("postgres", `SELECT xact_commit FROM pg_stat_database WHERE datname = $1`, f.c.dataDB)
		sessions0, _ := pg.queryInt("postgres", `SELECT sessions FROM pg_stat_database WHERE datname = $1`, f.c.dataDB)

		var lockWaiters, lockSamples atomic.Int64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case <-time.After(50 * time.Millisecond):
				}
				n, err := pg.queryInt("postgres", `SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`)
				if err == nil {
					lockWaiters.Add(n)
					lockSamples.Add(1)
				}
			}
		}()
		profDone := make(chan string, 1)
		if pprofAddr != "" {
			go func() { profDone <- captureProfile(pprofAddr, dur, os.Getenv("PERF_PPROF")+"-"+wl.name+".pprof") }()
		}

		res := runLoad(workers, dur, wl.op)
		close(stop)
		wg.Wait()
		if pprofAddr != "" {
			if msg := <-profDone; msg != "" {
				t.Log(msg)
			}
		}

		appCPU1, pgCPU1 := f.c.appCPU(), pgCPU()
		commits1, _ := pg.queryInt("postgres", `SELECT xact_commit FROM pg_stat_database WHERE datname = $1`, f.c.dataDB)
		sessions1, _ := pg.queryInt("postgres", `SELECT sessions FROM pg_stat_database WHERE datname = $1`, f.c.dataDB)
		statsAfter := f.dbStats(nodes)
		calls, execMs, top := stmtStats(f.c.dataDB)
		ok := res.requests - res.errors
		avgWaiters := 0.0
		if lockSamples.Load() > 0 {
			avgWaiters = float64(lockWaiters.Load()) / float64(lockSamples.Load())
		}
		fmt.Printf("PERF %-10s %-14s %7.0f req/s  p50 %7.2fms  p99 %7.2fms  err %d  stmts/req %5.1f  dbms/req %6.3f  appCPU %6.3fms/req  pgCPU %6.3fms/req  commits/req %4.1f  lockWaiters %5.1f  newConns %d  poolWait %s\n",
			label, wl.name, float64(res.requests)/res.elapsed.Seconds(),
			float64(res.percentile(0.5).Microseconds())/1000, float64(res.percentile(0.99).Microseconds())/1000,
			res.errors, float64(calls)/float64(max(ok, 1)), execMs/float64(max(ok, 1)),
			float64((appCPU1-appCPU0).Microseconds())/1000/float64(max(ok, 1)),
			float64((pgCPU1-pgCPU0).Microseconds())/1000/float64(max(ok, 1)),
			float64(commits1-commits0)/float64(max(ok, 1)), avgWaiters, sessions1-sessions0, poolWaitDelta(statsBefore, statsAfter))
		if res.errors > 0 {
			fmt.Printf("PERF %-10s %-14s sample error: %s\n", label, wl.name, truncate(res.sample, 300))
		}
		if os.Getenv("PERF_TOP") != "" {
			for _, s := range top {
				q := s.query
				if os.Getenv("PERF_TOP") != "full" {
					q = truncate(q, 160)
				}
				fmt.Printf("    %8.2f/req %7.3fms  %s\n", float64(s.calls)/float64(max(ok, 1)), s.meanMs, q)
			}
		}
	}
}

type stmtStat struct {
	calls  int64
	meanMs float64
	query  string
}

func stmtStats(db string) (int64, float64, []stmtStat) {
	conn, err := pg.connect("postgres")
	if err != nil {
		return 0, 0, nil
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), `
		SELECT s.calls, s.mean_exec_time, s.total_exec_time, regexp_replace(s.query, '\s+', ' ', 'g')
		FROM pg_stat_statements s JOIN pg_database d ON d.oid = s.dbid
		WHERE d.datname = $1 ORDER BY s.calls DESC`, db)
	if err != nil {
		return 0, 0, nil
	}
	defer rows.Close()
	var calls int64
	var total float64
	var top []stmtStat
	for rows.Next() {
		var s stmtStat
		var tot float64
		if err := rows.Scan(&s.calls, &s.meanMs, &tot, &s.query); err != nil {
			continue
		}
		calls += s.calls
		total += tot
		if len(top) < 12 {
			top = append(top, s)
		}
	}
	return calls, total, top
}

func pgStrings(db, sql string, args ...any) ([]string, error) {
	conn, err := pg.connect(db)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		rows.Scan(&s)
		out = append(out, s)
	}
	return out, rows.Err()
}

type poolStats struct {
	WaitCount     int64
	WaitDuration  int64
	MaxIdleClosed int64
}

func (f *fixture) dbStats(nodes int) []poolStats {
	out := make([]poolStats, nodes)
	for i := range nodes {
		r, err := call(f.node(i), f.suToken, "GET", "/api/prodtest/dbstats", nil)
		if err != nil || r.Status != 200 {
			continue
		}
		var s map[string]poolStats
		json.Unmarshal(r.Body, &s)
		out[i] = s["data"]
	}
	return out
}

func poolWaitDelta(a, b []poolStats) string {
	var n, d, c int64
	for i := range a {
		n += b[i].WaitCount - a[i].WaitCount
		d += b[i].WaitDuration - a[i].WaitDuration
		c += b[i].MaxIdleClosed - a[i].MaxIdleClosed
	}
	return fmt.Sprintf("%d/%s idleClosed %d", n, time.Duration(d).Round(time.Millisecond), c)
}

func captureProfile(addr string, d time.Duration, file string) string {
	resp, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/profile?seconds=%d", addr, int(d.Seconds())-1))
	if err != nil {
		return err.Error()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if err := os.WriteFile(file, raw, 0o644); err != nil {
		return err.Error()
	}
	return ""
}

// appCPU returns the user+system CPU time of the running node processes.
func (c *cluster) appCPU() time.Duration {
	var ticks int64
	for _, n := range c.nodes {
		n.mu.Lock()
		pid := 0
		if n.cmd != nil && n.cmd.Process != nil {
			pid = n.cmd.Process.Pid
		}
		n.mu.Unlock()
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if pid == 0 || err != nil {
			continue
		}
		s := string(raw)
		fields := strings.Fields(s[strings.LastIndexByte(s, ')')+2:]) // fields from #3 (state)
		utime, _ := strconv.ParseInt(fields[11], 10, 64)              // #14 utime
		stime, _ := strconv.ParseInt(fields[12], 10, 64)              // #15 stime
		ticks += utime + stime
	}
	return time.Duration(ticks) * time.Second / 100 // USER_HZ (CLK_TCK) is 100 on Linux
}

var (
	pgCgroupOnce sync.Once
	pgCgroupStat string
)

// pgCPU returns the CPU time of the harness PostgreSQL container (backends
// and background processes), 0 for an external server or without cgroup v2.
func pgCPU() time.Duration {
	pgCgroupOnce.Do(func() {
		if pg.container == "" {
			return
		}
		out, err := exec.Command("docker", "inspect", "-f", "{{.Id}}", pg.container).Output()
		if err == nil {
			pgCgroupStat = "/sys/fs/cgroup/system.slice/docker-" + strings.TrimSpace(string(out)) + ".scope/cpu.stat"
		}
	})
	raw, err := os.ReadFile(pgCgroupStat)
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		if v, ok := strings.CutPrefix(line, "usage_usec "); ok {
			us, _ := strconv.ParseInt(v, 10, 64)
			return time.Duration(us) * time.Microsecond
		}
	}
	return 0
}
