//go:build prodtest

package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestStartup covers deployment-time behavior: N nodes (re)starting at the
// same time against the same databases, like a rolling deploy or an
// autoscaler adding replicas.
func TestStartup(t *testing.T) {
	t.Run("ColdStartConcurrent", func(t *testing.T) {
		// 3 nodes start at the same moment against databases that do not
		// exist yet: they race on CREATE DATABASE, the system migrations
		// (functions, collation, extension) and the app migrations.
		c := newCluster("cold", 3)
		defer c.shutdown()

		errs := make([]error, len(c.nodes))
		var wg sync.WaitGroup
		for i, n := range c.nodes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := n.start(); err != nil {
					errs[i] = err
					return
				}
				errs[i] = n.waitHealthy(90 * time.Second)
			}()
		}
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				t.Errorf("node %d: %v", i+1, err)
			}
		}
		if t.Failed() {
			return
		}

		// every migration must be recorded exactly once
		dup, err := pg.queryInt(c.dataDB, `SELECT COUNT(*) FROM (SELECT file FROM _migrations GROUP BY file HAVING COUNT(*) > 1) d`)
		if err != nil {
			t.Fatal(err)
		}
		if dup != 0 {
			t.Errorf("%d migrations were applied more than once", dup)
		}

		n, err := pg.queryInt(c.dataDB, `SELECT COUNT(*) FROM _collections WHERE name IN ('posts','comments','orgs','post_stats')`)
		if err != nil {
			t.Fatal(err)
		}
		if n != 4 {
			t.Errorf("expected 4 app collections, got %d", n)
		}
	})

	t.Run("RollingRestart", func(t *testing.T) {
		// restart nodes one by one while traffic flows through the LB;
		// writes must keep succeeding (the LB skips stopped nodes).
		c := newCluster("rolling", 3)
		defer c.shutdown()
		if err := c.startSequential(); err != nil {
			t.Fatal(err)
		}

		email, pass := "su@prodtest.local", randHex(12)
		if err := c.upsertSuperuser(email, pass); err != nil {
			t.Fatal(err)
		}
		r := mustCall(t, c.proxy.url, "", "POST", "/api/collections/_superusers/auth-with-password",
			map[string]any{"identity": email, "password": pass}, 200)
		var auth struct{ Token string }
		r.decode(t, &auth)

		stop := make(chan struct{})
		var mu sync.Mutex
		var total, failed int
		var lastErr string
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					r, err := call(c.proxy.url, auth.Token, "POST", "/api/collections/accounts/records",
						map[string]any{"owner": "rolling", "balance": 1})
					mu.Lock()
					total++
					if err != nil || !r.ok() {
						failed++
						lastErr = fmt.Sprintf("%v %s", err, r)
					}
					mu.Unlock()
				}
			}()
		}

		for _, n := range c.nodes {
			time.Sleep(time.Second)
			n.stop()
			if err := n.start(); err != nil {
				t.Fatal(err)
			}
			if err := n.waitHealthy(60 * time.Second); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
		close(stop)
		wg.Wait()

		t.Logf("rolling restart: %d writes, %d failed", total, failed)
		// in-flight requests on a node that is shutting down may fail; more
		// than a handful means connections are not drained gracefully
		if failed > 6 {
			t.Errorf("%d/%d writes failed during the rolling restart (last: %s)", failed, total, lastErr)
		}
	})
}
