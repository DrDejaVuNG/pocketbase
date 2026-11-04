//go:build prodtest

package main

import (
	"fmt"
	"syscall"
	"testing"
	"time"
)

// awaitDelivery creates posts through writeBase until sub receives one of
// them or the timeout elapses; it returns the time until the first delivery.
func awaitDelivery(t *testing.T, f *fixture, sub *sseClient, writeBase string, timeout time.Duration) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	for time.Since(start) < timeout {
		r, err := call(writeBase, f.bob.Token, "POST", "/api/collections/posts/records",
			map[string]any{"title": "probe", "org": f.orgA, "author": f.bob.ID})
		if err == nil && r.ok() {
			if _, err := sub.next("posts", time.Second); err == nil {
				return time.Since(start), nil
			}
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}
	return time.Since(start), fmt.Errorf("no cross-node delivery within %s", timeout)
}

// TestResilience covers node and database failures.
func TestResilience(t *testing.T) {
	t.Run("ListenerConnectionsKilled", func(t *testing.T) {
		f := dedicated(t, "listen", 2)

		sub := connectSSE(t, f.node(0))
		defer sub.Close()
		sub.mustSubscribe(t, f.node(0), f.alice.Token, "posts")
		if _, err := awaitDelivery(t, f, sub, f.node(1), 5*time.Second); err != nil {
			t.Fatalf("before: %v", err)
		}

		pids, err := listenerBackends(f.c.dataDB)
		if err != nil {
			t.Fatal(err)
		}
		if len(pids) == 0 {
			t.Fatal("no LISTEN backends found")
		}
		for _, pid := range pids {
			if err := pg.exec("postgres", `SELECT pg_terminate_backend($1)`, pid); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("terminated %d LISTEN backends", len(pids))

		took, err := awaitDelivery(t, f, sub, f.node(1), 20*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("cross-node delivery recovered after %s", took)
	})

	t.Run("PostgresRestart", func(t *testing.T) {
		if !pg.destructive() {
			t.Skip("needs the harness-managed PostgreSQL container")
		}
		f := dedicated(t, "pgrestart", 2)

		sub := connectSSE(t, f.node(0))
		defer sub.Close()
		sub.mustSubscribe(t, f.node(0), f.alice.Token, "posts")
		if _, err := awaitDelivery(t, f, sub, f.node(1), 5*time.Second); err != nil {
			t.Fatalf("before: %v", err)
		}

		start := time.Now()
		if err := pg.restart(); err != nil {
			t.Fatal(err)
		}
		t.Logf("postgres restarted in %s", time.Since(start))

		// every node must serve writes again
		for i := range f.c.nodes {
			took, err := eventually(30*time.Second, 250*time.Millisecond, func() error {
				r, err := call(f.node(i), f.bob.Token, "POST", "/api/collections/posts/records",
					map[string]any{"title": "after restart", "org": f.orgA, "author": f.bob.ID})
				if err != nil || !r.ok() {
					return fmt.Errorf("%v %s", err, r)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("node%d writes after restart: %v", i+1, err)
			}
			t.Logf("node%d writes recovered after %s", i+1, took)
		}

		// the SSE stream survived (it is between the client and the node);
		// cross-node delivery to it must recover
		sub.drain()
		took, err := awaitDelivery(t, f, sub, f.node(1), 30*time.Second)
		if err != nil {
			t.Fatalf("existing subscriber: %v", err)
		}
		t.Logf("cross-node delivery to an existing subscriber recovered after %s", took)
	})

	t.Run("PausedNodeRejoins", func(t *testing.T) {
		// A node that cannot heartbeat for longer than the channel TTL
		// (GC stall, VM freeze, network partition, long DB failover) is
		// declared offline by its peers. Its SSE clients are still
		// connected when it comes back and must keep receiving events.
		if testing.Short() {
			t.Skip("takes ~90s")
		}
		f := dedicated(t, "pause", 2)

		sub := connectSSE(t, f.node(1))
		defer sub.Close()
		sub.mustSubscribe(t, f.node(1), f.alice.Token, "posts")
		if _, err := awaitDelivery(t, f, sub, f.node(0), 5*time.Second); err != nil {
			t.Fatalf("before: %v", err)
		}

		if err := f.c.nodes[1].signal(syscall.SIGSTOP); err != nil {
			t.Fatal(err)
		}
		time.Sleep(75 * time.Second) // > 40s TTL + 30s heartbeat interval
		if err := f.c.nodes[1].signal(syscall.SIGCONT); err != nil {
			t.Fatal(err)
		}
		sub.drain()

		took, err := awaitDelivery(t, f, sub, f.node(0), 45*time.Second)
		if err != nil {
			t.Fatalf("subscriber on the resumed node: %v", err)
		}
		t.Logf("delivery to the resumed node's subscriber after %s", took)
	})

	t.Run("CrashedNodeCleanup", func(t *testing.T) {
		if testing.Short() {
			t.Skip("takes ~75s")
		}
		f := dedicated(t, "crash", 2)

		sub := connectSSE(t, f.node(1))
		sub.mustSubscribe(t, f.node(1), f.alice.Token, "posts")
		defer sub.Close()

		f.c.nodes[1].kill()

		took, err := eventually(90*time.Second, time.Second, func() error {
			n, err := pg.queryInt(f.c.dataDB, `SELECT COUNT(*) FROM "_realtimeClients" WHERE "clientId" = $1`, sub.ClientID)
			if err != nil {
				return err
			}
			if n != 0 {
				return fmt.Errorf("subscription of the crashed node still registered")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("crashed node's subscriptions removed after %s", took)

		// the surviving node keeps serving writes
		mustCall(t, f.node(0), f.bob.Token, "POST", "/api/collections/posts/records",
			map[string]any{"title": "after crash", "org": f.orgA, "author": f.bob.ID}, 200)
	})

	t.Run("GracefulShutdownCleanup", func(t *testing.T) {
		f := dedicated(t, "graceful", 2)

		sub := connectSSE(t, f.node(1))
		sub.mustSubscribe(t, f.node(1), f.alice.Token, "posts")
		defer sub.Close()

		f.c.nodes[1].stop()

		_, err := eventually(5*time.Second, 200*time.Millisecond, func() error {
			n, err := pg.queryInt(f.c.dataDB, `SELECT COUNT(*) FROM "_realtimeClients" WHERE "clientId" = $1`, sub.ClientID)
			if err != nil {
				return err
			}
			if n != 0 {
				return fmt.Errorf("subscription still registered after a graceful stop")
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
	})
}
