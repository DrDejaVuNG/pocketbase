//go:build prodtest

package main

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const deliveryTimeout = 5 * time.Second

// TestRealtime covers SSE delivery when the subscriber, the subscription
// request and the write land on different nodes (LB without sticky sessions).
func TestRealtime(t *testing.T) {
	f := shared(t)

	t.Run("CrossNodeDelivery", func(t *testing.T) {
		sub := connectSSE(t, f.node(0))
		defer sub.Close()
		sub.mustSubscribe(t, f.node(0), f.alice.Token, "posts")

		start := time.Now()
		post := f.newPost(t, f.node(1), f.bob, f.orgA, nil)

		ev, err := sub.next("posts", deliveryTimeout)
		if err != nil {
			t.Fatal(err)
		}
		re := decodeRecordEvent(t, ev)
		if re.Action != "create" || re.Record["id"] != post["id"] {
			t.Fatalf("unexpected event %s %v", re.Action, re.Record["id"])
		}
		t.Logf("cross-node create delivered in %s", time.Since(start))
	})

	t.Run("SubscribeViaOtherNode", func(t *testing.T) {
		// SSE stream on node1, subscription POST lands on node2.
		sub := connectSSE(t, f.node(0))
		defer sub.Close()

		r := sub.subscribe(f.node(1), f.alice.Token, "posts")
		if r.Status != 204 {
			t.Fatalf("subscription POST on another node than the SSE stream: %s", r)
		}

		post := f.newPost(t, f.node(2), f.bob, f.orgA, nil)
		ev, err := sub.next("posts", deliveryTimeout)
		if err != nil {
			t.Fatal(err)
		}
		if re := decodeRecordEvent(t, ev); re.Record["id"] != post["id"] {
			t.Fatalf("unexpected record %v", re.Record["id"])
		}
	})

	t.Run("RulesEnforcedOnRemoteNodes", func(t *testing.T) {
		// carol (org B) must not see org A posts, wherever the write happens
		carol := connectSSE(t, f.node(0))
		defer carol.Close()
		carol.mustSubscribe(t, f.node(0), f.carol.Token, "posts")

		alice := connectSSE(t, f.node(2))
		defer alice.Close()
		alice.mustSubscribe(t, f.node(2), f.alice.Token, "posts")

		f.newPost(t, f.node(1), f.bob, f.orgA, nil)

		if _, err := alice.next("posts", deliveryTimeout); err != nil {
			t.Fatalf("alice (same org): %v", err)
		}
		if err := carol.quiet("posts", 2*time.Second); err != nil {
			t.Fatalf("carol (other org) received an org A post: %v", err)
		}
	})

	t.Run("HiddenFieldsNotLeaked", func(t *testing.T) {
		// bob watches users of his org; alice's email is hidden
		// (emailVisibility=false) and must not be sent to bob.
		sub := connectSSE(t, f.node(0))
		defer sub.Close()
		sub.mustSubscribe(t, f.node(0), f.bob.Token, "users/"+f.alice.ID)

		mustCall(t, f.node(1), f.alice.Token, "PATCH", "/api/collections/users/records/"+f.alice.ID,
			map[string]any{"bio": "bio " + randHex(3)}, 200)

		ev, err := sub.next("users", deliveryTimeout)
		if err != nil {
			t.Fatal(err)
		}
		re := decodeRecordEvent(t, ev)
		for _, k := range []string{"email", "password", "tokenKey", "passwordHash"} {
			if v, ok := re.Record[k]; ok && v != "" {
				t.Errorf("hidden field %q delivered to another user: %v", k, v)
			}
		}
	})

	t.Run("NoSecretsInNotifyPayloads", func(t *testing.T) {
		// Any role that can connect to the database can LISTEN on the
		// bridge channels; payloads must not carry password hashes or token keys.
		conn, err := pg.connect(f.c.dataDB)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(context.Background())
		if _, err := conn.Exec(context.Background(), "LISTEN shared_bridge_channel"); err != nil {
			t.Fatal(err)
		}

		sub := connectSSE(t, f.node(0))
		defer sub.Close()
		sub.mustSubscribe(t, f.node(0), f.alice.Token, "posts")

		var payloads []string
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for {
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				break
			}
			if strings.Contains(n.Payload, sub.ClientID) {
				payloads = append(payloads, n.Payload)
			}
		}
		if len(payloads) == 0 {
			t.Fatal("no bridge notification observed for the subscription")
		}
		for _, p := range payloads {
			for _, secret := range []string{`"password"`, `"tokenKey"`, "$2a$"} {
				if strings.Contains(p, secret) {
					t.Errorf("NOTIFY payload exposes %s: %s", secret, truncate(p, 400))
				}
			}
		}
	})

	t.Run("FilterWithOrOperator", func(t *testing.T) {
		// "||" is the PocketBase OR operator; it must survive the bridge.
		opts, _ := json.Marshal(map[string]any{"query": map[string]any{
			"filter": "status = 'published' || featured = true",
		}})
		topic := "posts?options=" + url.QueryEscape(string(opts))

		sub := connectSSE(t, f.node(0))
		defer sub.Close()
		sub.mustSubscribe(t, f.node(0), f.alice.Token, topic)
		time.Sleep(300 * time.Millisecond) // let the other nodes learn the subscription

		post := f.newPost(t, f.node(1), f.bob, f.orgA, map[string]any{"status": "published"})
		ev, err := sub.next("posts", deliveryTimeout)
		if err != nil {
			t.Fatalf("create on another node: %v", err)
		}
		if re := decodeRecordEvent(t, ev); re.Record["id"] != post["id"] {
			t.Fatalf("unexpected record %v", re.Record["id"])
		}

		f.newPost(t, f.node(1), f.bob, f.orgA, map[string]any{"status": "draft"})
		if err := sub.quiet("posts", 1500*time.Millisecond); err != nil {
			t.Fatalf("filtered-out record delivered: %v", err)
		}
	})

	t.Run("LargeRecordCrossNode", func(t *testing.T) {
		// PostgreSQL NOTIFY payloads are limited to 8000 bytes.
		sub := connectSSE(t, f.node(0))
		defer sub.Close()
		sub.mustSubscribe(t, f.node(0), f.alice.Token, "big_docs")
		time.Sleep(300 * time.Millisecond)

		for _, size := range []int{2_000, 7_900, 20_000, 200_000} {
			content := strings.Repeat("x", size)
			r := mustCall(t, f.node(1), f.alice.Token, "POST", "/api/collections/big_docs/records",
				map[string]any{"title": "doc", "content": content, "org": f.orgA}, 200)
			var rec map[string]any
			r.decode(t, &rec)

			ev, err := sub.next("big_docs", deliveryTimeout)
			if err != nil {
				t.Errorf("record with %d bytes content: %v", size, err)
				continue
			}
			re := decodeRecordEvent(t, ev)
			if re.Record["id"] != rec["id"] || re.Record["content"] != content {
				t.Errorf("record with %d bytes content: payload mismatch", size)
			}
		}
	})

	t.Run("AuthChangePropagates", func(t *testing.T) {
		// dave subscribes on node1; a superuser moves him to org B through
		// node2; a post in org A created through node3 must no longer reach him.
		dave, err := f.createUser("dave"+randHex(2), f.orgA)
		if err != nil {
			t.Fatal(err)
		}

		sub := connectSSE(t, f.node(0))
		defer sub.Close()
		sub.mustSubscribe(t, f.node(0), dave.Token, "posts")

		f.newPost(t, f.node(2), f.bob, f.orgA, nil)
		if _, err := sub.next("posts", deliveryTimeout); err != nil {
			t.Fatalf("before the org change: %v", err)
		}

		mustCall(t, f.node(1), f.suToken, "PATCH", "/api/collections/users/records/"+dave.ID,
			map[string]any{"org": f.orgB}, 200)
		time.Sleep(time.Second)
		sub.drain()

		f.newPost(t, f.node(2), f.bob, f.orgA, nil)
		if err := sub.quiet("posts", 2*time.Second); err != nil {
			t.Fatalf("after moving the user to another org: %v", err)
		}
	})

	t.Run("DisconnectCleansUp", func(t *testing.T) {
		sub := connectSSE(t, f.node(0))
		sub.mustSubscribe(t, f.node(0), f.alice.Token, "posts")
		id := sub.ClientID

		count := func() int64 {
			n, _ := pg.queryInt(f.c.dataDB, `SELECT COUNT(*) FROM "_realtimeClients" WHERE "clientId" = $1`, id)
			return n
		}
		if _, err := eventually(3*time.Second, 100*time.Millisecond, func() error {
			if count() != 1 {
				return errString("client row not created")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		sub.Close()
		if _, err := eventually(5*time.Second, 100*time.Millisecond, func() error {
			if count() != 0 {
				return errString("client row still present after disconnect")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("FanOutBurst", func(t *testing.T) {
		// 30 subscribers spread over the nodes, 50 writes spread over the
		// nodes: every subscriber must receive every event exactly once.
		const subs, writes = 30, 50
		clients := make([]*sseClient, subs)
		for i := range subs {
			clients[i] = connectSSE(t, f.node(i%3))
			defer clients[i].Close()
			clients[i].mustSubscribe(t, f.node(i%3), f.alice.Token, "posts")
		}
		time.Sleep(500 * time.Millisecond)

		ids := make(map[string]bool)
		var mu sync.Mutex
		var wg sync.WaitGroup
		for i := range writes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, err := call(f.node(i%3), f.bob.Token, "POST", "/api/collections/posts/records",
					map[string]any{"title": "burst", "org": f.orgA, "author": f.bob.ID})
				if err != nil || !r.ok() {
					t.Errorf("create: %v %s", err, r)
					return
				}
				var p struct{ ID string }
				json.Unmarshal(r.Body, &p)
				mu.Lock()
				ids[p.ID] = true
				mu.Unlock()
			}()
		}
		wg.Wait()

		start := time.Now()
		for i, c := range clients {
			got := map[string]int{}
			for len(got) < writes {
				ev, err := c.next("posts", deliveryTimeout)
				if err != nil {
					t.Errorf("subscriber %d (node%d): got %d/%d events: %v", i, i%3+1, len(got), writes, err)
					break
				}
				re := decodeRecordEvent(t, ev)
				id, _ := re.Record["id"].(string)
				if ids[id] {
					got[id]++
				}
			}
			for id, n := range got {
				if n > 1 {
					t.Errorf("subscriber %d received %s %d times", i, id, n)
				}
			}
		}
		t.Logf("fan-out: %d subscribers x %d events drained in %s", subs, writes, time.Since(start))
	})
}

type errString string

func (e errString) Error() string { return string(e) }

// listenerBackends returns the pids of the backends that run LISTEN for the bridge.
func listenerBackends(db string) ([]int32, error) {
	conn, err := pg.connect(db)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(),
		`SELECT pid FROM pg_stat_activity WHERE datname = $1 AND query ILIKE 'LISTEN %'`, db)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int32])
}
