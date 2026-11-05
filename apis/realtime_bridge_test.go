package apis_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/apis"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tests"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/security"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/subscriptions"
	"github.com/jackc/pgx/v5"
)

const bridgeTestPostgresURL = "postgres://user:pass@127.0.0.1:5432/postgres?sslmode=disable"

func TestRealtimeBridge(t *testing.T) {
	nodes := newBridgeCluster(t, 2, false)
	app1, app2 := nodes[0].app, nodes[1].app
	bridgeA := nodes[0].bridge

	local_client1_in_channelA := apis.NewBridgedClient(bridgeA)
	// test client is used to capture messages sent to the ws clients
	testClient := &TestClient{Client: local_client1_in_channelA.Client}
	local_client1_in_channelA.Client = testClient

	// client1 online
	app1.SubscriptionsBroker().Register(local_client1_in_channelA)
	local_client1_in_channelA.Subscribe("sub1")
	local_client1_in_channelA.BroadcastChanges()

	remote_client1_in_channelB := waitForMirror(t, app2, local_client1_in_channelA.Id())

	t.Run("Test client online events broadcasted to other servers", func(t *testing.T) {
		if !remote_client1_in_channelB.IsRemoteClient() {
			t.Fatalf("Expected remote client to be true, but got false")
		}
		if local_client1_in_channelA.IsRemoteClient() {
			t.Fatalf("Expected local client to be false, but got true")
		}
		if remote_client1_in_channelB.Id() != local_client1_in_channelA.Id() {
			t.Fatalf("Expected the client ID to be the same, but got %s and %s", remote_client1_in_channelB.Id(), local_client1_in_channelA.Id())
		}
		if !remote_client1_in_channelB.HasSubscription("sub1") {
			t.Fatalf("Expected the remote client to have the sub1 subscription")
		}
	})

	t.Run("Test local client message sent to local channel", func(t *testing.T) {
		// Send message from to client1 from the same pocketbase instance
		message := subscriptions.Message{Name: "test1", Data: []byte("Hello World!")}
		local_client1_in_channelA.Send(message)
		messages := testClient.Messages()
		if len(messages) != 1 {
			t.Fatalf("Expected local client channel to have first message, got %d", len(messages))
		}
		if messages[0].Name != "test1" || string(messages[0].Data) != "Hello World!" {
			t.Fatalf("Expected message to be 'Hello World!', but got %s", string(messages[0].Data))
		}
	})

	t.Run("Test remote client message sent to remote channel", func(t *testing.T) {
		// send message to client1 from different pocketbase instance
		message := subscriptions.Message{Name: "test2", Data: []byte("Hello World! |with|pipes|")}
		remote_client1_in_channelB.Send(message)
		messages := testClient.WaitMessages(t, 2, 5*time.Second)
		if messages[1].Name != "test2" || string(messages[1].Data) != "Hello World! |with|pipes|" {
			t.Fatalf("Unexpected message %s: %s", messages[1].Name, string(messages[1].Data))
		}
	})

	t.Run("Test client auth state broadcasted to other servers", func(t *testing.T) {
		// auth state updated in app2
		authRecord1, err := app2.FindRecordById("users", "4q1xlclmfloku33")
		if err != nil {
			t.Fatalf("Failed to find record by ID: %v", err)
		}
		remote_client1_in_channelB.Set(apis.RealtimeClientAuthKey, authRecord1)
		remote_client1_in_channelB.BroadcastChanges()

		// then auth state should be updated in app1
		var authRecord2 *core.Record
		waitFor(t, 3*time.Second, "auth state on the owner node", func() bool {
			authRecord2, _ = local_client1_in_channelA.Get(apis.RealtimeClientAuthKey).(*core.Record)
			return authRecord2 != nil
		})
		if authRecord2.Id != authRecord1.Id {
			t.Fatalf("Expected auth record ID to be %s, but got %s", authRecord1.Id, authRecord2.Id)
		}

		json1, _ := json.Marshal(authRecord1.Clone().Unhide(authRecord1.Collection().Fields.FieldNames()...).IgnoreEmailVisibility(true).PublicExport())
		json2, _ := json.Marshal(authRecord2.Clone().Unhide(authRecord2.Collection().Fields.FieldNames()...).IgnoreEmailVisibility(true).PublicExport())
		if string(json1) != string(json2) {
			t.Fatalf("AuthRecord JSON does not match: expected \n%s\n, got \n%s", string(json2), string(json1))
		}
	})

	t.Run("Test client offline event broadcasted to other servers", func(t *testing.T) {
		// client1 offline
		app1.SubscriptionsBroker().Unregister(local_client1_in_channelA.Id())
		local_client1_in_channelA.BroadcastGoOffline()

		waitFor(t, 3*time.Second, "mirror removal", func() bool {
			_, err := app2.SubscriptionsBroker().ClientById(local_client1_in_channelA.Id())
			return err != nil
		})
	})
}

// A dead instance (expired channel) is evicted together with its clients,
// while the live instances keep their local clients.
func TestRealtimeBridge_ChannelOffline(t *testing.T) {
	t.Cleanup(apis.SetRealtimeBridgeTimings(300*time.Millisecond, time.Second))

	nodes := newBridgeCluster(t, 2, false)
	app1, app2 := nodes[0].app, nodes[1].app

	live := apis.NewBridgedClient(nodes[0].bridge)
	app1.SubscriptionsBroker().Register(live)
	live.Subscribe("sub1")
	live.BroadcastChanges()
	waitForMirror(t, app2, live.Id())

	// a crashed instance: expired channel + client row
	mustExec(t, app1, `INSERT INTO "_realtimeChannels" ("channelId", "validUntil") VALUES ('c_dead_test', now() - interval '1 hour')`)
	mustExec(t, app1, `INSERT INTO "_realtimeClients" ("clientId", "channelId", "subscriptions", "authCollectionRef", "authRecordRef", "updatedByChannelId")
		VALUES ('deadclient', 'c_dead_test', '{sub1}', '', '', 'c_dead_test')`)
	notifyShared(t, app1, map[string]any{"type": "client_upsert", "from": "c_dead_test", "clientId": "deadclient"})

	// the mirrors are created and then removed by the next heartbeat
	waitFor(t, 5*time.Second, "dead client eviction", func() bool {
		_, err1 := app1.SubscriptionsBroker().ClientById("deadclient")
		_, err2 := app2.SubscriptionsBroker().ClientById("deadclient")
		return err1 != nil && err2 != nil && countRows(t, app1, `SELECT count(*) FROM "_realtimeChannels" WHERE "channelId" = 'c_dead_test'`) == 0
	})
	if n := countRows(t, app1, `SELECT count(*) FROM "_realtimeClients" WHERE "channelId" = 'c_dead_test'`); n != 0 {
		t.Fatalf("Expected the dead channel clients to be deleted, got %d", n)
	}

	if live.IsDiscarded() {
		t.Fatal("Expected the live local client to stay connected")
	}
	if _, err := app2.SubscriptionsBroker().ClientById(live.Id()); err != nil {
		t.Fatal("Expected the live client mirror to stay registered")
	}
}

// Item 1: NOTIFY payloads carry only ids; receivers load the auth record by id.
func TestRealtimeBridge_NoRecordDataInNotifyPayloads(t *testing.T) {
	nodes := newBridgeCluster(t, 2, false)
	app1, app2 := nodes[0].app, nodes[1].app

	payloads := captureNotifications(t, app1, "shared_bridge_channel")

	superuser, err := app1.FindAuthRecordByEmail(core.CollectionNameSuperusers, "test@example.com")
	if err != nil {
		t.Fatal(err)
	}

	local := apis.NewBridgedClient(nodes[0].bridge)
	tc := &TestClient{Client: local.Client}
	local.Client = tc
	app1.SubscriptionsBroker().Register(local)
	local.Set(apis.RealtimeClientAuthKey, superuser)
	local.Subscribe("demo1/*")
	local.BroadcastChanges()

	mirror := waitForMirror(t, app2, local.Id())
	var mirrorAuth *core.Record
	waitFor(t, 3*time.Second, "mirror auth", func() bool {
		mirrorAuth, _ = mirror.Get(apis.RealtimeClientAuthKey).(*core.Record)
		return mirrorAuth != nil
	})
	if mirrorAuth.Id != superuser.Id || mirrorAuth.Collection().Name != core.CollectionNameSuperusers {
		t.Fatalf("Expected the mirror auth to be the superuser, got %s/%s", mirrorAuth.Collection().Name, mirrorAuth.Id)
	}

	// a write on node2 reaches the superuser subscriber of node1
	col, _ := app2.FindCollectionByNameOrId("demo1")
	rec := core.NewRecord(col)
	rec.Set("text", "created-on-node2")
	if err := app2.Save(rec); err != nil {
		t.Fatal(err)
	}
	tc.WaitMessages(t, 1, 5*time.Second)

	all := payloads()
	found := false
	for _, p := range all {
		if !strings.Contains(p, local.Id()) {
			continue
		}
		found = true
		for _, secret := range []string{`"password"`, `"tokenKey"`, "$2a$", superuser.Email(), superuser.TokenKey()} {
			if strings.Contains(p, secret) {
				t.Errorf("NOTIFY payload exposes %q: %s", secret, p)
			}
		}
	}
	if !found {
		t.Fatalf("No notification observed for the client, got %v", all)
	}
}

// Item 2: mirrors carry the client IP (OAuth2 redirect XSRF check) and the
// @oauth2 one-shot unsubscribe applies cluster-wide.
func TestRealtimeBridge_OAuth2RedirectOnOtherNode(t *testing.T) {
	nodes := newBridgeCluster(t, 2, false)
	app1, app2 := nodes[0].app, nodes[1].app

	local := apis.NewBridgedClient(nodes[0].bridge)
	tc := &TestClient{Client: local.Client}
	local.Client = tc
	local.Set(apis.RealtimeClientIPKey, "1.1.1.1")
	app1.SubscriptionsBroker().Register(local)

	subscribeOAuth2 := func() apis.IBridgedClient {
		local.Subscribe("@oauth2")
		local.BroadcastChanges()
		mirror := waitForMirror(t, app2, local.Id())
		waitFor(t, 3*time.Second, "mirror @oauth2 subscription", func() bool {
			return mirror.HasSubscription("@oauth2")
		})
		return mirror
	}

	redirect := func(ip string, code string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/api/oauth2-redirect?state="+local.Id()+"&code="+code, nil)
		req.RemoteAddr = ip + ":1234"
		rec := httptest.NewRecorder()
		nodes[1].mux.ServeHTTP(rec, req)
		return rec
	}

	mirror := subscribeOAuth2()
	if ip, _ := mirror.Get(apis.RealtimeClientIPKey).(string); ip != "1.1.1.1" {
		t.Fatalf("Expected the mirror IP to be 1.1.1.1, got %q", ip)
	}

	// another IP (eg. the victim of an XSRF attack)
	redirect("9.9.9.9", "VICTIM_CODE")
	time.Sleep(500 * time.Millisecond)
	if messages := tc.Messages(); len(messages) != 0 {
		t.Fatalf("Expected no code delivered for a different IP, got %s", messages[0].Data)
	}

	// the same IP
	subscribeOAuth2()
	redirect("1.1.1.1", "GOOD_CODE")
	messages := tc.WaitMessages(t, 1, 5*time.Second)
	if messages[0].Name != "@oauth2" || !strings.Contains(string(messages[0].Data), "GOOD_CODE") {
		t.Fatalf("Unexpected message %s: %s", messages[0].Name, messages[0].Data)
	}

	// the state is one-shot on every node
	waitFor(t, 3*time.Second, "cluster-wide @oauth2 unsubscribe", func() bool {
		return !local.HasSubscription("@oauth2") && !mirror.HasSubscription("@oauth2")
	})
	if n := countRows(t, app1, fmt.Sprintf(`SELECT count(*) FROM "_realtimeClients" WHERE "clientId" = '%s' AND '@oauth2' = ANY("subscriptions")`, local.Id())); n != 0 {
		t.Fatal("Expected the @oauth2 subscription to be removed from the client row")
	}
	redirect("1.1.1.1", "REPLAYED_CODE")
	time.Sleep(500 * time.Millisecond)
	if n := len(tc.Messages()); n != 1 {
		t.Fatalf("Expected the replayed state to be rejected, got %d messages", n)
	}
}

// Item 3: the full refresh (startup and listener reconnects) keeps the remote
// auth, never touches local clients and survives query errors.
func TestRealtimeBridge_FullRefresh(t *testing.T) {
	nodes := newBridgeCluster(t, 1, false)
	app1 := nodes[0].app

	user, err := app1.FindRecordById("users", "4q1xlclmfloku33")
	if err != nil {
		t.Fatal(err)
	}
	local := apis.NewBridgedClient(nodes[0].bridge)
	app1.SubscriptionsBroker().Register(local)
	local.Set(apis.RealtimeClientAuthKey, user)
	local.Subscribe("demo2/*")
	local.BroadcastChanges()

	// a second node joins (scale-out / rolling restart) and loads the client via the full refresh
	node2 := addBridgeNode(t, nodes[0])
	app2 := node2.app
	mirror := waitForMirror(t, app2, local.Id())
	assertAuth := func(c subscriptions.Client, label string) {
		t.Helper()
		auth, _ := c.Get(apis.RealtimeClientAuthKey).(*core.Record)
		if auth == nil || auth.Id != user.Id {
			t.Fatalf("%s: expected auth %s, got %v", label, user.Id, auth)
		}
	}
	assertAuth(mirror, "mirror after the initial refresh")

	// node2 updates the row (updatedByChannelId = node2)
	u2, _ := app2.FindRecordById("users", user.Id)
	u2.Set("name", "renamed")
	if err := app2.Save(u2); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "auth update on the owner", func() bool {
		auth, _ := local.Get(apis.RealtimeClientAuthKey).(*core.Record)
		return auth != nil && auth.GetString("name") == "renamed"
	})

	// listener reconnects (eg. PG restart / network blip)
	terminateListeners(t, app1)
	time.Sleep(2500 * time.Millisecond)

	mirror = waitForMirror(t, app2, local.Id())
	assertAuth(mirror, "mirror after the reconnect")
	assertAuth(local, "owner local client after the reconnect")
	if local.IsDiscarded() {
		t.Fatal("Expected the local client to stay connected")
	}

	// a failing refresh query must not crash the process and must be retried
	mustExec(t, app1, `ALTER TABLE "_realtimeClients" RENAME TO "_realtimeClients_tmp"`)
	terminateListeners(t, app1)
	time.Sleep(2500 * time.Millisecond)
	mustExec(t, app1, `ALTER TABLE "_realtimeClients_tmp" RENAME TO "_realtimeClients"`)

	// a client that joined while the listeners were down is picked up by the retried refresh
	other := apis.NewBridgedClient(node2.bridge)
	app2.SubscriptionsBroker().Register(other)
	other.Subscribe("demo2/*")
	other.BroadcastChanges()
	waitForMirror(t, app1, other.Id())
	assertAuth(waitForMirror(t, app2, local.Id()), "mirror after the failed refresh")
}

// Item 4: heartbeat eviction never loses the clients of a live node.
func TestRealtimeBridge_HeartbeatReannounce(t *testing.T) {
	t.Cleanup(apis.SetRealtimeBridgeTimings(300*time.Millisecond, time.Second))

	t.Run("own expired channel is renewed, not evicted", func(t *testing.T) {
		nodes := newBridgeCluster(t, 1, false)
		app1 := nodes[0].app

		local := apis.NewBridgedClient(nodes[0].bridge)
		app1.SubscriptionsBroker().Register(local)
		local.Subscribe("sub1")
		local.BroadcastChanges()

		mustExec(t, app1, `UPDATE "_realtimeChannels" SET "validUntil" = now() - interval '1 hour'`)
		time.Sleep(time.Second)

		if local.IsDiscarded() {
			t.Fatal("Expected the local client to stay connected")
		}
		if n := countRows(t, app1, fmt.Sprintf(`SELECT count(*) FROM "_realtimeClients" WHERE "clientId" = '%s'`, local.Id())); n != 1 {
			t.Fatalf("Expected the client row to be kept, got %d", n)
		}
		if n := countRows(t, app1, `SELECT count(*) FROM "_realtimeChannels" WHERE "validUntil" > now()`); n != 1 {
			t.Fatalf("Expected the own channel to be renewed, got %d", n)
		}
	})

	t.Run("evicted live node re-announces its clients", func(t *testing.T) {
		nodes := newBridgeCluster(t, 2, false)
		app1, app2 := nodes[0].app, nodes[1].app
		channel1 := nodes[0].bridge.SelfChannelId()

		local := apis.NewBridgedClient(nodes[0].bridge)
		tc := &TestClient{Client: local.Client}
		local.Client = tc
		app1.SubscriptionsBroker().Register(local)
		local.Subscribe("demo2/*")
		local.BroadcastChanges()
		oldMirror := waitForMirror(t, app2, local.Id())

		// node2 evicts node1 (eg. node1 was paused for longer than the TTL)
		mustExec(t, app2, fmt.Sprintf(`
			WITH
				ch AS (DELETE FROM "_realtimeChannels" WHERE "channelId" = '%[1]s'),
				cl AS (DELETE FROM "_realtimeClients" WHERE "channelId" = '%[1]s')
			SELECT pg_notify('shared_bridge_channel', json_build_object('type', 'channel_offline', 'from', '%[2]s', 'channelId', '%[1]s')::text)
		`, channel1, nodes[1].bridge.SelfChannelId()))

		waitFor(t, 3*time.Second, "re-announced rows", func() bool {
			return countRows(t, app1, fmt.Sprintf(`SELECT count(*) FROM "_realtimeClients" WHERE "clientId" = '%s'`, local.Id())) == 1 &&
				countRows(t, app1, fmt.Sprintf(`SELECT count(*) FROM "_realtimeChannels" WHERE "channelId" = '%s'`, channel1)) == 1
		})
		if local.IsDiscarded() {
			t.Fatal("Expected the local client to stay connected")
		}

		// the evicted mirror is replaced by the re-announced one
		var mirror apis.IBridgedClient
		waitFor(t, 3*time.Second, "re-created mirror", func() bool {
			mirror = waitForMirror(t, app2, local.Id())
			return mirror != oldMirror
		})
		mirror.Send(subscriptions.Message{Name: "demo2/*", Data: []byte(`{"after":"eviction"}`)})
		tc.WaitMessages(t, 1, 5*time.Second)
	})

	t.Run("listener reconnect re-announces the local clients", func(t *testing.T) {
		nodes := newBridgeCluster(t, 2, false)
		app1, app2 := nodes[0].app, nodes[1].app

		local := apis.NewBridgedClient(nodes[0].bridge)
		app1.SubscriptionsBroker().Register(local)
		local.Subscribe("demo2/*")
		local.BroadcastChanges()
		waitForMirror(t, app2, local.Id())

		// the row is lost without notification (eg. deleted while the listeners were down)
		mustExec(t, app1, fmt.Sprintf(`DELETE FROM "_realtimeClients" WHERE "clientId" = '%s'`, local.Id()))
		terminateListeners(t, app1)

		waitFor(t, 5*time.Second, "re-announced row", func() bool {
			return countRows(t, app1, fmt.Sprintf(`SELECT count(*) FROM "_realtimeClients" WHERE "clientId" = '%s'`, local.Id())) == 1
		})
		waitForMirror(t, app2, local.Id())
	})

	t.Run("client rows without a channel are garbage collected", func(t *testing.T) {
		nodes := newBridgeCluster(t, 2, false)
		app1, app2 := nodes[0].app, nodes[1].app

		mustExec(t, app1, `INSERT INTO "_realtimeClients" ("clientId", "channelId", "subscriptions", "authCollectionRef", "authRecordRef", "updatedByChannelId")
			VALUES ('orphanclient', 'c_missing_channel', '{sub1}', '', '', 'c_missing_channel')`)
		notifyShared(t, app1, map[string]any{"type": "client_upsert", "from": "c_missing_channel", "clientId": "orphanclient"})

		waitFor(t, 5*time.Second, "orphan removal", func() bool {
			_, err1 := app1.SubscriptionsBroker().ClientById("orphanclient")
			_, err2 := app2.SubscriptionsBroker().ClientById("orphanclient")
			return err1 != nil && err2 != nil &&
				countRows(t, app1, `SELECT count(*) FROM "_realtimeClients" WHERE "clientId" = 'orphanclient'`) == 0
		})
	})
}

// Item 5: a slow local client doesn't block the delivery to the other clients.
func TestRealtimeBridge_ListenerNotBlockedBySlowClient(t *testing.T) {
	nodes := newBridgeCluster(t, 2, false)
	app1, app2 := nodes[0].app, nodes[1].app

	slow := apis.NewBridgedClient(nodes[0].bridge) // its channel is never read
	fast := apis.NewBridgedClient(nodes[0].bridge)
	for _, c := range []*apis.BridgedClient{slow, fast} {
		app1.SubscriptionsBroker().Register(c)
		c.Subscribe("demo2/*")
		c.BroadcastChanges()
	}
	slowMirror := waitForMirror(t, app2, slow.Id())
	fastMirror := waitForMirror(t, app2, fast.Id())

	slowMirror.Send(subscriptions.Message{Name: "demo2/*", Data: []byte(`{"to":"slow"}`)})
	slowMirror.Send(subscriptions.Message{Name: "demo2/*", Data: []byte(`{"to":"slow"}`)})
	fastMirror.Send(subscriptions.Message{Name: "demo2/*", Data: []byte(`{"to":"fast"}`)})

	select {
	case msg := <-fast.Channel():
		if string(msg.Data) != `{"to":"fast"}` {
			t.Fatalf("Unexpected message %s", msg.Data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("The fast client message was blocked by the slow client")
	}
}

// Item 6: only the owner node creates client rows; leftover rows are removed.
func TestRealtimeBridge_NoGhostClients(t *testing.T) {
	nodes := newBridgeCluster(t, 2, false)
	app1, app2 := nodes[0].app, nodes[1].app

	x := apis.NewBridgedClient(nodes[0].bridge)
	app1.SubscriptionsBroker().Register(x)
	x.Subscribe("demo2/*")
	x.BroadcastChanges()
	staleMirror := waitForMirror(t, app2, x.Id())

	// owner-side disconnect (the same as the deferred block in realtimeConnect)
	app1.SubscriptionsBroker().Unregister(x.Id())
	x.BroadcastGoOffline()
	time.Sleep(500 * time.Millisecond)

	// racing cross-node change on a stale mirror (eg. an auth refresh)
	staleMirror.BroadcastChanges()
	staleMirror.BroadcastAuthChanges()
	time.Sleep(500 * time.Millisecond)

	if n := countRows(t, app1, fmt.Sprintf(`SELECT count(*) FROM "_realtimeClients" WHERE "clientId" = '%s'`, x.Id())); n != 0 {
		t.Fatalf("Expected the client row to stay deleted, got %d", n)
	}
	if _, err := app1.SubscriptionsBroker().ClientById(x.Id()); err == nil {
		t.Fatal("Expected no ghost client on the owner node")
	}
	if _, err := app2.SubscriptionsBroker().ClientById(x.Id()); err == nil {
		t.Fatal("Expected the stale mirror to be removed")
	}

	// a leftover row of node1 without a live connection
	mustExec(t, app1, fmt.Sprintf(`INSERT INTO "_realtimeClients" ("clientId", "channelId", "subscriptions", "authCollectionRef", "authRecordRef", "updatedByChannelId")
		VALUES ('ghostclient', '%s', '{demo2/*}', '', '', 'test')`, nodes[0].bridge.SelfChannelId()))
	notifyShared(t, app1, map[string]any{"type": "client_upsert", "from": "test", "clientId": "ghostclient"})

	waitFor(t, 3*time.Second, "ghost row removal", func() bool {
		_, err1 := app1.SubscriptionsBroker().ClientById("ghostclient")
		_, err2 := app2.SubscriptionsBroker().ClientById("ghostclient")
		return err1 != nil && err2 != nil &&
			countRows(t, app1, `SELECT count(*) FROM "_realtimeClients" WHERE "clientId" = 'ghostclient'`) == 0
	})

	// the listener is still healthy
	healthy := apis.NewBridgedClient(nodes[0].bridge)
	tc := &TestClient{Client: healthy.Client}
	healthy.Client = tc
	app1.SubscriptionsBroker().Register(healthy)
	healthy.Subscribe("demo2/*")
	healthy.BroadcastChanges()
	waitForMirror(t, app2, healthy.Id()).Send(subscriptions.Message{Name: "demo2/*", Data: []byte(`{}`)})
	tc.WaitMessages(t, 1, 3*time.Second)
}

// Item 7: auth revocation reaches the owner node and all mirrors.
func TestRealtimeBridge_AuthRevocationPropagates(t *testing.T) {
	nodes := newBridgeCluster(t, 2, false)
	app1, app2 := nodes[0].app, nodes[1].app

	authRecord, err := app1.FindRecordById("clients", "gk390qegs4y47wn")
	if err != nil {
		t.Fatal(err)
	}
	local := apis.NewBridgedClient(nodes[0].bridge)
	tc := &TestClient{Client: local.Client}
	local.Client = tc
	app1.SubscriptionsBroker().Register(local)

	authenticate := func() apis.IBridgedClient {
		t.Helper()
		record, err := app1.FindRecordById("clients", authRecord.Id)
		if err != nil {
			t.Fatal(err)
		}
		local.Set(apis.RealtimeClientAuthKey, record)
		local.Subscribe("demo3/*")
		local.BroadcastChanges()
		mirror := waitForMirror(t, app2, local.Id())
		waitFor(t, 3*time.Second, "mirror auth", func() bool {
			auth, _ := mirror.Get(apis.RealtimeClientAuthKey).(*core.Record)
			return auth != nil && auth.TokenKey() == record.TokenKey()
		})
		return mirror
	}

	waitRevoked := func(mirror apis.IBridgedClient, label string) {
		t.Helper()
		waitFor(t, 3*time.Second, label, func() bool {
			return local.Get(apis.RealtimeClientAuthKey) == nil && mirror.Get(apis.RealtimeClientAuthKey) == nil
		})
		if n := countRows(t, app1, fmt.Sprintf(`SELECT count(*) FROM "_realtimeClients" WHERE "clientId" = '%s' AND "authRecordRef" = ''`, local.Id())); n != 1 {
			t.Fatalf("%s: expected the client row auth to be cleared", label)
		}
	}

	t.Run("tokenKey change on another node", func(t *testing.T) {
		mirror := authenticate()

		r2, _ := app2.FindRecordById("clients", authRecord.Id)
		r2.SetPassword("new-password-123")
		r2.RefreshTokenKey()
		if err := app2.Save(r2); err != nil {
			t.Fatal(err)
		}
		waitRevoked(mirror, "tokenKey revocation")

		// auth-only events are no longer delivered
		col, _ := app1.FindCollectionByNameOrId("demo3")
		rec := core.NewRecord(col)
		rec.Set("title", "written after the revocation")
		if err := app1.Save(rec); err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
		if n := len(tc.Messages()); n != 0 {
			t.Fatalf("Expected no events for the revoked client, got %d", n)
		}
	})

	t.Run("tokenKey changed without the update hooks", func(t *testing.T) {
		mirror := authenticate()

		// the owner detects the stale tokenKey on the next client update
		mustExec(t, app1, fmt.Sprintf(`UPDATE "clients" SET "tokenKey" = 'changed_outside_of_the_hooks_%s' WHERE "id" = '%s'`, security.RandomString(10), authRecord.Id))
		mirror.BroadcastAuthChanges()
		waitRevoked(mirror, "owner-side tokenKey check")
	})

	t.Run("auth collection secret rotation on another node", func(t *testing.T) {
		mirror := authenticate()

		collection, err := app2.FindCollectionByNameOrId("clients")
		if err != nil {
			t.Fatal(err)
		}
		collection.AuthToken.Secret = security.RandomString(50)
		if err := app2.Save(collection); err != nil {
			t.Fatal(err)
		}
		waitRevoked(mirror, "secret rotation")
	})
}

// Item 8: collections and settings changes reach the other nodes for every
// change path; a collection created on another node is usable immediately.
func TestRealtimeBridge_CollectionsAndSettingsPropagation(t *testing.T) {
	// separate data dirs (as on separate hosts) so that the fsnotify based
	// watcher can't mask a missing bridge notification
	nodes := newBridgeCluster(t, 2, true)
	app1, app2 := nodes[0].app, nodes[1].app

	listRule := func(app core.App, name string) *string {
		c, err := app.FindCachedCollectionByNameOrId(name)
		if err != nil {
			t.Fatal(err)
		}
		return c.ListRule
	}

	// collections import
	col, _ := app1.FindCollectionByNameOrId("demo2")
	raw, _ := json.Marshal(col)
	data := map[string]any{}
	_ = json.Unmarshal(raw, &data)
	data["listRule"] = "@request.auth.id != ''"
	if err := app1.ImportCollections([]map[string]any{data}, false); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "imported rule on node2", func() bool {
		r := listRule(app2, "demo2")
		return r != nil && *r == "@request.auth.id != ''"
	})

	// app.Save (hooks, cron jobs, migrations)
	col, _ = app1.FindCollectionByNameOrId("demo2")
	col.ListRule = nil
	if err := app1.Save(col); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "app.Save rule on node2", func() bool {
		return listRule(app2, "demo2") == nil
	})

	// programmatic settings save
	settings := app1.Settings()
	settings.Meta.AppName = "propagated_" + security.RandomString(5)
	if err := app1.Save(settings); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "settings on node2", func() bool {
		return app2.Settings().Meta.AppName == settings.Meta.AppName
	})

	// a new collection is usable on node2 right away (cache miss fallback)
	newCol := core.NewBaseCollection("bridge_new_" + security.RandomStringWithAlphabet(5, "abcdefghijklmnopqrstuvwxyz"))
	newCol.Fields.Add(&core.TextField{Name: "title"})
	if err := app1.Save(newCol); err != nil {
		t.Fatal(err)
	}
	found, err := app2.FindCachedCollectionByNameOrId(newCol.Name)
	if err != nil || found.Id != newCol.Id {
		t.Fatalf("Expected the new collection to be found on node2, got %v (%v)", found, err)
	}
}

// Item 9: the client is announced at connect, so the first subscription can be
// submitted through any node; zero subscriptions and remote delete
// notifications never close a live connection.
func TestRealtimeBridge_SubscribeViaOtherNodeAtConnect(t *testing.T) {
	nodes := newBridgeCluster(t, 2, false)
	app1, app2 := nodes[0].app, nodes[1].app

	server := httptest.NewServer(nodes[0].mux)
	defer server.Close()

	sse := connectSSE(t, server.URL)
	defer sse.close()
	clientId := sse.clientId

	subscribe := func(node *bridgeNode, subs ...string) int {
		body, _ := json.Marshal(map[string]any{"clientId": clientId, "subscriptions": subs})
		req := httptest.NewRequest(http.MethodPost, "/api/realtime", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		req.RemoteAddr = "127.0.0.1:1234"
		rec := httptest.NewRecorder()
		node.mux.ServeHTTP(rec, req)
		return rec.Code
	}

	// right after PB_CONNECT, through the other node
	if code := subscribe(nodes[1], "demo2/*"); code != http.StatusNoContent {
		t.Fatalf("Expected 204 for the subscription through the other node, got %d", code)
	}
	local, err := app1.SubscriptionsBroker().ClientById(clientId)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "owner subscriptions", func() bool {
		return local.HasSubscription("demo2/*")
	})

	col, _ := app2.FindCollectionByNameOrId("demo2")
	rec := core.NewRecord(col)
	rec.Set("title", "created on node2")
	if err := app2.Save(rec); err != nil {
		t.Fatal(err)
	}
	if ev := sse.next(t, 5*time.Second); ev.name != "demo2/*" || !strings.Contains(ev.data, rec.Id) {
		t.Fatalf("Unexpected event %s: %s", ev.name, ev.data)
	}

	// zero subscriptions keep the connection and the row
	if code := subscribe(nodes[0]); code != http.StatusNoContent {
		t.Fatalf("Expected 204 for the unsubscribe, got %d", code)
	}
	time.Sleep(500 * time.Millisecond)
	if n := countRows(t, app1, fmt.Sprintf(`SELECT count(*) FROM "_realtimeClients" WHERE "clientId" = '%s'`, clientId)); n != 1 {
		t.Fatalf("Expected the client row to be kept, got %d", n)
	}
	mirror := waitForMirror(t, app2, clientId)
	if mirror.HasSubscription("demo2/*") {
		t.Fatal("Expected the mirror subscriptions to be cleared")
	}

	// a delete notification for a live local client is ignored
	notifyShared(t, app1, map[string]any{"type": "client_delete", "from": "test", "clientId": clientId})
	time.Sleep(500 * time.Millisecond)
	if local.IsDiscarded() || sse.closed() {
		t.Fatal("Expected the live SSE connection to stay open")
	}

	// disconnect
	sse.close()
	waitFor(t, 3*time.Second, "cleanup after disconnect", func() bool {
		_, err := app2.SubscriptionsBroker().ClientById(clientId)
		return err != nil && countRows(t, app1, fmt.Sprintf(`SELECT count(*) FROM "_realtimeClients" WHERE "clientId" = '%s'`, clientId)) == 0
	})
}

// Item 10: messages and subscriptions larger than the NOTIFY limit are delivered.
func TestRealtimeBridge_LargePayloads(t *testing.T) {
	nodes := newBridgeCluster(t, 2, false)
	app1, app2 := nodes[0].app, nodes[1].app

	local := apis.NewBridgedClient(nodes[0].bridge)
	tc := &TestClient{Client: local.Client}
	local.Client = tc
	app1.SubscriptionsBroker().Register(local)

	bigDocs := core.NewBaseCollection("big_docs")
	bigDocs.ListRule = new(string)
	bigDocs.Fields.Add(&core.TextField{Name: "content", Max: 100000})
	if err := app1.Save(bigDocs); err != nil {
		t.Fatal(err)
	}

	// many long subscriptions (validation allows 1000 x 2500 chars)
	subs := []string{"demo2/*", "big_docs/*"}
	for i := range 80 {
		subs = append(subs, fmt.Sprintf("demo2/%015d?options=%s", i, url.QueryEscape(`{"query":{"expand":"rel_one,rel_many","filter":"title = 'a' || title = 'b'"}}`)))
	}
	local.Subscribe(subs...)
	local.BroadcastChanges()
	mirror := waitForMirror(t, app2, local.Id())
	waitFor(t, 3*time.Second, "all subscriptions on the mirror", func() bool {
		got := mirror.RawSubscriptions()
		slices.Sort(got)
		want := slices.Clone(subs)
		slices.Sort(want)
		return slices.Equal(got, want)
	})

	// a large message
	big := `{"action":"update","record":{"id":"x","body":"` + strings.Repeat("a", 20000) + `"}}`
	mirror.Send(subscriptions.Message{Name: "demo2/*", Data: []byte(big)})
	messages := tc.WaitMessages(t, 1, 5*time.Second)
	if string(messages[0].Data) != big {
		t.Fatalf("Expected the large message to be delivered intact, got %d bytes", len(messages[0].Data))
	}

	// a large record written on another node
	col, err := app2.FindCollectionByNameOrId("big_docs")
	if err != nil {
		t.Fatal(err)
	}
	rec := core.NewRecord(col)
	rec.Set("content", strings.Repeat("b", 50000))
	if err := app2.Save(rec); err != nil {
		t.Fatal(err)
	}
	messages = tc.WaitMessages(t, 2, 5*time.Second)
	if messages[1].Name != "big_docs/*" || !strings.Contains(string(messages[1].Data), strings.Repeat("b", 50000)) {
		t.Fatal("Expected the large record event to be delivered intact")
	}

	waitFor(t, 3*time.Second, "outbox drained", func() bool {
		return countRows(t, app1, `SELECT count(*) FROM "_realtimeMessages"`) == 0
	})
}

// Item 11: concurrent local and remote changes of the same client (run with -race).
func TestRealtimeBridge_ConcurrentChanges(t *testing.T) {
	nodes := newBridgeCluster(t, 2, false)
	app1, app2 := nodes[0].app, nodes[1].app

	local := apis.NewBridgedClient(nodes[0].bridge)
	app1.SubscriptionsBroker().Register(local)
	local.Subscribe("demo2/*")
	local.BroadcastChanges()
	mirror := waitForMirror(t, app2, local.Id())

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 20 {
			local.BroadcastChanges()
		}
	}()
	go func() {
		defer wg.Done()
		for range 20 {
			mirror.BroadcastChanges()
			_ = mirror.ClientSubscription()
		}
	}()
	wg.Wait()

	waitFor(t, 3*time.Second, "consistent subscriptions", func() bool {
		return mirror.HasSubscription("demo2/*") && local.HasSubscription("demo2/*")
	})
}

func TestRealtimeBridge_ChannelIdLength(t *testing.T) {
	for _, hostname := range []string{"", "host", strings.Repeat("Long-K8s.Pod-Name", 10)} {
		id := apis.GenRealtimeChannelId(hostname)
		if len(id) > 63 {
			t.Errorf("Expected the channel id of %q to be at most 63 bytes, got %d (%s)", hostname, len(id), id)
		}
		for _, r := range id {
			if !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') && r != '_' {
				t.Errorf("Invalid channel id char %q in %s", r, id)
			}
		}
	}
}

// -------------------------------------------------------------------
// helpers
// -------------------------------------------------------------------

type bridgeNode struct {
	app    *tests.TestApp
	bridge *apis.RealtimeBridge
	mux    http.Handler
}

// newBridgeCluster starts n "pocketbase instances" that share the same
// postgres db, each with its own realtime bridge.
//
// With separateDataDirs each instance has its own data dir (as on separate hosts).
func newBridgeCluster(t *testing.T, n int, separateDataDirs bool) []*bridgeNode {
	t.Helper()

	_, currentFile, _, _ := runtime.Caller(0)
	config := core.BaseAppConfig{
		DataDir:          filepath.Join(path.Dir(currentFile), "..", "tests", "data"),
		EncryptionEnv:    "pb_test_env",
		PostgresURL:      bridgeTestPostgresURL,
		PostgresDataDB:   "pb_test_" + security.RandomStringWithAlphabet(5, "abcdefghijklmnopqrstuvwxyz0123456789"),
		IsRealtimeBridge: true,
	}
	config.PostgresAuxDB = config.PostgresDataDB + "_aux"

	app1, err := tests.NewTestAppWithConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app1.Cleanup)

	nodes := []*bridgeNode{startBridgeNode(t, app1)}
	for i := 1; i < n; i++ {
		var node *bridgeNode
		if separateDataDirs {
			node = addBridgeNodeWithDataDir(t, nodes[0], config.DataDir)
		} else {
			node = addBridgeNode(t, nodes[0])
		}
		nodes = append(nodes, node)
	}

	// wait for the listeners to initialize
	time.Sleep(500 * time.Millisecond)

	return nodes
}

// addBridgeNode starts another instance sharing the data dir and db of first.
func addBridgeNode(t *testing.T, first *bridgeNode) *bridgeNode {
	return addBridgeNodeWithDataDir(t, first, "")
}

func addBridgeNodeWithDataDir(t *testing.T, first *bridgeNode, dataDirToClone string) *bridgeNode {
	t.Helper()

	dataDir := first.app.DataDir()
	if dataDirToClone != "" {
		var err error
		dataDir, err = tests.TempDirClone(dataDirToClone)
		if err != nil {
			t.Fatal(err)
		}
	}

	app := &tests.TestApp{
		BaseApp: core.NewBaseApp(core.BaseAppConfig{
			DataDir:          dataDir,
			EncryptionEnv:    "pb_test_env",
			PostgresURL:      bridgeTestPostgresURL,
			PostgresDataDB:   first.app.PostgresDataDB(),
			PostgresAuxDB:    first.app.PostgresAuxDB(),
			IsRealtimeBridge: true,
		}),
		EventCalls: map[string]int{},
		TestMailer: &tests.TestMailer{},
		// the db is dropped by the first app
		KeepTestPostgresDB: true,
	}
	if err := app.Bootstrap(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Cleanup)

	node := startBridgeNode(t, app)
	time.Sleep(300 * time.Millisecond)

	return node
}

func startBridgeNode(t *testing.T, app *tests.TestApp) *bridgeNode {
	t.Helper()

	router, err := apis.NewRouter(app)
	if err != nil {
		t.Fatal(err)
	}

	serveEvent := new(core.ServeEvent)
	serveEvent.App = app
	serveEvent.Server = &http.Server{}
	serveEvent.Router = router
	err = app.OnServe().Trigger(serveEvent, func(se *core.ServeEvent) error {
		return se.Next()
	})
	if err != nil {
		t.Fatal(err)
	}

	bridge, ok := app.Store().Get(apis.RealtimeBridgeInstanceKey).(*apis.RealtimeBridge)
	if !ok {
		t.Fatal("Failed to create the realtime bridge")
	}

	mux, err := router.BuildMux()
	if err != nil {
		t.Fatal(err)
	}

	return &bridgeNode{app: app, bridge: bridge, mux: mux}
}

func waitFor(t *testing.T, timeout time.Duration, label string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("Timeout waiting for %s", label)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitForMirror(t *testing.T, app core.App, clientId string) apis.IBridgedClient {
	t.Helper()

	var mirror apis.IBridgedClient
	waitFor(t, 3*time.Second, "mirror of "+clientId, func() bool {
		c, err := app.SubscriptionsBroker().ClientById(clientId)
		if err != nil {
			return false
		}
		mirror, _ = c.(apis.IBridgedClient)
		return mirror != nil && mirror.IsRemoteClient() && !mirror.IsDiscarded()
	})

	return mirror
}

func mustExec(t *testing.T, app core.App, query string) {
	t.Helper()

	if _, err := app.DB().NewQuery(query).Execute(); err != nil {
		t.Fatalf("%v: %s", err, query)
	}
}

func countRows(t *testing.T, app core.App, query string) int {
	t.Helper()

	var n int
	if err := app.DB().NewQuery(query).Row(&n); err != nil {
		t.Fatalf("%v: %s", err, query)
	}

	return n
}

func notifyShared(t *testing.T, app core.App, event map[string]any) {
	t.Helper()

	payload, _ := json.Marshal(event)
	_, err := app.DB().NewQuery(`SELECT pg_notify('shared_bridge_channel', {:payload})`).
		Bind(dbx.Params{"payload": string(payload)}).
		Execute()
	if err != nil {
		t.Fatal(err)
	}
}

// terminateListeners kills the LISTEN connections of all instances of the app's db.
func terminateListeners(t *testing.T, app core.App) {
	t.Helper()

	n := countRows(t, app, `SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = current_database() AND query ILIKE 'LISTEN %'`)
	if n == 0 {
		t.Fatal("No LISTEN connections found")
	}
}

// captureNotifications listens on channel with a separate connection and
// returns a func that returns the payloads received so far.
func captureNotifications(t *testing.T, app *tests.TestApp, channel string) func() []string {
	t.Helper()

	u, _ := url.Parse(bridgeTestPostgresURL)
	u.Path = "/" + app.PostgresDataDB()

	ctx, cancel := context.WithCancel(context.Background())
	conn, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var payloads []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				return
			}
			mu.Lock()
			payloads = append(payloads, n.Payload)
			mu.Unlock()
		}
	}()

	t.Cleanup(func() {
		cancel()
		<-done
		conn.Close(context.Background())
	})

	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(payloads)
	}
}

type sseEvent struct {
	name string
	data string
}

type sseConn struct {
	clientId string
	resp     *http.Response
	events   chan sseEvent
	done     chan struct{}
	once     sync.Once
}

func connectSSE(t *testing.T, baseURL string) *sseConn {
	t.Helper()

	resp, err := http.Get(baseURL + "/api/realtime")
	if err != nil {
		t.Fatal(err)
	}

	c := &sseConn{resp: resp, events: make(chan sseEvent, 100), done: make(chan struct{})}
	go func() {
		defer close(c.done)
		reader := bufio.NewReader(resp.Body)
		var ev sseEvent
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\n")
			switch {
			case line == "":
				if ev.name != "" {
					c.events <- ev
				}
				ev = sseEvent{}
			case strings.HasPrefix(line, "event:"):
				ev.name = strings.TrimPrefix(line, "event:")
			case strings.HasPrefix(line, "data:"):
				ev.data = strings.TrimPrefix(line, "data:")
			}
		}
	}()

	connect := c.next(t, 5*time.Second)
	if connect.name != "PB_CONNECT" {
		t.Fatalf("Expected PB_CONNECT, got %s", connect.name)
	}
	var data struct {
		ClientId string `json:"clientId"`
	}
	if err := json.Unmarshal([]byte(connect.data), &data); err != nil {
		t.Fatal(err)
	}
	c.clientId = data.ClientId

	return c
}

func (c *sseConn) next(t *testing.T, timeout time.Duration) sseEvent {
	t.Helper()

	select {
	case ev := <-c.events:
		return ev
	case <-time.After(timeout):
		t.Fatal("Timeout waiting for an SSE event")
	}

	return sseEvent{}
}

func (c *sseConn) closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *sseConn) close() {
	c.once.Do(func() {
		c.resp.Body.Close()
		<-c.done
	})
}

// TestClient captures the messages sent to a local client.
type TestClient struct {
	subscriptions.Client

	mu           sync.Mutex
	sentMessages []subscriptions.Message
}

func (c *TestClient) Send(message subscriptions.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sentMessages = append(c.sentMessages, message)
}

// Messages returns the messages sent so far.
func (c *TestClient) Messages() []subscriptions.Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	return slices.Clone(c.sentMessages)
}

// WaitMessages waits until at least n messages are sent and returns them.
func (c *TestClient) WaitMessages(t *testing.T, n int, timeout time.Duration) []subscriptions.Message {
	t.Helper()

	var messages []subscriptions.Message
	waitFor(t, timeout, fmt.Sprintf("%d message(s)", n), func() bool {
		messages = c.Messages()
		return len(messages) >= n
	})

	return messages
}
