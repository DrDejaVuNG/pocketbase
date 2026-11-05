package apis

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/hook"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/routine"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/security"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/subscriptions"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

type IRealtimeBridge interface {
	// App returns the current app instance.
	App() core.App

	// SelfChannelId returns the channel id of the current instance.
	SelfChannelId() string

	// SendViaBridge sends a message to a remote client via the bridge.
	SendViaBridge(channelId string, clientId string, message subscriptions.Message)
}

var _ IRealtimeBridge = (*RealtimeBridge)(nil)

const (
	// realtimeSharedChannel is the NOTIFY channel shared by all instances.
	realtimeSharedChannel = "shared_bridge_channel"

	// realtimeMaxNotifyPayload is the max NOTIFY payload size sent inline.
	// PostgreSQL rejects payloads of 8000 bytes or more; larger messages
	// are stored in the _realtimeMessages outbox and only their id is notified.
	realtimeMaxNotifyPayload = 7900

	// realtimeMaxChannelIdLength is PostgreSQL's identifier length limit (NAMEDATALEN-1).
	realtimeMaxChannelIdLength = 63
)

// Shared channel event types.
const (
	bridgeEventClientUpsert       = "client_upsert"
	bridgeEventClientDelete       = "client_delete"
	bridgeEventChannelOffline     = "channel_offline"
	bridgeEventCollectionsChanged = "collections_changed"
	bridgeEventSettingsChanged    = "settings_changed"
)

var (
	// realtimeBridgeHeartbeatInterval is how often an instance renews its channel.
	realtimeBridgeHeartbeatInterval = 10 * time.Second

	// realtimeBridgeChannelTTL is how long a channel stays valid without a heartbeat
	// (must be at least 3x the heartbeat interval). It is also the max age of
	// undelivered _realtimeMessages rows.
	realtimeBridgeChannelTTL = 40 * time.Second
)

// bridgeEvent is the JSON envelope of the shared channel notifications.
//
// It carries only ids; the receivers load the client row and its auth record from the db.
type bridgeEvent struct {
	Type      string `json:"type"`
	From      string `json:"from,omitempty"`
	ClientId  string `json:"clientId,omitempty"`
	ChannelId string `json:"channelId,omitempty"`
}

// bridgeMessage is the JSON envelope of the per-instance channel notifications.
//
// Ref is set (and the other fields are empty) when the message is stored in the
// _realtimeMessages outbox because it doesn't fit in a NOTIFY payload.
type bridgeMessage struct {
	Ref      int64  `json:"ref,omitempty"`
	ClientId string `json:"clientId,omitempty"`
	Name     string `json:"name,omitempty"`
	Data     string `json:"data,omitempty"`
}

// RealtimeBridge leverage PostgresSQL's LISTEN/NOTIFY feature to synchronize
// realtime information between different instances of the pocketbase server.
type RealtimeBridge struct {
	channelId string
	app       core.App
	wg        sync.WaitGroup

	heartbeatInterval time.Duration
	channelTTL        time.Duration

	started        atomic.Bool
	reannounceCh   chan struct{}
	reannounceMu   sync.Mutex
	registrationMu sync.Mutex
}

var RealtimeBridgeInstanceKey = "realtime_bridge_instance"

func bindRealtimeBridge(app core.App) {
	ctx, cancel := context.WithCancel(context.Background())
	hostname, _ := os.Hostname()
	bridge := &RealtimeBridge{
		channelId:         genChannelId(hostname),
		app:               app,
		heartbeatInterval: realtimeBridgeHeartbeatInterval,
		channelTTL:        realtimeBridgeChannelTTL,
		reannounceCh:      make(chan struct{}, 1),
	}

	app.OnServe().BindFunc(func(e *core.ServeEvent) error {
		if err := bridge.createTables(); err != nil {
			return err
		}

		// register the channel before accepting any realtime client
		if _, err := bridge.sendHeartbeat(ctx); err != nil {
			return fmt.Errorf("realtime bridge heartbeat: %w", err)
		}

		// allow bridge instance to be accessible from via app.Store()
		app.Store().Set(RealtimeBridgeInstanceKey, bridge)
		bridge.started.Store(true)

		bridge.wg.Add(3)
		go func() { defer bridge.wg.Done(); bridge.heartbeatLoop(ctx) }()
		go func() { defer bridge.wg.Done(); bridge.listenSharedBridgeChannelLoop(ctx) }()
		go func() { defer bridge.wg.Done(); bridge.listenBridgeMessagesLoop(ctx) }()

		// let the running instances pick up collection changes applied by
		// this instance's startup migrations (they run before the hooks below are bound)
		bridge.notifyShared(bridgeEvent{Type: bridgeEventCollectionsChanged})

		return e.Next()
	})

	app.OnTerminate().BindFunc(func(e *core.TerminateEvent) error {
		cancel() // stopping all background goroutines
		bridge.wg.Wait()
		if bridge.started.Load() {
			bridge.goOffline()
		}
		return e.Next()
	})

	// Notify the other instances from the model level success hooks so that every
	// change path is covered (Web API, collections import, app.Save from hooks,
	// cron jobs and migrations). The success hooks run after the transaction commit.
	collectionsChanged := func(e *core.CollectionEvent) error {
		if err := e.Next(); err != nil {
			return err
		}
		bridge.notifyShared(bridgeEvent{Type: bridgeEventCollectionsChanged})
		return nil
	}
	app.OnCollectionAfterCreateSuccess().Bind(&hook.Handler[*core.CollectionEvent]{Func: collectionsChanged, Priority: 999})
	app.OnCollectionAfterUpdateSuccess().Bind(&hook.Handler[*core.CollectionEvent]{Func: collectionsChanged, Priority: 999})
	app.OnCollectionAfterDeleteSuccess().Bind(&hook.Handler[*core.CollectionEvent]{Func: collectionsChanged, Priority: 999})

	settingsChanged := func(e *core.ModelEvent) error {
		if err := e.Next(); err != nil {
			return err
		}
		bridge.notifyShared(bridgeEvent{Type: bridgeEventSettingsChanged})
		return nil
	}
	app.OnModelAfterCreateSuccess(paramsTableName).Bind(&hook.Handler[*core.ModelEvent]{Func: settingsChanged, Priority: 999})
	app.OnModelAfterUpdateSuccess(paramsTableName).Bind(&hook.Handler[*core.ModelEvent]{Func: settingsChanged, Priority: 999})
}

// paramsTableName is the table of the app settings model (see core.Settings.TableName()).
const paramsTableName = "_params"

// notifyShared sends ev to all instances (including the current one, which ignores it).
func (t *RealtimeBridge) notifyShared(ev bridgeEvent) {
	ev.From = t.channelId
	payload, err := json.Marshal(ev)
	if err == nil {
		_, err = t.app.DB().NewQuery(`SELECT pg_notify({:channel}, {:payload})`).Bind(dbx.Params{
			"channel": realtimeSharedChannel,
			"payload": string(payload),
		}).Execute()
	}
	if err != nil {
		t.app.Logger().Error("Realtime bridge: failed to notify the other instances", "type", ev.Type, "error", err)
	}
}

// listenSharedBridgeChannelLoop listens to the shared bridge channel.
// It is a common communication channel between all pocketbase instances.
// It is used for:
// 1. client upsert/delete events of the _realtimeClients table;
// 2. channel offline events;
// 3. collections and settings change events.
func (t *RealtimeBridge) listenSharedBridgeChannelLoop(ctx context.Context) {
	loopOnNotification(ctx, t.app, realtimeSharedChannel, func() error {
		// (re)connected: notifications may have been missed in the meantime,
		// so resync the remote clients and the caches and re-announce the
		// local clients (in case they were evicted while disconnected)
		if err := t.fullRefreshSubscriptions(); err != nil {
			return err
		}

		if err := t.app.ReloadCachedCollections(); err != nil {
			t.app.Logger().Warn("Realtime bridge: failed to reload the cached collections", "error", err)
		}
		if err := t.app.ReloadSettings(); err != nil {
			t.app.Logger().Warn("Realtime bridge: failed to reload the settings", "error", err)
		}

		return t.reannounceLocalClients()
	}, func(notification *pgconn.Notification) {
		t.handleSharedEvent(notification.Payload)
	})
}

func (t *RealtimeBridge) handleSharedEvent(payload string) {
	var ev bridgeEvent
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		t.app.Logger().Warn("Realtime bridge: invalid shared notification payload", "error", err)
		return
	}

	switch ev.Type {
	case bridgeEventClientUpsert:
		if ev.From == t.channelId {
			return // already applied locally
		}
		t.syncClient(ev.ClientId)
	case bridgeEventClientDelete:
		// note: not skipped when sent by the current instance because
		// the heartbeat garbage collection doesn't update the local mirrors
		t.unregisterRemoteClient(ev.ClientId)
	case bridgeEventChannelOffline:
		if ev.ChannelId == t.channelId {
			// another instance evicted this one (eg. after a stall);
			// the local clients are still connected so announce them again
			if ev.From != t.channelId {
				t.requestReannounce()
			}
			return
		}
		// unregister all remote clients in that channel
		for _, c := range t.app.SubscriptionsBroker().Clients() {
			if bc, ok := c.(IBridgedClient); ok && bc.IsRemoteClient() && bc.ClientSubscription().ChannelId == ev.ChannelId {
				t.app.SubscriptionsBroker().Unregister(bc.Id())
			}
		}
	case bridgeEventCollectionsChanged:
		if ev.From == t.channelId {
			return
		}
		if err := t.app.ReloadCachedCollections(); err != nil {
			t.app.Logger().Warn("Realtime bridge: failed to reload the cached collections", "error", err)
		}
	case bridgeEventSettingsChanged:
		if ev.From == t.channelId {
			return
		}
		if err := t.app.ReloadSettings(); err != nil {
			t.app.Logger().Warn("Realtime bridge: failed to reload the settings", "error", err)
		}
	default:
		t.app.Logger().Warn("Realtime bridge: unknown shared notification type", "type", ev.Type)
	}
}

// syncClient applies the current _realtimeClients row of clientId.
func (t *RealtimeBridge) syncClient(clientId string) {
	row, err := t.loadClientRow(clientId)
	if errors.Is(err, sql.ErrNoRows) {
		t.unregisterRemoteClient(clientId)
		return
	}
	if err != nil {
		t.app.Logger().Error("Realtime bridge: failed to load the realtime client", "clientId", clientId, "error", err)
		return
	}

	t.applyClientRow(row, nil)
}

// applyClientRow updates (or registers) the client described by row.
//
// authCache is optional and is used to load each auth record only once.
func (t *RealtimeBridge) applyClientRow(row *ClientSubscription, authCache map[string]*core.Record) {
	t.registrationMu.Lock()
	defer t.registrationMu.Unlock()

	broker := t.app.SubscriptionsBroker()
	existing, _ := broker.ClientById(row.ClientId)

	if row.ChannelId == t.channelId {
		// only the owner instance creates the rows of its clients,
		// a row without a live local connection is a leftover
		bc, ok := existing.(IBridgedClient)
		if !ok || bc.IsRemoteClient() || bc.IsDiscarded() {
			t.deleteClientRow(row.ClientId)
			return
		}
		bc.ReceiveChanges(row, t.loadAuth(row, authCache))
		return
	}

	if existing == nil {
		mirror := NewBridgedClient(t, row)
		mirror.ReceiveChanges(row, t.loadAuth(row, authCache))
		broker.Register(mirror)
		return
	}

	bc, ok := existing.(IBridgedClient)
	if !ok || !bc.IsRemoteClient() {
		t.app.Logger().Warn("Realtime bridge: remote client id clashes with a local client", "clientId", row.ClientId)
		return
	}
	bc.ReceiveChanges(row, t.loadAuth(row, authCache))
}

// loadAuth loads the auth record referenced by row (nil for guests or missing records).
func (t *RealtimeBridge) loadAuth(row *ClientSubscription, cache map[string]*core.Record) *core.Record {
	if row.AuthCollectionRef == "" || row.AuthRecordRef == "" {
		return nil
	}

	key := row.AuthCollectionRef + "/" + row.AuthRecordRef
	if cache != nil {
		if record, ok := cache[key]; ok {
			return record
		}
	}

	record, err := t.app.FindRecordById(row.AuthCollectionRef, row.AuthRecordRef)
	if err != nil || !record.Collection().IsAuth() {
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.app.Logger().Warn("Realtime bridge: failed to load the client auth record", "clientId", row.ClientId, "error", err)
		}
		record = nil
	}

	if cache != nil {
		cache[key] = record
	}

	return record
}

// unregisterRemoteClient removes the remote client with the specified id (if any).
//
// Local clients are never removed because their connection is still alive.
func (t *RealtimeBridge) unregisterRemoteClient(clientId string) {
	t.registrationMu.Lock()
	defer t.registrationMu.Unlock()

	c, err := t.app.SubscriptionsBroker().ClientById(clientId)
	if err != nil {
		return
	}
	if bc, ok := c.(IBridgedClient); ok && bc.IsRemoteClient() {
		t.app.SubscriptionsBroker().Unregister(clientId)
	}
}

// findOrLoadClient returns the registered client with the specified id.
//
// If the client is not registered yet (eg. its online notification is still
// in flight), it is loaded from the _realtimeClients table.
func (t *RealtimeBridge) findOrLoadClient(clientId string) (subscriptions.Client, error) {
	if c, err := t.app.SubscriptionsBroker().ClientById(clientId); err == nil {
		return c, nil
	}

	row, err := t.loadClientRow(clientId)
	if err != nil {
		return nil, err
	}
	if row.ChannelId == t.channelId {
		return nil, errors.New("missing local client")
	}

	t.applyClientRow(row, nil)

	return t.app.SubscriptionsBroker().ClientById(clientId)
}

// note: the subscriptions array is selected as JSON to avoid sharing a
// (non goroutine safe) pgtype.Map between the listeners and the requests
const clientRowColumns = `"clientId", "channelId", to_json("subscriptions")::text, "authCollectionRef", "authRecordRef", "updatedByChannelId", "ip", "version"`

func scanClientRow(scan func(dest ...any) error) (*ClientSubscription, error) {
	row := &ClientSubscription{}
	var subscriptionsJSON string
	err := scan(
		&row.ClientId,
		&row.ChannelId,
		&subscriptionsJSON,
		&row.AuthCollectionRef,
		&row.AuthRecordRef,
		&row.UpdatedByChannelId,
		&row.IP,
		&row.Version,
	)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(subscriptionsJSON), &row.Subscriptions); err != nil {
		return nil, fmt.Errorf("decoding the client subscriptions: %w", err)
	}
	return row, nil
}

func (t *RealtimeBridge) loadClientRow(clientId string) (*ClientSubscription, error) {
	var row *ClientSubscription
	var scanErr error
	rows, err := t.app.DB().NewQuery(`SELECT ` + clientRowColumns + ` FROM "_realtimeClients" WHERE "clientId" = {:clientId}`).
		Bind(dbx.Params{"clientId": clientId}).
		Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		row, scanErr = scanClientRow(rows.Scan)
		if scanErr != nil {
			return nil, scanErr
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if row == nil {
		return nil, sql.ErrNoRows
	}

	return row, nil
}

// deleteClientRow deletes the row of a client of the current instance.
func (t *RealtimeBridge) deleteClientRow(clientId string) {
	_, err := t.app.DB().NewQuery(`
		WITH deleted AS (
			DELETE FROM "_realtimeClients"
			WHERE "clientId" = {:clientId} AND "channelId" = {:channelId}
			RETURNING "clientId"
		)
		SELECT pg_notify('shared_bridge_channel', json_build_object('type', 'client_delete', 'from', {:channelId}::text, 'clientId', "clientId")::text)
		FROM deleted
	`).Bind(dbx.Params{
		"clientId":  clientId,
		"channelId": t.channelId,
	}).Execute()
	if err != nil {
		t.app.Logger().Error("Realtime bridge: failed to delete the realtime client", "clientId", clientId, "error", err)
	}
}

func (t *RealtimeBridge) SendViaBridge(channelId string, clientId string, message subscriptions.Message) {
	if channelId == t.SelfChannelId() {
		t.app.Logger().Error("Realtime bridge: cannot send a bridged message to the self channel", "channelId", channelId)
		return
	}

	payload, err := json.Marshal(bridgeMessage{
		ClientId: clientId,
		Name:     message.Name,
		Data:     string(message.Data),
	})
	if err != nil {
		t.app.Logger().Error("Realtime bridge: failed to encode the message", "clientId", clientId, "error", err)
		return
	}

	params := dbx.Params{
		"channelId": channelId,
		"payload":   string(payload),
	}

	if len(payload) <= realtimeMaxNotifyPayload {
		_, err = t.app.DB().NewQuery(`SELECT pg_notify({:channelId}, {:payload})`).Bind(params).Execute()
	} else {
		// too large for NOTIFY: store it in the outbox and notify its id
		_, err = t.app.DB().NewQuery(`
			WITH msg AS (
				INSERT INTO "_realtimeMessages" ("channelId", "payload")
				VALUES ({:channelId}, {:payload})
				RETURNING "id"
			)
			SELECT pg_notify({:channelId}, json_build_object('ref', "id")::text) FROM msg
		`).Bind(params).Execute()
	}
	if err != nil {
		t.app.Logger().Error(
			"Realtime bridge: failed to send the message",
			"channelId", channelId,
			"clientId", clientId,
			"size", len(payload),
			"error", err,
		)
	}
}

func (t *RealtimeBridge) listenBridgeMessagesLoop(ctx context.Context) {
	loopOnNotification(ctx, t.app, t.SelfChannelId(), nil, func(notification *pgconn.Notification) {
		payload := notification.Payload

		// never block the listener on a client (or on fetching an outbox message)
		routine.FireAndForget(func() {
			t.deliverMessage(payload)
		})
	})
}

func (t *RealtimeBridge) deliverMessage(payload string) {
	var msg bridgeMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		t.app.Logger().Warn("Realtime bridge: invalid message payload", "error", err)
		return
	}

	if msg.Ref != 0 {
		var stored string
		err := t.app.DB().NewQuery(`
			DELETE FROM "_realtimeMessages"
			WHERE "id" = {:id} AND "channelId" = {:channelId}
			RETURNING "payload"
		`).Bind(dbx.Params{
			"id":        msg.Ref,
			"channelId": t.channelId,
		}).Row(&stored)
		if err != nil {
			t.app.Logger().Error("Realtime bridge: failed to load the stored message", "ref", msg.Ref, "error", err)
			return
		}
		msg = bridgeMessage{}
		if err := json.Unmarshal([]byte(stored), &msg); err != nil {
			t.app.Logger().Warn("Realtime bridge: invalid stored message payload", "error", err)
			return
		}
	}

	c, err := t.app.SubscriptionsBroker().ClientById(msg.ClientId)
	if err != nil {
		t.app.Logger().Debug("Realtime bridge: client not found, it may be already disconnected", "clientId", msg.ClientId)
		return
	}

	// the message was meant for a client of another instance
	if bc, ok := c.(IBridgedClient); ok && bc.IsRemoteClient() {
		t.app.Logger().Warn("Realtime bridge: message sent to the wrong channel", "clientId", msg.ClientId)
		return
	}

	c.Send(subscriptions.Message{
		Name: msg.Name,
		Data: []byte(msg.Data),
	})
}

// fullRefreshSubscriptions reloads all remote clients from the _realtimeClients table.
//
// Local clients are never touched (their state is owned by the current instance).
func (t *RealtimeBridge) fullRefreshSubscriptions() error {
	rows, err := t.app.DB().NewQuery(`SELECT ` + clientRowColumns + ` FROM "_realtimeClients" WHERE "channelId" <> {:selfChannelId}`).
		Bind(dbx.Params{"selfChannelId": t.channelId}).
		Rows()
	if err != nil {
		return fmt.Errorf("loading the realtime clients: %w", err)
	}
	defer rows.Close()

	remoteRows := []*ClientSubscription{}
	for rows.Next() {
		row, err := scanClientRow(rows.Scan)
		if err != nil {
			return fmt.Errorf("scanning a realtime client: %w", err)
		}
		remoteRows = append(remoteRows, row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("loading the realtime clients: %w", err)
	}
	rows.Close()

	seen := make(map[string]struct{}, len(remoteRows))
	authCache := map[string]*core.Record{}
	for _, row := range remoteRows {
		seen[row.ClientId] = struct{}{}
		t.applyClientRow(row, authCache)
	}

	for _, c := range t.app.SubscriptionsBroker().Clients() {
		bc, ok := c.(IBridgedClient)
		if !ok || !bc.IsRemoteClient() {
			continue
		}
		if _, ok := seen[bc.Id()]; !ok {
			t.unregisterRemoteClient(bc.Id())
		}
	}

	return nil
}

// requestReannounce schedules a re-announce of the local clients (see heartbeatLoop).
func (t *RealtimeBridge) requestReannounce() {
	select {
	case t.reannounceCh <- struct{}{}:
	default: // already scheduled
	}
}

// reannounceLocalClients (re)writes the rows of all live local clients and
// deletes the rows of the current channel without a live local connection.
func (t *RealtimeBridge) reannounceLocalClients() error {
	t.reannounceMu.Lock()
	defer t.reannounceMu.Unlock()

	var errs []error

	for _, c := range t.app.SubscriptionsBroker().Clients() {
		bc, ok := c.(*BridgedClient)
		if !ok || bc.IsRemoteClient() || bc.IsDiscarded() {
			continue
		}
		if err := bc.broadcast(false); err != nil {
			errs = append(errs, err)
		}
	}

	// note: a client is registered before its row is created, so a row
	// without a registered local client is a leftover (eg. a disconnect race)
	var ownIds []string
	err := t.app.DB().Select("clientId").
		From("_realtimeClients").
		Where(dbx.HashExp{"channelId": t.channelId}).
		Column(&ownIds)
	if err != nil {
		errs = append(errs, err)
	}
	for _, id := range ownIds {
		c, err := t.app.SubscriptionsBroker().ClientById(id)
		if err == nil && !c.IsDiscarded() {
			if bc, ok := c.(IBridgedClient); ok && !bc.IsRemoteClient() {
				continue
			}
		}
		t.deleteClientRow(id)
	}

	if len(errs) > 0 {
		return fmt.Errorf("re-announcing the local realtime clients: %w", errors.Join(errs...))
	}

	return nil
}

// goOffline removes the channel and its clients on graceful shutdown.
func (t *RealtimeBridge) goOffline() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := t.app.DB().NewQuery(`
		WITH
			deleted_channel AS (
				DELETE FROM "_realtimeChannels" WHERE "channelId" = {:channelId}
			),
			deleted_clients AS (
				DELETE FROM "_realtimeClients" WHERE "channelId" = {:channelId}
			)
		SELECT pg_notify('shared_bridge_channel', json_build_object('type', 'channel_offline', 'from', {:channelId}::text, 'channelId', {:channelId}::text)::text)
	`).WithContext(ctx).Bind(dbx.Params{
		"channelId": t.channelId,
	}).Execute()
	if err != nil {
		t.app.Logger().Error("Realtime bridge: failed to unregister the channel on shutdown", "error", err)
	}
}

func (t *RealtimeBridge) createTables() error {
	// note: the advisory lock serializes concurrently starting instances
	// (concurrent CREATE TABLE IF NOT EXISTS could fail with a unique violation)
	_, err := t.app.DB().NewQuery(`
		SELECT pg_advisory_xact_lock(hashtext('pb_realtime_bridge_tables'));
		CREATE TABLE IF NOT EXISTS "_realtimeChannels" (
			"channelId" TEXT PRIMARY KEY,
			"validUntil" TIMESTAMPTZ NOT NULL
		);
		CREATE TABLE IF NOT EXISTS "_realtimeClients" (
			"clientId" TEXT PRIMARY KEY,
			"channelId" TEXT NOT NULL,
			"subscriptions" TEXT[] NOT NULL,
			"authCollectionRef" TEXT NOT NULL,
			"authRecordRef" TEXT NOT NULL,
			"updatedByChannelId" TEXT NOT NULL
		);
		ALTER TABLE "_realtimeClients" ADD COLUMN IF NOT EXISTS "ip" TEXT NOT NULL DEFAULT '';
		ALTER TABLE "_realtimeClients" ADD COLUMN IF NOT EXISTS "version" BIGINT NOT NULL DEFAULT 0;
		CREATE SEQUENCE IF NOT EXISTS "_realtimeClients_version_seq";
		CREATE INDEX IF NOT EXISTS "_realtimeClients_channelId_idx" ON "_realtimeClients" ("channelId");
		CREATE UNLOGGED TABLE IF NOT EXISTS "_realtimeMessages" (
			"id" BIGSERIAL PRIMARY KEY,
			"channelId" TEXT NOT NULL,
			"payload" TEXT NOT NULL,
			"created" TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`).Execute()
	if err != nil {
		return fmt.Errorf("creating realtime bridge tables: %w", err)
	}
	return nil
}

// heartbeatLoop periodically updates its status in the _realtimeChannels table
// to tell other pocketbase instances that it is still alive.
// It also helps to broadcast the channel offline event to all pocketbase instances
// if any of them is offline and re-announces the local clients if the current
// instance was considered offline.
func (t *RealtimeBridge) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(t.heartbeatInterval)
	defer ticker.Stop()

	pendingReannounce := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-t.reannounceCh:
			pendingReannounce = true
		}

		expired, err := t.sendHeartbeat(ctx)
		if err != nil {
			if ctx.Err() == nil {
				t.app.Logger().Error("Realtime bridge: failed to send the heartbeat", "error", err)
			}
			continue
		}

		if expired || pendingReannounce {
			if err := t.reannounceLocalClients(); err != nil {
				t.app.Logger().Error("Realtime bridge: failed to re-announce the local clients", "error", err)
				pendingReannounce = true // retry with the next tick
			} else {
				pendingReannounce = false
			}
		}

		t.deleteStaleMessages(ctx)
	}
}

// sendHeartbeat renews the current channel, evicts the expired channels of
// the other instances and removes the client rows without a channel.
//
// It returns true if the current channel was missing or expired (the other
// instances may have evicted it together with the local clients rows).
func (t *RealtimeBridge) sendHeartbeat(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, t.heartbeatInterval)
	defer cancel()

	var notified int
	var expired bool
	err := t.app.DB().NewQuery(`
		WITH
			previous AS (
				SELECT "validUntil" FROM "_realtimeChannels" WHERE "channelId" = {:channelId}
			),
			renewed AS (
				INSERT INTO "_realtimeChannels" ("channelId", "validUntil")
				VALUES ({:channelId}, now() + make_interval(secs => {:ttl}))
				ON CONFLICT ("channelId") DO UPDATE
				SET "validUntil" = EXCLUDED."validUntil"
			),
			dead_channels AS (
				DELETE FROM "_realtimeChannels"
				WHERE "validUntil" < now() AND "channelId" <> {:channelId}
				RETURNING "channelId"
			),
			orphan_clients AS (
				DELETE FROM "_realtimeClients" c
				WHERE c."channelId" <> {:channelId} AND (
					c."channelId" IN (SELECT "channelId" FROM dead_channels) OR
					NOT EXISTS (SELECT 1 FROM "_realtimeChannels" ch WHERE ch."channelId" = c."channelId")
				)
				RETURNING c."clientId", c."channelId"
			),
			notified AS (
				SELECT pg_notify('shared_bridge_channel', json_build_object('type', 'channel_offline', 'from', {:channelId}::text, 'channelId', "channelId")::text)
				FROM dead_channels
				UNION ALL
				SELECT pg_notify('shared_bridge_channel', json_build_object('type', 'client_delete', 'from', {:channelId}::text, 'clientId', "clientId")::text)
				FROM orphan_clients
				WHERE "channelId" NOT IN (SELECT "channelId" FROM dead_channels)
			)
		SELECT
			(SELECT count(*) FROM notified),
			COALESCE((SELECT "validUntil" < now() FROM previous), true)
	`).WithContext(ctx).Bind(dbx.Params{
		"channelId": t.channelId,
		"ttl":       t.channelTTL.Seconds(),
	}).Row(&notified, &expired)

	return expired, err
}

// deleteStaleMessages removes the outbox messages that were never fetched
// (eg. the receiver went offline).
func (t *RealtimeBridge) deleteStaleMessages(ctx context.Context) {
	_, err := t.app.DB().NewQuery(`
		DELETE FROM "_realtimeMessages" WHERE "created" < now() - make_interval(secs => {:ttl})
	`).WithContext(ctx).Bind(dbx.Params{
		"ttl": t.channelTTL.Seconds(),
	}).Execute()
	if err != nil && ctx.Err() == nil {
		t.app.Logger().Warn("Realtime bridge: failed to delete the stale messages", "error", err)
	}
}

func (t *RealtimeBridge) SelfChannelId() string {
	return t.channelId
}

func (t *RealtimeBridge) App() core.App {
	return t.app
}

// genChannelId generates a new unique channel id that is a valid
// PostgreSQL identifier (lowercase, max 63 bytes).
func genChannelId(hostname string) string {
	const prefix = "c_"
	const randLength = 5

	// Normalize the hostname to be a valid Postgres identifier
	// Only allow lowercase letters, numbers and underscores
	hostname = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A') // Convert to lowercase
		}
		return '_'
	}, hostname)

	// leave room for the prefix, the separator and the random suffix
	if maxHostname := realtimeMaxChannelIdLength - len(prefix) - 1 - randLength; len(hostname) > maxHostname {
		hostname = hostname[:maxHostname]
	}

	return prefix + hostname + "_" + security.RandomStringWithAlphabet(randLength, "abcdefghijklmnopqrstuvwxyz0123456789")
}

func loopOnNotification(ctx context.Context, app core.App, channel string, afterListenFunc func() error, handler func(*pgconn.Notification)) {
	for {
		if ctx.Err() != nil {
			return
		}

		err := runPgxCommand(ctx, app, func(pgxConn *pgx.Conn) error {
			_, err := pgxConn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize())
			if err != nil {
				return fmt.Errorf("error listening on channel %s: %w", channel, err)
			}
			// the connection goes back to the pool: stop buffering notifications on it
			defer func() {
				unlistenCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, _ = pgxConn.Exec(unlistenCtx, "UNLISTEN *")
			}()

			if afterListenFunc != nil {
				if err := afterListenFunc(); err != nil {
					return fmt.Errorf("error initializing channel %s: %w", channel, err)
				}
			}

			for {
				notification, err := pgxConn.WaitForNotification(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("error waiting for notification on channel %s: %w", channel, err)
				}
				safeHandle(app, channel, handler, notification)
			}
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			app.Logger().Error("Realtime bridge: listener error, reconnecting", slog.String("channel", channel), slog.String("error", err.Error()))
			if !wait(ctx, time.Second) {
				return
			}
		}
	}
}

// safeHandle runs handler and recovers from a panic so that it doesn't stop the listener.
func safeHandle(app core.App, channel string, handler func(*pgconn.Notification), notification *pgconn.Notification) {
	defer func() {
		if r := recover(); r != nil {
			app.Logger().Error("Realtime bridge: notification handler panic", slog.String("channel", channel), slog.Any("panic", r))
		}
	}()

	handler(notification)
}

func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// runPgxCommand aquires a connection from connection pool, runs the provided pgxFunc,
// and put the connection back to the pool.
// *dbx.DB -> *sql.DB -> *sql.Conn -> *pgx.Conn
func runPgxCommand(ctx context.Context, app core.App, pgxFunc func(*pgx.Conn) error) error {
	dbxDB, _ := app.NonconcurrentDB().(*dbx.DB)
	if dbxDB == nil {
		return fmt.Errorf("app.NonconcurrentDB() is not a *dbx.DB instance")
	}
	sqlDB := dbxDB.DB() // *sql.DB, a connection pool
	sqlConn, err := sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("error acquiring SQL connection: %w", err)
	}
	defer sqlConn.Close() // Put back to connection pool

	return sqlConn.Raw(func(driverConn any) error {
		stdlibConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("driverConn is not a *stdlib.Conn instance, are you using the pgx's database/sql driver?")
		}
		pgxConn := stdlibConn.Conn()
		return pgxFunc(pgxConn)
	})
}
