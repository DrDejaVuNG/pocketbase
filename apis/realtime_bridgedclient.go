package apis

import (
	"slices"
	"sync"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/subscriptions"
)

type IBridgedClient interface {
	subscriptions.Client

	// ClientSubscription returns a copy of the client's last known _realtimeClients row.
	ClientSubscription() *ClientSubscription

	// IsRemoteClient returns false if the client is connected to the current pocketbase instance via SSE.
	// It returns true if the client is connected to another pocketbase instance.
	IsRemoteClient() bool

	// BroadcastGoOffline deletes the client row and notifies all other pocketbase instances
	// that the client is offline (only the instance that owns the client connection does that).
	BroadcastGoOffline()

	// BroadcastChanges stores the client's subscriptions and auth state and
	// notifies all other pocketbase instances about the change.
	//
	// The instance that owns the client connection creates the row if missing,
	// the other instances only update an existing row.
	BroadcastChanges()

	// BroadcastAuthChanges is similar to BroadcastChanges but stores only the client's auth state.
	BroadcastAuthChanges()

	// ReceiveChanges applies the subscription changes received from other pocketbase instances.
	//
	// authRecord is the auth record referenced by newSubscription loaded from the db (nil for guests).
	ReceiveChanges(newSubscription *ClientSubscription, authRecord *core.Record)
}

// ClientSubscription is stored in the _realtimeClients table in PostgreSQL.
// Every time a new client connects to the pocketbase server, a new row is created.
// And other pocketbase servers are notified via Postgres's Listen/Notify feature.
//
// Note that only ids are stored: the other instances load the auth record from the db.
type ClientSubscription struct {
	ClientId           string   `db:"pk,clientId" json:"clientId"`
	ChannelId          string   `db:"channelId" json:"channelId"`
	Subscriptions      []string `db:"subscriptions" json:"subscriptions"`
	AuthCollectionRef  string   `db:"authCollectionRef" json:"authCollectionRef"`
	AuthRecordRef      string   `db:"authRecordRef" json:"authRecordRef"`
	UpdatedByChannelId string   `db:"updatedByChannelId" json:"updatedByChannelId"`
	IP                 string   `db:"ip" json:"ip"`

	// Version increases with every write of the row (from a sequence shared by
	// all rows) and is used to ignore rows older than the already applied state.
	Version int64 `db:"version" json:"version"`
}

func (s *ClientSubscription) clone() *ClientSubscription {
	c := *s
	c.Subscriptions = append([]string{}, s.Subscriptions...) // never nil (NOT NULL column)
	return &c
}

var _ subscriptions.Client = (*BridgedClient)(nil)
var _ IBridgedClient = (*BridgedClient)(nil)

// BridgedClient defines a subscription client that is synchronized between the pocketbase instances.
type BridgedClient struct {
	subscriptions.Client
	bridge       IRealtimeBridge
	mu           sync.RWMutex
	subscription *ClientSubscription

	// syncMu serializes the row writes and the received changes of the client
	syncMu sync.Mutex
}

// NewBridgedClient
// - NewBridgedClient(bridge) creates a local client
// - NewBridgedClient(bridge, optionalSubscription) creates a remote client
func NewBridgedClient(bridge IRealtimeBridge, optionalSubscription ...*ClientSubscription) *BridgedClient {
	var client subscriptions.Client
	var subscription *ClientSubscription

	if len(optionalSubscription) > 0 && optionalSubscription[0] != nil {
		// remote client (client is connected to other pocketbase instance)
		client = subscriptions.NewDefaultClient(optionalSubscription[0].ClientId)
		subscription = optionalSubscription[0].clone()
		subscription.Version = 0 // not applied yet (see ReceiveChanges)
	} else {
		// local client (client is connected to current pocketbase instance)
		client = subscriptions.NewDefaultClient()
		subscription = &ClientSubscription{
			ClientId:      client.Id(),
			ChannelId:     bridge.SelfChannelId(),
			Subscriptions: []string{},
		}
	}

	return &BridgedClient{
		bridge:       bridge,
		Client:       client,
		subscription: subscription,
	}
}

func (r *BridgedClient) BroadcastGoOffline() {
	r.mu.RLock()
	clientId := r.subscription.ClientId
	channelId := r.subscription.ChannelId
	r.mu.RUnlock()

	selfChannelId := r.bridge.SelfChannelId()
	if channelId != selfChannelId {
		return // only the owner instance removes the client
	}

	// delete and notify
	_, err := r.bridge.App().DB().NewQuery(`
		WITH deleted AS (
			DELETE FROM "_realtimeClients"
			WHERE "clientId" = {:clientId} AND "channelId" = {:channelId}
			RETURNING "clientId"
		)
		SELECT pg_notify('shared_bridge_channel', json_build_object('type', 'client_delete', 'from', {:channelId}::text, 'clientId', "clientId")::text)
		FROM deleted
	`).Bind(dbx.Params{
		"clientId":  clientId,
		"channelId": channelId,
	}).Execute()
	if err != nil {
		r.bridge.App().Logger().Error("Realtime bridge: failed to delete the realtime client", "clientId", clientId, "error", err)
	}
}

func (r *BridgedClient) BroadcastChanges() {
	if err := r.broadcast(false); err != nil {
		r.bridge.App().Logger().Error("Realtime bridge: failed to store the realtime client", "clientId", r.Id(), "error", err)
	}
}

func (r *BridgedClient) BroadcastAuthChanges() {
	if err := r.broadcast(true); err != nil {
		r.bridge.App().Logger().Error("Realtime bridge: failed to store the realtime client auth", "clientId", r.Id(), "error", err)
	}
}

// broadcast stores the client state in its _realtimeClients row and notifies
// the other instances (with the client id only).
//
// The owner instance upserts the row while the other instances only update
// the existing row so that a disconnected client is never resurrected.
func (r *BridgedClient) broadcast(authOnly bool) error {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()

	selfChannelId := r.bridge.SelfChannelId()

	var authCollectionRef, authRecordRef string
	if authRecord, ok := r.Get(RealtimeClientAuthKey).(*core.Record); ok && authRecord != nil {
		authCollectionRef = authRecord.Collection().Id
		authRecordRef = authRecord.Id
	}
	subs := r.RawSubscriptions()
	slices.Sort(subs)
	ip, _ := r.Get(RealtimeClientIPKey).(string)

	r.mu.Lock()
	r.subscription.AuthCollectionRef = authCollectionRef
	r.subscription.AuthRecordRef = authRecordRef
	if !authOnly {
		r.subscription.Subscriptions = subs
	}
	r.subscription.UpdatedByChannelId = selfChannelId
	isOwner := r.subscription.ChannelId == selfChannelId
	if isOwner {
		r.subscription.IP = ip
	}
	s := r.subscription.clone()
	r.mu.Unlock()

	params := dbx.Params{
		"clientId":           s.ClientId,
		"channelId":          s.ChannelId,
		"subscriptions":      s.Subscriptions,
		"authCollectionRef":  s.AuthCollectionRef,
		"authRecordRef":      s.AuthRecordRef,
		"updatedByChannelId": s.UpdatedByChannelId,
		"ip":                 s.IP,
		"selfChannelId":      selfChannelId,
	}

	setColumns := `
		"authCollectionRef" = {:authCollectionRef},
		"authRecordRef" = {:authRecordRef},
		"updatedByChannelId" = {:updatedByChannelId}`
	if !authOnly {
		setColumns += `,
		"subscriptions" = {:subscriptions}`
	}
	setColumns += `,
		"version" = nextval('"_realtimeClients_version_seq"')`

	var upsert string
	if isOwner {
		if r.IsDiscarded() {
			return nil // disconnected
		}
		upsert = `
			INSERT INTO "_realtimeClients" (
				"clientId", "channelId", "subscriptions", "authCollectionRef", "authRecordRef", "updatedByChannelId", "ip", "version"
			) VALUES (
				{:clientId}, {:channelId}, {:subscriptions}, {:authCollectionRef}, {:authRecordRef}, {:updatedByChannelId}, {:ip}, nextval('"_realtimeClients_version_seq"')
			) ON CONFLICT ("clientId") DO UPDATE
			SET ` + setColumns + `
			WHERE "_realtimeClients"."channelId" = {:channelId}
			RETURNING "clientId", "version"`
	} else {
		upsert = `
			UPDATE "_realtimeClients"
			SET ` + setColumns + `
			WHERE "clientId" = {:clientId} AND "channelId" = {:channelId}
			RETURNING "clientId", "version"`
	}

	var notified int
	var version int64
	err := r.bridge.App().DB().NewQuery(`
		WITH updated AS (`+upsert+`)
		SELECT
			count(pg_notify('shared_bridge_channel', json_build_object('type', 'client_upsert', 'from', {:selfChannelId}::text, 'clientId', "clientId")::text)),
			COALESCE(max("version"), 0)
		FROM updated
	`).Bind(params).Row(&notified, &version)
	if err != nil {
		return err
	}

	if version > 0 {
		r.mu.Lock()
		r.subscription.Version = version
		r.mu.Unlock()
	}

	if notified == 0 && !isOwner {
		// the row is gone, aka. the client has disconnected from its instance
		if c, err := r.bridge.App().SubscriptionsBroker().ClientById(s.ClientId); err == nil && c == subscriptions.Client(r) {
			r.bridge.App().SubscriptionsBroker().Unregister(s.ClientId)
		}
	}

	return nil
}

func (r *BridgedClient) ReceiveChanges(newSubscription *ClientSubscription, authRecord *core.Record) {
	r.syncMu.Lock()

	r.mu.RLock()
	remote := r.subscription.ChannelId != r.bridge.SelfChannelId()
	stale := newSubscription.Version != 0 && newSubscription.Version <= r.subscription.Version
	r.mu.RUnlock()

	// the row was read before a newer change of this client was applied or written
	if stale {
		r.syncMu.Unlock()
		return
	}

	r.replaceSubscriptions(newSubscription.Subscriptions)

	revoked := false
	if remote {
		r.Client.Set(RealtimeClientIPKey, newSubscription.IP)
	} else if authRecord != nil {
		// the owner knows the tokenKey the client authenticated with:
		// a different one means that the token was revoked (eg. password change)
		prev, _ := r.Client.Get(RealtimeClientAuthKey).(*core.Record)
		revoked = prev != nil && isSameAuth(prev, authRecord) && prev.TokenKey() != authRecord.TokenKey()
	}

	if authRecord == nil || revoked {
		r.Client.Unset(RealtimeClientAuthKey)
	} else {
		r.Client.Set(RealtimeClientAuthKey, authRecord)
	}

	r.mu.Lock()
	s := newSubscription.clone()
	if !remote {
		// the channel and the IP of a local client never change
		s.ChannelId = r.subscription.ChannelId
		s.IP = r.subscription.IP
	}
	r.subscription = s
	r.mu.Unlock()

	r.syncMu.Unlock()

	if revoked {
		r.BroadcastAuthChanges()
	}
}

// replaceSubscriptions replaces the client subscriptions with subs
// without a window where the common subscriptions are missing.
func (r *BridgedClient) replaceSubscriptions(subs []string) {
	var stale []string
	for _, s := range r.Client.RawSubscriptions() {
		if !slices.Contains(subs, s) {
			stale = append(stale, s)
		}
	}
	if len(stale) > 0 {
		r.Client.Unsubscribe(stale...)
	}
	r.Client.Subscribe(subs...)
}

// Send sends the specified message to the client's channel (if not discarded).
func (r *BridgedClient) Send(m subscriptions.Message) {
	if r.IsDiscarded() {
		return
	}

	r.mu.RLock()
	channelId := r.subscription.ChannelId
	r.mu.RUnlock()

	if channelId != r.bridge.SelfChannelId() {
		// send to sibling servers
		r.bridge.SendViaBridge(channelId, r.Id(), m)
	} else {
		// send to connected clients
		r.Client.Send(m)
	}
}

func (r *BridgedClient) IsRemoteClient() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.subscription.ChannelId != r.bridge.SelfChannelId()
}

func (r *BridgedClient) ClientSubscription() *ClientSubscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.subscription.clone()
}
