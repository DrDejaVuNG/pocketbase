package apis

import "time"

// SetRealtimeBridgeTimings changes the heartbeat interval and the channel TTL
// of the realtime bridges bound afterwards and returns a func restoring the defaults.
func SetRealtimeBridgeTimings(heartbeatInterval, channelTTL time.Duration) (restore func()) {
	oldInterval, oldTTL := realtimeBridgeHeartbeatInterval, realtimeBridgeChannelTTL
	realtimeBridgeHeartbeatInterval, realtimeBridgeChannelTTL = heartbeatInterval, channelTTL
	return func() {
		realtimeBridgeHeartbeatInterval, realtimeBridgeChannelTTL = oldInterval, oldTTL
	}
}

// GenRealtimeChannelId exposes genChannelId to the tests.
func GenRealtimeChannelId(hostname string) string {
	return genChannelId(hostname)
}
