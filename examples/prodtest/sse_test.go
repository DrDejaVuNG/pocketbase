//go:build prodtest

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type sseEvent struct {
	Name string
	Data string
}

// sseClient is a minimal PocketBase realtime client.
type sseClient struct {
	ClientID string
	base     string // node that holds the SSE stream
	cancel   context.CancelFunc
	events   chan sseEvent
	done     chan struct{}

	mu  sync.Mutex
	err error
}

// connectSSE opens an SSE stream to base and waits for PB_CONNECT.
func connectSSE(t testing.TB, base string) *sseClient {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", base+"/api/realtime", nil)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		cancel()
		t.Fatalf("sse connect %s: %v", base, err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel()
		t.Fatalf("sse connect %s: HTTP %d", base, resp.StatusCode)
	}

	c := &sseClient{
		base:   base,
		cancel: cancel,
		events: make(chan sseEvent, 4096),
		done:   make(chan struct{}),
	}

	go func() {
		defer close(c.done)
		defer close(c.events)
		defer resp.Body.Close()

		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		var ev sseEvent
		var data []string
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if ev.Name != "" || len(data) > 0 {
					ev.Data = strings.Join(data, "\n")
					c.events <- ev
				}
				ev, data = sseEvent{}, nil
			case strings.HasPrefix(line, ":"):
				// comment (keepalive ping)
			case strings.HasPrefix(line, "event:"):
				ev.Name = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			}
		}
		c.mu.Lock()
		c.err = sc.Err()
		c.mu.Unlock()
	}()

	select {
	case ev, ok := <-c.events:
		if !ok || ev.Name != "PB_CONNECT" {
			c.Close()
			t.Fatalf("sse %s: expected PB_CONNECT, got %+v (open=%v)", base, ev, ok)
		}
		var payload struct{ ClientId string }
		json.Unmarshal([]byte(ev.Data), &payload)
		c.ClientID = payload.ClientId
	case <-time.After(15 * time.Second):
		c.Close()
		t.Fatalf("sse %s: no PB_CONNECT", base)
	}

	return c
}

// subscribe sets the client subscriptions through base (which may be a
// different node than the one holding the stream).
func (c *sseClient) subscribe(base, token string, topics ...string) apiResp {
	r, err := call(base, token, "POST", "/api/realtime", map[string]any{
		"clientId":      c.ClientID,
		"subscriptions": topics,
	})
	if err != nil {
		return apiResp{Status: -1, Body: []byte(err.Error())}
	}
	return r
}

// mustSubscribe subscribes through base and fails the test on error.
func (c *sseClient) mustSubscribe(t testing.TB, base, token string, topics ...string) {
	t.Helper()
	if r := c.subscribe(base, token, topics...); r.Status != http.StatusNoContent {
		t.Fatalf("subscribe %v via %s: %s", topics, base, r)
	}
}

// next returns the next event with the given name prefix or an error on timeout.
func (c *sseClient) next(name string, timeout time.Duration) (sseEvent, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-c.events:
			if !ok {
				c.mu.Lock()
				err := c.err
				c.mu.Unlock()
				return sseEvent{}, fmt.Errorf("stream closed (err=%v) waiting for %q", err, name)
			}
			if strings.HasPrefix(ev.Name, name) {
				return ev, nil
			}
		case <-timer.C:
			return sseEvent{}, fmt.Errorf("timeout after %s waiting for %q", timeout, name)
		}
	}
}

// drain discards pending events.
func (c *sseClient) drain() {
	for {
		select {
		case _, ok := <-c.events:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

// quiet returns an error if any event with the given name prefix arrives within d.
func (c *sseClient) quiet(name string, d time.Duration) error {
	ev, err := c.next(name, d)
	if err == nil {
		return fmt.Errorf("unexpected event %s: %s", ev.Name, truncate(ev.Data, 300))
	}
	if strings.HasPrefix(err.Error(), "stream closed") {
		return err
	}
	return nil
}

func (c *sseClient) Close() {
	c.cancel()
	<-c.done
}

type recordEvent struct {
	Action string         `json:"action"`
	Record map[string]any `json:"record"`
}

func decodeRecordEvent(t testing.TB, ev sseEvent) recordEvent {
	t.Helper()
	var re recordEvent
	if err := json.Unmarshal([]byte(ev.Data), &re); err != nil {
		t.Fatalf("decode event %s: %v: %s", ev.Name, err, truncate(ev.Data, 300))
	}
	return re
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
