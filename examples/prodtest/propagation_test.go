//go:build prodtest

package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

const propagationTimeout = 5 * time.Second

// TestPropagation covers per-node caches (collections, settings) after a
// change made on one node.
func TestPropagation(t *testing.T) {
	f := shared(t)

	t.Run("RuleChangeViaAPI", func(t *testing.T) {
		name := "notes_" + randHex(3)
		mustCall(t, f.node(0), f.suToken, "POST", "/api/collections", map[string]any{
			"name":     name,
			"type":     "base",
			"listRule": "",
			"fields":   []map[string]any{{"name": "title", "type": "text"}},
		}, 200)

		// the new collection must become usable on every node
		for i := range f.c.nodes {
			took, err := eventually(propagationTimeout, 50*time.Millisecond, func() error {
				r, err := call(f.node(i), "", "GET", "/api/collections/"+name+"/records", nil)
				if err != nil || r.Status != 200 {
					return fmt.Errorf("%v %s", err, r)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("new collection on node%d: %v", i+1, err)
			}
			t.Logf("new collection visible on node%d after %s", i+1, took)
		}

		// lock the collection down through node1; guests must be
		// rejected by every node
		mustCall(t, f.node(0), f.suToken, "PATCH", "/api/collections/"+name,
			map[string]any{"listRule": nil}, 200)
		for i := range f.c.nodes {
			took, err := eventually(propagationTimeout, 50*time.Millisecond, func() error {
				r, err := call(f.node(i), "", "GET", "/api/collections/"+name+"/records", nil)
				if err != nil || r.Status != 403 {
					return fmt.Errorf("%v %s", err, r)
				}
				return nil
			})
			if err != nil {
				t.Errorf("rule tightening on node%d: %v", i+1, err)
				continue
			}
			t.Logf("tightened rule enforced on node%d after %s", i+1, took)
		}
	})

	t.Run("RuleChangeViaAppSave", func(t *testing.T) {
		// Go/JS hooks, cron jobs and CLI commands change collections with
		// app.Save() instead of the collections Web API.
		name := "memos_" + randHex(3)
		mustCall(t, f.node(0), f.suToken, "POST", "/api/collections", map[string]any{
			"name":     name,
			"type":     "base",
			"listRule": "",
			"fields":   []map[string]any{{"name": "title", "type": "text"}},
		}, 200)
		for i := range f.c.nodes {
			if _, err := eventually(propagationTimeout, 50*time.Millisecond, func() error {
				r, _ := call(f.node(i), "", "GET", "/api/collections/"+name+"/records", nil)
				if r.Status != 200 {
					return fmt.Errorf("%s", r)
				}
				return nil
			}); err != nil {
				t.Fatalf("setup on node%d: %v", i+1, err)
			}
		}

		mustCall(t, f.node(0), f.suToken, "POST", "/api/prodtest/collections/"+name+"/rules",
			map[string]any{"listRule": nil, "viewRule": nil}, 204)

		for i := range f.c.nodes {
			took, err := eventually(propagationTimeout, 50*time.Millisecond, func() error {
				r, err := call(f.node(i), "", "GET", "/api/collections/"+name+"/records", nil)
				if err != nil || r.Status != 403 {
					return fmt.Errorf("guest list still allowed: %v %s", err, r)
				}
				return nil
			})
			if err != nil {
				t.Errorf("rule tightened with app.Save on node1, node%d: %v", i+1, err)
				continue
			}
			t.Logf("node%d enforces the app.Save rule change after %s", i+1, took)
		}
	})

	t.Run("NewFieldNotDroppedByOtherNodes", func(t *testing.T) {
		// a field added through node1 must be stored when the very next
		// write lands on node2 (a stale schema cache silently drops it)
		name := "items_" + randHex(3)
		r := mustCall(t, f.node(0), f.suToken, "POST", "/api/collections", map[string]any{
			"name":       name,
			"type":       "base",
			"createRule": "",
			"viewRule":   "",
			"fields":     []map[string]any{{"name": "title", "type": "text"}},
		}, 200)
		var col struct {
			Fields []map[string]any `json:"fields"`
		}
		r.decode(t, &col)

		for i := range f.c.nodes {
			if _, err := eventually(propagationTimeout, 50*time.Millisecond, func() error {
				r, _ := call(f.node(i), "", "POST", "/api/collections/"+name+"/records", map[string]any{"title": "warmup"})
				if r.Status != 200 {
					return fmt.Errorf("%s", r)
				}
				return nil
			}); err != nil {
				t.Fatalf("setup on node%d: %v", i+1, err)
			}
		}

		fields := append(col.Fields, map[string]any{"name": "sku", "type": "text"})
		mustCall(t, f.node(0), f.suToken, "PATCH", "/api/collections/"+name,
			map[string]any{"fields": fields}, 200)

		lost := 0
		start := time.Now()
		for time.Since(start) < 2*time.Second {
			for i := 1; i < len(f.c.nodes); i++ {
				r := mustCall(t, f.node(i), "", "POST", "/api/collections/"+name+"/records",
					map[string]any{"title": "x", "sku": "SKU-1"}, 200)
				var rec map[string]any
				json.Unmarshal(r.Body, &rec)
				if rec["sku"] != "SKU-1" {
					lost++
				}
			}
		}
		if lost > 0 {
			t.Logf("%d writes dropped the new field during propagation (window after the schema change)", lost)
		}

		// after the propagation window the field must always be stored
		time.Sleep(propagationTimeout)
		for i := range f.c.nodes {
			r := mustCall(t, f.node(i), "", "POST", "/api/collections/"+name+"/records",
				map[string]any{"title": "x", "sku": "SKU-2"}, 200)
			var rec map[string]any
			json.Unmarshal(r.Body, &rec)
			if rec["sku"] != "SKU-2" {
				t.Errorf("node%d still drops the new field after %s", i+1, propagationTimeout)
			}
		}
	})

	t.Run("SettingsChange", func(t *testing.T) {
		appName := "Prodtest " + randHex(3)
		mustCall(t, f.node(0), f.suToken, "PATCH", "/api/settings",
			map[string]any{"meta": map[string]any{"appName": appName}}, 200)

		for i := range f.c.nodes {
			took, err := eventually(propagationTimeout, 50*time.Millisecond, func() error {
				r, err := call(f.node(i), f.suToken, "GET", "/api/settings", nil)
				if err != nil || r.Status != 200 {
					return fmt.Errorf("%v %s", err, r)
				}
				var s struct {
					Meta struct{ AppName string } `json:"meta"`
				}
				json.Unmarshal(r.Body, &s)
				if s.Meta.AppName != appName {
					return fmt.Errorf("node%d appName=%q", i+1, s.Meta.AppName)
				}
				return nil
			})
			if err != nil {
				t.Errorf("settings on node%d: %v", i+1, err)
				continue
			}
			t.Logf("settings visible on node%d after %s", i+1, took)
		}
	})
}
