//go:build prodtest

package main

import (
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// parallel runs fn n times concurrently, each call routed to node i%nodes.
func parallel(n int, fn func(i int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			fn(i)
		}()
	}
	close(start)
	wg.Wait()
}

type statusTally struct {
	mu     sync.Mutex
	counts map[int]int
	sample map[int]string
}

func newTally() *statusTally {
	return &statusTally{counts: map[int]int{}, sample: map[int]string{}}
}

func (s *statusTally) add(r apiResp, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := r.Status
	if err != nil {
		status = -1
		r.Body = []byte(err.Error())
	}
	s.counts[status]++
	if _, ok := s.sample[status]; !ok {
		s.sample[status] = r.String()
	}
}

func (s *statusTally) String() string {
	parts := []string{}
	for status, n := range s.counts {
		parts = append(parts, fmt.Sprintf("%d x%d", status, n))
	}
	return strings.Join(parts, ", ")
}

// TestConcurrency covers concurrent writers on several nodes. SQLite
// PocketBase serializes all transactions on one connection; PostgreSQL runs
// them concurrently (READ COMMITTED).
func TestConcurrency(t *testing.T) {
	f := shared(t)

	t.Run("UniqueIndexCollision", func(t *testing.T) {
		slug := "slug-" + randHex(4)
		tally := newTally()
		parallel(24, func(i int) {
			tally.add(call(f.node(i%3), f.alice.Token, "POST", "/api/collections/posts/records",
				map[string]any{"title": "dup", "slug": slug, "org": f.orgA, "author": f.alice.ID}))
		})
		t.Logf("duplicate slug: %s", tally)
		if tally.counts[200] != 1 {
			t.Errorf("expected exactly 1 success, got %d", tally.counts[200])
		}
		if n := 24 - tally.counts[200] - tally.counts[400]; n > 0 {
			t.Errorf("%d requests failed with something else than 400; e.g. %v", n, tally.sample)
		}
	})

	t.Run("DuplicateSignup", func(t *testing.T) {
		email := "dup-" + randHex(4) + "@prodtest.local"
		tally := newTally()
		parallel(24, func(i int) {
			pass := randHex(8)
			tally.add(call(f.node(i%3), "", "POST", "/api/collections/users/records", map[string]any{
				"email": email, "password": pass, "passwordConfirm": pass,
			}))
		})
		t.Logf("duplicate signup: %s", tally)
		if tally.counts[200] != 1 {
			t.Errorf("expected exactly 1 success, got %d", tally.counts[200])
		}
		if n := 24 - tally.counts[200] - tally.counts[400]; n > 0 {
			t.Errorf("%d requests failed with something else than 400; e.g. %v", n, tally.sample)
		}
	})

	t.Run("CounterInTransaction", func(t *testing.T) {
		// read-modify-write in RunInTransaction from 3 nodes
		name := "c-" + randHex(4)
		if _, err := f.createRecord("counters", map[string]any{"name": name, "n": 0}); err != nil {
			t.Fatal(err)
		}

		const n = 150
		tally := newTally()
		parallel(n, func(i int) {
			tally.add(call(f.node(i%3), f.suToken, "POST", "/api/prodtest/counters/"+name+"/incr", nil))
		})

		r := mustCall(t, f.node(0), f.suToken, "GET",
			"/api/collections/counters/records?filter="+urlq("name='"+name+"'"), nil, 200)
		var list struct {
			Items []struct{ N int } `json:"items"`
		}
		r.decode(t, &list)
		final := list.Items[0].N
		t.Logf("counter: %s, final value %d", tally, final)
		if final != tally.counts[200] {
			t.Errorf("lost updates: %d increments succeeded but the counter is %d", tally.counts[200], final)
		}
	})

	t.Run("TransferInvariant", func(t *testing.T) {
		// 24 concurrent clients run 240 transfers between 6 accounts;
		// deadlock aborts (400) are acceptable, lost money is not
		const accounts, initial, workers, perWorker = 6, 1000, 24, 10
		ids := make([]string, accounts)
		for i := range ids {
			id, err := f.createRecord("accounts", map[string]any{"owner": "transfer", "balance": initial})
			if err != nil {
				t.Fatal(err)
			}
			ids[i] = id
		}

		tally := newTally()
		parallel(workers, func(w int) {
			for range perWorker {
				from := rand.IntN(accounts)
				to := (from + 1 + rand.IntN(accounts-1)) % accounts
				tally.add(call(f.node(w%3), f.suToken, "POST", "/api/prodtest/transfer", map[string]any{
					"from": ids[from], "to": ids[to], "amount": 1 + rand.IntN(50),
				}))
			}
		})

		sum := 0
		for _, id := range ids {
			r := mustCall(t, f.node(0), f.suToken, "GET", "/api/collections/accounts/records/"+id, nil, 200)
			var acc struct{ Balance int }
			r.decode(t, &acc)
			if acc.Balance < 0 {
				t.Errorf("account %s has a negative balance %d", id, acc.Balance)
			}
			sum += acc.Balance
		}
		t.Logf("transfers: %s; total %d (want %d); samples %v", tally, sum, accounts*initial, tally.sample)
		if sum != accounts*initial {
			t.Errorf("money was created or destroyed: total %d, want %d", sum, accounts*initial)
		}
	})

	t.Run("OTPSingleUse", func(t *testing.T) {
		r := mustCall(t, f.node(0), "", "POST", "/api/collections/users/request-otp",
			map[string]any{"email": f.alice.Email}, 200)
		var otp struct{ OtpId string }
		r.decode(t, &otp)

		// the OTP mail is sent (and captured) in the background after the response
		var mails struct {
			Items []struct{ Password string } `json:"items"`
		}
		if _, err := eventually(5*time.Second, 100*time.Millisecond, func() error {
			r := mustCall(t, f.node(1), f.suToken, "GET",
				"/api/collections/mail_outbox/records?filter="+urlq("otpId='"+otp.OtpId+"'"), nil, 200)
			r.decode(t, &mails)
			if len(mails.Items) != 1 {
				return fmt.Errorf("expected 1 captured OTP mail, got %d", len(mails.Items))
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		tally := newTally()
		parallel(12, func(i int) {
			tally.add(call(f.node(i%3), "", "POST", "/api/collections/users/auth-with-otp",
				map[string]any{"otpId": otp.OtpId, "password": mails.Items[0].Password}))
		})
		t.Logf("OTP replay: %s", tally)
		if tally.counts[200] != 1 {
			t.Errorf("one OTP authenticated %d sessions", tally.counts[200])
		}
	})

	t.Run("BatchAtomic", func(t *testing.T) {
		marker := "batch-" + randHex(4)
		r := mustCall(t, f.node(0), f.suToken, "POST", "/api/batch", map[string]any{
			"requests": []map[string]any{
				{"method": "POST", "url": "/api/collections/accounts/records", "body": map[string]any{"owner": marker, "balance": 1}},
				{"method": "POST", "url": "/api/collections/accounts/records", "body": map[string]any{"owner": marker, "balance": 2}},
				{"method": "POST", "url": "/api/collections/accounts/records", "body": map[string]any{"balance": 3}}, // owner required
			},
		}, 400)
		_ = r

		r = mustCall(t, f.node(1), f.suToken, "GET",
			"/api/collections/accounts/records?filter="+urlq("owner='"+marker+"'"), nil, 200)
		var list struct{ TotalItems int }
		r.decode(t, &list)
		if list.TotalItems != 0 {
			t.Errorf("failed batch left %d records behind", list.TotalItems)
		}
	})

	t.Run("CascadeDeleteVsCreate", func(t *testing.T) {
		// deleting posts (cascade to comments) while comments are created
		// for them on other nodes must not leave orphan comments
		const posts = 20
		postIDs := make([]string, posts)
		for i := range postIDs {
			postIDs[i] = f.newPost(t, f.node(i%3), f.alice, f.orgA, nil)["id"].(string)
		}

		tally := newTally()
		parallel(posts*4, func(i int) {
			id := postIDs[i/4]
			if i%4 == 0 {
				tally.add(call(f.node(i%3), f.suToken, "DELETE", "/api/collections/posts/records/"+id, nil))
				return
			}
			tally.add(call(f.node(i%3), f.bob.Token, "POST", "/api/collections/comments/records",
				map[string]any{"post": id, "author": f.bob.ID, "body": "hi"}))
		})
		t.Logf("delete vs create: %s", tally)

		for status, sample := range tally.sample {
			if status >= 500 || status < 0 {
				t.Errorf("unexpected failure: %s", sample)
			}
		}

		quoted := make([]string, len(postIDs))
		for i, id := range postIDs {
			quoted[i] = "'" + id + "'"
		}
		orphans, err := pg.queryInt(f.c.dataDB, `SELECT COUNT(*) FROM comments WHERE post IN (`+strings.Join(quoted, ",")+`)`)
		if err != nil {
			t.Fatal(err)
		}
		if orphans > 0 {
			t.Errorf("%d comments reference deleted posts", orphans)
		}
	})
}

func urlq(s string) string {
	return url.QueryEscape(s)
}
