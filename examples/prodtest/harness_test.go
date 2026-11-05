//go:build prodtest

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// -------------------------------------------------------------------
// Suite setup
// -------------------------------------------------------------------

var (
	workDir string
	binPath string
	pg      *pgServer
)

func TestMain(m *testing.M) {
	code, err := runSuite(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "prodtest setup failed:", err)
		code = 1
	}
	os.Exit(code)
}

func runSuite(m *testing.M) (int, error) {
	var err error

	workDir, err = os.MkdirTemp("", "prodtest-")
	if err != nil {
		return 1, err
	}

	// PRODTEST_BIN runs the scenarios against a prebuilt backend binary
	// (e.g. to compare two builds of the fork under the same load)
	if binPath = os.Getenv("PRODTEST_BIN"); binPath == "" {
		binPath = filepath.Join(workDir, "prodtest")
		build := exec.Command("go", "build", "-o", binPath, ".")
		build.Stdout, build.Stderr = os.Stderr, os.Stderr
		if err := build.Run(); err != nil {
			return 1, fmt.Errorf("build backend: %w", err)
		}
	}

	pg, err = startPostgres()
	if err != nil {
		return 1, err
	}

	code := m.Run()

	if sharedFixture != nil && sharedFixture.c != nil {
		sharedFixture.c.shutdown()
	}
	if pg.container != "" {
		if out, err := exec.Command("docker", "logs", pg.container).CombinedOutput(); err == nil {
			os.WriteFile(filepath.Join(workDir, "postgres.log"), out, 0o644)
		}
	}
	pg.close()

	if code == 0 && os.Getenv("PRODTEST_KEEP") == "" {
		os.RemoveAll(workDir)
	} else {
		fmt.Fprintln(os.Stderr, "prodtest: node logs kept in", workDir)
	}

	return code, nil
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// -------------------------------------------------------------------
// PostgreSQL server
// -------------------------------------------------------------------

type pgServer struct {
	container string // empty for an external server
	host      string // host:port
	adminURL  *url.URL
	appURL    *url.URL // role used by the PocketBase nodes
	appRole   string
}

// destructive reports whether scenarios may restart the server or change
// server-level settings (only for the harness-managed container).
func (p *pgServer) destructive() bool {
	return p.container != ""
}

func startPostgres() (*pgServer, error) {
	if raw := os.Getenv("PRODTEST_POSTGRES_URL"); raw != "" {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("PRODTEST_POSTGRES_URL: %w", err)
		}
		return &pgServer{host: u.Host, adminURL: u, appURL: u, appRole: u.User.Username()}, nil
	}

	image := os.Getenv("PRODTEST_POSTGRES_IMAGE")
	if image == "" {
		image = "postgres:18-alpine"
	}

	// a fixed host port: "docker restart" would assign a new random one
	p := &pgServer{container: "prodtest-pg-" + randHex(4), appRole: "pbapp", host: freeAddr()}
	adminPass := randHex(16)
	appPass := randHex(16)

	args := []string{"run", "-d", "--rm",
		"--name", p.container,
		"-e", "POSTGRES_USER=prodtest",
		"-e", "POSTGRES_PASSWORD=" + adminPass,
		"-p", p.host + ":5432",
		image,
		"-c", "max_connections=400",
		"-c", "shared_preload_libraries=pg_stat_statements",
	}
	// PRODTEST_POSTGRES_DURABLE keeps the production default WAL durability
	// (commits wait for the WAL flush); needed for realistic write throughput
	if os.Getenv("PRODTEST_POSTGRES_DURABLE") == "" {
		args = append(args,
			"-c", "fsync=off",
			"-c", "synchronous_commit=off",
			"-c", "full_page_writes=off",
		)
	}

	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker run: %w: %s", err, out)
	}

	p.adminURL = &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("prodtest", adminPass),
		Host:     p.host,
		Path:     "/postgres",
		RawQuery: "sslmode=disable",
	}
	p.appURL = &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(p.appRole, appPass),
		Host:     p.host,
		Path:     "/postgres",
		RawQuery: "sslmode=disable",
	}

	if err := p.waitReady(60 * time.Second); err != nil {
		p.close()
		return nil, err
	}

	// the nodes use a regular (non-superuser) role, like on managed Postgres
	if err := p.exec("postgres", fmt.Sprintf(`CREATE ROLE %s LOGIN CREATEDB PASSWORD '%s'`, p.appRole, appPass)); err != nil {
		p.close()
		return nil, err
	}

	// statement statistics for load scenarios (pg_stat_statements view in the "postgres" db)
	if err := p.exec("postgres", `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`); err != nil {
		p.close()
		return nil, err
	}

	return p, nil
}

func (p *pgServer) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, p.adminURL.String())
		if err == nil {
			err = conn.Ping(ctx)
			conn.Close(ctx)
		}
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("postgres not ready: %w", lastErr)
}

func (p *pgServer) close() {
	if p.container != "" {
		exec.Command("docker", "rm", "-f", p.container).Run()
	}
}

func (p *pgServer) restart() error {
	if out, err := exec.Command("docker", "restart", "-t", "5", p.container).CombinedOutput(); err != nil {
		return fmt.Errorf("docker restart: %w: %s", err, out)
	}
	return p.waitReady(60 * time.Second)
}

func (p *pgServer) connect(db string) (*pgx.Conn, error) {
	u := *p.adminURL
	u.Path = "/" + db
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return pgx.Connect(ctx, u.String())
}

func (p *pgServer) exec(db string, sql string, args ...any) error {
	conn, err := p.connect(db)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, err = conn.Exec(ctx, sql, args...)
	return err
}

func (p *pgServer) queryInt(db string, sql string, args ...any) (int64, error) {
	conn, err := p.connect(db)
	if err != nil {
		return 0, err
	}
	defer conn.Close(context.Background())
	var n int64
	err = conn.QueryRow(context.Background(), sql, args...).Scan(&n)
	return n, err
}

func (p *pgServer) dropDB(name string) {
	p.exec("postgres", fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, name))
}

// -------------------------------------------------------------------
// PocketBase nodes
// -------------------------------------------------------------------

type node struct {
	name    string
	url     string
	dir     string
	logPath string
	env     []string

	mu     sync.Mutex
	cmd    *exec.Cmd
	exited chan struct{}

	// inRotation is the load balancer view: set once the node is healthy,
	// cleared before a graceful stop (like an LB deregistration/drain).
	inRotation atomic.Bool
}

// routable reports whether the load balancer may send traffic to the node.
func (n *node) routable() bool {
	return n.inRotation.Load() && n.running()
}

type cluster struct {
	dataDB string
	auxDB  string
	nodes  []*node
	proxy  *roundRobinProxy
}

func freeAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// newCluster prepares (without starting) n nodes sharing a fresh pair of databases.
func newCluster(prefix string, n int, extraEnv ...string) *cluster {
	id := randHex(3)
	c := &cluster{
		dataDB: "pt_" + prefix + "_" + id,
		auxDB:  "pt_" + prefix + "_" + id + "_aux",
	}

	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("%s-node%d", prefix, i)
		addr := freeAddr()
		nd := &node{
			name:    name,
			url:     "http://" + addr,
			dir:     filepath.Join(workDir, c.dataDB, name),
			logPath: filepath.Join(workDir, c.dataDB, name+".log"),
		}
		nd.env = append(cleanEnv(),
			"POSTGRES_URL="+pg.appURL.String(),
			"POSTGRES_DATA_DB="+c.dataDB,
			"POSTGRES_AUX_DB="+c.auxDB,
			"PRODTEST_NODE="+name,
			"PRODTEST_ADDR="+addr,
		)
		nd.env = append(nd.env, extraEnv...)
		c.nodes = append(c.nodes, nd)
	}

	c.proxy = newRoundRobinProxy(c.nodes)

	return c
}

func cleanEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "POSTGRES_") || strings.HasPrefix(kv, "PB_") {
			continue
		}
		env = append(env, kv)
	}
	return env
}

func (n *node) addr() string {
	return strings.TrimPrefix(n.url, "http://")
}

func (n *node) start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := os.MkdirAll(n.dir, 0o755); err != nil {
		return err
	}
	logFile, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	cmd := exec.Command(binPath, "serve", "--dev=false", "--http="+n.addr(), "--dir="+n.dir)
	cmd.Env = n.env
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}

	exited := make(chan struct{})
	go func() {
		cmd.Wait()
		logFile.Close()
		close(exited)
	}()

	n.cmd = cmd
	n.exited = exited

	return nil
}

func (n *node) running() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil {
		return false
	}
	select {
	case <-n.exited:
		return false
	default:
		return true
	}
}

func (n *node) signal(sig syscall.Signal) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil || n.cmd.Process == nil {
		return errors.New("node not started")
	}
	return n.cmd.Process.Signal(sig)
}

// stop terminates the node gracefully (SIGTERM), killing it after a grace period.
func (n *node) stop() {
	n.mu.Lock()
	cmd, exited := n.cmd, n.exited
	n.mu.Unlock()
	if cmd == nil {
		return
	}
	// drain: take the node out of the LB and let in-flight requests finish
	n.inRotation.Store(false)
	time.Sleep(300 * time.Millisecond)
	cmd.Process.Signal(syscall.SIGCONT) // in case it is paused
	cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-exited:
	case <-time.After(15 * time.Second):
		cmd.Process.Kill()
		<-exited
	}
}

func (n *node) kill() {
	n.mu.Lock()
	cmd, exited := n.cmd, n.exited
	n.mu.Unlock()
	if cmd == nil {
		return
	}
	cmd.Process.Kill()
	<-exited
}

func (n *node) waitHealthy(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !n.running() {
			return fmt.Errorf("%s exited during startup:\n%s", n.name, n.logTail(40))
		}
		resp, err := http.Get(n.url + "/api/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				n.inRotation.Store(true)
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("%s not healthy after %s:\n%s", n.name, timeout, n.logTail(40))
}

func (n *node) logTail(lines int) string {
	raw, _ := os.ReadFile(n.logPath)
	all := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	return strings.Join(all, "\n")
}

// startSequential starts the nodes one by one (the first one applies the migrations).
func (c *cluster) startSequential() error {
	for _, n := range c.nodes {
		if err := n.start(); err != nil {
			return err
		}
		if err := n.waitHealthy(90 * time.Second); err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) shutdown() {
	var wg sync.WaitGroup
	for _, n := range c.nodes {
		wg.Add(1)
		go func() { defer wg.Done(); n.stop() }()
	}
	wg.Wait()
	c.proxy.close()
	pg.dropDB(c.dataDB)
	pg.dropDB(c.auxDB)
}

// upsertSuperuser runs the superuser CLI command against the cluster databases.
func (c *cluster) upsertSuperuser(email, password string) error {
	n := c.nodes[0]
	cmd := exec.Command(binPath, "superuser", "upsert", email, password, "--dev=false", "--dir="+n.dir)
	cmd.Env = n.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("superuser upsert: %w: %s", err, out)
	}
	return nil
}

// -------------------------------------------------------------------
// Load balancer without sticky sessions
// -------------------------------------------------------------------

type roundRobinProxy struct {
	srv  *httptest.Server
	next atomic.Uint64
	url  string
}

func newRoundRobinProxy(nodes []*node) *roundRobinProxy {
	p := &roundRobinProxy{}
	proxies := make([]*httputil.ReverseProxy, len(nodes))
	for i, n := range nodes {
		target, _ := url.Parse(n.url)
		rp := httputil.NewSingleHostReverseProxy(target)
		rp.FlushInterval = -1 // stream SSE
		rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			w.WriteHeader(http.StatusBadGateway)
		}
		proxies[i] = rp
	}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// skip nodes that are not running (like a LB with health checks)
		for range nodes {
			i := int(p.next.Add(1) % uint64(len(nodes)))
			if nodes[i].routable() {
				proxies[i].ServeHTTP(w, r)
				return
			}
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	p.url = p.srv.URL
	return p
}

func (p *roundRobinProxy) close() {
	p.srv.CloseClientConnections()
	p.srv.Close()
}

// -------------------------------------------------------------------
// HTTP API client
// -------------------------------------------------------------------

var httpClient = &http.Client{
	Timeout: 60 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 512,
		IdleConnTimeout:     30 * time.Second,
	},
}

type apiResp struct {
	Status int
	Body   []byte
}

func (r apiResp) ok() bool {
	return r.Status >= 200 && r.Status < 300
}

func (r apiResp) String() string {
	body := string(r.Body)
	if len(body) > 600 {
		body = body[:600] + "..."
	}
	return fmt.Sprintf("HTTP %d %s", r.Status, body)
}

func (r apiResp) decode(t testing.TB, out any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, out); err != nil {
		t.Fatalf("decode %s: %v", r, err)
	}
}

func call(base, token, method, path string, body any) (apiResp, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return apiResp{}, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, base+path, reader)
	if err != nil {
		return apiResp{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return apiResp{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	return apiResp{Status: resp.StatusCode, Body: raw}, err
}

// mustCall fails the test on transport errors or on an unexpected status.
func mustCall(t testing.TB, base, token, method, path string, body any, wantStatus int) apiResp {
	t.Helper()
	r, err := call(base, token, method, path, body)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	if r.Status != wantStatus {
		t.Fatalf("%s %s%s: want %d, got %s", method, base, path, wantStatus, r)
	}
	return r
}

// eventually polls fn until it returns nil or the timeout elapses and
// returns the time it took plus the last error.
func eventually(timeout, every time.Duration, fn func() error) (time.Duration, error) {
	start := time.Now()
	for {
		err := fn()
		if err == nil {
			return time.Since(start), nil
		}
		if time.Since(start) > timeout {
			return time.Since(start), err
		}
		time.Sleep(every)
	}
}

// -------------------------------------------------------------------
// Shared fixture (3 nodes + seed data)
// -------------------------------------------------------------------

type user struct {
	ID       string
	Email    string
	Password string
	Token    string
}

type fixture struct {
	c        *cluster
	run      string
	suEmail  string
	suPass   string
	suToken  string
	orgA     string
	orgB     string
	alice    user // org A
	bob      user // org A
	carol    user // org B
	setupErr error
}

var (
	sharedFixture *fixture
	fixtureOnce   sync.Once
)

// shared returns the lazily started 3-node cluster with seed data.
func shared(t *testing.T) *fixture {
	t.Helper()
	fixtureOnce.Do(func() {
		sharedFixture = &fixture{}
		sharedFixture.setupErr = sharedFixture.setup("main", 3)
	})
	if sharedFixture.setupErr != nil {
		t.Fatalf("shared cluster setup: %v", sharedFixture.setupErr)
	}
	return sharedFixture
}

// dedicated starts a separate cluster with seed data for scenarios that
// stop nodes or disturb PostgreSQL; it is shut down with the test.
func dedicated(t *testing.T, prefix string, n int, extraEnv ...string) *fixture {
	t.Helper()
	f := &fixture{}
	err := f.setup(prefix, n, extraEnv...)
	t.Cleanup(func() {
		if f.c != nil {
			f.c.shutdown()
		}
	})
	if err != nil {
		t.Fatalf("%s cluster setup: %v", prefix, err)
	}
	return f
}

func (f *fixture) setup(prefix string, n int, extraEnv ...string) error {
	f.run = randHex(3)
	f.c = newCluster(prefix, n, extraEnv...)
	if err := f.c.startSequential(); err != nil {
		return err
	}

	f.suEmail = "admin-" + f.run + "@prodtest.local"
	f.suPass = randHex(12)
	if err := f.c.upsertSuperuser(f.suEmail, f.suPass); err != nil {
		return err
	}

	r, err := call(f.c.proxy.url, "", "POST", "/api/collections/_superusers/auth-with-password",
		map[string]any{"identity": f.suEmail, "password": f.suPass})
	if err != nil || !r.ok() {
		return fmt.Errorf("superuser auth: %v %s", err, r)
	}
	var auth struct{ Token string }
	json.Unmarshal(r.Body, &auth)
	f.suToken = auth.Token

	r, err = call(f.c.proxy.url, f.suToken, "PATCH", "/api/settings",
		map[string]any{"batch": map[string]any{"enabled": true, "maxRequests": 50}})
	if err != nil || !r.ok() {
		return fmt.Errorf("enable batch: %v %s", err, r)
	}
	for _, n := range f.c.nodes {
		if _, err := eventually(10*time.Second, 50*time.Millisecond, func() error {
			r, err := call(n.url, f.suToken, "GET", "/api/settings", nil)
			if err != nil || !strings.Contains(string(r.Body), `"batch":{"enabled":true`) {
				return fmt.Errorf("%s: batch settings not applied: %v %s", n.name, err, r)
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if f.orgA, err = f.createRecord("orgs", map[string]any{"name": "Org A"}); err != nil {
		return err
	}
	if f.orgB, err = f.createRecord("orgs", map[string]any{"name": "Org B"}); err != nil {
		return err
	}

	if f.alice, err = f.createUser("alice", f.orgA); err != nil {
		return err
	}
	if f.bob, err = f.createUser("bob", f.orgA); err != nil {
		return err
	}
	if f.carol, err = f.createUser("carol", f.orgB); err != nil {
		return err
	}

	return nil
}

func (f *fixture) createRecord(collection string, data map[string]any) (string, error) {
	r, err := call(f.c.proxy.url, f.suToken, "POST", "/api/collections/"+collection+"/records", data)
	if err != nil {
		return "", err
	}
	if !r.ok() {
		return "", fmt.Errorf("create %s: %s", collection, r)
	}
	var rec struct{ ID string }
	json.Unmarshal(r.Body, &rec)
	return rec.ID, nil
}

func (f *fixture) createUser(name, org string) (user, error) {
	u := user{
		Email:    name + "-" + f.run + "@prodtest.local",
		Password: randHex(10),
	}
	id, err := f.createRecord("users", map[string]any{
		"email":           u.Email,
		"password":        u.Password,
		"passwordConfirm": u.Password,
		"name":            strings.ToUpper(name[:1]) + name[1:],
		"org":             org,
		"role":            "member",
		"verified":        true,
		"emailVisibility": false,
	})
	if err != nil {
		return u, err
	}
	u.ID = id

	r, err := call(f.c.proxy.url, "", "POST", "/api/collections/users/auth-with-password",
		map[string]any{"identity": u.Email, "password": u.Password})
	if err != nil || !r.ok() {
		return u, fmt.Errorf("auth %s: %v %s", name, err, r)
	}
	var auth struct{ Token string }
	json.Unmarshal(r.Body, &auth)
	u.Token = auth.Token

	return u, nil
}

// node returns the direct URL of the i-th node (0-based).
func (f *fixture) node(i int) string {
	return f.c.nodes[i].url
}

// createCollection creates a collection through node1 and waits until every
// node serves it, so the scenario does not depend on the propagation delay
// (covered separately by TestPropagation).
func (f *fixture) createCollection(t testing.TB, data map[string]any) {
	t.Helper()
	mustCall(t, f.node(0), f.suToken, "POST", "/api/collections", data, 200)
	name := data["name"].(string)
	for i, n := range f.c.nodes {
		if _, err := eventually(10*time.Second, 50*time.Millisecond, func() error {
			r, err := call(n.url, f.suToken, "GET", "/api/collections/"+name+"/records?perPage=1", nil)
			if err != nil || r.Status != 200 {
				return fmt.Errorf("%v %s", err, r)
			}
			return nil
		}); err != nil {
			t.Fatalf("collection %s on node%d: %v", name, i+1, err)
		}
	}
}

// postData builds a posts record body; kv are extra key/value pairs.
func postData(title, org string, author user, kv ...any) map[string]any {
	body := map[string]any{"title": title, "org": org}
	body["author"] = author.ID
	for i := 0; i+1 < len(kv); i += 2 {
		body[kv[i].(string)] = kv[i+1]
	}
	return body
}

// newPost creates a post as the given user through the given base URL.
func (f *fixture) newPost(t testing.TB, base string, u user, org string, data map[string]any) map[string]any {
	t.Helper()
	body := postData("post "+randHex(3), org, u)
	for k, v := range data {
		body[k] = v
	}
	r := mustCall(t, base, u.Token, "POST", "/api/collections/posts/records", body, 200)
	var rec map[string]any
	r.decode(t, &rec)
	return rec
}
