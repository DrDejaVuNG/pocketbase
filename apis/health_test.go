package apis_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tests"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/logger"
)

func TestHealthAPI(t *testing.T) {
	t.Parallel()

	scenarios := []tests.ApiScenario{
		{
			Name:           "GET health status (guest)",
			Method:         http.MethodGet, // automatically matches also HEAD as a side-effect of the Go std mux
			URL:            "/api/health",
			ExpectedStatus: 200,
			ExpectedContent: []string{
				`"code":200`,
				`"data":{}`,
			},
			NotExpectedContent: []string{
				"canBackup",
				"realIP",
				"possibleProxyHeader",
			},
			ExpectedEvents: map[string]int{"*": 0},
		},
		{
			Name:   "GET health status (regular user)",
			Method: http.MethodGet,
			URL:    "/api/health",
			Headers: map[string]string{
				"Authorization": "eyJhbGciOiJIUzI1NiJ9.eyJpZCI6IjRxMXhsY2xtZmxva3UzMyIsInR5cGUiOiJhdXRoIiwiY29sbGVjdGlvbklkIjoiX3BiX3VzZXJzX2F1dGhfIiwiZXhwIjoyNTI0NjA0NDYxLCJyZWZyZXNoYWJsZSI6dHJ1ZX0.ZT3F0Z3iM-xbGgSG3LEKiEzHrPHr8t8IuHLZGGNuxLo",
			},
			ExpectedStatus: 200,
			ExpectedContent: []string{
				`"code":200`,
				`"data":{}`,
			},
			NotExpectedContent: []string{
				"canBackup",
				"realIP",
				"possibleProxyHeader",
			},
			ExpectedEvents: map[string]int{"*": 0},
		},
		{
			Name:   "GET health status (superuser)",
			Method: http.MethodGet,
			URL:    "/api/health",
			Headers: map[string]string{
				"Authorization": "eyJhbGciOiJIUzI1NiJ9.eyJpZCI6InN5d2JoZWNuaDQ2cmhtMCIsInR5cGUiOiJhdXRoIiwiY29sbGVjdGlvbklkIjoicGJjXzMxNDI2MzU4MjMiLCJleHAiOjI1MjQ2MDQ0NjEsInJlZnJlc2hhYmxlIjp0cnVlfQ.UXgO3j-0BumcugrFjbd7j0M4MQvbrLggLlcu_YNGjoY",
			},
			ExpectedStatus: 200,
			ExpectedContent: []string{
				`"code":200`,
				`"data":{`,
				`"canBackup":true`,
				`"realIP"`,
				`"possibleProxyHeader"`,
			},
			ExpectedEvents: map[string]int{"*": 0},
		},
	}

	for _, scenario := range scenarios {
		scenario.Test(t)
	}
}

func TestHealthAPIActivityLog(t *testing.T) {
	// note: no t.Parallel() because the scenarios mutate PB_HEALTH_LOG_SUCCESS
	// which is read once at the api routes registration time

	// force enable the request logs since they are disabled by default in tests
	maxDays := 5

	scenarios := []tests.ApiScenario{
		{
			Name:           "success log is skipped by default",
			Method:         http.MethodGet,
			URL:            "/api/health",
			ExpectedStatus: 200,
			ExpectedContent: []string{
				`"code":200`,
			},
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				app.Settings().Logs.MaxDays = maxDays
			},
			AfterTestFunc: func(t testing.TB, app *tests.TestApp, res *http.Response) {
				// wait for any fire-and-forget activity log goroutine to enqueue
				time.Sleep(100 * time.Millisecond)

				flushLogs(t, app)

				if total := countHealthLogs(t, app); total != 0 {
					t.Fatalf("Expected no activity logs for GET /api/health, got %d", total)
				}
			},
		},
		{
			Name:           "success log is persisted with PB_HEALTH_LOG_SUCCESS",
			Method:         http.MethodGet,
			URL:            "/api/health",
			ExpectedStatus: 200,
			ExpectedContent: []string{
				`"code":200`,
			},
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				app.Settings().Logs.MaxDays = maxDays
			},
			AfterTestFunc: func(t testing.TB, app *tests.TestApp, res *http.Response) {
				var total int

				deadline := time.Now().Add(3 * time.Second)
				for {
					time.Sleep(20 * time.Millisecond)

					flushLogs(t, app)

					if total = countHealthLogs(t, app); total > 0 || time.Now().After(deadline) {
						break
					}
				}

				if total == 0 {
					t.Fatal("Expected at least 1 activity log for GET /api/health")
				}
			},
		},
	}

	t.Setenv("PB_HEALTH_LOG_SUCCESS", "false")
	scenarios[0].Test(t)

	// the second scenario rebinds the api routes with PB_HEALTH_LOG_SUCCESS=true
	t.Setenv("PB_HEALTH_LOG_SUCCESS", "true")
	scenarios[1].Test(t)
}

// flushLogs forces a blocking write of all queued log entries.
func flushLogs(t testing.TB, app core.App) {
	handler, ok := app.Logger().Handler().(*logger.BatchHandler)
	if !ok {
		t.Fatalf("Expected the app logger to use %T, got %T", logger.BatchHandler{}, app.Logger().Handler())
	}

	ctx := context.WithValue(context.Background(), logger.BlockKey, true)
	if err := handler.WriteAll(ctx); err != nil {
		t.Fatalf("Failed to flush the batch logs: %v", err)
	}
}

// countHealthLogs returns the total number of persisted activity logs
// for the health endpoint.
func countHealthLogs(t testing.TB, app core.App) int {
	var total int

	err := app.AuxModelQuery(&core.Log{}).
		Select("count(*)").
		AndWhere(dbx.HashExp{"message": "GET /api/health"}).
		Row(&total)
	if err != nil {
		t.Fatalf("Failed to count the health activity logs: %v", err)
	}

	return total
}
