package apis

import (
	"net/http"
	"os"
	"slices"
	"strconv"

	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/router"
)

// bindHealthApi registers the health api endpoint.
func bindHealthApi(app core.App, rg *router.RouterGroup[*core.RequestEvent]) {
	subGroup := rg.Group("/health")

	// Successful health checks are usually triggered automatically and
	// frequently by client SDKs (e.g. connectivity keepalive probes), which
	// can flood the logs table and dashboard with low-value entries.
	//
	// Set PB_HEALTH_LOG_SUCCESS=true to restore logging of the successful
	// health checks (failed ones are always logged).
	if logSuccess, _ := strconv.ParseBool(os.Getenv("PB_HEALTH_LOG_SUCCESS")); !logSuccess {
		subGroup.Bind(SkipSuccessActivityLog())
	}

	subGroup.GET("", healthCheck)
}

// healthCheck returns a 200 OK response if the server is healthy.
func healthCheck(e *core.RequestEvent) error {
	resp := struct {
		Message string         `json:"message"`
		Code    int            `json:"code"`
		Data    map[string]any `json:"data"`
	}{
		Code:    http.StatusOK,
		Message: "API is healthy.",
	}

	// @todo evaluate whether it is worth removing the extra info from the health endpoint
	if e.HasSuperuserAuth() {
		resp.Data = make(map[string]any, 3)
		resp.Data["canBackup"] = !e.App.Store().Has(core.StoreKeyActiveBackup)
		resp.Data["realIP"] = e.RealIP()

		// loosely check if behind a reverse proxy
		// (usually used in the dashboard to remind superusers in case deployed behind reverse-proxy)
		possibleProxyHeader := ""
		headersToCheck := append(
			slices.Clone(e.App.Settings().TrustedProxy.Headers),
			// common proxy headers
			"CF-Connecting-IP", "Fly-Client-IP", "X-Forwarded-For",
		)
		for _, header := range headersToCheck {
			if e.Request.Header.Get(header) != "" {
				possibleProxyHeader = header
				break
			}
		}
		resp.Data["possibleProxyHeader"] = possibleProxyHeader
	} else {
		resp.Data = map[string]any{} // ensure that it is returned as object
	}

	return e.JSON(http.StatusOK, resp)
}
