package main

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
)

// bindMailCapture stores every OTP in the superuser-only "mail_outbox"
// collection and suppresses all outgoing mail, so that scenarios can complete
// email-based auth flows without an SMTP server.
func bindMailCapture(app core.App) {
	app.OnMailerRecordOTPSend().BindFunc(func(e *core.MailerRecordEvent) error {
		outbox, err := e.App.FindCachedCollectionByNameOrId("mail_outbox")
		if err != nil {
			return err
		}

		msg := core.NewRecord(outbox)
		msg.Set("to", e.Record.Email())
		msg.Set("kind", "otp")
		msg.Set("otpId", e.Meta["otpId"])
		msg.Set("password", e.Meta["password"])

		// the mail itself is intentionally not sent (no e.Next())
		return e.App.Save(msg)
	})

	app.OnMailerSend().BindFunc(func(e *core.MailerEvent) error {
		return nil // drop all other outgoing mail
	})
}

// bindRoutes registers the custom routes used by the scenarios.
func bindRoutes(se *core.ServeEvent, nodeName string) {
	// node identity, used to assert which instance served a request
	se.Router.GET("/api/prodtest/node", func(e *core.RequestEvent) error {
		channel := ""
		if bridge, ok := e.App.Store().Get(apis.RealtimeBridgeInstanceKey).(apis.IRealtimeBridge); ok {
			channel = bridge.SelfChannelId()
		}
		return e.JSON(http.StatusOK, map[string]string{
			"node":    nodeName,
			"channel": channel,
		})
	})

	g := se.Router.Group("/api/prodtest")
	g.Bind(apis.RequireSuperuserAuth())

	// connection pool statistics of this node (pool sizing and starvation)
	g.GET("/dbstats", func(e *core.RequestEvent) error {
		stats := map[string]sql.DBStats{}
		if db, ok := e.App.NonconcurrentDB().(*dbx.DB); ok {
			stats["data"] = db.DB().Stats()
		}
		if db, ok := e.App.AuxNonconcurrentDB().(*dbx.DB); ok {
			stats["aux"] = db.DB().Stats()
		}
		return e.JSON(http.StatusOK, stats)
	})

	// Read-modify-write inside RunInTransaction: the idiomatic PocketBase
	// pattern that SQLite serializes through its single writer connection.
	g.POST("/counters/{name}/incr", func(e *core.RequestEvent) error {
		name := e.Request.PathValue("name")

		var n int
		err := e.App.RunInTransaction(func(txApp core.App) error {
			counter, err := txApp.FindFirstRecordByData("counters", "name", name)
			if err != nil {
				return err
			}
			n = counter.GetInt("n") + 1
			counter.Set("n", n)
			return txApp.Save(counter)
		})
		if err != nil {
			return e.BadRequestError("increment failed", err)
		}

		return e.JSON(http.StatusOK, map[string]int{"n": n})
	})

	// Balance transfer with the same read-modify-write pattern.
	g.POST("/transfer", func(e *core.RequestEvent) error {
		body := struct {
			From   string `json:"from"`
			To     string `json:"to"`
			Amount int    `json:"amount"`
		}{}
		if err := e.BindBody(&body); err != nil {
			return e.BadRequestError("invalid body", err)
		}
		if body.Amount <= 0 || body.From == body.To {
			return e.BadRequestError("invalid transfer", nil)
		}

		err := e.App.RunInTransaction(func(txApp core.App) error {
			from, err := txApp.FindRecordById("accounts", body.From)
			if err != nil {
				return err
			}
			to, err := txApp.FindRecordById("accounts", body.To)
			if err != nil {
				return err
			}
			if from.GetInt("balance") < body.Amount {
				return errors.New("insufficient funds")
			}
			from.Set("balance", from.GetInt("balance")-body.Amount)
			to.Set("balance", to.GetInt("balance")+body.Amount)
			if err := txApp.Save(from); err != nil {
				return err
			}
			return txApp.Save(to)
		})
		if err != nil {
			return e.BadRequestError("transfer failed", err)
		}

		return e.NoContent(http.StatusNoContent)
	})

	// Changes collection API rules through app.Save (not through the
	// collections Web API), the way Go/JS hooks, cron jobs and CLI tasks do.
	g.POST("/collections/{name}/rules", func(e *core.RequestEvent) error {
		body := struct {
			ListRule *string `json:"listRule"`
			ViewRule *string `json:"viewRule"`
		}{}
		if err := e.BindBody(&body); err != nil {
			return e.BadRequestError("invalid body", err)
		}

		collection, err := e.App.FindCollectionByNameOrId(e.Request.PathValue("name"))
		if err != nil {
			return e.NotFoundError("", err)
		}
		collection.ListRule = body.ListRule
		collection.ViewRule = body.ViewRule
		if err := e.App.Save(collection); err != nil {
			return e.BadRequestError("save failed", err)
		}

		return e.NoContent(http.StatusNoContent)
	})
}
