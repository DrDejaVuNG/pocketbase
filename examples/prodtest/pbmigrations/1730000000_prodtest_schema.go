// Package pbmigrations holds the prodtest schema as a regular app migration,
// so that every node applies it on start (like a real deployment).
package pbmigrations

import (
	"github.com/pocketbase/pocketbase/core"
	m "github.com/pocketbase/pocketbase/migrations"
	"github.com/pocketbase/pocketbase/tools/types"
)

const (
	ruleAuthed    = `@request.auth.id != ""`
	ruleSameOrg   = `@request.auth.id != "" && org = @request.auth.org`
	ruleOwnCreate = `@request.auth.id != "" && org = @request.auth.org && author = @request.auth.id`
)

func init() {
	m.Register(func(app core.App) error {
		orgs := core.NewBaseCollection("orgs")
		orgs.ListRule = types.Pointer(ruleAuthed)
		orgs.ViewRule = types.Pointer(ruleAuthed)
		orgs.Fields.Add(&core.TextField{Name: "name", Required: true})
		if err := app.Save(orgs); err != nil {
			return err
		}

		users, err := app.FindCollectionByNameOrId("users")
		if err != nil {
			return err
		}
		users.ListRule = types.Pointer(`id = @request.auth.id || (org != "" && org = @request.auth.org)`)
		users.ViewRule = users.ListRule
		users.OTP.Enabled = true
		users.Fields.Add(
			&core.RelationField{Name: "org", CollectionId: orgs.Id, MaxSelect: 1},
			&core.SelectField{Name: "role", Values: []string{"member", "admin"}, MaxSelect: 1},
			&core.TextField{Name: "bio"},
		)
		if err := app.Save(users); err != nil {
			return err
		}

		posts := core.NewBaseCollection("posts")
		posts.ListRule = types.Pointer(ruleSameOrg)
		posts.ViewRule = types.Pointer(ruleSameOrg)
		posts.CreateRule = types.Pointer(ruleOwnCreate)
		posts.UpdateRule = types.Pointer(`author = @request.auth.id`)
		posts.Fields.Add(
			&core.TextField{Name: "title", Required: true, Max: 300},
			&core.TextField{Name: "slug"},
			&core.EditorField{Name: "body"},
			&core.SelectField{Name: "status", Values: []string{"draft", "published", "archived"}, MaxSelect: 1},
			&core.SelectField{Name: "tags", Values: []string{"go", "pg", "sql", "web", "ops"}, MaxSelect: 5},
			&core.NumberField{Name: "views", OnlyInt: true},
			&core.NumberField{Name: "rating"},
			&core.BoolField{Name: "featured"},
			&core.JSONField{Name: "meta"},
			&core.DateField{Name: "publishedAt"},
			&core.RelationField{Name: "author", CollectionId: users.Id, MaxSelect: 1},
			&core.RelationField{Name: "org", CollectionId: orgs.Id, MaxSelect: 1, Required: true},
			&core.AutodateField{Name: "created", OnCreate: true},
			&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true},
		)
		posts.AddIndex("idx_posts_slug", true, "slug", "slug != ''")
		posts.AddIndex("idx_posts_org_created", false, "org, created", "")
		if err := app.Save(posts); err != nil {
			return err
		}

		comments := core.NewBaseCollection("comments")
		comments.ListRule = types.Pointer(`@request.auth.id != "" && post.org = @request.auth.org`)
		comments.ViewRule = comments.ListRule
		comments.CreateRule = types.Pointer(`@request.auth.id != "" && post.org = @request.auth.org && author = @request.auth.id`)
		comments.Fields.Add(
			&core.RelationField{Name: "post", CollectionId: posts.Id, MaxSelect: 1, Required: true, CascadeDelete: true},
			&core.RelationField{Name: "author", CollectionId: users.Id, MaxSelect: 1},
			&core.TextField{Name: "body", Required: true},
			&core.AutodateField{Name: "created", OnCreate: true},
		)
		if err := app.Save(comments); err != nil {
			return err
		}

		bigDocs := core.NewBaseCollection("big_docs")
		bigDocs.ListRule = types.Pointer(ruleSameOrg)
		bigDocs.ViewRule = types.Pointer(ruleSameOrg)
		bigDocs.CreateRule = types.Pointer(ruleSameOrg)
		bigDocs.Fields.Add(
			&core.TextField{Name: "title", Required: true},
			&core.TextField{Name: "content", Max: 1_000_000},
			&core.RelationField{Name: "org", CollectionId: orgs.Id, MaxSelect: 1, Required: true},
		)
		if err := app.Save(bigDocs); err != nil {
			return err
		}

		// superuser-only collections (nil rules)
		accounts := core.NewBaseCollection("accounts")
		accounts.Fields.Add(
			&core.TextField{Name: "owner", Required: true},
			&core.NumberField{Name: "balance", OnlyInt: true},
		)
		if err := app.Save(accounts); err != nil {
			return err
		}

		counters := core.NewBaseCollection("counters")
		counters.Fields.Add(
			&core.TextField{Name: "name", Required: true},
			&core.NumberField{Name: "n", OnlyInt: true},
		)
		counters.AddIndex("idx_counters_name", true, "name", "")
		if err := app.Save(counters); err != nil {
			return err
		}

		outbox := core.NewBaseCollection("mail_outbox")
		outbox.Fields.Add(
			&core.TextField{Name: "to"},
			&core.TextField{Name: "kind"},
			&core.TextField{Name: "otpId"},
			&core.TextField{Name: "password"},
			&core.AutodateField{Name: "created", OnCreate: true},
		)
		if err := app.Save(outbox); err != nil {
			return err
		}

		stats := core.NewViewCollection("post_stats")
		stats.ListRule = types.Pointer(`@request.auth.id != "" && id = @request.auth.org`)
		stats.ViewRule = stats.ListRule
		stats.ViewQuery = `SELECT p.org AS id, COUNT(p.id) AS total, COALESCE(SUM(p.views), 0) AS "totalViews" FROM posts p GROUP BY p.org`
		return app.Save(stats)
	}, func(app core.App) error {
		for _, name := range []string{"post_stats", "mail_outbox", "counters", "accounts", "big_docs", "comments", "posts"} {
			c, err := app.FindCollectionByNameOrId(name)
			if err != nil {
				return err
			}
			if err := app.Delete(c); err != nil {
				return err
			}
		}

		users, err := app.FindCollectionByNameOrId("users")
		if err != nil {
			return err
		}
		users.Fields.RemoveByName("org")
		users.Fields.RemoveByName("role")
		users.Fields.RemoveByName("bio")
		users.OTP.Enabled = false
		if err := app.Save(users); err != nil {
			return err
		}

		orgs, err := app.FindCollectionByNameOrId("orgs")
		if err != nil {
			return err
		}
		return app.Delete(orgs)
	})
}
