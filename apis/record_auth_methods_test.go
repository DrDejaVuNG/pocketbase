package apis_test

import (
	"encoding/json/v2"
	"net/http"
	"net/url"
	"testing"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketbase/pocketbase/tools/auth"
)

func TestRecordAuthMethodsList(t *testing.T) {
	t.Parallel()

	scenarios := []tests.ApiScenario{
		{
			Name:            "missing collection",
			Method:          http.MethodGet,
			URL:             "/api/collections/missing/auth-methods",
			ExpectedStatus:  404,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:            "non auth collection",
			Method:          http.MethodGet,
			URL:             "/api/collections/demo1/auth-methods",
			ExpectedStatus:  404,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:           "auth collection with none auth methods allowed",
			Method:         http.MethodGet,
			URL:            "/api/collections/nologin/auth-methods",
			ExpectedStatus: 200,
			ExpectedContent: []string{
				`"password":{"identityFields":[],"enabled":false}`,
				`"oauth2":{"providers":[],"enabled":false}`,
				`"mfa":{"enabled":false,"duration":0}`,
				`"otp":{"enabled":false,"duration":0}`,
			},
			ExpectedEvents: map[string]int{"*": 0},
		},
		{
			Name:           "auth collection with all auth methods allowed",
			Method:         http.MethodGet,
			URL:            "/api/collections/users/auth-methods",
			ExpectedStatus: 200,
			ExpectedContent: []string{
				`"password":{"identityFields":["email","username"],"enabled":true}`,
				`"mfa":{"enabled":true,"duration":1800}`,
				`"otp":{"enabled":true,"duration":300}`,
				`"oauth2":{`,
				`"providers":[{`,
				`"name":"google"`,
				`"name":"gitlab"`,
				`"logo":"<svg`,
				`"logo":""`, // for the legacy fields
				`"state":`,
				`"displayName":`,
				`"codeVerifier":`,
				`"codeChallenge":`,
				`"codeChallengeMethod":`,
				`"authURL":`,
				`redirect_uri="`, // ensures that the redirect_uri is the last url param
			},
			ExpectedEvents: map[string]int{"*": 0},
		},

		// rate limit checks
		// -----------------------------------------------------------
		{
			Name:   "RateLimit rule - nologin:listAuthMethods",
			Method: http.MethodGet,
			URL:    "/api/collections/nologin/auth-methods",
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				app.Settings().RateLimits.Enabled = true
				app.Settings().RateLimits.Rules = []core.RateLimitRule{
					{MaxRequests: 100, Label: "abc"},
					{MaxRequests: 100, Label: "*:listAuthMethods"},
					{MaxRequests: 0, Label: "nologin:listAuthMethods"},
				}
			},
			ExpectedStatus:  429,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
		{
			Name:   "RateLimit rule - *:listAuthMethods",
			Method: http.MethodGet,
			URL:    "/api/collections/nologin/auth-methods",
			BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
				app.Settings().RateLimits.Enabled = true
				app.Settings().RateLimits.Rules = []core.RateLimitRule{
					{MaxRequests: 100, Label: "abc"},
					{MaxRequests: 0, Label: "*:listAuthMethods"},
				}
			},
			ExpectedStatus:  429,
			ExpectedContent: []string{`"data":{}`},
			ExpectedEvents:  map[string]int{"*": 0},
		},
	}

	for _, scenario := range scenarios {
		scenario.Test(t)
	}
}

// note: not parallel because other tests temporary replace the global Apple providers
func TestRecordAuthMethodsAppleResponseMode(t *testing.T) {
	expectedResponseModes := map[string]string{
		auth.NameAppleNative: "",
		auth.NameAppleWeb:    "form_post",
		auth.NameApple:       "form_post", // legacy provider name
	}

	scenario := tests.ApiScenario{
		Method: http.MethodGet,
		URL:    "/api/collections/users/auth-methods",
		BeforeTestFunc: func(t testing.TB, app *tests.TestApp, e *core.ServeEvent) {
			users, err := app.FindCollectionByNameOrId("users")
			if err != nil {
				t.Fatal(err)
			}
			users.OAuth2.Enabled = true
			users.OAuth2.Providers = nil
			for name := range expectedResponseModes {
				users.OAuth2.Providers = append(users.OAuth2.Providers, core.OAuth2ProviderConfig{
					Name:         name,
					ClientId:     "123",
					ClientSecret: "456",
				})
			}
			if err := app.Save(users); err != nil {
				t.Fatal(err)
			}
		},
		ExpectedStatus:  200,
		ExpectedContent: []string{`"authURL":`},
		AfterTestFunc: func(t testing.TB, app *tests.TestApp, res *http.Response) {
			result := struct {
				OAuth2 struct {
					Providers []struct {
						Name    string `json:"name"`
						AuthURL string `json:"authURL"`
					} `json:"providers"`
				} `json:"oauth2"`
			}{}
			if err := json.UnmarshalRead(res.Body, &result); err != nil {
				t.Fatal(err)
			}

			if len(result.OAuth2.Providers) != len(expectedResponseModes) {
				t.Fatalf("Expected %d providers, got %d", len(expectedResponseModes), len(result.OAuth2.Providers))
			}

			for _, p := range result.OAuth2.Providers {
				expected, ok := expectedResponseModes[p.Name]
				if !ok {
					t.Fatalf("Unexpected provider %q", p.Name)
				}

				u, err := url.Parse(p.AuthURL)
				if err != nil {
					t.Fatal(err)
				}

				if mode := u.Query().Get("response_mode"); mode != expected {
					t.Errorf("[%s] Expected response_mode %q, got %q", p.Name, expected, mode)
				}
			}
		},
	}

	scenario.Test(t)
}
