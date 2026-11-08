package zerotrust

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io"
	"log"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/cedar-policy/cedar-go"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
)

// quiet drops the server's handshake error logs, the impostor makes them on purpose.
var quiet = log.New(io.Discard, "", 0)

func init() {
	set.Add("policies", "cedar decisions for a grid of callers and actions, no network", policies)
	set.Add("mtls", "services with svids, users without, and an impostor from another trust domain", mtls)
}

func policies(s *Session) {
	owners := map[string]string{"alices-link": "alice"}
	a, err := NewAuthorizer(roles, func(slug string) (string, bool) { o, ok := owners[slug]; return o, ok })
	if err != nil {
		s.Fail(err)
		return
	}
	u := func(n string) cedar.EntityUID { return cedar.NewEntityUID("User", cedar.String(n)) }
	sv := func(n string) cedar.EntityUID { return cedar.NewEntityUID("Service", cedar.String(n)) }
	cases := []struct {
		who                  cedar.EntityUID
		action, slug, target string
		vanity               bool
	}{
		{u("anonymous"), "Create", "", "https://go.dev", false},
		{u("bob"), "Create", "", "https://go.dev", false},
		{u("bob"), "Create", "bobs", "https://go.dev", true},
		{u("alice"), "Create", "alices", "https://go.dev", true},
		{u("alice"), "Create", "", "http://db.internal/admin", false},
		{u("root"), "Create", "", "http://localhost:6379", false},
		{u("bob"), "Delete", "alices-link", "", false},
		{u("alice"), "Delete", "alices-link", "", false},
		{u("root"), "Delete", "alices-link", "", false},
		{u("bob"), "List", "", "", false},
		{u("root"), "List", "", "", false},
		{sv("redirector"), "Resolve", "golang", "", false},
		{sv("redirector"), "Delete", "golang", "", false},
		{sv("operator"), "Create", "docs", "https://go.dev", true},
	}
	var rows [][]any
	for _, c := range cases {
		ok, diag := a.Decide(c.who, c.action, c.slug, c.target, c.vanity)
		decision, why := "allow", ""
		if !ok {
			decision, why = "deny", a.reasons(diag)
		}
		rows = append(rows, []any{c.who.String(), c.action, c.slug, c.target, decision, why})
	}
	s.Table([]string{"principal", "action", "slug", "target", "decision", "why"}, rows)
}

func client(cfg *tls.Config) *http.Client {
	return &http.Client{Transport: &http.Transport{TLSClientConfig: cfg, ForceAttemptHTTP2: true}, Timeout: 5 * time.Second}
}

func mtls(s *Session) {
	// every client pins the api's spiffe id instead of a hostname
	pin := tlsconfig.AuthorizeID(s.svids["links"].ID)
	as := func(w string) *http.Client { return client(tlsconfig.MTLSClientConfig(s.svids[w], s.ca.Bundle(), pin)) }
	user := client(tlsconfig.TLSClientConfig(s.ca.Bundle(), pin))

	rogue, err := NewCA("evil.local")
	if err != nil {
		s.Fail(err)
		return
	}
	fake, err := rogue.Issue("redirector", time.Hour)
	if err != nil {
		s.Fail(err)
		return
	}
	evil := client(tlsconfig.MTLSClientConfig(fake, s.ca.Bundle(), pin))

	calls := []struct {
		who    string
		c      *http.Client
		header string
		method string
		path   string
		body   string
	}{
		{"alice, no cert", user, "alice", "POST", "/links", `{"slug":"golang","target":"https://go.dev"}`},
		{"bob, no cert", user, "bob", "POST", "/links", `{"slug":"bobby","target":"https://go.dev"}`},
		{"bob, no cert", user, "bob", "POST", "/links", `{"target":"https://go.dev/blog"}`},
		{"svid redirector", as("redirector"), "", "GET", "/links/golang", ""},
		{"svid redirector", as("redirector"), "", "DELETE", "/links/golang", ""},
		{"svid redirector, X-User alice", as("redirector"), "alice", "DELETE", "/links/golang", ""},
		{"bob, no cert", user, "bob", "DELETE", "/links/golang", ""},
		{"evil.local redirector", evil, "", "GET", "/links/golang", ""},
		{"svid operator", as("operator"), "", "POST", "/links", `{"slug":"docs","target":"https://go.dev/doc"}`},
		{"alice, no cert", user, "alice", "DELETE", "/links/golang", ""},
		{"root, no cert", user, "root", "GET", "/links", ""},
	}
	var rows [][]any
	for _, c := range calls {
		req, _ := http.NewRequestWithContext(s.ctx, c.method, s.base+c.path, bytes.NewBufferString(c.body))
		if c.header != "" {
			req.Header.Set("X-User", c.header)
		}
		res, err := c.c.Do(req)
		if err != nil {
			msg := err.Error()
			if i := strings.LastIndex(msg, ": "); i >= 0 {
				msg = msg[i+2:]
			}
			rows = append(rows, []any{c.who, c.method + " " + c.path, "tls", msg})
			continue
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := strings.TrimSpace(string(b))
		if c.path == "/links" && c.method == "GET" {
			var m map[string]link
			json.Unmarshal(b, &m)
			out = ""
			for _, k := range slices.Sorted(maps.Keys(m)) {
				out += k + ":" + m[k].Owner + " "
			}
		}
		rows = append(rows, []any{c.who, c.method + " " + c.path, res.StatusCode, out})
	}
	s.Table([]string{"caller", "call", "status", "body"}, rows)
}
