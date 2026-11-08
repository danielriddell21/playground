package zerotrust

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"net/url"
	"path"

	"github.com/cedar-policy/cedar-go"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
)

//go:embed policies.cedar
var policyText []byte

type principalKey struct{}

// Principal is who is calling, as a cedar entity uid.
func Principal(ctx context.Context) cedar.EntityUID {
	if p, ok := ctx.Value(principalKey{}).(cedar.EntityUID); ok {
		return p
	}
	return cedar.NewEntityUID("User", "anonymous")
}

// Identify is http middleware. A verified client cert wins: its spiffe id
// becomes Service::"<last path segment>". Otherwise X-User names a User.
func Identify(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := cedar.NewEntityUID("User", "anonymous")
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			id, err := x509svid.IDFromCert(r.TLS.PeerCertificates[0])
			if err != nil {
				http.Error(w, "bad svid", http.StatusUnauthorized)
				return
			}
			p = cedar.NewEntityUID("Service", cedar.String(path.Base(id.Path())))
			r.Header.Del("X-User") // a service can't claim to be a user
		} else if u := r.Header.Get("X-User"); u != "" {
			p = cedar.NewEntityUID("User", cedar.String(u))
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	})
}

// Authorizer checks every shortener rpc against the cedar policies.
type Authorizer struct {
	policies *cedar.PolicySet
	// Roles maps user names to the roles they hold.
	Roles map[string][]string
	// Owner looks up a link's owner for delete checks.
	Owner func(slug string) (string, bool)
}

func NewAuthorizer(roles map[string][]string, owner func(string) (string, bool)) (*Authorizer, error) {
	ps, err := cedar.NewPolicySetFromBytes("policies.cedar", policyText)
	if err != nil {
		return nil, err
	}
	return &Authorizer{policies: ps, Roles: roles, Owner: owner}, nil
}

func (a *Authorizer) entities(principal cedar.EntityUID, link cedar.EntityUID, owner string) cedar.EntityMap {
	em := cedar.EntityMap{}
	if principal.Type == "User" {
		var parents []cedar.EntityUID
		for _, r := range a.Roles[string(principal.ID)] {
			parents = append(parents, cedar.NewEntityUID("Role", cedar.String(r)))
		}
		em[principal] = cedar.Entity{UID: principal, Parents: cedar.NewEntityUIDSet(parents...)}
	}
	if owner != "" {
		em[link] = cedar.Entity{UID: link, Attributes: cedar.NewRecord(cedar.RecordMap{
			"owner": cedar.NewEntityUID("User", cedar.String(owner)),
		})}
	}
	return em
}

// Decide is the check on its own, for tables and tests.
func (a *Authorizer) Decide(principal cedar.EntityUID, action, slug, target string, vanity bool) (bool, cedar.Diagnostic) {
	link := cedar.NewEntityUID("Link", cedar.String(slug))
	owner := ""
	if action == "Delete" && a.Owner != nil {
		owner, _ = a.Owner(slug)
	}
	host := ""
	if u, err := url.Parse(target); err == nil {
		host = u.Hostname()
	}
	req := cedar.Request{
		Principal: principal,
		Action:    cedar.NewEntityUID("Action", cedar.String(action)),
		Resource:  link,
		Context: cedar.NewRecord(cedar.RecordMap{
			"vanity":     cedar.Boolean(vanity),
			"targetHost": cedar.String(host),
		}),
	}
	ok, diag := a.policies.IsAuthorized(a.entities(principal, link, owner), req)
	return bool(ok), diag
}

// Check answers 403 with the reason and returns false when the caller in
// ctx may not do action to slug.
func (a *Authorizer) Check(w http.ResponseWriter, r *http.Request, action, slug, target string, vanity bool) bool {
	p := Principal(r.Context())
	if ok, diag := a.Decide(p, action, slug, target, vanity); !ok {
		http.Error(w, fmt.Sprintf("%s may not %s %q%s", p, action, slug, a.reasons(diag)), http.StatusForbidden)
		return false
	}
	return true
}

// reasons names the forbid that fired, by its @id if it has one.
func (a *Authorizer) reasons(d cedar.Diagnostic) string {
	if len(d.Reasons) == 0 {
		return " (no permit matched)"
	}
	s := " (forbidden by"
	for _, r := range d.Reasons {
		name := string(r.PolicyID)
		if p := a.policies.Get(r.PolicyID); p != nil {
			if id, ok := p.Annotations()["id"]; ok {
				name = string(id)
			}
		}
		s += " " + name
	}
	return s + ")"
}
