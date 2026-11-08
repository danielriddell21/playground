package zerotrust

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"

	"playground/internal/play"
)

var roles = map[string][]string{
	"alice": {"pro"},
	"bob":   {},
	"root":  {"admin"},
}

// Session runs the links api on a tls listener. Workloads get svids from a
// throwaway ca; users come in without a client cert.
type Session struct {
	ctx   context.Context
	ca    *CA
	svids map[string]*x509svid.SVID
	az    *Authorizer
	srv   *http.Server
	base  string
	err   error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() {
	if s.srv != nil {
		s.srv.Close()
	}
}

func (s *Session) Ctx() context.Context { return s.ctx }

func (s *Session) Fail(err error) {
	if s.err == nil {
		s.err = err
	}
}

func (s *Session) Note(format string, args ...any) {
	if s.err != nil {
		return
	}
	fmt.Printf(format+"\n", args...)
}

func (s *Session) Table(headers []string, rows [][]any) {
	if s.err != nil {
		return
	}
	play.Table(headers, rows)
}

var set = &play.Set[*Session]{
	Use:        "zerotrust",
	Short:      "spiffe mtls identities and cedar policies in front of a small api",
	DSNEnv:     "SPIFFE_TRUST_DOMAIN",
	DefaultDSN: "playground.local",
	Connect:    connect,
}

func connect(ctx context.Context, td string) (*Session, error) {
	ca, err := NewCA(td)
	if err != nil {
		return nil, err
	}
	s := &Session{ctx: ctx, ca: ca, svids: map[string]*x509svid.SVID{}}
	for _, w := range []string{"links", "redirector", "operator"} {
		if s.svids[w], err = ca.Issue(w, time.Hour); err != nil {
			return nil, err
		}
	}
	api := newLinks()
	if api.az, err = NewAuthorizer(roles, api.owner); err != nil {
		return nil, err
	}
	s.az = api.az
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s.srv = &http.Server{Handler: api.mux(), TLSConfig: serverTLS(s.svids["links"], ca.Bundle(), ca.TD), ErrorLog: quiet}
	go s.srv.ServeTLS(ln, "", "")
	s.base = "https://" + ln.Addr().String()
	return s, nil
}

// serverTLS asks for a client cert but doesn't require one: with one it
// must be a valid svid from our trust domain, without one you are a user.
func serverTLS(svid x509svid.Source, bundle x509bundle.Source, td spiffeid.TrustDomain) *tls.Config {
	cfg := tlsconfig.TLSServerConfig(svid)
	verify := tlsconfig.VerifyPeerCertificate(bundle, tlsconfig.AuthorizeMemberOf(td))
	cfg.ClientAuth = tls.RequestClientCert
	cfg.VerifyPeerCertificate = func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) == 0 {
			return nil
		}
		return verify(raw, nil)
	}
	return cfg
}

func Command() *cobra.Command { return set.Command() }
