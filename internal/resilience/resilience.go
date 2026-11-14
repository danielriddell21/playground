package resilience

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/spf13/cobra"

	"playground/internal/play"
)

// Session runs a flaky upstream; demos put different edges in front of it.
type Session struct {
	ctx      context.Context
	faults   *Faults
	upstream string
	servers  []*http.Server
	err      error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() {
	for _, srv := range s.servers {
		srv.Close()
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

// serve starts h on a random port and returns its base url.
func (s *Session) serve(h http.Handler) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		s.Fail(err)
		return ""
	}
	srv := &http.Server{Handler: h}
	s.servers = append(s.servers, srv)
	go srv.Serve(ln)
	return "http://" + ln.Addr().String()
}

var slugs = map[string]string{"golang": "https://go.dev", "vegeta": "https://github.com/tsenart/vegeta", "breaker": "https://github.com/sony/gobreaker"}

var set = &play.Set[*Session]{
	Use:        "resilience",
	Short:      "rate limits, circuit breaker with stale cache and fault injection under vegeta load",
	DSNEnv:     "RESILIENCE_UPSTREAM",
	DefaultDSN: "in-process",
	Connect:    connect,
}

func connect(ctx context.Context, _ string) (*Session, error) {
	s := &Session{ctx: ctx, faults: &Faults{}}
	s.upstream = s.serve(s.faults.Wrap(upstream(slugs)))
	return s, s.err
}

func Command() *cobra.Command { return set.Command() }
