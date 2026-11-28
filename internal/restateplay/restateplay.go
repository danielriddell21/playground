package restateplay

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/ingress"
	"github.com/restatedev/sdk-go/server"
	"github.com/spf13/cobra"

	"playground/internal/play"
)

// Session serves the restate endpoint from this process, registers it with
// restate-server and runs the ledger the services write to.
type Session struct {
	ctx     context.Context
	cancel  context.CancelFunc
	ingress *ingress.Client
	ledger  *ledger
	ledgerS *http.Server
	run     string // per run suffix, restate state outlives the process
	err     error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() {
	s.cancel()
	s.ledgerS.Close()
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

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var set = &play.Set[*Session]{
	Use:        "restate",
	Short:      "durable execution with restate: journaled steps, idempotency, virtual objects, timers",
	DSNEnv:     "RESTATE_INGRESS",
	DefaultDSN: "http://localhost:8080",
	Connect:    connect,
}

// connect needs restate-server (npx @restatedev/restate-server). The admin
// api and where restate-server reaches this process come from env.
func connect(ctx context.Context, ingressURL string) (*Session, error) {
	admin := env("RESTATE_ADMIN", "http://localhost:9070")
	addr := env("RESTATE_ENDPOINT_ADDR", ":9080")
	self := env("RESTATE_ENDPOINT_URL", "http://localhost:9080")

	l := newLedger()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ls := &http.Server{Handler: l}
	go ls.Serve(ln)

	ctx, cancel := context.WithCancel(ctx)
	s := &Session{ctx: ctx, cancel: cancel, ingress: ingress.NewClient(ingressURL), ledger: l, ledgerS: ls, run: fmt.Sprint(time.Now().Unix())}

	ep := server.NewRestate().
		Bind(restate.Reflect(&Events{ledgerURL: "http://" + ln.Addr().String()})).
		Bind(restate.Reflect(Tally{}))
	go ep.Start(ctx, addr)
	time.Sleep(300 * time.Millisecond)
	if err := register(ctx, admin, self); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// register asks restate-server's admin api to discover our endpoint.
func register(ctx context.Context, admin, self string) error {
	body := fmt.Sprintf(`{"uri":%q,"force":true}`, self)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, admin+"/deployments", strings.NewReader(body))
	req.Header.Set("content-type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("register with %s (is restate-server running?): %w", admin, err)
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("register: %s", res.Status)
	}
	return nil
}

func Command() *cobra.Command { return set.Command() }
