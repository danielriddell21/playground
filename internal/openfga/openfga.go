package openfga

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"time"

	"github.com/spf13/cobra"

	"playground/internal/play"
)

type Session struct {
	ctx    context.Context
	addr   string
	server *exec.Cmd
	store  string
	model  string
	err    error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Ctx() context.Context { return s.ctx }

func (s *Session) Addr() string { return s.addr }

func (s *Session) Close() {
	if s.server != nil && s.server.Process != nil {
		s.server.Process.Kill()
		s.server.Wait()
	}
}

func (s *Session) Fail(err error) {
	if s.err == nil && err != nil {
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
	Use:        "openfga",
	Short:      "zanzibar style relationship authorization, as a service",
	DSNEnv:     "OPENFGA_ADDR",
	DefaultDSN: "127.0.0.1:8080",
	Connect:    connect,
}

func connect(ctx context.Context, addr string) (*Session, error) {
	s := &Session{ctx: ctx, addr: addr}

	if reachable(addr) {
		fmt.Println("using the openfga server already listening on", addr)
		return s, nil
	}

	bin, err := exec.LookPath("openfga")
	if err != nil {
		return nil, fmt.Errorf("no openfga server on %s and none on PATH.\n"+
			"start one with:\n"+
			"  go install github.com/openfga/openfga/cmd/openfga@latest && openfga run", addr)
	}

	cmd := exec.Command(bin, "run", "--datastore-engine", "memory")
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	s.server = cmd
	fmt.Println("started an in-memory openfga server for this run")

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if reachable(addr) {
			return s, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	cmd.Process.Kill()
	return nil, fmt.Errorf("openfga did not come up on %s", addr)
}

func reachable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func Command() *cobra.Command { return set.Command() }
