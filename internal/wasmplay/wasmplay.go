package wasmplay

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"playground/internal/play"
)

type Session struct {
	ctx   context.Context
	extra []string
	err   error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() {}

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

var env = map[string]map[string]string{
	"blocklist": {"BLOCK": "evil.test,malware.test", "BLOCK_UA": "badbot"},
}

// load builds a fresh engine with the named bundled plugins plus any extra
// .wasm files from the dsn.
func (s *Session) load(names ...string) *Engine {
	if s.err != nil {
		return nil
	}
	e := NewEngine(s.ctx)
	for _, n := range names {
		wasm, err := BuildPlugin(n)
		if err != nil {
			s.Fail(err)
			return e
		}
		if err := e.Load(s.ctx, n, wasm, env[n]); err != nil {
			s.Fail(err)
			return e
		}
		s.Note("loaded %s (%d KiB)", n, len(wasm)/1024)
	}
	for _, path := range s.extra {
		wasm, err := os.ReadFile(path)
		if err != nil {
			s.Fail(err)
			return e
		}
		if err := e.Load(s.ctx, path, wasm, nil); err != nil {
			s.Fail(err)
			return e
		}
		s.Note("loaded %s (%d KiB)", path, len(wasm)/1024)
	}
	return e
}

var set = &play.Set[*Session]{
	Use:        "wasm",
	Short:      "wazero plugins that rewrite or block requests, sandboxed and time boxed",
	DSNEnv:     "WASM_PLUGINS",
	DefaultDSN: "bundled",
	Connect:    connect,
}

// connect takes a comma separated list of extra wasip1 command modules to
// chain after the bundled ones, in any language.
func connect(ctx context.Context, dsn string) (*Session, error) {
	s := &Session{ctx: ctx}
	if dsn != "bundled" {
		s.extra = strings.Split(dsn, ",")
	}
	return s, nil
}

func Command() *cobra.Command { return set.Command() }
