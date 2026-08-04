package lang

import (
	"context"
	"fmt"
	"runtime"

	"github.com/spf13/cobra"

	"playground/internal/play"
)

type Session struct {
	ctx context.Context
	err error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() {}

func (s *Session) Ctx() context.Context { return s.ctx }

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
	Use:        "lang",
	Short:      "go language and standard library features",
	DSNEnv:     "GOTOOLCHAIN",
	DefaultDSN: "local",
	Connect:    connect,
}

func connect(ctx context.Context, _ string) (*Session, error) {
	fmt.Println("toolchain:", runtime.Version())
	return &Session{ctx: ctx}, nil
}

func Command() *cobra.Command { return set.Command() }
