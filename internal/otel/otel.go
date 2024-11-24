package otel

import (
	"context"
	"fmt"

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
	Use:        "otel",
	Short:      "opentelemetry traces, metrics and propagation",
	DSNEnv:     "OTEL_EXPORTER_OTLP_ENDPOINT",
	DefaultDSN: "in-memory",
	Connect:    connect,
}

func connect(ctx context.Context, _ string) (*Session, error) {
	return &Session{ctx: ctx}, nil
}

func Command() *cobra.Command { return set.Command() }
