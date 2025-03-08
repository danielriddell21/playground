package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/graphql-go/graphql"
	"github.com/spf13/cobra"

	"playground/internal/play"
)

type Session struct {
	ctx    context.Context
	schema graphql.Schema
	err    error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() {}

func (s *Session) Note(format string, args ...any) {
	if s.err != nil {
		return
	}
	fmt.Printf(format+"\n", args...)
}

func (s *Session) Query(query string) {
	s.QueryWith(query, nil)
}

func (s *Session) QueryWith(query string, vars map[string]any) {
	if s.err != nil {
		return
	}
	result := graphql.Do(graphql.Params{
		Schema:         s.schema,
		RequestString:  query,
		VariableValues: vars,
		Context:        s.ctx,
	})

	out := map[string]any{}
	if result.Data != nil {
		out["data"] = result.Data
	}
	if len(result.Errors) > 0 {
		msgs := make([]string, len(result.Errors))
		for i, e := range result.Errors {
			msgs[i] = e.Message
		}
		out["errors"] = msgs
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		s.err = err
	}
}

var set = &play.Set[*Session]{
	Use:        "graphql",
	Short:      "graphql playgrounds",
	DSNEnv:     "GRAPHQL_URL",
	DefaultDSN: "in-process",
	Connect:    connect,
}

func connect(ctx context.Context, _ string) (*Session, error) {
	schema, err := buildSchema()
	if err != nil {
		return nil, err
	}
	return &Session{ctx: ctx, schema: schema}, nil
}

func Command() *cobra.Command { return set.Command() }
