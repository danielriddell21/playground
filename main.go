package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"playground/internal/dapr"
	"playground/internal/graphql"
	grpcplay "playground/internal/grpcplay"
	"playground/internal/kafka"
	"playground/internal/melange"
	fga "playground/internal/openfga"
	otelplay "playground/internal/otel"
	"playground/internal/postgres"
	sqlcplay "playground/internal/sqlcplay"
)

func main() {
	root := &cobra.Command{
		Use:   "playground",
		Short: "playgrounds for tools worth messing around with",
	}
	root.AddCommand(dapr.Command())
	root.AddCommand(graphql.Command())
	root.AddCommand(grpcplay.Command())
	root.AddCommand(kafka.Command())
	root.AddCommand(otelplay.Command())
	root.AddCommand(sqlcplay.Command())
	root.AddCommand(fga.Command())
	root.AddCommand(melange.Command())
	root.AddCommand(postgres.Command())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
