package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"playground/internal/graphql"
	grpcplay "playground/internal/grpcplay"
	otelplay "playground/internal/otel"
	"playground/internal/postgres"
)

func main() {
	root := &cobra.Command{
		Use:   "playground",
		Short: "playgrounds for tools worth messing around with",
	}
	root.AddCommand(graphql.Command())
	root.AddCommand(grpcplay.Command())
	root.AddCommand(otelplay.Command())
	root.AddCommand(postgres.Command())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
