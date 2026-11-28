package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"playground/internal/bigdata"
	"playground/internal/dapr"
	"playground/internal/graphql"
	grpcplay "playground/internal/grpcplay"
	"playground/internal/kafka"
	"playground/internal/lang"
	"playground/internal/melange"
	"playground/internal/mix"
	fga "playground/internal/openfga"
	"playground/internal/operator"
	otelplay "playground/internal/otel"
	"playground/internal/postgres"
	"playground/internal/resilience"
	"playground/internal/restateplay"
	sqlcplay "playground/internal/sqlcplay"
	"playground/internal/wasmplay"
	"playground/internal/xdsplay"
	"playground/internal/zerotrust"
)

func main() {
	root := &cobra.Command{
		Use:   "playground",
		Short: "playgrounds for tools worth messing around with",
	}
	root.AddCommand(bigdata.Command())
	root.AddCommand(dapr.Command())
	root.AddCommand(graphql.Command())
	root.AddCommand(grpcplay.Command())
	root.AddCommand(kafka.Command())
	root.AddCommand(lang.Command())
	root.AddCommand(mix.Command())
	root.AddCommand(otelplay.Command())
	root.AddCommand(sqlcplay.Command())
	root.AddCommand(wasmplay.Command())
	root.AddCommand(xdsplay.Command())
	root.AddCommand(zerotrust.Command())
	root.AddCommand(fga.Command())
	root.AddCommand(operator.Command())
	root.AddCommand(melange.Command())
	root.AddCommand(postgres.Command())
	root.AddCommand(resilience.Command())
	root.AddCommand(restateplay.Command())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
