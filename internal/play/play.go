package play

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

type Session interface {
	Err() error
	Close()
}

type Connect[T Session] func(ctx context.Context, dsn string) (T, error)

type Set[T Session] struct {
	Use        string
	Short      string
	DSNEnv     string
	DefaultDSN string
	Connect    Connect[T]

	demos []demo[T]
}

type demo[T Session] struct {
	name string
	desc string
	fn   func(T)
}

func (s *Set[T]) Add(name, desc string, fn func(T)) {
	s.demos = append(s.demos, demo[T]{name: name, desc: desc, fn: fn})
}

func (s *Set[T]) Command() *cobra.Command {
	var dsn string

	root := &cobra.Command{
		Use:   s.Use,
		Short: s.Short,
	}
	root.PersistentFlags().StringVar(&dsn, "dsn", "", "connection string (default $"+s.DSNEnv+")")

	run := func(picked []demo[T]) error {
		sess, err := s.Connect(context.Background(), resolve(dsn, s.DSNEnv, s.DefaultDSN))
		if err != nil {
			return err
		}
		defer sess.Close()

		for _, d := range picked {
			fmt.Printf("\n### %s\n\n", d.name)
			d.fn(sess)
			if err := sess.Err(); err != nil {
				return fmt.Errorf("%s: %w", d.name, err)
			}
		}
		return nil
	}

	for _, d := range s.demos {
		root.AddCommand(&cobra.Command{
			Use:   d.name,
			Short: d.desc,
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				return run([]demo[T]{d})
			},
		})
	}

	root.AddCommand(&cobra.Command{
		Use:   "all",
		Short: "run every " + s.Use + " playground",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(s.demos)
		},
	})

	return root
}

func resolve(flag, env, fallback string) string {
	if flag != "" {
		return flag
	}
	if v := os.Getenv(env); v != "" {
		return v
	}
	return fallback
}
