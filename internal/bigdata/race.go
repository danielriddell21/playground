package bigdata

import (
	"fmt"
	"runtime"
	"time"

	"playground/internal/play"
)

type approach struct {
	name string
	grpc string
	pg   string
	pick string
	// run does the work and returns the scenario's answer, which must be
	// identical across approaches or the race is void
	run func() (string, error)
}

// race runs every approach against the same rows, resetting tables between
// them, and prints how each did. alloc is client and server together, both
// live in this process
func (s *Session) race(reset func() error, as ...approach) {
	if s.err != nil {
		return
	}
	var out [][]any
	var first string
	for i, a := range as {
		var best time.Duration
		var alloc uint64
		var answer string
		for range runs {
			if err := reset(); err != nil {
				s.Fail(fmt.Errorf("%s reset: %w", a.name, err))
				return
			}
			s.Exec(`checkpoint`)
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			start := time.Now()
			got, err := a.run()
			took := time.Since(start)
			runtime.ReadMemStats(&after)
			if err != nil {
				s.Fail(fmt.Errorf("%s: %w", a.name, err))
				return
			}
			if best == 0 || took < best {
				best, alloc = took, after.TotalAlloc-before.TotalAlloc
			}
			answer = got
		}
		if i == 0 {
			first = answer
		} else if answer != first {
			s.Fail(fmt.Errorf("%s answered %q, %s answered %q", a.name, answer, as[0].name, first))
			return
		}
		out = append(out, []any{
			a.name, a.grpc, a.pg,
			best.Round(time.Millisecond).String(),
			fmt.Sprintf("%.0fk", float64(rows)/best.Seconds()/1000),
			fmt.Sprintf("%d", alloc>>20),
		})
	}
	play.Table([]string{"approach", "grpc", "postgres", "elapsed", "rows/s", "alloc MB"}, out)
	fmt.Println()
	for _, a := range as {
		fmt.Printf("  %-12s pick when %s\n", a.name, a.pick)
	}
	fmt.Printf("\nall %d agree: %s\n", len(as), first)
}

// checksum fingerprints every row of a table, so two loads that agree here
// stored the same thing
func (s *Session) checksum(table string) (string, error) {
	var count int64
	var sum string
	err := s.pool.QueryRow(s.ctx, `select count(*), coalesce(sum(hashtextextended(t::text, 0)), 0)::text from `+table+` t`).Scan(&count, &sum)
	return fmt.Sprintf("%d rows, hash %s", count, sum), err
}

func (s *Session) execAll(sqls ...string) error {
	for _, q := range sqls {
		if _, err := s.pool.Exec(s.ctx, q); err != nil {
			return fmt.Errorf("%w\n%s", err, q)
		}
	}
	return nil
}
