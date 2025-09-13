package postgres

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

func init() {
	set.Add("copy", "bulk loading with the copy protocol", copyFrom)
}

func copyFrom(s *Session) {
	s.Exec(`drop table if exists readings`)
	s.Exec(`create table readings (sensor text, taken_at timestamptz, value numeric)`)

	rows := make([][]any, 0, 5000)
	for i := 0; i < 5000; i++ {
		rows = append(rows, []any{
			fmt.Sprintf("sensor-%d", i%10),
			fmt.Sprintf("2026-01-01 00:00:00+00"),
			float64(i%100) / 2,
		})
	}

	s.Note("copy 5000 rows in one round trip")
	n, err := s.conn.CopyFrom(s.Ctx(), pgx.Identifier{"readings"}, []string{"sensor", "taken_at", "value"}, pgx.CopyFromRows(rows))
	if err != nil {
		s.Fail(err)
		return
	}
	s.Note("  inserted %d rows", n)

	s.Show(`select sensor, count(*) as n, round(avg(value), 2) as avg_value from readings group by sensor order by sensor limit 5`)

	s.Note("copy out to text")
	s.Show(`select sensor, value from readings limit 3`)

	s.Note("unnest is the other bulk insert trick")
	s.Exec(`insert into readings (sensor, taken_at, value)
		select * from unnest(
			array['bulk-a','bulk-b'],
			array[now(), now()],
			array[1.5, 2.5]
		)`)
	s.Show(`select sensor, value from readings where sensor like 'bulk%' order by sensor`)

	s.Show(`select count(*) as total from readings`)
}
