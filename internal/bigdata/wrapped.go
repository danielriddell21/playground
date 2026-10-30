package bigdata

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"playground/internal/bigdata/pb"
)

func init() {
	set.Add("wrapped", "year end music wrapped served over grpc: parallel hash agg, covering index ranges, materialized view", wrapped)
}

const wrappedSQL = `select distinct on (listener) listener, artist, total
	from (select listener, artist, sum(secs)::int8 as total from bigdata_play %s group by 1, 2) x
	order by listener, total desc, artist`

func (s *server) Wrapped(req *pb.WrappedRequest, stream pb.Bigdata_WrappedServer) error {
	ctx := stream.Context()
	var mu sync.Mutex
	send := func(sql string, args ...any) error {
		rows, err := s.pool.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		b := &pb.WrappedBatch{}
		flush := func() error {
			if len(b.Listener) == 0 {
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			err := stream.Send(b)
			b.Listener, b.Artist, b.Secs = b.Listener[:0], b.Artist[:0], b.Secs[:0]
			return err
		}
		var l, a int32
		var t int64
		for rows.Next() {
			if err := rows.Scan(&l, &a, &t); err != nil {
				return err
			}
			b.Listener, b.Artist, b.Secs = append(b.Listener, l), append(b.Artist, a), append(b.Secs, t)
			if len(b.Listener) == batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return flush()
	}

	switch req.Mode {
	case pb.WrappedRequest_VIEW:
		return send(`select listener, artist, total from bigdata_wrapped`)
	case pb.WrappedRequest_RANGES:
		// each range walks the covering index in order: no sort, no hash
		// table, and its listeners go out the moment its range is done
		var top int
		if err := s.pool.QueryRow(ctx, `select coalesce(max(listener), 0) + 1 from bigdata_play`).Scan(&top); err != nil {
			return err
		}
		g, _ := errgroup.WithContext(ctx)
		for _, r := range split(top, workers) {
			g.Go(func() error {
				return send(fmt.Sprintf(wrappedSQL, `where listener >= $1 and listener < $2`), r[0], r[1])
			})
		}
		return g.Wait()
	default:
		return send(fmt.Sprintf(wrappedSQL, ``))
	}
}

func wrapped(s *Session) {
	s.Note("%d plays this year; every listener wants their top artist and minutes, served by the wrapped endpoint", rows)
	s.Exec(`drop materialized view if exists bigdata_wrapped`)
	s.Exec(`drop table if exists bigdata_play`)
	s.Exec(`create table bigdata_play (listener int4, artist int4, song int4, secs int4)`)
	// somebody played one song four thousand times
	s.Exec(`insert into bigdata_play
		select case when i < least(4000, $1::int8 / 10) then 0 else 1 + abs(hashint8(i)) % $2 end,
			a, case when i < least(4000, $1::int8 / 10) then 70 else a * 10 + abs(hashint8(i + 2)) % 10 end, 30 + abs(hashint8(i + 3)) % 270
		from generate_series(0, $1::int8 - 1) i,
			lateral (select case when i < least(4000, $1::int8 / 10) then 7 else abs(hashint8(i + 1)) % 2000 end::int4 as a) x`,
		rows, max(rows/50, 10))
	s.Exec(`vacuum analyze bigdata_play`)

	fetch := func(mode pb.WrappedRequest_Mode) (string, error) {
		stream, err := s.client.Wrapped(s.ctx, &pb.WrappedRequest{Mode: mode})
		if err != nil {
			return "", err
		}
		var n, sum int64
		err = recvAll(stream, func(b *pb.WrappedBatch) {
			n += int64(len(b.Listener))
			for i := range b.Listener {
				sum += int64(b.Listener[i])*7 + int64(b.Artist[i])*13 + b.Secs[i]
			}
		})
		return fmt.Sprintf("%d listeners, checksum %d", n, sum), err
	}

	s.race(
		func() error {
			return s.execAll(`drop materialized view if exists bigdata_wrapped`, `drop index if exists bigdata_play_cover`)
		},
		approach{
			name: "hash agg",
			grpc: "server stream",
			pg:   "group by, parallel workers",
			pick: "the default; let the planner fan out",
			run:  func() (string, error) { return fetch(pb.WrappedRequest_AGG) },
		},
		approach{
			name: "index ranges",
			grpc: "server stream, N producers",
			pg:   "covering index, ranged index only scans",
			pick: "queried often and the first listeners should arrive early; time includes the index build",
			run: func() (string, error) {
				if err := s.execAll(`create index bigdata_play_cover on bigdata_play (listener, artist) include (secs)`, `vacuum bigdata_play`); err != nil {
					return "", err
				}
				return fetch(pb.WrappedRequest_RANGES)
			},
		},
		approach{
			name: "mat view",
			grpc: "server stream",
			pg:   "materialized view + unique index",
			pick: "read far more than it changes; refresh concurrently on a schedule",
			run: func() (string, error) {
				if err := s.execAll(
					`create materialized view bigdata_wrapped as `+fmt.Sprintf(wrappedSQL, ``),
					`create unique index on bigdata_wrapped (listener)`,
				); err != nil {
					return "", err
				}
				return fetch(pb.WrappedRequest_VIEW)
			},
		},
	)
	start := time.Now()
	_, err := fetch(pb.WrappedRequest_VIEW)
	s.Fail(err)
	reread := time.Since(start)
	s.Note("\nonce built, the view answers again in %s", reread.Round(time.Millisecond))

	s.Note("\nthe one with the song on repeat")
	s.Show(`select listener, song, count(*) as plays, sum(secs) / 3600 as hours from bigdata_play group by 1, 2 order by 3 desc limit 1`)
}
