package bigdata

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"

	"playground/internal/bigdata/pb"
)

func init() {
	set.Add("fraud", "card swipes in, impossible travel flagged out: per batch with card state, copy then one window, staged server pipeline", fraud)
}

const fraudParts = 4

// swipes arrive in time order across three days. each card stays near home
// except one swipe in two hundred, which is somewhere else entirely
func fillSwipes(b *pb.SwipeBatch, lo, hi int) {
	b.Id, b.Card, b.At, b.Lat, b.Lon, b.Cents = b.Id[:0], b.Card[:0], b.At[:0], b.Lat[:0], b.Lon[:0], b.Cents[:0]
	step := 3 * 24 * time.Hour / time.Duration(rows)
	cards := max(rows/50, 10)
	for i := lo; i < hi; i++ {
		card := 1 + pick(i, 1, cards)
		lat, lon := -60+unit(card, 5)*120+unit(i, 2)*0.1-0.05, -180+unit(card, 6)*360+unit(i, 3)*0.1-0.05
		if pick(i, 4, 200) == 0 {
			lat, lon = -60+unit(i, 7)*120, -180+unit(i, 8)*360
		}
		b.Id = append(b.Id, int64(i))
		b.Card = append(b.Card, int64(card))
		b.At = append(b.At, micros(epoch.Add(time.Duration(i)*step)))
		b.Lat, b.Lon = append(b.Lat, lat), append(b.Lon, lon)
		b.Cents = append(b.Cents, int64(pick(i, 9, 20000)))
	}
}

func swipeRow(b *pb.SwipeBatch, i int, v []any) {
	v[0], v[1], v[2], v[3], v[4], v[5] = b.Id[i], b.Card[i], fromMicros(b.At[i]), b.Lat[i], b.Lon[i], b.Cents[i]
}

var swipeCols = []string{"id", "card", "at", "lat", "lon", "cents"}

// tooFast is haversine km over hours since the card's previous swipe
const tooFast = `2 * 6371 * asin(least(1, sqrt(power(sin(radians(lat - plat) / 2), 2)
		+ cos(radians(plat)) * cos(radians(lat)) * power(sin(radians(lon - plon) / 2), 2))))
	/ greatest(extract(epoch from at - pat), 1) * 3600 > 900`

func sweepSQL(table string) string {
	return `select id from (
			select id, at, lat, lon, lag(at) over w as pat, lag(lat) over w as plat, lag(lon) over w as plon
			from ` + table + `
			window w as (partition by card order by at)
		) x where pat is not null and ` + tooFast
}

// screenSQL does a whole batch in one statement: store the swipes, compare
// each with the one before it (earlier in the batch, or the card's last
// known swipe), and move every card's last known swipe forward. all the
// ctes read the same snapshot, so last is the state before this batch
const screenSQL = `with b as (
		select * from unnest($1::int8[], $2::int8[], $3::timestamptz[], $4::float8[], $5::float8[], $6::int8[])
			as b(id, card, at, lat, lon, cents)
	),
	stored as (
		insert into bigdata_swipe select * from b
	),
	moved as (
		insert into bigdata_card_last
		select distinct on (card) card, at, lat, lon from b order by card, at desc
		on conflict (card) do update set at = excluded.at, lat = excluded.lat, lon = excluded.lon
	),
	paired as (
		select b.id, b.at, b.lat, b.lon,
			coalesce(lag(b.at) over w, l.at) as pat,
			coalesce(lag(b.lat) over w, l.lat) as plat,
			coalesce(lag(b.lon) over w, l.lon) as plon
		from b left join bigdata_card_last l using (card)
		window w as (partition by b.card order by b.at)
	)
	select id from paired where pat is not null and ` + tooFast

func (s *server) sendFlags(stream interface{ Send(*pb.Flags) error }, mu *sync.Mutex, rows pgx.Rows) error {
	defer rows.Close()
	out := &pb.Flags{}
	var id int64
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return err
		}
		out.Id = append(out.Id, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(out.Id) == 0 {
		return nil
	}
	if mu != nil {
		mu.Lock()
		defer mu.Unlock()
	}
	return stream.Send(out)
}

func (s *server) Screen(stream pb.Bigdata_ScreenServer) error {
	ctx := stream.Context()
	for {
		b, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		rows, err := s.pool.Query(ctx, screenSQL, b.Id, b.Card, times(b.At), b.Lat, b.Lon, b.Cents)
		if err != nil {
			return err
		}
		if err := s.sendFlags(stream, nil, rows); err != nil {
			return err
		}
	}
}

func (s *server) LoadSwipes(stream pb.Bigdata_LoadSwipesServer) error {
	n, err := s.pool.CopyFrom(stream.Context(), pgx.Identifier{"bigdata_swipe"}, swipeCols,
		pgx.CopyFromFunc(fromStream(len(swipeCols), stream.Recv, func(b *pb.SwipeBatch) int { return len(b.Id) }, swipeRow)))
	if err != nil {
		return err
	}
	return stream.SendAndClose(&pb.Summary{Rows: n, Inserted: n})
}

func (s *server) SweepFlags(_ *pb.Empty, stream pb.Bigdata_SweepFlagsServer) error {
	rows, err := s.pool.Query(stream.Context(), sweepSQL("bigdata_swipe"))
	if err != nil {
		return err
	}
	return s.sendFlags(stream, nil, rows)
}

// ScreenStaged is a pipeline inside the handler: receive, deal each swipe
// to the copy worker that owns its card's partition, and once the stream
// ends sweep every partition at once. the lanes are bounded, so a slow
// copy backs up into grpc flow control instead of into memory
func (s *server) ScreenStaged(stream pb.Bigdata_ScreenStagedServer) error {
	g, ctx := errgroup.WithContext(stream.Context())
	lanes := make([]chan *pb.SwipeBatch, fraudParts)
	for p := range fraudParts {
		lanes[p] = make(chan *pb.SwipeBatch, 16)
		g.Go(func() error {
			_, err := s.pool.CopyFrom(ctx, pgx.Identifier{fmt.Sprintf("bigdata_swipe_part_%d", p)}, swipeCols,
				pgx.CopyFromFunc(fromStream(len(swipeCols), func() (*pb.SwipeBatch, error) {
					select {
					case b, ok := <-lanes[p]:
						if !ok {
							return nil, io.EOF
						}
						return b, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}, func(b *pb.SwipeBatch) int { return len(b.Id) }, swipeRow)))
			return err
		})
	}
	g.Go(func() error {
		defer func() {
			for _, l := range lanes {
				close(l)
			}
		}()
		for {
			b, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			parts := make([]*pb.SwipeBatch, fraudParts)
			for i, card := range b.Card {
				p := parts[card%fraudParts]
				if p == nil {
					p = &pb.SwipeBatch{}
					parts[card%fraudParts] = p
				}
				p.Id, p.Card, p.At = append(p.Id, b.Id[i]), append(p.Card, card), append(p.At, b.At[i])
				p.Lat, p.Lon, p.Cents = append(p.Lat, b.Lat[i]), append(p.Lon, b.Lon[i]), append(p.Cents, b.Cents[i])
			}
			for p, part := range parts {
				if part == nil {
					continue
				}
				select {
				case lanes[p] <- part:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	})
	if err := g.Wait(); err != nil {
		return err
	}

	var mu sync.Mutex
	g, ctx = errgroup.WithContext(stream.Context())
	for p := range fraudParts {
		g.Go(func() error {
			rows, err := s.pool.Query(ctx, sweepSQL(fmt.Sprintf("bigdata_swipe_part_%d", p)))
			if err != nil {
				return err
			}
			return s.sendFlags(stream, &mu, rows)
		})
	}
	return g.Wait()
}

type flagged struct {
	n, idSum int64
}

func (f *flagged) take(b *pb.Flags) {
	f.n += int64(len(b.Id))
	for _, id := range b.Id {
		f.idSum += id
	}
}

func (f *flagged) answer() string {
	return fmt.Sprintf("%d swipes flagged, id sum %d", f.n, f.idSum)
}

func fraud(s *Session) {
	s.Note("%d card swipes over three days; flag every one that needed faster than 900 km/h travel since the card's last swipe", rows)
	s.Exec(`drop table if exists bigdata_swipe, bigdata_card_last, bigdata_swipe_part`)
	s.Exec(`create table bigdata_swipe (id int8 primary key, card int8, at timestamptz, lat float8, lon float8, cents int8)`)
	s.Exec(`create table bigdata_card_last (card int8 primary key, at timestamptz, lat float8, lon float8)`)
	s.Exec(`create table bigdata_swipe_part (id int8, card int8, at timestamptz, lat float8, lon float8, cents int8) partition by list ((card % 4))`)
	for p := range fraudParts {
		s.Exec(fmt.Sprintf(`create table bigdata_swipe_part_%d partition of bigdata_swipe_part for values in (%d)`, p, p))
	}

	bidi := func(open func() (pb.Bigdata_ScreenClient, error)) (string, error) {
		stream, err := open()
		if err != nil {
			return "", err
		}
		f := &flagged{}
		done := make(chan error, 1)
		go func() {
			for {
				b, err := stream.Recv()
				if err != nil {
					if err == io.EOF {
						err = nil
					}
					done <- err
					return
				}
				f.take(b)
			}
		}()
		for b := range batches(0, rows, fillSwipes) {
			if err := stream.Send(b); err != nil {
				return "", err
			}
		}
		stream.CloseSend()
		if err := <-done; err != nil {
			return "", err
		}
		return f.answer(), nil
	}

	s.race(
		func() error { return s.execAll(`truncate bigdata_swipe, bigdata_card_last, bigdata_swipe_part`) },
		approach{
			name: "card state",
			grpc: "bidi, flags back per batch",
			pg:   "one statement: insert, lag vs last swipe, upsert state",
			pick: "real time: a swipe is judged seconds after it happens, history is never rescanned",
			run: func() (string, error) {
				return bidi(func() (pb.Bigdata_ScreenClient, error) { return s.client.Screen(s.ctx) })
			},
		},
		approach{
			name: "one window",
			grpc: "client stream, then server stream",
			pg:   "COPY, one lag() window over everything",
			pick: "a nightly sweep, and the simplest sql there is",
			run: func() (string, error) {
				stream, err := s.client.LoadSwipes(s.ctx)
				if err != nil {
					return "", err
				}
				if _, err := sendAll(stream, batches(0, rows, fillSwipes)); err != nil {
					return "", err
				}
				out, err := s.client.SweepFlags(s.ctx, &pb.Empty{})
				if err != nil {
					return "", err
				}
				f := &flagged{}
				err = recvAll(out, f.take)
				return f.answer(), err
			},
		},
		approach{
			name: "staged",
			grpc: "bidi, flags at the end",
			pg:   "4 COPY lanes into list partitions, 4 parallel windows",
			pick: "heavy ingest with bounded memory and the sweep can wait for the end",
			run: func() (string, error) {
				return bidi(func() (pb.Bigdata_ScreenClient, error) { return s.client.ScreenStaged(s.ctx) })
			},
		},
	)

	s.Note("\nthe most well travelled card")
	s.Show(`with x as (` + sweepSQL("bigdata_swipe_part") + `)
		select card, count(*) as impossible_trips, (sum(cents) / 100.0)::numeric(10, 2) as spent
		from bigdata_swipe_part where id in (select id from x) group by 1 order by 2 desc, 1 limit 3`)
}
