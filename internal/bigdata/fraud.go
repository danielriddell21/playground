package bigdata

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
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

func swipeLen(b *pb.SwipeBatch) int { return len(b.Id) }

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
		pgx.CopyFromFunc(fromStream(len(swipeCols), stream.Recv, swipeLen, swipeRow)))
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
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	lanes := make([]*lane[*pb.SwipeBatch], fraudParts)
	for p := range lanes {
		lanes[p] = copyLane(ctx, s.pool, fmt.Sprintf("bigdata_swipe_part_%d", p), swipeCols, swipeLen, swipeRow)
	}
	recvErr := func() error {
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
				if err := lanes[p].send(ctx, part); err != nil {
					return err
				}
			}
		}
	}()
	if recvErr != nil {
		cancel()
	}
	for _, l := range lanes {
		if err := l.close(); err != nil && recvErr == nil {
			recvErr = err
		}
	}
	if recvErr != nil {
		return recvErr
	}

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(stream.Context())
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

// ScreenCached keeps each card's last swipe in one redis hash, 24 bytes a
// card. a batch is one HMGET for its cards, the same haversine in go, and
// one HSET of where every card ended up; the swipes go to postgres down a
// COPY lane and postgres is never asked about history
func (s *server) ScreenCached(stream pb.Bigdata_ScreenCachedServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	l := copyLane(ctx, s.pool, "bigdata_swipe", swipeCols, swipeLen, swipeRow)
	err := func() error {
		type spot struct {
			at       int64
			lat, lon float64
		}
		last := map[int64]spot{}
		for {
			b, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			clear(last)
			var fields []string
			for _, card := range b.Card {
				if _, ok := last[card]; !ok {
					last[card] = spot{at: -1}
					fields = append(fields, strconv.FormatInt(card, 10))
				}
			}
			known, err := s.rdb.HMGet(ctx, keyPrefix+"card", fields...).Result()
			if err != nil {
				return err
			}
			for i, v := range known {
				if v, ok := v.(string); ok && len(v) == 24 {
					card, _ := strconv.ParseInt(fields[i], 10, 64)
					last[card] = spot{
						at:  int64(binary.LittleEndian.Uint64([]byte(v[0:]))),
						lat: math.Float64frombits(binary.LittleEndian.Uint64([]byte(v[8:]))),
						lon: math.Float64frombits(binary.LittleEndian.Uint64([]byte(v[16:]))),
					}
				}
			}
			out := &pb.Flags{}
			for i, card := range b.Card {
				cur := spot{b.At[i], b.Lat[i], b.Lon[i]}
				if prev := last[card]; prev.at >= 0 && impossible(prev.lat, prev.lon, prev.at, cur.lat, cur.lon, cur.at) {
					out.Id = append(out.Id, b.Id[i])
				}
				last[card] = cur
			}
			pairs := make([]any, 0, len(last)*2)
			for card, sp := range last {
				var v [24]byte
				binary.LittleEndian.PutUint64(v[0:], uint64(sp.at))
				binary.LittleEndian.PutUint64(v[8:], math.Float64bits(sp.lat))
				binary.LittleEndian.PutUint64(v[16:], math.Float64bits(sp.lon))
				pairs = append(pairs, strconv.FormatInt(card, 10), v[:])
			}
			if err := s.rdb.HSet(ctx, keyPrefix+"card", pairs...).Err(); err != nil {
				return err
			}
			if err := l.send(ctx, b); err != nil {
				return err
			}
			if len(out.Id) > 0 {
				if err := stream.Send(out); err != nil {
					return err
				}
			}
		}
	}()
	if err != nil {
		cancel()
	}
	if cerr := l.close(); err == nil {
		err = cerr
	}
	return err
}

// impossible is tooFast in go, operation for operation
func impossible(plat, plon float64, pat int64, lat, lon float64, at int64) bool {
	rad := math.Pi / 180
	a := math.Pow(math.Sin(rad*(lat-plat)/2), 2) + math.Cos(rad*plat)*math.Cos(rad*lat)*math.Pow(math.Sin(rad*(lon-plon)/2), 2)
	km := 2 * 6371 * math.Asin(min(1, math.Sqrt(a)))
	return km/max(float64(at-pat)/1e6, 1)*3600 > 900
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

	approaches := []approach{
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
	}
	if s.withRedis() {
		approaches = append(approaches, approach{
			name: "redis state",
			grpc: "bidi, flags back per batch",
			pg:   "COPY lane; card state in a redis hash",
			pick: "real time at bulk speed, and you can run redis with persistence",
			run: func() (string, error) {
				return bidi(func() (pb.Bigdata_ScreenClient, error) { return s.client.ScreenCached(s.ctx) })
			},
		})
	}
	s.race(func() error {
		if err := s.resetRedis(); err != nil {
			return err
		}
		return s.execAll(`truncate bigdata_swipe, bigdata_card_last, bigdata_swipe_part`)
	}, approaches...)

	s.Note("\nthe most well travelled card")
	s.Show(`with swipes as (select * from bigdata_swipe union all select * from bigdata_swipe_part),
		x as (` + sweepSQL("swipes") + `)
		select card, count(*) as impossible_trips, (sum(cents) / 100.0)::numeric(10, 2) as spent
		from swipes where id in (select id from x) group by 1 order by 2 desc, 1 limit 3`)
}
