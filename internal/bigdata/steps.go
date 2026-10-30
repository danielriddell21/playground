package bigdata

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"playground/internal/bigdata/pb"
)

func init() {
	set.Add("steps", "fitness pings in, leaderboard out: copy then group by, live running totals, list partitions in parallel", steps)
}

const stepParts = 8

func stepUsers() int { return max(rows/100, 10) }

func stepRow(i int) (user, steps int32, at int64) {
	return int32(1 + pick(i, 1, stepUsers())), int32(pick(i, 2, 3000)), micros(epoch.Add(time.Duration(i) * time.Second))
}

var stepCols = []string{"user_id", "at", "steps"}

func stepLen(b *pb.StepBatch) int { return len(b.UserId) }

func stepCopyRow(b *pb.StepBatch, i int, v []any) {
	v[0], v[1], v[2] = b.UserId[i], fromMicros(b.At[i]), b.Steps[i]
}

func fillSteps(b *pb.StepBatch, lo, hi int) {
	b.Part, b.UserId, b.At, b.Steps = -1, b.UserId[:0], b.At[:0], b.Steps[:0]
	for i := lo; i < hi; i++ {
		u, st, at := stepRow(i)
		b.UserId, b.Steps, b.At = append(b.UserId, u), append(b.Steps, st), append(b.At, at)
	}
}

// boardSQL ranks totals once and reads both ends of them
func boardSQL(totals string) string {
	return `with t as materialized (` + totals + `)
		(select * from t order by s desc, user_id limit 10)
		union all
		(select * from t order by s, user_id limit 1)`
}

func (s *server) board(ctx context.Context, totals string) (*pb.Board, error) {
	rows, err := s.pool.Query(ctx, boardSQL(totals))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []int32
	var sums []int64
	for rows.Next() {
		var u int32
		var v int64
		if err := rows.Scan(&u, &v); err != nil {
			return nil, err
		}
		users, sums = append(users, u), append(sums, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(users) == 0 {
		return &pb.Board{}, nil
	}
	last := len(users) - 1
	return &pb.Board{UserId: users[:last], Steps: sums[:last], CouchUser: users[last], CouchSteps: sums[last]}, nil
}

func (s *server) IngestSteps(stream pb.Bigdata_IngestStepsServer) error {
	first, err := stream.Recv()
	if err == io.EOF {
		return stream.SendAndClose(&pb.Summary{})
	}
	if err != nil {
		return err
	}
	// copying straight into the partition skips tuple routing
	table := "bigdata_step"
	if first.Part >= 0 {
		table = fmt.Sprintf("bigdata_step_part_%d", first.Part)
	}
	pending := first
	recv := func() (*pb.StepBatch, error) {
		if b := pending; b != nil {
			pending = nil
			return b, nil
		}
		return stream.Recv()
	}
	n, err := s.pool.CopyFrom(stream.Context(), pgx.Identifier{table}, stepCols,
		pgx.CopyFromFunc(fromStream(len(stepCols), recv, stepLen, stepCopyRow)))
	if err != nil {
		return err
	}
	return stream.SendAndClose(&pb.Summary{Rows: n, Inserted: n})
}

func (s *server) Leaderboard(ctx context.Context, req *pb.BoardRequest) (*pb.Board, error) {
	if !req.Partitioned {
		return s.board(ctx, `select user_id, sum(steps)::int8 as s from bigdata_step group by 1`)
	}
	// a user lives in exactly one partition, so the global top 10 is among
	// the per partition top 10s
	boards := make([]*pb.Board, stepParts)
	g, gctx := errgroup.WithContext(ctx)
	for p := range stepParts {
		g.Go(func() error {
			b, err := s.board(gctx, fmt.Sprintf(`select user_id, sum(steps)::int8 as s from bigdata_step_part_%d group by 1`, p))
			boards[p] = b
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	type entry struct {
		user  int32
		steps int64
	}
	var top []entry
	out := &pb.Board{CouchUser: -1}
	for _, b := range boards {
		for i := range b.UserId {
			top = append(top, entry{b.UserId[i], b.Steps[i]})
		}
		if len(b.UserId) > 0 && (out.CouchUser < 0 || b.CouchSteps < out.CouchSteps || b.CouchSteps == out.CouchSteps && b.CouchUser < out.CouchUser) {
			out.CouchUser, out.CouchSteps = b.CouchUser, b.CouchSteps
		}
	}
	slices.SortFunc(top, func(a, b entry) int { return cmp.Or(cmp.Compare(b.steps, a.steps), cmp.Compare(a.user, b.user)) })
	for _, e := range top[:min(10, len(top))] {
		out.UserId, out.Steps = append(out.UserId, e.user), append(out.Steps, e.steps)
	}
	return out, nil
}

// Track keeps only running totals. each batch is summed per user in go
// first: on conflict do update refuses to touch the same key twice in one
// statement, and it shrinks a 5000 row batch to one row per user
func (s *server) Track(stream pb.Bigdata_TrackServer) error {
	ctx := stream.Context()
	const totals = `select user_id, steps as s from bigdata_step_total`
	sums := map[int32]int64{}
	for n := 1; ; n++ {
		b, err := stream.Recv()
		if err == io.EOF {
			board, err := s.board(ctx, totals)
			if err != nil {
				return err
			}
			return stream.Send(board)
		}
		if err != nil {
			return err
		}
		clear(sums)
		for i, u := range b.UserId {
			sums[u] += int64(b.Steps[i])
		}
		users, steps := make([]int32, 0, len(sums)), make([]int64, 0, len(sums))
		for u, v := range sums {
			users, steps = append(users, u), append(steps, v)
		}
		_, err = s.pool.Exec(ctx, `insert into bigdata_step_total
			select * from unnest($1::int4[], $2::int8[])
			on conflict (user_id) do update set steps = bigdata_step_total.steps + excluded.steps`, users, steps)
		if err != nil {
			return err
		}
		if n%100 == 0 {
			board, err := s.board(ctx, totals)
			if err != nil {
				return err
			}
			if err := stream.Send(board); err != nil {
				return err
			}
		}
	}
}

// TrackCached is Track with the totals in a redis sorted set: one pipelined
// ZINCRBY per user per batch, and the board is two short range reads.
// raw pings still reach postgres, down a COPY lane
func (s *server) TrackCached(stream pb.Bigdata_TrackCachedServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	l := copyLane(ctx, s.pool, "bigdata_step", stepCols, stepLen, stepCopyRow)
	err := func() error {
		sums := map[int32]int64{}
		for n := 1; ; n++ {
			b, err := stream.Recv()
			if err == io.EOF {
				board, err := s.redisBoard(ctx)
				if err != nil {
					return err
				}
				return stream.Send(board)
			}
			if err != nil {
				return err
			}
			clear(sums)
			for i, u := range b.UserId {
				sums[u] += int64(b.Steps[i])
			}
			pipe := s.rdb.Pipeline()
			for u, v := range sums {
				pipe.ZIncrBy(ctx, keyPrefix+"steps", float64(v), strconv.Itoa(int(u)))
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
			if err := l.send(ctx, b); err != nil {
				return err
			}
			if n%100 == 0 {
				board, err := s.redisBoard(ctx)
				if err != nil {
					return err
				}
				if err := stream.Send(board); err != nil {
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

// redisBoard reads both ends of the sorted set. redis breaks score ties by
// member as text, postgres here by user_id as a number, so everything tied
// with the boundary score is fetched and the tie broken in go
func (s *server) redisBoard(ctx context.Context) (*pb.Board, error) {
	key := keyPrefix + "steps"
	top, err := s.rdb.ZRevRangeWithScores(ctx, key, 0, 9).Result()
	if err != nil || len(top) == 0 {
		return &pb.Board{}, err
	}
	score := func(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }
	type entry struct {
		user  int32
		steps int64
	}
	entries := func(zs []redis.Z) []entry {
		out := make([]entry, len(zs))
		for i, z := range zs {
			u, _ := strconv.Atoi(z.Member.(string))
			out[i] = entry{int32(u), int64(z.Score)}
		}
		return out
	}
	high, err := s.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{Min: score(top[len(top)-1].Score), Max: "+inf"}).Result()
	if err != nil {
		return nil, err
	}
	low, err := s.rdb.ZRangeWithScores(ctx, key, 0, 0).Result()
	if err != nil {
		return nil, err
	}
	floor, err := s.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{Min: score(low[0].Score), Max: score(low[0].Score)}).Result()
	if err != nil {
		return nil, err
	}
	best := entries(high)
	slices.SortFunc(best, func(a, b entry) int { return cmp.Or(cmp.Compare(b.steps, a.steps), cmp.Compare(a.user, b.user)) })
	out := &pb.Board{}
	for _, e := range best[:min(10, len(best))] {
		out.UserId, out.Steps = append(out.UserId, e.user), append(out.Steps, e.steps)
	}
	couch := slices.MinFunc(entries(floor), func(a, b entry) int { return cmp.Compare(a.user, b.user) })
	out.CouchUser, out.CouchSteps = couch.user, couch.steps
	return out, nil
}

// boardString fingerprints the whole top 10 but only spells out the ends
func boardString(b *pb.Board) string {
	if len(b.UserId) == 0 {
		return "empty board"
	}
	var h uint64
	for i := range b.UserId {
		h = h*31 + uint64(b.UserId[i])*1_000_003 + uint64(b.Steps[i])
	}
	return fmt.Sprintf("leader %d (%d steps), couch %d (%d steps), top 10 hash %x", b.UserId[0], b.Steps[0], b.CouchUser, b.CouchSteps, h)
}

func steps(s *Session) {
	s.Note("%d fitness tracker pings from %d people; the office wants a leaderboard and to know who never got off the couch", rows, stepUsers())
	s.Exec(`drop table if exists bigdata_step, bigdata_step_total, bigdata_step_part`)
	s.Exec(`create table bigdata_step (user_id int4, at timestamptz, steps int4)`)
	s.Exec(`create table bigdata_step_total (user_id int4 primary key, steps int8)`)
	s.Exec(`create table bigdata_step_part (user_id int4, at timestamptz, steps int4) partition by list ((user_id % 8))`)
	for p := range stepParts {
		s.Exec(fmt.Sprintf(`create table bigdata_step_part_%d partition of bigdata_step_part for values in (%d)`, p, p))
	}

	track := func(open func(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[pb.StepBatch, pb.Board], error)) (string, error) {
		stream, err := open(s.ctx)
		if err != nil {
			return "", err
		}
		var last *pb.Board
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
				last = b
			}
		}()
		for b := range batches(0, rows, fillSteps) {
			if err := stream.Send(b); err != nil {
				return "", err
			}
		}
		stream.CloseSend()
		if err := <-done; err != nil {
			return "", err
		}
		return boardString(last), nil
	}

	approaches := []approach{
		approach{
			name: "copy, group",
			grpc: "client stream + unary",
			pg:   "COPY raw rows, group by",
			pick: "a nightly job, and the raw pings are worth keeping",
			run: func() (string, error) {
				stream, err := s.client.IngestSteps(s.ctx)
				if err != nil {
					return "", err
				}
				if _, err := sendAll(stream, batches(0, rows, fillSteps)); err != nil {
					return "", err
				}
				b, err := s.client.Leaderboard(s.ctx, &pb.BoardRequest{})
				if err != nil {
					return "", err
				}
				return boardString(b), nil
			},
		},
		approach{
			name: "running",
			grpc: "bidi, live boards back",
			pg:   "unnest upsert of per batch sums",
			pick: "the board is on a screen all day and nobody needs the raw pings",
			run:  func() (string, error) { return track(s.client.Track) },
		},
		approach{
			name: "partitions",
			grpc: "8 client streams + unary",
			pg:   "COPY into list partitions, 8 parallel group bys",
			pick: "the volume is huge and the table is partitioned anyway",
			run: func() (string, error) {
				lanes := make([]chan *pb.StepBatch, stepParts)
				g, gctx := errgroup.WithContext(s.ctx)
				for p := range stepParts {
					lanes[p] = make(chan *pb.StepBatch, 4)
					g.Go(func() error {
						stream, err := s.client.IngestSteps(gctx)
						if err != nil {
							return err
						}
						_, err = sendAll(stream, func(yield func(*pb.StepBatch) bool) {
							for b := range lanes[p] {
								if !yield(b) {
									return
								}
							}
						})
						return err
					})
				}
				// one producer deals rows to the partition that owns the user
				g.Go(func() error {
					defer func() {
						for _, l := range lanes {
							close(l)
						}
					}()
					open := make([]*pb.StepBatch, stepParts)
					for i := range rows {
						u, st, at := stepRow(i)
						p := u % stepParts
						b := open[p]
						if b == nil {
							b = &pb.StepBatch{Part: p}
							open[p] = b
						}
						b.UserId, b.Steps, b.At = append(b.UserId, u), append(b.Steps, st), append(b.At, at)
						if len(b.UserId) == batchSize {
							select {
							case lanes[p] <- b:
							case <-gctx.Done():
								return gctx.Err()
							}
							open[p] = nil
						}
					}
					for p, b := range open {
						if b != nil {
							lanes[p] <- b
						}
					}
					return nil
				})
				if err := g.Wait(); err != nil {
					return "", err
				}
				b, err := s.client.Leaderboard(s.ctx, &pb.BoardRequest{Partitioned: true})
				if err != nil {
					return "", err
				}
				return boardString(b), nil
			},
		},
	}
	if s.withRedis() {
		approaches = append(approaches, approach{
			name: "redis zset",
			grpc: "bidi, live boards back",
			pg:   "COPY lane; totals in a redis sorted set",
			pick: "a live board at bulk speed, raw pings kept in postgres",
			run:  func() (string, error) { return track(s.client.TrackCached) },
		})
	}
	s.race(func() error {
		if err := s.resetRedis(); err != nil {
			return err
		}
		return s.execAll(`truncate bigdata_step, bigdata_step_total, bigdata_step_part`)
	}, approaches...)

	s.Note("\nthe podium")
	s.Show(`select user_id, sum(steps) as steps, round(sum(steps) * 0.0008, 1) as km
		from (select * from bigdata_step union all select * from bigdata_step_part) x
		group by 1 order by 2 desc, 1 limit 3`)
}
