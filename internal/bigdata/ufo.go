package bigdata

import (
	"context"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"

	"playground/internal/bigdata/pb"
)

func init() {
	set.Add("ufo", "sightings with duplicate reports, deduped on the way in: on conflict, merge, pipelined upserts", ufo)
}

// one report in five repeats an earlier sighting, same place same minute,
// with its own opinion of the shape. the first report stands
func sightingKey(i int) int {
	for i > 0 && pick(i, 1, 5) == 0 {
		i = pick(i, 2, i)
	}
	return i
}

func fillSightings(b *pb.SightingBatch, lo, hi int) {
	b.Id, b.Cell, b.Minute, b.Shape = b.Id[:0], b.Cell[:0], b.Minute[:0], b.Shape[:0]
	for i := lo; i < hi; i++ {
		k := sightingKey(i)
		b.Id = append(b.Id, int64(i))
		b.Cell = append(b.Cell, int64(hash(k, 3)>>20))
		b.Minute = append(b.Minute, micros(epoch.Add(time.Duration(k%525_600)*time.Minute)))
		b.Shape = append(b.Shape, int32(pick(i, 4, 8)))
	}
}

func sightingRow(b *pb.SightingBatch, i int, v []any) {
	v[0], v[1], v[2], v[3] = b.Id[i], b.Cell[i], fromMicros(b.Minute[i]), int16(b.Shape[i])
}

// stage copies the stream into a temp table that has no indexes and writes
// no wal, then lets final fold it into the real table in the same transaction
func (s *server) stage(ctx context.Context, recv func() (*pb.SightingBatch, error), final string) (*pb.Summary, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `create temp table stage (like bigdata_sighting) on commit drop`); err != nil {
		return nil, err
	}
	n, err := tx.CopyFrom(ctx, pgx.Identifier{"stage"}, []string{"id", "cell", "minute", "shape"},
		pgx.CopyFromFunc(fromStream(4, recv, func(b *pb.SightingBatch) int { return len(b.Id) }, sightingRow)))
	if err != nil {
		return nil, err
	}
	sum := &pb.Summary{Rows: n}
	if err := tx.QueryRow(ctx, final).Scan(&sum.Inserted, &sum.Updated); err != nil {
		return nil, err
	}
	return sum, tx.Commit(ctx)
}

func (s *server) LoadSightingsUpsert(stream pb.Bigdata_LoadSightingsUpsertServer) error {
	sum, err := s.stage(stream.Context(), stream.Recv, `with ins as (
			insert into bigdata_sighting
			select distinct on (cell, minute) * from stage order by cell, minute, id
			on conflict do nothing
			returning 1
		) select count(*), 0 from ins`)
	if err != nil {
		return err
	}
	return stream.SendAndClose(sum)
}

// merge can also replace a stored sighting when an earlier report turns up
// late, and says which branch every row took
func (s *server) LoadSightingsMerge(stream pb.Bigdata_LoadSightingsMergeServer) error {
	sum, err := s.stage(stream.Context(), stream.Recv, `with m as (
			merge into bigdata_sighting t
			using (select distinct on (cell, minute) * from stage order by cell, minute, id) s
			on t.cell = s.cell and t.minute = s.minute
			when matched and s.id < t.id then update set id = s.id, shape = s.shape
			when not matched then insert values (s.id, s.cell, s.minute, s.shape)
			returning merge_action() as action
		) select count(*) filter (where action = 'INSERT'), count(*) filter (where action = 'UPDATE') from m`)
	if err != nil {
		return err
	}
	return stream.SendAndClose(sum)
}

// TrickleSightings queues four batches of upserts and sends them in one
// round trip. batches arrive in id order and unnest keeps that order, so
// on conflict do nothing still lets the first report win
func (s *server) TrickleSightings(stream pb.Bigdata_TrickleSightingsServer) error {
	ctx := stream.Context()
	var queued pgx.Batch
	var seqs []int64
	flush := func() error {
		if queued.Len() == 0 {
			return nil
		}
		res := s.pool.SendBatch(ctx, &queued)
		for _, seq := range seqs {
			tag, err := res.Exec()
			if err != nil {
				res.Close()
				return err
			}
			if err := stream.Send(&pb.Ack{Seq: seq, Rows: tag.RowsAffected()}); err != nil {
				res.Close()
				return err
			}
		}
		queued, seqs = pgx.Batch{}, seqs[:0]
		return res.Close()
	}
	for {
		b, err := stream.Recv()
		if err == io.EOF {
			return flush()
		}
		if err != nil {
			return err
		}
		queued.Queue(`insert into bigdata_sighting
			select * from unnest($1::int8[], $2::int8[], $3::timestamptz[], $4::int2[])
			on conflict do nothing`,
			b.Id, b.Cell, times(b.Minute), convert[int32, int16](b.Shape))
		seqs = append(seqs, b.Seq)
		if queued.Len() == 4 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func ufo(s *Session) {
	s.Note("%d ufo reports, about a fifth of them the same sighting reported again; the registry keeps one per place and minute", rows)
	s.Exec(`drop table if exists bigdata_sighting`)
	s.Exec(`create table bigdata_sighting (id int8, cell int8, minute timestamptz, shape int2, unique (cell, minute))`)

	load := func(open func(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[pb.SightingBatch, pb.Summary], error)) (string, error) {
		stream, err := open(s.ctx)
		if err != nil {
			return "", err
		}
		if _, err := sendAll(stream, batches(0, rows, fillSightings)); err != nil {
			return "", err
		}
		return s.checksum("bigdata_sighting")
	}

	s.race(
		func() error { return s.execAll(`truncate bigdata_sighting`) },
		approach{
			name: "on conflict",
			grpc: "client stream",
			pg:   "COPY to temp, insert distinct on conflict",
			pick: "bulk load and a simple keep-the-first rule",
			run:  func() (string, error) { return load(s.client.LoadSightingsUpsert) },
		},
		approach{
			name: "merge",
			grpc: "client stream",
			pg:   "COPY to temp, merge returning merge_action()",
			pick: "reloading into a table that already has rows, and you want insert vs update counts",
			run:  func() (string, error) { return load(s.client.LoadSightingsMerge) },
		},
		approach{
			name: "pipelined",
			grpc: "bidi, 8 in flight",
			pg:   "unnest upserts, 4 per round trip",
			pick: "reports trickle in all day and there is no load window to stage in",
			run: func() (string, error) {
				stream, err := s.client.TrickleSightings(s.ctx)
				if err != nil {
					return "", err
				}
				if err := sendAcked(stream, 8, batches(0, rows, fillSightings)); err != nil {
					return "", err
				}
				return s.checksum("bigdata_sighting")
			},
		},
	)

	s.Note("\nreports per stored sighting")
	s.Show(`select count(*) as stored, round($1::numeric / count(*), 2) as reports_each from bigdata_sighting`, rows)
}
