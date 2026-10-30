package bigdata

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"playground/internal/bigdata/pb"
)

func init() {
	set.Add("rocket", "launch telemetry streamed into postgres: copy, parallel copy, acked batches", rocket)
}

func fillPings(b *pb.PingBatch, lo, hi int) {
	b.Id, b.Sensor, b.At, b.Alt, b.Temp = b.Id[:0], b.Sensor[:0], b.At[:0], b.Alt[:0], b.Temp[:0]
	for i := lo; i < hi; i++ {
		b.Id = append(b.Id, int64(i))
		b.Sensor = append(b.Sensor, int32(pick(i, 1, 64)))
		b.At = append(b.At, micros(epoch.Add(time.Duration(i)*10*time.Millisecond)))
		b.Alt = append(b.Alt, unit(i, 2)*400_000)
		b.Temp = append(b.Temp, float32(unit(i, 3)*400-150))
	}
}

func pingRow(b *pb.PingBatch, i int, v []any) {
	v[0], v[1], v[2], v[3], v[4] = b.Id[i], int16(b.Sensor[i]), fromMicros(b.At[i]), b.Alt[i], b.Temp[i]
}

var pingCols = []string{"id", "sensor", "at", "alt", "temp"}

// IngestPings is one COPY fed straight off the stream: one transaction,
// nothing buffered beyond the batch on the wire
func (s *server) IngestPings(stream pb.Bigdata_IngestPingsServer) error {
	n, err := s.pool.CopyFrom(stream.Context(), pgx.Identifier{"bigdata_ping"}, pingCols,
		pgx.CopyFromFunc(fromStream(len(pingCols), stream.Recv, func(b *pb.PingBatch) int { return len(b.Id) }, pingRow)))
	if err != nil {
		return err
	}
	return stream.SendAndClose(&pb.Summary{Rows: n, Inserted: n})
}

// IngestPingsAck commits each batch on its own and only then acks it, so a
// producer that dies knows exactly where to resume
func (s *server) IngestPingsAck(stream pb.Bigdata_IngestPingsAckServer) error {
	ctx := stream.Context()
	for {
		b, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		tag, err := s.pool.Exec(ctx, `insert into bigdata_ping
			select * from unnest($1::int8[], $2::int2[], $3::timestamptz[], $4::float8[], $5::float4[])`,
			b.Id, convert[int32, int16](b.Sensor), times(b.At), b.Alt, b.Temp)
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.Ack{Seq: b.Seq, Rows: tag.RowsAffected()}); err != nil {
			return err
		}
	}
}

const (
	pingStream = keyPrefix + "pings"
	pingGroup  = "copiers"
)

// IngestPingsBuffered acks a batch the moment it is in a redis stream, one
// entry per batch holding the marshalled message. copiers in a consumer
// group drain the stream into postgres; an entry is only deleted once its
// COPY has committed, and one a dead copier left pending can be claimed by
// another with XAUTOCLAIM. the ack is as durable as redis is configured to
// be: appendfsync everysec can lose the last second
func (s *server) IngestPingsBuffered(stream pb.Bigdata_IngestPingsBufferedServer) error {
	s.copiers.Do(func() {
		s.copierErr = make(chan error, 1)
		for c := range workers {
			go s.copier(fmt.Sprintf("copier-%d", c))
		}
	})
	ctx := stream.Context()
	for {
		b, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		data, err := proto.Marshal(b)
		if err != nil {
			return err
		}
		if err := s.rdb.XAdd(ctx, &redis.XAddArgs{Stream: pingStream, Values: []any{"b", data}}).Err(); err != nil {
			return err
		}
		if err := stream.Send(&pb.Ack{Seq: b.Seq, Rows: int64(len(b.Id))}); err != nil {
			return err
		}
	}
}

// copier takes up to 8 batches at a time and lands them in one COPY
func (s *server) copier(name string) {
	for s.bg.Err() == nil {
		res, err := s.rdb.XReadGroup(s.bg, &redis.XReadGroupArgs{
			Group: pingGroup, Consumer: name, Streams: []string{pingStream, ">"}, Count: 8, Block: 50 * time.Millisecond,
		}).Result()
		if err != nil {
			// nil is a quiet stream; anything else is usually the group
			// being recreated between runs
			if err != redis.Nil {
				time.Sleep(10 * time.Millisecond)
			}
			continue
		}
		var ids []string
		var bs []*pb.PingBatch
		for _, m := range res[0].Messages {
			b := &pb.PingBatch{}
			if err := proto.Unmarshal([]byte(m.Values["b"].(string)), b); err != nil {
				s.copyFailed(err)
				continue
			}
			ids, bs = append(ids, m.ID), append(bs, b)
		}
		next := 0
		_, err = s.pool.CopyFrom(s.bg, pgx.Identifier{"bigdata_ping"}, pingCols,
			pgx.CopyFromFunc(fromStream(len(pingCols), func() (*pb.PingBatch, error) {
				if next == len(bs) {
					return nil, io.EOF
				}
				next++
				return bs[next-1], nil
			}, func(b *pb.PingBatch) int { return len(b.Id) }, pingRow)))
		if err != nil {
			s.copyFailed(err)
			continue
		}
		pipe := s.rdb.TxPipeline()
		pipe.XAck(s.bg, pingStream, pingGroup, ids...)
		pipe.XDel(s.bg, pingStream, ids...)
		if _, err := pipe.Exec(s.bg); err != nil {
			s.copyFailed(err)
		}
	}
}

func (s *server) copyFailed(err error) {
	if s.bg.Err() != nil {
		return
	}
	select {
	case s.copierErr <- err:
	default:
	}
}

// Drain returns once every buffered batch has been copied and deleted
func (s *server) Drain(ctx context.Context, _ *pb.Empty) (*pb.Summary, error) {
	for {
		select {
		case err := <-s.copierErr:
			return nil, err
		default:
		}
		n, err := s.rdb.XLen(ctx, pingStream).Result()
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return &pb.Summary{}, nil
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func rocket(s *Session) {
	s.Note("%d telemetry pings from the launch pad have to land in mission control's postgres, durably", rows)
	s.Exec(`drop table if exists bigdata_ping`)
	s.Exec(`create table bigdata_ping (id int8 primary key, sensor int2, at timestamptz, alt float8, temp float4)`)

	ingest := func(lo, hi int) error {
		stream, err := s.client.IngestPings(s.ctx)
		if err != nil {
			return err
		}
		_, err = sendAll(stream, batches(lo, hi, fillPings))
		return err
	}

	approaches := []approach{
		{
			name: "copy",
			grpc: "client stream",
			pg:   "CopyFrom off the stream",
			pick: "one producer and you want raw speed, all or nothing is fine",
			run: func() (string, error) {
				if err := ingest(0, rows); err != nil {
					return "", err
				}
				return s.checksum("bigdata_ping")
			},
		},
		{
			name: "parallel",
			grpc: "N client streams",
			pg:   "N concurrent COPYs",
			pick: "spare cores on both sides and the disk is not the limit yet",
			run: func() (string, error) {
				var g errgroup.Group
				for _, r := range split(rows, workers) {
					g.Go(func() error { return ingest(r[0], r[1]) })
				}
				if err := g.Wait(); err != nil {
					return "", err
				}
				return s.checksum("bigdata_ping")
			},
		},
		{
			name: "acked",
			grpc: "bidi, 8 in flight",
			pg:   "unnest insert, commit per batch",
			pick: "producers drop out and must resume from the last committed batch",
			run: func() (string, error) {
				stream, err := s.client.IngestPingsAck(s.ctx)
				if err != nil {
					return "", err
				}
				if err := sendAcked(stream, 8, batches(0, rows, fillPings)); err != nil {
					return "", err
				}
				return s.checksum("bigdata_ping")
			},
		},
	}
	var acked time.Duration
	if s.withRedis() {
		approaches = append(approaches, approach{
			name: "redis buffer",
			grpc: "bidi, 64 in flight",
			pg:   "consumer group copiers, COPY 8 batches at a time",
			pick: "producers need a fast ack and postgres can lag a little behind",
			run: func() (string, error) {
				start := time.Now()
				stream, err := s.client.IngestPingsBuffered(s.ctx)
				if err != nil {
					return "", err
				}
				if err := sendAcked(stream, 64, batches(0, rows, fillPings)); err != nil {
					return "", err
				}
				acked = time.Since(start)
				if _, err := s.client.Drain(s.ctx, &pb.Empty{}); err != nil {
					return "", err
				}
				return s.checksum("bigdata_ping")
			},
		})
	}
	s.race(func() error {
		if err := s.resetRedis(); err != nil {
			return err
		}
		if s.rdb != nil {
			if err := s.rdb.XGroupCreateMkStream(s.ctx, pingStream, pingGroup, "0").Err(); err != nil {
				return err
			}
		}
		return s.execAll(`truncate bigdata_ping`)
	}, approaches...)
	if acked > 0 {
		s.Note("\nredis acked every batch after %s; the rest of its time was postgres catching up", acked.Round(time.Millisecond))
	}

	s.Note("\nhighest ping and the hottest sensor")
	s.Show(`select max(alt)::int as apogee_m,
		(select sensor from bigdata_ping group by sensor order by max(temp) desc limit 1) as hottest_sensor
		from bigdata_ping`)
}
