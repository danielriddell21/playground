package bigdata

import (
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"

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

	s.race(
		func() error { return s.execAll(`truncate bigdata_ping`) },
		approach{
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
		approach{
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
		approach{
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
	)

	s.Note("\nhighest ping and the hottest sensor")
	s.Show(`select max(alt)::int as apogee_m,
		(select sensor from bigdata_ping group by sensor order by max(temp) desc limit 1) as hottest_sensor
		from bigdata_ping`)
}
