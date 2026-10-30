package bigdata

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"playground/internal/bigdata/pb"
)

func init() {
	set.Add("heist", "vault door logs streamed out to the detectives: one query, tid ranges in parallel, binary copy", heist)
}

const vaultDoor = 0

// postgres binary timestamps count microseconds from 2000-01-01
const pgEpochMicros = 946_684_800_000_000

type vaultBatch struct {
	b    *pb.LogBatch
	send func(*pb.LogBatch) error
}

func (v *vaultBatch) add(id int64, badge, door int32, at int64) error {
	b := v.b
	b.Id, b.Badge, b.Door, b.At = append(b.Id, id), append(b.Badge, badge), append(b.Door, door), append(b.At, at)
	if len(b.Id) < batchSize {
		return nil
	}
	return v.flush()
}

func (v *vaultBatch) flush() error {
	b := v.b
	if len(b.Id) == 0 {
		return nil
	}
	err := v.send(b)
	b.Id, b.Badge, b.Door, b.At = b.Id[:0], b.Badge[:0], b.Door[:0], b.At[:0]
	return err
}

func (s *server) VaultBounds(ctx context.Context, _ *pb.Empty) (*pb.Bounds, error) {
	var pages int64
	err := s.pool.QueryRow(ctx, `select pg_relation_size('bigdata_vault') / current_setting('block_size')::int`).Scan(&pages)
	return &pb.Bounds{Pages: pages}, err
}

func (s *server) ExportVault(req *pb.ExportRequest, stream pb.Bigdata_ExportVaultServer) error {
	ctx := stream.Context()
	out := &vaultBatch{b: &pb.LogBatch{}, send: stream.Send}

	if req.Mode == pb.ExportRequest_COPY {
		conn, err := s.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()
		w := &copyReader{row: out.add}
		if _, err := conn.Conn().PgConn().CopyTo(ctx, w, `copy bigdata_vault (id, badge, door, at) to stdout (format binary)`); err != nil {
			return err
		}
		if w.err != nil {
			return w.err
		}
		return out.flush()
	}

	sql, args := `select id, badge, door, at from bigdata_vault`, []any(nil)
	if req.Mode == pb.ExportRequest_CTID {
		// a tid range scan reads exactly these heap pages, no index involved
		sql += ` where ctid >= $1::text::tid and ctid < $2::text::tid`
		args = []any{fmt.Sprintf("(%d,0)", req.Lo), fmt.Sprintf("(%d,0)", req.Hi)}
	}
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	// raw binary values skip the reflection Scan would do per column
	for rows.Next() {
		v := rows.RawValues()
		err := out.add(
			int64(binary.BigEndian.Uint64(v[0])),
			int32(binary.BigEndian.Uint32(v[1])),
			int32(int16(binary.BigEndian.Uint16(v[2]))),
			int64(binary.BigEndian.Uint64(v[3]))+pgEpochMicros,
		)
		if err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return out.flush()
}

// copyReader parses COPY binary as it arrives: a 19 byte header, then per
// tuple an int16 field count and per field an int32 length and the bytes.
// a field count of -1 ends the stream
type copyReader struct {
	buf    []byte
	header bool
	done   bool
	row    func(id int64, badge, door int32, at int64) error
	err    error
}

func (c *copyReader) Write(p []byte) (int, error) {
	c.buf = append(c.buf, p...)
	if !c.header {
		if len(c.buf) < 19 {
			return len(p), nil
		}
		ext := int(binary.BigEndian.Uint32(c.buf[15:19]))
		if len(c.buf) < 19+ext {
			return len(p), nil
		}
		c.buf, c.header = c.buf[19+ext:], true
	}
	const tuple = 2 + 4 + 8 + 4 + 4 + 4 + 2 + 4 + 8
	off := 0
	for !c.done && len(c.buf)-off >= 2 {
		if int16(binary.BigEndian.Uint16(c.buf[off:])) == -1 {
			c.done = true
			off += 2
			break
		}
		if len(c.buf)-off < tuple {
			break
		}
		t := c.buf[off:]
		err := c.row(
			int64(binary.BigEndian.Uint64(t[6:])),
			int32(binary.BigEndian.Uint32(t[18:])),
			int32(int16(binary.BigEndian.Uint16(t[26:]))),
			int64(binary.BigEndian.Uint64(t[32:]))+pgEpochMicros,
		)
		if err != nil {
			c.err = err
			return 0, err
		}
		off += tuple
	}
	c.buf = append(c.buf[:0], c.buf[off:]...)
	return len(p), nil
}

// tally is the detectives' side: every row is counted, vault entries
// between 2 and 4am are pinned to the badge
type tally struct {
	rows   int64
	idSum  int64
	visits map[int32]int32
}

func newTally() *tally { return &tally{visits: map[int32]int32{}} }

func (t *tally) take(b *pb.LogBatch) {
	t.rows += int64(len(b.Id))
	for i, id := range b.Id {
		t.idSum += id
		if b.Door[i] != vaultDoor {
			continue
		}
		if h := fromMicros(b.At[i]).Hour(); h >= 2 && h < 4 {
			t.visits[b.Badge[i]]++
		}
	}
}

func (t *tally) merge(o *tally) {
	t.rows += o.rows
	t.idSum += o.idSum
	for k, v := range o.visits {
		t.visits[k] += v
	}
}

func (t *tally) answer() string {
	suspects, top, topN := 0, int32(-1), int32(0)
	for badge, n := range t.visits {
		if n >= 3 {
			suspects++
		}
		if n > topN || n == topN && badge < top {
			top, topN = badge, n
		}
	}
	return fmt.Sprintf("%d rows, id sum %d, %d suspects, badge %d in the vault %d times", t.rows, t.idSum, suspects, top, topN)
}

func heist(s *Session) {
	s.Note("the casino vault was emptied; %d door scans go to the detectives, who want every badge in the vault 3+ times between 2 and 4am", rows)
	s.Exec(`drop table if exists bigdata_vault`)
	s.Exec(`create table bigdata_vault (id int8 primary key, badge int4, door int2, at timestamptz)`)
	s.Exec(`insert into bigdata_vault
		select i, abs(hashint8(i)) % $2, abs(hashint8(i + 7919)) % 12, '2026-01-05'::timestamptz + i * interval '500 ms'
		from generate_series(0, $1::int8 - 1) i`, rows, max(rows/150, 10))
	s.Exec(`vacuum analyze bigdata_vault`)

	export := func(req *pb.ExportRequest, t *tally) error {
		stream, err := s.client.ExportVault(s.ctx, req)
		if err != nil {
			return err
		}
		return recvAll(stream, t.take)
	}

	s.race(
		func() error { return nil },
		approach{
			name: "one query",
			grpc: "server stream",
			pg:   "streamed select, raw binary rows",
			pick: "the simple default, one consumer, constant memory",
			run: func() (string, error) {
				t := newTally()
				err := export(&pb.ExportRequest{Mode: pb.ExportRequest_QUERY}, t)
				return t.answer(), err
			},
		},
		approach{
			name: "tid ranges",
			grpc: "N server streams",
			pg:   "tid range scans over heap pages",
			pick: "dumping a whole table with cores to spare and order does not matter",
			run: func() (string, error) {
				bounds, err := s.client.VaultBounds(s.ctx, &pb.Empty{})
				if err != nil {
					return "", err
				}
				total := newTally()
				var mu sync.Mutex
				var g errgroup.Group
				for _, r := range split(int(bounds.Pages)+1, workers) {
					g.Go(func() error {
						t := newTally()
						if err := export(&pb.ExportRequest{Mode: pb.ExportRequest_CTID, Lo: int64(r[0]), Hi: int64(r[1])}, t); err != nil {
							return err
						}
						mu.Lock()
						total.merge(t)
						mu.Unlock()
						return nil
					})
				}
				if err := g.Wait(); err != nil {
					return "", err
				}
				return total.answer(), nil
			},
		},
		approach{
			name: "binary copy",
			grpc: "server stream",
			pg:   "copy to stdout (format binary)",
			pick: "one stream as fast as postgres can push it, no row protocol",
			run: func() (string, error) {
				t := newTally()
				err := export(&pb.ExportRequest{Mode: pb.ExportRequest_COPY}, t)
				return t.answer(), err
			},
		},
	)
}
