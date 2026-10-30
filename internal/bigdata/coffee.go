package bigdata

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"

	"playground/internal/bigdata/pb"
)

func init() {
	set.Add("coffee", "orders in, priced orders out: bidi insert returning, copy then set based, concurrent unary batches", coffee)
}

// everyone orders four coffees a day for a working week. rows are ordered by
// person and batches hold whole people, so a person's daily caffeine total
// can be worked out inside any one batch
const ordersPerPerson = 20

func coffeeBatch() int { return max(batchSize/ordersPerPerson, 1) * ordersPerPerson }

func fillOrders(b *pb.OrderBatch, lo, hi int) {
	b.Id, b.Person, b.Drink, b.Size, b.At = b.Id[:0], b.Person[:0], b.Drink[:0], b.Size[:0], b.At[:0]
	for i := lo; i < hi; i++ {
		day, slot := i%ordersPerPerson/4, i%4
		at := epoch.Add(time.Duration(day)*24*time.Hour + time.Duration(8+slot*2)*time.Hour + time.Duration(pick(i, 3, 90))*time.Minute)
		b.Id = append(b.Id, int64(i))
		b.Person = append(b.Person, int32(i/ordersPerPerson))
		b.Drink = append(b.Drink, int32(pick(i, 1, 5)))
		b.Size = append(b.Size, int32(pick(i, 2, 3)))
		b.At = append(b.At, micros(at))
	}
}

func orderBatches(lo, hi int) func(yield func(*pb.OrderBatch) bool) {
	return func(yield func(*pb.OrderBatch) bool) {
		b := &pb.OrderBatch{}
		for blo, bhi := range chunks(hi-lo, coffeeBatch()) {
			fillOrders(b, lo+blo, lo+bhi)
			if !yield(b) {
				return
			}
		}
	}
}

// priced is the whole pricing rule in one projection: monday costs half
// again, and anyone past 600mg in a day gets an intervention
func priced(src string) string {
	return `select s.id, s.person, s.drink, s.size, s.at,
			(m.cents * case when extract(isodow from s.at) = 1 then 3 else 2 end / 2)::int8,
			m.mg,
			sum(m.mg) over (partition by s.person, s.at::date) > 600
		from ` + src + ` s join bigdata_menu m using (drink, size)`
}

var brewSQL = `with s as (
		select * from unnest($1::int8[], $2::int4[], $3::int2[], $4::int2[], $5::timestamptz[]) as s(id, person, drink, size, at)
	)
	insert into bigdata_order ` + priced("s") + `
	on conflict (id) do nothing
	returning id, cents, mg, intervention`

// brew prices one batch in a single statement and hands the rows straight
// back; on conflict do nothing makes a retried batch harmless
func (s *server) brew(ctx context.Context, b *pb.OrderBatch) (*pb.PricedBatch, error) {
	rows, err := s.pool.Query(ctx, brewSQL,
		b.Id, b.Person, convert[int32, int16](b.Drink), convert[int32, int16](b.Size), times(b.At))
	if err != nil {
		return nil, err
	}
	return collectPriced(rows, len(b.Id))
}

func collectPriced(rows pgx.Rows, size int) (*pb.PricedBatch, error) {
	defer rows.Close()
	out := &pb.PricedBatch{
		Id: make([]int64, 0, size), Cents: make([]int64, 0, size),
		Mg: make([]int32, 0, size), Intervention: make([]bool, 0, size),
	}
	var id, cents int64
	var mg int32
	var flag bool
	for rows.Next() {
		if err := rows.Scan(&id, &cents, &mg, &flag); err != nil {
			return nil, err
		}
		out.Id, out.Cents, out.Mg, out.Intervention = append(out.Id, id), append(out.Cents, cents), append(out.Mg, mg), append(out.Intervention, flag)
	}
	return out, rows.Err()
}

// Brew prices up to four batches at once on separate connections; replies
// go back in whatever order they finish
func (s *server) Brew(stream pb.Bigdata_BrewServer) error {
	g, ctx := errgroup.WithContext(stream.Context())
	g.SetLimit(4)
	var mu sync.Mutex
	for {
		b, err := stream.Recv()
		if err == io.EOF {
			return g.Wait()
		}
		if err != nil {
			g.Wait()
			return err
		}
		g.Go(func() error {
			out, err := s.brew(ctx, b)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			return stream.Send(out)
		})
	}
}

func (s *server) BrewBatch(ctx context.Context, b *pb.OrderBatch) (*pb.PricedBatch, error) {
	return s.brew(ctx, b)
}

func (s *server) LoadOrders(stream pb.Bigdata_LoadOrdersServer) error {
	ctx := stream.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `create temp table stage (id int8, person int4, drink int2, size int2, at timestamptz) on commit drop`); err != nil {
		return err
	}
	n, err := tx.CopyFrom(ctx, pgx.Identifier{"stage"}, []string{"id", "person", "drink", "size", "at"},
		pgx.CopyFromFunc(fromStream(5, stream.Recv, func(b *pb.OrderBatch) int { return len(b.Id) }, func(b *pb.OrderBatch, i int, v []any) {
			v[0], v[1], v[2], v[3], v[4] = b.Id[i], b.Person[i], int16(b.Drink[i]), int16(b.Size[i]), fromMicros(b.At[i])
		})))
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `insert into bigdata_order `+priced("stage")); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return stream.SendAndClose(&pb.Summary{Rows: n, Inserted: n})
}

func (s *server) ExportPriced(_ *pb.Empty, stream pb.Bigdata_ExportPricedServer) error {
	ctx := stream.Context()
	rows, err := s.pool.Query(ctx, `select id, cents, mg, intervention from bigdata_order`)
	if err != nil {
		return err
	}
	defer rows.Close()
	b := &pb.PricedBatch{}
	var id, cents int64
	var mg int32
	var flag bool
	for rows.Next() {
		if err := rows.Scan(&id, &cents, &mg, &flag); err != nil {
			return err
		}
		b.Id, b.Cents, b.Mg, b.Intervention = append(b.Id, id), append(b.Cents, cents), append(b.Mg, mg), append(b.Intervention, flag)
		if len(b.Id) == batchSize {
			if err := stream.Send(b); err != nil {
				return err
			}
			b.Id, b.Cents, b.Mg, b.Intervention = b.Id[:0], b.Cents[:0], b.Mg[:0], b.Intervention[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(b.Id) > 0 {
		return stream.Send(b)
	}
	return nil
}

type bill struct {
	mu                       sync.Mutex
	orders, cents, mg, flags int64
}

func (t *bill) take(b *pb.PricedBatch) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.orders += int64(len(b.Id))
	for i := range b.Id {
		t.cents += b.Cents[i]
		t.mg += int64(b.Mg[i])
		if b.Intervention[i] {
			t.flags++
		}
	}
}

func (t *bill) answer() string {
	return fmt.Sprintf("%d orders, £%d.%02d, %d mg caffeine, %d cups past the line", t.orders, t.cents/100, t.cents%100, t.mg, t.flags)
}

func coffee(s *Session) {
	s.Note("%d office coffee orders; price them (mondays cost half again), count the caffeine, flag anyone past 600mg in a day", rows)
	s.Exec(`drop table if exists bigdata_order, bigdata_menu`)
	s.Exec(`create table bigdata_menu (drink int2, size int2, name text, cents int4, mg int4, primary key (drink, size))`)
	s.Exec(`insert into bigdata_menu values
		(0, 0, 'espresso', 180, 64),   (0, 1, 'espresso', 230, 128),  (0, 2, 'espresso', 280, 192),
		(1, 0, 'latte', 290, 75),      (1, 1, 'latte', 340, 150),     (1, 2, 'latte', 390, 225),
		(2, 0, 'cold brew', 320, 150), (2, 1, 'cold brew', 370, 200), (2, 2, 'cold brew', 420, 300),
		(3, 0, 'matcha', 310, 35),     (3, 1, 'matcha', 360, 70),     (3, 2, 'matcha', 410, 105),
		(4, 0, 'decaf', 270, 3),       (4, 1, 'decaf', 320, 5),       (4, 2, 'decaf', 370, 8)`)
	s.Exec(`create table bigdata_order (id int8 primary key, person int4, drink int2, size int2, at timestamptz, cents int8, mg int4, intervention bool)`)

	s.race(
		func() error { return s.execAll(`truncate bigdata_order`) },
		approach{
			name: "returning",
			grpc: "bidi",
			pg:   "unnest insert ... returning, 4 batches at once",
			pick: "the caller wants its priced rows back while it is still sending",
			run: func() (string, error) {
				stream, err := s.client.Brew(s.ctx)
				if err != nil {
					return "", err
				}
				t := &bill{}
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
						t.take(b)
					}
				}()
				for b := range orderBatches(0, rows) {
					if err := stream.Send(b); err != nil {
						return "", err
					}
				}
				stream.CloseSend()
				if err := <-done; err != nil {
					return "", err
				}
				return t.answer(), nil
			},
		},
		approach{
			name: "set based",
			grpc: "client stream, then server stream",
			pg:   "COPY to temp, one insert ... select with a window",
			pick: "a whole day lands at once, or the window crosses batches",
			run: func() (string, error) {
				stream, err := s.client.LoadOrders(s.ctx)
				if err != nil {
					return "", err
				}
				if _, err := sendAll(stream, orderBatches(0, rows)); err != nil {
					return "", err
				}
				out, err := s.client.ExportPriced(s.ctx, &pb.Empty{})
				if err != nil {
					return "", err
				}
				t := &bill{}
				err = recvAll(out, t.take)
				return t.answer(), err
			},
		},
		approach{
			name: "unary fanout",
			grpc: "unary, 8 in flight",
			pg:   "same insert ... returning per call",
			pick: "stateless servers behind a load balancer, retry per batch",
			run: func() (string, error) {
				t := &bill{}
				g, gctx := errgroup.WithContext(s.ctx)
				g.SetLimit(8)
				for lo, hi := range chunks(rows, coffeeBatch()) {
					g.Go(func() error {
						b := &pb.OrderBatch{}
						fillOrders(b, lo, hi)
						out, err := s.client.BrewBatch(gctx, b)
						if err != nil {
							return err
						}
						t.take(out)
						return nil
					})
				}
				err := g.Wait()
				return t.answer(), err
			},
		},
	)

	s.Note("\nwho needs a word")
	s.Show(`select o.person, o.at::date as day, sum(o.mg) as mg, (sum(o.cents) / 100.0)::numeric(8, 2) as spent, string_agg(m.name, ', ' order by o.at) as order_log
		from bigdata_order o join bigdata_menu m using (drink, size)
		group by 1, 2 order by 3 desc, 1 limit 3`)
}
