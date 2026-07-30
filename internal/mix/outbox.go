package mix

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	set.Add("outbox", "why dual writes lose events, and how an outbox fixes it", outbox)
}

const (
	naiveTopic = "mix.orders.naive"
	safeTopic  = "mix.orders.outbox"
)

func outbox(s *Session) {
	admin := s.Kafka()
	if admin == nil {
		return
	}
	defer admin.Close()
	adm := kadm.NewClient(admin)
	adm.DeleteTopics(s.Ctx(), naiveTopic, safeTopic)
	for _, t := range []string{naiveTopic, safeTopic} {
		if _, err := adm.CreateTopic(s.Ctx(), 1, 1, nil, t); err != nil {
			s.Fail(err)
			return
		}
	}

	producer := s.Kafka()
	if producer == nil {
		return
	}
	defer producer.Close()

	s.Exec(`drop table if exists mix_outbox`)
	s.Exec(`drop table if exists mix_ob_orders`)
	s.Exec(`create table mix_ob_orders (id int primary key, total numeric)`)
	s.Exec(`create table mix_outbox (
		id serial primary key,
		order_id int,
		payload text,
		published_at timestamptz
	)`)

	s.Note("=== the naive way: write the row, then publish")
	s.Note("order 3 will crash after commit but before the publish")
	for id := 1; id <= 4; id++ {
		s.Exec(`insert into mix_ob_orders (id, total) values ($1, $2)`, id, id*100)

		if id == 3 {
			s.Note("  order %d committed, then the process died", id)
			continue
		}
		if err := producer.ProduceSync(s.Ctx(),
			&kgo.Record{Topic: naiveTopic, Value: fmt.Appendf(nil, "order-%d", id)}).FirstErr(); err != nil {
			s.Fail(err)
			return
		}
	}

	s.Show(`select count(*) as orders_in_postgres from mix_ob_orders`)
	s.Note("  events in kafka: %d", countTopic(s, naiveTopic))
	s.Note("order 3 exists and nobody will ever hear about it, the event is gone for good")

	s.Note("=== the outbox way: one transaction, two tables")
	s.Exec(`truncate mix_ob_orders`)
	s.Exec(`truncate mix_outbox`)
	for id := 1; id <= 4; id++ {
		s.Exec(`begin`)
		s.Exec(`insert into mix_ob_orders (id, total) values ($1, $2)`, id, id*100)
		s.Exec(`insert into mix_outbox (order_id, payload) values ($1, $2)`, id, fmt.Sprintf("order-%d", id))
		s.Exec(`commit`)
	}
	s.Note("nothing has been published yet, and that is fine")
	s.Show(`select count(*) as orders, (select count(*) from mix_outbox where published_at is null) as pending from mix_ob_orders`)
	s.Note("  events in kafka: %d", countTopic(s, safeTopic))

	s.Note("the relay drains the outbox, and can crash safely at any point")
	relayed := relay(s, producer, 2)
	s.Note("  relay published %d then died", relayed)
	s.Show(`select count(*) filter (where published_at is not null) as published,
			count(*) filter (where published_at is null) as still_pending
		from mix_outbox`)

	s.Note("it restarts and picks up exactly where it left off")
	relayed = relay(s, producer, 10)
	s.Note("  relay published %d more", relayed)

	s.Show(`select count(*) as orders, (select count(*) from mix_outbox where published_at is null) as pending from mix_ob_orders`)
	s.Note("  events in kafka: %d", countTopic(s, safeTopic))
	s.Note("every order has exactly one event, and no step had to be atomic across two systems")
}

func relay(s *Session, producer *kgo.Client, limit int) int {
	if s.err != nil {
		return 0
	}
	pool := s.PG()
	if pool == nil {
		return 0
	}

	rows, err := pool.Query(s.Ctx(),
		`select id, payload from mix_outbox where published_at is null order by id limit $1 for update skip locked`, limit)
	if err != nil {
		s.Fail(err)
		return 0
	}
	type item struct {
		id      int32
		payload string
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.id, &it.payload); err != nil {
			rows.Close()
			s.Fail(err)
			return 0
		}
		items = append(items, it)
	}
	rows.Close()

	sent := 0
	for _, it := range items {
		if err := producer.ProduceSync(s.Ctx(), &kgo.Record{Topic: safeTopic, Value: []byte(it.payload)}).FirstErr(); err != nil {
			s.Fail(err)
			return sent
		}
		if _, err := pool.Exec(s.Ctx(), `update mix_outbox set published_at = now() where id = $1`, it.id); err != nil {
			s.Fail(err)
			return sent
		}
		sent++
	}
	return sent
}

func countTopic(s *Session, topic string) int {
	if s.err != nil {
		return 0
	}
	cl := s.Kafka(kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if cl == nil {
		return 0
	}
	defer cl.Close()

	total, empties := 0, 0
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && empties < 2 {
		ctx, cancel := context.WithTimeout(s.Ctx(), 1500*time.Millisecond)
		fetches := cl.PollFetches(ctx)
		cancel()
		got := 0
		fetches.EachRecord(func(*kgo.Record) { got++ })
		total += got
		if got == 0 {
			empties++
		} else {
			empties = 0
		}
	}
	return total
}
