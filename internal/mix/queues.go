package mix

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	set.Add("queues", "the same work through postgres skip locked and a kafka group", queues)
}

const jobCount = 12

func queues(s *Session) {
	s.Note("=== postgres, a queue made of rows")
	s.Exec(`drop table if exists mix_jobs cascade`)
	s.Exec(`drop table if exists mix_orders cascade`)
	s.Exec(`create table mix_orders (id int primary key, total numeric)`)
	s.Exec(`create table mix_jobs (
		id serial primary key,
		order_id int references mix_orders(id),
		state text default 'queued',
		worker text
	)`)

	s.Note("the enqueue is in the same transaction as the business row")
	for i := range jobCount {
		s.Exec(`begin`)
		s.Exec(`insert into mix_orders (id, total) values ($1, $2)`, i+1, (i+1)*10)
		s.Exec(`insert into mix_jobs (order_id) values ($1)`, i+1)
		s.Exec(`commit`)
	}
	s.Show(`select count(*) as orders, (select count(*) from mix_jobs) as jobs from mix_orders`)

	s.Note("three workers claim with for update skip locked")
	for _, worker := range []string{"w1", "w2", "w3"} {
		s.Exec(`with picked as (
				select id from mix_jobs where state = 'queued' order by id limit 4 for update skip locked
			)
			update mix_jobs set state = 'done', worker = $1 where id in (select id from picked)`, worker)
	}
	s.Show(`select worker, count(*) as handled from mix_jobs group by worker order by worker`)

	s.Note("done means gone, there is nothing to replay")
	s.Show(`select state, count(*) from mix_jobs group by state`)

	s.Note("=== kafka, a queue made of a log")
	const topic = "mix.jobs"
	const group = "mix.workers"

	admin := s.Kafka()
	if admin == nil {
		return
	}
	defer admin.Close()
	adm := kadm.NewClient(admin)
	adm.DeleteGroups(s.Ctx(), group, "mix.audit")
	adm.DeleteTopics(s.Ctx(), topic)
	if _, err := adm.CreateTopic(s.Ctx(), 3, 1, nil, topic); err != nil {
		s.Fail(err)
		return
	}

	s.Note("three consumers join the group first, so all three get a partition")
	counts := map[string]int{}
	var clients []*kgo.Client
	for _, name := range []string{"c1", "c2", "c3"} {
		cl := s.Kafka(kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group),
			kgo.ClientID(name), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if cl == nil {
			return
		}
		clients = append(clients, cl)
	}
	defer func() {
		for _, cl := range clients {
			cl.Close()
		}
	}()
	settle(s, adm, group, clients)

	producer := s.Kafka()
	if producer == nil {
		return
	}
	defer producer.Close()
	var records []*kgo.Record
	for i := range jobCount {
		records = append(records, &kgo.Record{
			Topic: topic,
			Key:   fmt.Appendf(nil, "order-%d", i+1),
			Value: fmt.Appendf(nil, "order-%d", i+1),
		})
	}
	if err := producer.ProduceSync(s.Ctx(), records...).FirstErr(); err != nil {
		s.Fail(err)
		return
	}
	s.Note("produced %d jobs across 3 partitions", jobCount)

	s.Note("each consumer drains only what it was assigned")
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i, cl := range clients {
		name := []string{"c1", "c2", "c3"}[i]
		wg.Go(func() {
			n := drainCount(s, cl)
			mu.Lock()
			counts[name] = n
			mu.Unlock()
		})
	}
	wg.Wait()
	var rows [][]any
	for _, name := range []string{"c1", "c2", "c3"} {
		rows = append(rows, []any{name, counts[name]})
	}
	s.Table([]string{"consumer", "handled"}, rows)

	s.Note("the log is still there, so a new group can read it all again")
	replay := s.Kafka(kgo.ConsumeTopics(topic), kgo.ConsumerGroup("mix.audit"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if replay == nil {
		return
	}
	defer replay.Close()
	s.Note("  a brand new group read %d records that were already handled", drainCount(s, replay))

	s.Note("=== so")
	s.Table([]string{"want", "reach for"}, [][]any{
		{"enqueue atomic with your data", "postgres"},
		{"one job handled exactly once, then forgotten", "postgres"},
		{"many independent readers of the same events", "kafka"},
		{"replay history after a bug", "kafka"},
		{"ordering per key across restarts", "kafka"},
		{"a queue you can query with SQL", "postgres"},
	})
}

func drainCount(s *Session, cl *kgo.Client) int {
	if s.err != nil {
		return 0
	}
	total, empties := 0, 0
	start := time.Now()
	deadline := start.Add(40 * time.Second)
	for time.Now().Before(deadline) && empties < 3 {
		ctx, cancel := context.WithTimeout(s.Ctx(), 1500*time.Millisecond)
		fetches := cl.PollFetches(ctx)
		cancel()
		if fetches.IsClientClosed() {
			break
		}
		got := 0
		fetches.EachRecord(func(*kgo.Record) { got++ })
		total += got
		switch {
		case got > 0:
			empties = 0
		case time.Since(start) < 12*time.Second:
		default:
			empties++
		}
	}
	cl.CommitUncommittedOffsets(s.Ctx())
	return total
}

func settle(s *Session, adm *kadm.Client, group string, clients []*kgo.Client) {
	if s.err != nil {
		return
	}
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		for _, cl := range clients {
			ctx, cancel := context.WithTimeout(s.Ctx(), 250*time.Millisecond)
			cl.PollFetches(ctx)
			cancel()
		}
		described, err := adm.DescribeGroups(s.Ctx(), group)
		if err != nil {
			s.Fail(err)
			return
		}
		for _, g := range described {
			if g.State != "Stable" || len(g.Members) != len(clients) {
				continue
			}
			total := 0
			for _, m := range g.Members {
				if a, ok := m.Assigned.AsConsumer(); ok {
					for _, t := range a.Topics {
						total += len(t.Partitions)
					}
				}
			}
			if total == 3 {
				s.Note("  group is stable, 3 members holding 3 partitions")
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.Note("  group did not settle in time")
}
