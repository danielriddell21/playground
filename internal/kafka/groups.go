package kafka

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	set.Add("groups", "a consumer group splits partitions, and rebalances when a member leaves", groups)
}

const (
	groupTopic = "play.events"
	groupName  = "play.workers"
)

func groups(s *Session) {
	s.ResetTopic(groupTopic, 4, nil)

	producer := s.Client()
	if producer == nil {
		return
	}
	defer producer.Close()

	one := s.Client(kgo.ConsumeTopics(groupTopic), kgo.ConsumerGroup(groupName),
		kgo.ClientID("worker-one"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if one == nil {
		return
	}
	defer one.Close()

	s.Note("one member owns every partition")
	members := map[string]*kgo.Client{"worker-one": one}
	s.Table([]string{"client", "partitions"}, settle(s, members, 1))

	send(s, producer, 40)
	s.Table([]string{"member", "records per partition"}, [][]any{{"worker-one", drain(s, one)}})

	s.Note("a second member joins and the group rebalances")
	two := s.Client(kgo.ConsumeTopics(groupTopic), kgo.ConsumerGroup(groupName),
		kgo.ClientID("worker-two"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if two == nil {
		return
	}
	members["worker-two"] = two
	s.Table([]string{"client", "partitions"}, settle(s, members, 2))

	send(s, producer, 40)
	s.Table([]string{"member", "records per partition"}, [][]any{
		{"worker-one", drain(s, one)},
		{"worker-two", drain(s, two)},
	})

	s.Note("member two leaves and member one takes the work back")
	two.Close()
	delete(members, "worker-two")
	s.Table([]string{"client", "partitions"}, settle(s, members, 1))

	send(s, producer, 20)
	s.Table([]string{"member", "records per partition"}, [][]any{{"worker-one", drain(s, one)}})

	s.Note("lag is what the group still owes")
	lags, err := s.Admin().Lag(s.Ctx(), groupName)
	if err != nil {
		s.Fail(err)
		return
	}
	var rows [][]any
	for _, l := range lags {
		for _, topicLag := range l.Lag {
			for _, p := range topicLag {
				rows = append(rows, []any{p.Partition, p.Commit.At, p.End.Offset, p.Lag})
			}
		}
	}
	slices.SortFunc(rows, func(a, b []any) int { return cmp.Compare(a[0].(int32), b[0].(int32)) })
	s.Table([]string{"partition", "committed", "end", "lag"}, rows)
}

func send(s *Session, cl *kgo.Client, n int) {
	if s.err != nil {
		return
	}
	var records []*kgo.Record
	for i := range n {
		records = append(records, &kgo.Record{
			Topic: groupTopic,
			Key:   []byte{byte('a' + i%8)},
			Value: []byte("event"),
		})
	}
	if err := cl.ProduceSync(s.Ctx(), records...).FirstErr(); err != nil {
		s.Fail(err)
	}
}

func settle(s *Session, members map[string]*kgo.Client, want int) [][]any {
	if s.err != nil {
		return nil
	}
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		for _, cl := range members {
			ctx, cancel := context.WithTimeout(s.Ctx(), 250*time.Millisecond)
			cl.PollFetches(ctx)
			cancel()
		}

		described, err := s.Admin().DescribeGroups(s.Ctx(), groupName)
		if err != nil {
			s.Fail(err)
			return nil
		}

		var rows [][]any
		total := 0
		for _, g := range described {
			if g.State != "Stable" || len(g.Members) != want {
				rows = nil
				break
			}
			for _, m := range g.Members {
				assigned, ok := m.Assigned.AsConsumer()
				if !ok {
					continue
				}
				var parts []int32
				for _, t := range assigned.Topics {
					parts = append(parts, t.Partitions...)
				}
				slices.Sort(parts)
				total += len(parts)
				rows = append(rows, []any{m.ClientID, parts})
			}
		}
		if len(rows) == want && total == 4 {
			slices.SortFunc(rows, func(a, b []any) int { return cmp.Compare(a[0].(string), b[0].(string)) })
			return rows
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.Note("  group did not settle in time")
	return nil
}

func drain(s *Session, cl *kgo.Client) map[int32]int {
	counts := map[int32]int{}
	if s.err != nil {
		return counts
	}
	empties := 0
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && empties < 3 {
		ctx, cancel := context.WithTimeout(s.Ctx(), 1500*time.Millisecond)
		fetches := cl.PollFetches(ctx)
		cancel()
		if fetches.IsClientClosed() {
			break
		}
		got := 0
		fetches.EachRecord(func(r *kgo.Record) {
			counts[r.Partition]++
			got++
		})
		if got == 0 {
			empties++
		} else {
			empties = 0
		}
	}
	cl.CommitUncommittedOffsets(s.Ctx())
	return counts
}
