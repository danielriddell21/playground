package kafka

import (
	"maps"
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	set.Add("produce", "keys decide partitions, and a partition keeps its order", produce)
}

func produce(s *Session) {
	const topic = "play.orders"
	s.ResetTopic(topic, 3, nil)

	cl := s.Client()
	if cl == nil {
		return
	}
	defer cl.Close()

	s.Note("the same key always lands on the same partition")
	var records []*kgo.Record
	for _, r := range []struct{ key, value string }{
		{"ada", "order-1"}, {"linus", "order-2"}, {"ada", "order-3"},
		{"grace", "order-4"}, {"ada", "order-5"}, {"linus", "order-6"},
	} {
		records = append(records, &kgo.Record{
			Topic: topic,
			Key:   []byte(r.key),
			Value: []byte(r.value),
			Headers: []kgo.RecordHeader{
				{Key: "source", Value: []byte("playground")},
			},
		})
	}

	results := cl.ProduceSync(s.Ctx(), records...)
	if err := results.FirstErr(); err != nil {
		s.Fail(err)
		return
	}

	var rows [][]any
	for _, res := range results {
		rows = append(rows, []any{string(res.Record.Key), string(res.Record.Value), res.Record.Partition, res.Record.Offset})
	}
	s.Table([]string{"key", "value", "partition", "offset"}, rows)

	s.Note("every key maps to one partition, which is why per key order holds")
	home := map[string]int32{}
	for _, res := range results {
		home[string(res.Record.Key)] = res.Record.Partition
	}
	rows = nil
	for _, k := range slices.Sorted(maps.Keys(home)) {
		rows = append(rows, []any{k, home[k]})
	}
	s.Table([]string{"key", "always partition"}, rows)

	s.Note("no key means the client chooses, and it sticks to one partition per batch")
	var unkeyed []*kgo.Record
	for range 4 {
		unkeyed = append(unkeyed, &kgo.Record{Topic: topic, Value: []byte("no-key")})
	}
	results = cl.ProduceSync(s.Ctx(), unkeyed...)
	if err := results.FirstErr(); err != nil {
		s.Fail(err)
		return
	}
	rows = nil
	for _, res := range results {
		rows = append(rows, []any{"(none)", string(res.Record.Value), res.Record.Partition, res.Record.Offset})
	}
	s.Table([]string{"key", "value", "partition", "offset"}, rows)

	s.Note("read it back, one partition at a time, and headers come too")
	reader := s.Client(kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if reader == nil {
		return
	}
	defer reader.Close()

	rows = nil
	seen := 0
	for seen < len(records)+len(unkeyed) {
		fetches := reader.PollFetches(s.Ctx())
		if err := fetches.Err(); err != nil {
			s.Fail(err)
			return
		}
		fetches.EachRecord(func(r *kgo.Record) {
			header := ""
			for _, h := range r.Headers {
				header = h.Key + "=" + string(h.Value)
			}
			rows = append(rows, []any{r.Partition, r.Offset, string(r.Key), string(r.Value), header})
			seen++
		})
	}
	s.Table([]string{"partition", "offset", "key", "value", "header"}, rows)
}
