package kafka

import (
	"context"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	set.Add("offsets", "the log stays put, so you can rewind and replay it", offsets)
}

func offsets(s *Session) {
	const topic = "play.log"
	s.ResetTopic(topic, 1, nil)

	producer := s.Client()
	if producer == nil {
		return
	}
	defer producer.Close()

	var records []*kgo.Record
	for i := range 10 {
		records = append(records, &kgo.Record{
			Topic: topic,
			Value: []byte(string(rune('A' + i))),
		})
	}
	if err := producer.ProduceSync(s.Ctx(), records...).FirstErr(); err != nil {
		s.Fail(err)
		return
	}
	s.Note("wrote 10 records, offsets 0 to 9")

	s.Note("read from the start")
	s.Table([]string{"offsets read", "values"}, [][]any{readAll(s, topic, kgo.NewOffset().AtStart())})

	s.Note("read the same topic again, from the start, and get the same data")
	s.Table([]string{"offsets read", "values"}, [][]any{readAll(s, topic, kgo.NewOffset().AtStart())})

	s.Note("start at offset 6 instead")
	s.Table([]string{"offsets read", "values"}, [][]any{readAll(s, topic, kgo.NewOffset().At(6))})

	s.Note("start at the end and see nothing until something new arrives")
	s.Table([]string{"offsets read", "values"}, [][]any{readAll(s, topic, kgo.NewOffset().AtEnd())})

	s.Note("where does the log begin and end")
	starts, err := s.Admin().ListStartOffsets(s.Ctx(), topic)
	if err != nil {
		s.Fail(err)
		return
	}
	ends, err := s.Admin().ListEndOffsets(s.Ctx(), topic)
	if err != nil {
		s.Fail(err)
		return
	}
	var rows [][]any
	for _, ps := range starts {
		for p, o := range ps {
			end, _ := ends.Lookup(topic, p)
			rows = append(rows, []any{p, o.Offset, end.Offset})
		}
	}
	s.Table([]string{"partition", "first offset", "next offset"}, rows)
}

func readAll(s *Session, topic string, at kgo.Offset) []any {
	if s.err != nil {
		return nil
	}
	cl := s.Client(kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(at))
	if cl == nil {
		return nil
	}
	defer cl.Close()

	var first, last int64 = -1, -1
	values := ""
	empties := 0
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && empties < 2 {
		ctx, cancel := context.WithTimeout(s.Ctx(), 1500*time.Millisecond)
		fetches := cl.PollFetches(ctx)
		cancel()
		got := 0
		fetches.EachRecord(func(r *kgo.Record) {
			if first < 0 {
				first = r.Offset
			}
			last = r.Offset
			values += string(r.Value)
			got++
		})
		if got == 0 {
			empties++
		} else {
			empties = 0
		}
	}
	if first < 0 {
		return []any{"nothing", ""}
	}
	return []any{formatRange(first, last), values}
}

func formatRange(first, last int64) string {
	return strconv.FormatInt(first, 10) + " to " + strconv.FormatInt(last, 10)
}
