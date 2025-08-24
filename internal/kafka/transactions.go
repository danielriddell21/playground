package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func init() {
	set.Add("transactions", "aborted writes stay invisible to read_committed readers", transactions)
}

func transactions(s *Session) {
	const topic = "play.tx"
	s.ResetTopic(topic, 1, nil)

	tx := s.Client(kgo.TransactionalID("play.tx.producer"), kgo.DefaultProduceTopic(topic))
	if tx == nil {
		return
	}
	defer tx.Close()

	s.Note("a committed transaction")
	if err := tx.BeginTransaction(); err != nil {
		s.Fail(err)
		return
	}
	if err := tx.ProduceSync(s.Ctx(),
		&kgo.Record{Value: []byte("committed-1")},
		&kgo.Record{Value: []byte("committed-2")},
	).FirstErr(); err != nil {
		s.Fail(err)
		return
	}
	if err := tx.EndTransaction(s.Ctx(), kgo.TryCommit); err != nil {
		s.Fail(err)
		return
	}

	s.Note("an aborted transaction, written to the log but rolled back")
	if err := tx.BeginTransaction(); err != nil {
		s.Fail(err)
		return
	}
	if err := tx.ProduceSync(s.Ctx(),
		&kgo.Record{Value: []byte("aborted-1")},
		&kgo.Record{Value: []byte("aborted-2")},
	).FirstErr(); err != nil {
		s.Fail(err)
		return
	}
	if err := tx.EndTransaction(s.Ctx(), kgo.TryAbort); err != nil {
		s.Fail(err)
		return
	}

	s.Note("one more committed transaction")
	if err := tx.BeginTransaction(); err != nil {
		s.Fail(err)
		return
	}
	if err := tx.ProduceSync(s.Ctx(), &kgo.Record{Value: []byte("committed-3")}).FirstErr(); err != nil {
		s.Fail(err)
		return
	}
	if err := tx.EndTransaction(s.Ctx(), kgo.TryCommit); err != nil {
		s.Fail(err)
		return
	}

	s.Note("read_uncommitted sees everything that was ever written")
	s.Table([]string{"offset", "value"}, readTx(s, topic, kgo.ReadUncommitted()))

	s.Note("read_committed skips the aborted batch, and offsets have gaps")
	s.Table([]string{"offset", "value"}, readTx(s, topic, kgo.ReadCommitted()))
}

func readTx(s *Session, topic string, level kgo.IsolationLevel) [][]any {
	if s.err != nil {
		return nil
	}
	cl := s.Client(
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(level),
	)
	if cl == nil {
		return nil
	}
	defer cl.Close()

	var rows [][]any
	empties := 0
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && empties < 2 {
		ctx, cancel := context.WithTimeout(s.Ctx(), 1500*time.Millisecond)
		fetches := cl.PollFetches(ctx)
		cancel()
		got := 0
		fetches.EachRecord(func(r *kgo.Record) {
			rows = append(rows, []any{r.Offset, string(r.Value)})
			got++
		})
		if got == 0 {
			empties++
		} else {
			empties = 0
		}
	}
	return rows
}
