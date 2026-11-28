package restateplay

import (
	"fmt"
	"sync"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/ingress"
)

func init() {
	set.Add("outage", "the ledger goes down mid stream, nothing is lost or doubled", outage)
	set.Add("idempotency", "the same event sent three times runs once", idempotency)
	set.Add("object", "concurrent adds to one key, no locks, no lost updates", object)
	set.Add("timer", "a durable timer fires a report after the first event", timer)
}

// send records ev through ingress, one way, keyed by its id.
func (s *Session) send(ev Event) string {
	if s.err != nil {
		return ""
	}
	res, err := ingress.ServiceSend[Event](s.ingress, "Events", "Record").Send(s.ctx, ev, restate.WithIdempotencyKey(ev.ID))
	if err != nil {
		s.Fail(err)
		return ""
	}
	return res.Id()
}

func (s *Session) tally(key string) TallyState {
	if s.err != nil {
		return TallyState{}
	}
	t, err := ingress.Object[restate.Void, TallyState](s.ingress, "Tally", key, "Get").Request(s.ctx, restate.Void{})
	if err != nil {
		s.Fail(err)
	}
	return t
}

// waitFor polls until ok or the deadline.
func waitFor(d time.Duration, ok func() bool) bool {
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return true
}

func outage(s *Session) {
	key := "outage-" + s.run
	const n = 20
	s.ledger.open.Store(false)
	for i := range n {
		s.send(Event{ID: fmt.Sprintf("%s-%d", key, i), Key: key, At: time.Now().UnixMilli()})
	}
	time.Sleep(2 * time.Second)
	counted, _ := s.ledger.count(key)
	s.Note("sent %d with the ledger down: ledger has %d, turned away %d attempts", n, counted, s.ledger.rejected.Load())

	s.ledger.open.Store(true)
	waitFor(15*time.Second, func() bool { c, _ := s.ledger.count(key); return c == n })
	counted, writes := s.ledger.count(key)
	s.Note("ledger back: %d counted from %d writes (want %d and %d)", counted, writes, n, n)
	waitFor(5*time.Second, func() bool { return s.tally(key).Count == n })
	s.Note("Tally/%s: %d", key, s.tally(key).Count)
	if counted != n {
		s.Fail(fmt.Errorf("ledger has %d, want %d", counted, n))
	}
}

func idempotency(s *Session) {
	key := "idem-" + s.run
	ev := Event{ID: key + "-1", Key: key, At: time.Now().UnixMilli()}
	var rows [][]any
	for i := range 3 {
		rows = append(rows, []any{i + 1, s.send(ev)})
	}
	s.Table([]string{"send", "invocation id"}, rows)
	waitFor(5*time.Second, func() bool { c, _ := s.ledger.count(key); return c > 0 })
	time.Sleep(500 * time.Millisecond)
	counted, writes := s.ledger.count(key)
	s.Note("same invocation id every time; ledger saw %d write(s), counted %d", writes, counted)
	if writes != 1 {
		s.Fail(fmt.Errorf("ledger saw %d writes, want 1", writes))
	}
}

func object(s *Session) {
	key := "object-" + s.run
	const n = 100
	add := ingress.ObjectSend[Event](s.ingress, "Tally", key, "Add")
	var wg sync.WaitGroup
	var mu sync.Mutex
	t0 := time.Now()
	for i := range n {
		wg.Go(func() {
			if _, err := add.Send(s.ctx, Event{ID: fmt.Sprint(i), Key: key}); err != nil {
				mu.Lock()
				s.Fail(err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	ok := waitFor(10*time.Second, func() bool { return s.tally(key).Count == n })
	s.Note("%d concurrent Adds to Tally/%s: count %d after %s (complete: %v)", n, key, s.tally(key).Count, time.Since(t0).Round(time.Millisecond), ok)
	if !ok {
		s.Fail(fmt.Errorf("lost updates"))
	}
}

func timer(s *Session) {
	key := "timer-" + s.run
	s.send(Event{ID: key + "-1", Key: key, At: time.Now().UnixMilli()})
	waitFor(5*time.Second, func() bool { return s.tally(key).Count == 1 })
	s.Note("first event in, reports so far: %d", s.tally(key).Reports)
	s.Note("waiting %s for the timer (kill restate-server or this process meanwhile, it still fires)", reportAfter)
	ok := waitFor(reportAfter+5*time.Second, func() bool { return s.tally(key).Reports == 1 })
	s.Note("reports: %d", s.tally(key).Reports)
	if !ok {
		s.Fail(fmt.Errorf("timer did not fire"))
	}
}
