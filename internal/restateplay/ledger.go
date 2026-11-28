package restateplay

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
)

// ledger is the flaky downstream: it counts events per key, ignores repeats
// of an id, and answers 503 while its gate is closed.
type ledger struct {
	open     atomic.Bool
	rejected atomic.Int64
	mu       sync.Mutex
	seen     map[string]bool
	counts   map[string]int64
	writes   map[string]int64 // accepted posts per key, repeats included
}

func newLedger() *ledger {
	l := &ledger{seen: map[string]bool{}, counts: map[string]int64{}, writes: map[string]int64{}}
	l.open.Store(true)
	return l
}

func (l *ledger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !l.open.Load() {
		l.rejected.Add(1)
		http.Error(w, "ledger down", http.StatusServiceUnavailable)
		return
	}
	var ev Event
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.writes[ev.Key]++
	if !l.seen[ev.ID] {
		l.seen[ev.ID] = true
		l.counts[ev.Key]++
	}
}

func (l *ledger) count(key string) (counted, writes int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.counts[key], l.writes[key]
}
