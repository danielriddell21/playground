package resilience

import (
	"fmt"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/sony/gobreaker/v2"
)

func init() {
	set.Add("ratelimit", "one greedy client and some polite ones against a per ip token bucket", ratelimit)
	set.Add("faults", "what latency and errors upstream do to an edge with no protection", faults)
	set.Add("breaker", "the same sick upstream behind a circuit breaker and stale cache", breaker)
}

func (s *Session) urls(base string) []string {
	var out []string
	for _, k := range slices.Sorted(maps.Keys(slugs)) {
		out = append(out, base+"/"+k)
	}
	return out
}

func (s *Session) lookup() Lookup {
	return httpLookup{base: s.upstream, c: &http.Client{}}
}

func ratelimit(s *Session) {
	base := s.serve(RateLimit(50, 10, edge(s.lookup())))
	urls := s.urls(base)
	s.Note("limit: 50 rps per ip, burst 10")
	rows := [][]any{{"greedy 10.0.0.1 @200rps", Attack(urls, 200, 2*time.Second, http.Header{"X-Forwarded-For": {"10.0.0.1"}}).String()}}
	for i := range 3 {
		ip := fmt.Sprintf("10.0.1.%d", i)
		rows = append(rows, []any{"polite " + ip + " @30rps", Attack(urls, 30, time.Second, http.Header{"X-Forwarded-For": {ip}}).String()})
	}
	s.Table([]string{"client", "result"}, rows)
}

// phases runs the same three phases against an edge: healthy, upstream down
// and slow, recovered.
func (s *Session) phases(name, base string) [][]any {
	urls := s.urls(base)
	rows := [][]any{{name, "healthy", Attack(urls, 100, time.Second, nil).String()}}
	s.faults.ErrorPct.Store(100)
	s.faults.Latency.Store(int64(300 * time.Millisecond))
	rows = append(rows, []any{name, "upstream down, slow", Attack(urls, 100, 2*time.Second, nil).String()})
	s.faults.ErrorPct.Store(0)
	s.faults.Latency.Store(0)
	time.Sleep(600 * time.Millisecond) // past the breaker's open timeout
	rows = append(rows, []any{name, "recovered", Attack(urls, 100, time.Second, nil).String()})
	return rows
}

func faults(s *Session) {
	s.Table([]string{"edge", "phase", "result"}, s.phases("plain", s.serve(edge(s.lookup()))))
	s.Note("every request waits out the slow upstream and still fails")
}

func breaker(s *Session) {
	var transitions []string
	g := NewGuarded(s.lookup(), func(from, to gobreaker.State) {
		transitions = append(transitions, fmt.Sprintf("%s->%s", from, to))
	})
	s.Table([]string{"edge", "phase", "result"}, s.phases("breaker+cache", s.serve(edge(g))))
	s.Note("breaker: %v", transitions)
	s.Note("%d answers served stale from cache", g.Stale.Load())
}
