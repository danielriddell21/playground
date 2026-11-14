package resilience

import (
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

// Result is one vegeta run boiled down.
type Result struct {
	Codes map[int]int
	P50   time.Duration
	P99   time.Duration
	N     uint64
}

func (r Result) String() string {
	var parts []string
	for _, c := range slices.Sorted(maps.Keys(r.Codes)) {
		parts = append(parts, fmt.Sprintf("%d:%d", c, r.Codes[c]))
	}
	return fmt.Sprintf("n=%d p50=%s p99=%s %s", r.N, r.P50.Round(10*time.Microsecond), r.P99.Round(10*time.Microsecond), strings.Join(parts, " "))
}

// Attack fires GETs at urls round robin at rps for d without following
// redirects. headers lets a run pretend to be one client or many.
func Attack(urls []string, rps int, d time.Duration, header http.Header) Result {
	var targets []vegeta.Target
	for _, u := range urls {
		targets = append(targets, vegeta.Target{Method: http.MethodGet, URL: u, Header: header})
	}
	atk := vegeta.NewAttacker(vegeta.Redirects(vegeta.NoFollow), vegeta.Timeout(2*time.Second))
	var m vegeta.Metrics
	codes := map[int]int{}
	for res := range atk.Attack(vegeta.NewStaticTargeter(targets...), vegeta.Rate{Freq: rps, Per: time.Second}, d, "shorty") {
		m.Add(res)
		codes[int(res.Code)]++
	}
	m.Close()
	return Result{Codes: codes, P50: m.Latencies.P50, P99: m.Latencies.P99, N: m.Requests}
}
