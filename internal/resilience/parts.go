package resilience

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sony/gobreaker/v2"
	"golang.org/x/time/rate"
)

// Faults is http middleware adding latency and errors, toggled at runtime.
type Faults struct {
	ErrorPct atomic.Int32
	Latency  atomic.Int64 // nanoseconds
}

func (f *Faults) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if d := time.Duration(f.Latency.Load()); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			}
		}
		if p := f.ErrorPct.Load(); p > 0 && rand.Int32N(100) < p {
			http.Error(w, "injected fault", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// upstream is the dependency: slug -> target lookups.
func upstream(targets map[string]string) http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /links/{slug}", func(w http.ResponseWriter, r *http.Request) {
		t, ok := targets[r.PathValue("slug")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"target": t})
	})
	return m
}

var errNotFound = errors.New("not found")

// Lookup asks the upstream for a slug's target.
type Lookup interface {
	Target(ctx context.Context, slug string) (string, error)
}

type httpLookup struct {
	base string
	c    *http.Client
}

func (h httpLookup) Target(ctx context.Context, slug string) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, h.base+"/links/"+slug, nil)
	res, err := h.c.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	switch {
	case res.StatusCode == http.StatusNotFound:
		return "", errNotFound
	case res.StatusCode != http.StatusOK:
		return "", fmt.Errorf("upstream %s", res.Status)
	}
	var out struct{ Target string }
	return out.Target, json.NewDecoder(res.Body).Decode(&out)
}

// Guarded wraps a Lookup with a timeout, a circuit breaker and a stale cache:
// while the upstream is sick, known slugs keep answering from cache and the
// breaker stops piling requests onto it.
type Guarded struct {
	Inner   Lookup
	Breaker *gobreaker.CircuitBreaker[string]
	Timeout time.Duration

	mu    sync.RWMutex
	cache map[string]string

	Stale atomic.Int64
}

func NewGuarded(inner Lookup, onState func(from, to gobreaker.State)) *Guarded {
	g := &Guarded{Inner: inner, Timeout: 200 * time.Millisecond, cache: map[string]string{}}
	g.Breaker = gobreaker.NewCircuitBreaker[string](gobreaker.Settings{
		Name:        "upstream",
		MaxRequests: 3,
		Interval:    time.Second,
		Timeout:     500 * time.Millisecond, // open -> half open
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.Requests >= 10 && float64(c.TotalFailures)/float64(c.Requests) > 0.5
		},
		IsSuccessful: func(err error) bool {
			return err == nil || errors.Is(err, errNotFound) // a missing slug is the upstream working fine
		},
		OnStateChange: func(_ string, from, to gobreaker.State) {
			if onState != nil {
				onState(from, to)
			}
		},
	})
	return g
}

func (g *Guarded) Target(ctx context.Context, slug string) (string, error) {
	t, err := g.Breaker.Execute(func() (string, error) {
		ctx, cancel := context.WithTimeout(ctx, g.Timeout)
		defer cancel()
		return g.Inner.Target(ctx, slug)
	})
	switch {
	case err == nil:
		g.mu.Lock()
		g.cache[slug] = t
		g.mu.Unlock()
		return t, nil
	case errors.Is(err, errNotFound):
		g.mu.Lock()
		delete(g.cache, slug)
		g.mu.Unlock()
		return "", err
	}
	g.mu.RLock()
	t, ok := g.cache[slug]
	g.mu.RUnlock()
	if ok {
		g.Stale.Add(1)
		return t, nil
	}
	return "", err
}

// edge is the service under test: it redirects /{slug} to the upstream's target.
func edge(l Lookup) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t, err := l.Target(r.Context(), r.URL.Path[1:])
		switch {
		case errors.Is(err, errNotFound):
			http.NotFound(w, r)
		case err != nil:
			http.Error(w, err.Error(), http.StatusBadGateway)
		default:
			http.Redirect(w, r, t, http.StatusFound)
		}
	})
}

// RateLimit is per client ip token bucket middleware answering 429.
func RateLimit(perSec float64, burst int, next http.Handler) http.Handler {
	var mu sync.Mutex
	buckets := map[string]*rate.Limiter{}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := r.Header.Get("X-Forwarded-For")
		if ip == "" {
			ip, _, _ = net.SplitHostPort(r.RemoteAddr)
		}
		mu.Lock()
		l := buckets[ip]
		if l == nil {
			l = rate.NewLimiter(rate.Limit(perSec), burst)
			buckets[ip] = l
		}
		mu.Unlock()
		if !l.Allow() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
