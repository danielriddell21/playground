package xdsplay

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"playground/internal/play"
)

// NodeID is the node envoy says it is in its bootstrap; snapshots are keyed by it.
const NodeID = "edge"

// ControlPlane pushes versioned snapshots that envoy (or anything speaking
// ads) picks up over one grpc stream.
type ControlPlane struct {
	Cache   cache.SnapshotCache
	mu      sync.Mutex
	version int
}

func NewControlPlane() *ControlPlane {
	return &ControlPlane{Cache: cache.NewSnapshotCache(true, cache.IDHash{}, nil)}
}

func (cp *ControlPlane) Version() int {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.version
}

func (cp *ControlPlane) Push(ctx context.Context, m Mesh) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.version++
	snap, err := m.Snapshot(cp.version)
	if err != nil {
		return err
	}
	if err := snap.Consistent(); err != nil {
		return fmt.Errorf("snapshot v%d: %w", cp.version, err)
	}
	slog.Debug("push", "version", cp.version, "mesh", describe(m))
	return cp.Cache.SetSnapshot(ctx, NodeID, snap)
}

func (cp *ControlPlane) Serve(ctx context.Context, ln net.Listener) error {
	g := grpc.NewServer()
	discovery.RegisterAggregatedDiscoveryServiceServer(g, server.NewServer(ctx, cp.Cache, nil))
	go func() {
		<-ctx.Done()
		g.Stop()
	}()
	return g.Serve(ln)
}

var Steps = []uint32{0, 10, 25, 50, 100}

// Rollout walks CanaryPct through Steps. check runs before each step and can
// halt it, which is where real code would look at the canary's error rate.
func (cp *ControlPlane) Rollout(ctx context.Context, m Mesh, every time.Duration, check func(pct uint32) error) error {
	for _, pct := range Steps {
		if check != nil {
			if err := check(pct); err != nil {
				m.CanaryPct = 0
				cp.Push(ctx, m)
				return fmt.Errorf("rolled back at %d%%: %w", pct, err)
			}
		}
		m.CanaryPct = pct
		if err := cp.Push(ctx, m); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(every):
		}
	}
	return nil
}

type Session struct {
	ctx    context.Context
	cancel context.CancelFunc
	cp     *ControlPlane
	addr   string
	err    error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() { s.cancel() }

func (s *Session) Ctx() context.Context { return s.ctx }

func (s *Session) Fail(err error) {
	if s.err == nil {
		s.err = err
	}
}

func (s *Session) Note(format string, args ...any) {
	if s.err != nil {
		return
	}
	fmt.Printf(format+"\n", args...)
}

func (s *Session) Table(headers []string, rows [][]any) {
	if s.err != nil {
		return
	}
	play.Table(headers, rows)
}

var set = &play.Set[*Session]{
	Use:        "xds",
	Short:      "envoy control plane with go-control-plane, a fake envoy on ads and canary rollouts",
	DSNEnv:     "XDS_ADDR",
	DefaultDSN: "127.0.0.1:0",
	Connect:    connect,
}

// connect starts the control plane on the dsn address. Point a real envoy
// at it with an ads bootstrap and node id "edge" to watch it reconfigure.
func connect(ctx context.Context, addr string) (*Session, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	cp := NewControlPlane()
	go cp.Serve(ctx, ln)
	return &Session{ctx: ctx, cancel: cancel, cp: cp, addr: ln.Addr().String()}, nil
}

func Command() *cobra.Command { return set.Command() }
