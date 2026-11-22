package operator

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"playground/internal/play"
)

//go:embed crd.yaml
var CRD string

// Session is a fake api server and an in memory backend. Watches don't run
// against the fake client, so demos call Reconcile themselves, the way the
// manager would after each change.
type Session struct {
	ctx     context.Context
	client  client.Client
	backend *Memory
	r       *Reconciler
	err     error
}

func (s *Session) Err() error { return s.err }

func (s *Session) Close() {}

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

// reconcile runs one pass for name and returns the object after it.
func (s *Session) reconcile(name string) *ShortLink {
	if s.err != nil {
		return nil
	}
	key := types.NamespacedName{Namespace: "default", Name: name}
	if _, err := s.r.Reconcile(s.ctx, ctrl.Request{NamespacedName: key}); err != nil {
		s.Fail(fmt.Errorf("reconcile %s: %w", name, err))
		return nil
	}
	var sl ShortLink
	if err := s.client.Get(s.ctx, key, &sl); err != nil {
		return nil // gone
	}
	return &sl
}

// row is a ShortLink and what the backend really has for it.
func (s *Session) row(step string, sl *ShortLink) []any {
	live := "-"
	if t, owner, err := s.backend.Get(sl.Status.Slug); err == nil {
		live = t + " (" + owner + ")"
	}
	return []any{step, sl.Name, sl.Status.Slug, sl.Status.Ready, live, sl.Status.Message}
}

var headers = []string{"step", "object", "status.slug", "ready", "backend has", "message"}

var set = &play.Set[*Session]{
	Use:        "operator",
	Short:      "a ShortLink crd reconciled into a backend with controller-runtime, against a fake api server",
	DSNEnv:     "OPERATOR_BASE_URL",
	DefaultDSN: "https://go.example",
	Connect:    connect,
}

func connect(ctx context.Context, base string) (*Session, error) {
	c := fake.NewClientBuilder().WithScheme(NewScheme()).WithStatusSubresource(&ShortLink{}).Build()
	b := NewMemory()
	return &Session{ctx: ctx, client: c, backend: b, r: &Reconciler{Client: c, Backend: b, BaseURL: base}}, nil
}

func Command() *cobra.Command { return set.Command() }

func keyOf(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: "default", Name: name}
}
