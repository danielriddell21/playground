package operator

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	set.Add("crd", "the custom resource definition, by hand instead of controller-gen", crd)
	set.Add("lifecycle", "create, change the target, pick a slug, delete through the finalizer", lifecycle)
	set.Add("drift", "the backend loses everything and the next resync puts it back", drift)
	set.Add("conflict", "two objects want the same slug, the second is not ready and not retried", conflict)
}

func crd(s *Session) {
	fmt.Print(CRD)
}

func (s *Session) create(name, slug, target string) {
	if s.err != nil {
		return
	}
	sl := &ShortLink{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec:       ShortLinkSpec{Target: target, Slug: slug},
	}
	if err := s.client.Create(s.ctx, sl); err != nil {
		s.Fail(err)
	}
}

// change edits the spec, bumping generation like the api server would.
func (s *Session) change(sl *ShortLink, edit func(*ShortLinkSpec)) {
	if s.err != nil || sl == nil {
		return
	}
	edit(&sl.Spec)
	sl.Generation++
	if err := s.client.Update(s.ctx, sl); err != nil {
		s.Fail(err)
	}
}

func lifecycle(s *Session) {
	var rows [][]any
	s.create("docs", "", "https://go.dev/doc")
	sl := s.reconcile("docs")
	if sl == nil {
		return
	}
	generated := sl.Status.Slug
	rows = append(rows, s.row("create", sl))

	s.change(sl, func(sp *ShortLinkSpec) { sp.Target = "https://go.dev/ref/spec" })
	if sl = s.reconcile("docs"); sl == nil {
		return
	}
	rows = append(rows, s.row("edit target", sl))
	if sl.Status.Slug != generated {
		s.Fail(fmt.Errorf("generated slug moved %s -> %s", generated, sl.Status.Slug))
		return
	}

	s.change(sl, func(sp *ShortLinkSpec) { sp.Slug = "go-spec" })
	if sl = s.reconcile("docs"); sl == nil {
		return
	}
	rows = append(rows, s.row("pick slug", sl))
	_, _, err := s.backend.Get(generated)
	rows = append(rows, []any{"", "", generated, "", map[bool]string{true: "gone", false: "still there"}[err != nil], "old slug"})

	s.Note("finalizers on the object: %v", sl.Finalizers)
	if err := s.client.Delete(s.ctx, sl); err != nil {
		s.Fail(err)
		return
	}
	gone := s.reconcile("docs") == nil
	_, _, err = s.backend.Get("go-spec")
	rows = append(rows, []any{"delete", "docs", "go-spec", "", map[bool]string{true: "gone", false: "still there"}[err != nil], fmt.Sprintf("object released: %v", gone)})
	s.Table(headers, rows)
}

func drift(s *Session) {
	names := map[string]string{"blog": "https://go.dev/blog", "play": "https://go.dev/play", "tour": "https://go.dev/tour"}
	var rows [][]any
	for _, n := range []string{"blog", "play", "tour"} {
		s.create(n, n, names[n])
		if sl := s.reconcile(n); sl != nil {
			rows = append(rows, s.row("create", sl))
		}
	}
	s.backend.Reset()
	for _, n := range []string{"blog", "play", "tour"} {
		var sl ShortLink
		s.client.Get(s.ctx, keyOf(n), &sl)
		rows = append(rows, s.row("backend wiped", &sl))
	}
	s.Note("requeued every %s; a resync later:", Resync)
	for _, n := range []string{"blog", "play", "tour"} {
		if sl := s.reconcile(n); sl != nil {
			rows = append(rows, s.row("resync", sl))
		}
	}
	s.Table(headers, rows)
}

func conflict(s *Session) {
	s.create("first", "gopher", "https://go.dev")
	s.create("second", "gopher", "https://example.com")
	var rows [][]any
	for _, n := range []string{"first", "second"} {
		if sl := s.reconcile(n); sl != nil {
			rows = append(rows, s.row("create", sl))
		}
	}
	s.Table(headers, rows)
	s.Note("second is not requeued, only a spec change can fix it")
}
