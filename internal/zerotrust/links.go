package zerotrust

import (
	"crypto/rand"
	"encoding/json"
	"net/http"
	"sync"
)

// links is the small api being protected: a map of slug to target, with an
// owner per link. Every handler asks the authorizer first.
type links struct {
	az    *Authorizer
	mu    sync.Mutex
	items map[string]link
}

type link struct {
	Slug   string `json:"slug"`
	Target string `json:"target"`
	Owner  string `json:"owner,omitempty"`
}

func newLinks() *links { return &links{items: map[string]link{}} }

func (l *links) owner(slug string) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	it, ok := l.items[slug]
	return it.Owner, ok
}

func (l *links) mux() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("POST /links", func(w http.ResponseWriter, r *http.Request) {
		var in link
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Target == "" {
			http.Error(w, "want {target, slug?}", http.StatusBadRequest)
			return
		}
		vanity := in.Slug != "" // picking your own slug
		if !vanity {
			in.Slug = rand.Text()[:7]
		}
		if !l.az.Check(w, r, "Create", in.Slug, in.Target, vanity) {
			return
		}
		if p := Principal(r.Context()); p.Type == "User" {
			in.Owner = string(p.ID)
		}
		l.mu.Lock()
		l.items[in.Slug] = in
		l.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(in)
	})
	m.HandleFunc("GET /links/{slug}", func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		if !l.az.Check(w, r, "Resolve", slug, "", false) {
			return
		}
		l.mu.Lock()
		it, ok := l.items[slug]
		l.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(it)
	})
	m.HandleFunc("DELETE /links/{slug}", func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		if !l.az.Check(w, r, "Delete", slug, "", false) {
			return
		}
		l.mu.Lock()
		delete(l.items, slug)
		l.mu.Unlock()
	})
	m.HandleFunc("GET /links", func(w http.ResponseWriter, r *http.Request) {
		if !l.az.Check(w, r, "List", "", "", false) {
			return
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		json.NewEncoder(w).Encode(l.items)
	})
	return Identify(m)
}
