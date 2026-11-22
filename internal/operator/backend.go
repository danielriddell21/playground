package operator

import (
	"crypto/rand"
	"errors"
	"regexp"
	"sync"
)

// Backend is the system outside kubernetes the operator keeps in step with
// ShortLink objects. Here it is in memory; it would be an api client.
// Every link records its owner, so two objects can't fight over one slug.
type Backend interface {
	Get(slug string) (target, owner string, err error)
	Create(slug, target, owner string) (string, error) // empty slug picks one
	Delete(slug string) error
}

var (
	ErrNotFound = errors.New("not found")
	ErrTaken    = errors.New("slug taken")
	ErrInvalid  = errors.New("slug must be 3-32 of [a-zA-Z0-9_-]")
)

var slugRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{3,32}$`)

type entry struct{ target, owner string }

type Memory struct {
	mu    sync.Mutex
	links map[string]entry
}

func NewMemory() *Memory { return &Memory{links: map[string]entry{}} }

func (m *Memory) Get(slug string) (string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.links[slug]
	if !ok {
		return "", "", ErrNotFound
	}
	return e.target, e.owner, nil
}

func (m *Memory) Create(slug, target, owner string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slug == "" {
		slug = rand.Text()[:7]
	} else if !slugRe.MatchString(slug) {
		return "", ErrInvalid
	}
	if _, ok := m.links[slug]; ok {
		return "", ErrTaken
	}
	m.links[slug] = entry{target, owner}
	return slug, nil
}

func (m *Memory) Delete(slug string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.links[slug]; !ok {
		return ErrNotFound
	}
	delete(m.links, slug)
	return nil
}

// Reset forgets everything, like the backend losing its data.
func (m *Memory) Reset() {
	m.mu.Lock()
	m.links = map[string]entry{}
	m.mu.Unlock()
}
