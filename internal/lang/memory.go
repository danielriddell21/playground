package lang

import (
	"runtime"
	"strings"
	"time"
	"unique"
	"weak"
)

func init() {
	set.Add("memory", "unique interning, weak pointers and runtime cleanups", memory)
}

type payload struct{ data []byte }

func memory(s *Session) {
	s.Note("unique.Make collapses equal values onto one copy")
	first := strings.Repeat("long-key-", 8) + "id"
	second := strings.Repeat("long-key-", 8) + "id"
	a := unique.Make(first)
	b := unique.Make(second)
	s.Table([]string{"strings equal", "handles equal", "value"}, [][]any{
		{first == second, a == b, a.Value()[:14] + "..."},
	})
	s.Note("comparing handles is a pointer compare, no matter how long the string is")

	s.Note("weak pointers do not keep anything alive")
	obj := &payload{data: make([]byte, 4<<20)}
	wp := weak.Make(obj)

	swept := make(chan string, 1)
	runtime.AddCleanup(obj, func(msg string) { swept <- msg }, "cleanup ran after collection")

	s.Note("  while a strong reference exists, weak value is non nil: %v", wp.Value() != nil)

	obj = nil
	runtime.GC()
	runtime.GC()

	select {
	case msg := <-swept:
		s.Note("  %s", msg)
	case <-time.After(5 * time.Second):
		s.Note("  cleanup did not run within 5s")
	}
	s.Note("  after collection, weak value is nil:      %v", wp.Value() == nil)

	s.Note("that is a cache which cannot leak, entries vanish when nobody else holds them")
	s.Note("AddCleanup replaces SetFinalizer, it cannot resurrect the object it is watching")
}
