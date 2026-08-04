package lang

import (
	"iter"
	"maps"
	"slices"
	"strings"
)

func init() {
	set.Add("iterators", "range over functions, and iter.Pull to drive one by hand", iterators)
}

func countdown(from int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := from; i > 0; i-- {
			if !yield(i) {
				return
			}
		}
	}
}

func spelled(words ...string) iter.Seq2[int, string] {
	return func(yield func(int, string) bool) {
		for i, w := range words {
			if !yield(i+1, strings.ToUpper(w)) {
				return
			}
		}
	}
}

func iterators(s *Session) {
	s.Note("a function is a sequence you can range over")
	var got []int
	for n := range countdown(5) {
		got = append(got, n)
	}
	s.Table([]string{"countdown(5)"}, [][]any{{got}})

	s.Note("break stops the producer, it does not just stop consuming")
	var early []int
	for n := range countdown(100) {
		early = append(early, n)
		if len(early) == 3 {
			break
		}
	}
	s.Table([]string{"first three of countdown(100)"}, [][]any{{early}})

	s.Note("two value sequences")
	var rows [][]any
	for i, w := range spelled("ada", "grace", "linus") {
		rows = append(rows, []any{i, w})
	}
	s.Table([]string{"n", "word"}, rows)

	s.Note("collecting straight into slices")
	s.Table([]string{"slices.Collect", "slices.Sorted"}, [][]any{{
		slices.Collect(countdown(4)),
		slices.Sorted(countdown(4)),
	}})

	scores := map[string]int{"ada": 91, "grace": 99, "linus": 77}
	s.Table([]string{"slices.Sorted(maps.Keys)"}, [][]any{{slices.Sorted(maps.Keys(scores))}})

	s.Note("iter.Pull turns a push sequence into a pull one")
	s.Note("which is the only way to walk two sequences in lockstep")
	left, stopL := iter.Pull(countdown(3))
	right, stopR := iter.Pull(slices.Values([]string{"a", "b", "c", "d"}))
	defer stopL()
	defer stopR()

	var zipped [][]any
	for {
		l, okL := left()
		r, okR := right()
		if !okL || !okR {
			break
		}
		zipped = append(zipped, []any{l, r})
	}
	s.Table([]string{"left", "right"}, zipped)
	s.Note("the shorter sequence ends the walk, and stop releases the other one")
}
