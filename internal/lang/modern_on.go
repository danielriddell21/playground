//go:build go1.27

package lang

import (
	jsonv1 "encoding/json"
	json "encoding/json/v2"
	"fmt"
	"uuid"
)

func init() {
	set.Add("modern", "go 1.27: generic methods, the uuid package and json/v2", modern)
}

type Box[T any] struct{ value T }

func (b Box[T]) MapTo[U any](f func(T) U) Box[U] {
	return Box[U]{value: f(b.value)}
}

func (b Box[T]) Value() T { return b.value }

func modern(s *Session) {
	s.Note("a method can now declare its own type parameter")
	s.Note("  func (b Box[T]) MapTo[U any](f func(T) U) Box[U]")

	start := Box[int]{value: 21}
	doubled := start.MapTo(func(i int) string { return fmt.Sprintf("<%d>", i*2) })
	lengths := doubled.MapTo(func(str string) int { return len(str) })
	s.Table([]string{"start", "MapTo string", "MapTo length"}, [][]any{
		{start.Value(), doubled.Value(), lengths.Value()},
	})
	s.Note("before 1.27 that had to be a package level function, outside the type")

	s.Note("uuid is in the standard library now")
	id := uuid.New()
	parsed, err := uuid.Parse(id.String())
	if err != nil {
		s.Fail(err)
		return
	}
	s.Table([]string{"new", "round trips", "compares"}, [][]any{
		{id.String(), parsed == id, id.Compare(uuid.Max()) < 0},
	})

	s.Note("encoding/json/v2 no longer needs an experiment flag")
	type order struct {
		ID    uuid.UUID `json:"id"`
		Total float64   `json:"total"`
		Tags  []string  `json:"tags"`
	}
	value := order{ID: id, Total: 12.5}
	v2out, err := json.Marshal(value)
	if err != nil {
		s.Fail(err)
		return
	}
	v1out, err := jsonv1.Marshal(value)
	if err != nil {
		s.Fail(err)
		return
	}
	s.Table([]string{"encoder", "output"}, [][]any{
		{"encoding/json (v1)", string(v1out)},
		{"encoding/json/v2", string(v2out)},
	})
	s.Note("look at tags: it is a nil slice")
	s.Note("v1 marshals a nil slice as null, v2 marshals it as an empty array")
	s.Note("that is the kind of quiet difference to check before switching a real service over")
}
