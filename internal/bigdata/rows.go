package bigdata

import (
	"iter"
	"time"
)

// every generated value is a pure function of the row index, so a stream
// that only owns every eighth row can build its share without coordinating

var epoch = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) // a monday

func hash(i, salt int) uint64 {
	x := uint64(i)*0x9e3779b97f4a7c15 + uint64(salt)*0xbf58476d1ce4e5b9
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ x>>31
}

func pick(i, salt, n int) int { return int(hash(i, salt) % uint64(n)) }

func unit(i, salt int) float64 { return float64(hash(i, salt)>>11) / (1 << 53) }

func micros(t time.Time) int64 { return t.UnixMicro() }

func fromMicros(us int64) time.Time { return time.UnixMicro(us).UTC() }

// chunks yields [lo, hi) windows of size over n rows
func chunks(n, size int) iter.Seq2[int, int] {
	return func(yield func(int, int) bool) {
		for lo := 0; lo < n; lo += size {
			if !yield(lo, min(lo+size, n)) {
				return
			}
		}
	}
}

// split cuts n rows into k contiguous ranges
func split(n, k int) [][2]int {
	out := make([][2]int, 0, k)
	for p := range k {
		out = append(out, [2]int{n * p / k, n * (p + 1) / k})
	}
	return out
}

func convert[A, B int16 | int32 | int64](in []A) []B {
	out := make([]B, len(in))
	for i, v := range in {
		out[i] = B(v)
	}
	return out
}

func times(us []int64) []time.Time {
	out := make([]time.Time, len(us))
	for i, v := range us {
		out[i] = fromMicros(v)
	}
	return out
}
