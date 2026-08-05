package lang

import (
	"bytes"
	"runtime/trace"
	"sync"
	"time"
)

func init() {
	set.Add("trace", "a flight recorder keeps the last few seconds of trace in memory", flight)
}

func flight(s *Session) {
	fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{
		MinAge:   2 * time.Second,
		MaxBytes: 1 << 20,
	})
	if err := fr.Start(); err != nil {
		s.Fail(err)
		return
	}
	defer fr.Stop()

	s.Note("recorder running: %v, keeping at most 1MiB in a ring buffer", fr.Enabled())
	s.Note("nothing is written to disk unless something interesting happens")

	s.Note("doing some work worth tracing")
	var wg sync.WaitGroup
	start := time.Now()
	for range 8 {
		wg.Go(func() {
			total := 0
			for i := range 2_000_000 {
				total += i % 7
			}
			time.Sleep(20 * time.Millisecond)
		})
	}
	wg.Wait()
	s.Note("  8 goroutines finished in %s", time.Since(start).Round(time.Millisecond))

	s.Note("something looked wrong, so snapshot the window")
	var buf bytes.Buffer
	n, err := fr.WriteTo(&buf)
	if err != nil {
		s.Fail(err)
		return
	}

	s.Table([]string{"snapshot bytes", "starts with a trace header"}, [][]any{
		{n, bytes.HasPrefix(buf.Bytes(), []byte("go 1.")) || bytes.Contains(buf.Bytes()[:min(32, buf.Len())], []byte("trace"))},
	})
	s.Note("write that to a file and `go tool trace` opens it like any other trace")
	s.Note("the point is you pay for tracing continuously but only keep the interesting seconds")
}
