package wasmplay

import (
	"time"
)

func init() {
	set.Add("rules", "plugins tag links with utm and block bad hosts and bots", rules)
	set.Add("timeout", "a plugin that never returns gets killed at its deadline", timeout)
	set.Add("bench", "what a fresh sandbox per call costs", bench)
}

func rules(s *Session) {
	e := s.load("blocklist", "utm")
	if e == nil {
		return
	}
	defer e.Close(s.ctx)
	cases := []Request{
		{Slug: "golang", Target: "https://go.dev/doc", UserAgent: "curl/8"},
		{Slug: "golang", Target: "https://go.dev/doc", UserAgent: "firefox", Referrer: "https://news.ycombinator.com/item?id=1"},
		{Slug: "golang", Target: "https://go.dev/doc", UserAgent: "BadBot/1.0"},
		{Slug: "sketchy", Target: "https://download.malware.test/x", UserAgent: "firefox"},
	}
	var rows [][]any
	for _, req := range cases {
		t0 := time.Now()
		res, by, err := e.Apply(s.ctx, req)
		if err != nil {
			s.Fail(err)
			return
		}
		out := res.Target
		if res.Status != 302 {
			out = res.Reason + " (" + by + ")"
		}
		rows = append(rows, []any{req.UserAgent, req.Target, res.Status, out, time.Since(t0).Round(time.Millisecond).String()})
	}
	s.Table([]string{"agent", "target", "status", "result", "took"}, rows)
}

func timeout(s *Session) {
	e := s.load("spin")
	if e == nil {
		return
	}
	defer e.Close(s.ctx)
	t0 := time.Now()
	_, _, err := e.Apply(s.ctx, Request{Slug: "golang", Target: "https://go.dev"})
	s.Note("after %s: %v", time.Since(t0).Round(time.Millisecond), err)
	s.Note("the host is fine, the caller falls back to the plain target")
}

func bench(s *Session) {
	e := s.load("utm")
	if e == nil {
		return
	}
	defer e.Close(s.ctx)
	const n = 200
	req := Request{Slug: "golang", Target: "https://go.dev", Referrer: "https://example.com"}
	t0 := time.Now()
	var last Response
	for range n {
		res, err := e.Call(s.ctx, e.Plugins[0], req)
		if err != nil {
			s.Fail(err)
			return
		}
		last = res
	}
	s.Note("%d calls, %s each, last -> %s", n, time.Since(t0)/n, last.Target)
	s.Note("go's wasip1 runtime boots on every call; tinygo or rust plugins start far faster")
}
