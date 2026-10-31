//go:build wasip1

// utm tags the target with where the click came from.
package main

import (
	"encoding/json"
	"net/url"
	"os"
)

type req struct {
	Target   string `json:"target"`
	Slug     string `json:"slug"`
	Referrer string `json:"referrer"`
}

type resp struct {
	Target string `json:"target,omitempty"`
}

func main() {
	var r req
	if err := json.NewDecoder(os.Stdin).Decode(&r); err != nil {
		os.Exit(1)
	}
	u, err := url.Parse(r.Target)
	if err != nil {
		os.Exit(1)
	}
	q := u.Query()
	q.Set("utm_source", "playground")
	q.Set("utm_campaign", r.Slug)
	if ref, err := url.Parse(r.Referrer); err == nil && ref.Host != "" {
		q.Set("utm_medium", ref.Host)
	}
	u.RawQuery = q.Encode()
	json.NewEncoder(os.Stdout).Encode(resp{Target: u.String()})
}
