//go:build wasip1

// blocklist refuses targets on hosts listed in $BLOCK and bots in $BLOCK_UA.
// The host passes config through env; the plugin can't see anything else.
package main

import (
	"encoding/json"
	"net/url"
	"os"
	"strings"
)

type req struct {
	Target    string `json:"target"`
	UserAgent string `json:"userAgent"`
}

type resp struct {
	Status int    `json:"status,omitempty"`
	Reason string `json:"reason,omitempty"`
}

func main() {
	var r req
	if err := json.NewDecoder(os.Stdin).Decode(&r); err != nil {
		os.Exit(1)
	}
	out := resp{}
	u, _ := url.Parse(r.Target)
	for _, h := range strings.Split(os.Getenv("BLOCK"), ",") {
		if h != "" && u != nil && (u.Hostname() == h || strings.HasSuffix(u.Hostname(), "."+h)) {
			out = resp{Status: 451, Reason: "blocked host " + h}
		}
	}
	for _, b := range strings.Split(os.Getenv("BLOCK_UA"), ",") {
		if b != "" && strings.Contains(strings.ToLower(r.UserAgent), b) {
			out = resp{Status: 403, Reason: "blocked agent " + b}
		}
	}
	json.NewEncoder(os.Stdout).Encode(out)
}
