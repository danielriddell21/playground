//go:build goexperiment.runtimesecret && linux && (amd64 || arm64)

package lang

import (
	"crypto/sha256"
	"encoding/hex"
	"runtime/secret"
)

func init() {
	set.Add("secret", "runtime/secret wipes the stack and registers a function used", secretDemo)
}

func secretDemo(s *Session) {
	s.Note("Enabled() is not 'is the feature compiled in'")
	s.Note("it is a nesting counter, so it answers 'am I inside Do right now'")

	var rows [][]any
	rows = append(rows, []any{"before Do", secret.Enabled()})
	secret.Do(func() {
		rows = append(rows, []any{"inside Do", secret.Enabled()})
		secret.Do(func() {
			rows = append(rows, []any{"nested Do", secret.Enabled()})
		})
	})
	rows = append(rows, []any{"after Do", secret.Enabled()})
	s.Table([]string{"where", "secret.Enabled()"}, rows)

	s.Note("derive a key inside Do, so the intermediate material does not outlive it")
	var digest [32]byte
	secret.Do(func() {
		shared := []byte("client-nonce|server-nonce|long-term-key")
		digest = sha256.Sum256(shared)
		for i := range shared {
			shared[i] = 0
		}
	})
	s.Table([]string{"derived key (first 8 bytes)"}, [][]any{{hex.EncodeToString(digest[:8])}})

	s.Note("on return Do zeroes the stack and registers the function touched")
	s.Note("its heap allocations are wiped once the collector agrees they are unreachable")
	s.Note("this is for people writing TLS and WireGuard, not for application code")
}
