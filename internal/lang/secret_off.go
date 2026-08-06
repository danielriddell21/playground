//go:build !(goexperiment.runtimesecret && linux && (amd64 || arm64))

package lang

func init() {
	set.Add("secret", "runtime/secret wipes the stack and registers a function used", secretDemo)
}

func secretDemo(s *Session) {
	s.Note("runtime/secret is experimental and only exists behind a build flag")
	s.Note("it needs linux on amd64 or arm64, and go 1.26 or newer")
	s.Note("")
	s.Note("  GOEXPERIMENT=runtimesecret go run . lang secret")
}
