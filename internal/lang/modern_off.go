//go:build !go1.27

package lang

func init() {
	set.Add("modern", "go 1.27: generic methods, the uuid package and json/v2", modern)
}

func modern(s *Session) {
	s.Note("generic methods, the std uuid package and un-flagged json/v2 need go 1.27")
	s.Note("")
	s.Note("  GOTOOLCHAIN=go1.27rc1 go run . lang modern")
}
