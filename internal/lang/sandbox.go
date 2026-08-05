package lang

import (
	"io"
	"os"
	"path/filepath"
)

func init() {
	set.Add("sandbox", "os.Root refuses to leave the directory you gave it", sandbox)
}

func sandbox(s *Session) {
	base, err := os.MkdirTemp("", "playground-root")
	if err != nil {
		s.Fail(err)
		return
	}
	defer os.RemoveAll(base)

	inside := filepath.Join(base, "inside")
	if err := os.Mkdir(inside, 0o755); err != nil {
		s.Fail(err)
		return
	}
	if err := os.WriteFile(filepath.Join(inside, "ok.txt"), []byte("safe content"), 0o644); err != nil {
		s.Fail(err)
		return
	}
	if err := os.WriteFile(filepath.Join(base, "secret.txt"), []byte("should stay hidden"), 0o644); err != nil {
		s.Fail(err)
		return
	}
	if err := os.Symlink(filepath.Join(base, "secret.txt"), filepath.Join(inside, "shortcut.txt")); err != nil {
		s.Fail(err)
		return
	}

	root, err := os.OpenRoot(inside)
	if err != nil {
		s.Fail(err)
		return
	}
	defer root.Close()

	s.Note("the root is %s", inside)
	s.Note("secret.txt sits one level above it, plus a symlink pointing at it")

	var rows [][]any
	for _, name := range []string{"ok.txt", "../secret.txt", "shortcut.txt", "/etc/hostname"} {
		rows = append(rows, []any{name, attempt(root, name)})
	}
	s.Table([]string{"root.Open(...)", "result"}, rows)

	s.Note("plain os.Open has no such opinion")
	data, err := os.ReadFile(filepath.Join(inside, "../secret.txt"))
	if err != nil {
		s.Fail(err)
		return
	}
	s.Table([]string{"os.ReadFile(../secret.txt)"}, [][]any{{string(data)}})

	s.Note("the check happens per path element, so it also survives a symlink swapped in mid walk")
}

func attempt(root *os.Root, name string) string {
	f, err := root.Open(name)
	if err != nil {
		return "refused: " + trimErr(err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 40))
	if err != nil {
		return "refused: " + trimErr(err)
	}
	return "read " + string(b)
}

func trimErr(err error) string {
	msg := err.Error()
	if i := len(msg); i > 70 {
		return msg[:70] + "..."
	}
	return msg
}
