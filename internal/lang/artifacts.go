package lang

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func init() {
	set.Add("artifacts", "go 1.26 gives every test a directory for its output files", artifacts)
}

func artifacts(s *Session) {
	out, err := os.MkdirTemp("", "playground-artifacts")
	if err != nil {
		s.Fail(err)
		return
	}
	defer os.RemoveAll(out)

	s.Note("tests used to scatter output into TempDir, which is deleted, or the repo, which is worse")
	s.Note("t.ArtifactDir() gives each test its own directory instead")
	s.Note("")
	s.Note("running: go test ./internal/lang -run TestWritesAnArtifact -artifacts -outputdir %s", out)

	cmd := exec.CommandContext(s.Ctx(), "go", "test", "./internal/lang",
		"-run", "TestWritesAnArtifact", "-artifacts", "-outputdir", out, "-v")
	cmd.Dir = repoRoot()
	combined, err := cmd.CombinedOutput()
	for _, line := range strings.Split(strings.TrimSpace(string(combined)), "\n") {
		s.Note("  %s", line)
	}
	if err != nil {
		s.Fail(err)
		return
	}

	var rows [][]any
	filepath.WalkDir(out, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, _ := d.Info()
		rel, _ := filepath.Rel(out, path)
		rows = append(rows, []any{rel, info.Size()})
		return nil
	})
	s.Table([]string{"file kept under -outputdir", "bytes"}, rows)
	s.Note("without -artifacts the same test still runs, but the directory is temporary and swept away")
}

func repoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "."
		}
		dir = parent
	}
}
