package lang

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWritesAnArtifact(t *testing.T) {
	t.Attr("purpose", "show off go 1.26 test artifacts")

	dir := t.ArtifactDir()
	path := filepath.Join(dir, "report.txt")
	if err := os.WriteFile(path, []byte("written by the test run\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("artifact written to %s", path)
}
