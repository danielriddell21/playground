package wasmplay

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

const pluginPkg = "playground/internal/wasmplay/plugins/"

// BuildPlugin compiles one of the bundled plugins to wasip1 with the local
// go toolchain and caches the result by source hash. Run from the repo root.
func BuildPlugin(name string) ([]byte, error) {
	dir := filepath.Join("internal", "wasmplay", "plugins", name)
	src, err := os.ReadFile(filepath.Join(dir, "main.go"))
	if err != nil {
		return nil, fmt.Errorf("plugin source (run from the repo root): %w", err)
	}
	sum := sha256.Sum256(src)
	cache, err := os.UserCacheDir()
	if err != nil {
		cache = os.TempDir()
	}
	out := filepath.Join(cache, "playground-wasm", name+"-"+hex.EncodeToString(sum[:8])+".wasm")
	if b, err := os.ReadFile(out); err == nil {
		return b, nil
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return nil, err
	}
	cmd := exec.Command("go", "build", "-trimpath", "-ldflags=-s -w", "-o", out, pluginPkg+name)
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("build %s: %w", name, err)
	}
	return os.ReadFile(out)
}
