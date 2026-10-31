package wasmplay

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
)

// Request is what a plugin reads as json on stdin.
type Request struct {
	Slug      string `json:"slug"`
	Target    string `json:"target"`
	Country   string `json:"country"`
	Referrer  string `json:"referrer"`
	UserAgent string `json:"userAgent"`
}

// Response is what a plugin writes as json on stdout. Empty fields mean
// leave it alone.
type Response struct {
	Target string `json:"target,omitempty"`
	Status int    `json:"status,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// Plugin is a compiled wasi command module plus the env it is allowed to see.
type Plugin struct {
	Name     string
	Env      map[string]string
	compiled wazero.CompiledModule
}

// Engine runs plugins in order for every request. Each call gets a fresh
// instance: no state leaks between requests, no filesystem, no network, and
// a deadline that wazero enforces by closing the module.
type Engine struct {
	rt      wazero.Runtime
	Plugins []*Plugin
	Timeout time.Duration
}

func NewEngine(ctx context.Context) *Engine {
	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCloseOnContextDone(true))
	wasi_snapshot_preview1.MustInstantiate(ctx, rt)
	return &Engine{rt: rt, Timeout: 250 * time.Millisecond}
}

func (e *Engine) Close(ctx context.Context) error { return e.rt.Close(ctx) }

func (e *Engine) Load(ctx context.Context, name string, wasm []byte, env map[string]string) error {
	c, err := e.rt.CompileModule(ctx, wasm)
	if err != nil {
		return fmt.Errorf("compile %s: %w", name, err)
	}
	e.Plugins = append(e.Plugins, &Plugin{Name: name, Env: env, compiled: c})
	return nil
}

func (e *Engine) Call(ctx context.Context, p *Plugin, req Request) (Response, error) {
	ctx, cancel := context.WithTimeout(ctx, e.Timeout)
	defer cancel()
	in, _ := json.Marshal(req)
	var out, stderr bytes.Buffer
	cfg := wazero.NewModuleConfig().
		WithName(""). // anonymous so instances don't collide
		WithStdin(bytes.NewReader(in)).
		WithStdout(&out).
		WithStderr(&stderr).
		WithSysWalltime().
		WithSysNanotime().
		WithRandSource(rand.Reader).
		WithArgs(p.Name)
	for k, v := range p.Env {
		cfg = cfg.WithEnv(k, v)
	}
	mod, err := e.rt.InstantiateModule(ctx, p.compiled, cfg)
	if mod != nil {
		mod.Close(ctx)
	}
	var exit *sys.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 0 {
		err = nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return Response{}, fmt.Errorf("plugin %s: timed out after %s", p.Name, e.Timeout)
		}
		return Response{}, fmt.Errorf("plugin %s: %w %s", p.Name, err, stderr.String())
	}
	var res Response
	if err := json.Unmarshal(out.Bytes(), &res); err != nil {
		return Response{}, fmt.Errorf("plugin %s: bad output %q: %w", p.Name, out.String(), err)
	}
	return res, nil
}

// Apply runs every plugin in order. Each sees the target the previous one
// produced; the first non zero status stops the chain.
func (e *Engine) Apply(ctx context.Context, req Request) (Response, string, error) {
	for _, p := range e.Plugins {
		res, err := e.Call(ctx, p, req)
		if err != nil {
			return Response{}, p.Name, err
		}
		if res.Status != 0 {
			res.Target = req.Target
			return res, p.Name, nil
		}
		if res.Target != "" {
			req.Target = res.Target
		}
	}
	return Response{Target: req.Target, Status: 302}, "", nil
}
