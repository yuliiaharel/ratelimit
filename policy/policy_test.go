package policy

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "policies.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

func TestLoadAndLookup(t *testing.T) {
	path := writeFile(t, t.TempDir(), `
policies:
  api:
    rate: 10
    burst: 20
    reserve_for_critical: 0.3
default:
  rate: 5
  burst: 10
`)

	s, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	p, name := s.Lookup("api")
	if name != "api" || p.Rate != 10 || p.Burst != 20 || p.ReserveForCritical != 0.3 {
		t.Fatalf("Lookup(api) = %+v, %q, want rate 10 burst 20 reserve 0.3, name api", p, name)
	}

	p, name = s.Lookup("unknown-domain")
	if name != "default" || p.Rate != 5 || p.Burst != 10 {
		t.Fatalf("Lookup(unknown-domain) = %+v, %q, want the file's default policy", p, name)
	}
}

func TestLoadWithoutDefaultFallsBackToConservativePlaceholder(t *testing.T) {
	path := writeFile(t, t.TempDir(), `
policies:
  api:
    rate: 10
    burst: 20
`)

	s, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	p, name := s.Lookup("no-such-domain")
	if name != "default" {
		t.Fatalf("name = %q, want default", name)
	}
	if p != defaultFallback {
		t.Fatalf("p = %+v, want the built-in placeholder %+v", p, defaultFallback)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml")); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestLoadInvalidPolicyErrors(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"negative rate", "policies:\n  api:\n    rate: -1\n    burst: 10\n"},
		{"zero burst", "policies:\n  api:\n    rate: 1\n    burst: 0\n"},
		{"reserve above 1", "policies:\n  api:\n    rate: 1\n    burst: 10\n    reserve_for_critical: 1.5\n"},
		{"invalid default", "default:\n  rate: 0\n  burst: 10\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeFile(t, t.TempDir(), tt.content)
			if _, err := Load(path); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestWatchHotReloads(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, `
policies:
  api:
    rate: 10
    burst: 20
`)

	s, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	watchErr := make(chan error, 1)
	go func() { watchErr <- s.Watch(ctx) }()

	// Give the watcher a moment to start before editing the file.
	time.Sleep(100 * time.Millisecond)

	writeFile(t, dir, `
policies:
  api:
    rate: 99
    burst: 42
`)

	deadline := time.After(5 * time.Second)
	for {
		p, _ := s.Lookup("api")
		if p.Burst == 42 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("policy wasn't hot-reloaded within the deadline; last seen Burst=%d", p.Burst)
		case <-time.After(20 * time.Millisecond):
		}
	}

	cancel()
	if err := <-watchErr; err != nil {
		t.Fatalf("Watch returned an error: %v", err)
	}
}
