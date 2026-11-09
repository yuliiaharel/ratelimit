// Package policy loads per-domain rate-limit policies from YAML and
// watches the file for changes, so RateLimitService's limits can be
// retuned without a restart.
package policy

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"

	"github.com/yuliiaharel/ratelimit/redisstore"
)

// Policy is one domain's rate-limit configuration, as it appears in
// the YAML file. See redisstore.Policy (which this converts to) for
// exactly what each field means.
type Policy struct {
	Rate               float64 `yaml:"rate"`
	Burst              int     `yaml:"burst"`
	ReserveForCritical float64 `yaml:"reserve_for_critical"`
	ReserveForDefault  float64 `yaml:"reserve_for_default"`
}

func (p Policy) toRedisstore() redisstore.Policy {
	return redisstore.Policy{
		Rate:               p.Rate,
		Burst:              p.Burst,
		ReserveForCritical: p.ReserveForCritical,
		ReserveForDefault:  p.ReserveForDefault,
	}
}

// file is the YAML file's on-disk shape: named policies keyed by
// domain, plus an optional fallback for domains with no entry of
// their own.
type file struct {
	Policies map[string]Policy `yaml:"policies"`
	Default  *Policy           `yaml:"default"`
}

// fallbackName is what Lookup reports as the policy name when a
// domain has no entry of its own.
const fallbackName = "default"

// defaultFallback is used if the file doesn't define its own
// "default" policy: a conservative, obviously-a-placeholder limit, so
// a typo'd or missing domain fails loud (very low burst) rather than
// silently inheriting a generous one.
var defaultFallback = redisstore.Policy{Rate: 1, Burst: 1}

// snapshot is one immutable, fully-validated view of the policy file.
// Store swaps a *snapshot atomically on each successful reload, so
// Lookup never blocks on, or sees a half-applied, reload.
type snapshot struct {
	byDomain map[string]redisstore.Policy
	fallback redisstore.Policy
}

// Store holds the currently-loaded policies and can watch its file
// for changes. Safe for concurrent use.
type Store struct {
	path    string
	current atomic.Pointer[snapshot]
}

// Load reads and parses the policy file at path.
func Load(path string) (*Store, error) {
	s := &Store{path: path}
	if err := s.reload(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) reload() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return fmt.Errorf("policy: reading %s: %w", s.path, err)
	}

	var f file
	if err := yaml.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("policy: parsing %s: %w", s.path, err)
	}

	snap := &snapshot{
		byDomain: make(map[string]redisstore.Policy, len(f.Policies)),
		fallback: defaultFallback,
	}
	for domain, p := range f.Policies {
		rp := p.toRedisstore()
		if err := rp.Validate(); err != nil {
			return fmt.Errorf("policy: domain %q: %w", domain, err)
		}
		snap.byDomain[domain] = rp
	}
	if f.Default != nil {
		rp := f.Default.toRedisstore()
		if err := rp.Validate(); err != nil {
			return fmt.Errorf("policy: default: %w", err)
		}
		snap.fallback = rp
	}

	s.current.Store(snap)
	return nil
}

// Lookup returns domain's policy and the name that governed the
// decision -- domain itself, or "default" if domain has no entry of
// its own. The name is what the service surfaces as PolicyId.
func (s *Store) Lookup(domain string) (p redisstore.Policy, policyName string) {
	snap := s.current.Load()
	if p, ok := snap.byDomain[domain]; ok {
		return p, domain
	}
	return snap.fallback, fallbackName
}

// Watch reloads the policy file whenever it changes on disk, until ctx
// is canceled. A failed reload is logged and otherwise ignored: the
// Store keeps serving its last good snapshot rather than falling back
// to defaultFallback for every domain over one bad edit.
//
// It watches the file's parent directory, not the file itself: many
// editors and config-management tools replace a file via rename
// rather than in-place write, and a watch on the file alone can miss
// that (fsnotify's own documentation recommends this pattern).
func (s *Store) Watch(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("policy: starting watcher: %w", err)
	}
	defer func() { _ = watcher.Close() }()

	dir := filepath.Dir(s.path)
	if err := watcher.Add(dir); err != nil {
		return fmt.Errorf("policy: watching %s: %w", dir, err)
	}
	target := filepath.Clean(s.path)

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if filepath.Clean(event.Name) != target {
				continue
			}
			if event.Op&(fsnotify.Write|fsnotify.Create) == 0 {
				continue
			}
			if err := s.reload(); err != nil {
				log.Printf("policy: reload failed, keeping previous policies: %v", err)
				continue
			}
			log.Printf("policy: reloaded %s", s.path)
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			log.Printf("policy: watch error: %v", err)
		}
	}
}
