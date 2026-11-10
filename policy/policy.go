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
	Rate               float64     `yaml:"rate"`
	Burst              int         `yaml:"burst"`
	ReserveForCritical float64     `yaml:"reserve_for_critical"`
	ReserveForDefault  float64     `yaml:"reserve_for_default"`
	FailureMode        FailureMode `yaml:"failure_mode"`
}

// FailureMode is what RateLimitService answers for a domain when its
// backend (Redis) errors or times out.
type FailureMode string

const (
	// FailOpen allows requests while the backend is unavailable:
	// availability over protection. This is the default for any domain
	// that doesn't set failure_mode, so a limiter outage doesn't become
	// an outage of every service that calls it.
	FailOpen FailureMode = "fail_open"

	// FailClosed denies requests while the backend is unavailable:
	// protection over availability, for domains guarding something that
	// must not be overrun (e.g. a paid third-party API).
	FailClosed FailureMode = "fail_closed"
)

// DefaultFailureMode applies to any domain (and the file's default
// policy) that leaves failure_mode unset.
const DefaultFailureMode = FailOpen

// resolve applies the default and rejects unknown values, so a typo
// like "fail-closed" fails the reload instead of silently failing open.
func (m FailureMode) resolve() (FailureMode, error) {
	switch m {
	case "":
		return DefaultFailureMode, nil
	case FailOpen, FailClosed:
		return m, nil
	default:
		return "", fmt.Errorf("unknown failure_mode %q (want %q or %q)", m, FailOpen, FailClosed)
	}
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

// entry is one domain's validated limits plus its failure mode.
type entry struct {
	limits      redisstore.Policy
	failureMode FailureMode
}

func (p Policy) toEntry() (entry, error) {
	rp := p.toRedisstore()
	if err := rp.Validate(); err != nil {
		return entry{}, err
	}
	mode, err := p.FailureMode.resolve()
	if err != nil {
		return entry{}, err
	}
	return entry{limits: rp, failureMode: mode}, nil
}

// snapshot is one immutable, fully-validated view of the policy file.
// Store swaps a *snapshot atomically on each successful reload, so
// Lookup never blocks on, or sees a half-applied, reload.
type snapshot struct {
	byDomain map[string]entry
	fallback entry
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
		byDomain: make(map[string]entry, len(f.Policies)),
		fallback: entry{limits: defaultFallback, failureMode: DefaultFailureMode},
	}
	for domain, p := range f.Policies {
		e, err := p.toEntry()
		if err != nil {
			return fmt.Errorf("policy: domain %q: %w", domain, err)
		}
		snap.byDomain[domain] = e
	}
	if f.Default != nil {
		e, err := f.Default.toEntry()
		if err != nil {
			return fmt.Errorf("policy: default: %w", err)
		}
		snap.fallback = e
	}

	s.current.Store(snap)
	return nil
}

// Lookup returns domain's limits, its failure mode, and the name that
// governed the decision -- domain itself, or "default" if domain has
// no entry of its own. The name is what the service surfaces as
// PolicyId. All three come from the same snapshot, so a concurrent
// reload can't pair one version's limits with another's failure mode.
func (s *Store) Lookup(domain string) (p redisstore.Policy, mode FailureMode, policyName string) {
	snap := s.current.Load()
	if e, ok := snap.byDomain[domain]; ok {
		return e.limits, e.failureMode, domain
	}
	return snap.fallback.limits, snap.fallback.failureMode, fallbackName
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
