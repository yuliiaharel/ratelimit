package policy_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yuliiaharel/ratelimit/policy"
)

// Load a policy file and look up the limits for two domains: one with
// its own entry, one that falls back to the default.
func ExampleLoad() {
	dir, err := os.MkdirTemp("", "policy-example")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = os.RemoveAll(dir) }()

	path := filepath.Join(dir, "policies.yaml")
	content := `
policies:
  send-message:
    rate: 2
    burst: 5
    reserve_for_critical: 0.5
    failure_mode: fail_closed
default:
  rate: 5
  burst: 10
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		fmt.Println(err)
		return
	}

	store, err := policy.Load(path)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, domain := range []string{"send-message", "search"} {
		limits, mode, name := store.Lookup(domain)
		fmt.Printf("%s: policy=%s rate=%v burst=%d reserve_for_critical=%v failure_mode=%s\n",
			domain, name, limits.Rate, limits.Burst, limits.ReserveForCritical, mode)
	}

	// Output:
	// send-message: policy=send-message rate=2 burst=5 reserve_for_critical=0.5 failure_mode=fail_closed
	// search: policy=default rate=5 burst=10 reserve_for_critical=0 failure_mode=fail_open
}
