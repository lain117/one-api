package affinity

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"
)

const stickyConfig = `{"enabled":true,"rules":[{"enabled":true,"model_pattern":"*","strategy":"session_sticky","options":{}}]}`

func restoreConfig(t *testing.T) {
	t.Helper()
	old := current.Load()
	t.Cleanup(func() { current.Store(old) })
}

func TestParseConfigDefaultsAndValidation(t *testing.T) {
	cfg, err := ParseConfig(stickyConfig)
	if err != nil {
		t.Fatal(err)
	}
	o := cfg.Rules[0].Options
	if o.Header != "X-Session-Id" || o.TTLSeconds != 3600 || !*o.LoadAware || o.BalanceAbsThreshold != 32 || o.BalanceRelThreshold != 1.5 {
		t.Fatalf("defaults: %+v", o)
	}
	if cfg.InflightEnabled {
		t.Fatal("inflight must default off")
	}
	for _, raw := range []string{"", "  ", `{}`, `{"enabled":false}`} {
		parsed, err := ParseConfig(raw)
		if err != nil || parsed.Enabled {
			t.Fatalf("disabled %q: %+v %v", raw, parsed, err)
		}
	}
	for name, raw := range map[string]string{
		"null": `null`, "array": `[]`, "malformed": `{`, "trailing": `{} {}`, "unknown": `{"enable":true}`,
		"strategy":     `{"rules":[{"enabled":true,"strategy":"random"}]}`,
		"header":       `{"rules":[{"enabled":true,"strategy":"session_sticky","options":{"header":"X Session"}}]}`,
		"ttl":          `{"rules":[{"enabled":true,"strategy":"session_sticky","options":{"ttl_seconds":-1}}]}`,
		"ttl_overflow": `{"rules":[{"enabled":true,"strategy":"session_sticky","options":{"ttl_seconds":9223372037}}]}`,
		"absolute":     `{"rules":[{"enabled":true,"strategy":"session_sticky","options":{"balance_abs_threshold":-1}}]}`,
		"relative":     `{"rules":[{"enabled":true,"strategy":"session_sticky","options":{"balance_rel_threshold":-1}}]}`,
		"body":         `{"rules":[{"enabled":true,"strategy":"session_sticky","options":{"body":"session"}}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseConfig(raw); err == nil {
				t.Fatalf("accepted %s", raw)
			}
		})
	}
	parsed, err := ParseConfig(`{"enabled":true,"rules":[{"enabled":true,"strategy":"consistent_hash","options":{"header":"x-custom-session","load_aware":false,"ttl_seconds":42,"balance_abs_threshold":4,"balance_rel_threshold":2}}]}`)
	if err != nil || parsed.Rules[0].Options.Header != "X-Custom-Session" || *parsed.Rules[0].Options.LoadAware {
		t.Fatalf("explicit options: %+v %v", parsed, err)
	}
}

func TestPublishSnapshotAndInvalidConfigure(t *testing.T) {
	restoreConfig(t)
	cfg, _ := ParseConfig(stickyConfig)
	Publish(cfg)
	cfg.Rules[0].Options.Header = "Other"
	*cfg.Rules[0].Options.LoadAware = false
	cfg.Enabled = false
	if !current.Load().Enabled || current.Load().Rules[0].Options.Header != "X-Session-Id" || !*current.Load().Rules[0].Options.LoadAware {
		t.Fatal("snapshot changed with caller")
	}
	before := current.Load()
	if err := Configure(`{`); err == nil || current.Load() != before {
		t.Fatal("invalid update replaced snapshot")
	}
	if err := Configure(""); err != nil || current.Load().Enabled {
		t.Fatal("empty update did not disable")
	}
	Publish(nil)
	if current.Load() == nil || current.Load().Enabled {
		t.Fatal("nil publish")
	}
}

func TestConcurrentConfigurationSnapshots(t *testing.T) {
	restoreConfig(t)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if err := Configure(stickyConfig); err != nil {
					t.Error(err)
				}
				snapshot := current.Load()
				if snapshot.Enabled && len(snapshot.Rules) != 1 {
					t.Error("partial snapshot")
				}
			}
		}()
	}
	wg.Wait()
}

func TestScopeIsolation(t *testing.T) {
	original := scopeHash("1", "group", "model", "session")
	for _, parts := range [][]string{{"2", "group", "model", "session"}, {"1", "other", "model", "session"}, {"1", "group", "other", "session"}, {"1", "group", "model", "other"}} {
		if scopeHash(parts...) == original {
			t.Fatal("scope collision")
		}
	}
	if scopeHash("ab", "c") == scopeHash("a", "bc") {
		t.Fatal("ambiguous concatenation")
	}
	if scopeHash("1", "group", "model", "session") != original {
		t.Fatal("unstable scope")
	}
}

func TestModelPatterns(t *testing.T) {
	for _, tc := range []struct {
		pattern, model string
		match          bool
	}{{"", "m", true}, {"*", "m", true}, {"gpt-*", "gpt-4", true}, {"gpt-*", "other", false}, {"exact", "exact", true}, {"exact", "exact-more", false}} {
		if matches(tc.pattern, tc.model) != tc.match {
			t.Errorf("%+v", tc)
		}
	}
}

func TestNoContextDoesNotSelect(t *testing.T) {
	restoreConfig(t)
	Publish(nil)
	if NewRequest(context.Background(), 1, "g", "m", http.Header{"X-Session-Id": []string{"s"}}) != nil {
		t.Fatal("disabled request")
	}
	if result := Pick(context.Background(), nil, []Candidate{{ID: 1}}); result.ChannelID != 0 || result.Commit != nil {
		t.Fatal("nil request selected")
	}
	Acquire(context.Background(), "m", 1)()
}

func TestRendezvousOrderAndMembership(t *testing.T) {
	candidates := []Candidate{{ID: 1, Weight: 0}, {ID: 2, Weight: 1}, {ID: 3, Weight: 4}}
	reversed := []Candidate{candidates[2], candidates[1], candidates[0]}
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("session-%d", i)
		winner := candidates[rendezvous(key, candidates)].ID
		if reversed[rendezvous(key, reversed)].ID != winner {
			t.Fatal("candidate order changed selection")
		}
		reduced := []Candidate{candidates[0], candidates[1]}
		if winner != 3 && reduced[rendezvous(key, reduced)].ID != winner {
			t.Fatal("unaffected winner moved")
		}
	}
	if rendezvous("key", nil) != -1 || rendezvous("key", []Candidate{{ID: 0}}) != -1 {
		t.Fatal("invalid candidate selected")
	}
}

func TestRendezvousZeroWeightAndDistribution(t *testing.T) {
	zero := []Candidate{{ID: 1}, {ID: 2}}
	counts := map[int]int{}
	for i := 0; i < 500; i++ {
		counts[zero[rendezvous(fmt.Sprint(i), zero)].ID]++
	}
	if len(counts) != 2 {
		t.Fatal("zero weights are not eligible")
	}
	weighted := []Candidate{{ID: 1, Weight: 1}, {ID: 2, Weight: 3}}
	count := 0
	for i := 0; i < 8000; i++ {
		if weighted[rendezvous(fmt.Sprint(i), weighted)].ID == 2 {
			count++
		}
	}
	if count < 5600 || count > 6400 {
		t.Fatalf("weighted distribution: %d/8000", count)
	}
	if effectiveWeight(0) != 1 || effectiveWeight(-1) != 1 || effectiveWeight(3) != 3 {
		t.Fatal("weight normalization")
	}
}

func TestLoadThresholds(t *testing.T) {
	options := Options{BalanceAbsThreshold: 32, BalanceRelThreshold: 1.5}
	candidates := []Candidate{{ID: 1}, {ID: 2}}
	for _, tc := range []struct {
		loads map[int]int64
		want  bool
	}{{map[int]int64{1: 32, 2: 0}, false}, {map[int]int64{1: 33, 2: 0}, true}, {map[int]int64{1: 100, 2: 80}, false}, {map[int]int64{1: 100, 2: 1}, true}} {
		if got := overloaded(candidates, tc.loads, 0, options); got != tc.want {
			t.Errorf("%v = %v", tc.loads, got)
		}
	}
	candidates[0].Weight = 100
	if overloaded(candidates, map[int]int64{1: 100, 2: 10}, 0, options) {
		t.Fatal("ignored effective weights")
	}
	before := append([]Candidate(nil), candidates...)
	_ = overloaded(candidates, map[int]int64{1: 100, 2: 10}, 0, options)
	if !reflect.DeepEqual(candidates, before) {
		t.Fatal("mutated candidates")
	}
}

func TestDetachedContextPreservesValuesWithIndependentDeadline(t *testing.T) {
	type keyType struct{}
	original, cancel := context.WithCancel(context.WithValue(context.Background(), keyType{}, "request-id"))
	cancel()
	detached := detachedContext{original}
	if detached.Err() != nil || detached.Done() != nil || detached.Value(keyType{}) != "request-id" {
		t.Fatal("detached context lost values or retained cancellation")
	}
	if _, ok := detached.Deadline(); ok {
		t.Fatal("detached context retained deadline")
	}
	bounded, stop := context.WithTimeout(detached, time.Millisecond)
	defer stop()
	<-bounded.Done()
	if bounded.Err() != context.DeadlineExceeded {
		t.Fatal("independent timeout missing")
	}
}
