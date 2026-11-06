package lengthrouting

import "testing"

func TestUnconfiguredPolicyPreservesLegacyRequests(t *testing.T) {
	for _, config := range []string{"", "null", `{}`, `{"api_version":"old"}`, `{"input_length":null}`} {
		rule, err := ParseRule(config)
		if err != nil || rule != nil {
			t.Fatalf("config %q: rule=%+v err=%v", config, rule, err)
		}
		for _, modality := range []string{"text", "multimodal", ""} {
			count := &CountResult{Kind: "unknown", Modality: modality}
			if !rule.Allows(count) || !rule.AllowsModality(modality) {
				t.Fatalf("unconfigured policy rejected legacy modality %q", modality)
			}
		}
	}
}

func TestExplicitLengthPolicyBoundariesAndUnknown(t *testing.T) {
	rule, err := ParseRule(`{"api_version":"unchanged","input_length":{"known_range":{"min_tokens":10,"max_tokens":20},"accept_unknown":true}}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		tokens  int64
		allowed bool
	}{{9, false}, {10, true}, {20, true}, {21, false}} {
		if got := rule.Allows(&CountResult{Kind: "estimated", Tokens: tc.tokens}); got != tc.allowed {
			t.Errorf("tokens %d: got %v want %v", tc.tokens, got, tc.allowed)
		}
	}
	if !rule.Allows(&CountResult{Kind: "unknown"}) || !rule.Allows(nil) {
		t.Fatal("unknown or non-applicable request rejected")
	}
	if !rule.AllowsModality("multimodal") || !rule.AllowsModality("") {
		t.Fatal("length-only policy introduced a modality restriction")
	}
	if rule.Allows(&CountResult{Kind: "unexpected", Tokens: 15}) {
		t.Fatal("unexpected count kind accepted")
	}
}

func TestExplicitModalityAndLengthAreIndependent(t *testing.T) {
	for _, tc := range []struct {
		config, modality string
		allowed          bool
	}{
		{`{"input_modality":"text"}`, "text", true},
		{`{"input_modality":"text"}`, "multimodal", false},
		{`{"input_modality":"text"}`, "", false},
		{`{"input_modality":"multimodal"}`, "text", true},
		{`{"input_modality":"multimodal"}`, "multimodal", true},
		{`{"input_modality":"multimodal"}`, "", false},
	} {
		rule, err := ParseRule(tc.config)
		if err != nil {
			t.Fatal(err)
		}
		if got := rule.AllowsModality(tc.modality); got != tc.allowed {
			t.Errorf("%s modality %q: %v", tc.config, tc.modality, got)
		}
		if !rule.Allows(&CountResult{Kind: "unknown"}) {
			t.Fatal("modality-only rule introduced a length restriction")
		}
	}
	rule, err := ParseRule(`{"input_modality":"multimodal","input_length":{"known_range":{"min_tokens":1280}}}`)
	if err != nil {
		t.Fatal(err)
	}
	if rule.Allows(&CountResult{Kind: "estimated", Tokens: 1279}) || !rule.Allows(&CountResult{Kind: "estimated", Tokens: 1280}) || rule.Allows(&CountResult{Kind: "unknown"}) {
		t.Fatal("combined policy mismatch")
	}
}

func TestRejectInvalidExplicitPolicy(t *testing.T) {
	for _, config := range []string{
		`[]`, `false`, `{"input_modality":null}`, `{"input_modality":"audio"}`, `{"input_modality":1}`,
		`{"input_length":false}`, `{"input_length":{"unknown":true}}`,
		`{"input_length":{"accept_unknown":null}}`, `{"input_length":{"accept_unknown":1}}`,
		`{"input_length":{"known_range":[]}}`, `{"input_length":{"known_range":{"max_tokens":20}}}`,
		`{"input_length":{"known_range":{"min_tokens":-1}}}`, `{"input_length":{"known_range":{"min_tokens":21,"max_tokens":20}}}`,
		`{"input_length":{"known_range":{"min_tokens":1.5}}}`, `{"input_length":{"known_range":{"min_tokens":0,"extra":1}}}`,
	} {
		if _, err := ParseRule(config); err == nil {
			t.Errorf("accepted %s", config)
		}
	}
}
