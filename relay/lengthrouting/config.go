// Package lengthrouting 提供输入估算和渠道资格判断，复用现有调度器。
package lengthrouting

import (
	"bytes"
	"encoding/json"
	"errors"
)

type CountResult struct {
	Tokens   int64  `json:"tokens"`
	Modality string `json:"modality"`
	Kind     string `json:"kind"`
	Reason   string `json:"reason,omitempty"`
}

func Unknown(reason string) CountResult { return CountResult{Kind: "unknown", Reason: reason} }

type Range struct {
	MinTokens *int64 `json:"min_tokens"`
	MaxTokens *int64 `json:"max_tokens,omitempty"`
}
type Rule struct {
	Modality      string `json:"-"`
	Unlimited     bool   `json:"-"`
	KnownRange    *Range `json:"known_range,omitempty"`
	AcceptUnknown bool   `json:"accept_unknown"`
}

func parseConfig(config string) (map[string]json.RawMessage, error) {
	if config == "" || bytes.Equal(bytes.TrimSpace([]byte(config)), []byte("null")) {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(config), &fields) != nil || fields == nil {
		return nil, errors.New("channel config must be a JSON object")
	}
	return fields, nil
}

func configModality(fields map[string]json.RawMessage) (string, error) {
	modality := ""
	if raw, exists := fields["input_modality"]; exists {
		if json.Unmarshal(raw, &modality) != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || (modality != "text" && modality != "multimodal") {
			return "", errors.New("input_modality must be text or multimodal")
		}
	}
	return modality, nil
}

// ParseModality 用于指定渠道首发，不因其豁免的长度规则阻断请求。
func ParseModality(config string) (string, error) {
	fields, err := parseConfig(config)
	if err != nil {
		return "", err
	}
	return configModality(fields)
}

// ParseRule 只读取新增字段，不重新编码渠道原有的 Config。
func ParseRule(config string) (*Rule, error) {
	fields, err := parseConfig(config)
	if err != nil {
		return nil, err
	}
	modality, err := configModality(fields)
	if err != nil {
		return nil, err
	}
	raw, ok := fields["input_length"]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if modality != "" {
			return &Rule{Modality: modality, Unlimited: true}, nil
		}
		return nil, nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return nil, errors.New("invalid input_length object")
	}
	if value, exists := object["accept_unknown"]; exists && !bytes.Equal(bytes.TrimSpace(value), []byte("true")) && !bytes.Equal(bytes.TrimSpace(value), []byte("false")) {
		return nil, errors.New("input_length accept_unknown must be a boolean")
	}
	for name := range object {
		if name != "known_range" && name != "accept_unknown" {
			return nil, errors.New("unknown input_length field")
		}
	}
	if rangeRaw := object["known_range"]; len(rangeRaw) != 0 && !bytes.Equal(bytes.TrimSpace(rangeRaw), []byte("null")) {
		var fields map[string]json.RawMessage
		if json.Unmarshal(rangeRaw, &fields) != nil || fields == nil {
			return nil, errors.New("invalid input_length known_range")
		}
		for name := range fields {
			if name != "min_tokens" && name != "max_tokens" {
				return nil, errors.New("unknown input_length range field")
			}
		}
	}
	var rule Rule
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&rule) != nil {
		return nil, errors.New("invalid input_length object")
	}
	if r := rule.KnownRange; r != nil {
		if r.MinTokens == nil || *r.MinTokens < 0 || (r.MaxTokens != nil && *r.MaxTokens < *r.MinTokens) {
			return nil, errors.New("input_length requires nonnegative min_tokens and max_tokens >= min_tokens")
		}
	}
	rule.Modality = modality
	return &rule, nil
}
func (r *Rule) Allows(count *CountResult) bool {
	if count == nil || r == nil || r.Unlimited {
		return true
	}
	if count.Kind == "unknown" {
		return r.AcceptUnknown
	}
	b := r.KnownRange
	return count.Kind == "estimated" && b != nil && b.MinTokens != nil && count.Tokens >= *b.MinTokens && (b.MaxTokens == nil || count.Tokens <= *b.MaxTokens)
}

// 未声明模态时保持旧渠道的准入行为；显式多模态渠道也允许纯文本。
// 模态未知的请求只由未声明模态限制的渠道接收，长度规则另行判断。
func (r *Rule) AllowsModality(modality string) bool {
	if r == nil || r.Modality == "" {
		return true
	}
	return (modality == "text" || modality == "multimodal") &&
		(r.Modality == modality || r.Modality == "multimodal")
}
