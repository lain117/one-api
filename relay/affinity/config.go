package affinity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const (
	SessionSticky  = "session_sticky"
	ConsistentHash = "consistent_hash"
	defaultHeader  = "X-Session-ID"
)

type Config struct {
	Enabled         bool   `json:"enabled"`
	InflightEnabled bool   `json:"inflight_enabled"`
	Rules           []Rule `json:"rules"`
}

type Rule struct {
	Enabled      bool    `json:"enabled"`
	ModelPattern string  `json:"model_pattern"`
	Strategy     string  `json:"strategy"`
	Options      Options `json:"options"`
}

type Options struct {
	Header              string  `json:"header,omitempty"`
	TTLSeconds          int64   `json:"ttl_seconds,omitempty"`
	LoadAware           *bool   `json:"load_aware,omitempty"`
	BalanceAbsThreshold int64   `json:"balance_abs_threshold,omitempty"`
	BalanceRelThreshold float64 `json:"balance_rel_threshold,omitempty"`
}

var current atomic.Pointer[Config]

func init() { current.Store(&Config{}) }

// ParseConfig 在发布前完成规则校验，不改变当前配置。
func ParseConfig(raw string) (*Config, error) {
	cfg := &Config{}
	if strings.TrimSpace(raw) == "" {
		return cfg, nil
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(cfg); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("affinity config must contain one JSON object")
	}
	if strings.TrimSpace(raw) == "null" {
		return nil, fmt.Errorf("affinity config must be a JSON object")
	}
	for i := range cfg.Rules {
		r := &cfg.Rules[i]
		if !r.Enabled {
			continue
		}
		if r.Strategy != SessionSticky && r.Strategy != ConsistentHash {
			return nil, fmt.Errorf("affinity rule %d: unknown strategy %q", i, r.Strategy)
		}
		o := &r.Options
		o.Header = strings.TrimSpace(o.Header)
		if o.Header == "" {
			o.Header = defaultHeader
		}
		if !validHeader(o.Header) {
			return nil, fmt.Errorf("affinity rule %d: invalid header name", i)
		}
		o.Header = http.CanonicalHeaderKey(o.Header)
		if o.TTLSeconds < 0 || o.TTLSeconds > int64((time.Duration(1<<63-1))/time.Second) {
			return nil, fmt.Errorf("affinity rule %d: invalid ttl_seconds", i)
		}
		if o.TTLSeconds == 0 {
			o.TTLSeconds = 3600
		}
		if o.BalanceAbsThreshold < 0 || o.BalanceRelThreshold < 0 {
			return nil, fmt.Errorf("affinity rule %d: balance thresholds must be nonnegative", i)
		}
		if o.BalanceAbsThreshold == 0 {
			o.BalanceAbsThreshold = 32
		}
		if o.BalanceRelThreshold == 0 {
			o.BalanceRelThreshold = 1.5
		}
		if o.LoadAware == nil {
			enabled := true
			o.LoadAware = &enabled
		}
	}
	return cfg, nil
}

// Publish 发布独立快照，调用方后续修改解析结果不会影响正在处理的请求。
func Publish(cfg *Config) {
	if cfg == nil {
		current.Store(&Config{})
		return
	}
	snapshot := *cfg
	snapshot.Rules = append([]Rule(nil), cfg.Rules...)
	for i := range snapshot.Rules {
		if v := snapshot.Rules[i].Options.LoadAware; v != nil {
			copied := *v
			snapshot.Rules[i].Options.LoadAware = &copied
		}
	}
	current.Store(&snapshot)
}

func Configure(raw string) error {
	cfg, err := ParseConfig(raw)
	if err != nil {
		return err
	}
	Publish(cfg)
	return nil
}

func validHeader(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", c) {
			continue
		}
		return false
	}
	return true
}

func matches(pattern, model string) bool {
	if pattern == "" || pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(model, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == model
}
