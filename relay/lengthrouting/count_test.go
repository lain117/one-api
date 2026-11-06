package lengthrouting

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strconv"
	"sync"
	"testing"
)

// 用简单、可计算的字节计数隔离协议提取，不把某个模型分词器误当作路由规则。
func textBytes(s string) int { return len(s) }

func TestCountInputProtocolBudgets(t *testing.T) {
	cases := []struct {
		name, path, body string
		tokens           int64
		modality         string
	}{
		{"chat output max", "/v1/chat/completions", `{"messages":[{"role":"user","content":"abcd"}],"max_tokens":7,"max_completion_tokens":9}`, 13, "text"},
		{"chat legacy output larger", "/v1/chat/completions", `{"messages":[{"content":"a"}],"max_tokens":9,"max_completion_tokens":7}`, 10, "text"},
		{"chat functions and calls", "/v1/chat/completions", `{"messages":[{"content":null,"function_call":{"name":"go","arguments":"{\"a\":1}"},"tool_calls":[{"function":{"name":"f","arguments":"{}"}}]}]}`, 12, "text"},
		{"chat tool definition", "/v1/chat/completions", `{"messages":[{"content":"a"}],"tools":[{"type":"function","function":{"name":"run","description":"do","parameters":{"x":1}}}]}`, 13, "text"},
		{"chat legacy function", "/v1/chat/completions", `{"messages":[{"content":"a"}],"functions":[{"name":"run","description":"do","parameters":{"x":1}}]}`, 13, "text"},
		{"schema compact once", "/v1/chat/completions", `{"messages":[{"content":"a"}],"response_format":{"type":"json_schema","json_schema":{"name":"x","description":"d","schema":{ "a" : 1 }}}}`, 10, "text"},
		{"image fixed budget", "/v1/chat/completions", `{"messages":[{"content":[{"type":"text","text":"ab"},{"type":"image_url","image_url":{"url":"https://never-fetched.example/image"}}]}]}`, 1282, "multimodal"},
		{"completions string", "/v1/completions", `{"prompt":"abc","max_tokens":4}`, 7, "text"},
		{"completions batch", "/v1/completions", `{"prompt":["a","bc"],"max_tokens":4}`, 7, "text"},
		{"responses full fields", "/v1/responses", `{"input":"ab","instructions":"sys","tools":[{"type":"function","name":"f","description":"D","parameters":{"p":"x"}}],"text":{"format":{"name":"X","description":"d","schema":{"a":1}}},"max_output_tokens":4}`, 29, "text"},
		{"responses call result", "/v1/responses", `{"input":[{"type":"function_call","name":"go","arguments":"{}"},{"type":"function_call_output","output":"result"}]}`, 10, "text"},
		{"messages system thinking tools", "/v1/messages", `{"system":"sys","messages":[{"content":[{"type":"text","text":"ab"},{"type":"thinking","thinking":"zz"},{"type":"redacted_thinking","data":"opaque-not-counted"},{"type":"tool_use","name":"go","input":{"x":1}},{"type":"tool_result","content":"a"}]}],"tools":[{"name":"t","description":"d","input_schema":{"z":0}}],"max_tokens":10}`, 36, "text"},
		{"messages nested image", "/v1/messages", `{"messages":[{"content":[{"type":"tool_result","content":[{"type":"image","source":{"data":"opaque"}}]}]}]}`, 1280, "multimodal"},
		{"messages two schema forms", "/v1/messages", `{"messages":[],"output_config":{"format":{"schema":{"x":1}}},"output_format":{"schema":{"y":2}}}`, 14, "text"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.body)
			before := append([]byte(nil), body...)
			got := CountInput(context.Background(), tc.path, body, textBytes)
			if got.Kind != "estimated" || got.Tokens != tc.tokens || got.Modality != tc.modality {
				t.Fatalf("got %+v want tokens=%d modality=%s", got, tc.tokens, tc.modality)
			}
			if !bytes.Equal(body, before) {
				t.Fatal("request body changed")
			}
		})
	}
}

func TestCountInputUnknownAndModality(t *testing.T) {
	cases := []struct{ name, path, body, reason, modality string }{
		{"invalid json", "/v1/chat/completions", `{`, "invalid_json", ""},
		{"missing input", "/v1/chat/completions", `{}`, "missing_input", ""},
		{"wrong protocol", "/v1/embeddings", `{"input":"a"}`, "unsupported_protocol", ""},
		{"token ids", "/v1/completions", `{"prompt":[1,2]}`, "invalid_input", "text"},
		{"token id batches", "/v1/completions", `{"prompt":[[1],[2]]}`, "invalid_input", "text"},
		{"invalid output after image", "/v1/chat/completions", `{"messages":[{"content":[{"type":"image_url"}]}],"max_tokens":-1}`, "invalid_output_limit", "multimodal"},
		{"bad tools after image", "/v1/chat/completions", `{"messages":[{"content":[{"type":"image_url"}]}],"tools":{}}`, "invalid_tools", "multimodal"},
		{"missing video url", "/v1/chat/completions", `{"messages":[{"content":[{"type":"video_url"}]}]}`, "video_count_failed", "multimodal"},
		{"system checked after invalid messages", "/v1/messages", `{"messages":1,"system":[{"type":"unsupported"}]}`, "invalid_input", ""},
		{"invalid instructions", "/v1/responses", `{"input":"a","instructions":42}`, "invalid_instructions", "text"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CountInput(context.Background(), tc.path, []byte(tc.body), textBytes)
			if got.Kind != "unknown" || got.Reason != tc.reason || got.Modality != tc.modality {
				t.Fatalf("got %+v want reason=%s modality=%q", got, tc.reason, tc.modality)
			}
		})
	}
}

func TestUnsupportedContentDoesNotInventModality(t *testing.T) {
	got := CountInput(context.Background(), "/v1/chat/completions", []byte(`{"messages":[{"content":[{"type":"new_provider_block"},{"type":"text","text":"a"},{"type":"image_url"}]}]}`), textBytes)
	if got.Modality != "" || got.Kind != "unknown" || got.Reason != "unsupported_content" {
		t.Fatalf("unknown content was hidden: %+v", got)
	}
	lengthOnly, _ := ParseRule(`{"input_length":{"known_range":{"min_tokens":0,"max_tokens":100000}}}`)
	if lengthOnly.Allows(&got) {
		t.Fatal("partial token count passed as a known length")
	}
	acceptsUnknown, _ := ParseRule(`{"input_length":{"accept_unknown":true}}`)
	if !acceptsUnknown.Allows(&got) {
		t.Fatal("explicit unknown admission was ignored")
	}

	var legacy *Rule
	if !legacy.AllowsModality(got.Modality) {
		t.Fatal("unconfigured legacy channel rejected")
	}
	explicit, _ := ParseRule(`{"input_modality":"multimodal","input_length":{"accept_unknown":true}}`)
	if !explicit.Allows(&got) {
		t.Fatal("test requires explicit unknown length admission")
	}
	if explicit.AllowsModality(got.Modality) {
		t.Fatal("unknown modality passed explicit capability policy")
	}
}

func TestOverflowKeepsDetectedMedia(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("overflow test requires 64-bit int")
	}
	got := CountInput(context.Background(), "/v1/chat/completions", []byte(`{"messages":[{"content":[{"type":"text","text":"large"},{"type":"image_url"}]}]}`), func(string) int { value := int64(math.MaxInt64); return int(value) })
	if got.Kind != "unknown" || got.Reason != "token_overflow" || got.Modality != "multimodal" {
		t.Fatalf("got %+v", got)
	}
}

func TestMalformedScalarValuesRemainUnknown(t *testing.T) {
	for _, value := range []any{true, "bad", 1.5, map[string]any{"x": 1}} {
		body, _ := json.Marshal(map[string]any{"prompt": "a", "max_tokens": value})
		got := CountInput(context.Background(), "/v1/completions", body, textBytes)
		if got.Kind != "unknown" || got.Reason != "invalid_output_limit" {
			t.Errorf("value=%v result=%+v", value, got)
		}
	}
}

func TestCountInputConcurrentRequests(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := CountInput(context.Background(), "/v1/completions", []byte(`{"prompt":"abc","max_tokens":4}`), textBytes)
			if got.Tokens != 7 || got.Kind != "estimated" {
				t.Errorf("got %+v", got)
			}
		}()
	}
	wg.Wait()
}

func TestCountInputVideoFailurePreservesModality(t *testing.T) {
	// 内联坏数据不发网络请求；有无 ffmpeg 都必须保留已识别的多模态。
	body := []byte(`{"messages":[{"content":[{"type":"video_url","video_url":{"url":"data:video/mp4;base64,%%%"}},{"type":"text","text":"after"}]}]}`)
	got := CountInput(context.Background(), "/v1/chat/completions", body, textBytes)
	if got.Kind != "unknown" || got.Reason != "video_count_failed" || got.Modality != "multimodal" {
		t.Fatalf("got %+v", got)
	}
}
