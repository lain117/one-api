package openai

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common/ctxkey"
	dbmodel "github.com/songquanpeng/one-api/model"
	relaymodel "github.com/songquanpeng/one-api/relay/model"
	"github.com/songquanpeng/one-api/relay/relaymode"
)

func TestSanitizeChatResponseID(t *testing.T) {
	for _, body := range []string{
		` { "id": "chatcmpl-normal", "choices": [] } `,
		`{"nested":{"id":"decode_addr_private"}}`,
		`{"id":null}`, `{"id":7}`, `{"id":true}`,
		`{"id":{"value":"prefill_addr_private"}}`, `{"id":["decode_addr_private"]}`,
		`{"id":"prefill_addr_broken"`, `null`, `[]`,
	} {
		t.Run(body, func(t *testing.T) {
			got, changed := sanitizeChatResponseID([]byte(body))
			if changed || string(got) != body {
				t.Fatalf("unexpected rewrite: %s, changed=%v", got, changed)
			}
		})
	}
	for _, id := range []string{
		"prefill_addr_private_" + strings.Repeat("a", 32),
		"decode_addr_private_" + strings.Repeat("B", 64),
		"prefill_addr_private_decode_addr_private_" + strings.Repeat("c", 64),
		"decode_addr_private_no_hash",
		"prefill_addr_private_" + strings.Repeat("g", 32),
		"decode_addr_private_" + strings.Repeat("a", 33),
	} {
		t.Run(id, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"id":%q,"unknown":{"id":"decode_addr_nested","number":9007199254740993},"usage":{"total_tokens":3}}`, id))
			got, changed := sanitizeChatResponseID(body)
			if !changed {
				t.Fatal("address ID was not rewritten")
			}
			wantID := id[strings.LastIndexByte(id, '_')+1:]
			if _, err := hex.DecodeString(wantID); err != nil || (len(wantID) != 32 && len(wantID) != 64) {
				digest := sha256.Sum256([]byte(id))
				wantID = hex.EncodeToString(digest[:])
			}
			want := decodeResponseIDJSON(t, body)
			want["id"] = wantID
			if !reflect.DeepEqual(decodeResponseIDJSON(t, got), want) {
				t.Fatalf("rewritten response = %s, want ID %s with other fields unchanged", got, wantID)
			}
			again, changedAgain := sanitizeChatResponseID(got)
			if changedAgain || !bytes.Equal(again, got) {
				t.Fatal("sanitization must be idempotent")
			}
		})
	}
}

func TestResponseIDHandlerHTTP(t *testing.T) {
	id := "prefill_addr_private_decode_addr_private_" + strings.Repeat("a", 32)
	body := fmt.Sprintf(`{"id":%q,"object":"chat.completion","choices":[{"message":{"role":"assistant","content":"hello"}}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3},"extension":{"id":"decode_addr_nested","number":9007199254740993}}`, id)
	for _, tc := range []struct {
		name    string
		path    string
		config  any
		changed bool
	}{
		{"unset", "/v1/chat/completions", nil, false},
		{"disabled", "/v1/chat/completions", dbmodel.ChannelConfig{}, false},
		{"wrong-context-type", "/v1/chat/completions", true, false},
		{"enabled", "/v1/chat/completions", dbmodel.ChannelConfig{SanitizeResponseID: true}, true},
		{"completions", "/v1/completions", dbmodel.ChannelConfig{SanitizeResponseID: true}, false},
		{"embeddings", "/v1/embeddings", dbmodel.ChannelConfig{SanitizeResponseID: true}, false},
		{"noncanonical-chat-path", "/other/v1/chat/completions", dbmodel.ChannelConfig{SanitizeResponseID: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, got, result := runResponseIDHTTP(t, body, tc.path, false, relaymode.ChatCompletions, tc.config)
			if result.err != nil || result.usage == nil || result.usage.TotalTokens != 3 {
				t.Fatalf("handler result = %+v", result)
			}
			if resp.Header.Get("X-Upstream-Header") != "kept" || resp.ContentLength != int64(len(got)) || resp.Header.Get("Content-Length") != strconv.Itoa(len(got)) {
				t.Fatalf("response headers do not match body: %v; bytes=%d", resp.Header, len(got))
			}
			if !tc.changed && string(got) != body {
				t.Fatalf("disabled or non-chat response changed: %s", got)
			}
			want := decodeResponseIDJSON(t, []byte(body))
			if tc.changed {
				want["id"] = strings.Repeat("a", 32)
			}
			if !reflect.DeepEqual(decodeResponseIDJSON(t, got), want) {
				t.Fatalf("unexpected response: %s", got)
			}
			// 仅模拟 SDK 的字符串字段解码约束，不宣称已运行实际 SDK。
			var typed struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(got, &typed); err != nil || typed.ID != want["id"] {
				t.Fatalf("typed ID decode failed: %q, %v", typed.ID, err)
			}
		})
	}
	for _, idJSON := range []string{`"normal"`, `7`, `null`, `true`, `{"id":"decode_addr_nested"}`} {
		t.Run("unchanged-ID-"+idJSON, func(t *testing.T) {
			input := `{"id":` + idJSON + `,"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`
			_, got, result := runResponseIDHTTP(t, input, "/v1/chat/completions", false, relaymode.ChatCompletions, dbmodel.ChannelConfig{SanitizeResponseID: true})
			if result.err != nil || string(got) != input {
				t.Fatalf("response changed: %s, result=%+v", got, result)
			}
		})
	}
}

func TestResponseIDHandlerKeepsErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		code string
	}{
		{"invalid-json", `{"id":"decode_addr_private"`, "unmarshal_response_body_failed"},
		{"upstream-error", `{"id":"decode_addr_private","error":{"type":"upstream_error","code":"upstream_failure","message":"failed"}}`, "upstream_failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, enabled := range []bool{false, true} {
				_, body, result := runResponseIDHTTP(t, tc.body, "/v1/chat/completions", false, relaymode.ChatCompletions, dbmodel.ChannelConfig{SanitizeResponseID: enabled})
				if result.err == nil || result.err.Error.Code != tc.code || result.usage != nil || len(body) != 0 {
					t.Fatalf("enabled=%v, result=%+v, body=%s", enabled, result, body)
				}
			}
		})
	}
}

func TestResponseIDStreamHTTP(t *testing.T) {
	id := "decode_addr_private_invalid_suffix"
	digest := sha256.Sum256([]byte(id))
	wantID := hex.EncodeToString(digest[:])
	choice := fmt.Sprintf(`{"id":%q,"choices":[{"delta":{"content":"hello"}}],"extension":{"id":"decode_addr_nested","number":9007199254740993}}`, id)
	usage := fmt.Sprintf(`{"id":%q,"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`, id)
	empty := fmt.Sprintf(`{"id":%q,"choices":[]}`, id)
	errorFrame := fmt.Sprintf(`{"id":%q,"error":{"type":"upstream_error","message":"failed"}}`, id)
	numeric := `{"id":7,"choices":[{"delta":{"content":"ignored"}}]}`
	malformed := `{"id":`
	invalidChoices := fmt.Sprintf(`{"id":%q,"choices":"invalid"}`, id)
	body := "data: " + strings.Join([]string{choice, usage, empty, errorFrame, numeric, malformed, invalidChoices, "[DONE]"}, "\n\ndata: ") + "\n\n"
	_, got, result := runResponseIDHTTP(t, body, "/v1/chat/completions", true, relaymode.ChatCompletions, dbmodel.ChannelConfig{SanitizeResponseID: true})
	if result.err != nil || result.text != "hello" || result.usage == nil || result.usage.TotalTokens != 3 {
		t.Fatalf("stream result = %+v", result)
	}
	frames := responseIDFrames(string(got))
	if len(frames) != 6 || frames[2] != numeric || frames[3] != malformed || frames[5] != "[DONE]" {
		t.Fatalf("unexpected frame forwarding: %q", frames)
	}
	for i, input := range map[int]string{0: choice, 1: usage, 4: invalidChoices} {
		want := decodeResponseIDJSON(t, []byte(input))
		want["id"] = wantID
		if !reflect.DeepEqual(decodeResponseIDJSON(t, []byte(frames[i])), want) {
			t.Fatalf("frame %d changed unrelated fields: %s", i, frames[i])
		}
		var typed struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(frames[i]), &typed); err != nil || typed.ID != wantID {
			t.Fatalf("typed stream ID decode failed: %q, %v", typed.ID, err)
		}
	}
	for _, tc := range []struct {
		name    string
		config  any
		mode    int
		payload string
		text    string
	}{
		{"unset", nil, relaymode.ChatCompletions, choice, "hello"},
		{"disabled", dbmodel.ChannelConfig{}, relaymode.ChatCompletions, choice, "hello"},
		{"non-chat", dbmodel.ChannelConfig{SanitizeResponseID: true}, relaymode.Completions, fmt.Sprintf(`{"id":%q,"choices":[{"text":"completion"}]}`, id), "completion"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := "data: " + tc.payload + "\n\ndata: [DONE]\n\n"
			_, got, result := runResponseIDHTTP(t, input, "/v1/chat/completions", true, tc.mode, tc.config)
			if string(got) != input || result.err != nil || result.text != tc.text {
				t.Fatalf("unexpected stream: %s, result=%+v", got, result)
			}
		})
	}
}

type responseIDHandlerResult struct {
	err   *relaymodel.ErrorWithStatusCode
	text  string
	usage *relaymodel.Usage
}

func runResponseIDHTTP(t *testing.T, body, path string, streaming bool, mode int, config any) (*http.Response, []byte, responseIDHandlerResult) {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream-Header", "kept")
		if streaming {
			w.Header().Set("Content-Type", "text/event-stream")
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		}
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()
	results := make(chan responseIDHandlerResult, 1)
	router := gin.New()
	router.POST(path, func(c *gin.Context) {
		if config != nil {
			c.Set(ctxkey.Config, config)
		}
		resp, err := upstream.Client().Get(upstream.URL)
		if err != nil {
			t.Errorf("upstream request failed: %v", err)
			results <- responseIDHandlerResult{}
			return
		}
		if streaming {
			handlerErr, text, usage := StreamHandler(c, resp, mode)
			results <- responseIDHandlerResult{handlerErr, text, usage}
		} else {
			handlerErr, usage := Handler(c, resp, 1, "test-model")
			results <- responseIDHandlerResult{err: handlerErr, usage: usage}
		}
	})
	gateway := httptest.NewServer(router)
	defer gateway.Close()
	resp, err := gateway.Client().Post(gateway.URL+path, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, got, <-results
}

func decodeResponseIDJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func responseIDFrames(body string) []string {
	var frames []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, dataPrefix) {
			frames = append(frames, strings.TrimPrefix(line, dataPrefix))
		}
	}
	return frames
}
