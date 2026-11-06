package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common/client"
	"github.com/songquanpeng/one-api/relay/apitype"
	"github.com/songquanpeng/one-api/relay/channeltype"
	"github.com/songquanpeng/one-api/relay/meta"
	"github.com/songquanpeng/one-api/relay/model"
	"github.com/songquanpeng/one-api/relay/relaymode"
)

func sse(payloads ...string) string { return "data: " + strings.Join(payloads, "\n\ndata: ") + "\n\n" }

const streamErrorFrame = `{"error":{"message":"fixture failure","type":"server_error","code":"fixture_failure"}}`

func TestStreamRegressionCases(t *testing.T) {
	InitTokenEncoders()
	for _, mode := range []int{relaymode.ChatCompletions, relaymode.Completions} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			content := `{"choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}`
			finish := `{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`
			if mode == relaymode.Completions {
				content = `{"choices":[{"index":0,"text":"hello","finish_reason":null}]}`
				finish = `{"choices":[{"index":0,"text":"","finish_reason":"stop"}]}`
			}
			usage := `{"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`
			for _, tc := range []struct {
				name, body, code  string
				count             int
				readErr, closeErr error
			}{
				{name: "normal-done", body: sse(content, finish, usage, "[DONE]"), count: 1},
				{name: "finished-eof", body: sse(content, finish), count: 1},
				{name: "explicit-error-first", body: sse(streamErrorFrame, "[DONE]"), code: "fixture_failure", count: 1},
				{name: "explicit-error-after-content", body: sse(content, streamErrorFrame, "[DONE]"), code: "fixture_failure", count: 1},
				{name: "broken-json", body: sse(content, `{"broken":`, "[DONE]"), code: "invalid_stream_response", count: 1},
				{name: "invalid-choices", body: sse(`{"choices":"bad"}`, "[DONE]"), code: "invalid_stream_response", count: 1},
				{name: "partial-eof", body: sse(content), code: "incomplete_stream", count: 1},
				{name: "empty-eof", code: "incomplete_stream", count: 1},
				{name: "usage-only-eof", body: sse(usage), code: "incomplete_stream", count: 1},
				{name: "missing-choice", body: sse(finish), code: "incomplete_stream", count: 2},
				{name: "duplicate-choice", body: sse(finish, finish), code: "incomplete_stream", count: 2},
				{name: "out-of-range-choice", body: sse(finish, `{"choices":[{"index":2,"finish_reason":"stop"}]}`), code: "incomplete_stream", count: 2},
				{name: "out-of-order-finish", body: sse(`{"choices":[{"index":1,"finish_reason":"length"}]}`, finish), count: 2},
				{name: "reader-failure", body: sse(content), code: "read_stream_failed", count: 1, readErr: io.ErrUnexpectedEOF},
				{name: "scanner-limit", body: "data: " + strings.Repeat("x", 70*1024) + "\n\n", code: "read_stream_failed", count: 1},
				{name: "close-failure", body: sse(content, finish, "[DONE]"), code: "close_response_body_failed", count: 1, closeErr: errors.New("fixture close failure")},
				{name: "error-survives-close-failure", body: sse(streamErrorFrame), code: "fixture_failure", count: 1, closeErr: errors.New("fixture close failure")},
				{name: "done-is-terminal", body: sse("[DONE]", `broken`), count: 1, readErr: io.ErrUnexpectedEOF},
				{name: "done-prefix-is-not-terminal", body: sse("[DONE]garbage"), code: "invalid_stream_response", count: 1},
				{name: "numeric-id", body: sse(`{"id":7,"choices":[{"index":0,"delta":{"content":"x"},"text":"x","finish_reason":"stop"}]}`), count: 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					w := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(w)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
					body := &streamTestBody{Reader: strings.NewReader(tc.body), readErr: tc.readErr, closeErr: tc.closeErr}
					m := &meta.Meta{Mode: mode, ExpectedChoices: tc.count, ActualModelName: "gpt-3.5-turbo", PromptTokens: 4}
					err, _, _ := StreamHandler(c, &http.Response{Body: body}, m)
					if tc.code == "" {
						if err != nil {
							t.Fatalf("err=%+v body=%s", err, w.Body)
						}
					} else {
						if err == nil || err.Code != tc.code {
							t.Fatalf("err=%+v want=%s body=%s", err, tc.code, w.Body)
						}
						if strings.Contains(w.Body.String(), "[DONE]") {
							t.Fatalf("failure emitted DONE: %s", w.Body)
						}
					}
					if body.closes != 1 {
						t.Fatalf("body closed %d times", body.closes)
					}
					if tc.name == "explicit-error-first" && (c.Writer.Written() || w.Header().Get("Content-Type") != "") {
						t.Fatalf("early failure committed SSE: %v", w.Header())
					}
					if tc.name == "explicit-error-after-content" && strings.Count(w.Body.String(), `"error"`) != 1 {
						t.Fatalf("error frames=%s", w.Body)
					}
				})
			}
		})
	}
}

type streamTestBody struct {
	io.Reader
	readErr, closeErr error
	closes            int
}

func (b *streamTestBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if err == io.EOF && b.readErr != nil {
		return n, b.readErr
	}
	return n, err
}
func (b *streamTestBody) Close() error { b.closes++; return b.closeErr }

func TestExpectedStreamChoices(t *testing.T) {
	for _, tc := range []struct {
		body  string
		count int
	}{
		{`{"prompt":"a"}`, 1}, {`{"prompt":"a","n":2,"best_of":4}`, 2},
		{`{"prompt":["a","b"],"n":2}`, 4}, {`{"prompt":[11,22],"n":2}`, 2},
		{`{"prompt":[[11],[22]],"n":2}`, 4}, {`{"prompt":[]}`, 0}, {`{"prompt":["a",2]}`, 0},
		{`{"prompt":[[1],[]]}`, 0}, {`{"prompt":[1.5]}`, 0}, {`{"prompt":null}`, 0},
	} {
		t.Run(tc.body, func(t *testing.T) {
			var req model.GeneralOpenAIRequest
			if err := json.Unmarshal([]byte(tc.body), &req); err != nil {
				t.Fatal(err)
			}
			if n := ExpectedStreamChoices(&req, relaymode.Completions); n != tc.count {
				t.Fatalf("count=%d want=%d", n, tc.count)
			}
		})
	}
	req := &model.GeneralOpenAIRequest{N: int(^uint(0) >> 1), Prompt: []string{"a", "b"}}
	if ExpectedStreamChoices(req, relaymode.Completions) != 0 {
		t.Fatal("overflow should require DONE")
	}
	if ExpectedStreamChoices(&model.GeneralOpenAIRequest{N: 2}, relaymode.ChatCompletions) != 2 {
		t.Fatal("chat count")
	}
}

func TestFailedStreamUsage(t *testing.T) {
	InitTokenEncoders()
	m := &meta.Meta{PromptTokens: 4, ActualModelName: "gpt-3.5-turbo"}
	for _, tc := range []struct {
		name, text         string
		content            bool
		usage              *model.Usage
		prompt, completion int
		none               bool
	}{
		{name: "empty", none: true}, {name: "text-estimate", text: "hello", content: true, prompt: 4, completion: 1},
		{name: "non-text-estimate", content: true, prompt: 4},
		{name: "reported", usage: &model.Usage{PromptTokens: 7, CompletionTokens: 3, TotalTokens: 10}, prompt: 7, completion: 3},
		{name: "total-only", usage: &model.Usage{TotalTokens: 2}, prompt: 2},
		{name: "zero-is-authoritative", text: "hello", content: true, usage: &model.Usage{}},
		{name: "negative-empty", usage: &model.Usage{PromptTokens: -1}, none: true},
		{name: "negative-estimate", text: "hello", content: true, usage: &model.Usage{PromptTokens: -1}, prompt: 4, completion: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := failedStreamUsage(tc.text, tc.content, tc.usage, m)
			if tc.none {
				if u != nil {
					t.Fatalf("usage=%+v", u)
				}
				return
			}
			if u == nil || u.PromptTokens != tc.prompt || u.CompletionTokens != tc.completion || u.TotalTokens != tc.prompt+tc.completion {
				t.Fatalf("usage=%+v", u)
			}
		})
	}
}

func TestStreamNonTextUsageAndCanceled(t *testing.T) {
	InitTokenEncoders()
	for _, delta := range []string{`{"reasoning_content":"thought"}`, `{"refusal":"no"}`, `{"tool_calls":[{"id":"call-1","function":{"arguments":"{}"}}]}`, `{"function_call":{"arguments":"{}"}}`, `{"role":"assistant"}`} {
		t.Run(delta, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
			m := &meta.Meta{Mode: relaymode.ChatCompletions, ExpectedChoices: 1, PromptTokens: 4, ActualModelName: "gpt-3.5-turbo"}
			body := sse(`{"choices":[{"index":0,"delta":`+delta+`}]}`, streamErrorFrame)
			err, _, u := StreamHandler(c, &http.Response{Body: io.NopCloser(strings.NewReader(body))}, m)
			want := !strings.Contains(delta, `"role"`)
			if err == nil || m.RecordFailedStreamUsage != want || (u != nil) != want {
				t.Fatalf("err=%+v usage=%+v record=%v", err, u, m.RecordFailedStreamUsage)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil).WithContext(ctx)
	m := &meta.Meta{Mode: relaymode.ChatCompletions, ExpectedChoices: 1}
	err, _, u := StreamHandler(c, &http.Response{Body: io.NopCloser(strings.NewReader(sse("[DONE]")))}, m)
	if err == nil || err.Code != "request_canceled" || u != nil || w.Body.Len() != 0 {
		t.Fatalf("err=%+v usage=%+v body=%s", err, u, w.Body)
	}
}

func TestTextRequestCancellationHTTP(t *testing.T) {
	oldClient := client.HTTPClient
	client.HTTPClient = &http.Client{Timeout: 3 * time.Second}
	defer func() { client.HTTPClient = oldClient }()
	for _, mode := range []int{relaymode.ChatCompletions, relaymode.Completions} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode=%d/stream=%v", mode, stream), func(t *testing.T) {
				arrived, upstreamCanceled := make(chan struct{}), make(chan struct{})
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					_, _ = io.Copy(io.Discard, r.Body)
					close(arrived)
					select {
					case <-r.Context().Done():
						close(upstreamCanceled)
					case <-time.After(3 * time.Second):
					}
				}))
				defer server.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{}`)).WithContext(ctx)
				m := &meta.Meta{APIType: apitype.OpenAI, ChannelType: channeltype.OpenAI, Mode: mode, IsStream: stream, BaseURL: server.URL, RequestURLPath: "/v1/chat/completions"}
				a := &Adaptor{}
				a.Init(m)
				done := make(chan error, 1)
				go func() { _, err := a.DoRequest(c, m, strings.NewReader(`{}`)); done <- err }()
				select {
				case <-arrived:
				case <-time.After(time.Second):
					t.Fatal("request did not arrive")
				}
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("err=%v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("request ignored cancellation")
				}
				select {
				case <-upstreamCanceled:
				case <-time.After(time.Second):
					t.Fatal("upstream remained active")
				}
				if calls.Load() != 1 {
					t.Fatal("duplicate upstream request")
				}
			})
		}
	}
}

func TestStreamCancellationWhileReadingHTTP(t *testing.T) {
	oldClient := client.HTTPClient
	client.HTTPClient = &http.Client{Timeout: 3 * time.Second}
	defer func() { client.HTTPClient = oldClient }()
	stopped := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sse(`{"choices":[{"delta":{"content":"hello"}}]}`))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			close(stopped)
		case <-time.After(3 * time.Second):
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := &streamSignalRecorder{ResponseRecorder: httptest.NewRecorder(), flushed: make(chan struct{})}
	c, _ := gin.CreateTestContext(writer)
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{}`)).WithContext(ctx)
	m := &meta.Meta{APIType: apitype.OpenAI, ChannelType: channeltype.OpenAI, Mode: relaymode.ChatCompletions, IsStream: true, ExpectedChoices: 1, PromptTokens: 4, ActualModelName: "gpt-3.5-turbo", BaseURL: server.URL, RequestURLPath: "/v1/chat/completions"}
	a := &Adaptor{}
	a.Init(m)
	resp, err := a.DoRequest(c, m, strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *model.ErrorWithStatusCode, 1)
	go func() { e, _, _ := StreamHandler(c, resp, m); done <- e }()
	select {
	case <-writer.flushed:
	case <-time.After(time.Second):
		t.Fatal("first frame not written")
	}
	cancel()
	select {
	case e := <-done:
		if e == nil || e.Code != "request_canceled" || !m.RecordFailedStreamUsage {
			t.Fatalf("err=%+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not stop")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("upstream did not stop")
	}
}

func TestFailedStreamKeepsLastValidUsage(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
	m := &meta.Meta{Mode: relaymode.ChatCompletions, ExpectedChoices: 1, PromptTokens: 4}
	body := sse(`{"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`, `{"usage":{"prompt_tokens":-1}}`, streamErrorFrame)
	err, _, usage := StreamHandler(c, &http.Response{Body: io.NopCloser(strings.NewReader(body))}, m)
	if err == nil || usage == nil || usage.PromptTokens != 7 || usage.CompletionTokens != 3 || !m.RecordFailedStreamUsage {
		t.Fatalf("err=%+v usage=%+v", err, usage)
	}
}

func TestOtherStreamModesKeepTermination(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/v1/embeddings", nil)
	m := &meta.Meta{Mode: relaymode.Embeddings}
	err, text, usage := StreamHandler(c, &http.Response{Body: io.NopCloser(strings.NewReader(sse(streamErrorFrame)))}, m)
	if err != nil || text != "" || usage != nil || w.Body.String() != sse("[DONE]") {
		t.Fatalf("err=%+v text=%s usage=%+v body=%s", err, text, usage, w.Body)
	}
}

type streamSignalRecorder struct {
	*httptest.ResponseRecorder
	flushed chan struct{}
	once    sync.Once
}

func (w *streamSignalRecorder) Flush() {
	w.ResponseRecorder.Flush()
	w.once.Do(func() { close(w.flushed) })
}

func TestStreamUsagePresence(t *testing.T) {
	InitTokenEncoders()
	for _, tc := range []struct {
		name, raw          string
		prompt, completion int
	}{
		{"empty-object", `{}`, 4, 1},
		{"details-only", `{"completion_tokens_details":{"reasoning_tokens":1}}`, 4, 1},
		{"prompt-only", `{"prompt_tokens":7}`, 4, 1},
		{"completion-only", `{"completion_tokens":7}`, 4, 1},
		{"prompt-and-total", `{"prompt_tokens":7,"total_tokens":9}`, 4, 1},
		{"completion-and-total", `{"completion_tokens":7,"total_tokens":9}`, 4, 1},
		{"inconsistent-total", `{"prompt_tokens":7,"completion_tokens":2,"total_tokens":10}`, 4, 1},
		{"complete-without-total", `{"prompt_tokens":7,"completion_tokens":2}`, 7, 2},
		{"explicit-zero-pair", `{"prompt_tokens":0,"completion_tokens":0}`, 0, 0},
		{"explicit-zero-total", `{"total_tokens":0}`, 0, 0},
		{"reported-total-only", `{"total_tokens":2}`, 2, 0},
		{"reported-complete", `{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}`, 7, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
			m := &meta.Meta{Mode: relaymode.ChatCompletions, ExpectedChoices: 1, PromptTokens: 4, ActualModelName: "gpt-3.5-turbo"}
			body := sse(`{"choices":[{"delta":{"content":"hello"}}]}`, `{"usage":`+tc.raw+`}`, streamErrorFrame)
			err, _, u := StreamHandler(c, &http.Response{Body: io.NopCloser(strings.NewReader(body))}, m)
			if err == nil || u == nil || u.PromptTokens != tc.prompt || u.CompletionTokens != tc.completion || m.RecordFailedStreamUsage != (tc.prompt+tc.completion > 0) {
				t.Fatalf("err=%+v usage=%+v record=%v", err, u, m.RecordFailedStreamUsage)
			}
		})
	}
}

func TestStreamDoneClosesWithoutWaiting(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	defer reader.Close()
	go func() { _, _ = io.WriteString(writer, sse("[DONE]")) }()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
	completed := make(chan *model.ErrorWithStatusCode, 1)
	go func() {
		err, _, _ := StreamHandler(c, &http.Response{Body: reader}, &meta.Meta{Mode: relaymode.ChatCompletions, ExpectedChoices: 1})
		completed <- err
	}()
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		_ = writer.Close()
		<-completed
		t.Fatal("DONE waited for EOF")
	}
}

func TestStreamCancellationDuringClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil).WithContext(ctx)
	body := &cancelOnCloseBody{Reader: strings.NewReader(sse("[DONE]")), cancel: cancel}
	err, _, _ := StreamHandler(c, &http.Response{Body: body}, &meta.Meta{Mode: relaymode.ChatCompletions, ExpectedChoices: 1})
	if err == nil || err.Code != "request_canceled" || w.Body.Len() != 0 {
		t.Fatalf("err=%+v body=%s", err, w.Body)
	}
}

type cancelOnCloseBody struct {
	io.Reader
	cancel context.CancelFunc
}

func (b *cancelOnCloseBody) Close() error { b.cancel(); return nil }
