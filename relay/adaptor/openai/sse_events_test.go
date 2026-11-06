package openai

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/relay/meta"
	"github.com/songquanpeng/one-api/relay/relaymode"
)

func TestStreamSSEEventFraming(t *testing.T) {
	InitTokenEncoders()
	finish := `{"choices":[{"index":0,"text":"hello","delta":{"content":"hello"},"finish_reason":"stop"}]}`
	content := `{"choices":[{"index":0,"text":"hello","delta":{"content":"hello"},"finish_reason":null}]}`
	for _, mode := range []int{relaymode.ChatCompletions, relaymode.Completions} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			for _, tc := range []struct {
				name, body, code, text string
				usageTotal             int
			}{
				{name: "compact-success", body: "data:" + finish + "\n\ndata:[DONE]\n\n", text: "hello"},
				{name: "compact-error", body: "data:" + streamErrorFrame + "\n\ndata: [DONE]\n\n", code: "fixture_failure"},
				{name: "compact-bad-json", body: "data:{broken\n\ndata: [DONE]\n\n", code: "invalid_stream_response"},
				{name: "multiline-success", body: "data: {\"choices\":\ndata: [{\"index\":0,\"text\":\"hello\",\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\n", text: "hello"},
				{name: "multiline-error", body: "data: {\"error\":\ndata: {\"code\":\"fixture_failure\",\"message\":\"bad\"}}\n\ndata:[DONE]\n\n", code: "fixture_failure"},
				{name: "multiline-broken-json", body: "data: {\"choices\":\ndata: broken}\n\ndata:[DONE]\n\n", code: "invalid_stream_response"},
				{name: "single-space", body: "data: " + finish + "\n\n", text: "hello"},
				{name: "two-spaces", body: "data:  " + finish + "\n\n", text: "hello"},
				{name: "tab-value-json-whitespace", body: "data:\t" + finish + "\n\n", text: "hello"},
				{name: "crlf-without-event-boundary", body: "data:" + finish + "\r\ndata:[DONE]\r\n\r\n", code: "invalid_stream_response"},
				{name: "crlf-events", body: "data:" + finish + "\r\n\r\ndata:[DONE]\r\n\r\n", text: "hello"},
				{name: "cr-success", body: "data:" + finish + "\r\rdata:[DONE]\r\r", text: "hello"},
				{name: "crlf-error", body: "data:" + streamErrorFrame + "\r\n\r\ndata:[DONE]\r\n\r\n", code: "fixture_failure"},
				{name: "cr-error", body: "data:" + streamErrorFrame + "\r\rdata:[DONE]\r\r", code: "fixture_failure"},
				{name: "bom-success", body: "\uFEFFdata:" + finish + "\n\n", text: "hello"},
				{name: "bom-error", body: "\uFEFFdata:" + streamErrorFrame + "\n\ndata:[DONE]\n\n", code: "fixture_failure"},
				{name: "empty-data", body: "data:\n\ndata:[DONE]\n\n", code: "invalid_stream_response"},
				{name: "colonless-empty-data", body: "data\n\ndata:[DONE]\n\n", code: "invalid_stream_response"},
				{name: "done-no-blank-line", body: "data:[DONE]\n", code: "incomplete_stream"},
				{name: "done-no-newline", body: "data:[DONE]", code: "incomplete_stream"},
				{name: "finish-no-blank-line", body: "data:" + finish + "\n", code: "incomplete_stream"},
				{name: "finished-then-unclosed-data", body: "data:" + finish + "\n\ndata:{broken", code: "incomplete_stream", text: "hello", usageTotal: 5},
				{name: "finished-then-unclosed-metadata", body: "data:" + finish + "\n\nid:abc\nevent:message\n: heartbeat", text: "hello"},
				{name: "only-comment", body: ": heartbeat\n\nid:abc\n", code: "incomplete_stream"},
				{name: "metadata-heartbeats", body: ": heartbeat\n\nid:abc\nevent:message\nretry:1000\n\ndata:" + finish + "\n\n", text: "hello"},
				{name: "field-case-sensitive", body: "Data:" + streamErrorFrame + "\n\ndata:" + finish + "\n\n", text: "hello"},
				{name: "unclosed-usage-not-counted", body: "data:" + content + "\n\ndata:{\"usage\":{\"prompt_tokens\":40,\"completion_tokens\":20,\"total_tokens\":60}}\n", code: "incomplete_stream", text: "hello", usageTotal: 5},
				{name: "closed-usage-kept", body: "data:" + content + "\n\ndata:{\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2,\"total_tokens\":6}}\n\ndata:{broken", code: "incomplete_stream", text: "hello", usageTotal: 6},
				{name: "complete-done-ignores-tail", body: "data:" + finish + "\n\ndata:[DONE]\n\ndata:{broken", text: "hello"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					w := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(w)
					c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
					m := &meta.Meta{Mode: mode, ExpectedChoices: 1, PromptTokens: 4, ActualModelName: "gpt-3.5-turbo"}
					err, text, usage := StreamHandler(c, &http.Response{Body: io.NopCloser(strings.NewReader(tc.body))}, m)
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
					if text != tc.text {
						t.Fatalf("text=%q want=%q", text, tc.text)
					}
					if tc.usageTotal != 0 && (usage == nil || usage.TotalTokens != tc.usageTotal) {
						t.Fatalf("usage=%+v want_total=%d", usage, tc.usageTotal)
					}
					if tc.text == "" && tc.code != "" && (usage != nil || m.RecordFailedStreamUsage) {
						t.Fatalf("empty failure billed: %+v", usage)
					}
					if tc.code == "" && tc.text != "" {
						output := newStreamEventScanner(strings.NewReader(w.Body.String()))
						count := 0
						for output.Scan() {
							count++
						}
						if output.Err() != nil || count != 2 {
							t.Fatalf("output events=%d err=%v body=%s", count, output.Err(), w.Body)
						}
					}
				})
			}
		})
	}
}

func TestSSEEventPayloads(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       []string
	}{
		{"optional-space", "data:a\n\ndata: a\n\ndata:  a \n\n", []string{"a", "a", " a "}},
		{"empty-data", "data\n\ndata:\n\ndata:\ndata:\n\n", []string{"", "", "\n"}},
		{"multiline", "data:one\ndata:two\ndata:\n\n", []string{"one\ntwo\n"}},
		{"unknown-fields", ":data:bad\nData:bad\n data:bad\nid:a\nevent:x\nretry:1\n\ndata:ok\n\n", []string{"ok"}},
		{"colon-value", "data:a:b:c\n\n", []string{"a:b:c"}},
		{"leading-bom", "\uFEFFdata:one\r\ndata:two\r\n\r\n", []string{"one\ntwo"}},
		{"bom-only-at-start", "\uFEFFdata:one\n\n\uFEFFdata:ignored\n\ndata:two\n\n", []string{"one", "two"}},
		{"crlf-fragmented", "data:one\r\n\r\ndata:two\r\r", []string{"one", "two"}},
	} {
		for _, fragmented := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fragmented=%v", tc.name, fragmented), func(t *testing.T) {
				var reader io.Reader = strings.NewReader(tc.body)
				if fragmented {
					reader = &oneByteStreamReader{Reader: reader}
				}
				scanner := newStreamEventScanner(reader)
				var got []string
				for scanner.Scan() {
					got = append(got, scanner.Text())
				}
				if scanner.Err() != nil || !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("got=%q want=%q err=%v", got, tc.want, scanner.Err())
				}
			})
		}
	}
}

type oneByteStreamReader struct{ io.Reader }

func (r *oneByteStreamReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}

func TestSSEEventAggregateBound(t *testing.T) {
	s := newStreamEventScanner(strings.NewReader(strings.Repeat("data:{}\n", bufio.MaxScanTokenSize/3+1) + "\n"))
	if s.Scan() || !errors.Is(s.Err(), bufio.ErrTooLong) {
		t.Fatalf("err=%v", s.Err())
	}
}

func TestStreamCROnlyDoneDoesNotWait(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	go func() { _, _ = io.WriteString(writer, "data:[DONE]\r\r") }()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
	done := make(chan error, 1)
	go func() {
		err, _, _ := StreamHandler(c, &http.Response{Body: reader}, &meta.Meta{Mode: relaymode.ChatCompletions, ExpectedChoices: 1})
		if err != nil {
			done <- fmt.Errorf("stream: %+v", err)
		} else {
			done <- nil
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		_ = writer.Close()
		<-done
		t.Fatal("CR-only event waited for another byte")
	}
}
