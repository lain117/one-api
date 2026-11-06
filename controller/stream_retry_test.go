package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common/client"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/common/ctxkey"
	"github.com/songquanpeng/one-api/middleware"
	dbmodel "github.com/songquanpeng/one-api/model"
	"github.com/songquanpeng/one-api/relay/adaptor/openai"
)

func TestStreamRetryStopsAfterWrittenResponse(t *testing.T) {
	if config.AutomaticDisableChannelEnabled || config.EnableMetric {
		t.Skip("requires isolated monitor settings")
	}
	openai.InitTokenEncoders()
	for _, cache := range []bool{false, true} {
		for _, firstError := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache=%v/retry-stream=%v", cache, firstError), func(t *testing.T) {
				setupInputRetryDB(t, cache)
				config.RetryTimes = 3
				var calls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					call := calls.Add(1)
					if firstError && call == 1 {
						_ = dbmodel.DB.Model(&dbmodel.Channel{}).Where("id = ?", 1).Update("status", dbmodel.ChannelStatusManuallyDisabled).Error
						_ = dbmodel.UpdateAbilityStatus(1, false)
						dbmodel.InitChannelCache()
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(503)
						_, _ = io.WriteString(w, `{"error":{"message":"first failure","code":"first_failure"}}`)
						return
					}
					id := 1
					if firstError {
						id = 2
					}
					_ = dbmodel.DB.Model(&dbmodel.Channel{}).Where("id = ?", id).Update("status", dbmodel.ChannelStatusManuallyDisabled).Error
					_ = dbmodel.UpdateAbilityStatus(id, false)
					dbmodel.InitChannelCache()
					w.Header().Set("Content-Type", "text/event-stream")
					// 空角色帧已提交协议，但不产生待结算内容。
					_, _ = io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n")
					w.(http.Flusher).Flush()
					_, _ = io.WriteString(w, "data: {\"error\":{\"message\":\"stream failure\",\"code\":\"stream_failure\"}}\n\n")
				}))
				defer upstream.Close()
				for id := 1; id <= 3; id++ {
					addInputRetryChannel(t, id, int64(40-id*10), `{}`, upstream.URL)
				}
				dbmodel.InitChannelCache()
				oldClient := client.HTTPClient
				client.HTTPClient = &http.Client{Timeout: 2 * time.Second}
				defer func() { client.HTTPClient = oldClient }()
				router := gin.New()
				router.Use(middleware.RequestId())
				router.Use(func(c *gin.Context) { c.Set(ctxkey.Id, 1); c.Set(ctxkey.RequestModel, "gpt-3.5-turbo"); c.Next() })
				router.Use(middleware.Distribute())
				router.POST("/v1/chat/completions", Relay)
				req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"gpt-3.5-turbo","messages":[{"role":"user","content":"hello"}],"stream":true}`))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				want := int32(1)
				if firstError {
					want = 2
				}
				if calls.Load() != want {
					t.Fatalf("calls=%d want=%d body=%s", calls.Load(), want, w.Body)
				}
				if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/event-stream") || strings.Contains(w.Body.String(), "[DONE]") || strings.Count(w.Body.String(), `"error"`) != 1 || !strings.HasSuffix(w.Body.String(), "\n\n") {
					t.Fatalf("status=%d headers=%v body=%s", w.Code, w.Header(), w.Body)
				}
			})
		}
	}
}

func TestStreamEarlyErrorJSONHeaders(t *testing.T) {
	if config.AutomaticDisableChannelEnabled || config.EnableMetric {
		t.Skip("requires isolated monitor settings")
	}
	openai.InitTokenEncoders()
	setupInputRetryDB(t, false)
	config.RetryTimes = 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"error\":{\"message\":\"early failure\",\"code\":\"early_failure\"}}\n\n")
	}))
	defer upstream.Close()
	addInputRetryChannel(t, 1, 10, `{}`, upstream.URL)
	oldClient := client.HTTPClient
	client.HTTPClient = &http.Client{Timeout: 2 * time.Second}
	defer func() { client.HTTPClient = oldClient }()
	router := gin.New()
	router.Use(func(c *gin.Context) { c.Set(ctxkey.Id, 1); c.Set(ctxkey.RequestModel, "gpt-3.5-turbo"); c.Next() })
	router.Use(middleware.Distribute())
	router.POST("/v1/chat/completions", Relay)
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"gpt-3.5-turbo","messages":[{"role":"user","content":"hello"}],"stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 502 || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") || w.Header().Get("Transfer-Encoding") != "" || !strings.Contains(w.Body.String(), "early_failure") {
		t.Fatalf("status=%d headers=%v body=%s", w.Code, w.Header(), w.Body)
	}
}

func TestRetryGuards(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		written, canceled, specific bool
		status                      int
		want                        bool
	}{
		{name: "unwritten-server-error", status: 502, want: true}, {name: "written-server-error", written: true, status: 502},
		{name: "canceled-server-error", canceled: true, status: 502}, {name: "specific-server-error", specific: true, status: 502},
		{name: "bad-request", status: 400}, {name: "rate-limit", status: 429, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil).WithContext(ctx)
			if tc.written {
				c.Writer.WriteHeaderNow()
			}
			if tc.specific {
				c.Set(ctxkey.SpecificChannelId, 1)
			}
			if shouldRetry(c, tc.status) != tc.want {
				t.Fatal("unexpected retry decision")
			}
		})
	}
}
