package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
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

type systemPromptMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type systemPromptObservation struct {
	key      string
	messages []systemPromptMessage
}

func TestSystemPromptChannelRetryHTTP(t *testing.T) {
	if config.AutomaticDisableChannelEnabled || config.EnableMetric {
		t.Skip("retry isolation requires automatic channel disabling and metrics to be disabled")
	}
	openai.InitTokenEncoders()
	if got := openai.CountContentTokens("hello"); got != 1 {
		t.Fatalf("real token count=%d", got)
	}
	prompt := func(s string) *string { return &s }
	for _, cache := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			for _, tc := range []struct {
				name              string
				first, second     *string
				retry, userSystem bool
				wantFirst         string
				wantSecond        string
			}{
				{"retry-nil-restores-user", prompt("channel-A"), nil, true, true, "channel-A", "user-system"},
				{"retry-empty-restores-user", prompt("channel-A"), prompt(""), true, true, "channel-A", "user-system"},
				{"retry-new-prompt", prompt("channel-A"), prompt("channel-B"), true, true, "channel-A", "channel-B"},
				{"retry-no-overrides", nil, nil, true, true, "user-system", "user-system"},
				{"retry-nil-without-user-system", prompt("channel-A"), nil, true, false, "channel-A", ""},
				{"first-configured", prompt("channel-A"), nil, false, true, "channel-A", ""},
				{"first-empty", prompt(""), nil, false, true, "user-system", ""},
				{"first-nil", nil, nil, false, false, "", ""},
			} {
				t.Run(fmt.Sprintf("cache=%v/stream=%v/%s", cache, stream, tc.name), func(t *testing.T) {
					setupInputRetryDB(t, cache)
					config.RetryTimes = 0
					if tc.retry {
						config.RetryTimes = 1
					}
					var mu sync.Mutex
					var attempts []systemPromptObservation
					var fixtureErr error
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var body struct {
							Messages []systemPromptMessage `json:"messages"`
						}
						err := json.NewDecoder(r.Body).Decode(&body)
						key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
						mu.Lock()
						attempts = append(attempts, systemPromptObservation{key: key, messages: body.Messages})
						if err != nil {
							fixtureErr = err
						}
						mu.Unlock()
						status := http.StatusBadGateway
						if key == "test-channel-1" {
							status = http.StatusServiceUnavailable
							if tc.retry {
								// 停用本用例首发渠道，使重试确定选择第二个渠道。
								err = dbmodel.DB.Model(&dbmodel.Channel{}).Where("id = ?", 1).Update("status", dbmodel.ChannelStatusManuallyDisabled).Error
								if err == nil {
									err = dbmodel.DB.Model(&dbmodel.Ability{}).Where("channel_id = ?", 1).Update("enabled", false).Error
								}
								if err != nil {
									mu.Lock()
									fixtureErr = err
									mu.Unlock()
								}
								dbmodel.InitChannelCache()
							}
						}
						// 以明确错误结束，断言请求内容且不启动成功扣费。
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(status)
						_, _ = fmt.Fprint(w, `{"error":{"message":"isolated prompt test","type":"upstream_test_error","code":"prompt_test"}}`)
					}))
					t.Cleanup(upstream.Close)
					addInputRetryChannel(t, 1, 30, "", upstream.URL)
					if err := dbmodel.DB.Model(&dbmodel.Channel{}).Where("id = ?", 1).Update("system_prompt", tc.first).Error; err != nil {
						t.Fatal(err)
					}
					if tc.retry {
						addInputRetryChannel(t, 2, 10, "", upstream.URL)
						if err := dbmodel.DB.Model(&dbmodel.Channel{}).Where("id = ?", 2).Update("system_prompt", tc.second).Error; err != nil {
							t.Fatal(err)
						}
					}
					dbmodel.InitChannelCache()
					transport := &http.Transport{Proxy: nil}
					oldClient := client.HTTPClient
					client.HTTPClient = &http.Client{Timeout: 5 * time.Second, Transport: transport}
					t.Cleanup(func() {
						transport.CloseIdleConnections()
						client.HTTPClient = oldClient
					})
					router := gin.New()
					router.Use(middleware.RequestId())
					router.Use(func(c *gin.Context) {
						// 配额与渠道仅来自本用例SQLite，鉴权上下文使用固定测试值。
						c.Set(ctxkey.Id, 1)
						c.Set(ctxkey.RequestModel, "gpt-3.5-turbo")
						c.Next()
					})
					router.Use(middleware.Distribute())
					router.POST("/v1/chat/completions", Relay)
					messages := []systemPromptMessage{{Role: "user", Content: "hello"}}
					if tc.userSystem {
						messages = append([]systemPromptMessage{{Role: "system", Content: "user-system"}}, messages...)
					}
					body, err := json.Marshal(map[string]any{
						"model": "gpt-3.5-turbo", "messages": messages, "max_tokens": 8, "stream": stream,
					})
					if err != nil {
						t.Fatal(err)
					}
					req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body)))
					req.Header.Set("Content-Type", "application/json")
					response := httptest.NewRecorder()
					router.ServeHTTP(response, req)
					wantStatus, wantAttempts := http.StatusServiceUnavailable, 1
					if tc.retry {
						wantStatus, wantAttempts = http.StatusBadGateway, 2
					}
					if response.Code != wantStatus {
						t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
					}
					mu.Lock()
					observed := append([]systemPromptObservation(nil), attempts...)
					observedErr := fixtureErr
					mu.Unlock()
					if observedErr != nil {
						t.Fatal(observedErr)
					}
					if len(observed) != wantAttempts {
						t.Fatalf("upstream attempts=%+v want=%d", observed, wantAttempts)
					}
					wantPrompts := []string{tc.wantFirst}
					if tc.retry {
						wantPrompts = append(wantPrompts, tc.wantSecond)
					}
					for i, attempt := range observed {
						want := []systemPromptMessage{{Role: "user", Content: "hello"}}
						if wantPrompts[i] != "" {
							want = append([]systemPromptMessage{{Role: "system", Content: wantPrompts[i]}}, want...)
						}
						if attempt.key != fmt.Sprintf("test-channel-%d", i+1) || !reflect.DeepEqual(attempt.messages, want) {
							t.Errorf("attempt=%d key=%s messages=%+v want=%+v", i+1, attempt.key, attempt.messages, want)
						}
					}
					t.Logf("upstream_attempts=%d cache=%v stream=%v", len(observed), cache, stream)
				})
			}
		}
	}
}
