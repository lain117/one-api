package middleware

import (
	"bytes"
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
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/common/ctxkey"
	"github.com/songquanpeng/one-api/common/helper"
	"github.com/songquanpeng/one-api/model"
	"github.com/songquanpeng/one-api/relay/adaptor/openai"
	"github.com/songquanpeng/one-api/relay/channeltype"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestInputRoutingDistributeHTTP(t *testing.T) {
	openai.InitTokenEncoders()
	if got := openai.CountContentTokens("hello"); got != 1 {
		t.Fatalf("GPT-3.5 hello token count = %d, want 1", got)
	}
	chat := "{\n \"model\":\"route\", \"messages\":[{\"role\":\"user\",\"content\":\"hello\"}], \"max_tokens\":8, \"extension\":\"must-not-log-content\"\n}"
	completion := ` {"model":"route", "prompt":"hello", "max_tokens":8,"extension":"must-not-log-content"} `
	image := `{"model":"route","messages":[{"role":"user","content":[{"type":"text","text":"hello"},{"type":"image_url","image_url":{"url":"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a1l8AAAAASUVORK5CYII="}}]}],"max_tokens":8,"extension":"must-not-log-content"}`
	for _, cache := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%v", cache), func(t *testing.T) {
			for _, tc := range []struct {
				name, path, body, policy, specified, contentType string
				noChannel                                        bool
				status                                           int
				counted                                          bool
				tokens                                           int64
				modality, message                                string
			}{
				{name: "chat-selected", body: chat, policy: `{"input_modality":"text","input_length":{"known_range":{"min_tokens":9,"max_tokens":9}}}`, status: 200, counted: true, tokens: 9, modality: "text"},
				{name: "completions-selected", path: "/v1/completions", body: completion, policy: `{"input_length":{"known_range":{"min_tokens":9,"max_tokens":9}}}`, status: 200, counted: true, tokens: 9, modality: "text"},
				{name: "image-selected", body: image, policy: `{"input_modality":"multimodal","input_length":{"known_range":{"min_tokens":1289,"max_tokens":1289}}}`, status: 200, counted: true, tokens: 1289, modality: "multimodal"},
				{name: "length-rejected", body: chat, policy: `{"input_length":{"known_range":{"min_tokens":0,"max_tokens":8}}}`, status: 400, counted: true, tokens: 9, modality: "text", message: "no channel accepts this input length"},
				{name: "modality-rejected", body: image, policy: `{"input_modality":"text"}`, status: 400, counted: true, tokens: 1289, modality: "multimodal", message: "no channel accepts this input modality"},
				{name: "no-candidate", body: chat, noChannel: true, status: 503, message: "无可用渠道"},
				{name: "legacy-no-policy", body: chat, policy: `{"api_version":"legacy"}`, contentType: "application/x-test+json; charset=utf-8", status: 200},
				{name: "specified-length-exemption", body: chat, policy: `{"input_modality":"text","input_length":{"known_range":{"min_tokens":0,"max_tokens":8}}}`, specified: "1", status: 200, counted: true, tokens: 9, modality: "text"},
				{name: "specified-modality-rejected", body: image, policy: `{"input_modality":"text"}`, specified: "1", status: 400, counted: true, tokens: 1289, modality: "multimodal", message: "no channel accepts this input modality"},
				{name: "specified-invalid-id", body: chat, specified: "invalid", status: 400, message: "无效的渠道 Id"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if tc.path == "" {
						tc.path = "/v1/chat/completions"
					}
					if tc.contentType == "" {
						tc.contentType = "application/json; charset=utf-8"
					}
					logs := &inputRoutingLogBuffer{}
					setupInputRoutingHTTPDB(t, cache, logs)
					var upstreamCalls int32
					received := make(chan inputRoutingUpstreamRequest, 1)
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						atomic.AddInt32(&upstreamCalls, 1)
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Errorf("read upstream body: %v", err)
						}
						received <- inputRoutingUpstreamRequest{body: string(body), contentType: r.Header.Get("Content-Type"), requestID: r.Header.Get(helper.RequestIdKey), path: r.URL.Path}
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"ok":true}`)
					}))
					defer upstream.Close()
					if !tc.noChannel {
						priority := int64(10)
						channel := model.Channel{Id: 1, Type: channeltype.OpenAI, Name: "test-channel", Key: "test-key", Status: model.ChannelStatusEnabled, Models: "route", Group: "default", Config: tc.policy, Priority: &priority, BaseURL: &upstream.URL}
						if err := channel.Insert(); err != nil {
							t.Fatal(err)
						}
					}
					model.InitChannelCache()
					observations := make(chan inputRoutingHTTPObservation, 1)
					router := gin.New()
					router.Use(RequestId())
					// 在访问日志完成之后发送快照，避免 HTTP 写出和日志断言的时序竞争。
					router.Use(func(c *gin.Context) {
						c.Next()
						observation := inputRoutingHTTPObservation{channel: c.GetInt(ctxkey.ChannelId), readsBeforeForward: c.GetInt("test_reads_before_forward")}
						if input := InputRoutingForRequest(c); input != nil && input.Result != nil {
							observation.counted, observation.tokens, observation.modality = true, input.Result.Tokens, input.Result.Modality
						}
						observations <- observation
					})
					SetUpLogger(router)
					router.Use(func(c *gin.Context) {
						// 此夹具只注入鉴权后上下文，不模拟真实鉴权或配额链。
						c.Set(ctxkey.Id, 1)
						c.Set(ctxkey.RequestModel, "route")
						if tc.specified != "" {
							c.Set(ctxkey.SpecificChannelId, tc.specified)
						}
						tracker := &inputRoutingBodyTracker{ReadCloser: c.Request.Body}
						c.Request.Body = tracker
						c.Set("test_body_tracker", tracker)
						c.Next()
					})
					router.Use(Distribute())
					router.POST(tc.path, func(c *gin.Context) {
						tracker, _ := c.Get("test_body_tracker")
						c.Set("test_reads_before_forward", tracker.(*inputRoutingBodyTracker).reads)
						// 只按分发结果转发原始正文，隔离测试不接入计费、重试或响应适配。
						req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, c.GetString(ctxkey.BaseURL)+c.Request.URL.Path, c.Request.Body)
						if err != nil {
							t.Errorf("create upstream request: %v", err)
							c.AbortWithStatus(502)
							return
						}
						req.Header = c.Request.Header.Clone()
						req.Header.Set(helper.RequestIdKey, helper.GetRequestID(c.Request.Context()))
						resp, err := upstream.Client().Do(req)
						if err != nil {
							t.Errorf("forward request: %v", err)
							c.AbortWithStatus(502)
							return
						}
						defer resp.Body.Close()
						c.Status(resp.StatusCode)
						if _, err := io.Copy(c.Writer, resp.Body); err != nil {
							t.Errorf("copy upstream body: %v", err)
						}
					})
					gateway := httptest.NewServer(router)
					defer gateway.Close()
					client := gateway.Client()
					client.Timeout = 5 * time.Second
					req, err := http.NewRequest(http.MethodPost, gateway.URL+tc.path, strings.NewReader(tc.body))
					if err != nil {
						t.Fatal(err)
					}
					req.Header.Set("Content-Type", tc.contentType)
					resp, err := client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					body, readErr := io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					if readErr != nil {
						t.Fatal(readErr)
					}
					observation := <-observations
					requestID := resp.Header.Get(helper.RequestIdKey)
					if resp.StatusCode != tc.status || requestID == "" {
						t.Fatalf("status=%d want=%d requestID=%q body=%s", resp.StatusCode, tc.status, requestID, body)
					}
					if observation.counted != tc.counted || (tc.counted && (observation.tokens != tc.tokens || observation.modality != tc.modality)) {
						t.Fatalf("routing observation=%+v, want counted=%v tokens=%d modality=%s", observation, tc.counted, tc.tokens, tc.modality)
					}
					if tc.status == 200 {
						if atomic.LoadInt32(&upstreamCalls) != 1 || observation.channel != 1 {
							t.Fatalf("upstreamCalls=%d channel=%d", upstreamCalls, observation.channel)
						}
						forwarded := <-received
						if forwarded.body != tc.body || forwarded.contentType != tc.contentType || forwarded.requestID != requestID || forwarded.path != tc.path {
							t.Fatalf("forwarding contract changed: %+v", forwarded)
						}
						if !tc.counted && observation.readsBeforeForward != 0 {
							t.Fatalf("legacy body was read before forwarding: %+v", observation)
						}
					} else {
						if atomic.LoadInt32(&upstreamCalls) != 0 || !strings.Contains(string(body), tc.message) {
							t.Fatalf("rejection body=%s upstreamCalls=%d", body, upstreamCalls)
						}
					}
					logText := logs.String()
					if !strings.Contains(logText, "[GIN]") || !strings.Contains(logText, requestID) || strings.Contains(logText, "must-not-log-content") {
						t.Fatalf("request log correlation or privacy failed: %s", logText)
					}
					decisionFound := false
					for _, line := range strings.Split(logText, "\n") {
						if strings.Contains(line, "input_routing_decision") {
							decisionFound = true
							if !strings.Contains(line, requestID) || !strings.Contains(line, fmt.Sprintf("tokens=%d", tc.tokens)) || !strings.Contains(line, "attempt=1") {
								t.Fatalf("decision is not correlated: %s", line)
							}
						}
					}
					if decisionFound != tc.counted {
						t.Fatalf("decision logged=%v, counted=%v", decisionFound, tc.counted)
					}
					t.Logf("cache=%v status=%d channel=%d counted=%v tokens=%d request_id=%s", cache, resp.StatusCode, observation.channel, observation.counted, observation.tokens, requestID)
				})
			}
		})
	}
}

func setupInputRoutingHTTPDB(t *testing.T, cache bool, logs *inputRoutingLogBuffer) {
	t.Helper()
	oldDB, oldCache, oldRedis := model.DB, config.MemoryCacheEnabled, common.RedisEnabled
	oldSQLite, oldPostgreSQL := common.UsingSQLite, common.UsingPostgreSQL
	oldWriter, oldErrorWriter := gin.DefaultWriter, gin.DefaultErrorWriter
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:middleware-routing-%d?mode=memory&cache=shared", time.Now().UnixNano())), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	model.DB, config.MemoryCacheEnabled, common.RedisEnabled = db, cache, false
	common.UsingSQLite, common.UsingPostgreSQL = true, false
	gin.DefaultWriter, gin.DefaultErrorWriter = logs, logs
	t.Cleanup(func() {
		// 先清掉测试缓存再恢复全局 DB，防止后续测试拿到已关闭的测试渠道。
		_ = db.Exec("DELETE FROM abilities").Error
		_ = db.Exec("DELETE FROM channels").Error
		model.InitChannelCache()
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
		model.DB, config.MemoryCacheEnabled, common.RedisEnabled = oldDB, oldCache, oldRedis
		common.UsingSQLite, common.UsingPostgreSQL = oldSQLite, oldPostgreSQL
		if oldDB != nil {
			model.InitChannelCache()
		}
		gin.DefaultWriter, gin.DefaultErrorWriter = oldWriter, oldErrorWriter
	})
	if err := db.AutoMigrate(&model.Channel{}, &model.Ability{}, &model.User{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&model.User{Id: 1, Username: "routing-test", Group: "default", Status: model.UserStatusEnabled}).Error; err != nil {
		t.Fatal(err)
	}
}

type inputRoutingHTTPObservation struct {
	channel, readsBeforeForward int
	counted                     bool
	tokens                      int64
	modality                    string
}
type inputRoutingUpstreamRequest struct{ body, contentType, requestID, path string }
type inputRoutingBodyTracker struct {
	io.ReadCloser
	reads int
}

func (r *inputRoutingBodyTracker) Read(p []byte) (int, error) { r.reads++; return r.ReadCloser.Read(p) }

type inputRoutingLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *inputRoutingLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}
func (b *inputRoutingLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}
