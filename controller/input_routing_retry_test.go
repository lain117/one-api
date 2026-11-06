package controller

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/client"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/common/ctxkey"
	"github.com/songquanpeng/one-api/middleware"
	dbmodel "github.com/songquanpeng/one-api/model"
	"github.com/songquanpeng/one-api/relay/adaptor/openai"
	"github.com/songquanpeng/one-api/relay/channeltype"
	"github.com/songquanpeng/one-api/relay/lengthrouting"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestInputRoutingRelayRetryHTTP(t *testing.T) {
	// 失败回调是原有异步监控；本测试不启动通知或指标消费者，也不改它们的生命周期。
	if config.AutomaticDisableChannelEnabled || config.EnableMetric {
		t.Skip("retry isolation requires automatic channel disabling and metrics to be disabled")
	}
	openai.InitTokenEncoders()
	if got := openai.CountContentTokens("hello"); got != 1 {
		t.Fatalf("real GPT-3.5 token count=%d", got)
	}
	chat := "{\n \"model\":\"gpt-3.5-turbo\",\"messages\":[{\"role\":\"user\",\"content\":\"hello\"}],\"max_tokens\":8,\"extension\":\"original-body-marker\"\n}"
	completion := ` {"model":"gpt-3.5-turbo","prompt":"hello","max_tokens":8,"extension":"original-body-marker"} `
	image := `{"model":"gpt-3.5-turbo","messages":[{"role":"user","content":[{"type":"text","text":"hello"},{"type":"image_url","image_url":{"url":"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a1l8AAAAASUVORK5CYII=","detail":"low"}}]}],"max_tokens":8,"extension":"original-body-marker"}`
	textPolicy := `{"input_modality":"text","input_length":{"known_range":{"min_tokens":9,"max_tokens":9}}}`
	shortPolicy := `{"input_length":{"known_range":{"min_tokens":0,"max_tokens":8}}}`
	imagePolicy := `{"input_modality":"multimodal","input_length":{"known_range":{"min_tokens":1289,"max_tokens":1289}}}`
	for _, cache := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%v", cache), func(t *testing.T) {
			for _, tc := range []struct {
				name, path, body, policy, blockedPolicy string
				tokens                                  int64
				retry                                   bool
			}{
				{"chat-qualified-retry-excludes-length", "/v1/chat/completions", chat, textPolicy, shortPolicy, 9, true},
				{"completion-qualified-retry-excludes-length", "/v1/completions", completion, textPolicy, shortPolicy, 9, true},
				{"image-qualified-retry-excludes-modality", "/v1/chat/completions", image, imagePolicy, `{"input_modality":"text"}`, 1289, true},
				{"length-exhaustion-keeps-upstream-error", "/v1/chat/completions", chat, textPolicy, shortPolicy, 9, false},
				{"modality-exhaustion-keeps-upstream-error", "/v1/chat/completions", image, imagePolicy, `{"input_modality":"text"}`, 1289, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					setupInputRetryDB(t, cache)
					var mu sync.Mutex
					var attempts []retryUpstreamObservation
					var fixtureErr error
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, err := io.ReadAll(r.Body)
						key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
						mu.Lock()
						attempts = append(attempts, retryUpstreamObservation{key: key, body: string(body), path: r.URL.Path})
						if err != nil {
							fixtureErr = err
						}
						mu.Unlock()
						status, code, message := http.StatusBadGateway, "second_failure", "retry-second-failure"
						if key == "test-channel-1" {
							// 仅改变本用例的 SQLite 候选，令数据库随机重试也有确定的合格备选。
							query := dbmodel.DB.Model(&dbmodel.Channel{}).Where("id = ?", 1)
							if tc.retry {
								err = query.Update("status", dbmodel.ChannelStatusManuallyDisabled).Error
							} else {
								err = query.Update("config", tc.blockedPolicy).Error
							}
							if err != nil {
								mu.Lock()
								fixtureErr = err
								mu.Unlock()
							}
							dbmodel.InitChannelCache()
							status, code, message = http.StatusServiceUnavailable, "first_failure", "retry-first-failure"
						}
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(status)
						_, _ = fmt.Fprintf(w, `{"error":{"message":%q,"type":"upstream_test_error","code":%q}}`, message, code)
					}))
					t.Cleanup(upstream.Close)
					addInputRetryChannel(t, 1, 30, tc.policy, upstream.URL)
					// 此渠道在首发就不合格，且优先级高于真正备选，能直接发现重试遗漏过滤。
					addInputRetryChannel(t, 2, 20, tc.blockedPolicy, upstream.URL)
					if tc.retry {
						addInputRetryChannel(t, 3, 10, tc.policy, upstream.URL)
					}
					dbmodel.InitChannelCache()

					var active *gin.Context
					var input *lengthrouting.Input
					var result *lengthrouting.CountResult
					var counterCalls int32
					var identityErrors int32
					transport := &http.Transport{Proxy: nil}
					oldClient := client.HTTPClient
					client.HTTPClient = &http.Client{Timeout: 5 * time.Second, Transport: retryRoundTripper(func(r *http.Request) (*http.Response, error) {
						if active == nil || middleware.InputRoutingForRequest(active) != input || input.Result != result {
							atomic.AddInt32(&identityErrors, 1)
						}
						return transport.RoundTrip(r)
					})}
					t.Cleanup(func() { transport.CloseIdleConnections(); client.HTTPClient = oldClient })
					var originalReads, readsAtRelayEnd int
					router := gin.New()
					router.Use(middleware.RequestId())
					router.Use(func(c *gin.Context) {
						// 只注入已鉴权上下文，真实用户配额仍从隔离数据库读取。
						c.Set(ctxkey.Id, 1)
						c.Set(ctxkey.RequestModel, "gpt-3.5-turbo")
						tracker := &retryBodyTracker{ReadCloser: c.Request.Body}
						c.Request.Body = tracker
						c.Set("retry_body_tracker", tracker)
						c.Next()
					})
					router.Use(middleware.Distribute())
					router.POST(tc.path, func(c *gin.Context) {
						active = c
						input = middleware.InputRoutingForRequest(c)
						if input == nil || input.Result == nil {
							t.Error("initial dispatch did not resolve input")
							c.AbortWithStatus(500)
							return
						}
						result = input.Result
						if result.Kind != "estimated" || result.Tokens != tc.tokens {
							t.Errorf("initial count=%+v", result)
						}
						input.Counter = func() lengthrouting.CountResult { atomic.AddInt32(&counterCalls, 1); return *result }
						tracker, _ := c.Get("retry_body_tracker")
						originalReads = tracker.(*retryBodyTracker).reads
						Relay(c)
						readsAtRelayEnd = tracker.(*retryBodyTracker).reads
						if middleware.InputRoutingForRequest(c) != input || input.Result != result {
							atomic.AddInt32(&identityErrors, 1)
						}
						cached, err := common.GetRequestBody(c)
						if err != nil || !bytes.Equal(cached, []byte(tc.body)) {
							t.Errorf("cached original body changed: err=%v body=%q", err, cached)
						}
					})
					req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
					req.Header.Set("Content-Type", "application/json")
					response := httptest.NewRecorder()
					router.ServeHTTP(response, req)
					wantStatus, wantCode, wantMessage := http.StatusServiceUnavailable, "first_failure", "retry-first-failure"
					wantKeys := []string{"test-channel-1"}
					if tc.retry {
						wantStatus, wantCode, wantMessage = http.StatusBadGateway, "second_failure", "retry-second-failure"
						wantKeys = append(wantKeys, "test-channel-3")
					}
					if response.Code != wantStatus || !strings.Contains(response.Body.String(), wantCode) || !strings.Contains(response.Body.String(), wantMessage) || strings.Contains(response.Body.String(), "no channel accepts") {
						t.Fatalf("final status=%d body=%s", response.Code, response.Body.String())
					}
					mu.Lock()
					observed := append([]retryUpstreamObservation(nil), attempts...)
					observedErr := fixtureErr
					mu.Unlock()
					if observedErr != nil {
						t.Fatal(observedErr)
					}
					if len(observed) != len(wantKeys) {
						t.Fatalf("upstream attempts=%+v want keys=%v", observed, wantKeys)
					}
					for i, attempt := range observed {
						if attempt.key != wantKeys[i] || attempt.body != tc.body || attempt.path != tc.path {
							t.Errorf("attempt %d=%+v want key=%s exact original body", i+1, attempt, wantKeys[i])
						}
					}
					if atomic.LoadInt32(&counterCalls) != 0 || atomic.LoadInt32(&identityErrors) != 0 || originalReads == 0 || readsAtRelayEnd != originalReads {
						t.Fatalf("count recomputations=%d identity errors=%d original reads=%d final reads=%d", counterCalls, identityErrors, originalReads, readsAtRelayEnd)
					}
					t.Logf("cache=%v status=%d upstream_attempts=%d tokens=%d count_recomputed=%d original_body_preserved=true", cache, response.Code, len(observed), result.Tokens, counterCalls)
				})
			}
		})
	}
}

func setupInputRetryDB(t *testing.T, cache bool) {
	t.Helper()
	oldDB, oldCache, oldRedis := dbmodel.DB, config.MemoryCacheEnabled, common.RedisEnabled
	oldSQLite, oldPostgreSQL := common.UsingSQLite, common.UsingPostgreSQL
	oldRetry, oldIncludeUsage, oldDebug := config.RetryTimes, config.EnforceIncludeUsage, config.DebugEnabled
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "retry.sqlite")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	dbmodel.DB, config.MemoryCacheEnabled, common.RedisEnabled = db, cache, false
	common.UsingSQLite, common.UsingPostgreSQL = true, false
	config.RetryTimes, config.EnforceIncludeUsage, config.DebugEnabled = 2, false, false
	t.Cleanup(func() {
		_ = db.Exec("DELETE FROM abilities").Error
		_ = db.Exec("DELETE FROM channels").Error
		dbmodel.InitChannelCache()
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
		dbmodel.DB, config.MemoryCacheEnabled, common.RedisEnabled = oldDB, oldCache, oldRedis
		common.UsingSQLite, common.UsingPostgreSQL = oldSQLite, oldPostgreSQL
		config.RetryTimes, config.EnforceIncludeUsage, config.DebugEnabled = oldRetry, oldIncludeUsage, oldDebug
		if oldDB != nil {
			dbmodel.InitChannelCache()
		}
	})
	if err := db.AutoMigrate(&dbmodel.Channel{}, &dbmodel.Ability{}, &dbmodel.User{}); err != nil {
		t.Fatal(err)
	}
	// 高余额走原 preConsumedQuota=0 分支；所有模拟上游返回错误，避免成功异步计量。
	if err := db.Create(&dbmodel.User{Id: 1, Username: "retry-test", Group: "default", Status: dbmodel.UserStatusEnabled, Quota: 1 << 50}).Error; err != nil {
		t.Fatal(err)
	}
}

func addInputRetryChannel(t *testing.T, id int, priority int64, policy, baseURL string) {
	t.Helper()
	ch := dbmodel.Channel{Id: id, Type: channeltype.OpenAI, Name: fmt.Sprintf("retry-%d", id), Key: fmt.Sprintf("test-channel-%d", id), Status: dbmodel.ChannelStatusEnabled, Models: "gpt-3.5-turbo", Group: "default", Config: policy, Priority: &priority, BaseURL: &baseURL}
	if err := ch.Insert(); err != nil {
		t.Fatal(err)
	}
}

type retryUpstreamObservation struct{ key, body, path string }
type retryRoundTripper func(*http.Request) (*http.Response, error)

func (r retryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return r(req) }

type retryBodyTracker struct {
	io.ReadCloser
	reads int
}

func (r *retryBodyTracker) Read(p []byte) (int, error) { r.reads++; return r.ReadCloser.Read(p) }
