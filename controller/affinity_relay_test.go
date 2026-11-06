package controller

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/client"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/common/ctxkey"
	"github.com/songquanpeng/one-api/common/helper"
	"github.com/songquanpeng/one-api/middleware"
	dbmodel "github.com/songquanpeng/one-api/model"
	"github.com/songquanpeng/one-api/relay/adaptor/openai"
	"github.com/songquanpeng/one-api/relay/affinity"
	"github.com/songquanpeng/one-api/relay/channeltype"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// 使用真实 Redis 和本机 HTTP 上游，验证分桶、重试、流状态与最终绑定的交叉行为。
func TestAffinityRelayHTTP(t *testing.T) {
	rdb := affinityRelayRedis(t)
	openai.InitTokenEncoders()
	for _, cache := range []bool{false, true} {
		for _, path := range []string{"/v1/chat/completions", "/v1/completions"} {
			for _, scenario := range []string{"success", "stream-success", "partial-error", "empty-error-retry", "timeout-retry", "cancel", "length-reject", "no-session", "specified"} {
				t.Run(fmt.Sprintf("cache=%v%s/%s", cache, path, scenario), func(t *testing.T) {
					f := newAffinityRelayFixture(t, rdb, cache)
					ctx := context.Background()
					sticky := `{"enabled":true,"inflight_enabled":true,"rules":[{"enabled":true,"model_pattern":"*","strategy":"session_sticky","options":{}}]}`
					if err := affinity.Configure(sticky); err != nil {
						t.Fatal(err)
					}
					oldBinding := affinity.Pick(ctx, affinity.NewRequest(ctx, f.userID, "default", f.model, http.Header{"X-Session-Id": {f.session}}), []affinity.Candidate{{ID: 2}})
					oldBinding.Commit(ctx, 2)
					if err := rdb.Expire(ctx, f.bindingKey, 15*time.Second).Err(); err != nil {
						t.Fatal(err)
					}
					var mu sync.Mutex
					var attempts []int
					var fixtureErr error
					cancelRequest := func() {}
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						_, _ = io.Copy(io.Discard, r.Body)
						id, _ := strconv.Atoi(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer test-"))
						mu.Lock()
						attempts = append(attempts, id)
						mu.Unlock()
						if n, e := rdb.Get(ctx, f.inflightKey(id, time.Now().Unix()/3600)).Int(); e != nil || n != 1 {
							mu.Lock()
							fixtureErr = fmt.Errorf("active attempt count=%d error=%v", n, e)
							mu.Unlock()
						}
						if id == 1 && (scenario == "empty-error-retry" || scenario == "timeout-retry") {
							// 只禁用夹具渠道，保证重试目标确定且不碰运行中的服务。
							if e := f.db.Model(&dbmodel.Channel{}).Where("id = 1").Update("status", dbmodel.ChannelStatusManuallyDisabled).Error; e != nil {
								t.Error(e)
							}
							if e := f.db.Model(&dbmodel.Ability{}).Where("channel_id = 1").Update("enabled", false).Error; e != nil {
								t.Error(e)
							}
							dbmodel.InitChannelCache()
							if scenario == "timeout-retry" {
								<-r.Context().Done()
								return
							}
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, "data: {\"error\":{\"message\":\"fixture unavailable\"}}\n\n")
							return
						}
						if scenario == "cancel" {
							cancelRequest()
							<-r.Context().Done()
							return
						}
						if scenario == "stream-success" || scenario == "partial-error" || scenario == "empty-error-retry" {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, "data: {\"id\":\"prefill_addr=10.1.2.3:8000\",\"choices\":[{\"index\":0,\"text\":\"hello\",\"delta\":{\"content\":\"hello\"}}]}\n\n")
							if scenario == "partial-error" {
								_, _ = io.WriteString(w, "data: {\"error\":{\"message\":\"fixture failed after content\"}}\n\n")
								return
							}
							_, _ = io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":1,\"total_tokens\":5}}\n\ndata: [DONE]\n\n")
							return
						}
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"prefill_addr=10.1.2.3:8000","object":"chat.completion","choices":[{"index":0,"text":"hello","message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":1,"total_tokens":5}}`)
					}))
					t.Cleanup(upstream.Close)
					policy := `{"sanitize_response_id":true,"input_modality":"text","input_length":{"known_range":{"min_tokens":0,"max_tokens":32}}}`
					if scenario == "length-reject" {
						policy = `{"input_length":{"known_range":{"min_tokens":100}}}`
					}
					for id := 1; id <= 2; id++ {
						priority := int64(30 - id*10)
						mapping := fmt.Sprintf(`{%q:"gpt-3.5-turbo"}`, f.model)
						ch := dbmodel.Channel{Id: id, Name: "fixture", Key: fmt.Sprintf("test-%d", id), Type: channeltype.OpenAI, Status: dbmodel.ChannelStatusEnabled, Group: "default", Models: f.model, Priority: &priority, Config: policy, BaseURL: &upstream.URL, ModelMapping: &mapping}
						if err := ch.Insert(); err != nil {
							t.Fatal(err)
						}
					}
					dbmodel.InitChannelCache()
					router := gin.New()
					router.Use(middleware.RequestId())
					router.Use(func(c *gin.Context) {
						c.Set(ctxkey.Id, f.userID)
						c.Set(ctxkey.TokenId, f.userID)
						c.Set(ctxkey.TokenName, "fixture")
						c.Set(ctxkey.RequestModel, f.model)
						if scenario == "specified" {
							c.Set(ctxkey.SpecificChannelId, "2")
						}
					})
					router.Use(middleware.Distribute())
					router.POST(path, Relay)
					stream := scenario == "stream-success" || scenario == "partial-error" || scenario == "empty-error-retry"
					body := fmt.Sprintf(`{"model":%q,"prompt":"hello","messages":[{"role":"user","content":"hello"}],"stream":%v,"max_tokens":8}`, f.model, stream)
					request := httptest.NewRequest("POST", path, strings.NewReader(body))
					request.Header.Set("Content-Type", "application/json")
					if scenario != "no-session" {
						request.Header.Set("X-Session-ID", f.session)
					}
					if scenario == "timeout-retry" {
						client.HTTPClient.Timeout = 100 * time.Millisecond
					}
					if scenario == "cancel" {
						canceled, cancel := context.WithCancel(ctx)
						cancelRequest = cancel
						defer cancel()
						request = request.WithContext(canceled)
					}
					response := httptest.NewRecorder()
					router.ServeHTTP(response, request)
					wantAttempts, wantBinding, consumed := 1, "1", true
					if scenario == "empty-error-retry" || scenario == "timeout-retry" {
						wantAttempts, wantBinding = 2, "2"
					}
					if scenario == "partial-error" || scenario == "cancel" || scenario == "length-reject" || scenario == "no-session" || scenario == "specified" {
						wantBinding = "2"
					}
					if scenario == "cancel" || scenario == "length-reject" {
						consumed = false
					}
					if scenario == "length-reject" {
						wantAttempts = 0
						if response.Code != 400 {
							t.Fatalf("reject status=%d", response.Code)
						}
					}
					if scenario != "length-reject" && scenario != "cancel" && response.Code != 200 {
						t.Fatalf("status=%d body=%s", response.Code, response.Body)
					}
					mu.Lock()
					actual := append([]int(nil), attempts...)
					e := fixtureErr
					mu.Unlock()
					if e != nil {
						t.Fatal(e)
					}
					firstChannel := 1
					if scenario == "specified" {
						firstChannel = 2
					}
					if len(actual) != wantAttempts || (wantAttempts > 0 && actual[0] != firstChannel) || (wantAttempts == 2 && actual[1] != 2) {
						t.Fatalf("attempts=%v", actual)
					}
					if got := rdb.Get(ctx, f.bindingKey).Val(); got != wantBinding {
						t.Fatalf("binding=%q want=%q", got, wantBinding)
					}
					ttl := rdb.TTL(ctx, f.bindingKey).Val()
					if wantBinding == "1" || wantAttempts == 2 {
						if ttl < 3500*time.Second {
							t.Fatalf("success did not renew ttl=%v", ttl)
						}
					} else if ttl > 15*time.Second {
						t.Fatalf("failure/no-session renewed ttl=%v", ttl)
					}
					for id := 1; id <= 2; id++ {
						if n := rdb.Get(ctx, f.inflightKey(id, time.Now().Unix()/3600)).Val(); n != "" && n != "0" {
							t.Fatalf("residual inflight channel=%d value=%s", id, n)
						}
					}
					if scenario == "partial-error" && (strings.Contains(response.Body.String(), "[DONE]") || !strings.Contains(response.Body.String(), "fixture failed after content")) {
						t.Fatalf("failure tail=%s", response.Body)
					}
					if consumed {
						f.waitSettlement(t, actual[len(actual)-1])
					}
					if path == "/v1/chat/completions" && consumed && strings.Contains(response.Body.String(), "10.1.2.3") {
						t.Fatalf("unsanitized response id=%s", response.Body)
					}
					if response.Header().Get(helper.RequestIdKey) == "" {
						t.Fatal("missing request id")
					}
					t.Logf("attempts=%v binding=%s failure=%v inflight_released=true", actual, wantBinding, scenario == "partial-error" || scenario == "cancel" || scenario == "length-reject")
				})
			}
		}
	}
}

type affinityRelayFixture struct {
	db                         *gorm.DB
	userID                     int
	model, session, bindingKey string
}

func (f *affinityRelayFixture) inflightKey(channel int, bucket int64) string {
	digest := sha256.Sum256([]byte(f.model))
	return fmt.Sprintf("oneapi:affinity:v1:inflight:{%x}:%d:%d", digest, channel, bucket)
}
func (f *affinityRelayFixture) waitSettlement(t *testing.T, channel int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var user dbmodel.User
		var ch dbmodel.Channel
		if err := f.db.First(&user, f.userID).Error; err != nil {
			t.Fatal(err)
		}
		if err := f.db.First(&ch, channel).Error; err != nil {
			t.Fatal(err)
		}
		if user.RequestCount == 1 && ch.UsedQuota > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("settlement did not finish: requests=%d channel_usage=%d", user.RequestCount, ch.UsedQuota)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func newAffinityRelayFixture(t *testing.T, rdb redis.UniversalClient, cache bool) *affinityRelayFixture {
	t.Helper()
	oldDB, oldLogDB, oldRDB, oldClient := dbmodel.DB, dbmodel.LOG_DB, common.RDB, client.HTTPClient
	oldRedis, oldCache, oldSQLite, oldPG := common.RedisEnabled, config.MemoryCacheEnabled, common.UsingSQLite, common.UsingPostgreSQL
	oldBatch, oldLog, oldUsage, oldRetry := config.BatchUpdateEnabled, config.LogConsumeEnabled, config.EnforceIncludeUsage, config.RetryTimes
	if config.EnableMetric || config.AutomaticDisableChannelEnabled {
		t.Fatal("fixture requires notification and metric consumers disabled")
	}
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "affinity.sqlite")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1)
	dbmodel.DB, dbmodel.LOG_DB, common.RDB = db, db, rdb
	common.RedisEnabled, config.MemoryCacheEnabled, common.UsingSQLite, common.UsingPostgreSQL = true, cache, true, false
	config.BatchUpdateEnabled, config.LogConsumeEnabled, config.EnforceIncludeUsage, config.RetryTimes = false, true, false, 2
	transport := &http.Transport{Proxy: nil}
	client.HTTPClient = &http.Client{Timeout: 2 * time.Second, Transport: transport}
	f := &affinityRelayFixture{db: db, userID: 2000000000 + int(time.Now().UnixNano()%100000000), model: fmt.Sprintf("affinity-http-%d", time.Now().UnixNano()), session: "fixture"}
	h := sha256.New()
	var size [8]byte
	for _, part := range []string{strconv.Itoa(f.userID), "default", f.model, f.session} {
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		h.Write(size[:])
		h.Write([]byte(part))
	}
	f.bindingKey = "oneapi:affinity:v1:sticky:" + hex.EncodeToString(h.Sum(nil))
	if err = db.AutoMigrate(&dbmodel.User{}, &dbmodel.Token{}, &dbmodel.Channel{}, &dbmodel.Ability{}, &dbmodel.Log{}); err != nil {
		t.Fatal(err)
	}
	for _, row := range []any{&dbmodel.User{Id: f.userID, Username: f.model, Group: "default", Status: dbmodel.UserStatusEnabled, Quota: 1000000000}, &dbmodel.Token{Id: f.userID, UserId: f.userID, Key: f.model, RemainQuota: 1000000000, Status: dbmodel.TokenStatusEnabled, ExpiredTime: -1}} {
		if err = db.Create(row).Error; err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		keys := []string{f.bindingKey, fmt.Sprintf("user_group:%d", f.userID), fmt.Sprintf("user_quota:%d", f.userID)}
		bucket := time.Now().Unix() / 3600
		for id := 1; id <= 2; id++ {
			for b := bucket - 1; b <= bucket+1; b++ {
				keys = append(keys, f.inflightKey(id, b))
			}
		}
		for _, key := range keys {
			if err := rdb.Del(context.Background(), key).Err(); err != nil {
				t.Error(err)
			}
		}
		_ = affinity.Configure("")
		db.Exec("DELETE FROM abilities")
		db.Exec("DELETE FROM channels")
		dbmodel.InitChannelCache()
		sqlDB.Close()
		transport.CloseIdleConnections()
		dbmodel.DB, dbmodel.LOG_DB, common.RDB, client.HTTPClient = oldDB, oldLogDB, oldRDB, oldClient
		common.RedisEnabled, config.MemoryCacheEnabled, common.UsingSQLite, common.UsingPostgreSQL = oldRedis, oldCache, oldSQLite, oldPG
		config.BatchUpdateEnabled, config.LogConsumeEnabled, config.EnforceIncludeUsage, config.RetryTimes = oldBatch, oldLog, oldUsage, oldRetry
		if oldDB != nil {
			dbmodel.InitChannelCache()
		}
	})
	return f
}

func affinityRelayRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	addresses, url := os.Getenv("AFFINITY_TEST_REDIS_ADDRS"), os.Getenv("AFFINITY_TEST_REDIS_URL")
	if addresses == "" && url == "" {
		t.Skip("real Redis test endpoint is required")
	}
	var rdb redis.UniversalClient
	if addresses != "" {
		addrs := strings.Split(addresses, ",")
		password := os.Getenv("AFFINITY_TEST_REDIS_PASSWORD")
		discovery := redis.NewClient(&redis.Options{Addr: addrs[0], Password: password})
		t.Cleanup(func() { discovery.Close() })
		rdb = redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs, Password: password, ClusterSlots: func(ctx context.Context) ([]redis.ClusterSlot, error) {
			value, err := discovery.Do(ctx, "CLUSTER", "SLOTS").Result()
			if err != nil {
				return nil, err
			}
			entries, ok := value.([]interface{})
			if !ok {
				return nil, fmt.Errorf("invalid slots response")
			}
			var slots []redis.ClusterSlot
			for _, entry := range entries {
				fields := entry.([]interface{})
				master := fields[2].([]interface{})
				addr := net.JoinHostPort(master[0].(string), strconv.FormatInt(master[1].(int64), 10))
				slots = append(slots, redis.ClusterSlot{Start: int(fields[0].(int64)), End: int(fields[1].(int64)), Nodes: []redis.ClusterNode{{Addr: addr}}})
			}
			return slots, nil
		}})
	} else {
		opts, err := redis.ParseURL(url)
		if err != nil {
			t.Fatal("invalid test Redis URL")
		}
		rdb = redis.NewClient(opts)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}
