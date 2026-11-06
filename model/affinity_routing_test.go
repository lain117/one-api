package model

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/relay/affinity"
)

func affinityRoutingConfig(strategy, header string) string {
	return fmt.Sprintf(`{"enabled":true,"rules":[{"enabled":true,"model_pattern":"route","strategy":%q,"options":{"header":%q,"load_aware":false}}]}`, strategy, header)
}

func setupAffinityRoutingModel(t *testing.T, cache bool) {
	t.Helper()
	routingDB(t)
	config.MemoryCacheEnabled = cache
	if err := DB.AutoMigrate(&Option{}); err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := DB.DB()
	sqlDB.SetMaxOpenConns(1)
	config.OptionMapRWMutex.Lock()
	previousMap := config.OptionMap
	config.OptionMap = map[string]string{}
	config.OptionMapRWMutex.Unlock()
	t.Cleanup(func() {
		config.OptionMapRWMutex.Lock()
		config.OptionMap = previousMap
		config.OptionMapRWMutex.Unlock()
		_ = affinity.Configure(previousMap["AffinityConfig"])
	})
}

func affinityRoutingRequest(header, session string) *affinity.Request {
	headers := http.Header{}
	headers.Set(header, session)
	return affinity.NewRequest(context.Background(), 1, "default", "route", headers)
}

func TestAffinityRoutingWithoutRedis(t *testing.T) {
	oldClient, oldEnabled := common.RDB, common.RedisEnabled
	common.RDB, common.RedisEnabled = nil, false
	t.Cleanup(func() { common.RDB, common.RedisEnabled = oldClient, oldEnabled })
	for _, cache := range []bool{false, true} {
		for _, configured := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache=%v/input_policy=%v", cache, configured), func(t *testing.T) {
				setupAffinityRoutingModel(t, cache)
				if err := UpdateOption("AffinityConfig", affinityRoutingConfig(affinity.SessionSticky, "X-Session-ID")); err != nil {
					t.Fatal(err)
				}
				policy := ""
				if configured {
					policy = `{"input_modality":"text","input_length":{"known_range":{"min_tokens":0,"max_tokens":10}}}`
				}
				addRoutingChannel(t, 1, 50, policy)
				addRoutingChannel(t, 2, 10, policy)
				InitChannelCache()
				request := affinityRoutingRequest("X-Session-ID", "no-redis")
				if request != nil {
					t.Fatal("Redis disabled but affinity request was created")
				}
				var calls int32
				input := routingInput(4, "text", "estimated", &calls)
				channel, result, err := SelectChannel(context.Background(), "default", "route", false, input, request)
				if err != nil || channel == nil || channel.Id != 1 || result.ChannelID != 0 || result.Commit != nil {
					t.Fatalf("first channel=%v result=%+v err=%v", channel, result, err)
				}
				for i := 0; i < 8; i++ {
					channel, result, err = SelectChannel(context.Background(), "default", "route", true, input, request)
					if err != nil || channel == nil || result.ChannelID != 0 || result.Commit != nil {
						t.Fatalf("retry channel=%v result=%+v err=%v", channel, result, err)
					}
					if (cache && channel.Id != 2) || (!cache && channel.Id != 1 && channel.Id != 2) {
						t.Fatalf("retry priority changed: cache=%v channel=%d", cache, channel.Id)
					}
				}
				if configured && calls != 1 || !configured && calls != 0 {
					t.Fatalf("count calls=%d policy=%v", calls, configured)
				}
			})
		}
	}
}

func TestAffinityRoutingRealRedis(t *testing.T) {
	client := affinityRoutingRedis(t)
	ctx := context.Background()
	for _, cache := range []bool{false, true} {
		for _, strategy := range []string{affinity.SessionSticky, affinity.ConsistentHash} {
			t.Run(fmt.Sprintf("cache=%v/%s", cache, strategy), func(t *testing.T) {
				setupAffinityRoutingModel(t, cache)
				if err := UpdateOption("AffinityConfig", affinityRoutingConfig(strategy, "X-Session-ID")); err != nil {
					t.Fatal(err)
				}
				addRoutingChannel(t, 1, 40, `{"input_modality":"text","input_length":{"known_range":{"min_tokens":0,"max_tokens":4}}}`)
				addRoutingChannel(t, 2, 30, `{"input_modality":"text","input_length":{"known_range":{"min_tokens":0,"max_tokens":100}}}`)
				addRoutingChannel(t, 3, 20, `{"input_modality":"multimodal","input_length":{"known_range":{"min_tokens":0,"max_tokens":100}}}`)
				addRoutingChannel(t, 4, 10, `{"input_modality":"multimodal","input_length":{"known_range":{"min_tokens":0,"max_tokens":100}}}`)
				InitChannelCache()
				for _, tc := range []struct {
					name, modality string
					tokens         int64
					want           int
				}{{"length_before_priority", "text", 8, 2}, {"modality_before_priority", "multimodal", 8, 3}, {"highest_eligible_priority", "text", 2, 1}} {
					t.Run(tc.name, func(t *testing.T) {
						session, key := affinityRoutingSession(t, client)
						request := affinityRoutingRequest("X-Session-ID", session)
						if request == nil {
							t.Fatal("expected affinity request")
						}
						if strategy == affinity.SessionSticky {
							// 预置低优先级绑定，验证亲和不能扩大已经筛选的候选集合。
							if err := client.Set(ctx, key, "4", time.Minute).Err(); err != nil {
								t.Fatal(err)
							}
						}
						var calls int32
						channel, result, err := SelectChannel(ctx, "default", "route", false, routingInput(tc.tokens, tc.modality, "estimated", &calls), request)
						if err != nil || channel == nil || channel.Id != tc.want || calls != 1 {
							t.Fatalf("channel=%v err=%v calls=%d want=%d", channel, err, calls, tc.want)
						}
						if strategy == affinity.SessionSticky {
							if result.Commit == nil || client.Get(ctx, key).Val() != "4" {
								t.Fatal("selection committed or lost commit callback")
							}
							result.Commit(ctx, channel.Id)
							if got := client.Get(ctx, key).Val(); got != strconv.Itoa(tc.want) {
								t.Fatalf("binding=%q want=%d", got, tc.want)
							}
						} else if result.ChannelID != tc.want || result.Commit != nil {
							t.Fatalf("hash result=%+v", result)
						}
					})
				}
			})
		}
	}

	t.Run("equal_priority_strategy_and_retry", func(t *testing.T) {
		for _, cache := range []bool{false, true} {
			for _, strategy := range []string{affinity.SessionSticky, affinity.ConsistentHash} {
				t.Run(fmt.Sprintf("cache=%v/%s", cache, strategy), func(t *testing.T) {
					setupAffinityRoutingModel(t, cache)
					if err := UpdateOption("AffinityConfig", affinityRoutingConfig(strategy, "X-Session-ID")); err != nil {
						t.Fatal(err)
					}
					addRoutingChannel(t, 1, 10, "")
					addRoutingChannel(t, 2, 10, "")
					addRoutingChannel(t, 3, 5, "")
					InitChannelCache()
					session, key := affinityRoutingSession(t, client)
					request := affinityRoutingRequest("X-Session-ID", session)
					want := 2
					if strategy == affinity.SessionSticky {
						if err := client.Set(ctx, key, "2", time.Minute).Err(); err != nil {
							t.Fatal(err)
						}
					} else {
						want = affinity.Pick(ctx, request, []affinity.Candidate{{ID: 1, Weight: 1}, {ID: 2, Weight: 1}}).ChannelID
					}
					for i := 0; i < 8; i++ {
						channel, result, err := SelectChannel(ctx, "default", "route", false, nil, request)
						if err != nil || channel == nil || channel.Id != want || result.ChannelID != want {
							t.Fatalf("first=%v result=%+v want=%d err=%v", channel, result, want, err)
						}
					}
					if cache {
						want = 3
					} else if strategy == affinity.ConsistentHash {
						want = affinity.Pick(ctx, request, []affinity.Candidate{{ID: 1, Weight: 1}, {ID: 2, Weight: 1}, {ID: 3, Weight: 1}}).ChannelID
					}
					channel, _, err := SelectChannel(ctx, "default", "route", true, nil, request)
					if err != nil || channel == nil || channel.Id != want {
						t.Fatalf("retry=%v want=%d err=%v", channel, want, err)
					}
				})
			}
		}
	})
	t.Run("single_candidate_final_success_commit", func(t *testing.T) {
		for _, cache := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache=%v", cache), func(t *testing.T) {
				setupAffinityRoutingModel(t, cache)
				if err := UpdateOption("AffinityConfig", affinityRoutingConfig(affinity.SessionSticky, "X-Session-ID")); err != nil {
					t.Fatal(err)
				}
				addRoutingChannel(t, 1, 10, "")
				InitChannelCache()
				session, key := affinityRoutingSession(t, client)
				request := affinityRoutingRequest("X-Session-ID", session)
				first, firstResult, err := SelectChannel(ctx, "default", "route", false, nil, request)
				if err != nil || first == nil || first.Id != 1 || firstResult.Commit == nil || client.Exists(ctx, key).Val() != 0 {
					t.Fatalf("first=%v result=%+v err=%v", first, firstResult, err)
				}
				// 首次尝试失败不提交，后续成功只绑定实际使用的渠道。
				if err := DB.Model(&Channel{}).Where("id = ?", 1).Update("status", ChannelStatusManuallyDisabled).Error; err != nil {
					t.Fatal(err)
				}
				if err := DB.Model(&Ability{}).Where("channel_id = ?", 1).Update("enabled", false).Error; err != nil {
					t.Fatal(err)
				}
				addRoutingChannel(t, 2, 10, "")
				InitChannelCache()
				final, result, err := SelectChannel(ctx, "default", "route", false, nil, request)
				if err != nil || final == nil || final.Id != 2 || result.Commit == nil || client.Exists(ctx, key).Val() != 0 {
					t.Fatalf("final=%v result=%+v err=%v", final, result, err)
				}
				result.Commit(ctx, final.Id)
				if got := client.Get(ctx, key).Val(); got != "2" {
					t.Fatalf("final binding=%q", got)
				}
				hit, hitResult, err := SelectChannel(ctx, "default", "route", false, nil, affinityRoutingRequest("X-Session-ID", session))
				if err != nil || hit.Id != 2 || hitResult.ChannelID != 2 || hitResult.Commit == nil {
					t.Fatalf("hit=%v result=%+v err=%v", hit, hitResult, err)
				}
			})
		}
	})
}

func TestAffinityOptionUpdatesRealRedis(t *testing.T) {
	_ = affinityRoutingRedis(t)
	setupAffinityRoutingModel(t, false)
	first := affinityRoutingConfig(affinity.SessionSticky, "X-First-Session")
	if err := UpdateOption("AffinityConfig", first); err != nil {
		t.Fatal(err)
	}
	assertCurrent := func(want string) {
		t.Helper()
		var stored Option
		if err := DB.Where("key = ?", "AffinityConfig").First(&stored).Error; err != nil {
			t.Fatal(err)
		}
		config.OptionMapRWMutex.RLock()
		raw := config.OptionMap["AffinityConfig"]
		config.OptionMapRWMutex.RUnlock()
		if stored.Value != want || raw != want {
			t.Fatalf("DB/map mismatch: db=%q map=%q want=%q", stored.Value, raw, want)
		}
		cfg, err := affinity.ParseConfig(want)
		if err != nil || affinityRoutingRequest(cfg.Rules[0].Options.Header, "option-snapshot") == nil {
			t.Fatalf("snapshot not published: err=%v", err)
		}
	}
	t.Run("invalid_config_preserves_database_and_snapshot", func(t *testing.T) {
		for _, bad := range []string{
			"{", `{"enabled":true,"rules":[{"enabled":true,"strategy":"bad"}]}`,
			`{"enabled":true,"rules":[{"enabled":true,"strategy":"session_sticky","options":{"ttl_seconds":-1}}]}`,
			`{"enabled":true,"unknown":true}`,
		} {
			if err := UpdateOption("AffinityConfig", bad); err == nil {
				t.Fatalf("invalid config accepted: %s", bad)
			}
			assertCurrent(first)
		}
	})
	t.Run("save_failure_does_not_publish", func(t *testing.T) {
		if err := DB.Exec("CREATE TRIGGER affinity_save_failure BEFORE UPDATE ON options BEGIN SELECT RAISE(ABORT, 'isolated_save_failure'); END").Error; err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { DB.Exec("DROP TRIGGER IF EXISTS affinity_save_failure") })
		second := affinityRoutingConfig(affinity.ConsistentHash, "X-Second-Session")
		if err := UpdateOption("AffinityConfig", second); err == nil {
			t.Fatal("save failure was ignored")
		}
		assertCurrent(first)
		if affinityRoutingRequest("X-Second-Session", "option-snapshot") != nil {
			t.Fatal("failed save published new header")
		}
	})
	t.Run("database_refresh_publishes", func(t *testing.T) {
		second := affinityRoutingConfig(affinity.ConsistentHash, "X-Reload-Session")
		if err := DB.Save(&Option{Key: "AffinityConfig", Value: second}).Error; err != nil {
			t.Fatal(err)
		}
		loadOptionsFromDatabase()
		assertCurrent(second)
		if affinityRoutingRequest("X-First-Session", "option-snapshot") != nil {
			t.Fatal("previous header survived refresh")
		}
	})
	t.Run("concurrent_update_and_refresh_finish_consistently", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, 16)
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				value := affinityRoutingConfig(affinity.SessionSticky, fmt.Sprintf("X-Concurrent-%d", i))
				errors <- UpdateOption("AffinityConfig", value)
				loadOptionsFromDatabase()
			}(i)
		}
		wg.Wait()
		close(errors)
		for err := range errors {
			if err != nil {
				t.Fatal(err)
			}
		}
		var stored Option
		if err := DB.Where("key = ?", "AffinityConfig").First(&stored).Error; err != nil {
			t.Fatal(err)
		}
		assertCurrent(stored.Value)
		var cfg affinity.Config
		if err := json.Unmarshal([]byte(stored.Value), &cfg); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 16; i++ {
			header := fmt.Sprintf("X-Concurrent-%d", i)
			got := affinityRoutingRequest(header, "option-snapshot") != nil
			if got != strings.EqualFold(header, cfg.Rules[0].Options.Header) {
				t.Fatalf("snapshot header=%s active=%v stored=%s", header, got, cfg.Rules[0].Options.Header)
			}
		}
	})
}

func affinityRoutingRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	addresses, endpoint := os.Getenv("AFFINITY_TEST_REDIS_ADDRS"), os.Getenv("AFFINITY_TEST_REDIS_URL")
	if addresses == "" && endpoint == "" {
		t.Skip("real test Redis endpoint is required")
	}
	var client redis.UniversalClient
	if addresses != "" {
		addrs := strings.Split(addresses, ",")
		password := os.Getenv("AFFINITY_TEST_REDIS_PASSWORD")
		discovery := redis.NewClient(&redis.Options{Addr: addrs[0], Password: password})
		t.Cleanup(func() { discovery.Close() })
		client = redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs, Password: password, ClusterSlots: func(ctx context.Context) ([]redis.ClusterSlot, error) {
			return affinityRoutingClusterSlots(ctx, discovery)
		}})
	} else {
		opts, err := redis.ParseURL(endpoint)
		if err != nil {
			t.Fatal("invalid test Redis URL")
		}
		client = redis.NewClient(opts)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := client.Ping(ctx).Err()
	cancel()
	if err != nil {
		client.Close()
		t.Fatalf("test Redis unavailable: %v", err)
	}
	oldClient, oldEnabled := common.RDB, common.RedisEnabled
	common.RDB, common.RedisEnabled = client, true
	t.Cleanup(func() {
		common.RDB, common.RedisEnabled = oldClient, oldEnabled
		client.Close()
	})
	return client
}

func affinityRoutingSession(t *testing.T, client redis.UniversalClient) (string, string) {
	t.Helper()
	session := fmt.Sprintf("model-affinity-%d-%s", time.Now().UnixNano(), t.Name())
	h := sha256.New()
	var size [8]byte
	for _, part := range []string{"1", "default", "route", session} {
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(part))
	}
	key := "oneapi:affinity:v1:sticky:" + hex.EncodeToString(h.Sum(nil))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := client.Del(ctx, key).Err(); err != nil {
			t.Errorf("test binding cleanup failed: %v", err)
		}
	})
	return session, key
}

// 槽位发现只读取节点地址，附加元数据不进入测试客户端配置。
func affinityRoutingClusterSlots(ctx context.Context, client *redis.Client) ([]redis.ClusterSlot, error) {
	value, err := client.Do(ctx, "CLUSTER", "SLOTS").Result()
	if err != nil {
		return nil, err
	}
	entries, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected cluster slots response")
	}
	slots := make([]redis.ClusterSlot, 0, len(entries))
	for _, entry := range entries {
		fields, ok := entry.([]interface{})
		if !ok || len(fields) < 3 {
			return nil, fmt.Errorf("invalid cluster slot")
		}
		start, startOK := fields[0].(int64)
		end, endOK := fields[1].(int64)
		master, masterOK := fields[2].([]interface{})
		if !startOK || !endOK || !masterOK || len(master) < 2 {
			return nil, fmt.Errorf("invalid cluster slot range")
		}
		host, hostOK := master[0].(string)
		port, portOK := master[1].(int64)
		if !hostOK || !portOK {
			return nil, fmt.Errorf("invalid cluster slot owner")
		}
		slots = append(slots, redis.ClusterSlot{Start: int(start), End: int(end), Nodes: []redis.ClusterNode{{Addr: net.JoinHostPort(host, strconv.FormatInt(port, 10))}}})
	}
	return slots, nil
}
