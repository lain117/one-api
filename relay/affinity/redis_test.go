package affinity

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/songquanpeng/one-api/common"
)

func TestRedisAffinityLifecycle(t *testing.T) {
	endpoint := os.Getenv("AFFINITY_TEST_REDIS_URL")
	addresses := os.Getenv("AFFINITY_TEST_REDIS_ADDRS")
	if endpoint == "" && addresses == "" {
		t.Skip("AFFINITY_TEST_REDIS_URL or AFFINITY_TEST_REDIS_ADDRS is required for real Redis integration")
	}
	var client redis.UniversalClient
	if addresses != "" {
		addressesList := strings.Split(addresses, ",")
		password := os.Getenv("AFFINITY_TEST_REDIS_PASSWORD")
		discovery := redis.NewClient(&redis.Options{Addr: addressesList[0], Password: password})
		t.Cleanup(func() { discovery.Close() })
		client = redis.NewClusterClient(&redis.ClusterOptions{Addrs: addressesList, Password: password, ClusterSlots: func(ctx context.Context) ([]redis.ClusterSlot, error) { return testClusterSlots(ctx, discovery) }})
	} else {
		opts, err := redis.ParseURL(endpoint)
		if err != nil {
			t.Fatal("invalid test Redis URL")
		}
		if password := os.Getenv("AFFINITY_TEST_REDIS_PASSWORD"); password != "" {
			opts.Password = password
		}
		client = redis.NewClient(opts)
	}
	ctx := context.Background()
	pingCtx, cancel := context.WithTimeout(ctx, time.Second)
	err := client.Ping(pingCtx).Err()
	cancel()
	if err != nil {
		client.Close()
		t.Fatalf("test Redis unavailable: %v", err)
	}
	oldClient, oldEnabled, oldConfig := common.RDB, common.RedisEnabled, current.Load()
	common.RDB, common.RedisEnabled = client, true
	unique := fmt.Sprintf("affinity-test-%d", time.Now().UnixNano())
	keys := map[string]bool{}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for key := range keys {
			if err := client.Del(cleanupCtx, key).Err(); err != nil {
				t.Errorf("remove test key failed: %v", err)
			}
		}
		common.RDB, common.RedisEnabled = oldClient, oldEnabled
		current.Store(oldConfig)
		client.Close()
	})
	configure := func(raw string) {
		t.Helper()
		if err := Configure(raw); err != nil {
			t.Fatal(err)
		}
	}
	request := func(session string) *Request {
		t.Helper()
		req := NewRequest(ctx, 1, unique, unique, http.Header{"X-Session-Id": []string{session}})
		if req == nil {
			t.Fatal("expected request")
		}
		keys[keyPrefix+"sticky:"+req.hash] = true
		return req
	}
	bindingKey := func(req *Request) string { return keyPrefix + "sticky:" + req.hash }
	expectBinding := func(req *Request, want string) {
		t.Helper()
		got, err := client.Get(ctx, bindingKey(req)).Result()
		if err != nil || got != want {
			t.Fatalf("binding = %q %v, want %q", got, err, want)
		}
	}
	registerInflight := func(model string, ids ...int) {
		bucket := time.Now().Unix() / bucketSeconds
		for _, id := range ids {
			for _, b := range []int64{bucket - 1, bucket, bucket + 1} {
				keys[inflightKey(model, id, b)] = true
			}
		}
	}

	t.Run("sticky_first_success_and_single_candidate", func(t *testing.T) {
		configure(stickyConfig)
		req := request("first")
		selected := Pick(ctx, req, []Candidate{{ID: 7}})
		if selected.ChannelID != 0 || selected.Commit == nil {
			t.Fatalf("miss = %+v", selected)
		}
		if client.Exists(ctx, bindingKey(req)).Val() != 0 {
			t.Fatal("selection wrote binding")
		}
		selected.Commit(ctx, 7)
		expectBinding(req, "7")
		if ttl := client.TTL(ctx, bindingKey(req)).Val(); ttl < 3595*time.Second || ttl > 3600*time.Second {
			t.Fatalf("ttl=%s", ttl)
		}
		selected.Commit(ctx, 8)
		expectBinding(req, "7")
		hit := Pick(ctx, req, []Candidate{{ID: 7}})
		if hit.ChannelID != 7 || hit.Commit == nil {
			t.Fatal("single candidate did not hit/commit")
		}
	})
	t.Run("scope_and_missing_header", func(t *testing.T) {
		configure(stickyConfig)
		first := request("scope")
		for _, tc := range []struct {
			user         int
			group, model string
		}{{2, unique, unique}, {1, unique + "-g", unique}, {1, unique, unique + "-m"}} {
			req := NewRequest(ctx, tc.user, tc.group, tc.model, http.Header{"X-Session-Id": []string{"scope"}})
			if req == nil || req.hash == first.hash {
				t.Fatal("scope not isolated")
			}
		}
		if NewRequest(ctx, 1, unique, unique, nil) != nil {
			t.Fatal("missing header accepted")
		}
		if NewRequest(ctx, 1, unique, unique, http.Header{"X-Session-Id": []string{" "}}) != nil {
			t.Fatal("blank header accepted")
		}
		if NewRequest(ctx, 0, unique, unique, http.Header{"X-Session-Id": []string{"s"}}) != nil {
			t.Fatal("unauthenticated request accepted")
		}
		configure(`{"enabled":true,"rules":[{"enabled":true,"model_pattern":"*","strategy":"session_sticky","options":{"header":"x-custom"}}]}`)
		headers := http.Header{}
		headers.Set("X-Custom", " custom ")
		if NewRequest(ctx, 1, unique, unique, headers) == nil {
			t.Fatal("custom header ignored")
		}
		if NewRequest(ctx, 1, unique, unique, http.Header{"X-Session-Id": []string{"s"}}) != nil {
			t.Fatal("default header leaked into custom rule")
		}
	})
	t.Run("ttl_refresh_expiry_and_retry_commit", func(t *testing.T) {
		configure(stickyConfig)
		req := request("expiry")
		client.Set(ctx, bindingKey(req), "7", time.Minute)
		hit := Pick(ctx, req, []Candidate{{ID: 7}, {ID: 8}})
		client.PExpire(ctx, bindingKey(req), time.Millisecond)
		time.Sleep(5 * time.Millisecond)
		if client.Exists(ctx, bindingKey(req)).Val() != 0 {
			t.Fatal("test expiry did not occur")
		}
		hit.Commit(ctx, 7)
		expectBinding(req, "7")
		retry := Pick(ctx, req, []Candidate{{ID: 7}, {ID: 8}})
		retry.Commit(ctx, 8)
		expectBinding(req, "8")
		if next := Pick(ctx, req, []Candidate{{ID: 7}, {ID: 8}}); next.ChannelID != 8 {
			t.Fatal("retry binding did not follow actual channel")
		}
	})
	t.Run("candidate_migration_and_failure_does_not_write", func(t *testing.T) {
		configure(stickyConfig)
		req := request("migration")
		client.Set(ctx, bindingKey(req), "7", time.Minute)
		miss := Pick(ctx, req, []Candidate{{ID: 8}})
		if miss.ChannelID != 0 || miss.Commit == nil {
			t.Fatal("out-of-candidate binding selected")
		}
		expectBinding(req, "7")
		miss.Commit(ctx, 8)
		expectBinding(req, "8")
		disabled := Pick(ctx, req, []Candidate{{ID: 8}})
		configure("")
		disabled.Commit(ctx, 9)
		expectBinding(req, "8")
	})
	t.Run("invalid_binding_recovers_on_success", func(t *testing.T) {
		configure(stickyConfig)
		for i, value := range []string{"bad", "0", "-1"} {
			req := request("invalid" + strconv.Itoa(i))
			client.Set(ctx, bindingKey(req), value, time.Minute)
			result := Pick(ctx, req, []Candidate{{ID: 7}})
			if result.ChannelID != 0 || result.Commit == nil {
				t.Fatal("invalid binding selected")
			}
			result.Commit(ctx, 7)
			expectBinding(req, "7")
		}
		req := request("wrongtype")
		client.LPush(ctx, bindingKey(req), "value")
		result := Pick(ctx, req, []Candidate{{ID: 7}})
		if result.ChannelID != 0 || result.Commit == nil {
			t.Fatal("read failure did not fall back")
		}
		result.Commit(ctx, 7)
		expectBinding(req, "7")
	})
	t.Run("no_redis_disables_all_strategies", func(t *testing.T) {
		configure(`{"enabled":true,"inflight_enabled":true,"rules":[{"enabled":true,"strategy":"consistent_hash","options":{"load_aware":false}}]}`)
		req := request("hash")
		if Pick(ctx, req, []Candidate{{ID: 7}}).ChannelID != 7 {
			t.Fatal("hash unavailable")
		}
		common.RedisEnabled = false
		defer func() { common.RedisEnabled = true }()
		if NewRequest(ctx, 1, unique, unique, http.Header{"X-Session-Id": []string{"s"}}) != nil {
			t.Fatal("no Redis request")
		}
		if result := Pick(ctx, req, []Candidate{{ID: 7}}); result.ChannelID != 0 || result.Commit != nil {
			t.Fatal("hash ignored Redis gate")
		}
		Acquire(ctx, unique, 7)()
	})
	t.Run("inflight_release_cancel_disable_and_eviction", func(t *testing.T) {
		configure(`{"enabled":true,"inflight_enabled":true}`)
		model := unique + "-release"
		registerInflight(model, 7)
		bucket := time.Now().Unix() / bucketSeconds
		key := inflightKey(model, 7, bucket)
		cancelled, cancel := context.WithCancel(ctx)
		release := Acquire(cancelled, model, 7)
		if client.Get(ctx, key).Val() != "1" {
			t.Fatal("acquire without session did not count")
		}
		cancel()
		configure("")
		release()
		release()
		if client.Get(ctx, key).Val() != "0" {
			t.Fatal("cancel/disable leaked or double released")
		}
		configure(`{"enabled":true,"inflight_enabled":true}`)
		release = Acquire(ctx, model, 7)
		client.Del(ctx, key)
		release()
		if client.Exists(ctx, key).Val() != 0 {
			t.Fatal("release recreated evicted key")
		}
		cancelled, cancel = context.WithCancel(ctx)
		cancel()
		Acquire(cancelled, model, 7)()
		if client.Exists(ctx, key).Val() != 0 {
			t.Fatal("cancelled acquire wrote key")
		}
	})
	t.Run("inflight_buckets_and_load_fallback", func(t *testing.T) {
		configure(`{"enabled":true,"inflight_enabled":true,"rules":[{"enabled":true,"strategy":"session_sticky"}]}`)
		req := request("load")
		registerInflight(unique, 7, 8)
		bucket := time.Now().Unix() / bucketSeconds
		client.Set(ctx, bindingKey(req), "7", time.Minute)
		client.Set(ctx, inflightKey(unique, 7, bucket), "20", time.Minute)
		client.Set(ctx, inflightKey(unique, 7, bucket-1), "20", time.Minute)
		client.Set(ctx, inflightKey(unique, 8, bucket), "-5", time.Minute)
		values, err := loads(ctx, unique, []Candidate{{ID: 7}, {ID: 8}})
		if err != nil || values[7] != 40 || values[8] != 0 {
			t.Fatalf("load values=%v err=%v", values, err)
		}
		result := Pick(ctx, req, []Candidate{{ID: 7}, {ID: 8}})
		if result.ChannelID != 0 || result.Commit == nil {
			t.Fatal("overloaded sticky did not fall back")
		}
		result.Commit(ctx, 8)
		expectBinding(req, "8")
		first := inflightKey("model{special}", 7, bucket)
		second := inflightKey("model{special}", 8, bucket-1)
		tag := func(s string) string { return strings.Split(strings.Split(s, "{")[1], "}")[0] }
		if tag(first) != tag(second) || strings.Contains(tag(first), "special") {
			t.Fatal("unsafe cluster slot tags")
		}
	})
	t.Run("redis_connection_failure_is_bounded", func(t *testing.T) {
		configure(stickyConfig)
		req := request("unavailable")
		unavailable := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 20 * time.Millisecond, ReadTimeout: 20 * time.Millisecond, WriteTimeout: 20 * time.Millisecond, MaxRetries: -1})
		common.RDB = unavailable
		defer func() { common.RDB = client; unavailable.Close() }()
		start := time.Now()
		result := Pick(ctx, req, []Candidate{{ID: 7}})
		if result.ChannelID != 0 {
			t.Fatal("failed read selected a binding")
		}
		if time.Since(start) > time.Second {
			t.Fatal("failed read exceeded budget")
		}
	})
}

// 槽位发现只读取公开位置字段，兼容节点返回的附加元数据。
func testClusterSlots(ctx context.Context, client *redis.Client) ([]redis.ClusterSlot, error) {
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
