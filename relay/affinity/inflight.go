package affinity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/logger"
)

const bucketSeconds = int64(3600)
const inflightTTL = 3 * time.Hour

var acquireScript = redis.NewScript(`
redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 1
`)
var releaseScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
 return redis.call('DECR', KEYS[1])
end
return 0
`)

func inflightKey(model string, channelID int, bucket int64) string {
	digest := sha256.Sum256([]byte(model))
	return keyPrefix + "inflight:{" + hex.EncodeToString(digest[:]) + "}:" + strconv.Itoa(channelID) + ":" + strconv.FormatInt(bucket, 10)
}

// Acquire 按实际尝试计数；释放使用原桶且不受后续开关或请求取消影响。
func Acquire(ctx context.Context, model string, channelID int) func() {
	cfg := current.Load()
	if !cfg.Enabled || !cfg.InflightEnabled || !available() || channelID <= 0 || model == "" {
		return func() {}
	}
	ctx = requestContext(ctx)
	key := inflightKey(model, channelID, time.Now().Unix()/bucketSeconds)
	client := common.RDB
	acquireCtx, cancel := context.WithTimeout(ctx, readTimeout)
	err := acquireScript.Run(acquireCtx, client, []string{key}, int64(inflightTTL.Seconds())).Err()
	cancel()
	if err != nil {
		logger.Errorf(ctx, "affinity inflight acquire failed channel_id=%d: %v", channelID, err)
		return func() {}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			releaseCtx, cancel := context.WithTimeout(detachedContext{ctx}, writeTimeout)
			defer cancel()
			if err := releaseScript.Run(releaseCtx, client, []string{key}).Err(); err != nil {
				logger.Errorf(releaseCtx, "affinity inflight release failed channel_id=%d: %v", channelID, err)
			}
		})
	}
}

func loads(ctx context.Context, model string, candidates []Candidate) (map[int]int64, error) {
	bucket := time.Now().Unix() / bucketSeconds
	keys := make([]string, 0, len(candidates)*2)
	for _, candidate := range candidates {
		keys = append(keys, inflightKey(model, candidate.ID, bucket), inflightKey(model, candidate.ID, bucket-1))
	}
	readCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	values, err := common.RDB.MGet(readCtx, keys...).Result()
	if err != nil {
		return nil, err
	}
	if len(values) != len(keys) {
		return nil, fmt.Errorf("affinity inflight value count mismatch")
	}
	result := make(map[int]int64, len(candidates))
	for i, candidate := range candidates {
		count := parseCount(values[2*i]) + parseCount(values[2*i+1])
		if count < 0 {
			count = 0
		}
		result[candidate.ID] = count
	}
	return result, nil
}

func parseCount(value any) int64 {
	text, ok := value.(string)
	if !ok {
		return 0
	}
	count, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0
	}
	return count
}

func imbalanced(ctx context.Context, req *Request, candidates []Candidate, target int) bool {
	cfg := current.Load()
	o := req.rule.Options
	if !cfg.Enabled || !cfg.InflightEnabled || len(candidates) < 2 || o.LoadAware == nil || !*o.LoadAware || !available() {
		return false
	}
	values, err := loads(ctx, req.model, candidates)
	if err != nil {
		logger.Errorf(ctx, "affinity inflight read failed: %v", err)
		return false
	}
	return overloaded(candidates, values, target, o)
}

func overloaded(candidates []Candidate, values map[int]int64, target int, o Options) bool {
	absolute := make([]int64, 0, len(candidates))
	relative := make([]float64, 0, len(candidates))
	for _, candidate := range candidates {
		n := values[candidate.ID]
		absolute = append(absolute, n)
		relative = append(relative, float64(n)/effectiveWeight(candidate.Weight))
	}
	sort.Slice(absolute, func(i, j int) bool { return absolute[i] < absolute[j] })
	sort.Float64s(relative)
	n := len(candidates)
	baselineAbs := absolute[(n-1)/2]
	baselineRel := relative[n/2]
	if n%2 == 0 {
		baselineRel = (relative[n/2-1] + relative[n/2]) / 2
	}
	channel := candidates[target]
	targetLoad := values[channel.ID]
	if targetLoad-baselineAbs <= o.BalanceAbsThreshold {
		return false
	}
	if baselineRel <= 0 {
		return true
	}
	return (float64(targetLoad)/effectiveWeight(channel.Weight))/baselineRel > o.BalanceRelThreshold
}
