// Package affinity 在已有候选集合内选择亲和渠道，并在成功后提交会话绑定。
package affinity

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/logger"
)

const (
	keyPrefix    = "oneapi:affinity:v1:"
	readTimeout  = 200 * time.Millisecond
	writeTimeout = 3 * time.Second
)

type Candidate struct {
	ID     int
	Weight int
}

// Request 保存匹配规则和认证作用域的快照，不保存原始会话值。
type Request struct {
	model string
	hash  string
	rule  Rule
}

type Result struct {
	ChannelID int
	Commit    func(context.Context, int)
}

func available() bool { return common.RedisEnabled && common.RDB != nil }

// NewRequest 只读指定请求头；无规则、缺少会话或存储未启用时不产生亲和上下文。
func NewRequest(ctx context.Context, userID int, group, model string, headers http.Header) *Request {
	cfg := current.Load()
	if !cfg.Enabled || !available() || userID <= 0 || model == "" || (ctx != nil && ctx.Err() != nil) {
		return nil
	}
	for _, rule := range cfg.Rules {
		if !rule.Enabled || !matches(rule.ModelPattern, model) {
			continue
		}
		session := strings.TrimSpace(headers.Get(rule.Options.Header))
		if session == "" {
			return nil
		}
		return &Request{model: model, hash: scopeHash(strconv.Itoa(userID), group, model, session), rule: rule}
	}
	return nil
}

// 长度前缀避免不同作用域字段拼接成相同输入。
func scopeHash(parts ...string) string {
	h := sha256.New()
	var size [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func requestContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// detachedContext 保留请求标识等上下文值，提交与释放使用独立的超时预算。
type detachedContext struct{ context.Context }

func (detachedContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (detachedContext) Done() <-chan struct{}       { return nil }
func (detachedContext) Err() error                  { return nil }

// Pick 只返回候选集合中的渠道；未命中时交回调用方的原选路。
func Pick(ctx context.Context, req *Request, candidates []Candidate) Result {
	if req == nil || len(candidates) == 0 || !current.Load().Enabled || !available() {
		return Result{}
	}
	ctx = requestContext(ctx)
	if ctx.Err() != nil {
		return Result{}
	}
	switch req.rule.Strategy {
	case SessionSticky:
		return pickSticky(ctx, req, candidates)
	case ConsistentHash:
		winner := rendezvous(req.hash, candidates)
		if winner >= 0 {
			channelID := candidates[winner].ID
			if !imbalanced(ctx, req, candidates, winner) {
				logger.Debugf(ctx, "affinity selection strategy=consistent_hash outcome=hit channel_id=%d", channelID)
				return Result{ChannelID: channelID}
			}
			logger.Debugf(ctx, "affinity selection strategy=consistent_hash outcome=fallback reason=imbalanced channel_id=%d", channelID)
		}
	}
	return Result{}
}

func pickSticky(ctx context.Context, req *Request, candidates []Candidate) Result {
	key := keyPrefix + "sticky:" + req.hash
	result := Result{Commit: bindingCommit(key, req.rule.Options.TTLSeconds)}
	readCtx, cancel := context.WithTimeout(ctx, readTimeout)
	value, err := common.RDB.Get(readCtx, key).Result()
	cancel()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			logger.Debugf(ctx, "affinity selection strategy=session_sticky outcome=fallback reason=no_binding")
		} else {
			logger.Errorf(ctx, "affinity selection strategy=session_sticky outcome=fallback reason=read_error: %v", err)
		}
		return result
	}
	bound, err := strconv.Atoi(value)
	if err != nil || bound <= 0 {
		logger.Warnf(ctx, "affinity selection strategy=session_sticky outcome=fallback reason=invalid_binding")
		return result
	}
	for i, candidate := range candidates {
		if candidate.ID != bound {
			continue
		}
		if imbalanced(ctx, req, candidates, i) {
			logger.Debugf(ctx, "affinity selection strategy=session_sticky outcome=fallback reason=imbalanced channel_id=%d", bound)
			return result
		}
		result.ChannelID = bound
		logger.Debugf(ctx, "affinity selection strategy=session_sticky outcome=hit channel_id=%d", bound)
		return result
	}
	logger.Debugf(ctx, "affinity selection strategy=session_sticky outcome=fallback reason=not_in_candidates channel_id=%d", bound)
	return result
}

// 成功写入同时续期；请求执行期间已过期的绑定也会重新建立。
func bindingCommit(key string, ttl int64) func(context.Context, int) {
	var once sync.Once
	return func(ctx context.Context, channelID int) {
		if channelID <= 0 || !current.Load().Enabled || !available() {
			return
		}
		once.Do(func() {
			writeCtx, cancel := context.WithTimeout(detachedContext{requestContext(ctx)}, writeTimeout)
			defer cancel()
			if err := common.RDB.Set(writeCtx, key, strconv.Itoa(channelID), time.Duration(ttl)*time.Second).Err(); err != nil {
				logger.Errorf(writeCtx, "affinity binding write failed channel_id=%d: %v", channelID, err)
				return
			}
			logger.Debugf(writeCtx, "affinity commit strategy=session_sticky outcome=success channel_id=%d", channelID)
		})
	}
}

func effectiveWeight(weight int) float64 {
	if weight <= 0 {
		return 1
	}
	return float64(weight)
}

// rendezvous 使用稳定哈希和有效权重评分，候选排列不会改变结果。
func rendezvous(key string, candidates []Candidate) int {
	best := -1
	bestScore := -1.0
	for i, candidate := range candidates {
		if candidate.ID <= 0 {
			continue
		}
		digest := sha256.Sum256([]byte(key + ":" + strconv.Itoa(candidate.ID)))
		bits := binary.BigEndian.Uint64(digest[:8]) >> 11
		u := (float64(bits) + 1) / (float64(uint64(1)<<53) + 2)
		score := -effectiveWeight(candidate.Weight) / math.Log(u)
		if score > bestScore || (score == bestScore && (best < 0 || candidate.ID < candidates[best].ID)) {
			best, bestScore = i, score
		}
	}
	return best
}
