package middleware

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common/ctxkey"
	"github.com/songquanpeng/one-api/relay/affinity"
)

const affinityCommitKey = "affinity_commit"

func IsAffinityRequest(c *gin.Context) bool {
	return c.Request.Method == http.MethodPost && (c.Request.URL.Path == "/v1/chat/completions" || c.Request.URL.Path == "/v1/completions")
}

func prepareAffinity(c *gin.Context, userID int, group, model string) *affinity.Request {
	if !IsAffinityRequest(c) {
		return nil
	}
	return affinity.NewRequest(c.Request.Context(), userID, group, model, c.Request.Header)
}

func storeAffinityResult(c *gin.Context, result affinity.Result) {
	if result.Commit != nil {
		c.Set(affinityCommitKey, result.Commit)
	}
}

// 以最终成功渠道提交，重试不会提前覆盖绑定。
func CommitAffinity(c *gin.Context) {
	if c.Request.Context().Err() != nil {
		return
	}
	value, exists := c.Get(affinityCommitKey)
	if !exists {
		return
	}
	value.(func(context.Context, int))(c.Request.Context(), c.GetInt(ctxkey.ChannelId))
}
