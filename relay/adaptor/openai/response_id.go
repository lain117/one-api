package openai

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common/ctxkey"
	dbmodel "github.com/songquanpeng/one-api/model"
)

func responseIDSanitizationEnabled(c *gin.Context) bool {
	value, _ := c.Get(ctxkey.Config)
	cfg, ok := value.(dbmodel.ChannelConfig)
	return ok && cfg.SanitizeResponseID
}

// sanitizeChatResponseID 仅清理顶层字符串 ID 中的路由地址，未命中时保留原始正文。
func sanitizeChatResponseID(body []byte) ([]byte, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		// 格式错误继续交给既有响应解析，不改变 SSE 的透传和错误语义。
		return body, false
	}
	var value string
	if json.Unmarshal(fields["id"], &value) != nil ||
		(!strings.Contains(value, "prefill_addr") && !strings.Contains(value, "decode_addr")) {
		return body, false
	}
	suffix := value[strings.LastIndexByte(value, '_')+1:]
	validSuffix := false
	if len(suffix) == 32 || len(suffix) == 64 {
		_, err := hex.DecodeString(suffix)
		validSuffix = err == nil
	}
	if !validSuffix {
		// 异常地址格式使用确定性摘要，保证同一 ID 在不同流帧中的结果一致。
		digest := sha256.Sum256([]byte(value))
		suffix = hex.EncodeToString(digest[:])
	}
	fields["id"], _ = json.Marshal(suffix)
	// RawMessage 保留未知字段及数字精度，避免借助完整响应结构体丢失扩展字段。
	cleaned, err := json.Marshal(fields)
	if err != nil {
		return body, false
	}
	return cleaned, true
}
