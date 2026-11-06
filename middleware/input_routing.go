package middleware

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/logger"
	"github.com/songquanpeng/one-api/model"
	"github.com/songquanpeng/one-api/relay/adaptor/openai"
	"github.com/songquanpeng/one-api/relay/lengthrouting"
)

const inputRoutingKey = "oneapi_input_routing"

func prepareInputRouting(c *gin.Context) {
	if c.Request.Method != http.MethodPost {
		return
	}
	path := c.Request.URL.Path
	if path != "/v1/chat/completions" && path != "/v1/completions" {
		return
	}
	input := &lengthrouting.Input{Counter: func() lengthrouting.CountResult {
		if !strings.HasPrefix(c.GetHeader("Content-Type"), "application/json") {
			return lengthrouting.Unknown("unsupported_content_type")
		}
		body, err := common.GetRequestBody(c)
		if err != nil {
			return lengthrouting.Unknown("read_body_failed")
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(body))
		return lengthrouting.CountInput(c.Request.Context(), path, body, openai.CountContentTokens)
	}}
	c.Set(inputRoutingKey, input)
}
func InputRoutingForRequest(c *gin.Context) *lengthrouting.Input {
	value, _ := c.Get(inputRoutingKey)
	input, _ := value.(*lengthrouting.Input)
	return input
}

// 使用上游请求日志与 request ID，不记录正文、媒体 URL 或会话内容。
func RecordInputRouting(c *gin.Context, attempt int, channel *model.Channel, err error) {
	input := InputRoutingForRequest(c)
	if input == nil || input.Result == nil {
		return
	}
	result := "selected"
	if err != nil || channel == nil {
		result = "rejected"
	}
	channelID := 0
	if channel != nil {
		channelID = channel.Id
	}
	count := input.Result
	logger.Infof(c.Request.Context(), "input_routing_decision attempt=%d result=%s channel_id=%d modality=%s kind=%s tokens=%d reason=%s", attempt, result, channelID, count.Modality, count.Kind, count.Tokens, count.Reason)
}
