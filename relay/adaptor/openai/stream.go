package openai

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/conv"
	"github.com/songquanpeng/one-api/common/logger"
	"github.com/songquanpeng/one-api/common/render"
	"github.com/songquanpeng/one-api/relay/meta"
	"github.com/songquanpeng/one-api/relay/model"
	"github.com/songquanpeng/one-api/relay/relaymode"
)

// 仅解析完成性和用量字段，辅助字段的类型不影响流状态判断。
type streamFrame struct {
	Error   *model.Error    `json:"error"`
	Usage   json.RawMessage `json:"usage"`
	Choices []struct {
		Index        *int    `json:"index"`
		FinishReason *string `json:"finish_reason"`
		Text         string  `json:"text"`
		Delta        struct {
			Content          any            `json:"content"`
			ReasoningContent any            `json:"reasoning_content"`
			Refusal          any            `json:"refusal"`
			ToolCalls        []model.Tool   `json:"tool_calls"`
			FunctionCall     model.Function `json:"function_call"`
		} `json:"delta"`
	} `json:"choices"`
}

func StreamHandler(c *gin.Context, resp *http.Response, m *meta.Meta) (*model.ErrorWithStatusCode, string, *model.Usage) {
	m.RecordFailedStreamUsage = false
	if m.Mode != relaymode.ChatCompletions && m.Mode != relaymode.Completions {
		return otherStreamHandler(c, resp)
	}
	scanner := newStreamEventScanner(resp.Body)
	closed := false
	defer func() {
		if !closed {
			_ = resp.Body.Close()
		}
	}()
	responseText := ""
	hasContent := false
	var usage *model.Usage
	var validFailureUsage *model.Usage
	var streamErr *model.ErrorWithStatusCode
	doneReceived := false
	finished := make(map[int]struct{})
	writeData := func(data string) {
		// 首帧前保留普通错误响应和重试的能力。
		common.SetEventStreamHeaders(c)
		render.StringData(c, data)
	}
	for c.Request.Context().Err() == nil && scanner.Scan() {
		if c.Request.Context().Err() != nil {
			break
		}
		payload := scanner.Text()
		if strings.TrimSpace(payload) == done {
			doneReceived = true
			break
		}
		var frame streamFrame
		if err := json.Unmarshal([]byte(payload), &frame); err != nil {
			logger.Errorf(c.Request.Context(), "invalid stream response: %s", err.Error())
			streamErr = newStreamError("invalid_stream_response", "Invalid upstream stream response.", http.StatusBadGateway)
			break
		}
		frameUsage, validUsage, usageErr := parseStreamUsage(frame.Usage)
		if usageErr != nil {
			logger.Errorf(c.Request.Context(), "invalid stream usage: %s", usageErr.Error())
			streamErr = newStreamError("invalid_stream_response", "Invalid upstream stream response.", http.StatusBadGateway)
			break
		}
		if frameUsage != nil {
			usage = frameUsage
		}
		if validUsage {
			validFailureUsage = frameUsage
		}

		if frame.Error != nil {
			streamErr = &model.ErrorWithStatusCode{Error: *frame.Error, StatusCode: http.StatusBadGateway}
			if streamErr.Message == "" {
				streamErr.Message = "Upstream stream returned an error."
			}
			break
		}
		if m.Mode == relaymode.ChatCompletions && len(frame.Choices) == 0 && frameUsage == nil {
			continue
		}
		for _, choice := range frame.Choices {
			index := -1
			if choice.Index != nil {
				index = *choice.Index
			} else if m.ExpectedChoices == 1 {
				index = 0
			}
			if index >= 0 && index < m.ExpectedChoices && choice.FinishReason != nil && *choice.FinishReason != "" {
				finished[index] = struct{}{}
			}
			if m.Mode == relaymode.Completions {
				responseText += choice.Text
				hasContent = hasContent || choice.Text != ""
			} else {
				responseText += conv.AsString(choice.Delta.Content)
				d := choice.Delta
				hasContent = hasContent || conv.AsString(d.Content) != "" || conv.AsString(d.ReasoningContent) != "" || conv.AsString(d.Refusal) != "" || d.FunctionCall.Name != "" || conv.AsString(d.FunctionCall.Arguments) != ""
				for _, tool := range d.ToolCalls {
					hasContent = hasContent || tool.Id != "" || tool.Function.Name != "" || conv.AsString(tool.Function.Arguments) != ""
				}
			}
		}
		if m.Mode == relaymode.ChatCompletions && responseIDSanitizationEnabled(c) {
			cleaned, _ := sanitizeChatResponseID([]byte(payload))
			payload = string(cleaned)
		}
		writeData(payload)
	}
	if err := c.Request.Context().Err(); err != nil {
		streamErr = canceledStreamError(err)
	} else if err := scanner.Err(); err != nil && streamErr == nil {
		logger.Errorf(c.Request.Context(), "error reading stream: %s", err.Error())
		if errors.Is(err, errIncompleteStreamEvent) {
			streamErr = newStreamError("incomplete_stream", "Upstream stream ended before completion.", http.StatusBadGateway)
		} else {
			streamErr = newStreamError("read_stream_failed", "Failed to read upstream stream.", http.StatusBadGateway)
		}
	}
	closeErr := resp.Body.Close()
	closed = true
	if streamErr == nil && closeErr != nil {
		logger.Errorf(c.Request.Context(), "error closing stream: %s", closeErr.Error())
		streamErr = newStreamError("close_response_body_failed", "Failed to close upstream stream.", http.StatusInternalServerError)
	}
	if streamErr == nil && !doneReceived && (m.ExpectedChoices <= 0 || len(finished) != m.ExpectedChoices) {
		streamErr = newStreamError("incomplete_stream", "Upstream stream ended before completion.", http.StatusBadGateway)
	}
	if err := c.Request.Context().Err(); err != nil {
		streamErr = canceledStreamError(err)
	}
	if streamErr == nil {
		writeData(done)
		if err := c.Request.Context().Err(); err != nil {
			streamErr = canceledStreamError(err)
		}
	}
	if streamErr != nil {
		usage = failedStreamUsage(responseText, hasContent, validFailureUsage, m)
		m.RecordFailedStreamUsage = usage != nil && usage.TotalTokens > 0
		// 已写出的响应只能追加 SSE 错误，取消后不再写入，也不补正常结束标记。
		if c.Writer.Written() && c.Request.Context().Err() == nil {
			_ = render.ObjectData(c, gin.H{"error": streamErr.Error})
		}
		return streamErr, responseText, usage
	}
	return nil, responseText, usage
}

func newStreamError(code, message string, status int) *model.ErrorWithStatusCode {
	return &model.ErrorWithStatusCode{Error: model.Error{Type: "one_api_error", Code: code, Message: message}, StatusCode: status}
}

func canceledStreamError(err error) *model.ErrorWithStatusCode {
	if errors.Is(err, context.DeadlineExceeded) {
		return newStreamError("request_timeout", "Request timed out.", http.StatusGatewayTimeout)
	}
	return newStreamError("request_canceled", "Request was canceled.", 499)
}

// 无有效用量时，只有实际内容才允许沿用文本估算；空失败不按预估输入收费。
func failedStreamUsage(text string, hasContent bool, usage *model.Usage, m *meta.Meta) *model.Usage {
	maxInt := int(^uint(0) >> 1)
	if usage != nil && usage.PromptTokens >= 0 && usage.CompletionTokens >= 0 && usage.TotalTokens >= 0 && usage.PromptTokens <= maxInt-usage.CompletionTokens {
		result := *usage
		if result.PromptTokens == 0 && result.CompletionTokens == 0 && result.TotalTokens > 0 {
			result.PromptTokens = m.PromptTokens
			if result.PromptTokens < 0 {
				result.PromptTokens = 0
			}
			if result.PromptTokens > result.TotalTokens {
				result.PromptTokens = result.TotalTokens
			}
			result.CompletionTokens = result.TotalTokens - result.PromptTokens
		}
		result.TotalTokens = result.PromptTokens + result.CompletionTokens
		return &result
	}
	if !hasContent {
		return nil
	}
	return ResponseText2Usage(text, m.ActualModelName, m.PromptTokens)
}

// 无法确定数量时返回零，只接受显式结束标记；不改变透传参数。
func ExpectedStreamChoices(request *model.GeneralOpenAIRequest, mode int) int {
	n := request.N
	if n <= 0 {
		n = 1
	}
	if mode != relaymode.Completions {
		return n
	}
	prompts := completionPromptCount(request.Prompt)
	if prompts <= 0 || n > int(^uint(0)>>1)/prompts {
		return 0
	}
	return n * prompts
}

func completionPromptCount(prompt any) int {
	switch value := prompt.(type) {
	case string:
		return 1
	case []string:
		return len(value)
	case []int:
		if len(value) > 0 {
			return 1
		}
	case [][]int:
		for _, item := range value {
			if len(item) == 0 {
				return 0
			}
		}
		return len(value)
	case []any:
		if len(value) == 0 {
			return 0
		}
		stringsOnly, tokensOnly, batchesOnly := true, true, true
		for _, item := range value {
			_, isString := item.(string)
			stringsOnly = stringsOnly && isString
			tokensOnly = tokensOnly && isTokenID(item)
			batch, ok := item.([]any)
			if !ok || len(batch) == 0 {
				batchesOnly = false
				continue
			}
			for _, token := range batch {
				if !isTokenID(token) {
					batchesOnly = false
				}
			}
		}
		if stringsOnly || batchesOnly {
			return len(value)
		}
		if tokensOnly {
			return 1
		}
	}
	return 0
}

func isTokenID(value any) bool {
	number, ok := value.(float64)
	return ok && number >= 0 && !math.IsInf(number, 0) && math.Trunc(number) == number
}

// 非文本接口只保留结束帧处理，不套用文本结果数量和错误帧规则。
func otherStreamHandler(c *gin.Context, resp *http.Response) (*model.ErrorWithStatusCode, string, *model.Usage) {
	scanner := bufio.NewScanner(resp.Body)
	common.SetEventStreamHeaders(c)
	doneRendered := false
	for scanner.Scan() {
		data := scanner.Text()
		if len(data) < dataPrefixLength {
			continue
		}
		if data[:dataPrefixLength] != dataPrefix && data[:dataPrefixLength] != done {
			continue
		}
		if strings.HasPrefix(data[dataPrefixLength:], done) {
			render.StringData(c, data)
			doneRendered = true
		}
	}
	if err := scanner.Err(); err != nil {
		logger.SysError("error reading stream: " + err.Error())
	}
	if !doneRendered {
		render.Done(c)
	}
	if err := resp.Body.Close(); err != nil {
		return ErrorWrapper(err, "close_response_body_failed", http.StatusInternalServerError), "", nil
	}
	return nil, "", nil
}

// 区分缺失字段和显式零值，只有完整计数或单独总量可以用于失败结算。
func parseStreamUsage(raw json.RawMessage) (*model.Usage, bool, error) {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return nil, false, nil
	}
	var fields struct {
		Prompt     *int                           `json:"prompt_tokens"`
		Completion *int                           `json:"completion_tokens"`
		Total      *int                           `json:"total_tokens"`
		Details    *model.CompletionTokensDetails `json:"completion_tokens_details"`
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, false, err
	}
	usage := &model.Usage{CompletionTokensDetails: fields.Details}
	if fields.Prompt != nil {
		usage.PromptTokens = *fields.Prompt
	}
	if fields.Completion != nil {
		usage.CompletionTokens = *fields.Completion
	}
	if fields.Total != nil {
		usage.TotalTokens = *fields.Total
	}
	if usage.PromptTokens < 0 || usage.CompletionTokens < 0 || usage.TotalTokens < 0 {
		return usage, false, nil
	}
	if fields.Prompt == nil && fields.Completion == nil && fields.Total != nil {
		return usage, true, nil
	}
	maxInt := int(^uint(0) >> 1)
	if fields.Prompt == nil || fields.Completion == nil || usage.PromptTokens > maxInt-usage.CompletionTokens {
		return usage, false, nil
	}
	total := usage.PromptTokens + usage.CompletionTokens
	return usage, fields.Total == nil || *fields.Total == total, nil
}

var errIncompleteStreamEvent = errors.New("upstream stream ended with an undispatched data event")

type streamEventScanner struct {
	scanner   *bufio.Scanner
	firstLine bool
	payload   string
	err       error
}

func newStreamEventScanner(reader io.Reader) *streamEventScanner {
	scanner := bufio.NewScanner(reader)
	skipLF := false
	// CR 已结束当前行；下一次读取再跳过可选 LF，避免等待后续字节才派发事件。
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if len(data) == 0 {
			return 0, nil, nil
		}
		start := 0
		if skipLF {
			skipLF = false
			if data[0] == '\n' {
				start = 1
			}
		}
		for i := start; i < len(data); i++ {
			if data[i] == '\r' {
				skipLF = true
				return i + 1, data[start:i], nil
			}
			if data[i] == '\n' {
				return i + 1, data[start:i], nil
			}
		}
		if atEOF && start < len(data) {
			return len(data), data[start:], nil
		}
		return start, nil, nil
	})
	return &streamEventScanner{scanner: scanner, firstLine: true}
}

// 空行才派发事件，多行 data 用换行合并；EOF 的未闭合数据不进入文本和用量统计。
func (s *streamEventScanner) Scan() bool {
	var data strings.Builder
	hasData := false
	for s.scanner.Scan() {
		line := s.scanner.Text()
		if s.firstLine {
			line = strings.TrimPrefix(line, "\uFEFF")
			s.firstLine = false
		}
		if line == "" {
			if !hasData {
				continue
			}
			s.payload = strings.TrimSuffix(data.String(), "\n")
			return true
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			value = ""
		}
		if field != "data" {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		// 聚合事件沿用扫描器的大小上限，避免多行绕过缓冲限制。
		if len(value) >= bufio.MaxScanTokenSize || data.Len() > bufio.MaxScanTokenSize-len(value)-1 {
			s.err = bufio.ErrTooLong
			return false
		}
		data.WriteString(value)
		data.WriteByte('\n')
		hasData = true
	}
	s.err = s.scanner.Err()
	if s.err == nil && hasData {
		s.err = errIncompleteStreamEvent
	}
	return false
}

func (s *streamEventScanner) Text() string { return s.payload }
func (s *streamEventScanner) Err() error   { return s.err }
