package lengthrouting

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
)

type inputSchema struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"`
}

type inputFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

var errVideoCount = errors.New("video_count_failed")

// CountInput 统计路由预算，不改写正文或计费用量；视频只在首次出现时启动受限计数。
func CountInput(ctx context.Context, path string, body []byte, countText func(string) int) CountResult {
	var videoCounter func(string) (int, error)
	var cancel context.CancelFunc
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	countVideoFrames := func(videoURL string) (int, error) {
		if videoCounter == nil {
			videoCounter, cancel = newVideoFrameCounter(ctx)
		}
		return videoCounter(videoURL)
	}

	var request struct {
		Messages            json.RawMessage `json:"messages"`
		Input               json.RawMessage `json:"input"`
		Prompt              json.RawMessage `json:"prompt"`
		Instructions        json.RawMessage `json:"instructions"`
		Tools               json.RawMessage `json:"tools"`
		Functions           json.RawMessage `json:"functions"`
		ResponseFormat      json.RawMessage `json:"response_format"`
		Text                json.RawMessage `json:"text"`
		System              json.RawMessage `json:"system"`
		OutputConfig        json.RawMessage `json:"output_config"`
		OutputFormat        json.RawMessage `json:"output_format"`
		MaxTokens           json.RawMessage `json:"max_tokens"`
		MaxCompletionTokens json.RawMessage `json:"max_completion_tokens"`
		MaxOutputTokens     json.RawMessage `json:"max_output_tokens"`
	}
	if json.Unmarshal(body, &request) != nil {
		return Unknown("invalid_json")
	}
	var raw json.RawMessage
	switch path {
	case "/v1/chat/completions", "/v1/messages":
		raw = request.Messages
	case "/v1/responses":
		raw = request.Input
	case "/v1/completions":
		raw = request.Prompt
	default:
		return Unknown("unsupported_protocol")
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return Unknown("missing_input")
	}
	modality := "text"
	// token 计数失败不清除已经识别的模态；未知模态也不能被后续文本覆盖。
	unknown := func(reason string) CountResult { result := Unknown(reason); result.Modality = modality; return result }
	total, err := countInput(path, raw, countText, &modality, countVideoFrames)
	if err != nil && path != "/v1/messages" {
		if errors.Is(err, errVideoCount) {
			return unknown("video_count_failed")
		}
		return unknown("invalid_input")
	}
	if path == "/v1/messages" {
		// 消息计数失败也要检查 system，避免遗漏其中的未知模态。
		n, systemErr := countMessagesContent(request.System, countText, &modality)
		if err != nil {
			return unknown("invalid_input")
		}
		if systemErr != nil {
			return unknown("invalid_system")
		}
		total = countSum(total, n)
		var tools []struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			InputSchema json.RawMessage `json:"input_schema"`
		}
		if len(request.Tools) > 0 && json.Unmarshal(request.Tools, &tools) != nil {
			return unknown("invalid_tools")
		}
		for _, tool := range tools {
			total = countSum(total, int64(countText(tool.Name)), int64(countText(tool.Description)), countJSON(tool.InputSchema, countText))
		}
		var output struct {
			Format *inputSchema `json:"format"`
		}
		var legacy *inputSchema
		if len(request.OutputConfig) > 0 && json.Unmarshal(request.OutputConfig, &output) != nil {
			return unknown("invalid_output_schema")
		}
		if len(request.OutputFormat) > 0 && json.Unmarshal(request.OutputFormat, &legacy) != nil {
			return unknown("invalid_output_schema")
		}
		// 两种格式均按显式携带的 schema 计数，不推断上游优先级。
		total = countSum(total, countSchema(output.Format, countText), countSchema(legacy, countText))
	} else if path != "/v1/completions" {
		tools, err := countTools(request.Tools, false, countText)
		if err != nil {
			return unknown("invalid_tools")
		}
		total = countSum(total, tools)
		schemaRaw := request.ResponseFormat
		if path == "/v1/chat/completions" {
			functions, err := countTools(request.Functions, true, countText)
			if err != nil {
				return unknown("invalid_tools")
			}
			total = countSum(total, functions)
		} else {
			var instructions string
			if len(request.Instructions) > 0 && json.Unmarshal(request.Instructions, &instructions) != nil {
				return unknown("invalid_instructions")
			}
			total = countSum(total, int64(countText(instructions)))
			schemaRaw = request.Text
		}
		var output struct {
			JSONSchema *inputSchema `json:"json_schema"`
			Format     *inputSchema `json:"format"`
		}
		if len(schemaRaw) > 0 && json.Unmarshal(schemaRaw, &output) != nil {
			return unknown("invalid_output_schema")
		}
		if path == "/v1/chat/completions" {
			total = countSum(total, countSchema(output.JSONSchema, countText))
		} else {
			total = countSum(total, countSchema(output.Format, countText))
		}
	}
	outputRaw := request.MaxTokens
	if path == "/v1/responses" {
		outputRaw = request.MaxOutputTokens
	}
	output, err := countOutputLimit(outputRaw)
	if err != nil {
		return unknown("invalid_output_limit")
	}
	if path == "/v1/chat/completions" {
		completion, err := countOutputLimit(request.MaxCompletionTokens)
		if err != nil {
			return unknown("invalid_output_limit")
		}
		if completion > output {
			output = completion
		}
	}
	total = countSum(total, output)
	if total < 0 {
		return unknown("token_overflow")
	}
	// 未声明模态限制的长度策略也不能把未知块的部分计数当成完整预算。
	if modality == "" {
		return unknown("unsupported_content")
	}
	return CountResult{Tokens: total, Kind: "estimated", Modality: modality}
}

func countInput(path string, raw json.RawMessage, countText func(string) int, modality *string, countVideoFrames func(string) (int, error)) (int64, error) {
	if (path == "/v1/responses" || path == "/v1/completions") && raw[0] == '"' {
		return countContent(raw, countText, modality, countVideoFrames)
	}
	if path == "/v1/completions" {
		var prompts []string
		if err := json.Unmarshal(raw, &prompts); err != nil {
			// 已编码的文本仍是文本模态；仅保留原来的未知长度，不解码或重新计数。
			var tokenIDs []int
			var batches [][]int
			if json.Unmarshal(raw, &tokenIDs) != nil && json.Unmarshal(raw, &batches) != nil {
				*modality = ""
			}
			return 0, err
		}
		var total int64
		for _, prompt := range prompts {
			total = countSum(total, int64(countText(prompt)))
		}
		return total, nil
	}
	var messages []json.RawMessage
	if err := json.Unmarshal(raw, &messages); err != nil {
		*modality = ""
		return 0, err
	}
	var total int64
	var inputErr error
	for _, rawMessage := range messages {
		var message struct {
			Type         string          `json:"type"`
			Content      json.RawMessage `json:"content"`
			Output       json.RawMessage `json:"output"`
			FunctionCall json.RawMessage `json:"function_call"`
			ToolCalls    json.RawMessage `json:"tool_calls"`
		}
		if bytes.Equal(bytes.TrimSpace(rawMessage), []byte("null")) {
			*modality = ""
			continue
		}
		if err := json.Unmarshal(rawMessage, &message); err != nil {
			*modality = ""
			inputErr = err
			continue
		}
		content := message.Content
		if path == "/v1/responses" {
			switch message.Type {
			case "", "message":
			case "function_call":
				n, err := countCall(rawMessage, countText)
				total = countSum(total, n)
				if err != nil {
					inputErr = err
				}
				continue
			case "function_call_output":
				content = message.Output
			default:
				*modality = ""
				continue
			}
		}
		var n int64
		var err error
		if path == "/v1/messages" {
			n, err = countMessagesContent(content, countText, modality)
		} else {
			n, err = countContent(content, countText, modality, countVideoFrames)
		}
		total = countSum(total, n)
		if err != nil {
			inputErr = err
		}
		if path == "/v1/chat/completions" {
			n, err := countCall(message.FunctionCall, countText)
			total = countSum(total, n)
			if err != nil {
				inputErr = err
			}
			var calls []struct {
				Function json.RawMessage `json:"function"`
			}
			if len(message.ToolCalls) > 0 {
				if err := json.Unmarshal(message.ToolCalls, &calls); err != nil {
					inputErr = err
				}
			}
			for _, call := range calls {
				n, err := countCall(call.Function, countText)
				total = countSum(total, n)
				if err != nil {
					inputErr = err
				}
			}
		}
	}
	return total, inputErr
}

func countSchema(schema *inputSchema, countText func(string) int) int64 {
	if schema == nil {
		return 0
	}
	return countSum(int64(countText(schema.Name)), int64(countText(schema.Description)), countJSON(schema.Schema, countText))
}

func countCall(raw json.RawMessage, countText func(string) int) (int64, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	var call struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	if err := json.Unmarshal(raw, &call); err != nil {
		return 0, err
	}
	return countSum(int64(countText(call.Name)), int64(countText(call.Arguments))), nil
}

func countTools(raw json.RawMessage, legacy bool, countText func(string) int) (int64, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	var tools []struct {
		inputFunction
		Type     string         `json:"type"`
		Function *inputFunction `json:"function"`
	}
	if err := json.Unmarshal(raw, &tools); err != nil {
		return 0, err
	}
	var total int64
	for _, tool := range tools {
		if !legacy && tool.Type != "function" {
			continue
		}
		definition := tool.inputFunction
		if tool.Function != nil {
			definition = *tool.Function
		}
		total = countSum(total, int64(countText(definition.Name)), int64(countText(definition.Description)), countJSON(definition.Parameters, countText))
	}
	return total, nil
}

// schema 使用紧凑 JSON 保留属性名、枚举和结构；不递归重复累计其中的字符串。
func countJSON(raw json.RawMessage, countText func(string) int) int64 {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0
	}
	var compact bytes.Buffer
	_ = json.Compact(&compact, raw) // raw 已经过整个请求的 JSON 校验。
	return int64(countText(compact.String()))
}

func countContent(raw json.RawMessage, countText func(string) int, modality *string, countVideoFrames func(string) (int, error)) (int64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return 0, nil
	}
	if raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, err
		}
		return int64(countText(text)), nil
	}
	var parts []struct {
		Type     string          `json:"type"`
		Text     json.RawMessage `json:"text"`
		VideoURL json.RawMessage `json:"video_url"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		*modality = ""
		return 0, err
	}
	var total int64
	var contentErr error
	for _, part := range parts {
		switch part.Type {
		case "text", "input_text", "output_text":
			var text string
			if err := json.Unmarshal(part.Text, &text); err != nil {
				contentErr = err
				continue
			}
			total = countSum(total, int64(countText(text)))
		case "image_url", "input_image":
			if *modality != "" {
				*modality = "multimodal"
			}
			total = countSum(total, 1280)
		case "video_url", "input_video":
			if *modality != "" {
				*modality = "multimodal"
			}
			var url string
			if json.Unmarshal(part.VideoURL, &url) != nil {
				var video struct {
					URL string `json:"url"`
				}
				if json.Unmarshal(part.VideoURL, &video) == nil {
					url = video.URL
				}
			}
			if url == "" || countVideoFrames == nil {
				contentErr = errVideoCount
				continue
			}
			frames, err := countVideoFrames(url)
			if err != nil || frames <= 0 {
				// 不向外暴露媒体地址或底层错误，并继续识别后续内容模态。
				contentErr = errVideoCount
				continue
			}
			if int64(frames) > math.MaxInt64/384 {
				total = -1
			} else {
				total = countSum(total, int64(frames)*384)
			}
		default:
			*modality = ""
		}
	}
	return total, contentErr
}

// Messages 使用独立的内容类型，避免扩展原有 OpenAI 接口的准入范围。
func countMessagesContent(raw json.RawMessage, countText func(string) int, modality *string) (int64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) || raw[0] == '"' {
		return countContent(raw, countText, modality, nil)
	}
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		*modality = ""
		return 0, err
	}
	var total int64
	var contentErr error
	for _, rawPart := range parts {
		var part struct {
			Type     string          `json:"type"`
			Text     json.RawMessage `json:"text"`
			Thinking json.RawMessage `json:"thinking"`
			Name     string          `json:"name"`
			Input    json.RawMessage `json:"input"`
			Content  json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(rawPart, &part); err != nil {
			*modality = ""
			contentErr = err
			continue
		}
		var text string
		var err error
		switch part.Type {
		case "text", "thinking":
			value := part.Text
			if part.Type == "thinking" {
				value = part.Thinking
			}
			err = json.Unmarshal(value, &text)
		case "image":
			if *modality != "" {
				*modality = "multimodal"
			}
			total = countSum(total, 1280)
		case "tool_use":
			text = part.Name
			total = countSum(total, countJSON(part.Input, countText))
		case "tool_result":
			var n int64
			n, err = countMessagesContent(part.Content, countText, modality)
			total = countSum(total, n)
		case "redacted_thinking":
			// 不透明 data/signature 不计数，也不改写原请求。
		default:
			*modality = ""
		}
		total = countSum(total, int64(countText(text)))
		if err != nil {
			// 继续识别后续媒体；未知长度不能导致图片漏入文本渠道。
			contentErr = err
		}
	}
	return total, contentErr
}

// 溢出保留为负哨兵并继续遍历，避免长度失败掩盖后续模态。
func countSum(values ...int64) int64 {
	var total int64
	for _, value := range values {
		if value < 0 || value > math.MaxInt64-total {
			return -1
		}
		total += value
	}
	return total
}

func countOutputLimit(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, nil
	}
	var limit int64
	if err := json.Unmarshal(raw, &limit); err != nil || limit < 0 {
		return 0, errors.New("invalid_output_limit")
	}
	return limit, nil
}
