# 可选渠道输入路由

输入路由按渠道启用，不依赖亲和、Redis或外部计量平台。默认未配置时保持原选路行为。输入路由在候选渠道中筛选资格，再交给原优先级和随机选路；缓存与数据库路径均支持。不会自动把请求改成其他模型，也不会增加通配模型兜底。

## 渠道配置

通过现有渠道管理 API 的 `config` JSON 字符串保存，保留已有配置字段。例如一个最大预算 4096 的文本渠道：

```json
{"id":1,"config":"{\"input_modality\":\"text\",\"input_length\":{\"known_range\":{\"min_tokens\":0,\"max_tokens\":4096},\"accept_unknown\":false}}"}
```

发送到原有 `PUT /api/channel/`，使用已有管理员鉴权。`config` 仍采用原来的整体更新语义，提交时必须包含需要保留的其他配置。新建渠道使用同样的内层 JSON。这里没有增加数据库列。

内层配置也可以单独理解为：

```json
{
  "input_modality": "multimodal",
  "input_length": {
    "known_range": {"min_tokens": 4097},
    "accept_unknown": false
  }
}
```

- `known_range` 是闭区间：min、max都包含。省略max表示无上限；min必须存在且非负。
- 未设置 `input_length` 或设置为null，不限制长度；`{}`则没有已知长度可匹配，且默认不接受unknown。
- `accept_unknown` 控制计数失败时的资格，不代表忽略有效的长度规则。未知长度不是0。
- `input_modality` 可选：`text`只接受文本，`multimodal`接受文本与图片/视频。省略该字段不施加模态限制，以兼容旧图片渠道；显式空值/null非法。
- 两个字段分别生效。未知内容块令长度unknown且模态未知；只设长度时按accept_unknown处理，显式模态规则不接受未知模态。
- 未配置规则的渠道保持可接收，可能作为同一模型下的不限长候选。要让超范围输入一定拒绝，应给该模型的所有候选都配置规则。
- 缓存模式沿用 `SYNC_FREQUENCY` 刷新；无缓存直接读取DB规则。回退旧版本后旧程序会忽略新策略，因此不能把混跑或回退当作同等强制约束。

## 预算含义

字段名 `input_length` 表示选路预算，**不是实际模型usage，也不是计费或模型上下文上限的精确计算**：

```text
可见文本的GPT-3.5 tokenizer数量 + 每张图片1280 + 视频实际帧数×384 + 显式输出上限
```

文本包括content/prompt、工具定义与调用/结果、schema；不计metadata、采样参数、媒体base64字符串或模型协议包装。Chat同时有max_tokens与max_completion_tokens取较大值，只加一次；Completions取max_tokens。缺失/null/0不预留输出，非法/负数/小数/溢出产生unknown。实际usage和原计费处理完全沿用上游。

实际挂载接口只有上游已有的 `/v1/chat/completions` 和 `/v1/completions`。库中包含Responses/Messages解析与单元测试以方便以后接入，**本改动没有新增 `/v1/responses` 或 `/v1/messages` 转发入口**。

只有当前候选中有显式策略时才解析，首发与重试共用结果。不改变原请求正文。指定渠道首发保留原长度豁免，但显式模态规则仍检查。新规则筛空返回400；原本无候选/数据库失败保持原503；重试选渠失败保留最后一次真实转发错误。

## 视频处理边界

图片不下载、不解码，每张固定1280。视频需要本机 `ffmpeg` 与 `ffprobe`；工具缺失、下载/解码失败或超时令长度unknown，已识别的multimodal不会丢失。按2fps抽样，最多1280帧；同时保留100MiB、600秒、共享120秒媒体处理上限，因此600秒视频通常先受时长限制，不能理解为允许任意1280帧时长。重复URL在同一请求中复用成功或失败处理结果，但每次出现都累计预算。

URL视频只允许http/https的公网地址，不使用环境代理；DNS拨号固定校验、每跳重定向验证，拒绝私网/本地及特殊地址。内联视频支持base64 data URI。临时文件在处理结束清理；子进程跟随context终止，限制下载/解码并发、尺寸和工具输出。额外解码边界为最大边8192、像素8192×4320、单次分配256MiB、工具输出256KiB；超限按unknown处理。网关只计帧，不改写或抽帧后替换转发正文。

## 日志与验证

复用现有请求日志，显式策略触发时记录 `input_routing_decision`、尝试编号、选中渠道、模态、计数状态与预算，不记录输入正文或媒体URL。没有新增Prometheus部署或采集端点。

```sh
go test -race ./relay/lengthrouting
go test -race ./model ./middleware ./controller -run TestInputRouting
go build ./...
```

HTTP与SQL测试均为隔离mock/内存SQLite，不需要推理服务或生产账号。真实视频测试需要ffmpeg/ffprobe，未安装时明确skip；标准tokenizer测试复用tiktoken原缓存。发布还应验证实际数据库驱动、旧配置、缓存刷新和使用中的SDK。
