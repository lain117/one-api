package lengthrouting

// Input 惰性解析原始正文；没有显式策略时不引入计数或媒体下载开销。
// 每个请求独占实例，首发与顺序重试共用同一份结果。
type Input struct {
	Counter func() CountResult
	Result  *CountResult
}

func (in *Input) Resolve() *CountResult {
	if in == nil {
		return nil
	}
	if in.Result == nil {
		result := in.Counter()
		in.Result = &result
	}
	return in.Result
}
