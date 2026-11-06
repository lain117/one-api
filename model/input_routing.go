package model

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sort"
	"strings"

	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/logger"
	"github.com/songquanpeng/one-api/relay/affinity"
	"github.com/songquanpeng/one-api/relay/lengthrouting"
)

var ErrNoInputLengthChannel = errors.New("no channel accepts this input length")
var ErrNoInputModalityChannel = errors.New("no channel accepts this input modality")

// 旧渠道未使用新增字段时沿用原行为；损坏的显式策略不能当成无限制。
func hasInputPolicy(config string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(config), &fields) != nil {
		return strings.Contains(config, `"input_length"`) || strings.Contains(config, `"input_modality"`)
	}
	for _, key := range []string{"input_length", "input_modality"} {
		if raw, exists := fields[key]; exists && (key == "input_modality" || strings.TrimSpace(string(raw)) != "null") {
			return true
		}
	}
	return false
}
func ValidateInputRoutingConfig(config string) error {
	if !hasInputPolicy(config) {
		return nil
	}
	_, err := lengthrouting.ParseRule(config)
	return err
}
func (ch *Channel) prepareInputRule() {
	ch.inputPolicy = hasInputPolicy(ch.Config)
	ch.inputRule, ch.inputRuleInvalid = nil, false
	if !ch.inputPolicy {
		return
	}
	var err error
	ch.inputRule, err = lengthrouting.ParseRule(ch.Config)
	ch.inputRuleInvalid = err != nil
	if err != nil {
		logger.SysErrorf("invalid input routing rule channel_id=%d", ch.Id)
	}
}
func filterInputChannels(channels []*Channel, input *lengthrouting.Input) ([]*Channel, error) {
	if input == nil || len(channels) == 0 {
		return channels, nil
	}
	enabled := false
	for _, ch := range channels {
		enabled = enabled || ch.inputPolicy
	}
	if !enabled {
		return channels, nil
	}
	count := input.Resolve()
	result := make([]*Channel, 0, len(channels))
	err := ErrNoInputModalityChannel
	for _, ch := range channels {
		if ch.inputRuleInvalid {
			err = ErrNoInputLengthChannel
			continue
		}
		if !ch.inputRule.AllowsModality(count.Modality) {
			continue
		}
		err = ErrNoInputLengthChannel
		if ch.inputRule.Allows(count) {
			result = append(result, ch)
		}
	}
	if len(result) == 0 {
		return nil, err
	}
	return result, nil
}

// 指定首发保留原长度豁免，但显式声明的模态规则仍需满足。
func (ch *Channel) CheckInputModality(input *lengthrouting.Input) error {
	if input == nil {
		return nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(ch.Config), &fields) != nil {
		if strings.Contains(ch.Config, `"input_modality"`) {
			return ErrNoInputModalityChannel
		}
		return nil
	}
	if _, exists := fields["input_modality"]; !exists {
		return nil
	}
	modality, err := lengthrouting.ParseModality(ch.Config)
	if err != nil || !(&lengthrouting.Rule{Modality: modality}).AllowsModality(input.Resolve().Modality) {
		return ErrNoInputModalityChannel
	}
	return nil
}

func getDBInputChannel(ctx context.Context, group, model string, ignoreFirstPriority bool, input *lengthrouting.Input, request *affinity.Request) (*Channel, affinity.Result, error) {
	groupColumn := "`group`"
	if common.UsingPostgreSQL {
		groupColumn = `"group"`
	}
	var abilities []Ability
	if err := DB.Where(groupColumn+" = ? AND model = ? AND enabled = ?", group, model, true).Find(&abilities).Error; err != nil {
		return nil, affinity.Result{}, err
	}
	ids := make([]int, 0, len(abilities))
	priorities := make(map[int]int64, len(abilities))
	for _, ability := range abilities {
		ids = append(ids, ability.ChannelId)
		if ability.Priority != nil {
			priorities[ability.ChannelId] = *ability.Priority
		}
	}
	if len(ids) == 0 {
		channel, err := GetRandomSatisfiedChannel(group, model, ignoreFirstPriority)
		return channel, affinity.Result{}, err
	}
	var channels []*Channel
	if err := DB.Where("id IN ?", ids).Find(&channels).Error; err != nil {
		return nil, affinity.Result{}, err
	}
	configured := false
	for _, ch := range channels {
		ch.prepareInputRule()
		configured = configured || ch.inputPolicy
	}
	// 输入策略和亲和均未启用时，保留原 SQL 选路。
	if !configured && request == nil {
		channel, err := GetRandomSatisfiedChannel(group, model, ignoreFirstPriority)
		return channel, affinity.Result{}, err
	}
	eligible := make([]*Channel, 0, len(channels))
	for _, ch := range channels {
		if ch.Status == ChannelStatusEnabled {
			eligible = append(eligible, ch)
		}
	}
	if len(eligible) == 0 {
		return nil, affinity.Result{}, errors.New("channel not found")
	}
	filtered, err := filterInputChannels(eligible, input)
	if err != nil {
		return nil, affinity.Result{}, err
	}
	sort.SliceStable(filtered, func(i, j int) bool { return priorities[filtered[i].Id] > priorities[filtered[j].Id] })
	end := len(filtered)
	if !ignoreFirstPriority {
		for i, ch := range filtered {
			if priorities[ch.Id] != priorities[filtered[0].Id] {
				end = i
				break
			}
		}
	}
	// 数据库旧实现重试忽略优先级时在所有候选中随机，而缓存路径保留自己的规则。
	channel, result := chooseAffinityChannel(ctx, filtered[:end], request)
	return channel, result, nil
}

func chooseAffinityChannel(ctx context.Context, channels []*Channel, request *affinity.Request) (*Channel, affinity.Result) {
	var result affinity.Result
	if request != nil {
		candidates := make([]affinity.Candidate, 0, len(channels))
		for _, channel := range channels {
			weight := 1
			if channel.Weight != nil && *channel.Weight > 0 {
				weight = int(*channel.Weight)
				if weight < 0 {
					weight = int(^uint(0) >> 1)
				}
			}
			candidates = append(candidates, affinity.Candidate{ID: channel.Id, Weight: weight})
		}
		result = affinity.Pick(ctx, request, candidates)
		for _, channel := range channels {
			if channel.Id == result.ChannelID {
				return channel, result
			}
		}
	}
	return channels[rand.Intn(len(channels))], result
}
