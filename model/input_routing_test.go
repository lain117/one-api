package model

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/relay/lengthrouting"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func routingDB(t *testing.T) {
	t.Helper()
	previousDB, previousCache, previousSQLite, previousPG := DB, config.MemoryCacheEnabled, common.UsingSQLite, common.UsingPostgreSQL
	previousChannels := group2model2channels
	var err error
	DB, err = gorm.Open(sqlite.Open(fmt.Sprintf("file:routing-%d?mode=memory&cache=shared", time.Now().UnixNano())), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err = DB.AutoMigrate(&Channel{}, &Ability{}); err != nil {
		t.Fatal(err)
	}
	common.UsingSQLite, common.UsingPostgreSQL = true, false
	current := DB
	t.Cleanup(func() {
		db, _ := current.DB()
		_ = db.Close()
		DB = previousDB
		config.MemoryCacheEnabled = previousCache
		common.UsingSQLite = previousSQLite
		common.UsingPostgreSQL = previousPG
		group2model2channels = previousChannels
	})
}
func addRoutingChannel(t *testing.T, id int, priority int64, policy string) {
	t.Helper()
	ch := Channel{Id: id, Status: ChannelStatusEnabled, Models: "route", Group: "default", Config: policy, Priority: &priority}
	if err := ch.Insert(); err != nil {
		t.Fatal(err)
	}
}
func routingInput(tokens int64, modality, kind string, calls *int32) *lengthrouting.Input {
	return &lengthrouting.Input{Counter: func() lengthrouting.CountResult {
		atomic.AddInt32(calls, 1)
		return lengthrouting.CountResult{Tokens: tokens, Modality: modality, Kind: kind}
	}}
}
func TestInputRoutingBothStoragePaths(t *testing.T) {
	for _, cache := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%v", cache), func(t *testing.T) {
			routingDB(t)
			config.MemoryCacheEnabled = cache
			addRoutingChannel(t, 1, 10, `{"input_modality":"text","input_length":{"known_range":{"min_tokens":0,"max_tokens":32}}}`)
			addRoutingChannel(t, 2, 5, `{"input_modality":"multimodal","input_length":{"known_range":{"min_tokens":34},"accept_unknown":true}}`)
			InitChannelCache()
			for _, tc := range []struct {
				name           string
				tokens         int64
				modality, kind string
				want           int
				err            error
			}{
				{"min", 0, "text", "estimated", 1, nil}, {"max", 32, "text", "estimated", 1, nil},
				{"gap", 33, "text", "estimated", 0, ErrNoInputLengthChannel}, {"lower_priority_after_filter", 34, "text", "estimated", 2, nil},
				{"image", 1289, "multimodal", "estimated", 2, nil}, {"image_length_reject", 9, "multimodal", "estimated", 0, ErrNoInputLengthChannel},
				{"unknown_length", 0, "multimodal", "unknown", 2, nil}, {"unknown_modality", 0, "", "unknown", 0, ErrNoInputModalityChannel},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var calls int32
					input := routingInput(tc.tokens, tc.modality, tc.kind, &calls)
					for _, retry := range []bool{false, true} {
						ch, err := CacheGetRandomSatisfiedChannel("default", "route", retry, input)
						if !errors.Is(err, tc.err) {
							t.Fatalf("err=%v want=%v", err, tc.err)
						}
						if tc.err == nil && (ch == nil || ch.Id != tc.want) {
							t.Fatalf("channel=%v want=%d", ch, tc.want)
						}
					}
					if calls != 1 {
						t.Fatalf("count calls=%d", calls)
					}
				})
			}
		})
	}
}
func TestInputRoutingLegacyNoPolicyDoesNotCount(t *testing.T) {
	for _, cache := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%v", cache), func(t *testing.T) {
			routingDB(t)
			config.MemoryCacheEnabled = cache
			addRoutingChannel(t, 1, 10, `{"api_version":"legacy"}`)
			addRoutingChannel(t, 2, 5, "")
			InitChannelCache()
			var calls int32
			input := routingInput(0, "", "unknown", &calls)
			ch, err := CacheGetRandomSatisfiedChannel("default", "route", false, input)
			if err != nil || ch.Id != 1 || calls != 0 {
				t.Fatalf("channel=%v err=%v calls=%d", ch, err, calls)
			}
			_, err = CacheGetRandomSatisfiedChannel("default", "missing", false, input)
			if err == nil || errors.Is(err, ErrNoInputLengthChannel) || errors.Is(err, ErrNoInputModalityChannel) {
				t.Fatalf("legacy missing=%v", err)
			}
		})
	}
}
func TestInputRoutingUnconfiguredFallbackAndInvalidRules(t *testing.T) {
	routingDB(t)
	config.MemoryCacheEnabled = true
	addRoutingChannel(t, 1, 10, `{"input_modality":"text","input_length":{"known_range":{"min_tokens":0,"max_tokens":2}}}`)
	addRoutingChannel(t, 2, 5, "")
	InitChannelCache()
	var calls int32
	ch, err := CacheGetRandomSatisfiedChannel("default", "route", false, routingInput(1281, "multimodal", "estimated", &calls))
	if err != nil || ch.Id != 2 {
		t.Fatalf("unconfigured catchall: %v %v", ch, err)
	}
	if err = DB.Model(&Channel{}).Where("id = ?", 2).Update("config", `{"input_length":{"known_range":{"min_tokens":-1}}}`).Error; err != nil {
		t.Fatal(err)
	}
	InitChannelCache()
	calls = 0
	_, err = CacheGetRandomSatisfiedChannel("default", "route", false, routingInput(1281, "multimodal", "estimated", &calls))
	if err == nil {
		t.Fatal("invalid rules passed")
	}
}
func TestInputRoutingDoesNotHoldCacheLockDuringCount(t *testing.T) {
	routingDB(t)
	config.MemoryCacheEnabled = true
	addRoutingChannel(t, 1, 10, `{"input_length":{"known_range":{"min_tokens":0}}}`)
	InitChannelCache()
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	input := &lengthrouting.Input{Counter: func() lengthrouting.CountResult {
		close(started)
		<-release
		return lengthrouting.CountResult{Tokens: 1, Modality: "text", Kind: "estimated"}
	}}
	go func() { defer close(done); _, _ = CacheGetRandomSatisfiedChannel("default", "route", false, input) }()
	<-started
	syncDone := make(chan struct{})
	go func() { InitChannelCache(); close(syncDone) }()
	select {
	case <-syncDone:
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("count held channel lock")
	}
	close(release)
	<-done
}
func TestInputRoutingDBErrorsRemainErrors(t *testing.T) {
	routingDB(t)
	config.MemoryCacheEnabled = false
	db, _ := DB.DB()
	_ = db.Close()
	var calls int32
	_, err := CacheGetRandomSatisfiedChannel("default", "route", false, routingInput(0, "text", "estimated", &calls))
	if err == nil || errors.Is(err, ErrNoInputLengthChannel) || errors.Is(err, ErrNoInputModalityChannel) || calls != 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}
func TestInputRoutingValidationAndSpecifiedChannel(t *testing.T) {
	for _, policy := range []string{`{"input_length":{"known_range":{"min_tokens":-1}}}`, `{"input_modality":"audio"}`, `{"input_modality":null}`, `{"input_length":42}`} {
		if ValidateInputRoutingConfig(policy) == nil {
			t.Fatalf("invalid policy accepted: %s", policy)
		}
	}
	var calls int32
	ch := Channel{Config: `{"input_modality":"multimodal","input_length":{"known_range":{"min_tokens":100}}}`}
	if err := ch.CheckInputModality(routingInput(1, "text", "estimated", &calls)); err != nil {
		t.Fatal(err)
	}
	ch.Config = `{"input_modality":"multimodal","input_length":{"known_range":{"min_tokens":-1}}}`
	if err := ch.CheckInputModality(routingInput(1, "text", "estimated", &calls)); err != nil {
		t.Fatalf("specified length exemption: %v", err)
	}
	ch.Config = `{"input_modality":"text"}`
	if err := ch.CheckInputModality(routingInput(1, "multimodal", "estimated", &calls)); !errors.Is(err, ErrNoInputModalityChannel) {
		t.Fatal(err)
	}
}
