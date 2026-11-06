package controller

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/client"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/common/ctxkey"
	dbmodel "github.com/songquanpeng/one-api/model"
	"github.com/songquanpeng/one-api/relay/adaptor/openai"
	billingratio "github.com/songquanpeng/one-api/relay/billing/ratio"
	"github.com/songquanpeng/one-api/relay/channeltype"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestFailedStreamQuotaHTTP(t *testing.T) {
	openai.InitTokenEncoders()
	for _, path := range []string{"/v1/chat/completions", "/v1/completions"} {
		for _, tc := range []struct {
			name, frames       string
			prompt, completion int
			estimate           bool
		}{
			{name: "error-only", frames: `data: {"error":{"message":"fixture failure","code":"fixture_failure"}}`},
			{name: "reported", frames: "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2,\"total_tokens\":6}}\n\n" + `data: {"error":{"message":"fixture failure"}}`, prompt: 4, completion: 2},
			{name: "content-estimate", frames: `data: {"choices":[{"index":0,"text":"hello","delta":{"content":"hello"}}]}`, completion: 1, estimate: true},
			{name: "empty-usage-estimate", frames: "data: {\"choices\":[{\"index\":0,\"text\":\"hello\",\"delta\":{\"content\":\"hello\"}}]}\n\ndata: {\"usage\":{}}", completion: 1, estimate: true},
			{name: "role-only", frames: `data: {"choices":[{"index":0,"delta":{"role":"assistant"}}]}`},
			{name: "reported-zero", frames: "data: {\"choices\":[{\"text\":\"hello\",\"delta\":{\"content\":\"hello\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0,\"total_tokens\":0}}"},
		} {
			t.Run(path+"/"+tc.name, func(t *testing.T) {
				db := setupStreamUsageDB(t)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, tc.frames+"\n\n")
				}))
				defer upstream.Close()
				body := `{"model":"gpt-3.5-turbo","messages":[{"role":"user","content":"hello"}],"prompt":"hello","stream":true,"max_tokens":8}`
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				c.Request = httptest.NewRequest("POST", path, strings.NewReader(body))
				c.Request.Header.Set("Content-Type", "application/json")
				for key, value := range map[string]any{ctxkey.Id: 1, ctxkey.TokenId: 1, ctxkey.TokenName: "fixture", ctxkey.ChannelId: 1, ctxkey.Channel: channeltype.OpenAI, ctxkey.Group: "default", ctxkey.BaseURL: upstream.URL} {
					c.Set(key, value)
				}
				err := RelayTextHelper(c)
				if err == nil {
					t.Fatal("failed stream returned success")
				}
				if strings.Contains(w.Body.String(), "[DONE]") {
					t.Fatalf("failure emitted DONE: %s", w.Body)
				}
				prompt := tc.prompt
				if tc.estimate {
					if path == "/v1/completions" {
						prompt = openai.CountTokenText("hello", "gpt-3.5-turbo")
					} else {
						prompt = 8
					}
				}
				ratio := billingratio.GetModelRatio("gpt-3.5-turbo", channeltype.OpenAI) * billingratio.GetGroupRatio("default")
				cost := int64(math.Ceil((float64(prompt) + float64(tc.completion)*billingratio.GetCompletionRatio("gpt-3.5-turbo", channeltype.OpenAI)) * ratio))
				count := 0
				if prompt+tc.completion > 0 {
					count = 1
					if cost <= 0 && ratio != 0 {
						cost = 1
					}
				}
				deadline := time.Now().Add(3 * time.Second)
				for {
					var user dbmodel.User
					var token dbmodel.Token
					var channel dbmodel.Channel
					var logs []dbmodel.Log
					if e := db.First(&user, 1).Error; e != nil {
						t.Fatal(e)
					}
					if e := db.First(&token, 1).Error; e != nil {
						t.Fatal(e)
					}
					if e := db.First(&channel, 1).Error; e != nil {
						t.Fatal(e)
					}
					if e := db.Find(&logs).Error; e != nil {
						t.Fatal(e)
					}
					if user.Quota == 10000-cost && token.RemainQuota == 10000-cost && user.UsedQuota == cost && token.UsedQuota == cost && channel.UsedQuota == cost && user.RequestCount == count && len(logs) == count {
						if count > 0 && (logs[0].PromptTokens != prompt || logs[0].CompletionTokens != tc.completion || logs[0].Quota != int(cost)) {
							t.Fatalf("log=%+v", logs[0])
						}
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("cost=%d prompt=%d completion=%d count=%d user=%+v token=%+v channelUsed=%d logs=%+v", cost, prompt, tc.completion, count, user, token, channel.UsedQuota, logs)
					}
					time.Sleep(5 * time.Millisecond)
				}
			})
		}
	}
}

func setupStreamUsageDB(t *testing.T) *gorm.DB {
	t.Helper()
	oldDB, oldLogDB, oldClient := dbmodel.DB, dbmodel.LOG_DB, client.HTTPClient
	oldRedis, oldSQLite, oldPostgres := common.RedisEnabled, common.UsingSQLite, common.UsingPostgreSQL
	oldBatch, oldLog, oldInclude := config.BatchUpdateEnabled, config.LogConsumeEnabled, config.EnforceIncludeUsage
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "usage.sqlite")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	dbmodel.DB, dbmodel.LOG_DB = db, db
	client.HTTPClient = &http.Client{Timeout: 2 * time.Second}
	common.RedisEnabled = false
	common.UsingSQLite = true
	common.UsingPostgreSQL = false
	config.BatchUpdateEnabled = false
	config.LogConsumeEnabled = true
	config.EnforceIncludeUsage = false
	t.Cleanup(func() {
		_ = sqlDB.Close()
		dbmodel.DB, dbmodel.LOG_DB, client.HTTPClient = oldDB, oldLogDB, oldClient
		common.RedisEnabled, common.UsingSQLite, common.UsingPostgreSQL = oldRedis, oldSQLite, oldPostgres
		config.BatchUpdateEnabled, config.LogConsumeEnabled, config.EnforceIncludeUsage = oldBatch, oldLog, oldInclude
	})
	if err := db.AutoMigrate(&dbmodel.User{}, &dbmodel.Token{}, &dbmodel.Channel{}, &dbmodel.Log{}); err != nil {
		t.Fatal(err)
	}
	for _, row := range []any{&dbmodel.User{Id: 1, Username: "stream-fixture", Quota: 10000, Group: "default", Status: dbmodel.UserStatusEnabled}, &dbmodel.Token{Id: 1, UserId: 1, Key: "fixture-token", Name: "fixture", RemainQuota: 10000, Status: dbmodel.TokenStatusEnabled, ExpiredTime: -1}, &dbmodel.Channel{Id: 1, Name: "fixture-channel", Type: channeltype.OpenAI, Status: dbmodel.ChannelStatusEnabled}} {
		if err := db.Create(row).Error; err != nil {
			t.Fatal(fmt.Errorf("fixture create: %w", err))
		}
	}
	return db
}
