package config

import (
	"github.com/stretchr/testify/assert"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	os.Setenv("API_PORT", "55555")
	os.Setenv("LOG_LEVEL", "8")
	os.Setenv("DB_TABLE_LOCK_TTL_CREATE", "12m")
	cfg, err := NewConfigFromEnv()
	assert.Nil(t, err)
	assert.Equal(t, uint16(55555), cfg.Api.Port)
	assert.Equal(t, "mongodb://localhost:27017/?retryWrites=true&w=majority", cfg.Db.Uri)
	assert.Equal(t, "conditions-number", cfg.Db.Name)
	assert.Equal(t, "conditions-number", cfg.Db.Table.Name)
	assert.Equal(t, int(slog.LevelError), cfg.Log.Level)
	assert.Equal(t, 12*time.Minute, cfg.Db.Table.LockTtl.Create)
}
