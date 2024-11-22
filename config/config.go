package config

import (
	"github.com/kelseyhightower/envconfig"
	"time"
)

type Config struct {
	Api struct {
		Port uint16 `envconfig:"API_PORT" default:"50051" required:"true"`
	}
	Cache CacheConfig
	Db    DbConfig
	Log   struct {
		Level int `envconfig:"LOG_LEVEL" default:"-4" required:"true"`
	}
}

type CacheConfig struct {
	Enabled bool `envconfig:"CACHE_ENABLED" default:"false" required:"true"`
	Local   struct {
		Size uint32 `envconfig:"CACHE_LOCAL_SIZE" default:"1000000" required:"true"`
	}
	Ttl      time.Duration `envconfig:"CACHE_TTL" default:"1m" required:"true"`
	Addr     string        `envconfig:"CACHE_ADDR" default:"cache-keydb:6379" required:"true"`
	Password string        `envconfig:"CACHE_PASSWORD" required:"false"`
}

type DbConfig struct {
	Uri      string `envconfig:"DB_URI" default:"mongodb://localhost:27017/?retryWrites=true&w=majority" required:"true"`
	Name     string `envconfig:"DB_NAME" default:"conditions-number" required:"true"`
	UserName string `envconfig:"DB_USERNAME" default:""`
	Password string `envconfig:"DB_PASSWORD" default:""`
	Table    struct {
		Name    string `envconfig:"DB_NAME" default:"conditions-number" required:"true"`
		LockTtl struct {
			Create time.Duration `envconfig:"DB_TABLE_LOCK_TTL_CREATE" default:"1000s"`
		}
		Shard bool `envconfig:"DB_TABLE_SHARD" default:"true"`
	}
	Tls struct {
		Enabled  bool `envconfig:"DB_TLS_ENABLED" default:"false" required:"true"`
		Insecure bool `envconfig:"DB_TLS_INSECURE" default:"false" required:"true"`
	}
}

func NewConfigFromEnv() (cfg Config, err error) {
	err = envconfig.Process("", &cfg)
	return
}
