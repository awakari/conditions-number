package config

import (
	"github.com/kelseyhightower/envconfig"
	"time"
)

type Config struct {
	Api struct {
		Port uint16 `envconfig:"API_PORT" default:"50051" required:"true"`
	}
	Db  DbConfig
	Log struct {
		Level int `envconfig:"LOG_LEVEL" default:"-4" required:"true"`
	}
}

type DbConfig struct {
	Type     string `envconfig:"DB_TYPE" default:"mongo" required:"true"`
	Uri      string `envconfig:"DB_URI" default:"mongodb://localhost:27017/?retryWrites=true&w=majority" required:"true"`
	Host     string `envconfig:"DB_HOST" default:"127.0.0.1" required:"true"`
	Port     uint16 `envconfig:"DB_PORT" default:"5433" required:"true"`
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
	Connection struct {
		Count struct {
			Max int32 `envconfig:"DB_CONNECTION_COUNT_MAX" default:"16" required:"true"`
		}
	}
}

func NewConfigFromEnv() (cfg Config, err error) {
	err = envconfig.Process("", &cfg)
	return
}
