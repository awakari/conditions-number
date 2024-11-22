package main

import (
	"context"
	"fmt"
	apiGrpc "github.com/awakari/conditions-number/api/grpc"
	"github.com/awakari/conditions-number/config"
	"github.com/awakari/conditions-number/service"
	"github.com/awakari/conditions-number/storage/mongo"
	"github.com/go-redis/cache/v9"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"os"
)

func main() {
	//
	cfg, err := config.NewConfigFromEnv()
	if err != nil {
		slog.Error(fmt.Sprintf("failed to load the config from env: %s", err))
	}
	opts := slog.HandlerOptions{
		Level: slog.Level(cfg.Log.Level),
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &opts))
	log.Info("starting...")
	//
	stor, err := mongo.NewStorage(context.TODO(), cfg.Db)
	if err != nil {
		panic(err)
	}
	//
	svc := service.NewService(stor)
	svc = service.NewServiceLogging(svc, log)
	if cfg.Cache.Enabled {
		cacheClient := redis.NewClient(&redis.Options{
			Addr:     cfg.Cache.Addr,
			Password: cfg.Cache.Password,
		})
		defer cacheClient.Close()
		cacheSvc := cache.New(&cache.Options{
			Redis:      cacheClient,
			LocalCache: cache.NewTinyLFU(int(cfg.Cache.Local.Size), cfg.Cache.Ttl),
		})
		svc = service.NewCache(svc, cacheSvc, cfg.Cache.Ttl)
	}
	//
	log.Info("connected, starting to listen for incoming requests...")
	if err = apiGrpc.Serve(svc, cfg.Api.Port); err != nil {
		panic(err)
	}
}
