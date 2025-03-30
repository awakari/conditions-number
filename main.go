package main

import (
	"context"
	"fmt"
	apiGrpc "github.com/awakari/conditions-number/api/grpc"
	"github.com/awakari/conditions-number/config"
	"github.com/awakari/conditions-number/service"
	"github.com/awakari/conditions-number/storage"
	"github.com/awakari/conditions-number/storage/mongo"
	"github.com/awakari/conditions-number/storage/yugabyte"
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
	var stor storage.Storage
	switch cfg.Db.Type {
	case "mongo":
		stor, err = mongo.NewStorage(context.TODO(), cfg.Db)
	case "yugabyte":
		stor, err = yugabyte.NewStorage(context.TODO(), cfg.Db)
	default:
		panic("unknown db type")
	}
	if err != nil {
		panic(err)
	}
	//
	svc := service.NewService(stor)
	svc = service.NewServiceLogging(svc, log)
	//
	log.Info("connected, starting to listen for incoming requests...")
	if err = apiGrpc.Serve(svc, cfg.Api.Port); err != nil {
		panic(err)
	}
}
