package main

import (
	"context"
	apiGrpc "github.com/awakari/conditions-number/api/grpc"
	"github.com/awakari/conditions-number/config"
	"github.com/awakari/conditions-number/service"
	"github.com/awakari/conditions-number/storage/mongo"
	"golang.org/x/exp/slog"
	"os"
)

func main() {
	//
	cfg, err := config.NewConfigFromEnv()
	if err != nil {
		slog.Error("failed to load the config from env", err)
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
	//
	log.Info("connected, starting to listen for incoming requests...")
	if err = apiGrpc.Serve(svc, cfg.Api.Port); err != nil {
		panic(err)
	}
}
