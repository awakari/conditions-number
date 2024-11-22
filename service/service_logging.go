package service

import (
	"context"
	"fmt"
	"github.com/awakari/conditions-number/model"
	"log/slog"
)

type serviceLogging struct {
	svc Service
	log *slog.Logger
}

func NewServiceLogging(svc Service, log *slog.Logger) Service {
	return serviceLogging{
		svc: svc,
		log: log,
	}
}

func (sl serviceLogging) Create(ctx context.Context, k string, o model.Op, v float64) (id string, err error) {
	id, err = sl.svc.Create(ctx, k, o, v)
	ll := sl.logLevel(err)
	sl.log.Log(ctx, ll, fmt.Sprintf("Create(k=%s, o=%s, v=%f): id=%s, err=%s", k, o, v, id, err))
	return
}

func (sl serviceLogging) LockCreate(ctx context.Context, id string) (err error) {
	err = sl.svc.LockCreate(ctx, id)
	ll := sl.logLevel(err)
	sl.log.Log(ctx, ll, fmt.Sprintf("LockCreate(id=%s): err=%s", id, err))
	return
}

func (sl serviceLogging) UnlockCreate(ctx context.Context, id string) (err error) {
	err = sl.svc.UnlockCreate(ctx, id)
	ll := sl.logLevel(err)
	sl.log.Log(ctx, ll, fmt.Sprintf("UnlockCreate(id=%s): err=%s", id, err))
	return
}

func (sl serviceLogging) Delete(ctx context.Context, id string) (err error) {
	err = sl.svc.Delete(ctx, id)
	ll := sl.logLevel(err)
	sl.log.Log(ctx, ll, fmt.Sprintf("Delete(id=%s): err=%s", id, err))
	return
}

func (sl serviceLogging) SearchPage(ctx context.Context, k string, v float64, limit uint32, cursor string) (ids []string, err error) {
	ids, err = sl.svc.SearchPage(ctx, k, v, limit, cursor)
	ll := sl.logLevel(err)
	sl.log.Log(ctx, ll, fmt.Sprintf("SearchPage(k=%s, v=%f, limit=%d, cursor=%s): n=%d, err=%s", k, v, limit, cursor, len(ids), err))
	return
}

func (sl serviceLogging) logLevel(err error) (lvl slog.Level) {
	switch err {
	case nil:
		lvl = slog.LevelDebug
	default:
		lvl = slog.LevelError
	}
	return
}
