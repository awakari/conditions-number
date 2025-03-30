package service

import (
	"context"
	"github.com/awakari/conditions-number/model"
	"github.com/awakari/conditions-number/storage"
)

type Service interface {
	Create(ctx context.Context, interestId, k string, o model.Op, v float64) (id string, err error)
	LockCreate(ctx context.Context, id string) (err error)
	UnlockCreate(ctx context.Context, id string) (err error)
	Delete(ctx context.Context, interestId, id string) (err error)
	SearchPage(ctx context.Context, key string, val float64, limit uint32, cursor string) (ids []string, err error)
}

type service struct {
	stor storage.Storage
}

func NewService(stor storage.Storage) Service {
	return service{
		stor: stor,
	}
}

func (svc service) Create(ctx context.Context, interestId, k string, o model.Op, v float64) (id string, err error) {
	id, err = svc.stor.Create(ctx, interestId, k, o, v)
	return
}

func (svc service) LockCreate(ctx context.Context, id string) (err error) {
	return svc.stor.LockCreate(ctx, id)
}

func (svc service) UnlockCreate(ctx context.Context, id string) (err error) {
	return svc.stor.UnlockCreate(ctx, id)
}

func (svc service) Delete(ctx context.Context, interestId, id string) (err error) {
	return svc.stor.Delete(ctx, interestId, id)
}

func (svc service) SearchPage(ctx context.Context, key string, val float64, limit uint32, cursor string) (ids []string, err error) {
	ids, err = svc.stor.SearchPage(ctx, key, val, limit, cursor)
	return
}
