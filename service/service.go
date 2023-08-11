package service

import (
	"context"
	"github.com/awakari/conditions-number/model"
	"github.com/awakari/conditions-number/storage"
)

type Service interface {
	Create(ctx context.Context, k string, o model.Op, v float64) (id string, err error)
	LockCreate(ctx context.Context, id string) (err error)
	UnlockCreate(ctx context.Context, id string) (err error)
	Delete(ctx context.Context, id string) (err error)
	Search(ctx context.Context, k string, v float64, consumer func(id string) (err error)) (n uint64, err error)
}

type service struct {
	stor storage.Storage
}

func NewService(stor storage.Storage) Service {
	return service{
		stor: stor,
	}
}

func (svc service) Create(ctx context.Context, k string, o model.Op, v float64) (id string, err error) {
	id, err = svc.stor.Create(ctx, k, o, v)
	return
}

func (svc service) LockCreate(ctx context.Context, id string) (err error) {
	return svc.stor.LockCreate(ctx, id)
}

func (svc service) UnlockCreate(ctx context.Context, id string) (err error) {
	return svc.stor.UnlockCreate(ctx, id)
}

func (svc service) Delete(ctx context.Context, id string) (err error) {
	return svc.stor.Delete(ctx, id)
}

func (svc service) Search(ctx context.Context, k string, v float64, consumer func(id string) (err error)) (n uint64, err error) {
	n, err = svc.stor.Search(ctx, k, v, consumer)
	return
}
