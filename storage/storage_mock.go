package storage

import (
	"context"
	"fmt"
	"github.com/awakari/conditions-number/model"
)

type storageMock struct {
}

func NewStorageMock() Storage {
	return storageMock{}
}

func (sm storageMock) Close() error {
	return nil
}

func (sm storageMock) Create(ctx context.Context, interestId, k string, o model.Op, v float64) (id string, err error) {
	switch k {
	case "fail":
		err = ErrInternal
	case "conflict":
		err = ErrConflict
	default:
		id = "cond0"
	}
	return
}

func (sm storageMock) LockCreate(ctx context.Context, id string) (err error) {
	switch id {
	case "missing":
		err = ErrNotFound
	}
	return
}

func (sm storageMock) UnlockCreate(ctx context.Context, id string) (err error) {
	return
}

func (sm storageMock) Delete(ctx context.Context, interestId, id string) (err error) {
	switch id {
	case "fail":
		err = ErrInternal
	}
	return
}

func (sm storageMock) SearchPage(ctx context.Context, key string, val float64, limit uint32, cursor string) (ids []string, err error) {
	switch key {
	case "fail":
		err = ErrInternal
	default:
		for i := uint32(0); i < limit; i++ {
			ids = append(ids, fmt.Sprintf("cond%d", i))
		}
	}
	return
}
