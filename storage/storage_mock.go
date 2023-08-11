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

func (sm storageMock) Create(ctx context.Context, k string, o model.Op, v float64) (id string, err error) {
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

func (sm storageMock) Delete(ctx context.Context, id string) (err error) {
	switch id {
	case "fail":
		err = ErrInternal
	}
	return
}

func (sm storageMock) Search(ctx context.Context, k string, v float64, consumer func(idd string) (err error)) (n uint64, err error) {
	switch k {
	case "fail":
		err = ErrInternal
	default:
		for i := 0; i < 3; i++ {
			err = consumer(fmt.Sprintf("cond%d", i))
			if err != nil {
				break
			}
			n++
		}
	}
	return
}
