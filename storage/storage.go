package storage

import (
	"context"
	"errors"
	"github.com/awakari/conditions-number/model"
	"io"
)

type Storage interface {
	io.Closer
	Create(ctx context.Context, k string, o model.Op, v float64) (id string, err error)
	LockCreate(ctx context.Context, id string) (err error)
	UnlockCreate(ctx context.Context, id string) (err error)
	Delete(ctx context.Context, id string) (err error)
	SearchPage(ctx context.Context, key string, val float64, limit uint32, cursor string) (ids []string, err error)
}

var ErrInternal = errors.New("internal failure")

var ErrConflict = errors.New("already exists")

var ErrNotFound = errors.New("not found")
