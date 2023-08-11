package service

import (
	"context"
	"github.com/awakari/conditions-number/model"
	"github.com/awakari/conditions-number/storage"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slog"
	"testing"
)

func TestService_Create(t *testing.T) {
	//
	svc := NewService(storage.NewStorageMock())
	svc = NewServiceLogging(svc, slog.Default())
	cases := map[string]struct {
		key string
		val float64
		err error
	}{
		"ok": {
			key: "category",
			val: 42,
		},
		"fail": {
			key: "fail",
			err: storage.ErrInternal,
		},
		"conflict": {
			key: "conflict",
			err: storage.ErrConflict,
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			id, err := svc.Create(context.TODO(), c.key, model.OpEq, c.val)
			if c.err == nil {
				assert.Equal(t, "cond0", id)
			}
			assert.ErrorIs(t, err, c.err)
		})
	}
}

func TestService_LockCreate(t *testing.T) {
	//
	svc := NewService(storage.NewStorageMock())
	svc = NewServiceLogging(svc, slog.Default())
	cases := map[string]struct {
		id  string
		err error
	}{
		"ok": {
			id: "cond0",
		},
		"missing": {
			id:  "missing",
			err: storage.ErrNotFound,
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			err := svc.LockCreate(context.TODO(), c.id)
			assert.ErrorIs(t, err, c.err)
		})
	}
}

func TestService_Delete(t *testing.T) {
	//
	svc := NewService(storage.NewStorageMock())
	svc = NewServiceLogging(svc, slog.Default())
	cases := map[string]struct {
		id  string
		err error
	}{
		"ok": {
			id: "cond0",
		},
		"fail": {
			id:  "fail",
			err: storage.ErrInternal,
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			err := svc.Delete(context.TODO(), c.id)
			assert.ErrorIs(t, err, c.err)
		})
	}
}

func TestService_Search(t *testing.T) {
	//
	svc := NewService(storage.NewStorageMock())
	svc = NewServiceLogging(svc, slog.Default())
	cases := map[string]struct {
		key string
		val float64
		n   uint64
		err error
	}{
		"ok1": {
			key: "amount",
			val: 1.23,
			n:   3,
		},
		"ok2": {
			key: "power",
			val: 2.34,
			n:   3,
		},
		"fail": {
			key: "fail",
			val: 0,
			err: storage.ErrInternal,
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			var subIds []string
			consumer := func(subId string) (err error) {
				subIds = append(subIds, subId)
				return
			}
			n, err := svc.Search(context.TODO(), c.key, c.val, consumer)
			assert.Equal(t, c.n, n)
			assert.Equal(t, int(c.n), len(subIds))
			assert.ErrorIs(t, err, c.err)
		})
	}
}
