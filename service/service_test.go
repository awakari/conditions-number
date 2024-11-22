package service

import (
	"context"
	"github.com/awakari/conditions-number/model"
	"github.com/awakari/conditions-number/storage"
	"github.com/stretchr/testify/assert"
	"log/slog"
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

func TestService_SearchPage(t *testing.T) {
	//
	svc := NewService(storage.NewStorageMock())
	svc = NewServiceLogging(svc, slog.Default())
	cases := map[string]struct {
		key   string
		val   float64
		limit uint32
		n     int
		err   error
	}{
		"ok1": {
			key:   "amount",
			val:   1.23,
			limit: 10,
			n:     10,
		},
		"ok2": {
			key:   "power",
			val:   2.34,
			limit: 3,
			n:     3,
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
			var ids []string
			ids, err := svc.SearchPage(context.TODO(), c.key, c.val, c.limit, "")
			assert.Equal(t, c.n, len(ids))
			assert.ErrorIs(t, err, c.err)
		})
	}
}
