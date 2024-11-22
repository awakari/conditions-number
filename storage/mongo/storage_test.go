package mongo

import (
	"context"
	"fmt"
	"github.com/awakari/conditions-number/config"
	"github.com/awakari/conditions-number/model"
	"github.com/awakari/conditions-number/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"os"
	"testing"
	"time"
)

var dbUri = os.Getenv("DB_URI_TEST_MONGO")

func TestNewStorage(t *testing.T) {
	//
	collName := fmt.Sprintf("conditions-number-test-%d", time.Now().UnixMicro())
	dbCfg := config.DbConfig{
		Uri:  dbUri,
		Name: "conditions-number",
	}
	dbCfg.Table.Name = collName
	dbCfg.Table.Shard = false
	dbCfg.Tls.Enabled = true
	dbCfg.Tls.Insecure = true
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	s, err := NewStorage(ctx, dbCfg)
	assert.NotNil(t, s)
	assert.Nil(t, err)
	//
	clear(ctx, t, s.(storageImpl))
}

func clear(ctx context.Context, t *testing.T, s storageImpl) {
	require.Nil(t, s.coll.Drop(ctx))
	require.Nil(t, s.Close())
}

func TestStorageImpl_SearchPage(t *testing.T) {
	//
	collName := fmt.Sprintf("conditions-number-test-%d", time.Now().UnixMicro())
	dbCfg := config.DbConfig{
		Uri:  dbUri,
		Name: "conditions-number",
	}
	dbCfg.Table.Name = collName
	dbCfg.Tls.Enabled = true
	dbCfg.Tls.Insecure = true
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Minute)
	defer cancel()
	s, err := NewStorage(ctx, dbCfg)
	require.Nil(t, err)
	defer clear(ctx, t, s.(storageImpl))
	//
	cond0, err := s.Create(ctx, "salary", model.OpGt, 2.7182818)
	require.Nil(t, err)
	cond1, err := s.Create(ctx, "salary", model.OpGte, 3.1415926)
	require.Nil(t, err)
	cond2, err := s.Create(ctx, "salary", model.OpEq, 3)
	require.Nil(t, err)
	cond4, err := s.Create(ctx, "price", model.OpLte, 123)
	require.Nil(t, err)
	cond5, err := s.Create(ctx, "price", model.OpLt, 123)
	require.Nil(t, err)
	//
	cases := map[string]struct {
		key    string
		val    float64
		limit  uint32
		cursor string
		ids    []string
		err    error
	}{
		"salary = 3": {
			key:   "salary",
			val:   3,
			limit: 10,
			ids: []string{
				cond0,
				cond2,
			},
		},
		"salary = 3.1415926": {
			key:   "salary",
			val:   3.1415926,
			limit: 10,
			ids: []string{
				cond0,
				cond1,
			},
		},
		"salary = 2": {
			key:   "salary",
			val:   2,
			limit: 10,
			ids:   []string{},
		},
		"salary = 2.8": {
			key:   "salary",
			val:   2.8,
			limit: 10,
			ids: []string{
				cond0,
			},
		},
		"SourceId = 1000": {
			key:   "SourceId",
			val:   1000,
			limit: 10,
			ids:   []string{},
		},
		"price = 122.99": {
			key:   "price",
			val:   122.99,
			limit: 10,
			ids: []string{
				cond4,
				cond5,
			},
		},
		"price = 123.00": {
			key:   "price",
			val:   123.0,
			limit: 10,
			ids: []string{
				cond4,
			},
		},
		"price = 123.99": {
			key:   "price",
			val:   123.99,
			limit: 10,
			ids:   []string{},
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			var ids []string
			ids, err = s.SearchPage(ctx, c.key, c.val, c.limit, c.cursor)
			assert.Equal(t, len(c.ids), len(ids))
			assert.ErrorIs(t, err, c.err)
			assert.ElementsMatch(t, c.ids, ids)
		})
	}
}

func TestStorageImpl_Create(t *testing.T) {
	//
	collName := fmt.Sprintf("conditions-number-test-%d", time.Now().UnixMicro())
	dbCfg := config.DbConfig{
		Uri:  dbUri,
		Name: "conditions-number",
	}
	dbCfg.Table.Name = collName
	dbCfg.Tls.Enabled = true
	dbCfg.Tls.Insecure = true
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	s, err := NewStorage(ctx, dbCfg)
	require.Nil(t, err)
	defer clear(ctx, t, s.(storageImpl))
	//
	var existingId string
	existingId, err = s.Create(ctx, "price", model.OpEq, 42)
	require.Nil(t, err)
	//
	cases := map[string]struct {
		key string
		op  model.Op
		val float64
		dup bool
		err error
	}{
		"different key": {
			key: "",
			op:  model.OpEq,
			val: 42,
		},
		"different op": {
			key: "price",
			op:  model.OpLt,
			val: 42,
		},
		"different values": {
			key: "",
			op:  model.OpEq,
			val: 43,
		},
		"duplicate": {
			key: "price",
			op:  model.OpEq,
			val: 42,
			dup: true,
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			var id string
			id, err = s.Create(ctx, c.key, c.op, c.val)
			if c.dup {
				assert.Equal(t, existingId, id)
			} else {
				assert.NotEmpty(t, id)
			}
			assert.ErrorIs(t, err, c.err)
		})
	}
}

func TestStorageImpl_LockCreate(t *testing.T) {
	//
	collName := fmt.Sprintf("conditions-number-test-%d", time.Now().UnixMicro())
	dbCfg := config.DbConfig{
		Uri:  dbUri,
		Name: "conditions-number",
	}
	dbCfg.Table.Name = collName
	dbCfg.Table.LockTtl.Create = 1 * time.Second
	dbCfg.Tls.Enabled = true
	dbCfg.Tls.Insecure = true
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	s, err := NewStorage(ctx, dbCfg)
	require.Nil(t, err)
	defer clear(ctx, t, s.(storageImpl))
	//
	cases := map[string]struct {
		delay time.Duration
		err   error
	}{
		"lock present": {
			err: storage.ErrConflict,
		},
		"lock expired": {
			delay: 1 * time.Second,
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			var existingId string
			existingId, err = s.Create(ctx, "key0", model.OpEq, 42)
			require.Nil(t, err)
			err = s.LockCreate(ctx, existingId) // locks for 1 seconds
			require.Nil(t, err)
			time.Sleep(c.delay)
			var id string
			id, err = s.Create(ctx, "key0", model.OpEq, 42)
			if c.err == nil {
				assert.Equal(t, existingId, id)
			}
			assert.ErrorIs(t, err, c.err)
		})
	}
}

func TestStorageImpl_LockCreate_Missing(t *testing.T) {
	//
	collName := fmt.Sprintf("conditions-number-test-%d", time.Now().UnixMicro())
	dbCfg := config.DbConfig{
		Uri:  dbUri,
		Name: "conditions-number",
	}
	dbCfg.Table.Name = collName
	dbCfg.Table.LockTtl.Create = 1 * time.Second
	dbCfg.Tls.Enabled = true
	dbCfg.Tls.Insecure = true
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	s, err := NewStorage(ctx, dbCfg)
	require.Nil(t, err)
	defer clear(ctx, t, s.(storageImpl))
	//
	err = s.LockCreate(ctx, primitive.NewObjectID().Hex()) // locks for 1 seconds
	assert.ErrorIs(t, err, storage.ErrNotFound)
}

func TestStorageImpl_UnlockCreate(t *testing.T) {
	//
	collName := fmt.Sprintf("conditions-number-test-%d", time.Now().UnixMicro())
	dbCfg := config.DbConfig{
		Uri:  dbUri,
		Name: "conditions-number",
	}
	dbCfg.Table.Name = collName
	dbCfg.Table.LockTtl.Create = 1 * time.Minute
	dbCfg.Tls.Enabled = true
	dbCfg.Tls.Insecure = true
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	s, err := NewStorage(ctx, dbCfg)
	require.Nil(t, err)
	defer clear(ctx, t, s.(storageImpl))
	//
	var existingId string
	existingId, err = s.Create(ctx, "foo", model.OpEq, 3.1415926)
	require.Nil(t, err)
	//
	err = s.LockCreate(ctx, existingId) // locks for 1 minute, lock count -> 1
	require.Nil(t, err)
	//
	_, err = s.Create(ctx, "foo", model.OpEq, 3.1415926)
	assert.ErrorIs(t, err, storage.ErrConflict)
	//
	err = s.LockCreate(ctx, existingId) // locks for 1 minute, lock count -> 2
	require.Nil(t, err)
	//
	_, err = s.Create(ctx, "foo", model.OpEq, 3.1415926)
	assert.ErrorIs(t, err, storage.ErrConflict)
	//
	err = s.UnlockCreate(ctx, existingId) // unlocks, lock count -> 1
	require.Nil(t, err)
	//
	_, err = s.Create(ctx, "foo", model.OpEq, 3.1415926)
	assert.ErrorIs(t, err, storage.ErrConflict)
	//
	err = s.UnlockCreate(ctx, existingId) // unlocks, lock count -> 0
	require.Nil(t, err)
	//
	var id string
	id, err = s.Create(ctx, "foo", model.OpEq, 3.1415926)
	assert.Nil(t, err)
	assert.Equal(t, existingId, id)
}

func TestStorageImpl_Delete(t *testing.T) {
	//
	collName := fmt.Sprintf("conditions-number-test-%d", time.Now().UnixMicro())
	dbCfg := config.DbConfig{
		Uri:  dbUri,
		Name: "conditions-number",
	}
	dbCfg.Table.Name = collName
	dbCfg.Tls.Enabled = true
	dbCfg.Tls.Insecure = true
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	s, err := NewStorage(ctx, dbCfg)
	require.Nil(t, err)
	defer clear(ctx, t, s.(storageImpl))
	//
	id, err := s.Create(ctx, "key0", model.OpEq, 3.1415926)
	require.Nil(t, err)
	//
	cases := map[string]struct {
		id  string
		err error
	}{
		"ok": {
			id: id,
		},
		"not found is ok": {
			id: primitive.NewObjectID().Hex(),
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			err = s.Delete(ctx, c.id)
			assert.ErrorIs(t, err, c.err)
		})
	}
}
