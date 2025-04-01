package yugabyte

import (
	"context"
	"fmt"
	"github.com/awakari/conditions-number/config"
	"github.com/awakari/conditions-number/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yugabyte/pgx/v5/pgxpool"
	"os"
	"testing"
	"time"
)

/*
docker run -d --name yugabyte -p7000:7000 -p9000:9000 -p15433:15433 -p5433:5433 -p9042:9042 \
 yugabytedb/yugabyte:2.25.1.0-b381 bin/yugabyted start \
 --background=false
*/

func TestNewStorage(t *testing.T) {
	if os.Getenv("CI") == "true" {
		t.Skip()
	}

	tblName := fmt.Sprintf("conditions_number_test_%d", time.Now().UnixMicro())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cfgDb := config.DbConfig{
		Host:     "localhost",
		Port:     5433,
		Name:     "conditions_number",
		UserName: "yugabyte",
		Password: "yugabyte",
	}
	cfgDb.Connection.Count.Max = 16
	cfgDb.Table.Name = tblName
	s, err := NewStorage(ctx, cfgDb)
	assert.NotNil(t, s)
	assert.NoError(t, err)
	defer clean(ctx, t, s.(stor), tblName)
	s, err = NewStorage(ctx, cfgDb)
	assert.NoError(t, err)
}

func clean(ctx context.Context, t *testing.T, s stor, tblName string) {
	s.connPool.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tblName))
		require.NoError(t, err)
		return nil
	})
	require.Nil(t, s.Close())
}

func TestStorageImpl_SearchPage(t *testing.T) {
	//
	if os.Getenv("CI") == "true" {
		t.Skip()
	}

	tblName := fmt.Sprintf("conditions_number_test_%d", time.Now().UnixMicro())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cfgDb := config.DbConfig{
		Host:     "localhost",
		Port:     5433,
		Name:     "conditions_number",
		UserName: "yugabyte",
		Password: "yugabyte",
	}
	cfgDb.Connection.Count.Max = 16
	cfgDb.Table.Name = tblName
	s, err := NewStorage(ctx, cfgDb)
	assert.NotNil(t, s)
	assert.NoError(t, err)
	defer clean(ctx, t, s.(stor), tblName)
	//
	cond0, err := s.Create(ctx, "interest1", "salary", model.OpGt, 2.7182818)
	require.Nil(t, err)
	cond1, err := s.Create(ctx, "interest1", "salary", model.OpGte, 3.1415926)
	require.Nil(t, err)
	cond2, err := s.Create(ctx, "interest1", "salary", model.OpEq, 3)
	require.Nil(t, err)
	cond4, err := s.Create(ctx, "interest1", "price", model.OpLte, 123)
	require.Nil(t, err)
	cond5, err := s.Create(ctx, "interest1", "price", model.OpLt, 123)
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
	if os.Getenv("CI") == "true" {
		t.Skip()
	}

	tblName := fmt.Sprintf("conditions_number_test_%d", time.Now().UnixMicro())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cfgDb := config.DbConfig{
		Host:     "localhost",
		Port:     5433,
		Name:     "conditions_number",
		UserName: "yugabyte",
		Password: "yugabyte",
	}
	cfgDb.Connection.Count.Max = 16
	cfgDb.Table.Name = tblName
	s, err := NewStorage(ctx, cfgDb)
	assert.NotNil(t, s)
	assert.NoError(t, err)
	defer clean(ctx, t, s.(stor), tblName)
	//
	var existingId string
	existingId, err = s.Create(ctx, "interest1", "price", model.OpEq, 42)
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
			id, err = s.Create(ctx, "interest1", c.key, c.op, c.val)
			if c.dup {
				assert.Equal(t, existingId, id)
			} else {
				assert.NotEmpty(t, id)
			}
			assert.ErrorIs(t, err, c.err)
		})
	}
}

func TestStorageImpl_Delete(t *testing.T) {
	//
	if os.Getenv("CI") == "true" {
		t.Skip()
	}

	tblName := fmt.Sprintf("conditions_number_test_%d", time.Now().UnixMicro())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cfgDb := config.DbConfig{
		Host:     "localhost",
		Port:     5433,
		Name:     "conditions_number",
		UserName: "yugabyte",
		Password: "yugabyte",
	}
	cfgDb.Connection.Count.Max = 16
	cfgDb.Table.Name = tblName
	s, err := NewStorage(ctx, cfgDb)
	assert.NotNil(t, s)
	assert.NoError(t, err)
	defer clean(ctx, t, s.(stor), tblName)
	//
	id, err := s.Create(ctx, "interest1", "key0", model.OpEq, 3.1415926)
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
			id: "0",
		},
	}
	//
	for k, c := range cases {
		t.Run(k, func(t *testing.T) {
			err = s.Delete(ctx, "interest1", c.id)
			assert.ErrorIs(t, err, c.err)
		})
	}
}
