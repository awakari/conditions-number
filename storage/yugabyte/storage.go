package yugabyte

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/awakari/conditions-number/config"
	"github.com/awakari/conditions-number/model"
	"github.com/awakari/conditions-number/storage"
	"github.com/yugabyte/pgx/v5"
	"github.com/yugabyte/pgx/v5/pgxpool"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
)

type stor struct {
	connPool *pgxpool.Pool
	tblName  string
}

const colExternalId = "external_id"
const colInterestId = "interest_id"
const colKey = "key"
const colOperation = "operation"
const colVal = "val"

const idRadix = 16

func NewStorage(ctx context.Context, cfgDb config.DbConfig) (storage.Storage, error) {

	urlBase := fmt.Sprintf(
		"postgres://%s:%s@%s:%d",
		cfgDb.UserName, cfgDb.Password, cfgDb.Host, cfgDb.Port,
	)
	if err := createDb(ctx, cfgDb.Name, cfgDb.UserName, urlBase); err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s/%s", urlBase, cfgDb.Name)
	if cfgDb.Table.Shard {
		url += "?load_balance=true"
	}

	cfgConnPool, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("invalid connection pool config: %w", err)
	}
	cfgConnPool.MaxConns = cfgDb.Connection.Count.Max
	connPool, err := pgxpool.NewWithConfig(ctx, cfgConnPool)
	if err != nil {
		return nil, fmt.Errorf("faled to create connection pool: %w", err)
	}

	if err = createTable(ctx, connPool, cfgDb.Table.Name); err != nil {
		return nil, err
	}
	if err = createIndex(ctx, connPool, cfgDb.Table.Name, colExternalId); err != nil {
		return nil, err
	}
	if err = createIndex(ctx, connPool, cfgDb.Table.Name, colExternalId, colKey, colOperation, colVal); err != nil {
		return nil, err
	}
	return stor{
		connPool: connPool,
		tblName:  cfgDb.Table.Name,
	}, nil
}

func createDb(ctx context.Context, name, userName, urlBase string) error {
	connAdmin, err := pgx.Connect(ctx, fmt.Sprintf("%s/yugabyte", urlBase))
	if err != nil {
		return fmt.Errorf("faled to connect: %w", err)
	}
	defer connAdmin.Close(ctx)

	rows, err := connAdmin.Query(ctx, fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname = '%s';", name))
	if err != nil {
		return fmt.Errorf("faled to check if database %s exists: %w", name, err)
	}
	defer rows.Close()
	if rows.Next() {
		return nil
	}
	if _, err = connAdmin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s;", name)); err != nil {
		return fmt.Errorf("faled to create database %s: %w", name, err)
	}
	if _, err = connAdmin.Exec(ctx, fmt.Sprintf(`GRANT ALL PRIVILEGES ON DATABASE %s TO %s;`, name, userName)); err != nil {
		return fmt.Errorf("faled to grant privileges on database %s to %s: %w", name, userName, err)
	}
	return nil
}

func createTable(ctx context.Context, connPool *pgxpool.Pool, name string) error {
	stmt := fmt.Sprintf(
		`
        CREATE TABLE IF NOT EXISTS %s (
            id  SERIAL PRIMARY KEY,
            %s  TEXT NOT NULL,
            %s  BIGINT NOT NULL,
            %s  TEXT NOT NULL,
            %s  INT NOT NULL,
            %s  NUMERIC NOT NULL,
            UNIQUE (%s, %s)
        );`,
		name,
		colInterestId,
		colExternalId,
		colKey,
		colOperation,
		colVal,
		colExternalId, colInterestId,
	)
	_, err := connPool.Exec(ctx, stmt)
	if err != nil {
		return fmt.Errorf("faled to create table %s: %w, statement: %s", name, err, stmt)
	}
	return nil
}

func createIndex(ctx context.Context, connPool *pgxpool.Pool, tblName string, colNames ...string) error {
	stmt := fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS idx_%s ON %s (%s)`,
		strings.Join(colNames, "_"), tblName, strings.Join(colNames, ", "),
	)
	if _, err := connPool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("faled to create index on columns %s: %w", strings.Join(colNames, ", "), err)
	}
	return nil
}

func (s stor) Close() error {
	s.connPool.Close()
	return nil
}

func (s stor) Create(ctx context.Context, interestId, k string, o model.Op, v float64) (id string, err error) {
	bb := bytes.NewBufferString(k)
	_ = bb.WriteByte(byte(o))
	_ = binary.Write(bb, binary.BigEndian, v)
	hash := fnv.New64()
	_, _ = hash.Write(bb.Bytes())
	extId := int64(hash.Sum64() & math.MaxInt64)
	stmt := fmt.Sprintf(
		`
        INSERT INTO %s(%s, %s, %s, %s, %s) VALUES ($1, $2, $3, $4, $5) 
        ON CONFLICT DO NOTHING;`,
		s.tblName, colInterestId, colExternalId, colKey, colOperation, colVal,
	)
	_, err = s.connPool.Exec(ctx, stmt, interestId, extId, k, o, v)
	if err != nil {
		return "", fmt.Errorf("faled to create condition: %w", err)
	}
	return strconv.FormatInt(extId, idRadix), nil
}

func (s stor) LockCreate(ctx context.Context, id string) (err error) {
	return nil
}

func (s stor) UnlockCreate(ctx context.Context, id string) (err error) {
	return nil
}

func (s stor) Delete(ctx context.Context, interestId, id string) (err error) {
	extId, err := strconv.ParseInt(id, idRadix, 64)
	if err != nil {
		return fmt.Errorf("failed to parse id %s: %w", id, err)
	}
	stmt := fmt.Sprintf(
		`
        DELETE FROM %s 
        WHERE 
            %s = $1 
            AND %s = $2`,
		s.tblName, colInterestId, colExternalId,
	)
	_, err = s.connPool.Exec(ctx, stmt, interestId, extId)
	if err != nil {
		return fmt.Errorf("faled to delete condition: %w", err)
	}
	return nil
}

func (s stor) SearchPage(ctx context.Context, key string, val float64, limit uint32, cursor string) (ids []string, err error) {

	var cursorId int64
	if cursor != "" {
		cursorId, err = strconv.ParseInt(cursor, idRadix, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cursor %s: %w", cursor, err)
		}
	}

	query := fmt.Sprintf(
		`
        SELECT %s 
        FROM %s
        WHERE 
            %s > $1 
        	AND (
                %s = '' 
                OR %s = $2
            )
            AND (
                %s = %d AND %s < $3                
                OR %s = %d AND %s <= $3
                OR %s = %d AND %s = $3
                OR %s = %d AND %s >= $3
                OR %s = %d AND %s > $3
            )
        LIMIT $4`,
		colExternalId,
		s.tblName,
		colExternalId,
		colKey,
		colKey,
		colOperation, model.OpGt, colVal,
		colOperation, model.OpGte, colVal,
		colOperation, model.OpEq, colVal,
		colOperation, model.OpLte, colVal,
		colOperation, model.OpLt, colVal,
	)

	rows, err := s.connPool.Query(ctx, query, cursorId, key, val, limit)
	if err != nil {
		return nil, fmt.Errorf("faled to search conditions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var extId int64
		if err = rows.Scan(&extId); err != nil {
			err = fmt.Errorf("failed to scan row: %w", err)
			break
		}
		ids = append(ids, strconv.FormatInt(extId, idRadix))
	}

	err = errors.Join(err, rows.Err())

	return
}
