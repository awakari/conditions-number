package service

import (
	"context"
	"github.com/awakari/conditions-number/model"
	"github.com/go-redis/cache/v9"
	"github.com/redis/go-redis/v9"
	"math"
	"strconv"
	"strings"
	"time"
)

type svcCache struct {
	svc          Service
	cache        *cache.Cache
	cacheTtl     time.Duration
	omitAttrKeys map[string]bool
	cacheClient  *redis.Client
}

type cacheValueBytes struct {
	Bytes []byte
}

const keySep = ":"
const keyPrefixCondNum = "conds" + keySep + "num"
const valSep = ","

func NewCache(svc Service, cache *cache.Cache, cacheTtl time.Duration, omitAttrKeys []string, cacheClient *redis.Client) Service {
	omitAttrKeysSet := map[string]bool{}
	for _, key := range omitAttrKeys {
		omitAttrKeysSet[key] = true
	}
	return svcCache{
		svc:          svc,
		cache:        cache,
		cacheTtl:     cacheTtl,
		omitAttrKeys: omitAttrKeysSet,
		cacheClient:  cacheClient,
	}
}

func (sc svcCache) Create(ctx context.Context, k string, o model.Op, v float64) (id string, err error) {
	id, err = sc.svc.Create(ctx, k, o, v)
	if err == nil {
		err = sc.clear(ctx)
	}
	return
}

func (sc svcCache) LockCreate(ctx context.Context, id string) (err error) {
	err = sc.svc.LockCreate(ctx, id)
	return
}

func (sc svcCache) UnlockCreate(ctx context.Context, id string) (err error) {
	err = sc.svc.UnlockCreate(ctx, id)
	return
}

func (sc svcCache) Delete(ctx context.Context, id string) (err error) {
	err = sc.svc.Delete(ctx, id)
	if err == nil {
		err = sc.clear(ctx)
	}
	return
}

func (sc svcCache) SearchPage(ctx context.Context, key string, val float64, limit uint32, cursor string) (ids []string, err error) {
	if !sc.omitAttrKeys[key] && val >= math.MinInt64 && val <= math.MaxInt64 && val == float64(int64(val)) {
		v := new(cacheValueBytes)
		load := func(_ *cache.Item) (result any, err error) {
			var loaded []string
			loaded, err = sc.svc.SearchPage(ctx, key, val, limit, cursor)
			if err == nil {
				result = &cacheValueBytes{
					Bytes: []byte(strings.Join(loaded, valSep)),
				}
			}
			return
		}
		item := &cache.Item{
			Ctx:   ctx,
			Key:   cacheKey(key, int64(val), limit, cursor),
			Value: v,
			TTL:   sc.cacheTtl,
			Do:    load,
			SetNX: true,
		}
		err = sc.cache.Once(item)
		switch err {
		case nil:
			if len(v.Bytes) > 0 {
				ids = strings.Split(string(v.Bytes), valSep)
			}
		default:
			ids, err = sc.svc.SearchPage(ctx, key, val, limit, cursor)
		}
	} else {
		ids, err = sc.svc.SearchPage(ctx, key, val, limit, cursor)
	}
	return
}

func (sc svcCache) clear(ctx context.Context) (err error) {
	var cursor uint64
	for {
		var keys []string
		// SCAN for keys with the prefix
		keys, cursor, err = sc.cacheClient.Scan(ctx, cursor, keyPrefixCondNum+keySep+"*", 10).Result()
		if err == nil {
			// Delete the matching keys
			if len(keys) > 0 {
				_, err = sc.cacheClient.Del(ctx, keys...).Result()
			}
		}
		// Break if the cursor is 0 (end of scan)
		if cursor == 0 || err != nil {
			break
		}
	}
	return
}

func cacheKey(key string, val int64, limit uint32, cursor string) (k string) {
	return keyPrefixCondNum + keySep + key + keySep + strconv.Itoa(int(limit)) + keySep + cursor + keySep + strconv.FormatInt(val, 10)
}
