package mongo

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/awakari/conditions-number/config"
	"github.com/awakari/conditions-number/model"
	"github.com/awakari/conditions-number/storage"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

type storageImpl struct {
	conn          *mongo.Client
	db            *mongo.Database
	coll          *mongo.Collection
	collRo        *mongo.Collection
	createLockTtl time.Duration
}

var indices = []mongo.IndexModel{
	// unique index
	{
		Keys: bson.D{
			{
				Key:   attrKey,
				Value: 1,
			},
			{
				Key:   attrOp,
				Value: 1,
			},
			{
				Key:   attrVal,
				Value: 1,
			},
		},
		Options: options.
			Index().
			SetUnique(true),
	},
}
var projId = bson.D{
	{
		Key:   attrId,
		Value: 1,
	},
}
var optsSrvApi = options.ServerAPI(options.ServerAPIVersion1)
var optsUpsert = options.
	FindOneAndUpdate().
	SetUpsert(true).
	SetReturnDocument(options.After).
	SetProjection(projId)
var optsFindPage = options.
	Find().
	SetProjection(projId).
	SetSort(projId)
var clauseCreateLockMissing = bson.M{
	"$or": []bson.M{
		{
			attrCreateLockTime: bson.M{
				"$exists": false,
			},
		},
		{
			attrCreateLockCount: bson.M{
				"$lt": 1,
			},
		},
	},
}

func NewStorage(ctx context.Context, cfgDb config.DbConfig) (s storage.Storage, err error) {
	clientOpts := options.
		Client().
		ApplyURI(cfgDb.Uri).
		SetServerAPIOptions(optsSrvApi)
	if cfgDb.Tls.Enabled {
		clientOpts = clientOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: cfgDb.Tls.Insecure})
	}
	if len(cfgDb.UserName) > 0 {
		auth := options.Credential{
			Username:    cfgDb.UserName,
			Password:    cfgDb.Password,
			PasswordSet: len(cfgDb.Password) > 0,
		}
		clientOpts = clientOpts.SetAuth(auth)
	}
	conn, err := mongo.Connect(ctx, clientOpts)
	var stor storageImpl
	if err == nil {
		db := conn.Database(cfgDb.Name)
		coll := db.Collection(cfgDb.Table.Name)
		stor.conn = conn
		stor.db = db
		stor.coll = coll
		stor.collRo = db.Collection(cfgDb.Table.Name, options.
			Collection().
			SetReadPreference(readpref.SecondaryPreferred()))
		stor.createLockTtl = cfgDb.Table.LockTtl.Create
		_, err = stor.ensureIndices(ctx)
	}
	if err == nil && cfgDb.Table.Shard {
		err = stor.shardCollection(ctx)
	}
	if err == nil {
		s = stor
	}
	return
}

func (s storageImpl) ensureIndices(ctx context.Context) ([]string, error) {
	return s.coll.Indexes().CreateMany(ctx, indices)
}

func (s storageImpl) shardCollection(ctx context.Context) (err error) {
	adminDb := s.conn.Database("admin")
	cmd := bson.D{
		{
			Key:   "shardCollection",
			Value: fmt.Sprintf("%s.%s", s.db.Name(), s.coll.Name()),
		},
		{
			Key: "key",
			Value: bson.D{
				{
					Key:   attrKey,
					Value: 1,
				},
				{
					Key:   attrOp,
					Value: 1,
				},
				{
					Key:   attrVal,
					Value: "hashed",
				},
			},
		},
	}
	err = adminDb.RunCommand(ctx, cmd).Err()
	return
}

func (s storageImpl) Close() error {
	return s.conn.Disconnect(context.TODO())
}

func (s storageImpl) Create(ctx context.Context, _, k string, o model.Op, v float64) (id string, err error) {
	maxLockTime := time.Now().UTC().Add(-s.createLockTtl)
	clauseCreateLockExpired := bson.M{
		attrCreateLockTime: bson.M{
			"$lt": maxLockTime,
		},
	}
	q := bson.M{
		attrKey: k,
		attrOp:  o,
		attrVal: v,
		"$or": []bson.M{
			clauseCreateLockExpired,
			clauseCreateLockMissing,
		},
	}
	u := bson.M{
		"$set": bson.M{
			attrKey: k,
			attrOp:  o,
			attrVal: v,
		},
	}
	result := s.coll.FindOneAndUpdate(ctx, q, u, optsUpsert)
	var rec condition
	if err == nil {
		err = result.Decode(&rec)
	}
	if err == nil {
		id = rec.Id
	}
	err = decodeError(err)
	return
}

func (s storageImpl) LockCreate(ctx context.Context, id string) (err error) {
	var oid primitive.ObjectID
	oid, err = primitive.ObjectIDFromHex(id)
	var result *mongo.UpdateResult
	if err == nil {
		u := bson.M{
			"$set": bson.M{
				attrCreateLockTime: time.Now().UTC(),
			},
			"$inc": bson.M{
				attrCreateLockCount: 1,
			},
		}
		result, err = s.coll.UpdateByID(ctx, oid, u)
		err = decodeError(err)
	}
	if err == nil && result.MatchedCount < 1 {
		err = fmt.Errorf("%w: id=%s", storage.ErrNotFound, id)
	}
	return
}

func (s storageImpl) UnlockCreate(ctx context.Context, id string) (err error) {
	var oid primitive.ObjectID
	oid, err = primitive.ObjectIDFromHex(id)
	if err == nil {
		q := bson.M{
			attrId: oid,
			attrCreateLockCount: bson.M{
				"$gt": 0, // decrement if > 0, otherwise just skip
			},
		}
		u := bson.M{
			"$inc": bson.M{
				attrCreateLockCount: -1,
			},
		}
		_, err = s.coll.UpdateOne(ctx, q, u)
	}
	err = decodeError(err)
	return
}

func (s storageImpl) Delete(ctx context.Context, _, id string) (err error) {
	var oid primitive.ObjectID
	oid, err = primitive.ObjectIDFromHex(id)
	if err == nil {
		q := bson.M{
			attrId: oid,
		}
		_, err = s.coll.DeleteOne(ctx, q)
	}
	err = decodeError(err)
	return
}

func (s storageImpl) SearchPage(ctx context.Context, key string, val float64, limit uint32, cursor string) (ids []string, err error) {
	var cursorObjId primitive.ObjectID
	switch cursor {
	case "":
		cursorObjId = primitive.NilObjectID
	default:
		cursorObjId, err = primitive.ObjectIDFromHex(cursor)
	}
	var cur *mongo.Cursor
	if err == nil {
		q := searchQuery(key, val, cursorObjId)
		cur, err = s.collRo.Find(ctx, q, optsFindPage.SetLimit(int64(limit)))
	}
	if err == nil {
		defer cur.Close(ctx)
		for cur.Next(ctx) {
			var rec condition
			err = cur.Decode(&rec)
			if err == nil {
				ids = append(ids, rec.Id)
			}
			if err != nil {
				break
			}
		}
	}
	err = decodeError(err)
	return
}

func searchQuery(k string, v float64, cursor primitive.ObjectID) (q bson.M) {
	return bson.M{
		"$and": []bson.M{
			{
				attrId: bson.M{
					"$gt": cursor,
				},
			},
			{
				"$or": []bson.M{
					{
						attrKey: "",
					},
					{
						attrKey: k,
					},
				},
			},
			{
				"$or": []bson.M{
					{
						"$and": []bson.M{
							{
								attrOp: model.OpGt,
							},
							{
								attrVal: bson.M{
									"$lt": v,
								},
							},
						},
					},
					{
						"$and": []bson.M{
							{
								attrOp: model.OpGte,
							},
							{
								attrVal: bson.M{
									"$lte": v,
								},
							},
						},
					},
					{
						"$and": []bson.M{
							{
								attrOp: model.OpEq,
							},
							{
								attrVal: v,
							},
						},
					},
					{
						"$and": []bson.M{
							{
								attrOp: model.OpLte,
							},
							{
								attrVal: bson.M{
									"$gte": v,
								},
							},
						},
					},
					{
						"$and": []bson.M{
							{
								attrOp: model.OpLt,
							},
							{
								attrVal: bson.M{
									"$gt": v,
								},
							},
						},
					},
				},
			},
		},
	}
}

func decodeError(src error) (dst error) {
	switch {
	case src == nil:
	case mongo.IsDuplicateKeyError(src):
		dst = fmt.Errorf("%w: %s", storage.ErrConflict, src)
	default:
		dst = fmt.Errorf("%w: %s", storage.ErrInternal, src)
	}
	return
}
