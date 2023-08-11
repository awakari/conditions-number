package mongo

import "github.com/awakari/conditions-number/model"

type condition struct {
	Id  string   `bson:"_id"`
	Key string   `bson:"key"`
	Op  model.Op `bson:"op"`
	Val float64  `bson:"val"`
}

const attrId = "_id"
const attrKey = "key"
const attrOp = "op"
const attrVal = "val"
const attrCreateLockTime = "create_lock_time"
const attrCreateLockCount = "create_lock_count"
