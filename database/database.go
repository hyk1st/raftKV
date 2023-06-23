package database

import (
	"raftKV/datastruct/list"
	"raftKV/datastruct/set"
	"raftKV/datastruct/sortedset"
)

type SDS struct {
	TTL int64
	VAL string
}

type DB struct {
	KV   map[string]*SDS
	SET  map[string]*set.Set
	ZSET map[string]*sortedset.SortedSet
	HASH map[string]map[string]string
	LIST map[string]*list.QuickList
}

func NewDB() *DB {
	var db DB
	db.KV = make(map[string]*SDS)
	db.SET = make(map[string]*set.Set)
	db.ZSET = make(map[string]*sortedset.SortedSet)
	db.HASH = make(map[string]map[string]string)
	db.LIST = make(map[string]*list.QuickList)
	return &db
}
