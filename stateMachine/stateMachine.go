package stateMachine

import (
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"io"
	"math"
	"raftKV/database"
	"raftKV/datastruct/list"
	"raftKV/datastruct/set"
	"raftKV/datastruct/sortedset"
	"raftKV/resp"
	"strconv"
	"strings"
	"time"
)

var OK = []byte("ok")
var TimeSmall = []byte("time should not < 0")
var NotDigit = []byte("value are not digit")
var NotExist = []byte("key not exist")
var AlreadyExist = []byte("key already exist")
var maxInt64 int64 = math.MaxInt64

type ExampleStateMachine struct {
	ShardID   uint64
	ReplicaID uint64
	DB        *database.DB
}

// NewExampleStateMachine creates and return a new ExampleStateMachine object.
func NewExampleStateMachine(shardID uint64,
	replicaID uint64) sm.IStateMachine {
	return &ExampleStateMachine{
		ShardID:   shardID,
		ReplicaID: replicaID,
		DB:        database.NewDB(),
	}
}

// Lookup performs local lookup on the ExampleStateMachine instance. In this example,
// we always return the Count value as a little endian binary encoded byte
// slice.
func (s *ExampleStateMachine) Lookup(query interface{}) (interface{}, error) {
	oper := query.([][]byte)
	typ := strings.ToLower(string(oper[0]))

	key := string(oper[1])
	switch typ {
	case "get":
		sds, ok := s.DB.KV[key]
		if !ok || (*sds).TTL < time.Now().UnixMilli() {
			if ok {
				delete(s.DB.KV, key)
			}
			return nil, errors.New("没这个键，或者已过期")
		}
		return (*sds).VAL, nil
	case "ttl":
		sds, ok := s.DB.KV[key]
		if !ok || (*sds).TTL < time.Now().UnixMilli() {
			if ok {
				delete(s.DB.KV, key)
			}
			return nil, errors.New("没这个键，或者已过期")
		}
		return fmt.Sprintf("%d", ((*sds).TTL-time.Now().UnixMilli())/1000), nil
	case "exists":
		v, ok := s.DB.KV[key]
		if ok && v.TTL < time.Now().UnixNano() {
			return "true", nil
		} else {
			if ok {
				delete(s.DB.KV, key)
			}
			return "false", nil
		}
	case "strlen":
		sds, ok := s.DB.KV[key]
		if !ok || (*sds).TTL < time.Now().UnixMilli() {
			if ok {
				delete(s.DB.KV, key)
			}
			return "no such key", nil
		}
		return strconv.Itoa(len((*sds).VAL)), nil
	case "lrange":
		list, ok := s.DB.LIST[key]
		if !ok {
			return "no such key", nil
		}
		st, err := strconv.Atoi(string(oper[2]))
		if err != nil {
			return "start not digit", nil
		}
		end, err := strconv.Atoi(string(oper[3]))
		if err != nil {
			return "end not digit", nil
		}
		t := list.Range(st, end)
		ans := "[ "
		for _, v := range t {
			ans += v.(string)
			ans += ","
		}
		ans += " ]"
		return ans, nil
	case "hget":
		v, ok := s.DB.HASH[key]
		if !ok {
			return "not such hash", nil
		}
		t, ok := v[string(oper[2])]
		if ok {
			return t, nil
		}
		return "this hash has no such key", nil
	case "hlen":
		v, ok := s.DB.HASH[key]
		if !ok {
			return "not such hash", nil
		}
		return strconv.Itoa(len(v)), nil
	case "hgetall":
		v, ok := s.DB.HASH[key]
		if !ok {
			return "not such hash", nil
		}
		ans := "["
		for k, _ := range v {
			ans += k + ","
		}
		ans += "]"
		return ans, nil
	case "smembers":
		v, ok := s.DB.SET[key]
		if !ok {
			return "no such key", nil
		}
		ans := "["
		strs := v.ToSlice()
		for _, v := range strs {
			ans += v + ","
		}
		ans += "]"
		return ans, nil
	case "scard":
		v, ok := s.DB.SET[key]
		if !ok {
			return "no such key", nil
		}
		return strconv.Itoa(v.Len()), nil
	case "sismember":
		v, ok := s.DB.SET[key]
		if !ok {
			return "no such key", nil
		}
		if v.Has(string(oper[2])) {
			return "true", nil
		}
		return "false", nil
	case "srandmember":
		t, err := strconv.Atoi(string(oper[2]))
		if err != nil {
			return "args 3 is not digit", nil
		}
		v, ok := s.DB.SET[key]
		if !ok {
			return "no such key", nil
		}
		strs := v.RandomDistinctMembers(t)
		ans := "["
		for _, v := range strs {
			ans += v + ","
		}
		ans += "]"
		return ans, nil
	case "zscore":
		v, ok := s.DB.ZSET[key]
		if !ok {
			return "not exist this key", nil
		}
		ele, ok := v.Get(string(oper[2]))
		if !ok {
			return "not exist this member", nil
		}
		return fmt.Sprintf("%v", ele.Score), nil
	case "zcard":
		v, ok := s.DB.ZSET[key]
		if !ok {
			return "not exist this key", nil
		}
		return fmt.Sprintf("%v", v.Len()), nil
	case "zrange":
		v, ok := s.DB.ZSET[key]
		if !ok {
			return "not exist this key", nil
		}
		start, err := strconv.Atoi(string(oper[2]))
		if err != nil {
			return "args 3 should be digit", nil
		}
		end, err := strconv.Atoi(string(oper[3]))
		if err != nil {
			return "args 4 should be digit", nil
		}
		t := v.Range(int64(start), int64(end), false)
		ans := "[ "
		for _, val := range t {
			ans += val.Member + ","
		}
		ans += "]"
		return ans, nil
	case "zrevrange":
		v, ok := s.DB.ZSET[key]
		if !ok {
			return "not exist this key", nil
		}
		start, err := strconv.Atoi(string(oper[2]))
		if err != nil {
			return "args 3 should be digit", nil
		}
		end, err := strconv.Atoi(string(oper[3]))
		if err != nil {
			return "args 4 should be digit", nil
		}
		t := v.Range(int64(start), int64(end), true)
		ans := "[ "
		for _, val := range t {
			ans += val.Member + ","
		}
		ans += "]"
		return ans, nil
	case "zrangebyscore":
		v, ok := s.DB.ZSET[key]
		if !ok {
			return "not exist this key", nil
		}
		start, err := sortedset.ParseScoreBorder(string(oper[2]))
		if err != nil {
			return "args 3 should be digit", nil
		}
		end, err := sortedset.ParseScoreBorder(string(oper[3]))
		if err != nil {
			return "args 4 should be digit", nil
		}
		t := v.RangeByScore(start, end, 0, -1, false)
		ans := "[ "
		for _, val := range t {
			ans += val.Member + ","
		}
		ans += "]"
		return ans, nil
	}

	return nil, errors.New("unknown operation in look up")
}

// Update updates the object using the specified committed raft entry.
func (s *ExampleStateMachine) Update(e []byte) (sm.Result, error) {
	// in this example, we print out the following hello world message for each
	// incoming update request. we also increase the counter by one to remember
	// how many updates we have applied
	//s.Count++
	//fmt.Printf("from ExampleStateMachine.Update(), msg: %s, count:%d\n",
	//	string(e.Cmd), s.Count)
	//return sm.Result{Value: uint64(len(e.Cmd))}, nil
	var temp resp.Command
	err := sonic.Unmarshal(e, &temp.Args)
	if err != nil {
		fmt.Println("unmarshal error on statemachine: ", err)
		return sm.Result{}, nil
	}

	key := string(temp.Args[1])
	switch strings.ToLower(string(temp.Args[0])) {
	case "set":
		s.DB.KV[key] = &database.SDS{
			TTL: maxInt64,
			VAL: string(temp.Args[2]),
		}
		return sm.Result{Value: 1, Data: OK}, nil
	case "setex":
		ttl, err := strconv.Atoi(string(temp.Args[2]))
		if err != nil || ttl <= 0 {
			return sm.Result{Value: 1, Data: TimeSmall}, nil
		}
		s.DB.KV[key] = &database.SDS{
			TTL: time.Now().UnixMilli() + (int64)(ttl)*1000,
			VAL: string(temp.Args[3]),
		}
		return sm.Result{Value: 1, Data: OK}, nil
	case "setnx":
		if _, ok := s.DB.KV[key]; !ok {
			s.DB.KV[key] = &database.SDS{
				TTL: maxInt64,
				VAL: string(temp.Args[2]),
			}
			return sm.Result{Value: 1, Data: OK}, nil
		}
		return sm.Result{Value: 1, Data: AlreadyExist}, nil
	case "expire":
		ttl, err := strconv.Atoi(string(temp.Args[2]))
		if err != nil || ttl <= 0 {
			return sm.Result{Value: 1, Data: TimeSmall}, nil
		}
		if v, ok := s.DB.KV[key]; ok {
			v.TTL = time.Now().UnixMilli() + (int64)(ttl)*1000
		} else {
			return sm.Result{Value: 1, Data: NotExist}, nil
		}
		return sm.Result{Value: 1, Data: OK}, nil
	case "del":
		if _, ok := s.DB.KV[key]; ok {
			delete(s.DB.KV, key)
			return sm.Result{Value: 1, Data: OK}, nil
		}
		return sm.Result{Value: 1, Data: NotExist}, nil
	case "incrby":
		if v, ok := s.DB.KV[key]; ok {
			dig, err := strconv.Atoi(v.VAL)
			if err != nil {
				return sm.Result{Value: 1, Data: NotDigit}, nil
			}
			cha, err := strconv.Atoi(string(temp.Args[2]))
			if err != nil {
				return sm.Result{Value: 1, Data: NotDigit}, nil
			}
			dig += cha
			v.VAL = strconv.Itoa(dig)
			return sm.Result{Value: 1, Data: OK}, nil
		}
		return sm.Result{Value: 1, Data: NotExist}, nil
	case "lpush":
		if _, ok := s.DB.LIST[key]; !ok {
			s.DB.LIST[key] = list.NewQuickList()
		}
		for i := 2; i < len(temp.Args); i++ {
			s.DB.LIST[key].Insert(0, string(temp.Args[i]))
		}
		return sm.Result{Value: 1, Data: OK}, nil
	case "rpush":
		if _, ok := s.DB.LIST[key]; !ok {
			s.DB.LIST[key] = list.NewQuickList()
		}
		for i := 2; i < len(temp.Args); i++ {
			s.DB.LIST[key].Add(string(temp.Args[i]))
		}
		return sm.Result{Value: 1, Data: OK}, nil
	case "lpop":
		if _, ok := s.DB.LIST[key]; !ok {
			s.DB.LIST[key] = list.NewQuickList()
		}
		res := s.DB.LIST[key].Remove(0)
		if res == nil {
			return sm.Result{Value: 1, Data: NotExist}, nil
		}

		return sm.Result{Value: 1, Data: []byte(res.(string))}, nil
	case "rpop":
		if _, ok := s.DB.LIST[key]; !ok {
			s.DB.LIST[key] = list.NewQuickList()
		}
		leng := s.DB.LIST[key].Len()
		if leng == 0 {
			return sm.Result{Value: 1, Data: NotExist}, nil
		}
		res := s.DB.LIST[key].Remove(leng - 1)
		if res == nil {
			return sm.Result{Value: 1, Data: NotExist}, nil
		}

		return sm.Result{Value: 1, Data: []byte(res.(string))}, nil
	case "hset":
		if _, ok := s.DB.HASH[key]; !ok {
			s.DB.HASH[key] = make(map[string]string)
		}
		s.DB.HASH[key][string(temp.Args[2])] = string(temp.Args[3])
		return sm.Result{Value: 1, Data: OK}, nil
	case "hdel":
		if _, ok := s.DB.HASH[key]; !ok {
			s.DB.HASH[key] = make(map[string]string)
		}
		delete(s.DB.HASH[key], string(temp.Args[2]))
		return sm.Result{Value: 1, Data: OK}, nil
	case "sadd":
		if _, ok := s.DB.SET[key]; !ok {
			t := make([]string, 0, len(temp.Args)-2)
			for i := 2; i < len(temp.Args); i++ {
				t = append(t, string(temp.Args[i]))
			}
			s.DB.SET[key] = set.Make(t...)
			return sm.Result{Value: 1, Data: OK}, nil
		}
		//t := make([]string, len(temp.Args) - 2)
		for i := 2; i < len(temp.Args); i++ {
			s.DB.SET[key].Add(string(temp.Args[i]))
		}
		return sm.Result{Value: 1, Data: OK}, nil
	case "srem":
		if _, ok := s.DB.SET[key]; !ok {
			return sm.Result{Value: 1, Data: NotExist}, nil
		}
		//t := make([]string, len(temp.Args) - 2)
		for i := 2; i < len(temp.Args); i++ {
			s.DB.SET[key].Remove(string(temp.Args[i]))
		}
		return sm.Result{Value: 1, Data: OK}, nil
	case "zadd":
		if _, ok := s.DB.ZSET[key]; !ok {
			s.DB.ZSET[key] = sortedset.Make()
		}
		v := s.DB.ZSET[key]
		score, err := strconv.Atoi(string(temp.Args[2]))
		if err != nil {
			return sm.Result{Value: 1, Data: NotDigit}, nil
		}
		v.Add(string(temp.Args[3]), float64(score))
		return sm.Result{Value: 1, Data: OK}, nil
	case "zrem":
		if _, ok := s.DB.ZSET[key]; !ok {
			return sm.Result{Value: 0, Data: NotExist}, nil
		}
		v := s.DB.ZSET[key]
		f := v.Remove(string(temp.Args[2]))
		if !f {
			return sm.Result{Value: 1, Data: []byte("false")}, nil
		}
		return sm.Result{Value: 1, Data: OK}, nil
	}

	return sm.Result{Value: 0, Data: []byte("unknow command")}, nil
}

// SaveSnapshot saves the current IStateMachine state into a snapshot using the
// specified io.Writer object.
func (s *ExampleStateMachine) SaveSnapshot(w io.Writer,
	fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	// as shown above, the only state that can be saved is the Count variable
	// there is no external file in this IStateMachine example, we thus leave
	// the fc untouched
	data := make([]byte, 8)
	//binary.LittleEndian.PutUint64(data, s.Count)
	_, err := w.Write(data)
	return err
}

// RecoverFromSnapshot recovers the state using the provided snapshot.
func (s *ExampleStateMachine) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile,
	done <-chan struct{}) error {
	// restore the Count variable, that is the only state we maintain in this
	// example, the input files is expected to be empty
	//data, err := ioutil.ReadAll(r)
	//if err != nil {
	//	return err
	//}
	//v := binary.LittleEndian.Uint64(data)
	//s.Count = v
	return nil
}

// Close closes the IStateMachine instance. There is nothing for us to cleanup
// or release as this is a pure in memory data store. Note that the Close
// method is not guaranteed to be called as node can crash at any time.
func (s *ExampleStateMachine) Close() error { return nil }
