package rpc

import (
	"context"
	"fmt"
	"log"
	"raftKV/operation"
	"raftKV/raft"
	"raftKV/resp"
	"time"
)

func StartServer(port int) {
	addr := fmt.Sprintf(":%d", port)
	handler := operation.NewHandleFuns()
	mux := resp.NewServeMux()
	//sds
	mux.HandleFunc("detach", handler.Detach)
	mux.HandleFunc("ping", handler.Ping)
	mux.HandleFunc("quit", handler.Quit)
	mux.HandleFunc("set", handler.Set)
	mux.HandleFunc("get", handler.Get)
	mux.HandleFunc("del", handler.Delete)
	mux.HandleFunc("setnx", handler.SetNx)
	mux.HandleFunc("setex", handler.SetEx)
	mux.HandleFunc("exists", handler.Exist)
	mux.HandleFunc("expire", handler.Expire)
	mux.HandleFunc("incrby", handler.INCRBY)
	mux.HandleFunc("ttl", handler.TTL)
	//list
	mux.HandleFunc("lpush", handler.Lpush)
	mux.HandleFunc("lpop", handler.Lpop)
	mux.HandleFunc("rpush", handler.Rpush)
	mux.HandleFunc("rpop", handler.Rpop)
	mux.HandleFunc("lrange", handler.LRANGE)
	//hash
	mux.HandleFunc("hset", handler.Hset)
	mux.HandleFunc("hget", handler.HGet)
	mux.HandleFunc("hdel", handler.Hdel)
	mux.HandleFunc("hlen", handler.HLEN)
	mux.HandleFunc("hgetall", handler.HGETALL)
	//set
	mux.HandleFunc("sadd", handler.SADD)
	mux.HandleFunc("srem", handler.SREM)
	mux.HandleFunc("smembers", handler.SMEMBERS)
	mux.HandleFunc("scard", handler.SCARD)
	mux.HandleFunc("sismember", handler.SISMEMBER)
	mux.HandleFunc("srandmember", handler.SRANDMEMBER)
	//zset
	mux.HandleFunc("zadd", handler.ZADD)
	mux.HandleFunc("zrem", handler.ZREM)
	mux.HandleFunc("zscore", handler.ZSCORE)
	mux.HandleFunc("zcard", handler.ZCARD)
	mux.HandleFunc("zrange", handler.ZRANGE)
	mux.HandleFunc("zrevrange", handler.ZREVRANGE)
	mux.HandleFunc("zrangebyscore", handler.ZRANGEBYSCORE)
	resp.ListenAndServe(addr,
		mux.ServeRESP,
		func(conn resp.Conn) bool {
			// use this function to accept or deny the connection.
			ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
			session, err := raft.Raft.SyncGetSession(ctx, 1)
			if err != nil {
				return false
			}
			conn.SetSession(session)
			log.Printf("accept: %s", conn.RemoteAddr())

			return true
		},
		func(conn resp.Conn, err error) {
			// this is called when the connection has been closed
			ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
			raft.Raft.SyncCloseSession(ctx, conn.GetSession())
			log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		})
}

//type Server struct {
//	*gnet.EventServer
//}
//
//func (es *Server) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
//	var temp operation.Operation
//	err := json.Unmarshal(frame, &temp)
//	if err != nil {
//		log.Printf("server react unmarshal error: %t\n", err)
//		out = []byte("server react unmarshal error")
//		return
//	}
//	ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
//	if temp.Type <= operation.ZRANGEBYSCORE {
//		val, err := raft.Raft.SyncRead(ctx, 1, temp)
//		if err != nil {
//
//			log.Printf("command read error: %t\n", err)
//			out = []byte("command read error")
//			return
//		}
//		str := val.(string)
//		out = []byte(str)
//		return
//	}
//	fmt.Println(temp)
//	session, err := raft.Raft.SyncGetSession(ctx, temp.ID)
//	if err != nil {
//		log.Printf("get client session error: %t\n", err)
//		out = []byte("get client session error")
//	}
//	res, err := raft.Raft.SyncPropose(ctx, session, frame)
//	if err != nil {
//		log.Printf("syncPropose error: %t\n", err)
//		out = []byte("syncPropose error")
//		return
//	}
//	out = res.Data
//	return
//}
//
//func StartServer(port int) {
//	echo := new(Server)
//	log.Fatal(gnet.Serve(echo, fmt.Sprintf("tcp://:%d", port), gnet.WithMulticore(true)))
//}
