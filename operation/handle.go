package operation

import (
	"fmt"
	"github.com/bytedance/sonic"
	"golang.org/x/net/context"
	"log"
	"raftKV/raft"
	"raftKV/resp"
	"time"
)

type HandleFuns struct {
}

func NewHandleFuns() *HandleFuns {
	return &HandleFuns{}
}

func (h *HandleFuns) Detach(conn resp.Conn, cmd resp.Command) {
	detachedConn := conn.Detach()
	log.Printf("connection has been detached")
	go func(c resp.DetachedConn) {
		defer c.Close()

		c.WriteString("OK")
		c.Flush()
	}(detachedConn)
}

func (h *HandleFuns) Ping(conn resp.Conn, cmd resp.Command) {
	conn.WriteString("PONG")
}

func (h *HandleFuns) Quit(conn resp.Conn, cmd resp.Command) {
	conn.WriteString("OK")
	conn.Close()
}

func (h *HandleFuns) Set(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}
}

func (h *HandleFuns) SetNx(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}
}

func (h *HandleFuns) SetEx(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}
}

func (h *HandleFuns) Expire(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}
}

func (h *HandleFuns) TTL(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}

func (h *HandleFuns) Get(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}

func (h *HandleFuns) Exist(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}

func (h *HandleFuns) Delete(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}

func (h *HandleFuns) INCRBY(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}

func (h *HandleFuns) Lpush(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}
func (h *HandleFuns) Rpush(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}

func (h *HandleFuns) Lpop(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}
func (h *HandleFuns) Rpop(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}
func (h *HandleFuns) LRANGE(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}

func (h *HandleFuns) Hset(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}

func (h *HandleFuns) Hdel(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}

func (h *HandleFuns) HGet(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}
func (h *HandleFuns) HLEN(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}
func (h *HandleFuns) HGETALL(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}

func (h *HandleFuns) SADD(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}
func (h *HandleFuns) SREM(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}

func (h *HandleFuns) SMEMBERS(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}

func (h *HandleFuns) SCARD(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}
func (h *HandleFuns) SISMEMBER(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}

func (h *HandleFuns) SRANDMEMBER(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}

func (h *HandleFuns) ZADD(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}

func (h *HandleFuns) ZREM(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}
func (h *HandleFuns) ZINCRBY(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	by, _ := sonic.Marshal(cmd.Args)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncPropose(ctx, conn.GetSession(), by)
	if err != nil {
		conn.WriteError(fmt.Sprintf("error on command set: %t", err))
	} else {
		conn.GetSession().ProposalCompleted()
		conn.WriteBulk(res.Data)
	}

}
func (h *HandleFuns) ZSCORE(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}
func (h *HandleFuns) ZCARD(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}
func (h *HandleFuns) ZRANGE(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}
func (h *HandleFuns) ZREVRANGE(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}
func (h *HandleFuns) ZRANGEBYSCORE(conn resp.Conn, cmd resp.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	res, err := raft.Raft.SyncRead(ctx, 1, cmd.Args)
	//h.itemsMux.RLock()
	//val, ok := h.items[string(cmd.Args[1])]
	//h.itemsMux.RUnlock()
	if err != nil {
		log.Println(err)
		conn.WriteBulk([]byte(fmt.Sprintf("get key error: %t", err)))
	} else {
		conn.WriteString(res.(string))
	}
}
