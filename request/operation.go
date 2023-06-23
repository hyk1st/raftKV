package request

const (
	GET = iota
	EXISTS
	STRLEN
	MGET
	TTL
	LRANGE
	HGET
	HMGET
	HLEN
	HGETALL
	SMEMBERS
	SCARD
	SISMEMBER
	SRANDMEMBER
	ZSCORE
	ZCARD
	ZRANGE
	ZREVRANGE
	ZRANGEBYSCORE
	SET
)

type Operation struct {
	Type int      `json:"type"`
	Opts [][]byte `json:"opts"`
}
