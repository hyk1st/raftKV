package main

import (
	"flag"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"raftKV/raft"
	"raftKV/rpc"
)

type conf struct {
	Node map[uint64]string `yaml:"Node"`
}

func main() {
	replicaID := flag.Int("replicaid", -1, "ReplicaID to use")
	addr := flag.String("addr", "", "Nodehost address")
	path := flag.String("path", "", "Nodehost file path")
	port := flag.Int("port", -1, "server listen port")
	shardID := flag.Int("shard", 1, "server shard")
	join := flag.Bool("join", false, "Joining a new node")
	flag.Parse()
	if len(*addr) == 0 {
		fmt.Fprintf(os.Stderr, "缺少结点的监听地址\n")
		os.Exit(1)
	}
	if *replicaID == -1 {
		fmt.Fprintf(os.Stderr, "缺少结点的replicaID\n")
		os.Exit(1)
	}
	if *port == -1 {
		fmt.Fprintf(os.Stderr, "缺少该状态机的监听端口\n")
		os.Exit(1)
	}
	if len(*path) == 0 {
		fmt.Fprintf(os.Stderr, "缺少集群信息配置文件\n")
		os.Exit(1)
	}
	data, err := ioutil.ReadFile(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "集群信息配置文件读取错误，%v\n", err)
		os.Exit(1)
	}
	conf := conf{}
	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "集群信息配置文件解析错误，%t\n", err)
		os.Exit(1)
	}
	if v, ok := conf.Node[uint64(*replicaID)]; !ok || v != *addr {
		fmt.Fprintf(os.Stderr, "集群信息配置文件中本机结点信息与传入addr不符合，%t\n", err)
		os.Exit(1)
	}
	stop := make(chan struct{}, 0)
	msg := make(chan string, 20)
	go raft.StartRaftNode(uint64(*replicaID), uint64(*shardID), *addr, conf.Node, *join, stop, msg)
	rpc.StartServer(*port)
}
