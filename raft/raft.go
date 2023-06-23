package raft

import (
	"fmt"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"os"
	"path/filepath"
	"raftKV/stateMachine"
)

var Raft *dragonboat.NodeHost

func StartRaftNode(nodeID, shardId uint64, addr string, initMembers map[uint64]string, join bool, stop chan struct{}, msg chan string) {
	conf := config.Config{
		NodeID:             nodeID,
		ClusterID:          shardId,
		CheckQuorum:        true,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		SnapshotEntries:    10000,
		CompactionOverhead: 500,
	}
	walDir := filepath.Join("data", fmt.Sprintf("node%d", nodeID), "wal")
	dataDir := filepath.Join("data", fmt.Sprintf("node%d", nodeID), "data")
	nhc := config.NodeHostConfig{
		WALDir:         walDir,
		NodeHostDir:    dataDir,
		RTTMillisecond: 200,
		RaftAddress:    addr,
	}
	var err error
	Raft, err = dragonboat.NewNodeHost(nhc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "新建raft结点错误，%t\n", err)
		os.Exit(1)
	}

	err = Raft.StartCluster(initMembers, join, stateMachine.NewExampleStateMachine, conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "启动raft结点错误，%t\n", err)
		os.Exit(1)
	}
	//go func() {
	//	cs := nh.GetNoOPSession(1)
	//
	//	for {
	//		select {
	//		case <-stop:
	//			return
	//		case s, ok := <-msg:
	//			if !ok {
	//				return
	//			}
	//			fmt.Fprintf(os.Stdout, "node %v receive %v\n", nodeID, s)
	//			ctx, cacel := context.WithTimeout(context.Background(), time.Second*3)
	//			_, err = nh.SyncPropose(ctx, cs, []byte(s))
	//			//nh.SyncRead()
	//			if err != nil {
	//				fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
	//			}
	//			cacel()
	//		}
	//	}
	//}()
}
