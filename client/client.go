package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/panjf2000/gnet"
	"log"
	"os"
	"raftKV/request"
	"strings"
)

type echoServer struct {
	*gnet.EventServer
}

func (es *echoServer) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	fmt.Println(string(frame))
	return
}
func main() {
	client, err := gnet.NewClient(&echoServer{})
	var id uint64 = 1
	if err != nil {
		fmt.Println(err)
		return
	}
	con, err := client.Dial("tcp", "127.0.0.1:7001")
	client.Start()
	if err != nil {
		fmt.Println(err)
		return
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		s, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err)
			return
		}
		if s == "exit\n" {
			return
		}
		var cmd request.Operation
		strs := strings.Split(s, " ")
		if strings.ToUpper(strs[0]) == "GET" {
			cmd.ID = id
			cmd.Type = request.GET
			cmd.Opts = strs[1:]
			by, err := json.Marshal(cmd)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = con.SendTo(by)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
		if strings.ToUpper(strs[0]) == "SET" {
			cmd.ID = id
			cmd.Type = request.SET
			cmd.Opts = strs[1:]
			by, err := json.Marshal(cmd)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = con.SendTo(by)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}

}
