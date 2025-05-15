package utils

import (
	"dfs/rpc/client"
	"net"

	"google.golang.org/protobuf/proto"
)

// SendSingleRequest send a single request to a server and receive a response
func SendSingleRequest(address string, req proto.Message) (resp proto.Message, err error) {
	var conn net.Conn
	conn, err = net.Dial("tcp", address)
	if err != nil {
		return
	}
	c := client.NewClient(conn)
	defer c.Close()
	return c.SendRequest(req)
}
