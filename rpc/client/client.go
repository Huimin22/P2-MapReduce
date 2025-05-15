package client

import (
	"dfs/protogen/rpc"
	"dfs/rpc/helper"
	"errors"
	"net"
	"sync"

	"google.golang.org/protobuf/proto"
)

// Client is a client for rpc
type Client struct {
	conn net.Conn
	m    sync.Mutex
}

// NewClient creates a new client
func NewClient(conn net.Conn) *Client {
	return &Client{
		conn: conn,
	}
}

// SendRequest sends a request and receives a response
func (c *Client) SendRequest(req proto.Message) (resp proto.Message, err error) {
	c.m.Lock()
	defer c.m.Unlock()
	err = helper.Send(c.conn, helper.WrapMessage(req))
	if err != nil {
		return
	}
	var respTrac *rpc.Transaction
	respTrac, err = helper.Receive(c.conn)
	if err != nil {
		return
	}
	resp, err = respTrac.Payload.UnmarshalNew()
	if e, ok := resp.(*rpc.Error); ok {
		return nil, errors.New(e.Message)
	}
	return
}

// Close closes the connection
func (c *Client) Close() error {
	return c.conn.Close()
}
