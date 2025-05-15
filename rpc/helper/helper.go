package helper

import (
	"dfs/protogen/rpc"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Send sends a rpc.Transaction to a net.Conn.
// format is:
//
//	| length (8 bytes) | payload (length bytes) |
func Send(conn net.Conn, resp *rpc.Transaction) error {
	bytes, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	length := uint64(len(bytes))
	if err := binary.Write(conn, binary.BigEndian, length); err != nil {
		return err
	}
	if _, err := conn.Write(bytes); err != nil {
		return err
	}
	return nil
}

// Receive receives a rpc.Transaction from a net.Conn.
func Receive(conn net.Conn) (*rpc.Transaction, error) {
	var length uint64
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	bytes := make([]byte, length)
	n, err := io.ReadFull(conn, bytes)
	if err != nil {
		return nil, err
	}
	if uint64(n) != length {
		return nil, io.EOF
	}
	resp := &rpc.Transaction{}
	if err := proto.Unmarshal(bytes, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// WrapMessage wraps a proto.Message into a rpc.Transaction.
func WrapMessage(msg proto.Message) *rpc.Transaction {
	payload, err := anypb.New(msg)
	if err != nil {
		panic(fmt.Sprint("anypb.New error:", err))
	}
	return &rpc.Transaction{
		Payload: payload,
	}
}
