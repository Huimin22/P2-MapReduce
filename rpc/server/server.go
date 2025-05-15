package server

import (
	"bytes"
	"dfs/protogen/rpc"
	"dfs/rpc/helper"
	"fmt"
	"io"
	"net"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Context contains the context information for a RPC request.
type Context struct {
	Address string
}

// HandlerFunc is the handler function for a RPC request.
type HandlerFunc func(ctx Context, req proto.Message) (resp proto.Message, err error)

// Server is a RPC server.
type Server struct {
	handlerMap       map[string]HandlerFunc
	logPrefix        string
	recoverFromPanic bool
}

// NewServer creates a new Server instance.
func NewServer() *Server {
	return &Server{
		handlerMap: make(map[string]HandlerFunc),
		logPrefix:  "[rpc server]",
	}
}

// SetLogPrefix sets the prefix of the log messages.
func (s *Server) SetLogPrefix(prefix string) {
	s.logPrefix = prefix
}

// SetRecoverFromPanic sets whether the server should recover from panic caused
// by the registered handlers
func (s *Server) SetRecoverFromPanic(recover bool) {
	s.recoverFromPanic = recover
}

func (s *Server) printLog(format string, a ...any) {
	buf := bytes.NewBufferString(s.logPrefix)
	buf.WriteByte(' ')
	fmt.Fprintf(buf, format, a...)
	fmt.Println(buf.String())
}

// RegisterByTypeUrl registers the handler for the specified request message type.
func (s *Server) RegisterByTypeUrl(typeUrl string, handler HandlerFunc) error {
	_, err := protoregistry.GlobalTypes.FindMessageByURL(typeUrl)
	if err != nil {
		return err
	}
	s.handlerMap[typeUrl] = handler
	return nil
}

// RegisterByMessage registers the handler for the specified request message.
func (s *Server) RegisterByMessage(msg proto.Message, handler HandlerFunc) error {
	typeUrl := proto.MessageName(msg)
	s.handlerMap[string(typeUrl)] = handler
	return nil
}

// handle handles the request and returns the response.
func (s *Server) handle(ctx Context, req proto.Message) (resp proto.Message, err error) {
	typeUrl := proto.MessageName(req)
	handler, ok := s.handlerMap[string(typeUrl)]
	if !ok {
		err = fmt.Errorf("no handler for type %s", typeUrl)
		return
	}
	if s.recoverFromPanic {
		defer func() {
			// recover from panic
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
			}
		}()
	}
	return handler(ctx, req)
}

// handleConn handles the connection.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	remoteAddr := conn.RemoteAddr().String()
	for {
		req, err := helper.Receive(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
			s.printLog("receive error for conn %s: %v, closing conn\n", remoteAddr, err)
			return
		}
		// unmarshal the request
		reqMsg, err := req.Payload.UnmarshalNew()
		if err != nil {
			s.printLog("unknown request type %s from %s, send back error\n", req.Payload.TypeUrl, remoteAddr)
			respMsg := rpc.Error{
				Message: err.Error(),
			}
			resp := helper.WrapMessage(&respMsg)
			if err := helper.Send(conn, resp); err != nil {
				s.printLog("send message failed for conn %s: %v", remoteAddr, err)
				return
			}
		}

		// handle the request
		ctx := Context{
			Address: remoteAddr,
		}
		respMsg, err := s.handle(ctx, reqMsg)
		if err != nil {
			respMsg = &rpc.Error{
				Message: err.Error(),
			}
			s.printLog("request (%s) from %s handled with error: %s", req.Payload.TypeUrl, remoteAddr, err)
		} else {
			s.printLog("request (%s) from %s handled successful", req.Payload.TypeUrl, remoteAddr)
		}
		resp := helper.WrapMessage(respMsg)
		// send back the response
		if err := helper.Send(conn, resp); err != nil {
			s.printLog("send message failed for conn %s: %v", remoteAddr, err)
			return
		}
	}
}

// Serve starts the server and listens for incoming connections.
func (s *Server) Serve(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}
		go s.handleConn(conn)
	}
}
