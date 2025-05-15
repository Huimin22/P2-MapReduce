package main

import (
	"dfs/cmd/controller/manager"
	"dfs/protogen/client"
	"dfs/protogen/common"
	"dfs/protogen/node"
	"dfs/rpc/server"
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mux struct {
	m *manager.Manager
}

func main() {
	chunkSize := flag.Uint("chunkSize", 4*1024*1024, "Chunk size in bytes")
	replicaCount := flag.Uint("replicaCount", 3, "Replica count")
	flag.Parse()
	if *chunkSize < 1 || *replicaCount < 1 {
		panic("Invalid chunk size or replica count")
	}
	manager := manager.NewManager(uint64(*chunkSize), uint64(*replicaCount))
	log.Printf("Chunk size: %d, Replica count: %d, starting controller server at port 8080...\n", *chunkSize, *replicaCount)

	m := mux{
		m: manager,
	}

	// register the RPC request handler from the client side
	clientServer := server.NewServer()
	clientServer.SetLogPrefix("[controller-client]")
	clientServer.RegisterByMessage(&client.StoreRequest{}, m.storeRequest)
	clientServer.RegisterByMessage(&client.GetRequest{}, m.getRequest)
	clientServer.RegisterByMessage(&client.DeleteRequest{}, m.deleteRequest)
	clientServer.RegisterByMessage(&client.ListRequest{}, m.listRequest)
	clientServer.RegisterByMessage(&client.NodeInfoRequest{}, m.nodeInfoRequest)
	clientServer.RegisterByMessage(&client.MapReduceRequest{}, m.mapReduceRequest)
	// --- In main() -> clientServer registration ---
	clientServer.RegisterByMessage(&client.UploadPluginRequest{}, m.uploadPluginRequest)

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	go clientServer.Serve(listener)

	// register the RPC request handler from the storage node side
	controllerServer := server.NewServer()
	controllerServer.SetLogPrefix("[controller-node]")
	controllerServer.RegisterByMessage(&node.Register{}, m.registerNodeRequest)
	controllerServer.RegisterByMessage(&node.Heartbeat{}, m.heartbeatRequest)
	controllerServer.RegisterByMessage(&node.ChunkUploaded{}, m.chunkUploadedRequest)
	controllerServer.RegisterByMessage(&node.TaskCompletedRequest{}, m.taskCompletedRequest)
	// --- In main() -> controllerServer registration (for Node download) ---
	controllerServer.RegisterByMessage(&client.DownloadPluginRequest{}, m.downloadPluginRequest)
	listener, err = net.Listen("tcp", ":8081")
	if err != nil {
		panic(err)
	}
	controllerServer.Serve(listener)
}

func convertChunkInfo(chunkInfo []manager.ChunkInfo) []*common.ChunkInfo {
	res := make([]*common.ChunkInfo, 0, len(chunkInfo))
	for _, chunk := range chunkInfo {
		replicas := chunk.Replicas.GetUnderlyingList()
		res = append(res, &common.ChunkInfo{
			Id:       chunk.ChunkID,
			Size:     chunk.Size,
			Replicas: replicas,
		})
	}
	return res
}

// storeRequest handles the store file request from the client
func (m *mux) storeRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	// convert req to StoreRequest
	storeReq := req.(*client.StoreRequest)
	resp := &client.StoreResponse{}
	if storeReq.Finished {
		return resp, m.m.FinishStoreFile(storeReq.Key)
	}
	meta, err := m.m.StoreFile(storeReq.Key, storeReq.Size, storeReq.IsText)
	if err != nil {
		return nil, err
	}
	resp.Key = meta.Name
	resp.Chunks = convertChunkInfo(meta.Chunks)
	return resp, nil
}

// getRequest handles the get file request from the client
func (m *mux) getRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	// convert req to GetRequest
	getReq := req.(*client.GetRequest)
	meta, err := m.m.GetFile(getReq.Key)
	if err != nil {
		return nil, err
	}
	resp := &client.GetResponse{}
	resp.Chunks = convertChunkInfo(meta.Chunks)
	return resp, nil
}

// deleteRequest handles the delete file request from the client
func (m *mux) deleteRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	// convert req to DeleteRequest
	deleteReq := req.(*client.DeleteRequest)
	isCandiate, err := m.m.DeleteFile(deleteReq.Key)
	if err != nil {
		return nil, err
	}
	return &client.DeleteResponse{
		IsCandiate: isCandiate,
	}, nil
}

// listRequest handles the list file request from the client
func (m *mux) listRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	// convert req to ListRequest
	listReq := req.(*client.ListRequest)
	resp := &client.ListResponse{}
	files, err := m.m.ListFiles(listReq.Prefix)
	if err != nil {
		return nil, err
	}
	resp.Files = make([]*client.File, 0, len(files))
	for _, file := range files {
		resp.Files = append(resp.Files, &client.File{
			Key:       file.Name,
			Size:      file.Size,
			CreatedAt: timestamppb.New(file.CreatedAt),
			IsText:    file.IsText,
		})
	}
	return resp, nil
}

func convertNodeInfo(sn *manager.StorageNode) *client.NodeInfo {
	return &client.NodeInfo{
		Id:         sn.ID,
		Totalspace: sn.TotalSpace,
		Freespace:  sn.FreeSpace,
	}
}

// nodeInfoRequest handles the node info request from the client
func (m *mux) nodeInfoRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	reqMsg := req.(*client.NodeInfoRequest)
	resp := &client.NodeInfoResponse{}
	if reqMsg.NodeId != nil {
		node, err := m.m.GetNode(*reqMsg.NodeId)
		if err != nil {
			return nil, err
		}
		resp.Nodes = []*client.NodeInfo{convertNodeInfo(node)}
	} else {
		nodes, err := m.m.ListNodes()
		if err != nil {
			return nil, err
		}
		resp.Nodes = make([]*client.NodeInfo, 0, len(nodes))
		for _, node := range nodes {
			resp.Nodes = append(resp.Nodes, convertNodeInfo(node))
		}
	}
	return resp, nil
}

// registerNodeRequest handles the register node request from the storage node
func (m *mux) registerNodeRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	// convert req to Register
	register := req.(*node.Register)
	sn, err := m.m.AddNode(register.ListenAddr)
	if err != nil {
		return nil, err
	}
	register.Metadata.Id = sn.ID
	_, err = m.m.UpdateHeartbeat(sn.ID, register.Metadata)
	if err != nil {
		fmt.Printf("update heartbeat error: %v\n", err)
	}
	return &common.EmptyResponse{}, nil
}

// heartbeatRequest handles the heartbeat request from the storage node
func (m *mux) heartbeatRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	// convert req to Heartbeat
	heartbeat := req.(*node.Heartbeat)
	removeChunks, err := m.m.UpdateHeartbeat(heartbeat.Metadata.Id, heartbeat.Metadata)
	if err != nil {
		fmt.Printf("update heartbeat error: %v\n", err)
	}
	return &node.HeartbeatResponse{
		RemoveChunks: removeChunks,
	}, nil
}

// chunkUploadedRequest handles the chunk uploaded request from the storage node
func (m *mux) chunkUploadedRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	// convert req to ChunkUploaded
	chunkUploaded := req.(*node.ChunkUploaded)
	err := m.m.UpdateChunkUploaded(chunkUploaded.Id, chunkUploaded.Hash, chunkUploaded.Size, chunkUploaded.Replicas)
	if err != nil {
		fmt.Printf("update chunk uploaded error: %v\n", err)
	}
	return &common.EmptyResponse{}, nil
}

// mapReduceRequest handles the MapReduce job request from the client
func (m *mux) mapReduceRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	mapReduceReq := req.(*client.MapReduceRequest) // Ensure correct package alias if needed
	log.Printf("[Controller] Received MapReduce request: Input='%s', Output='%s', Map='%s', Reduce='%s', Aggregate=%t, NumReducers=%d",
		mapReduceReq.InputKey, mapReduceReq.OutputKey, mapReduceReq.MapFuncId, mapReduceReq.ReduceFuncId,
		mapReduceReq.ControllerAggregate, mapReduceReq.NumReduceTasks) // add NumReduceTasks to log

	jobId, err := m.m.StartMapReduceJob(
		mapReduceReq.InputKey,
		mapReduceReq.OutputKey,
		mapReduceReq.MapFuncId,
		mapReduceReq.ReduceFuncId,
		mapReduceReq.ControllerAggregate,
		mapReduceReq.NumReduceTasks, // pass NumReduceTasks
	)
	if err != nil {
		log.Printf("[Controller] Failed to start MapReduce job: %v", err)
		return nil, err
	}

	resp := &client.MapReduceResponse{ // Ensure correct package alias if needed
		JobId: jobId,
	}
	return resp, nil
}

// taskCompletedRequest handles the task completion report from a storage node
func (m *mux) taskCompletedRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	taskCompletedReq := req.(*node.TaskCompletedRequest)

	err := m.m.HandleTaskCompletion(taskCompletedReq)
	if err != nil {
		// Log the error, but don't necessarily return error to node?
		// The node has finished its work, controller needs to handle inconsistencies.
		log.Printf("[Controller] Error handling task completion for Job %s, Task %d: %v",
			taskCompletedReq.JobId, taskCompletedReq.TaskId, err)
	}

	// Always send a simple ack back to the node
	return &common.EmptyResponse{}, nil
}

// --- Add new handler methods to mux struct ---
// Handles plugin upload from client
func (m *mux) uploadPluginRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	uploadReq := req.(*client.UploadPluginRequest) // Use correct alias if needed
	err := m.m.StorePlugin(uploadReq.PluginId, uploadReq.PluginCode)
	resp := &client.UploadPluginResponse{} // Use correct alias if needed
	if err != nil {
		log.Printf("[Controller] Error storing plugin '%s': %v", uploadReq.PluginId, err)
		resp.Success = false
		resp.Message = err.Error()
		// Return error to client? Or just in response? Let's use response.
		return resp, nil // Return response, not error
	}
	log.Printf("[Controller] Successfully stored plugin '%s'", uploadReq.PluginId)
	resp.Success = true
	return resp, nil
}

// Handles plugin download request from storage node
func (m *mux) downloadPluginRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	downloadReq := req.(*client.DownloadPluginRequest) // Use correct alias if needed
	pluginCode, found, err := m.m.GetPlugin(downloadReq.PluginId)
	resp := &client.DownloadPluginResponse{} // Use correct alias if needed
	resp.PluginId = downloadReq.PluginId
	resp.Found = found
	if err != nil {
		log.Printf("[Controller] Error getting plugin '%s' for node: %v", downloadReq.PluginId, err)
		resp.ErrorMessage = err.Error()
		// Don't return error directly, let node know via response
		return resp, nil
	}
	if !found {
		log.Printf("[Controller] Plugin '%s' not found for node request.", downloadReq.PluginId)
		resp.ErrorMessage = "Plugin not found"
		return resp, nil
	}

	log.Printf("[Controller] Sending plugin '%s' (%d bytes) to node", downloadReq.PluginId, len(pluginCode))
	resp.PluginCode = pluginCode
	return resp, nil
}
