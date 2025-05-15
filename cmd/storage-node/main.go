package main

import (
	"bufio"               // For reading intermediate data
	"bytes"               // For buffers
	"dfs/mapreduce/types" // <--- Adjust import path based on your module root
	"dfs/protogen/client"
	"dfs/protogen/common"
	"dfs/protogen/node"
	rpcClient "dfs/rpc/client"
	"dfs/rpc/server"
	"dfs/utils"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"log"
	"net"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"plugin"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"hash/fnv" // For partitioning
)

var (
	// "/bigdata/students/" + $(whoami)
	StoragePathPrefix    string
	controllerClientAddr *string
)

var (
	errChunkHashMismatch = errors.New("chunk hash mismatch")
	errChunkNotFound     = errors.New("chunk not found")
)

func getUsername() string {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	return u.Username
}

func generateStoragePath(listenAddr string) string {
	addrReplacer := regexp.MustCompile(`[:\[\]]+`)
	folder := addrReplacer.ReplaceAllString(listenAddr, "_")
	folder = fmt.Sprintf("storage-node-%s", folder)
	return path.Join(StoragePathPrefix, folder)
}

type ChunkInfo struct {
	ChunkID     uint64
	Hash        string
	Size        uint64
	lastChecked int64
}

type mux struct {
	id            string
	storagePath   string
	cli           *rpcClient.Client
	listenAddress string

	// Plugin Cache
	pluginCache map[string]*plugin.Plugin // Map plugin_id to loaded plugin handle
	pluginMutex sync.Mutex                // Mutex to protect pluginCache access

	chunks         map[uint64]*ChunkInfo
	candiateChunks map[uint64]*ChunkInfo

	mutex sync.Mutex
}

func main() {
	controllerAddr := flag.String("controllerAddr", "localhost:8081", "Controller address")
	controllerClientAddr = flag.String("controllerClientAddr", "localhost:8080", "Controller address")

	listenAddr := flag.String("listenAddr", "localhost:9000", "Listen address (this part )")
	flag.StringVar(&StoragePathPrefix, "storePath", "/bigdata/students/"+getUsername(), "Storage path prefix")
	//flag.StringVar(&StoragePathPrefix, "storePath", "data", "Storage path prefix")
	flag.Parse()
	s := server.NewServer()
	s.SetLogPrefix("[storage-node]")

	conn, err := net.Dial("tcp", *controllerAddr)
	if err != nil {
		panic(err)
	}

	client := rpcClient.NewClient(conn)

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		panic(err)
	}

	storagePath := generateStoragePath(*listenAddr)

	// remove all files
	if err := os.RemoveAll(storagePath); err != nil {
		panic(err)
	}
	// create the directory for storage
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		panic(err)
	}

	fmt.Printf("[storage-node] start at %s, storage path: %s\n", *listenAddr, storagePath)

	m := mux{
		id:             *listenAddr,
		storagePath:    storagePath,
		cli:            client,
		listenAddress:  *listenAddr,
		chunks:         make(map[uint64]*ChunkInfo),
		candiateChunks: make(map[uint64]*ChunkInfo),
		pluginCache:    make(map[string]*plugin.Plugin), // Initialize the map
	}

	// register the RPC handlers
	s.RegisterByMessage(&node.PutChunk{}, m.putChunkRequest)
	s.RegisterByMessage(&node.GetChunk{}, m.getChunkRequest)
	s.RegisterByMessage(&node.SendChunk{}, m.sendChunkRequest)
	s.RegisterByMessage(&node.SendChunks{}, m.sendChunksRequest)
	s.RegisterByMessage(&node.DeleteChunk{}, m.deleteChunkRequest)
	// MapReduce Handlers
	s.RegisterByMessage(&node.MapTaskRequest{}, m.mapTaskRequest)
	s.RegisterByMessage(&node.ReduceTaskRequest{}, m.reduceTaskRequest)

	// serve the RPC
	go s.Serve(listener)

	// register to controller
	if err = m.register(); err != nil {
		panic(err)
	}
	// check chunk health
	go m.checkChunks()
	// heartbeat
	m.sendHeartbeat()
}

// updateLastChecked updates the last checked time of the chunk
func (c *ChunkInfo) updateLastChecked(t int64) {
	if c.lastChecked < t {
		c.lastChecked = t
	}
}

// checkChunks checks the health of the chunks every 20 minutes
func (m *mux) checkChunks() {
	const duration = 1 * time.Minute
	for timer := time.NewTimer(duration); true; timer.Reset(duration) {
		<-timer.C
		chunksNeedToBeCheck := make([]*ChunkInfo, 0)
		now := time.Now()
		// check chunks every 20 minutes
		expiredTimeStamp := now.Add(-time.Minute * 20).Unix()
		m.mutex.Lock()
		for _, chunk := range m.chunks {
			if chunk.lastChecked <= expiredTimeStamp {
				chunksNeedToBeCheck = append(chunksNeedToBeCheck, chunk)
			}
		}
		m.mutex.Unlock()

		chunksNeedToBeRemoved := make([]*ChunkInfo, 0)
		chunksChecked := make([]*ChunkInfo, 0)
		var hash string
		for _, chunk := range chunksNeedToBeCheck {
			fi, err := os.Open(m.getFilename(chunk.ChunkID))
			if err == nil {
				hash, err = utils.HashReader(fi)
				fi.Close()
				if err != nil {
					continue
				}
				if hash != chunk.Hash {
					err = errChunkHashMismatch
				}
			}
			if err != nil {
				fmt.Printf("[storage-node] the chunk %d is broken, remove it from storage, reason: %v\n", chunk.ChunkID, err)
				chunksNeedToBeRemoved = append(chunksNeedToBeRemoved, chunk)
			} else {
				chunksChecked = append(chunksChecked, chunk)
			}
		}
		timestamp := now.Unix()
		m.mutex.Lock()
		for _, chunk := range chunksNeedToBeRemoved {
			m.removeChunk(chunk.ChunkID, true, nil)
		}
		for _, chunk := range chunksChecked {
			chunk.updateLastChecked(timestamp)
		}
		m.mutex.Unlock()
	}
}

// sendHeartbeat sends heartbeat to controller every 10 seconds
// it will contains the chunks the node owns and the disk space usage.
//
// After receiving the response, the controller will tell the node to
// remove the chunks that are not needed. And the node will remove
// those chunks.
func (m *mux) sendHeartbeat() {
	const duration = 10 * time.Second
	for timer := time.NewTimer(duration); true; timer.Reset(duration) {
		<-timer.C
		metadata, err := m.gatherMetadata()
		if err != nil {
			fmt.Printf("update heartbeat error: %v\n", err)
		} else {
			resp, err := m.cli.SendRequest(&node.Heartbeat{
				Metadata: metadata,
			})
			if err != nil {
				fmt.Printf("update heartbeat error: %v\n", err)
			} else {
				fmt.Println("heartbeat sent")
				respMsg := resp.(*node.HeartbeatResponse)
				reason := errors.New("removed by controller")
				m.mutex.Lock()
				for _, chunkId := range respMsg.RemoveChunks {
					m.removeChunk(chunkId, true, reason)
				}
				m.mutex.Unlock()
			}
		}
	}
}

// findChunkById finds the chunk by id
// it will find the chunk in the stored chunks (the `chunks` field),
// and the candiate chunks (the `candiateChunks` field)
func (m *mux) findChunkById(chunkId uint64) (c *ChunkInfo, isCandiate bool, ok bool) {
	c, ok = m.chunks[chunkId]
	if ok {
		return c, false, true
	}
	if c, ok = m.candiateChunks[chunkId]; ok {
		return c, true, true
	}
	return
}

// getFilename returns the storage path of the chunk
func (m *mux) getFilename(chunkId uint64) string {
	return filepath.Join(m.storagePath, strconv.FormatUint(chunkId, 10))
}

// putChunkRequest handles the PutChunk RPC request
func (m *mux) putChunkRequest(ctx server.Context, req proto.Message) (resp proto.Message, err error) {
	reqMsg := req.(*node.PutChunk)
	// generate hash
	var hash string
	hash, err = utils.HashBytes(reqMsg.Chunk.Data)
	if err != nil {
		return nil, err
	}
	if reqMsg.Chunk.Size != nil {
		if len(reqMsg.Chunk.Data) != int(*reqMsg.Chunk.Size) {
			return nil, errors.New("chunk size mismatch")
		}
	} else {
		reqMsg.Chunk.Size = proto.Uint64(uint64(len(reqMsg.Chunk.Data)))
	}
	m.mutex.Lock()
	if chunk, _, ok := m.findChunkById(reqMsg.Chunk.Id); ok {
		m.mutex.Unlock()
		if chunk.Hash != hash {
			return nil, errChunkHashMismatch
		}
		return &common.EmptyResponse{}, nil
	}
	reqMsg.Chunk.Hash = &hash
	// add the chunk to the candidate map
	ci := ChunkInfo{
		ChunkID:     reqMsg.Chunk.Id,
		Hash:        hash,
		Size:        *reqMsg.Chunk.Size,
		lastChecked: time.Now().Unix(),
	}
	m.candiateChunks[reqMsg.Chunk.Id] = &ci
	m.mutex.Unlock()
	defer func() {
		m.mutex.Lock()
		if err == nil {
			m.chunks[reqMsg.Chunk.Id] = m.candiateChunks[reqMsg.Chunk.Id]
		}
		delete(m.candiateChunks, reqMsg.Chunk.Id)
		m.mutex.Unlock()
	}()
	filename := m.getFilename(reqMsg.Chunk.Id)
	err = os.WriteFile(filename, reqMsg.Chunk.Data, 0644)
	if err != nil {
		return nil, err
	}
	// send to controller and other nodes if needed
	if reqMsg.Replicas[0] == m.listenAddress {
		// send to other node
		otherReplicas := reqMsg.Replicas[1:]
		errs := make([]chan error, len(otherReplicas))
		for i, replica := range otherReplicas {
			errs[i] = make(chan error, 1)
			go func(replica string, req *node.PutChunk) {
				defer close(errs[i])
				if _, err := utils.SendSingleRequest(replica, req); err != nil {
					errs[i] <- err
				} else {
					errs[i] <- nil
				}
			}(replica, reqMsg)
		}
		joinErrs := make([]error, 0, len(otherReplicas))
		for _, err := range errs {
			joinErrs = append(joinErrs, <-err)
		}
		replicas := make([]string, 0, len(reqMsg.Replicas))
		replicas = append(replicas, m.id)
		for i := range joinErrs {
			if joinErrs[i] == nil {
				replicas = append(replicas, otherReplicas[i])
			}
		}
		// else report to controller
		// only the primary node will report to controller
		_, err = m.cli.SendRequest(&node.ChunkUploaded{
			Id:       ci.ChunkID,
			Size:     ci.Size,
			Replicas: replicas,
			Hash:     hash,
		})
		if err != nil {
			return nil, err
		}
	}

	return &common.EmptyResponse{}, nil
}

// onceFunc will only call the passed function once
// when the returned function is called.
func onceFunc(f func()) func() {
	done := false
	return func() {
		if !done {
			f()
			done = true
		}
	}
}

// sendChunk will send the chunk to the nodes specified in the replicas,
// and unlock the mutex.
func (m *mux) sendChunk(c *ChunkInfo, replicas []string) ([]string, error) {
	unlock := onceFunc(m.mutex.Unlock)
	defer unlock()
	filename := m.getFilename(c.ChunkID)
	data, err := os.ReadFile(filename)
	if err != nil {
		// the chunk is broken, remove it
		m.removeChunk(c.ChunkID, true, err)
		return nil, err
	}
	// do not prevent other tasks
	unlock()
	// check checksum
	hash, err := utils.HashBytes(data)
	if err != nil {
		return nil, err
	}
	if hash != c.Hash {
		// the chunk is broken, remove it
		m.removeChunk(c.ChunkID, false, errChunkHashMismatch)
		return nil, errChunkHashMismatch
	}
	// send the chunk to other nodes
	successNode := make([]string, 0, len(replicas))
	cp := &node.PutChunk{
		Chunk: &node.Chunk{
			Id:   c.ChunkID,
			Data: data,
			Hash: &c.Hash,
			Size: &c.Size,
		},
	}
	for _, node := range replicas {
		if node == m.id {
			continue
		}
		// only contains the destination node so the destination
		// node will report the acquisition of the chunk
		cp.Replicas = []string{node}
		if _, err := utils.SendSingleRequest(node, cp); err != nil {
			log.Printf("send chunk to %s failed, %v\n", node, err)
		} else {
			successNode = append(successNode, node)
		}
	}
	return successNode, nil
}

// sendChunkRequest handles the SendChunk RPC request
func (m *mux) sendChunkRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	reqMsg := req.(*node.SendChunk)
	m.mutex.Lock()
	c, ok := m.chunks[reqMsg.Id]
	if !ok {
		m.mutex.Unlock()
		return nil, errChunkNotFound
	}
	// the following function call will unlock the mutex
	successNode, err := m.sendChunk(c, reqMsg.Replicas)
	if err != nil {
		return nil, err
	}
	return &node.SendChunkResponse{
		Replicas: successNode,
	}, nil
}

// sendChunksRequest handles the SendChunks RPC request
func (m *mux) sendChunksRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	reqMsg := req.(*node.SendChunks)
	resp := &node.SendChunksResponse{}
	for _, chunk := range reqMsg.Chunks {
		m.mutex.Lock()
		c, ok := m.chunks[chunk.Id]
		if !ok {
			resp.Chunks = append(resp.Chunks, &node.SendChunkResponse{})
			m.mutex.Unlock()
			continue
		}
		// the following function call will unlock the mutex
		successNode, err := m.sendChunk(c, chunk.Replicas)
		if err != nil {
			fmt.Printf("[storage-node] failed to send chunk %d: %v\n", chunk.Id, err)
		}
		resp.Chunks = append(resp.Chunks, &node.SendChunkResponse{
			Replicas: successNode,
		})
	}
	return resp, nil
}

// deleteChunkRequest handles the DeleteChunk RPC request
func (m *mux) deleteChunkRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	deleteChunk := req.(*node.DeleteChunk)
	m.removeChunk(deleteChunk.Id, false, nil)
	return &common.EmptyResponse{}, nil
}

// removeChunk removes the chunk from the storage
func (m *mux) removeChunk(chunkId uint64, locked bool, reason error) {
	if !locked {
		m.mutex.Lock()
		defer m.mutex.Unlock()
	}
	if _, ok := m.chunks[chunkId]; !ok {
		return
	}
	if reason != nil {
		fmt.Printf("[storage-node] the chunk %d is broken, remove it from storage, reason: %v\n", chunkId, reason)
	}
	delete(m.chunks, chunkId)
	os.Remove(m.getFilename(chunkId))
}

// getChunkRequest handles the GetChunk RPC request
func (m *mux) getChunkRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	reqMsg := req.(*node.GetChunk)
	m.mutex.Lock()
	c, ok := m.chunks[reqMsg.Id]
	if !ok {
		m.mutex.Unlock()
		return nil, errChunkNotFound
	}
	// read file within the mutex
	// actually, we can open the file and unlock the mutex
	// and then read the file outside the mutex
	// This should be okay on Linux, but not on Windows
	bytes, err := os.ReadFile(m.getFilename(reqMsg.Id))
	if err != nil {
		m.removeChunk(reqMsg.Id, true, err)
		m.mutex.Unlock()
		return nil, err
	}
	m.mutex.Unlock()
	// calculate checksum
	hash, err := utils.HashBytes(bytes)
	if err != nil {
		return nil, err
	}
	if hash != c.Hash {
		m.removeChunk(reqMsg.Id, false, errChunkHashMismatch)
		return nil, errChunkHashMismatch
	}
	return &node.Chunk{
		Id:   reqMsg.Id,
		Data: bytes,
		Hash: &hash,
		Size: &c.Size,
	}, nil
}

// getFreespace returns the free space of the storage
// it only support Unix-like system
func (m *mux) getFreespace() (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(m.storagePath, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

//func (m *mux) getFreespace() (uint64, error) {
//	return uint64((rand.Intn(10) + 1) * 1024 * 1024 * 1024), nil
//}

// gatherMetadata gathers the metadata of the storage node
func (m *mux) gatherMetadata() (*node.Metadata, error) {
	freespace, err := m.getFreespace()
	if err != nil {
		return nil, err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	chunkIds := make([]uint64, 0, len(m.chunks))
	usedSpace := uint64(0)
	for _, chunk := range m.chunks {
		usedSpace += chunk.Size
		chunkIds = append(chunkIds, chunk.ChunkID)
	}
	metadata := &node.Metadata{
		Id:         m.id,
		Totalspace: freespace + usedSpace,
		Freespace:  freespace,
		Chunks:     chunkIds,
	}
	return metadata, nil
}

// register registers the storage node to the controller
func (m *mux) register() error {
	metadata, err := m.gatherMetadata()
	if err != nil {
		return err
	}
	req := &node.Register{
		ListenAddr: m.listenAddress,
		Metadata:   metadata,
	}
	_, err = m.cli.SendRequest(req)
	return err
}

// ================= MapReduce Handlers ==================

// mapTaskRequest handles the MapTaskRequest from the controller
func (m *mux) mapTaskRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	mapReq := req.(*node.MapTaskRequest)
	log.Printf("[Node %s] Received MapTask: Job=%s, Task=%d, Chunk=%d, MapFuncID=%s",
		m.id, mapReq.JobId, mapReq.TaskId, mapReq.InputChunkId, mapReq.MapFuncId)

	var success bool = false
	var errorMessage string = ""
	outputKeys := make([]string, 0)
	var err error
	var plug *plugin.Plugin
	intermediateKVs := []types.KeyValue{} // Used to store Map output
	pluginId := mapReq.MapFuncId
	pluginPath := ""
	var mapFunc func(string, string) []types.KeyValue // Define a function type variable
	// 1. Defer sending completion report
	defer func() {
		log.Printf("[Node %s] Sending MapTask completion: Job=%s, Task=%d, Success=%t, OutputKeys=%v",
			m.id, mapReq.JobId, mapReq.TaskId, success, outputKeys)
		completionReq := &node.TaskCompletedRequest{
			JobId:        mapReq.JobId,
			TaskId:       mapReq.TaskId,
			IsMapTask:    true,
			Success:      success,
			ErrorMessage: errorMessage,
			OutputKeys:   outputKeys, // Report the actual intermediate keys generated
		}
		_, reportErr := m.cli.SendRequest(completionReq)
		if reportErr != nil {
			log.Printf("[Node %s] Failed to send MapTask completion report for Job %s, Task %d: %v", m.id, mapReq.JobId, mapReq.TaskId, reportErr)
		}
	}()
	// Check cache (Protected by Mutex)
	m.pluginMutex.Lock()
	cachedPlug, found := m.pluginCache[pluginId]
	m.pluginMutex.Unlock() // Unlock quickly after reading the cache
	// 2. Load and execute plugins
	if found {
		log.Printf("[Node %s] Using cached plugin handle for '%s'", m.id, pluginId)
		plug = cachedPlug
	} else {
		log.Printf("[Node %s] Plugin '%s' not in cache. Downloading and loading...", m.id, pluginId)

		// Download Plugin from Controller
		pluginCode, err := m.downloadPlugin(mapReq.MapFuncId) // Use MapFuncId as PluginId
		if err != nil {
			errorMessage = fmt.Sprintf("failed to download plugin '%s': %v", mapReq.MapFuncId, err)
			log.Printf("[Node %s] Error for MapTask Job=%s, Task=%d: %s", m.id, mapReq.JobId, mapReq.TaskId, errorMessage)
			// Return job state if fail
			return &node.TaskCompletedRequest{
				JobId:        mapReq.JobId,
				TaskId:       mapReq.TaskId,
				IsMapTask:    true,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}

		// Write to Temporary File
		tmpFile, err := os.CreateTemp("", fmt.Sprintf("plugin-%s-*.so", mapReq.MapFuncId))
		if err != nil {
			errorMessage = fmt.Sprintf("failed to create temp file for plugin '%s': %v", mapReq.MapFuncId, err)
			log.Printf("[Node %s] Error for MapTask Job=%s, Task=%d: %s", m.id, mapReq.JobId, mapReq.TaskId, errorMessage)
			// Return job state if fail
			return &node.TaskCompletedRequest{
				JobId:        mapReq.JobId,
				TaskId:       mapReq.TaskId,
				IsMapTask:    true,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}
		pluginPath := tmpFile.Name()
		log.Printf("[Node %s] Writing downloaded plugin '%s' to temp file %s", m.id, mapReq.MapFuncId, pluginPath)
		_, err = tmpFile.Write(pluginCode)
		if err == nil {
			err = tmpFile.Close() // Close file before opening as plugin
		} else {
			tmpFile.Close() // Close even on write error
		}
		// Defer removal of the temporary file
		defer func() {
			log.Printf("[Node %s] Removing temp plugin file: %s", m.id, pluginPath)
			os.Remove(pluginPath)
		}()
		if err != nil {
			errorMessage = fmt.Sprintf("failed to write/close temp file %s for plugin '%s': %v", pluginPath, mapReq.MapFuncId, err)
			log.Printf("[Node %s] Error for MapTask Job=%s, Task=%d: %s", m.id, mapReq.JobId, mapReq.TaskId, errorMessage)
			// Return job state if fail
			return &node.TaskCompletedRequest{
				JobId:        mapReq.JobId,
				TaskId:       mapReq.TaskId,
				IsMapTask:    true,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}

		//Load Plugin from Temp File
		log.Printf("[Node %s] Loading plugin: %s for map task %d", m.id, pluginPath, mapReq.TaskId)
		loadedPlug, err := plugin.Open(pluginPath)
		if err != nil {
			// ... (handle plugin open error) ...
			errorMessage = fmt.Sprintf("failed to open plugin %s: %v", pluginPath, err)
			log.Printf("[Node %s] Error for MapTask Job=%s, Task=%d: %s", m.id, mapReq.JobId, mapReq.TaskId, errorMessage)
			return &node.TaskCompletedRequest{
				JobId:        mapReq.JobId,
				TaskId:       mapReq.TaskId,
				IsMapTask:    true,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}
		log.Printf("[Node %s] Successfully opened plugin: %s", m.id, pluginPath)

		// Store in Cache (Protected by Mutex)
		m.pluginMutex.Lock()
		if existingPlug, exists := m.pluginCache[pluginId]; exists {
			plug = existingPlug
			log.Printf("[Node %s] Another routine loaded plugin '%s' concurrently. Using existing.", m.id, pluginId)
		} else {
			m.pluginCache[pluginId] = loadedPlug
			plug = loadedPlug
			log.Printf("[Node %s] Stored new plugin handle for '%s' in cache", m.id, pluginId)
		}
		m.pluginMutex.Unlock()
	}
	// 3. Lookup Symbol (Assume standard names 'Map' or 'Reduce' for now, or use FuncId directly if unique)
	funcSymbolName := "WordCountMap" // Or derive from mapReq.MapFuncId if more dynamic
	log.Printf("[Node %s] Looking up symbol: %s in plugin", m.id, funcSymbolName)
	mapSymbol, err := plug.Lookup(funcSymbolName)
	if err != nil {
		// ... (handle lookup error) ...
		errorMessage = fmt.Sprintf("failed to lookup symbol %s in plugin %s: %v", funcSymbolName, pluginPath, err)
		log.Printf("[Node %s] Error for MapTask Job=%s, Task=%d: %s", m.id, mapReq.JobId, mapReq.TaskId, errorMessage)
		return &node.TaskCompletedRequest{
			JobId:        mapReq.JobId,
			TaskId:       mapReq.TaskId,
			IsMapTask:    true,
			Success:      false,
			ErrorMessage: errorMessage,
		}, nil
	}

	// Type Assertion
	var ok bool
	mapFunc, ok = mapSymbol.(func(string, string) []types.KeyValue) // Use local KeyValue
	if !ok {
		// ... (handle type assertion error) ...
		errorMessage = fmt.Sprintf("unexpected type for symbol %s in plugin %s", funcSymbolName, pluginPath)
		log.Printf("[Node %s] Error for MapTask Job=%s, Task=%d: %s", m.id, mapReq.JobId, mapReq.TaskId, errorMessage)
		return &node.TaskCompletedRequest{
			JobId:        mapReq.JobId,
			TaskId:       mapReq.TaskId,
			IsMapTask:    true,
			Success:      false,
			ErrorMessage: errorMessage,
		}, nil
	}
	log.Printf("[Node %s] Successfully loaded map function %s from plugin", m.id, funcSymbolName)
	// --- Plugin Handling End ---

	//4. Get input chunk data locally
	chunkData, err := m.getLocalChunkData(mapReq.InputChunkId)
	if err != nil {
		errorMessage = fmt.Sprintf("failed to read input chunk %d: %v", mapReq.InputChunkId, err)
		log.Printf("[Node %s] Error for MapTask Job=%s, Task=%d: %s", m.id, mapReq.JobId, mapReq.TaskId, errorMessage)
		return &node.TaskCompletedRequest{
			JobId:        mapReq.JobId,
			TaskId:       mapReq.TaskId,
			IsMapTask:    true,
			Success:      false,
			ErrorMessage: errorMessage,
		}, nil
	}
	// Load and execute map function in plugins
	log.Printf("[Node %s] MapTask Job=%s, Task=%d: Executing loaded map function\\' %s \\'on chunk %d...",
		m.id, mapReq.JobId, mapReq.TaskId, mapReq.MapFuncId, mapReq.InputChunkId)

	//Use defer-recover to catch potential panics
	func() {
		defer func() {
			if r := recover(); r != nil {
				errStr := fmt.Sprintf("mapper function panicked: %v", r)
				errorMessage = errStr
				log.Printf("[Node %s] Error for MapTask Job=%s, Task=%d: %s", m.id, mapReq.JobId, mapReq.TaskId, errStr)
			}
		}()
		intermediateKVs = mapFunc("chunk-"+strconv.FormatUint(mapReq.InputChunkId, 10), string(chunkData)) // <<< execute loaded function
	}()

	// check panic
	if errorMessage != "" {
		return &node.TaskCompletedRequest{
			JobId:        mapReq.JobId,
			TaskId:       mapReq.TaskId,
			IsMapTask:    true,
			Success:      false,
			ErrorMessage: errorMessage,
		}, nil
	}

	log.Printf("[Node %s] MapTask Job=%s, Task=%d: Loaded map function produced %d intermediate pairs.",
		m.id, mapReq.JobId, mapReq.TaskId, len(intermediateKVs))

	// 5. Partition intermediate results by target reducer
	partitionedData := make(map[uint32]*bytes.Buffer)
	for i := uint32(0); i < mapReq.NumReduceTasks; i++ {
		partitionedData[i] = new(bytes.Buffer)
	}

	//6. Partition the intermediate key-value pairs and write them into the corresponding buffers
	hasher := fnv.New32a()
	for _, kv := range intermediateKVs { // <<< intermediateKVs generated by the plugin
		hasher.Reset()
		hasher.Write([]byte(kv.Key)) // Partition based on the key
		reducerIdx := hasher.Sum32() % mapReq.NumReduceTasks

		// JSON encode
		encodedLine, errEnc := json.Marshal(kv) // <<< used defined KeyValue
		if errEnc != nil {
			errorMessage = fmt.Sprintf("failed to encode intermediate key-value %v: %v", kv, errEnc)
			log.Printf("[Node %s] Error for MapTask Job=%s, Task=%d: %s", m.id, mapReq.JobId, mapReq.TaskId, errorMessage)
			return &node.TaskCompletedRequest{
				JobId:        mapReq.JobId,
				TaskId:       mapReq.TaskId,
				IsMapTask:    true,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}
		partitionedData[reducerIdx].Write(encodedLine)
		partitionedData[reducerIdx].WriteByte('\n') // Add newline separator
	}

	// 7. Write partitioned intermediate output files back to DFS
	controllerClientPort := *controllerClientAddr // Assuming default
	log.Printf("[Node %s] MapTask Job=%s, Task=%d: Writing %d intermediate partitions...",
		m.id, mapReq.JobId, mapReq.TaskId, mapReq.NumReduceTasks)

	var wg sync.WaitGroup
	errChan := make(chan error, mapReq.NumReduceTasks)
	outputKeysChan := make(chan string, mapReq.NumReduceTasks)

	for i := uint32(0); i < mapReq.NumReduceTasks; i++ {
		partitionBytes := partitionedData[i].Bytes()
		if len(partitionBytes) == 0 {
			continue // Skip empty partitions
		}

		intermediateKey := fmt.Sprintf("%s%d", mapReq.OutputKeyPrefix, i)
		wg.Add(1)
		go func(key string, data []byte, partitionIdx uint32) {
			defer wg.Done()
			log.Printf("[Node %s] MapTask Job=%s, Task=%d: Storing intermediate partition %d (key: '%s', size: %d bytes)",
				m.id, mapReq.JobId, mapReq.TaskId, partitionIdx, key, len(data))
			//Write data back to dfs as IntermediateFiles
			err := m.storeIntermediateData(controllerClientPort, key, data)
			if err != nil {
				errChan <- fmt.Errorf("failed to store intermediate partition %d (key %s): %w", partitionIdx, key, err)
				return
			}
			outputKeysChan <- key
		}(intermediateKey, partitionBytes, i)
	}

	wg.Wait()
	close(errChan)
	close(outputKeysChan)

	// 8. Collect intermediateFiles key and sort
	for k := range outputKeysChan {
		outputKeys = append(outputKeys, k)
	}
	sort.Strings(outputKeys) // Sort for deterministic reporting

	// Check for errors during upload
	firstError := <-errChan // Get first error, if any
	if firstError != nil {
		errorMessage = firstError.Error()
		log.Printf("[Node %s] Error during intermediate storage for MapTask Job=%s, Task=%d: %s",
			m.id, mapReq.JobId, mapReq.TaskId, errorMessage)
		// Output keys generated so far might be incomplete/partially written
		outputKeys = []string{} // Clear output keys on error
		// Return job state
		return &node.TaskCompletedRequest{
			JobId:        mapReq.JobId,
			TaskId:       mapReq.TaskId,
			IsMapTask:    true,
			Success:      false,
			ErrorMessage: errorMessage,
		}, nil
	}

	// If we get here, all partitions were stored successfully
	success = true
	log.Printf("[Node %s] Completed MapTask: Job=%s, Task=%d. Success: %t. Intermediate Keys: %v",
		m.id, mapReq.JobId, mapReq.TaskId, success, outputKeys)

	// Return job state
	return &node.TaskCompletedRequest{
		JobId:      mapReq.JobId,
		TaskId:     mapReq.TaskId,
		IsMapTask:  true,
		Success:    success,
		OutputKeys: outputKeys,
	}, nil
}

// getLocalChunkData reads chunk data directly from the node's storage.
func (m *mux) getLocalChunkData(chunkId uint64) ([]byte, error) {
	m.mutex.Lock()
	chunkInfo, ok := m.chunks[chunkId]
	if !ok {
		m.mutex.Unlock()
		return nil, errChunkNotFound
	}
	filename := m.getFilename(chunkId)
	m.mutex.Unlock()

	data, err := os.ReadFile(filename)
	if err != nil {
		m.removeChunk(chunkId, false, err) // Mark as broken if read fails
		return nil, fmt.Errorf("failed reading chunk file %s: %w", filename, err)
	}

	// Verify checksum before returning
	hash, err := utils.HashBytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed hashing chunk %d: %w", chunkId, err)
	}
	if hash != chunkInfo.Hash {
		m.removeChunk(chunkId, false, errChunkHashMismatch)
		return nil, errChunkHashMismatch
	}

	return data, nil
}

// storeIntermediateData is a placeholder for writing data back to DFS.
// It needs a proper implementation using a DFS client.
func (m *mux) storeIntermediateData(controllerAddr string, key string, data []byte) error {
	// TODO: Implement proper DFS client logic here (now replaced by uploadDataToDFS)
	log.Printf("[Node %s] Placeholder: Storing data for key '%s' - Size: %d", m.id, key, len(data))
	// Simulate success for now
	return m.uploadDataToDFS(controllerAddr, key, data)
}

// readIntermediateData is a placeholder for reading data from DFS.
// It needs a proper implementation using a DFS client.
func (m *mux) readIntermediateData(controllerAddr string, key string) ([]byte, error) {
	// TODO: Implement proper DFS client logic here (now replaced by downloadDataFromDFS)
	log.Printf("[Node %s] Placeholder: Reading data for key '%s'", m.id, key)
	// Simulate success for now
	// return []byte(fmt.Sprintf("intermediate data for key %s", key)), nil
	return m.downloadDataFromDFS(controllerAddr, key)
}

// uploadDataToDFS handles uploading data bytes to the DFS.
func (m *mux) uploadDataToDFS(controllerClientAddr string, key string, data []byte) error {
	log.Printf("[Node %s] Uploading to DFS: Key='%s', Size=%d, ControllerClient=%s", m.id, key, len(data), controllerClientAddr)

	// 1. Connect to Controller (Client Port)
	connCtrl, err := net.Dial("tcp", controllerClientAddr)
	if err != nil {
		return fmt.Errorf("failed to dial controller client port %s: %w", controllerClientAddr, err)
	}
	ctrlCli := rpcClient.NewClient(connCtrl)
	defer ctrlCli.Close()

	// 2. Send initial StoreRequest
	fileSize := uint64(len(data))
	storeRespMsg, err := ctrlCli.SendRequest(&client.StoreRequest{
		Key:    key,
		Size:   fileSize,
		IsText: false, // Assume intermediate/final MR data isn't necessarily text
	})
	if err != nil {
		return fmt.Errorf("initial StoreRequest for key '%s' failed: %w", key, err)
	}
	resp, ok := storeRespMsg.(*client.StoreResponse)
	if !ok {
		return fmt.Errorf("unexpected response type for initial StoreRequest: %T", storeRespMsg)
	}

	// 3. Upload Chunks to Storage Nodes
	log.Printf("[Node %s] Uploading %d chunks for key '%s'", m.id, len(resp.Chunks), key)
	currentPos := uint64(0)
	for _, chunkInfo := range resp.Chunks {
		if len(chunkInfo.Replicas) == 0 {
			return fmt.Errorf("controller provided no replicas for chunk %d of key '%s'", chunkInfo.Id, key)
		}
		primaryReplicaAddr := chunkInfo.Replicas[0]
		start := currentPos
		end := currentPos + chunkInfo.Size
		if end > fileSize { // Ensure we don't read past the data buffer
			end = fileSize
		}
		chunkData := data[start:end]
		currentPos = end

		log.Printf("[Node %s] Uploading chunk %d (Size: %d) for key '%s' to primary replica %s",
			m.id, chunkInfo.Id, len(chunkData), key, primaryReplicaAddr)

		// Connect to primary replica and send PutChunk
		connReplica, err := net.Dial("tcp", primaryReplicaAddr)
		if err != nil {
			// TODO: Need retry logic / failover to other replicas? For now, fail fast.
			return fmt.Errorf("failed to dial primary replica %s for chunk %d: %w", primaryReplicaAddr, chunkInfo.Id, err)
		}
		replicaCli := rpcClient.NewClient(connReplica)

		putChunkReq := &node.PutChunk{
			Chunk: &node.Chunk{
				Id:   chunkInfo.Id,
				Data: chunkData,
			},
			Replicas: chunkInfo.Replicas, // Pass full replica list for potential replication chain
		}
		_, err = replicaCli.SendRequest(putChunkReq)
		replicaCli.Close() // Close connection after sending
		if err != nil {
			// TODO: Retry/failover?
			return fmt.Errorf("failed to send PutChunk %d to replica %s: %w", chunkInfo.Id, primaryReplicaAddr, err)
		}
		log.Printf("[Node %s] Successfully sent chunk %d to %s", m.id, chunkInfo.Id, primaryReplicaAddr)
	}

	// 4. Send Finish StoreRequest
	log.Printf("[Node %s] Sending finish StoreRequest for key '%s'", m.id, key)
	_, err = ctrlCli.SendRequest(&client.StoreRequest{
		Key:      key,
		Size:     fileSize,
		Finished: true,
	})
	if err != nil {
		return fmt.Errorf("finish StoreRequest for key '%s' failed: %w", key, err)
	}

	log.Printf("[Node %s] Successfully uploaded to DFS: Key='%s'", m.id, key)
	return nil
}

// downloadDataFromDFS handles downloading data bytes from the DFS.
func (m *mux) downloadDataFromDFS(controllerClientAddr string, key string) ([]byte, error) {
	log.Printf("[Node %s] Downloading from DFS: Key='%s', ControllerClient=%s", m.id, key, controllerClientAddr)

	// 1. Connect to Controller (Client Port)
	connCtrl, err := net.Dial("tcp", controllerClientAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial controller client port %s: %w", controllerClientAddr, err)
	}
	ctrlCli := rpcClient.NewClient(connCtrl)
	defer ctrlCli.Close()

	// 2. Send GetRequest
	getRespMsg, err := ctrlCli.SendRequest(&client.GetRequest{Key: key})
	if err != nil {
		return nil, fmt.Errorf("GetRequest for key '%s' failed: %w", key, err)
	}
	resp, ok := getRespMsg.(*client.GetResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type for GetRequest: %T", getRespMsg)
	}
	if len(resp.Chunks) == 0 {
		log.Printf("[Node %s] Warning: No chunks found for key '%s' during download.", m.id, key)
		return []byte{}, nil // Return empty byte slice for empty/non-existent file?
	}

	// 3. Download Chunks from Storage Nodes
	log.Printf("[Node %s] Downloading %d chunks for key '%s'", m.id, len(resp.Chunks), key)
	var fileDataBuffer bytes.Buffer
	// Calculate total size for pre-allocation (optional but good)
	var totalSize uint64
	for _, chunkInfo := range resp.Chunks {
		totalSize += chunkInfo.Size
	}
	fileDataBuffer.Grow(int(totalSize))

	for _, chunkInfo := range resp.Chunks {
		if len(chunkInfo.Replicas) == 0 {
			return nil, fmt.Errorf("controller provided no replicas for chunk %d of key '%s'", chunkInfo.Id, key)
		}

		var chunkData []byte
		var lastErr error
		success := false
		// Try replicas until one succeeds
		for _, replicaAddr := range chunkInfo.Replicas {
			log.Printf("[Node %s] Downloading chunk %d for key '%s' from replica %s",
				m.id, chunkInfo.Id, key, replicaAddr)
			connReplica, err := net.Dial("tcp", replicaAddr)
			if err != nil {
				lastErr = fmt.Errorf("failed to dial replica %s for chunk %d: %w", replicaAddr, chunkInfo.Id, err)
				log.Printf("[Node %s] %v", m.id, lastErr)
				continue // Try next replica
			}
			replicaCli := rpcClient.NewClient(connReplica)

			getChunkRespMsg, err := replicaCli.SendRequest(&node.GetChunk{Id: chunkInfo.Id})
			replicaCli.Close()
			if err != nil {
				lastErr = fmt.Errorf("failed to send GetChunk %d to replica %s: %w", chunkInfo.Id, replicaAddr, err)
				log.Printf("[Node %s] %v", m.id, lastErr)
				continue // Try next replica
			}
			chunk, ok := getChunkRespMsg.(*node.Chunk)
			if !ok {
				lastErr = fmt.Errorf("unexpected response type %T from GetChunk %d on %s", getChunkRespMsg, chunkInfo.Id, replicaAddr)
				log.Printf("[Node %s] %v", m.id, lastErr)
				continue // Try next replica
			}
			// TODO: Optional: Verify chunk hash if provided in node.Chunk?
			chunkData = chunk.Data
			success = true
			log.Printf("[Node %s] Successfully downloaded chunk %d from %s", m.id, chunkInfo.Id, replicaAddr)
			break // Success!
		}

		if !success {
			return nil, fmt.Errorf("failed to download chunk %d for key '%s' from all replicas. Last error: %w", chunkInfo.Id, key, lastErr)
		}
		fileDataBuffer.Write(chunkData)
	}

	log.Printf("[Node %s] Successfully downloaded from DFS: Key='%s', Size=%d", m.id, key, fileDataBuffer.Len())
	return fileDataBuffer.Bytes(), nil
}

// Handles the ReduceTaskRequest from the controller
func (m *mux) reduceTaskRequest(ctx server.Context, req proto.Message) (proto.Message, error) {
	reduceReq := req.(*node.ReduceTaskRequest)
	log.Printf("[Node %s] Received ReduceTask: Job=%s, Task=%d, InputKeys=%d, ReduceFunc=%s",
		m.id, reduceReq.JobId, reduceReq.TaskId, len(reduceReq.IntermediateKeys), reduceReq.ReduceFuncId)

	var success bool = false // Default to failure
	var errorMessage string = ""
	var finalOutputKey string = reduceReq.FinalOutputKey
	var err error
	finalOutputData := []byte{} // store the result
	// Defer sending completion report
	defer func() {
		log.Printf("[Node %s] Sending ReduceTask completion: Job=%s, Task=%d, Success=%t",
			m.id, reduceReq.JobId, reduceReq.TaskId, success)
		completionReq := &node.TaskCompletedRequest{
			JobId:        reduceReq.JobId,
			TaskId:       reduceReq.TaskId,
			IsMapTask:    false,
			Success:      success,
			ErrorMessage: errorMessage,
			OutputKeys:   []string{finalOutputKey}, // Report the final output key
		}
		_, reportErr := m.cli.SendRequest(completionReq)
		if reportErr != nil {
			log.Printf("[Node %s] Failed to send ReduceTask completion report for Job %s, Task %d: %v",
				m.id, reduceReq.JobId, reduceReq.TaskId, reportErr)
		}
	}()
	var plug *plugin.Plugin // Variable to hold the plugin handle
	pluginPath := ""        // Will be set only if loaded from temp file

	// --- Plugin Handling with Cache ---
	pluginId := reduceReq.ReduceFuncId
	// Determine symbol name (Could be passed in reduceReq or derived)
	funcSymbolName := "WordCountReduce" // Example: Needs to match exported name in plugin

	// Check cache (Protected by Mutex)
	m.pluginMutex.Lock()
	cachedPlug, found := m.pluginCache[pluginId]
	m.pluginMutex.Unlock() // Unlock quickly after reading the cache
	var reduceFunc func(string, []string) string

	if found {
		log.Printf("[Node %s] Using cached plugin handle for '%s'", m.id, pluginId)
		plug = cachedPlug
	} else {
		log.Printf("[Node %s] Plugin '%s' not in cache. Downloading and loading...", m.id, pluginId)

		// Download Plugin
		pluginCode, err := m.downloadPlugin(reduceReq.ReduceFuncId) // Use ReduceFuncId as PluginId
		if err != nil {
			errorMessage = fmt.Sprintf("failed to download plugin '%s': %v", reduceReq.ReduceFuncId, err)
			log.Printf("[Node %s] Error for ReduceTask Job=%s, Task=%d: %s", m.id, reduceReq.JobId, reduceReq.TaskId, errorMessage)
			return &node.TaskCompletedRequest{
				JobId:        reduceReq.JobId,
				TaskId:       reduceReq.TaskId,
				IsMapTask:    false,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}

		// Write to Temp File
		tmpFile, err := os.CreateTemp("", fmt.Sprintf("plugin-%s-*.so", reduceReq.ReduceFuncId))
		if err != nil {
			errorMessage = fmt.Sprintf("failed to create temp file for plugin '%s': %v", reduceReq.ReduceFuncId, err)
			log.Printf("[Node %s] Error for ReduceTask Job=%s, Task=%d: %s", m.id, reduceReq.JobId, reduceReq.TaskId, errorMessage)
			return &node.TaskCompletedRequest{
				JobId:        reduceReq.JobId,
				TaskId:       reduceReq.TaskId,
				IsMapTask:    false,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}
		pluginPath := tmpFile.Name()
		log.Printf("[Node %s] Writing downloaded plugin '%s' to temp file %s", m.id, reduceReq.ReduceFuncId, pluginPath)
		_, err = tmpFile.Write(pluginCode)
		if err == nil {
			err = tmpFile.Close()
		} else {
			tmpFile.Close()
		}
		defer func() { log.Printf("[Node %s] Removing temp plugin file: %s", m.id, pluginPath); os.Remove(pluginPath) }()
		if err != nil {
			errorMessage = fmt.Sprintf("failed to write/close temp file %s for plugin '%s': %v", pluginPath, reduceReq.ReduceFuncId, err)
			log.Printf("[Node %s] Error for ReduceTask Job=%s, Task=%d: %s", m.id, reduceReq.JobId, reduceReq.TaskId, errorMessage)
			return &node.TaskCompletedRequest{
				JobId:        reduceReq.JobId,
				TaskId:       reduceReq.TaskId,
				IsMapTask:    false,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}

		// Load Plugin
		log.Printf("[Node %s] Loading plugin: %s for reduce task %d", m.id, pluginPath, reduceReq.TaskId)
		loadedPlug, err := plugin.Open(pluginPath)
		if err != nil {
			// ... (handle plugin open error) ...
			errorMessage = fmt.Sprintf("failed to open plugin %s: %v", pluginPath, err)
			log.Printf("[Node %s] Error for ReduceTask Job=%s, Task=%d: %s", m.id, reduceReq.JobId, reduceReq.TaskId, errorMessage)
			return &node.TaskCompletedRequest{
				JobId:        reduceReq.JobId,
				TaskId:       reduceReq.TaskId,
				IsMapTask:    false,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}
		// Store in Cache (Protected by Mutex)
		m.pluginMutex.Lock()
		if existingPlug, exists := m.pluginCache[pluginId]; exists {
			plug = existingPlug
			log.Printf("[Node %s] Another routine loaded plugin '%s' concurrently. Using existing.", m.id, pluginId)
		} else {
			m.pluginCache[pluginId] = loadedPlug
			plug = loadedPlug
			log.Printf("[Node %s] Stored new plugin handle for '%s' in cache", m.id, pluginId)
		}
		m.pluginMutex.Unlock()
	}
	//  Lookup funcSymbol
	funcSymbolName = "WordCountReduce" // Or derive from reduceReq.ReduceFuncId
	log.Printf("[Node %s] Looking up symbol: %s in plugin", m.id, funcSymbolName)
	reduceSymbol, err := plug.Lookup(funcSymbolName)
	if err != nil {
		// ... (handle lookup error) ...
		errorMessage = fmt.Sprintf("failed to lookup symbol %s in plugin %s: %v", funcSymbolName, pluginPath, err)
		log.Printf("[Node %s] Error for ReduceTask Job=%s, Task=%d: %s", m.id, reduceReq.JobId, reduceReq.TaskId, errorMessage)
		return &node.TaskCompletedRequest{
			JobId:        reduceReq.JobId,
			TaskId:       reduceReq.TaskId,
			IsMapTask:    false,
			Success:      false,
			ErrorMessage: errorMessage,
		}, nil
	}

	// Type Assertion
	var ok bool
	reduceFunc, ok = reduceSymbol.(func(string, []string) string)
	if !ok {
		// ... (handle type assertion error) ...
		errorMessage = fmt.Sprintf("unexpected type for symbol %s in plugin %s", funcSymbolName, pluginPath)
		log.Printf("[Node %s] Error for ReduceTask Job=%s, Task=%d: %s", m.id, reduceReq.JobId, reduceReq.TaskId, errorMessage)
		return &node.TaskCompletedRequest{
			JobId:        reduceReq.JobId,
			TaskId:       reduceReq.TaskId,
			IsMapTask:    false,
			Success:      false,
			ErrorMessage: errorMessage,
		}, nil
	}
	log.Printf("[Node %s] Successfully loaded reduce function %s from plugin", m.id, funcSymbolName)
	// --- Plugin Handling End ---

	controllerClientPort := *controllerClientAddr // Assuming default
	intermediateKVs := []types.KeyValue{}
	log.Printf("[Node %s] ReduceTask Job=%s, Task=%d: Downloading %d intermediate files...",
		m.id, reduceReq.JobId, reduceReq.TaskId, len(reduceReq.IntermediateKeys))
	// 1. Get all intermediate file data for this reducer
	for _, key := range reduceReq.IntermediateKeys {
		log.Printf("[Node %s] ReduceTask Job=%s, Task=%d: Getting intermediate key '%s'", m.id, reduceReq.JobId, reduceReq.TaskId, key)
		//Read IntermediateData from controller with key
		data, err := m.readIntermediateData(controllerClientPort, key)
		if err != nil {
			// If an intermediate file is missing, maybe log warning and continue?
			// Or fail the task? Let's fail for now.
			errorMessage = fmt.Sprintf("failed to read intermediate key %s: %v", key, err)
			log.Printf("[Node %s] Error for ReduceTask Job=%s, Task=%d: %s", m.id, reduceReq.JobId, reduceReq.TaskId, errorMessage)
			return &node.TaskCompletedRequest{
				JobId:        reduceReq.JobId,
				TaskId:       reduceReq.TaskId,
				IsMapTask:    false,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}

		// 2. Decode the JSON lines
		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			// Use DecodeKeyValue
			var kv types.KeyValue
			errDec := json.Unmarshal(line, &kv)
			if errDec != nil {
				log.Printf("[Node %s] ReduceTask Job=%s, Task=%d: Failed to decode line '%s' from key '%s': %v. Skipping line.",
					m.id, reduceReq.JobId, reduceReq.TaskId, string(line), key, errDec)
				continue
			}
			//Put all the intermediateData to intermediateKVs
			intermediateKVs = append(intermediateKVs, kv)
		}
		if err := scanner.Err(); err != nil {
			// Error during scanning
			errorMessage = fmt.Sprintf("error scanning intermediate key %s: %v", key, err)
			log.Printf("[Node %s] Error for ReduceTask Job=%s, Task=%d: %s", m.id, reduceReq.JobId, reduceReq.TaskId, errorMessage)
			// Return job state
			return &node.TaskCompletedRequest{
				JobId:        reduceReq.JobId,
				TaskId:       reduceReq.TaskId,
				IsMapTask:    false,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}
	}
	log.Printf("[Node %s] ReduceTask Job=%s, Task=%d: Decoded %d total intermediate pairs.",
		m.id, reduceReq.JobId, reduceReq.TaskId, len(intermediateKVs))

	// 3. Group intermediate data by key e.g "apple":  {"2", "4", "1"}
	groupedValues := make(map[string][]string)
	for _, kv := range intermediateKVs {
		groupedValues[kv.Key] = append(groupedValues[kv.Key], kv.Value)
	}
	log.Printf("[Node %s] ReduceTask Job=%s, Task=%d: Grouped into %d unique keys.",
		m.id, reduceReq.JobId, reduceReq.TaskId, len(groupedValues))

	// 4. Execute reduce function for each key and collect results
	var finalOutputBuffer bytes.Buffer
	// Sort keys for deterministic output (optional, but good practice)
	uniqueKeys := make([]string, 0, len(groupedValues))
	for k := range groupedValues {
		uniqueKeys = append(uniqueKeys, k)
	}
	sort.Strings(uniqueKeys)

	log.Printf("[Node %s] ReduceTask Job=%s, Task=%d: Executing reduce function '%s' for %d keys...",
		m.id, reduceReq.JobId, reduceReq.TaskId, reduceReq.ReduceFuncId, len(uniqueKeys))
	//Call loaded function
	for _, key := range uniqueKeys {
		values := groupedValues[key]
		// Use defer-recover to catch potential panics
		func() {
			defer func() {
				if r := recover(); r != nil {
					errStr := fmt.Sprintf("reducer function panicked for key '%s': %v", key, r)
					errorMessage = errStr
					log.Printf("[Node %s] Error for ReduceTask Job=%s, Task=%d: %s", m.id, reduceReq.JobId, reduceReq.TaskId, errStr)
				}
			}()
			// Get the added frequence for unique keys, write to buffer
			reducedValue := reduceFunc(key, values)
			// Format output line, e.g., "key<TAB>value\n"
			fmt.Fprintf(&finalOutputBuffer, "%s\t%s\n", key, reducedValue)
		}()

		//
		//If an error message is set, a panic occurred and processing was interrupted
		if errorMessage != "" {
			// Return error if it fails
			return &node.TaskCompletedRequest{
				JobId:        reduceReq.JobId,
				TaskId:       reduceReq.TaskId,
				IsMapTask:    false,
				Success:      false,
				ErrorMessage: errorMessage,
			}, nil
		}
	}

	// 5. Write final output back to DFS
	finalOutputData = finalOutputBuffer.Bytes() //

	log.Printf("[Node %s] ReduceTask Job=%s, Task=%d: Storing final output key '%s' (Size: %d bytes)",
		m.id, reduceReq.JobId, reduceReq.TaskId, finalOutputKey, len(finalOutputData))
	err = m.storeIntermediateData(controllerClientPort, finalOutputKey, finalOutputData)
	if err != nil {
		errorMessage = fmt.Sprintf("failed to store output for key '%s': %v", finalOutputKey, err)
		log.Printf("[Node %s] Error for ReduceTask Job=%s, Task=%d: %s", m.id, reduceReq.JobId, reduceReq.TaskId, errorMessage)
		return &node.TaskCompletedRequest{
			JobId:        reduceReq.JobId,
			TaskId:       reduceReq.TaskId,
			IsMapTask:    false,
			Success:      false,
			ErrorMessage: errorMessage,
		}, nil
	}

	// All steps completed successfully
	success = true // Set success flag
	log.Printf("[Node %s] Completed ReduceTask: Job=%s, Task=%d. Success: %t. Final output key: %s",
		m.id, reduceReq.JobId, reduceReq.TaskId, success, finalOutputKey)

	return &node.TaskCompletedRequest{
		JobId:      reduceReq.JobId,
		TaskId:     reduceReq.TaskId,
		IsMapTask:  false,
		Success:    true,
		OutputKeys: []string{finalOutputKey},
	}, nil
}

// --- Add helper for downloading plugin ---
func (m *mux) downloadPlugin(pluginId string) ([]byte, error) {
	log.Printf("[Node %s] Requesting download of plugin '%s' from controller", m.id, pluginId)
	// Assuming m.cli connects to the controller's NODE port (e.g., 8081)
	//  registered the downloadPluginRequest handler
	req := &client.DownloadPluginRequest{PluginId: pluginId}
	respRaw, err := m.cli.SendRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send DownloadPluginRequest for '%s': %w", pluginId, err)
	}
	resp, ok := respRaw.(*client.DownloadPluginResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type for DownloadPluginRequest: %T", respRaw)
	}
	if !resp.Found {
		return nil, fmt.Errorf("plugin '%s' not found on controller: %s", pluginId, resp.ErrorMessage)
	}
	if resp.ErrorMessage != "" { // Controller might find it but still report an error reading it
		return nil, fmt.Errorf("controller reported error getting plugin '%s': %s", pluginId, resp.ErrorMessage)
	}
	if len(resp.PluginCode) == 0 {
		return nil, fmt.Errorf("controller returned empty code for plugin '%s'", pluginId)
	}
	log.Printf("[Node %s] Successfully downloaded plugin '%s' (%d bytes)", m.id, pluginId, len(resp.PluginCode))
	return resp.PluginCode, nil
}
