package main

import (
	"dfs/cmd/client/pfile"
	"dfs/cmd/client/taskmgr"
	"dfs/protogen/client"
	"dfs/protogen/common"
	"dfs/protogen/node"
	rpcClient "dfs/rpc/client"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"time"
)

type Context struct {
	cli *rpcClient.Client
}

func printUsage() {
	fmt.Printf(`Usage of %s: %s [OPTIONS] <COMMAND> [ARGS]
Options:
  -csa <address>             Controller server address.
  -h                         Print this help message.
  -aggregate                 (For mapreduce command) Perform final aggregation on controller (default false).
  -reducers <number>         (For mapreduce command) Number of reduce tasks to use (default 4).
Commands:
  upload <filename> <key>    Upload the file to remote file system with specified key.
  download <key> <filename>  Download the file from remote file system with specified key.
  delete <key>               Delete the file from remote file system with specified key.
  list [prefix]              List all files in remote file system with optional prefix.
  node [nodeId]              Show the node info, if the nodeId is not specified, it will show all nodes.
  mapreduce <input_key> <output_key> <map_id> <reduce_id>  Run a MapReduce job.
  upload_plugin <filepath> <plugin_id>  Upload a MapReduce plugin to the controller.
`, os.Args[0], os.Args[0])
}

func checkCommand(commands []string) error {
	if len(commands) == 0 {
		return errors.New("no command specified")
	}
	if commands[0] == "upload" || commands[0] == "download" {
		if len(commands) != 3 {
			return fmt.Errorf("%s command requires 2 arguments", commands[0])
		}
	} else if commands[0] == "delete" {
		if len(commands) != 2 {
			return errors.New("delete command requires 1 argument")
		}
	} else if commands[0] == "list" {
		if len(commands) > 2 {
			return errors.New("list command requires at most 1 argument")
		}
	} else if commands[0] == "node" {
		if len(commands) > 2 {
			return errors.New("node command requires at most 1 argument")
		}
	} else if commands[0] == "mapreduce" {
		if len(commands) != 5 {
			return errors.New("mapreduce command requires 4 arguments: <input_key> <output_key> <map_id> <reduce_id>")
		}
	} else if commands[0] == "upload_plugin" {
		if len(commands) != 3 {
			return errors.New("upload_plugin command requires 2 arguments: <filepath> <plugin_id>")
		}
	} else {
		return errors.New("unknown command")
	}
	return nil
}

func main() {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	controllerServerAddr := flagSet.String("csa", "localhost:8080", "Controller server address")
	aggregateFlag := flagSet.Bool("aggregate", false, "Perform final aggregation on controller for mapreduce")
	numReducersFlag := flagSet.Uint("reducers", 4, "Number of reduce tasks to use for mapreduce job")
	flagSet.Usage = printUsage
	flagSet.Parse(os.Args[1:])
	err := checkCommand(flagSet.Args())
	if err != nil {
		fmt.Println(err)
		printUsage()
		os.Exit(1)
	}

	conn, err := net.Dial("tcp", *controllerServerAddr)
	if err != nil {
		panic(err)
	}
	cli := rpcClient.NewClient(conn)
	defer cli.Close()
	ctx := Context{
		cli: cli,
	}
	var operation string
	args := flagSet.Args()
	switch args[0] {
	case "upload":
		err = ctx.StoreFile(args[1], args[2])
		operation = "upload file"
	case "download":
		err = ctx.DownloadFile(args[1], args[2])
		operation = "download file"
	case "delete":
		err = ctx.DeleteFile(args[1])
		operation = "delete file"
	case "list":
		prefix := ""
		if len(args) == 2 {
			prefix = args[1]
		}
		err = ctx.ListFiles(prefix)
		operation = "list files"
	case "node":
		var nodeId *string
		if len(args) == 2 {
			nodeId = &args[1]
		}
		err = ctx.ListNodes(nodeId)
	case "mapreduce":
		err = ctx.MapReduce(args[1], args[2], args[3], args[4], *aggregateFlag, uint32(*numReducersFlag))
		operation = "run mapreduce job"
	case "upload_plugin":
		err = ctx.UploadPlugin(args[1], args[2])
		operation = "upload plugin"
	default:
	}
	if err != nil {
		fmt.Printf("%s failed: %v\n", operation, err)
		os.Exit(1)
	}
}

type taskMgrContext struct {
	file *pfile.PFile
	cli  *rpcClient.Client
	mgr  *taskmgr.TaskManager
}

type fileRange struct {
	start   int64
	end     int64
	id      uint64
	servers []string
}

// handleUpload handles the upload task
// it will try to upload the file to the servers in the fileRange.servers
func (ctx *Context) handleUpload(c any, taskCtx any) error {
	tmc := c.(*taskMgrContext)
	fileRange := taskCtx.(*fileRange)
	data, err := tmc.file.ReadPart(fileRange.start, fileRange.end)
	if err != nil {
		return err
	}
	_, err = tmc.cli.SendRequest(&node.PutChunk{
		Chunk: &node.Chunk{
			Id:   fileRange.id,
			Data: data,
		},
		Replicas: fileRange.servers,
	})
	if err != nil {
		// try another server
		if len(fileRange.servers) <= 1 {
			return err
		}
		fileRange.servers = fileRange.servers[1:]
		tmc.mgr.AddTaskContext(fileRange.servers[0], fileRange)
		return nil
	}
	return nil
}

// gatherAllServers gathers all servers from the chunks
func gatherAllServers(chunks []*common.ChunkInfo) []string {
	servers := make(map[string]struct{})
	for _, chunk := range chunks {
		for _, server := range chunk.Replicas {
			servers[server] = struct{}{}
		}
	}
	res := make([]string, 0, len(servers))
	for server := range servers {
		res = append(res, server)
	}
	return res
}

func closeClientsFromTaskMgr(taskMgr *taskmgr.TaskManager, servers []string) {
	for _, server := range servers {
		ctx := taskMgr.GetContext(server)
		if ctx != nil {
			ctx.(*taskMgrContext).cli.Close()
		}
	}
}

// StoreFile uploads the file to the remote file system
func (ctx *Context) StoreFile(filename string, key string) error {
	// open the file to upload
	pfileHandle, err := pfile.NewPFile(filename, pfile.ReadPart)
	if err != nil {
		return err
	}
	defer pfileHandle.Close()
	// get file size
	fi, err := pfileHandle.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()

	// Check if the file is likely text
	isText, err := pfile.IsTextFile(filename)
	if err != nil {
		// Log the error but proceed? Or return error?
		// For now, let's log and assume it's not text if error occurs
		fmt.Printf("Warning: could not determine if file '%s' is text: %v\n", filename, err)
		isText = false
	}

	resp, err := ctx.cli.SendRequest(&client.StoreRequest{
		Key:    key,
		Size:   uint64(fileSize),
		IsText: isText,
	})
	if err != nil {
		return err
	}
	respMsg := resp.(*client.StoreResponse)
	allServers := gatherAllServers(respMsg.Chunks)

	taskMgr := taskmgr.NewTaskManager(ctx.handleUpload)
	defer closeClientsFromTaskMgr(taskMgr, allServers)
	for _, server := range allServers {
		conn, err := net.Dial("tcp", server)
		if err != nil {
			return err
		}
		taskMgr.AddContext(server, &taskMgrContext{
			file: pfileHandle,
			cli:  rpcClient.NewClient(conn),
			mgr:  taskMgr,
		})
	}
	rangeStart := uint64(0)
	for _, chunk := range respMsg.Chunks {
		end := rangeStart + chunk.Size
		fileRange := fileRange{
			start:   int64(rangeStart),
			end:     int64(end),
			id:      chunk.Id,
			servers: chunk.Replicas,
		}
		rangeStart = end
		taskMgr.AddTaskContext(chunk.Replicas[0], &fileRange)
	}
	err = taskMgr.Run()
	if err == nil {
		_, err = ctx.cli.SendRequest(&client.StoreRequest{
			Key:      key,
			Size:     uint64(fileSize),
			Finished: true,
		})
	}
	if err == nil {
		fmt.Printf("file %s uploaded successfully\n", key)
	}
	return err
}

// handleDownload handles the download task
func (ctx *Context) handleDownload(c any, taskCtx any) error {
	tmc := c.(*taskMgrContext)
	fileRange := taskCtx.(*fileRange)
	resp, err := tmc.cli.SendRequest(&node.GetChunk{
		Id: fileRange.id,
	})
	if err != nil {
		// try another server
		if len(fileRange.servers) == 0 {
			return err
		}
		fileRange.servers = fileRange.servers[1:]
		tmc.mgr.AddTaskContext(fileRange.servers[0], fileRange)
		return nil
	}
	chunk := resp.(*node.Chunk)
	return tmc.file.WritePart(fileRange.start, fileRange.end, chunk.Data)
}

// DownloadFile downloads the file from the remote file system
func (ctx *Context) DownloadFile(key string, filename string) error {
	pfile, err := pfile.NewPFile(filename, pfile.WritePart)
	if err != nil {
		return err
	}
	defer pfile.Close()

	resp, err := ctx.cli.SendRequest(&client.GetRequest{
		Key: key,
	})
	if err != nil {
		return err
	}

	respMsg := resp.(*client.GetResponse)
	allServers := gatherAllServers(respMsg.Chunks)

	taskMgr := taskmgr.NewTaskManager(ctx.handleDownload)
	defer closeClientsFromTaskMgr(taskMgr, allServers)
	for _, server := range allServers {
		conn, err := net.Dial("tcp", server)
		if err != nil {
			return err
		}
		taskMgr.AddContext(server, &taskMgrContext{
			file: pfile,
			cli:  rpcClient.NewClient(conn),
			mgr:  taskMgr,
		})
	}

	rangeStart := uint64(0)
	for _, chunk := range respMsg.Chunks {
		end := rangeStart + chunk.Size
		fileRange := fileRange{
			start:   int64(rangeStart),
			end:     int64(end),
			id:      chunk.Id,
			servers: chunk.Replicas,
		}
		rangeStart = end
		taskMgr.AddTaskContext(chunk.Replicas[0], &fileRange)
	}

	err = taskMgr.Run()
	if err == nil {
		fmt.Printf("Download file '%s' to '%s' successfully\n", key, filename)
	}
	return err
}

// DeleteFile deletes the file from the remote file system
func (ctx *Context) DeleteFile(key string) error {
	resp, err := ctx.cli.SendRequest(&client.DeleteRequest{
		Key: key,
	})
	if err != nil {
		return err
	}
	respMsg := resp.(*client.DeleteResponse)
	if respMsg.IsCandiate {
		fmt.Printf("tmp file '%s' deleted\n", key)
	} else {
		fmt.Printf("file '%s' deleted\n", key)
	}
	return nil
}

// ListFiles lists the files from the remote file system
func (ctx *Context) ListFiles(prefix string) error {
	resp, err := ctx.cli.SendRequest(&client.ListRequest{
		Prefix: prefix,
	})
	if err != nil {
		return err
	}
	respMsg := resp.(*client.ListResponse)
	fmt.Printf("Total files: %d\n", len(respMsg.Files))
	for _, file := range respMsg.Files {
		fmt.Printf("%d\t%s\t%s\n", file.Size, file.CreatedAt.AsTime().Local().Format(time.DateTime), file.Key)
	}
	return nil
}

// ListNodes lists the nodes from the remote file system
func (ctx Context) ListNodes(nodeId *string) error {
	resp, err := ctx.cli.SendRequest(&client.NodeInfoRequest{
		NodeId: nodeId,
	})
	if err != nil {
		return err
	}
	respMsg := resp.(*client.NodeInfoResponse)
	if nodeId == nil {
		fmt.Printf("Total nodes: %d\n", len(respMsg.Nodes))
	}
	fmt.Printf("%15s%15s%15s\n", "node id", "free space", "total space")
	for _, node := range respMsg.Nodes {
		fmt.Printf("%15s%15d%15d\n", node.Id, node.Freespace, node.Totalspace)
	}
	return nil
}

// MapReduce sends a request to the controller to start a MapReduce job.
func (ctx *Context) MapReduce(inputKey, outputKey, mapFuncId, reduceFuncId string, controllerAggregate bool, numReduceTasks uint32) error {
	req := &client.MapReduceRequest{
		InputKey:            inputKey,
		OutputKey:           outputKey,
		MapFuncId:           mapFuncId,
		ReduceFuncId:        reduceFuncId,
		ControllerAggregate: controllerAggregate,
		NumReduceTasks:      numReduceTasks,
	}

	fmt.Printf("Sending MapReduce request: Input='%s', Output='%s', Map='%s', Reduce='%s', Aggregate=%t, Reducers=%d...\n",
		inputKey, outputKey, mapFuncId, reduceFuncId, controllerAggregate, numReduceTasks)

	resp, err := ctx.cli.SendRequest(req)
	if err != nil {
		return fmt.Errorf("failed to send MapReduce request: %w", err)
	}

	respMsg, ok := resp.(*client.MapReduceResponse)
	if !ok {
		return fmt.Errorf("received unexpected response type: %T", resp)
	}

	fmt.Printf("MapReduce job successfully submitted. Job ID: %s\n", respMsg.JobId)
	return nil
}

// Add new method to Context struct
func (ctx *Context) UploadPlugin(filepath string, pluginId string) error {
	// Read plugin file content
	pluginCode, err := os.ReadFile(filepath)
	if err != nil {
		return fmt.Errorf("failed to read plugin file %s: %w", filepath, err)
	}

	if len(pluginCode) == 0 {
		return fmt.Errorf("plugin file %s is empty", filepath)
	}
	req := &client.UploadPluginRequest{
		PluginId:   pluginId,
		PluginCode: pluginCode,
	}

	fmt.Printf("Uploading plugin '%s' from %s (%d bytes)...\n", pluginId, filepath, len(pluginCode))

	respRaw, err := ctx.cli.SendRequest(req)
	if err != nil {
		return fmt.Errorf("failed to send UploadPlugin request: %w", err)
	}

	resp, ok := respRaw.(*client.UploadPluginResponse)
	if !ok {
		return fmt.Errorf("received unexpected response type: %T", respRaw)
	}

	if !resp.Success {
		return fmt.Errorf("controller failed to store plugin '%s': %s", pluginId, resp.Message)
	}

	fmt.Printf("Plugin '%s' uploaded successfully.\n", pluginId)
	return nil
}
