package main

import (
	"crazys3/src/pkg"
	"net/rpc"
	"strconv"
)

// master is responsible for data collecting and distributing
func main() {
	pkg.BootStrap()
	manager, err := pkg.NewS3Manager("us-west-2", "staging")
	if err != nil {
		pkg.GLogger.Error("Exception in creating S3Manager, reason: %v", err)
		return
	}
	clients := make([]*rpc.Client, len(pkg.GConfig.Workers))
	err = rpcConnect(clients)
	if err != nil {
		pkg.GLogger.Error("Exception in establishing rpc connection, reason: %v", err)
		return
	}
	// Todo: change name here
	err = runMigrateBucket("", "", manager, clients)
	if err != nil {
		pkg.GLogger.Error("Exception in migrating bucket %v, reason: %v", "jiateng-test", err)
	}

	rpcClose(clients)
}

func rpcConnect(clients []*rpc.Client) error {
	for i, addr := range pkg.GConfig.Workers {
		cli, err := rpc.Dial("tcp", addr+":"+strconv.Itoa(pkg.GConfig.WorkerPort))
		if err != nil {
			return err
		}
		clients[i] = cli
		pkg.GLogger.Info("successfully connected with %v:%v", addr, pkg.GConfig.WorkerPort)
	}
	return nil
}

func rpcClose(clients []*rpc.Client) {
	for i, client := range clients {
		client.Close()
		clients[i] = nil
		pkg.GLogger.Info("successfully closed connection %v:%v", pkg.GConfig.Workers[i], pkg.GConfig.WorkerPort)
	}
}

// Data migration job. Copy the whole bucket to the destination with acls preserved
func runMigrateBucket(from string, to string, manager *pkg.S3Manager, clients []*rpc.Client) error {
	buffers := make([][]*pkg.MigrationRequest, len(pkg.GConfig.Workers))
	err := manager.HandleFiles("jiateng-test", "", func(file *pkg.S3File) error {
		idx := file.Id % int64(len(pkg.GConfig.Workers))
		req := &pkg.MigrationRequest{
			Files:        file,
			SourceBucket: from,
			DestBucket:   to,
			DestFileName: file.Name,
		}
		buffers[idx] = append(buffers[idx], req)
		if len(buffers[idx]) >= 100 {
			clients[idx].Call("Listener.HandleMigration", buffers[idx], nil)
			pkg.GLogger.Debug("[runMigrateBucket] sent %v migration requests to %v", len(buffers[idx]), pkg.GConfig.Workers[idx])
			buffers[idx] = nil
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := 0; i < len(pkg.GConfig.Workers); i++ {
		if len(buffers[i]) > 0 {
			clients[i].Call("Listener.HandleMigration", buffers[i], nil)
			pkg.GLogger.Debug("[runMigrateBucket] sent %v migration requests to %v", len(buffers[i]), pkg.GConfig.Workers[i])
			buffers[i] = nil
		}
	}
	return nil
}
