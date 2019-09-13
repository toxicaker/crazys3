package main

import (
	"crazys3/src/pkg"
	"net/rpc"
	"strconv"
)

func main() {
	pkg.BootStrap()
	manager, err := pkg.NewS3Manager("us-west-2", "staging")
	if err != nil {
		pkg.GLogger.Error("Exception in creating S3Manager, reason: %v", err)
		return
	}
	buffers := make([][]*pkg.S3File, len(pkg.GConfig.Workers))
	clients := make([]*rpc.Client, len(pkg.GConfig.Workers))
	err = rpcConnect(clients)
	if err != nil {
		pkg.GLogger.Error("Exception in establishing rpc connection, reason: %v", err)
		return
	}
	err = manager.HandleFiles("jiateng-test", "", func(file *pkg.S3File) error {
		idx := file.Id % int64(len(pkg.GConfig.Workers))
		buffers[idx] = append(buffers[idx], file)
		if len(buffers[idx]) >= 100 {
			sendData(buffers[idx], clients[idx], int(idx))
			buffers[idx] = nil
		}
		return nil
	})
	if err != nil {
		pkg.GLogger.Error("Exception in handling bucket %v, reason: %v", "jiateng-test", err)
		return
	}
	for i := 0; i < len(pkg.GConfig.Workers); i++ {
		if len(buffers[i]) > 0 {
			sendData(buffers[i], clients[i], i)
			buffers[i] = nil
		}
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

func sendData(files []*pkg.S3File, client *rpc.Client, idx int) {
	pkg.GLogger.Debug("sending %v s3 files to %v", len(files), pkg.GConfig.Workers[idx])
	client.Call("Listener.ReceiveData", files, nil)
}
