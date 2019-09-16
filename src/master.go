package main

import (
	"crazys3/src/pkg"
	"errors"
	"github.com/AlecAivazis/survey/v2"
	"net/rpc"
	"strconv"
)

// master is responsible for data collecting and distributing
// author: jiateng.liang@nyu.edu

func main() {
	pkg.BootStrap()
	clients := make([]*rpc.Client, len(pkg.GConfig.Workers))
	err := rpcConnect(clients)
	if err != nil {
		pkg.GLogger.Error("Exception in establishing rpc connection, reason: %v", err)
		return
	}
	task := ""
	err = survey.AskOne(&survey.Select{
		Message: "Select a task to execute:",
		Options: []string{"S3 Bucket Migration", "blue", "green"},
	}, &task)
	if err != nil {
		pkg.GLogger.Error("Exception in selecting tasks, reason: %v", err)
		return
	}
	if task == "S3 Bucket Migration" {
		err = RunMigrationJob("k8s-test-stghouzz-state-store", "jiateng-test", "", clients, "staging")
		if err != nil {
			pkg.GLogger.Error("Exception in migrating bucket %v, reason: %v", "k8s-test-stghouzz-state-store", err)
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

// Data migration job. Copy the whole bucket to the destination with acls preserved
func RunMigrationJob(from string, to string, prefix string, clients []*rpc.Client, profile string) error {
	// create s3 manager
	manager, err := pkg.NewS3Manager("us-west-2", profile)
	if err != nil {
		return err
	}
	region1, err := manager.GetBucketRegion(from)
	region2, err := manager.GetBucketRegion(to)
	if err != nil {
		return err
	}
	if region1 != region2 {
		return errors.New("bucket1's region " + region1 + " != bucket2's region " + region2)
	}
	if region1 != "us-west-2" {
		manager, err = pkg.NewS3Manager(region1, profile)
		if err != nil {
			return err
		}
	}
	key, pwd := manager.GetCredential()
	s3InfoReq := &pkg.S3InfoRequest{
		Profile:   profile,
		Region:    region1,
		AwsKey:    key,
		AwsSecret: pwd,
	}
	for _, cli := range clients {
		err := cli.Call("RpcHandler.HandleS3Info", s3InfoReq, nil)
		if err != nil {
			return err
		}
		cli.Call("RpcHandler.StartMigraJob", "", nil)
	}
	pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data migration job started <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	buffers := make([][]*pkg.MigrationRequest, len(pkg.GConfig.Workers))
	err = manager.HandleFiles(from, prefix, func(file *pkg.S3File) error {
		idx := file.Id % int64(len(pkg.GConfig.Workers))
		req := &pkg.MigrationRequest{
			File:         file,
			SourceBucket: from,
			DestBucket:   to,
			DestFileName: file.Name,
		}
		buffers[idx] = append(buffers[idx], req)
		if len(buffers[idx]) >= 100 {
			clients[idx].Call("RpcHandler.HandleMigration", buffers[idx], nil)
			pkg.GLogger.Debug("[Migration Job] sent %v migration requests to %v", len(buffers[idx]), pkg.GConfig.Workers[idx])
			buffers[idx] = nil
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := 0; i < len(pkg.GConfig.Workers); i++ {
		buffers[i] = append(buffers[i], &pkg.MigrationRequest{Finished: true})
		clients[i].Call("RpcHandler.HandleMigration", buffers[i], nil)
		pkg.GLogger.Debug("[Migration Job] sent %v migration requests to %v", len(buffers[i]), pkg.GConfig.Workers[i])
		buffers[i] = nil
	}
	pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data migration job finished <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	return nil
}
