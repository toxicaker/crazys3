package main

import (
	"crazys3/src/pkg"
	"github.com/AlecAivazis/survey/v2"
	"net/rpc"
	"strconv"
	"time"
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
		Options: []string{"S3 Bucket Migration", "S3 Bucket Restoration", "S3 Bucket Recovery(Glacier to Standard)"},
	}, &task)
	if err != nil {
		pkg.GLogger.Error("Exception in selecting tasks, reason: %v", err)
		return
	}
	var startTime time.Time
	// task selecting
	switch task {
	case "S3 Bucket Migration":
		var qs = []*survey.Question{
			{
				Name:     "source",
				Prompt:   &survey.Input{Message: "Source Bucket Name"},
				Validate: survey.Required,
			},
			{
				Name:     "target",
				Prompt:   &survey.Input{Message: "Target Bucket Name"},
				Validate: survey.Required,
			},
			{
				Name:   "prefix",
				Prompt: &survey.Input{Message: "Prefix(leave blank if no prefix)"},
			},
			{
				Name:     "profile",
				Prompt:   &survey.Input{Message: "AWS Profile"},
				Validate: survey.Required,
			},
		}
		answers := struct {
			Source  string
			Target  string
			Profile string
			Prefix  string
		}{}
		err = survey.Ask(qs, &answers)
		if err != nil {
			pkg.GLogger.Error("Exception in configuration, reason: %v", err)
			return
		}
		startTime = time.Now()
		err = RunMigrationJob(answers.Source, answers.Target, answers.Prefix, clients, answers.Profile)
		if err != nil {
			pkg.GLogger.Error("Exception in running task [S3 Bucket Migration], reason: %v", err)
			return
		}
		break
	case "S3 Bucket Restoration":
		var qs = []*survey.Question{
			{
				Name:     "bucket",
				Prompt:   &survey.Input{Message: "Bucket Name"},
				Validate: survey.Required,
			},
			{
				Name:     "days",
				Prompt:   &survey.Input{Message: "How many days you want to restore"},
				Validate: survey.Required,
			},
			{
				Name:   "speed",
				Prompt: &survey.Input{Message: "Speed(Bulk, Standard, Expedited)"},
			},
			{
				Name:   "prefix",
				Prompt: &survey.Input{Message: "Prefix(leave blank if no prefix)"},
			},
			{
				Name:     "profile",
				Prompt:   &survey.Input{Message: "AWS Profile"},
				Validate: survey.Required,
			},
		}
		answers := struct {
			Bucket  string
			Days    int64
			Profile string
			Prefix  string
			Speed   string
		}{}
		err = survey.Ask(qs, &answers)
		if err != nil {
			pkg.GLogger.Error("Exception in configuration, reason: %v", err)
			return
		}
		startTime = time.Now()
		err = RunRestorationJob(answers.Bucket, answers.Prefix, clients, answers.Profile, answers.Days, answers.Speed)
		if err != nil {
			pkg.GLogger.Error("Exception in running task [S3 Bucket Restoration], reason: %v", err)
			return
		}
		break
	case "S3 Bucket Recovery(Glacier to Standard)":
		var qs = []*survey.Question{
			{
				Name:     "bucket",
				Prompt:   &survey.Input{Message: "Bucket Name"},
				Validate: survey.Required,
			},
			{
				Name:   "prefix",
				Prompt: &survey.Input{Message: "Prefix(leave blank if no prefix)"},
			},
			{
				Name:     "profile",
				Prompt:   &survey.Input{Message: "AWS Profile"},
				Validate: survey.Required,
			},
		}
		answers := struct {
			Bucket  string
			Profile string
			Prefix  string
		}{}
		err = survey.Ask(qs, &answers)
		if err != nil {
			pkg.GLogger.Error("Exception in configuration, reason: %v", err)
			return
		}
		startTime = time.Now()
		err = RunRecoveryJob(answers.Bucket, answers.Prefix, clients, answers.Profile)
		if err != nil {
			pkg.GLogger.Error("Exception in running task [S3 Bucket Recovery], reason: %v", err)
			return
		}
		break
	}
	timer := time.NewTimer(10 * time.Second)
	for {
		select {
		case <-timer.C:
			num := 0
			for _, cli := range clients {
				res := false
				cli.Call("RpcHandler.HandleTaskStatus", "", &res)
				if res {
					num++
				}
			}
			if num == len(clients) {
				pkg.GLogger.Info("Task finished. Time spent: %v hours", time.Since(startTime).Hours())
				goto EXIT
			}
			timer.Reset(10 * time.Second)
		}
	}
EXIT:
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
	key, pwd := manager.GetCredential()
	s3InfoReq := &pkg.S3InfoRequest{
		Profile:   profile,
		Region1:   region1,
		Region2:   region2,
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
		if len(buffers[idx]) >= 1000 {
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
	return nil
}

func RunRestorationJob(bucket string, prefix string, clients []*rpc.Client, profile string, days int64, speed string) error {
	// create s3 manager
	manager, err := pkg.NewS3Manager("us-west-2", profile)
	if err != nil {
		return err
	}
	region, err := manager.GetBucketRegion(bucket)
	if err != nil {
		return err
	}
	if region != "us-west-2" {
		manager, err = pkg.NewS3Manager(region, profile)
		if err != nil {
			return err
		}
	}
	key, pwd := manager.GetCredential()
	s3InfoReq := &pkg.S3InfoRequest{
		Profile:   profile,
		Region1:    region,
		AwsKey:    key,
		AwsSecret: pwd,
	}
	for _, cli := range clients {
		err := cli.Call("RpcHandler.HandleS3Info", s3InfoReq, nil)
		if err != nil {
			return err
		}
		cli.Call("RpcHandler.StartRestorationJob", "", nil)
	}
	pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data restoration job started <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	buffers := make([][]*pkg.RestorationRequest, len(pkg.GConfig.Workers))
	err = manager.HandleFiles(bucket, prefix, func(file *pkg.S3File) error {
		idx := file.Id % int64(len(pkg.GConfig.Workers))
		req := &pkg.RestorationRequest{
			File:   file,
			Bucket: bucket,
			Days:   days,
			Speed:  speed,
		}
		buffers[idx] = append(buffers[idx], req)
		if len(buffers[idx]) >= 1000 {
			clients[idx].Call("RpcHandler.HandleRestoration", buffers[idx], nil)
			pkg.GLogger.Debug("[Restoration Job] sent %v restoration requests to %v", len(buffers[idx]), pkg.GConfig.Workers[idx])
			buffers[idx] = nil
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := 0; i < len(pkg.GConfig.Workers); i++ {
		buffers[i] = append(buffers[i], &pkg.RestorationRequest{Finished: true})
		clients[i].Call("RpcHandler.HandleRestoration", buffers[i], nil)
		pkg.GLogger.Debug("[Restoration Job] sent %v restoration requests to %v", len(buffers[i]), pkg.GConfig.Workers[i])
		buffers[i] = nil
	}
	return nil
}

func RunRecoveryJob(bucket string, prefix string, clients []*rpc.Client, profile string) error {
	// create s3 manager
	manager, err := pkg.NewS3Manager("us-west-2", profile)
	if err != nil {
		return err
	}
	region, err := manager.GetBucketRegion(bucket)
	if err != nil {
		return err
	}
	if region != "us-west-2" {
		manager, err = pkg.NewS3Manager(region, profile)
		if err != nil {
			return err
		}
	}
	key, pwd := manager.GetCredential()
	s3InfoReq := &pkg.S3InfoRequest{
		Profile:   profile,
		Region1:    region,
		AwsKey:    key,
		AwsSecret: pwd,
	}
	for _, cli := range clients {
		err := cli.Call("RpcHandler.HandleS3Info", s3InfoReq, nil)
		if err != nil {
			return err
		}
		cli.Call("RpcHandler.StartRecoveryJob", "", nil)
	}
	pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data recovery job started <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	buffers := make([][]*pkg.RecoveryRequest, len(pkg.GConfig.Workers))
	err = manager.HandleFiles(bucket, prefix, func(file *pkg.S3File) error {
		idx := file.Id % int64(len(pkg.GConfig.Workers))
		req := &pkg.RecoveryRequest{
			File:   file,
			Bucket: bucket,
		}
		buffers[idx] = append(buffers[idx], req)
		if len(buffers[idx]) >= 1000 {
			clients[idx].Call("RpcHandler.HandleRestoration", buffers[idx], nil)
			pkg.GLogger.Debug("[Recovery Job] sent %v recovery requests to %v", len(buffers[idx]), pkg.GConfig.Workers[idx])
			buffers[idx] = nil
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := 0; i < len(pkg.GConfig.Workers); i++ {
		buffers[i] = append(buffers[i], &pkg.RecoveryRequest{Finished: true})
		clients[i].Call("RpcHandler.HandleRecovery", buffers[i], nil)
		pkg.GLogger.Debug("[Recovery Job] sent %v restoration requests to %v", len(buffers[i]), pkg.GConfig.Workers[i])
		buffers[i] = nil
	}
	return nil
}
