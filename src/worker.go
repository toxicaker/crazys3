package main

import (
	"crazys3/src/pkg"
	"net"
	"net/rpc"
	"runtime"
	"strconv"
)

func main() {
	pkg.BootStrap()
	manager, err := pkg.NewS3Manager("us-west-2", "staging")
	if err != nil {
		pkg.GLogger.Error("Exception in creating S3Manager, reason: %v", err)
		return
	}
	handler := &RpcHandler{
		manager:   manager,
		migraChan: make(chan *pkg.MigrationRequest, 10000),
	}
	startMigrateJobThreads(handler.migraChan, manager)
	err = rpcServe(handler)
	if err != nil {
		pkg.GLogger.Error("Exception in starting rpc server, reason: %v", err)
		return
	}
}

/******* rpc functions ********/

type RpcHandler struct {
	manager   *pkg.S3Manager
	migraChan chan *pkg.MigrationRequest
}

func (handler *RpcHandler) HandleMigration(reqs []*pkg.MigrationRequest, ack *bool) error {
	for _, req := range reqs {
		if req.Finished {
			for i := 0; i < runtime.NumCPU(); i++ {
				handler.migraChan <- req
			}
		} else {
			handler.migraChan <- req
		}
	}
	return nil
}

func startMigrateJobThreads(dataChan chan *pkg.MigrationRequest, manager *pkg.S3Manager) {
	pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data migration job %v threads are ready <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<", runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func(i int) {
			for {
				select {
				case req := <-dataChan:
					if req.Finished {
						goto EXIT
					}
					pkg.GLogger.Info("[Migration Job] thread %v is processing %v, id=%v", i, req.DestBucket+"/"+req.DestFileName, req.File.Id)
					err := manager.CopyFile(req.SourceBucket, req.File.Name, req.DestBucket, req.DestFileName)
					if err != nil {
						pkg.GLogger.Warning("[Migration Job] Exception in copying %v/%v to %v/%v, reason: %v", req.SourceBucket, req.File.Name, req.DestBucket, req.DestFileName, err)
					}
				}
			}
		EXIT:
			pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data migration thread %v closed <<<<<<<<<<<<<<<<<<<<<<<<<<", i)
			return
		}(i)
	}
	pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> all data migration threads are closed <<<<<<<<<<<<<<<<<<<<<<<<<<")
}

// blocking function
func rpcServe(handler *RpcHandler) error {
	addr, err := net.ResolveTCPAddr("tcp", pkg.GConfig.Master+":"+strconv.Itoa(pkg.GConfig.WorkerPort))
	if err != nil {
		return err
	}
	inbound, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	err = rpc.Register(handler)
	if err != nil {
		return err
	}
	pkg.GLogger.Info("rpc server is started at port %v", pkg.GConfig.WorkerPort)
	rpc.Accept(inbound)
	return nil
}
