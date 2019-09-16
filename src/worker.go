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
	handler := &RpcHandler{
		migraChan: make(chan *pkg.MigrationRequest, 10000),
	}
	err := rpcServe(handler)
	if err != nil {
		pkg.GLogger.Error("Exception in starting rpc server, reason: %v", err)
		return
	}
}

/******* rpc functions ********/

type RpcHandler struct {
	migraChan chan *pkg.MigrationRequest
	manager   *pkg.S3Manager
}

func (handler *RpcHandler) HandleS3Info(req *pkg.S3InfoRequest, ack *bool) error {
	pkg.GLogger.Debug("RPC CMD [HandleS3Info] received")
	manager, err := pkg.NewS3ManagerWithKey(req.Region, req.AwsKey, req.AwsSecret)
	if err != nil {
		return err
	}
	handler.manager = manager
	*ack = true
	return nil
}

func (handler *RpcHandler) HandleMigration(reqs []*pkg.MigrationRequest, ack *bool) error {
	pkg.GLogger.Debug("RPC CMD [HandleMigration] received")
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

/******* jobs ********/

func (handler *RpcHandler) StartMigraJob(cmd string, acl *bool) error {
	pkg.GLogger.Debug("RPC CMD [StartMigraJob] received")
	pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data migration job %v threads are ready <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<", runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func(i int) {
			for {
				select {
				case req := <-handler.migraChan:
					if req.Finished {
						goto EXIT
					}
					pkg.GLogger.Info("[Migration Job] thread %v is processing %v, id=%v", i, req.DestBucket+"/"+req.DestFileName, req.File.Id)
					err := handler.manager.CopyFile(req.SourceBucket, req.File.Name, req.DestBucket, req.DestFileName)
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
	return nil
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
