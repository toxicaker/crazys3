package main

import (
	"crazys3/src/pkg"
	"net"
	"net/rpc"
	"runtime"
	"strconv"
	"sync"
)

func main() {
	pkg.BootStrap()
	handler := &RpcHandler{
		migraChan:   make(chan *pkg.MigrationRequest, 10000),
		restoreChan: make(chan *pkg.RestorationRequest, 10000),
		recoverChan: make(chan *pkg.RecoveryRequest, 10000),
		mutex:       &sync.Mutex{},
	}
	err := rpcServe(handler)
	if err != nil {
		pkg.GLogger.Error("Exception in starting rpc server, reason: %v", err)
		return
	}
}

/******* rpc functions ********/

type RpcHandler struct {
	mutex           *sync.Mutex
	migraChan       chan *pkg.MigrationRequest
	restoreChan     chan *pkg.RestorationRequest
	recoverChan     chan *pkg.RecoveryRequest
	manager         *pkg.S3Manager
	taskFinished    bool
	finishedThreads int
}

func (handler *RpcHandler) HandleTaskStatus(cmd string, ack *bool) error {
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	if handler.taskFinished {
		*ack = true
	} else {
		*ack = false
	}
	return nil
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

func (handler *RpcHandler) HandleRestoration(reqs []*pkg.RestorationRequest, ack *bool) error {
	pkg.GLogger.Debug("RPC CMD [HandleRestoration] received")
	for _, req := range reqs {
		if req.Finished {
			for i := 0; i < runtime.NumCPU(); i++ {
				handler.restoreChan <- req
			}
		} else {
			handler.restoreChan <- req
		}
	}
	return nil
}

func (handler *RpcHandler) HandleRecovery(reqs []*pkg.RecoveryRequest, ack *bool) error {
	pkg.GLogger.Debug("RPC CMD [HandleRecovery] received")
	for _, req := range reqs {
		if req.Finished {
			for i := 0; i < runtime.NumCPU(); i++ {
				handler.recoverChan <- req
			}
		} else {
			handler.recoverChan <- req
		}
	}
	return nil
}

/******* jobs ********/

func (handler *RpcHandler) StartMigraJob(cmd string, acl *bool) error {
	pkg.GLogger.Debug("RPC CMD [StartMigraJob] received")
	pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data migration job %v threads are ready <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<", runtime.NumCPU())
	handler.mutex.Lock()
	handler.taskFinished = false
	handler.finishedThreads = 0
	handler.mutex.Unlock()
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
			handler.mutex.Lock()
			handler.finishedThreads++
			if handler.finishedThreads == runtime.NumCPU() {
				handler.taskFinished = true
			}
			handler.mutex.Unlock()
			return
		}(i)
	}
	return nil
}

func (handler *RpcHandler) StartRestorationJob(cmd string, acl *bool) error {
	pkg.GLogger.Debug("RPC CMD [StartRestorationJob] received")
	pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data restoration job %v threads are ready <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<", runtime.NumCPU())
	handler.mutex.Lock()
	handler.taskFinished = false
	handler.finishedThreads = 0
	handler.mutex.Unlock()
	for i := 0; i < runtime.NumCPU(); i++ {
		go func(i int) {
			for {
				select {
				case req := <-handler.restoreChan:
					if req.Finished {
						goto EXIT
					}
					pkg.GLogger.Info("[Restoration Job] thread %v is processing %v, id=%v", i, req.Bucket+"/"+req.File.Name, req.File.Id)
					err := handler.manager.RestoreFile(req.Bucket, req.File.Name, req.Days, req.Speed)
					if err != nil {
						pkg.GLogger.Warning("[Restoration Job] Exception in restoring %v/%v, reason: %v", req.Bucket, req.File.Name, err)
					}
				}
			}
		EXIT:
			pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data restoration thread %v closed <<<<<<<<<<<<<<<<<<<<<<<<<<", i)
			handler.mutex.Lock()
			handler.finishedThreads++
			if handler.finishedThreads == runtime.NumCPU() {
				handler.taskFinished = true
			}
			handler.mutex.Unlock()
			return
		}(i)
	}
	return nil
}

func (handler *RpcHandler) StartRecoveryJob(cmd string, acl *bool) error {
	pkg.GLogger.Debug("RPC CMD [StartRecoveryJob] received")
	pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data recovery job %v threads are ready <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<", runtime.NumCPU())
	handler.mutex.Lock()
	handler.taskFinished = false
	handler.finishedThreads = 0
	handler.mutex.Unlock()
	for i := 0; i < runtime.NumCPU(); i++ {
		go func(i int) {
			for {
				select {
				case req := <-handler.recoverChan:
					if req.Finished {
						goto EXIT
					}
					pkg.GLogger.Info("[Recovery Job] thread %v is processing %v, id=%v", i, req.Bucket+"/"+req.File.Name, req.File.Id)
					err := handler.manager.RecoverFile(req.Bucket, req.File.Name)
					if err != nil {
						pkg.GLogger.Warning("[Recovery Job] Exception in recovering %v/%v, reason: %v", req.Bucket, req.File.Name, err)
					}
				}
			}
		EXIT:
			pkg.GLogger.Info(">>>>>>>>>>>>>>>>>>>>>>>>> data recovery thread %v closed <<<<<<<<<<<<<<<<<<<<<<<<<<", i)
			handler.mutex.Lock()
			handler.finishedThreads++
			if handler.finishedThreads == runtime.NumCPU() {
				handler.taskFinished = true
			}
			handler.mutex.Unlock()
			return
		}(i)
	}
	return nil
}

// blocking function
func rpcServe(handler *RpcHandler) error {
	addr, err := net.ResolveTCPAddr("tcp", pkg.GConfig.Worker+":"+strconv.Itoa(pkg.GConfig.WorkerPort))
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
