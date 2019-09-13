package main

import (
	"crazys3/src/pkg"
	"net"
	"net/rpc"
	"strconv"
)

func main() {
	pkg.BootStrap()
	err := rpcServe()
	if err != nil {
		pkg.GLogger.Error("Exception in starting rpc server, reason: %v", err)
		return
	}
}

/******* rpc functions ********/

type Listener int

func (l *Listener) HandleMigration(reqs []*pkg.MigrationRequest, ack *bool) error {
	pkg.GLogger.Debug("[HandleMigration] received %v migration requests", len(reqs))

	return nil
}

// blocking function
func rpcServe() error {
	addr, err := net.ResolveTCPAddr("tcp", pkg.GConfig.Master+":"+strconv.Itoa(pkg.GConfig.WorkerPort))
	if err != nil {
		return err
	}
	inbound, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	listener := new(Listener)
	rpc.Register(listener)
	pkg.GLogger.Info("rpc server is started at port %v", pkg.GConfig.WorkerPort)
	rpc.Accept(inbound)
	return nil
}
