package main

import (
	"crazys3/src/pkg"
)

func main() {
	pkg.BootStrap()
	manager, err := pkg.NewS3Manager("us-east-1", "staging")
	if err != nil {
		pkg.GLogger.Error("Exception in creating S3Manager, reason: %v", err)
		return
	}
	err = manager.CopyFile("k8s-test-stghouzz-state-store", "k8s-test.stghouzz.com/config", "jiateng-test", "config")
	if err != nil{
		pkg.GLogger.Error("Exception in copying file, reason: %v", err)
		return
	}
}
