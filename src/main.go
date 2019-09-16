package main

import (
	"crazys3/src/pkg"
)

func main() {
	pkg.BootStrap()
	manager, err := pkg.NewS3Manager("us-west-2", "staging")
	if err != nil {
		pkg.GLogger.Error("Exception in creating S3Manager, reason: %v", err)
		return
	}
	res, err := manager.GetBucketRegion("k8s-test-stghouzz-state-store")
	//err = manager.HandleFiles("k8s-test-stghouzz-state-store", "", func(file *pkg.S3File) error {
	//	pkg.GLogger.Debug("%v", *file)
	//	return nil
	//})
	if err != nil {
		pkg.GLogger.Error("Exception in listing files, reason: %v", err)
		return
	}
	pkg.GLogger.Debug("%v", res)
}
