package main

import (
	"fmt"
	"pkg"
)

func main() {
	pkg.InitLogger(false, "../crazys3.log")
	err := pkg.InitConfig("../config.json")
	if err != nil {
		fmt.Println(err)
		return
	}
	pkg.GLogger.Debug("hello")
	pkg.GLogger.Info("hello %s", "world")
	pkg.GLogger.Warning("hello")
	pkg.GLogger.Error("hello")
}
