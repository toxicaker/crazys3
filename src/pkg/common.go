package pkg

import (
	"encoding/json"
	"github.com/mgutz/ansi"
	"log"
	"os"
	"time"
)

/* configuration setting */

type Config struct {
	Master  string   `json:"master"`
	Workers []string `json:"workers"`
}

var GConfig *Config

func InitConfig(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	GConfig = &Config{}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(GConfig)
	if err != nil {
		return err
	}
	return nil
}

/* logging setting */

type Logger struct {
	consoleLogger *log.Logger
	fileLogger    *log.Logger
	debugMode     bool
	red           func(string) string
	yellow        func(string) string
	green         func(string) string
}

func (logger *Logger) Debug(format string, v ...interface{}) {
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	format = "[Debug] " + currentTime + " " + format
	logger.consoleLogger.Printf(logger.green(format), v...)
	if !logger.debugMode {
		logger.fileLogger.Printf(format, v...)
	}
}

func (logger *Logger) Info(format string, v ...interface{}) {
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	format = "[Info] " + currentTime + " " + format
	logger.consoleLogger.Printf(format, v...)
	if !logger.debugMode {
		logger.fileLogger.Printf(format, v...)
	}
}

func (logger *Logger) Warning(format string, v ...interface{}) {
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	format = "[Warning] " + currentTime + " " + format
	logger.consoleLogger.Printf(logger.yellow(format), v...)
	if !logger.debugMode {
		logger.fileLogger.Printf(format, v...)
	}
}

func (logger *Logger) Error(format string, v ...interface{}) {
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	format = "[Error] " + currentTime + " " + format
	logger.consoleLogger.Printf(logger.red(format), v...)
	if !logger.debugMode {
		logger.fileLogger.Printf(format, v...)
	}
}

var GLogger *Logger

func InitLogger(debugMode bool, logPath ...string) error {
	console := log.New(os.Stdout, "", 0)
	GLogger = &Logger{
		consoleLogger: console,
		fileLogger:    nil,
		debugMode:     debugMode,
		yellow:        ansi.ColorFunc("yellow+"),
		green:         ansi.ColorFunc("cyan+"),
		red:           ansi.ColorFunc("red+"),
	}
	if !debugMode {
		lf, err := os.OpenFile(logPath[0], os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
		if err != nil {
			return err
		}
		f := log.New(lf, "", 0)
		GLogger.fileLogger = f
	}
	return nil
}
