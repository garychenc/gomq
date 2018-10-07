/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main

import (
	"common/config"
	"fmt"
	"github.com/astaxie/beego/logs"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"server/launch"
	"server/queue"
	"strconv"
	"strings"
)

type MainConfig struct {
	LogLevel                     string `yaml:"LogLevel"`
	LogMaxSizePerFile            string `yaml:"LogMaxSizePerFile"`
	LogMaxRetainDay              int    `yaml:"LogMaxRetainDay"`
	ListeningAddress             string `yaml:"ListeningAddress"`
	FlushFileCacheEveryOperation bool   `yaml:"FlushFileCacheEveryOperation"`

	Queues []QueueConfig `yaml:"Queues"`
}

type QueueConfig struct {
	QueueName            string `yaml:"QueueName"`
	MessageStoreTimeInMs int64  `yaml:"MessageStoreTimeInMs"`
}

const (
	DefaultStoreFileMinSizeInBytes = 512 * 1024 * 1024
	MainLogFilePath = "." + string(filepath.Separator) + "logs" + string(filepath.Separator) + "main.log"
	MqLogFilePath   = "." + string(filepath.Separator) + "logs" + string(filepath.Separator) + "mq.log"
	MqServerConfigFilePath = "." + string(filepath.Separator) + "config" + string(filepath.Separator) + "server.yml"

	LogLevelDebug = "DEBUG"
	LogLevelInfo  = "INFO"
	LogLevelWarn  = "WARN"
	LogLevelError = "ERROR"

	FileSizeUnitG = "G"
	FileSizeUnitM = "M"
)

func main() {
	mainLogConfig := &config.LogConfig{}
	mainLogConfig.AsyncLogChanLength = 3000000
	mainLogConfig.Level = logs.LevelInformational
	mainLogConfig.AdapterName = logs.AdapterFile
	mainLogConfig.AdapterParameters = "{\"filename\": \"" + MainLogFilePath + "\", \"maxLines\": -1, \"maxsize\": 1073741824, \"daily\": true, \"maxDays\": 30, \"rotate\": true, \"perm\": \"0777\"}"
	mainLogConfig.IsAsync = false

	mainLogger := logs.NewLogger(mainLogConfig.AsyncLogChanLength)
	mainLogger.SetLevel(mainLogConfig.Level)
	err := mainLogger.SetLogger(mainLogConfig.AdapterName, mainLogConfig.AdapterParameters)
	mainLogger.SetLogger(logs.AdapterConsole, "{}")
	if err != nil {
		fmt.Println("Set main logger failed. Server is stopped. Error is " + err.Error())
		return
	}

	if mainLogConfig.IsAsync {
		mainLogger.Async(mainLogConfig.AsyncLogChanLength)
	}

	configContentBytes, err := ioutil.ReadFile(MqServerConfigFilePath)
	if err != nil {
		mainLogger.Error("Read MQ server config file failed. Server is stopped. Config file path : %+v, Error is %+v.", MqServerConfigFilePath, err.Error())
		return
	}

	mainConfig := &MainConfig{}
	err = yaml.Unmarshal(configContentBytes, &mainConfig)
	if err != nil {
		mainLogger.Error("Unmarshal MQ server config to object failed. Server is stopped. Config file path : %+v, Error is %+v.", MqServerConfigFilePath, err.Error())
		return
	}

	logLevel := mainConfig.LogLevel
	logMaxSizePerFile := mainConfig.LogMaxSizePerFile
	logMaxRetainDay := mainConfig.LogMaxRetainDay
	listeningAddress := mainConfig.ListeningAddress
	flushFileCacheEveryOperation := mainConfig.FlushFileCacheEveryOperation
	queues := mainConfig.Queues

	var maxLogFileSize int64
	if strings.HasSuffix(logMaxSizePerFile, FileSizeUnitG) {
		logMaxSizePerFile = strings.TrimSuffix(logMaxSizePerFile, FileSizeUnitG)
		maxLogFileSize, err = strconv.ParseInt(logMaxSizePerFile, 10, 64)
		if err != nil {
			mainLogger.Error("Parse config item [LogMaxSizePerFile] to int64 failed. Current value : %+v. Error is %+v.", logMaxSizePerFile, err.Error())
			return
		}

		maxLogFileSize = maxLogFileSize * 1024 * 1024 * 1024
	} else if strings.HasSuffix(logMaxSizePerFile, FileSizeUnitM) {
		logMaxSizePerFile = strings.TrimSuffix(logMaxSizePerFile, FileSizeUnitM)
		maxLogFileSize, err = strconv.ParseInt(logMaxSizePerFile, 10, 64)
		if err != nil {
			mainLogger.Error("Parse config item [LogMaxSizePerFile] to int64 failed. Current value : %+v. Error is %+v.", logMaxSizePerFile, err.Error())
			return
		}

		maxLogFileSize = maxLogFileSize * 1024 * 1024
	} else {
		mainLogger.Error("Config Item [LogMaxSizePerFile] in server config file error. Must be ends with G or M. Current value : %+v.", logMaxSizePerFile)
		return
	}

	deployedQueueNumber := len(queues)
	mqServerConfig := launch.NewNetworkMqServerConfig(deployedQueueNumber)

	mqServerConfig.LogConfig.AsyncLogChanLength = 3000000
	mqServerConfig.LogConfig.Level = logs.LevelInformational
	mqServerConfig.LogConfig.AdapterName = logs.AdapterFile
	mqServerConfig.LogConfig.AdapterParameters = "{\"filename\": \"" + MqLogFilePath + "\", \"maxLines\": -1, \"maxsize\": " + strconv.FormatInt(maxLogFileSize, 10) + ", \"daily\": true, \"maxDays\": " + strconv.Itoa(logMaxRetainDay) + ", \"rotate\": true, \"perm\": \"0777\"}"
	mqServerConfig.LogConfig.IsAsync = false

	switch logLevel {
		case LogLevelDebug: {
			mqServerConfig.LogConfig.Level = logs.LevelDebug
		}

		case LogLevelInfo: {
			mqServerConfig.LogConfig.Level = logs.LevelInformational
		}

		case LogLevelWarn: {
			mqServerConfig.LogConfig.Level = logs.LevelWarning
		}

		case LogLevelError: {
			mqServerConfig.LogConfig.Level = logs.LevelError
		}

		default: {
			mainLogger.Error("Config Item [LogLevel] in server config file error. Supported Config Values : DEBUG, INFO, WARN, ERROR. Current value : %+v.", logLevel)
			return
		}
	}

	mqServerConfig.NetworkServerConfig.LogConfig.AsyncLogChanLength = mqServerConfig.LogConfig.AsyncLogChanLength
	mqServerConfig.NetworkServerConfig.LogConfig.Level = mqServerConfig.LogConfig.Level
	mqServerConfig.NetworkServerConfig.LogConfig.AdapterName = mqServerConfig.LogConfig.AdapterName
	mqServerConfig.NetworkServerConfig.LogConfig.AdapterParameters = mqServerConfig.LogConfig.AdapterParameters
	mqServerConfig.NetworkServerConfig.LogConfig.IsAsync = mqServerConfig.LogConfig.IsAsync

	mqServerConfig.NetworkServerConfig.ServerListeningAddress = listeningAddress
	mqServerConfig.IsSyncFileInEveryOpt = flushFileCacheEveryOperation

	for i, _ := range mqServerConfig.DeployedQueueConfigs {
		qc := queues[i]
		queueConfig := queue.NewFileQueueConfig()
		queueConfig.Name = qc.QueueName
		queueConfig.QueueStorePath = queue.QueueRelativeFilesStoreDir + qc.QueueName
		queueConfig.MessageStoreTimeInMs = qc.MessageStoreTimeInMs
		queueConfig.StoreFileMinSizeInBytes = DefaultStoreFileMinSizeInBytes

		mqServerConfig.DeployedQueueConfigs[i] = queueConfig
	}

	mqServer := launch.NewNetworkMqServer(mqServerConfig)
	if err := mqServer.Start(); err != nil {
		mainLogger.Error("Start MQ server failed. Error is %+v. Server is stopped.", err.Error())
		mqServer.Stop()
		return
	}

	stopChannel := make(chan os.Signal)
	signal.Notify(stopChannel)
	mainLogger.Info("-- MQ SERVER STARTED !! --")

	stopSignal := <- stopChannel
	mainLogger.Info("-- MQ SERVER RECEIVED STOP SIGNAL [%+v], IS GOING TO STOP !! --", stopSignal)

	mqServer.Stop()

	mainLogger.Info("-- MQ SERVER STOPPED !! --")
}




































