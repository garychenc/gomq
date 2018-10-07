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
	"github.com/astaxie/beego/logs"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"testing"
)

func TestMainFunctions(t *testing.T) {
	//currDir, err := utils.GetCurrentPath()
	//if err != nil {
	//	fmt.Println("Get current running directory failed. Server is stopped.")
	//	return
	//}

	currDir := "/Users/gary/Documents/workspace-go/go-mq/src/server/main"

	mainLogConfig := &config.LogConfig{}
	mainLogConfig.AsyncLogChanLength = 3000000
	mainLogConfig.Level = logs.LevelInformational
	mainLogConfig.AdapterName = logs.AdapterFile
	mainLogConfig.AdapterParameters = "{\"filename\": \"" + currDir + "/logs/main.log\", \"maxLines\": -1, \"maxsize\": 1073741824, \"daily\": true, \"maxDays\": 30, \"rotate\": true, \"perm\": \"0777\"}"
	mainLogConfig.IsAsync = false

	mainLogger := logs.NewLogger(mainLogConfig.AsyncLogChanLength)
	mainLogger.SetLevel(mainLogConfig.Level)
	mainLogger.SetLogger(mainLogConfig.AdapterName, mainLogConfig.AdapterParameters)
	mainLogger.SetLogger(logs.AdapterConsole, "{}")
	if mainLogConfig.IsAsync {
		mainLogger.Async(mainLogConfig.AsyncLogChanLength)
	}

	configContentBytes, err := ioutil.ReadFile(currDir + "/config/server.yml")
	if err != nil {
		mainLogger.Error("Read MQ server config file failed. Server is stopped. Config file path : %+v, Error is %+v.", currDir + "/config/server.yml", err.Error())
		return
	}

	mainConfig := &MainConfig{}
	err = yaml.Unmarshal(configContentBytes, &mainConfig)
	if err != nil {
		mainLogger.Error("Unmarshal MQ server config to object failed. Server is stopped. Config file path : %+v, Error is %+v.", currDir + "/config/server.yml", err.Error())
		return
	}

	mainLogger.Info("LogLevel: " + mainConfig.LogLevel)
	mainLogger.Info("ListeningAddress: " + mainConfig.ListeningAddress)
}