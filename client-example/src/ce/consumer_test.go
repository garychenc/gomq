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
package ce

import (
	"github.com/astaxie/beego/logs"
	"github.com/garychenc/gomq/mqd/src/client/mq"
	"strconv"
	"testing"
	"time"
)

func TestRealConsumer(t *testing.T) {
	logConfig := &mq.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterFile
	logConfig.AdapterParameters = "{\"filename\": \"./consumer.log\", \"maxLines\": -1, \"maxsize\": 1073741824, \"daily\": true, \"maxDays\": 30, \"rotate\": true, \"perm\": \"0777\"}"
	logConfig.IsAsync = false

	logger := logs.NewLogger(logConfig.AsyncLogChanLength)
	logger.SetLevel(logConfig.Level)
	err := logger.SetLogger(logConfig.AdapterName, logConfig.AdapterParameters)
	logger.SetLogger(logs.AdapterConsole, "{}")
	if err != nil {
		t.Error("Set main logger failed. Server is stopped. Error is " + err.Error())
		return
	}

	if logConfig.IsAsync {
		logger.Async(logConfig.AsyncLogChanLength)
	}

	mqClient := mq.NewNetworkMqClient("127.0.0.1:16800", -1)

	for j := 0; j < 1; j++ {
		go func(index int) {
			consumer, err := mqClient.CreateConsumer("Test-Consumer-1", "TestQueue-1")
			if err != nil {
				t.Error("Failed.")
				return
			}

			for {
				msg, err := consumer.Consume()
				if err != nil {
					logger.Info("[" + strconv.FormatInt(mq.CurrentTimeMillis(), 10) + "] Consume message error. Error is " + err.Error())
					time.Sleep(time.Duration(6000) * time.Millisecond)
				} else {
					logger.Info("[" + strconv.FormatInt(mq.CurrentTimeMillis(), 10) + "] Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))

					err = consumer.Commit()
					if err != nil {
						logger.Info("[" + strconv.FormatInt(mq.CurrentTimeMillis(), 10) + "] Commit message error. Error is " + err.Error())
					}
				}
			}

			consumer.Close()
		}(j)
	}

	time.Sleep(time.Duration(172800000) * time.Millisecond)
}
