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
package pe

import (
	"github.com/astaxie/beego/logs"
	"github.com/garychenc/gomq/mqd/src/client/mq"
	"strconv"
	"testing"
	"time"
)

func TestExampleProducer(t *testing.T) {
	logConfig := &mq.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterFile
	logConfig.AdapterParameters = "{\"filename\": \"./producer.log\", \"maxLines\": -1, \"maxsize\": 1073741824, \"daily\": true, \"maxDays\": 30, \"rotate\": true, \"perm\": \"0777\"}"
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
		go func() {
			producer, err := mqClient.CreateProducer("TestQueue-1")
			if err != nil {
				t.Error("Failed.")
				return
			}

			for {
				msg := mq.NewBytesMessageWithoutId([]byte("TEST-MESSAGE-1110-0000001-110001-1010010111000"))
				msgId, err := producer.Produce(msg)
				if err != nil {
					logger.Info("[" + strconv.FormatInt(mq.CurrentTimeMillis(), 10) + "] Produce Message Error. Error is " + err.Error())
				} else {
					logger.Info("[" + strconv.FormatInt(mq.CurrentTimeMillis(), 10) + "] Message sent, MSG ID : " + strconv.FormatInt(msgId, 10))
				}

				time.Sleep(time.Duration(1000) * time.Millisecond)
			}

			producer.Close()
		}()
	}

	time.Sleep(time.Duration(172800000) * time.Millisecond)
}
