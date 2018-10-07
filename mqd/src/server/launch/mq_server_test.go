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
package launch

import (
	"client/mq"
	"common/protocol"
	"fmt"
	"github.com/astaxie/beego/logs"
	"server/queue"
	"strconv"
	"sync"
	"testing"
)

func TestMqBaseFunctions(t *testing.T) {
	mqServerConfig := NewNetworkMqServerConfig(3)

	mqServerConfig.LogConfig.AsyncLogChanLength = 3000000
	mqServerConfig.LogConfig.Level = logs.LevelInformational
	mqServerConfig.LogConfig.AdapterName = logs.AdapterConsole
	mqServerConfig.LogConfig.AdapterParameters = "{}"
	mqServerConfig.LogConfig.IsAsync = true

	mqServerConfig.NetworkServerConfig.LogConfig.AsyncLogChanLength = 3000000
	mqServerConfig.NetworkServerConfig.LogConfig.Level = logs.LevelInformational
	mqServerConfig.NetworkServerConfig.LogConfig.AdapterName = logs.AdapterConsole
	mqServerConfig.NetworkServerConfig.LogConfig.AdapterParameters = "{}"
	mqServerConfig.NetworkServerConfig.LogConfig.IsAsync = true

	mqServerConfig.NetworkServerConfig.ServerListeningAddress = "127.0.0.1:16801"
	mqServerConfig.IsSyncFileInEveryOpt = false

	for i, _ := range mqServerConfig.DeployedQueueConfigs {
		queueConfig := queue.NewFileQueueConfig()
		queueConfig.Name = "mq-test-" + strconv.Itoa(i)
		queueConfig.QueueStorePath = queue.QueueRelativeFilesStoreDir + "mq-test-" + strconv.Itoa(i)
		queueConfig.MessageStoreTimeInMs = 24 * 3600 * 1000
		queueConfig.StoreFileMinSizeInBytes = 1024 * 1024 * 1024

		mqServerConfig.DeployedQueueConfigs[i] = queueConfig
	}

	mqServer := NewNetworkMqServer(mqServerConfig)
	if err := mqServer.Start(); err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	mqClient := mq.NewNetworkMqClient("127.0.0.1:16801", -1)

	producer1, err := mqClient.CreateProducer("mq-test-1")
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	consumer1, err := mqClient.CreateConsumer("test-consumer-1", "mq-test-1")
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	producer2, err := mqClient.CreateProducer("mq-test-1")
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	consumer2, err := mqClient.CreateConsumer("test-consumer-2", "mq-test-1")
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	consumer3, err := mqClient.CreateConsumer("test-consumer-3", "mq-test-1")
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	consumer4, err := mqClient.CreateConsumer("test-consumer-3", "mq-test-1")
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	consumer5, err := mqClient.CreateConsumer("test-consumer-5", "mq-test-1")
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	for i := 0; i < 100; i++ {
		msg := protocol.NewBytesMessageWithoutId([]byte("test-msg-1-" + strconv.Itoa(i)))
		msgId, err := producer1.Produce(msg)
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		fmt.Println("Message sent, MSG ID : " + strconv.FormatInt(msgId, 10))
	}

	for i := 0; i < 100; i++ {
		msg := protocol.NewBytesMessageWithoutId([]byte("test-msg-2-" + strconv.Itoa(i)))
		msgId, err := producer2.Produce(msg)
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		fmt.Println("Message sent, MSG ID : " + strconv.FormatInt(msgId, 10))
	}

	for i := 0; i < 200; i++ {
		msg, err := consumer1.Consume()
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		err = consumer1.Commit()
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		fmt.Println("1 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	for i := 0; i < 200; i++ {
		msg, err := consumer2.Consume()
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		err = consumer2.Commit()
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		fmt.Println("2 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	for i := 0; i < 100; i++ {
		msg, err := consumer3.Consume()
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		err = consumer3.Commit()
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		fmt.Println("3 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	for i := 0; i < 100; i++ {
		msg, err := consumer4.Consume()
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		err = consumer4.Commit()
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		fmt.Println("3 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	for i := 0; i < 200; i++ {
		msg, err := consumer5.Consume()
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		fmt.Println("5 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	err = consumer5.Reset()
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	for i := 0; i < 200; i++ {
		msg, err := consumer5.Consume()
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		fmt.Println("5 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	err = consumer5.Commit()
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	err = consumer5.Reset()
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err := consumer1.ConsumeNoWait()
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer1.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer2.ConsumeNoWait()
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer2.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer3.ConsumeNoWait()
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer3.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer4.ConsumeNoWait()
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer4.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer5.ConsumeNoWait()
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer5.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	producer2.Close()
	producer1.Close()
	consumer5.Close()
	consumer4.Close()
	consumer3.Close()
	consumer2.Close()
	consumer1.Close()
	mqServer.Stop()
}

func TestMultiRoutineMqFunctions(t *testing.T) {
	mqServerConfig := NewNetworkMqServerConfig(3)

	mqServerConfig.LogConfig.AsyncLogChanLength = 3000000
	mqServerConfig.LogConfig.Level = logs.LevelInformational
	mqServerConfig.LogConfig.AdapterName = logs.AdapterConsole
	mqServerConfig.LogConfig.AdapterParameters = "{}"
	mqServerConfig.LogConfig.IsAsync = true

	mqServerConfig.NetworkServerConfig.LogConfig.AsyncLogChanLength = 3000000
	mqServerConfig.NetworkServerConfig.LogConfig.Level = logs.LevelInformational
	mqServerConfig.NetworkServerConfig.LogConfig.AdapterName = logs.AdapterConsole
	mqServerConfig.NetworkServerConfig.LogConfig.AdapterParameters = "{}"
	mqServerConfig.NetworkServerConfig.LogConfig.IsAsync = true

	mqServerConfig.NetworkServerConfig.ServerListeningAddress = "127.0.0.1:16802"
	mqServerConfig.IsSyncFileInEveryOpt = false

	for i, _ := range mqServerConfig.DeployedQueueConfigs {
		queueConfig := queue.NewFileQueueConfig()
		queueConfig.Name = "mq-test-2-" + strconv.Itoa(i)
		queueConfig.QueueStorePath = queue.QueueRelativeFilesStoreDir + "mq-test-2-" + strconv.Itoa(i)
		queueConfig.MessageStoreTimeInMs = 24 * 3600 * 1000
		queueConfig.StoreFileMinSizeInBytes = 1024 * 1024 * 1024

		mqServerConfig.DeployedQueueConfigs[i] = queueConfig
	}

	mqServer := NewNetworkMqServer(mqServerConfig)
	if err := mqServer.Start(); err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	mqClient := mq.NewNetworkMqClient("127.0.0.1:16802", -1)

	producer, err := mqClient.CreateProducer("mq-test-2-2")
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	consumer, err := mqClient.CreateConsumer("test-consumer-1", "mq-test-2-2")
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(1000)

	for j := 0; j < 10; j++ {
		go func() {
			for {
				_, err := consumer.Consume()
				if err != nil {
					mqServer.Stop()
					t.Error("Failed.")
					return
				}

				err = consumer.Commit()
				if err != nil {
					mqServer.Stop()
					t.Error("Failed.")
					return
				}
				wg.Done()
			}
		}()
	}

	for j := 0; j < 10; j++ {
		go func() {
			for i := 0; i < 100; i++ {
				msg := protocol.NewBytesMessageWithoutId([]byte("test-msg-1-" + strconv.Itoa(i)))
				_, err := producer.Produce(msg)
				if err != nil {
					mqServer.Stop()
					t.Error("Failed.")
					return
				}
			}
		}()
	}

	wg.Wait()
	fmt.Println("All message received.")

	msg, err := consumer.ConsumeNoWait()
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	producer.Close()
	consumer.Close()

	wg1 := &sync.WaitGroup{}
	wg1.Add(10000)

	consumers := make([]mq.IConsumer, 10)
	for j := 0; j < 10; j++ {
		consumer1, err := mqClient.CreateConsumer("test-consumer-2", "mq-test-2-0")
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		consumers[j] = consumer1
		go func(c mq.IConsumer) {
			for {
				_, err := c.Consume()
				if err != nil {
					mqServer.Stop()
					t.Error("Failed.")
					return
				}

				err = c.Commit()
				if err != nil {
					mqServer.Stop()
					t.Error("Failed.")
					return
				}

				wg1.Done()
			}
		}(consumer1)
	}

	producers := make([]mq.IProducer, 10)
	for j := 0; j < 10; j++ {
		producer1, err := mqClient.CreateProducer("mq-test-2-0")
		if err != nil {
			mqServer.Stop()
			t.Error("Failed.")
			return
		}

		producers[j] =  producer1
		go func(p mq.IProducer) {
			for i := 0; i < 1000; i++ {
				msg := protocol.NewBytesMessageWithoutId([]byte("test-msg-1-" + strconv.Itoa(i)))
				_, err := p.Produce(msg)
				if err != nil {
					mqServer.Stop()
					t.Error("Failed.")
					return
				}

			}
		}(producer1)
	}

	wg1.Wait()
	fmt.Println("All message received.")

	consumer2, err := mqClient.CreateConsumer("test-consumer-2", "mq-test-2-0")
	if err != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer2.ConsumeNoWait()
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	msg, err = consumer2.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
		mqServer.Stop()
		t.Error("Failed.")
		return
	}

	for _, p := range producers {
		p.Close()
	}

	for _, c := range consumers {
		c.Close()
	}

	consumer2.Close()
	mqServer.Stop()
}
















































