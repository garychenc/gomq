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
package mq

import (
	"common/protocol"
	"common/utils"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestMqBaseFunctions(t *testing.T) {
	mqClient := NewNetworkMqClient("192.168.0.110:16800", -1)

	producer1, err := mqClient.CreateProducer("TestQueue-1")
	if err != nil {
		t.Error("Failed.")
		return
	}

	consumer1, err := mqClient.CreateConsumer("Test-Consumer-1", "TestQueue-1")
	if err != nil {
		t.Error("Failed.")
		return
	}

	producer2, err := mqClient.CreateProducer("TestQueue-1")
	if err != nil {
		t.Error("Failed.")
		return
	}

	consumer2, err := mqClient.CreateConsumer("Test-Consumer-2", "TestQueue-1")
	if err != nil {
		t.Error("Failed.")
		return
	}

	consumer3, err := mqClient.CreateConsumer("Test-Consumer-3", "TestQueue-1")
	if err != nil {
		t.Error("Failed.")
		return
	}

	consumer4, err := mqClient.CreateConsumer("Test-Consumer-3", "TestQueue-1")
	if err != nil {
		t.Error("Failed.")
		return
	}

	consumer5, err := mqClient.CreateConsumer("Test-Consumer-5", "TestQueue-1")
	if err != nil {
		t.Error("Failed.")
		return
	}

	for i := 0; i < 100; i++ {
		msg := protocol.NewBytesMessageWithoutId([]byte("test-msg-1-" + strconv.Itoa(i)))
		msgId, err := producer1.Produce(msg)
		if err != nil {
			t.Error("Failed.")
			return
		}

		fmt.Println("Message sent, MSG ID : " + strconv.FormatInt(msgId, 10))
	}

	for i := 0; i < 100; i++ {
		msg := protocol.NewBytesMessageWithoutId([]byte("test-msg-2-" + strconv.Itoa(i)))
		msgId, err := producer2.Produce(msg)
		if err != nil {
			t.Error("Failed.")
			return
		}

		fmt.Println("Message sent, MSG ID : " + strconv.FormatInt(msgId, 10))
	}

	for i := 0; i < 200; i++ {
		msg, err := consumer1.Consume()
		if err != nil {
			t.Error("Failed.")
			return
		}

		err = consumer1.Commit()
		if err != nil {
			t.Error("Failed.")
			return
		}

		fmt.Println("1 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	for i := 0; i < 200; i++ {
		msg, err := consumer2.Consume()
		if err != nil {
			t.Error("Failed.")
			return
		}

		err = consumer2.Commit()
		if err != nil {
			t.Error("Failed.")
			return
		}

		fmt.Println("2 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	for i := 0; i < 100; i++ {
		msg, err := consumer3.Consume()
		if err != nil {
			t.Error("Failed.")
			return
		}

		err = consumer3.Commit()
		if err != nil {
			t.Error("Failed.")
			return
		}

		fmt.Println("3 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	for i := 0; i < 100; i++ {
		msg, err := consumer4.Consume()
		if err != nil {
			t.Error("Failed.")
			return
		}

		err = consumer4.Commit()
		if err != nil {
			t.Error("Failed.")
			return
		}

		fmt.Println("3 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	for i := 0; i < 200; i++ {
		msg, err := consumer5.Consume()
		if err != nil {
			t.Error("Failed.")
			return
		}

		fmt.Println("5 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	err = consumer5.Reset()
	if err != nil {
		t.Error("Failed.")
		return
	}

	for i := 0; i < 200; i++ {
		msg, err := consumer5.Consume()
		if err != nil {
			t.Error("Failed.")
			return
		}

		fmt.Println("5 : Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))
	}

	err = consumer5.Commit()
	if err != nil {
		t.Error("Failed.")
		return
	}

	err = consumer5.Reset()
	if err != nil {
		t.Error("Failed.")
		return
	}

	msg, err := consumer1.ConsumeNoWait()
	if err != nil || msg != nil {
		t.Error("Failed.")
		return
	}

	msg, err = consumer1.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
		t.Error("Failed.")
		return
	}

	msg, err = consumer2.ConsumeNoWait()
	if err != nil || msg != nil {
		t.Error("Failed.")
		return
	}

	msg, err = consumer2.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
		t.Error("Failed.")
		return
	}

	msg, err = consumer3.ConsumeNoWait()
	if err != nil || msg != nil {
		t.Error("Failed.")
		return
	}

	msg, err = consumer3.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
		t.Error("Failed.")
		return
	}

	msg, err = consumer4.ConsumeNoWait()
	if err != nil || msg != nil {
		t.Error("Failed.")
		return
	}

	msg, err = consumer4.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
		t.Error("Failed.")
		return
	}

	msg, err = consumer5.ConsumeNoWait()
	if err != nil || msg != nil {
		t.Error("Failed.")
		return
	}

	msg, err = consumer5.ConsumeWithTimeout(500)
	if err != nil || msg != nil {
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
}

func TestRealSituation(t *testing.T) {
	mqClient := NewNetworkMqClient("192.168.0.110:16800", -1)

	for j := 0; j < 1; j++ {
		go func(index int) {
			consumer, err := mqClient.CreateConsumer("Test-Consumer-1", "TestQueue-2")
			if err != nil {
				t.Error("Failed.")
				return
			}

			for {
				msg, err := consumer.Consume()
				if err != nil {
					fmt.Println("[" + strconv.FormatInt(utils.CurrentTimeMillis(), 10) + "] Consume message error. Error is " + err.Error())
					time.Sleep(time.Duration(6000) * time.Millisecond)
				} else {
					fmt.Println("[" + strconv.FormatInt(utils.CurrentTimeMillis(), 10) + "] Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))

					err = consumer.Commit()
					if err != nil {
						fmt.Println("[" + strconv.FormatInt(utils.CurrentTimeMillis(), 10) + "] Commit message error. Error is " + err.Error())
					}
				}
			}

			consumer.Close()
		}(j)
	}

	time.Sleep(time.Duration(2000) * time.Millisecond)

	for j := 0; j < 1; j++ {
		go func() {
			producer, err := mqClient.CreateProducer("TestQueue-2")
			if err != nil {
				t.Error("Failed.")
				return
			}

			for {
				msg := protocol.NewBytesMessageWithoutId([]byte("TEST-MESSAGE-1"))
				msgId, err := producer.Produce(msg)
				if err != nil {
					fmt.Println("[" + strconv.FormatInt(utils.CurrentTimeMillis(), 10) + "] Produce Message Error. Error is " + err.Error())
				} else {
					fmt.Println("[" + strconv.FormatInt(utils.CurrentTimeMillis(), 10) + "] Message sent, MSG ID : " + strconv.FormatInt(msgId, 10))
				}

				time.Sleep(time.Duration(6000) * time.Millisecond)
			}

			producer.Close()
		}()
	}

	time.Sleep(time.Duration(3600000) * time.Millisecond)
}

func TestRealConsumer(t *testing.T) {
	mqClient := NewNetworkMqClient("192.168.0.110:16800", -1)

	for j := 0; j < 3; j++ {
		go func(index int) {
			consumer, err := mqClient.CreateConsumer("Test-Consumer-1", "TestQueue-3")
			if err != nil {
				t.Error("Failed.")
				return
			}

			for {
				msg, err := consumer.Consume()
				if err != nil {
					fmt.Println("[" + strconv.FormatInt(utils.CurrentTimeMillis(), 10) + "] Consume message error. Error is " + err.Error())
					time.Sleep(time.Duration(6000) * time.Millisecond)
				} else {
					fmt.Println("[" + strconv.FormatInt(utils.CurrentTimeMillis(), 10) + "] Message consumed, MSG ID : " + strconv.FormatInt(msg.MsgId(), 10))

					err = consumer.Commit()
					if err != nil {
						fmt.Println("[" + strconv.FormatInt(utils.CurrentTimeMillis(), 10) + "] Commit message error. Error is " + err.Error())
					}
				}
			}

			consumer.Close()
		}(j)
	}

	time.Sleep(time.Duration(172800000) * time.Millisecond)
}

func TestRealProducer(t *testing.T) {
	mqClient := NewNetworkMqClient("192.168.0.110:16800", -1)

	for j := 0; j < 3; j++ {
		go func() {
			producer, err := mqClient.CreateProducer("TestQueue-3")
			if err != nil {
				t.Error("Failed.")
				return
			}

			for {
				msg := protocol.NewBytesMessageWithoutId([]byte("TEST-MESSAGE-1110-0000001-110001-1010010111000"))
				msgId, err := producer.Produce(msg)
				if err != nil {
					fmt.Println("[" + strconv.FormatInt(utils.CurrentTimeMillis(), 10) + "] Produce Message Error. Error is " + err.Error())
				} else {
					fmt.Println("[" + strconv.FormatInt(utils.CurrentTimeMillis(), 10) + "] Message sent, MSG ID : " + strconv.FormatInt(msgId, 10))
				}

				time.Sleep(time.Duration(1000) * time.Millisecond)
			}

			producer.Close()
		}()
	}

	time.Sleep(time.Duration(172800000) * time.Millisecond)
}









































