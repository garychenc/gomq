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
package queue

import (
	"common/config"
	"common/protocol"
	"common/utils"
	"fmt"
	"github.com/astaxie/beego/logs"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestQueueBaseFunction(t *testing.T) {
	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	fileQueue, err := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue", false, 120000, DefaultStoreFileMinSizeInBytes)

	defer func(){
		if fileQueue != nil {
			fileQueue.Close()
		}
	}()

	if err != nil {
		fmt.Println("create file queue failed. error is %+v", err)
		return
	}

	err2 := fileQueue.Delete()
	if err2 != nil {
		fmt.Println("delete file queue failed. error is %+v", err2)
		return
	}

	fileQueue1, err1 := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue", false, 120000, DefaultStoreFileMinSizeInBytes)

	defer func(){
		if fileQueue1 != nil {
			fileQueue1.Close()
		}
	}()

	if err1 != nil {
		fmt.Println("create file queue failed. error is %+v", err1)
		return
	}

	queueProducer, err := NewFileQueueProducer(fileQueue1)
	if err != nil {
		fmt.Println("create file queue producer failed. error is %+v", err)
		return
	}

	seqGen, err := utils.NewSequenceGenerator()
	if err != nil {
		fmt.Println("create sequence generator failed. error is %+v", err)
		return
	}

	msgBody1 := []byte("abcde1234")
	bytesMsg1, err := protocol.NewBytesMessage(msgBody1, seqGen)
	if err != nil {
		fmt.Println("create bytes message failed. error is %+v", err)
		return
	}

	err = queueProducer.Produce(bytesMsg1)
	if err != nil {
		fmt.Println("send bytes message to queue failed. error is %+v", err)
		return
	}

	msgBody2 := []byte("abcde1234567")
	bytesMsg2, err := protocol.NewBytesMessage(msgBody2, seqGen)
	if err != nil {
		fmt.Println("create bytes message 1 failed. error is %+v", err)
		return
	}

	err = queueProducer.Produce(bytesMsg2)
	if err != nil {
		fmt.Println("send bytes message 2 to queue failed. error is %+v", err)
		return
	}

	queueIterator, err := NewFileQueueIterator(fileQueue1)
	if err != nil {
		fmt.Println("create file queue iterator failed. error is %+v", err)
		return
	}

	if !queueIterator.HasNext() {
		fmt.Println("No Next")
		return
	}

	msg1, err1 := queueIterator.Next()
	if err1 != nil {
		fmt.Println("get file queue next message failed. error is %+v", err1)
		return
	}

	payloadObj1 := msg1.Payload()
	if payload1, ok := payloadObj1.([]byte); ok {
		if string(payload1) != "abcde1234" {
			fmt.Println("read error message content from queue")
			return
		}
	} else {
		fmt.Println("read error message type from queue")
		return
	}

	if !queueIterator.HasNext() {
		fmt.Println("No Next")
		return
	}

	msg2, err2 := queueIterator.Next()
	if err2 != nil {
		fmt.Println("get file queue next message failed. error is %+v", err2)
		return
	}

	payloadObj2 := msg2.Payload()
	if payload2, ok := payloadObj2.([]byte); ok {
		if string(payload2) != "abcde1234567" {
			fmt.Println("read error message content from queue")
			return
		}
	} else {
		fmt.Println("read error message type from queue")
		return
	}

	if queueIterator.HasNext() {
		fmt.Println("Has Next")
		return
	}
}

func TestQueueFullAndFileRotate(t *testing.T) {
	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	fileQueue, err := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue1", false, 2000, 3000)

	defer func(){
		if fileQueue != nil {
			fileQueue.Close()
		}
	}()

	if err != nil {
		fmt.Println("create file queue failed. error is %+v", err)
		return
	}

	err2 := fileQueue.Delete()
	if err2 != nil {
		fmt.Println("delete file queue failed. error is %+v", err2)
		return
	}

	fileQueue1, err1 := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue1", false, 2000, 3000)

	defer func(){
		if fileQueue1 != nil {
			fileQueue1.Close()
		}
	}()

	if err1 != nil {
		fmt.Println("create file queue failed. error is %+v", err1)
		return
	}

	queueProducer, err := NewFileQueueProducer(fileQueue1)
	if err != nil {
		fmt.Println("create file queue producer failed. error is %+v", err)
		return
	}

	seqGen, err := utils.NewSequenceGenerator()
	if err != nil {
		fmt.Println("create sequence generator failed. error is %+v", err)
		return
	}

	for i := 0; i < 100; i++ {
		msgBody := []byte("abcde12_" + strconv.Itoa(i))
		bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
		if err != nil {
			fmt.Println("create bytes message failed. error is %+v", err)
			return
		}

		err = queueProducer.Produce(bytesMsg)
		if err != nil {
			fmt.Println("send bytes message to queue failed. error is %+v", err)
			return
		}
	}

	queueIterator1, err := NewFileQueueIterator(fileQueue1)
	if err != nil {
		fmt.Println("create file queue iterator failed. error is %+v", err)
		return
	}

	for i := 0; i < 100; i++ {
		if queueIterator1.HasNext() {
			msg1, err1 := queueIterator1.Next()
			if err1 != nil {
				fmt.Println("get file queue next message failed. error is %+v", err1)
				return
			}

			payloadObj1 := msg1.Payload()
			if payload1, ok := payloadObj1.([]byte); ok {
				if string(payload1) != "abcde12_" + strconv.Itoa(i) {
					fmt.Println("read error message content from queue")
					return
				}
			} else {
				fmt.Println("read error message type from queue")
				return
			}

		} else {
			fmt.Println("No Next")
			return
		}
	}

	time.Sleep(time.Duration(3) * time.Second)

	for i := 100; i < 120; i++ {
		msgBody := []byte("abcde12_" + strconv.Itoa(i))
		bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
		if err != nil {
			fmt.Println("create bytes message failed. error is %+v", err)
			return
		}

		err = queueProducer.Produce(bytesMsg)
		if err != nil {
			fmt.Println("send bytes message to queue failed. error is %+v", err)
			return
		}
	}

	queueIterator2, err := NewFileQueueIterator(fileQueue1)
	if err != nil {
		fmt.Println("create file queue iterator failed. error is %+v", err)
		return
	}


	for i := 22; i < 120; i++ {
		if queueIterator2.HasNext() {
			msg1, err1 := queueIterator2.Next()
			if err1 != nil {
				fmt.Println("get file queue next message failed. error is %+v", err1)
				return
			}

			payloadObj1 := msg1.Payload()
			if payload1, ok := payloadObj1.([]byte); ok {
				if string(payload1) != "abcde12_" + strconv.Itoa(i) {
					fmt.Println("read error message content from queue, content is %+v, i is %+v", string(payload1), i)
					return
				}
			} else {
				fmt.Println("read error message type from queue")
				return
			}

		} else {
			fmt.Println("No Next")
			return
		}
	}

	for i := 120; i < 220; i++ {
		msgBody := []byte("abcde12_" + strconv.Itoa(i))
		bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
		if err != nil {
			fmt.Println("create bytes message failed. error is %+v", err)
			return
		}

		err = queueProducer.Produce(bytesMsg)
		if err != nil {
			fmt.Println("send bytes message to queue failed. error is %+v", err)
			return
		}
	}

	queueIterator3, err := NewFileQueueIterator(fileQueue1)
	if err != nil {
		fmt.Println("create file queue iterator failed. error is %+v", err)
		return
	}


	for i := 101; i < 220; i++ {
		if queueIterator3.HasNext() {
			msg1, err1 := queueIterator3.Next()
			if err1 != nil {
				fmt.Println("get file queue next message failed. error is %+v", err1)
				return
			}

			payloadObj1 := msg1.Payload()
			if payload1, ok := payloadObj1.([]byte); ok {
				if string(payload1) != "abcde12_" + strconv.Itoa(i) {
					fmt.Println("read error message content from queue, content is %+v, i is %+v", string(payload1), i)
					return
				}
			} else {
				fmt.Println("read error message type from queue")
				return
			}

		} else {
			fmt.Println("No Next")
			return
		}
	}

	time.Sleep(time.Duration(3) * time.Second)

	for i := 220; i < 260; i++ {
		msgBody := []byte("abcde12_" + strconv.Itoa(i))
		bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
		if err != nil {
			fmt.Println("create bytes message failed. error is %+v", err)
			return
		}

		err = queueProducer.Produce(bytesMsg)
		if err != nil {
			fmt.Println("send bytes message to queue failed. error is %+v", err)
			return
		}
	}

	time.Sleep(time.Duration(3) * time.Second)

	for i := 260; i < 370; i++ {
		msgBody := []byte("abcde12_" + strconv.Itoa(i))
		bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
		if err != nil {
			fmt.Println("create bytes message failed. error is %+v", err)
			return
		}

		err = queueProducer.Produce(bytesMsg)
		if err != nil {
			fmt.Println("send bytes message to queue failed. error is %+v", err)
			return
		}
	}

	queueIterator4, err := NewFileQueueIterator(fileQueue1)
	if err != nil {
		fmt.Println("create file queue iterator failed. error is %+v", err)
		return
	}


	for i := 251; i < 370; i++ {
		if queueIterator4.HasNext() {
			msg1, err1 := queueIterator4.Next()
			if err1 != nil {
				fmt.Println("get file queue next message failed. error is %+v", err1)
				return
			}

			payloadObj1 := msg1.Payload()
			if payload1, ok := payloadObj1.([]byte); ok {
				if string(payload1) != "abcde12_" + strconv.Itoa(i) {
					fmt.Println("read error message content from queue, content is %+v, i is %+v", string(payload1), i)
					return
				}
			} else {
				fmt.Println("read error message type from queue")
				return
			}

		} else {
			fmt.Println("No Next")
			return
		}
	}
}

func TestMultiRoutineFunction(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 3)

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	fileQueue1, err1 := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue2", false, 2000, 90000)

	defer func(){
		if fileQueue1 != nil {
			fileQueue1.Delete()
		}

		if fileQueue1 != nil {
			fileQueue1.Close()
		}
	}()

	if err1 != nil {
		fmt.Println("create file queue failed. error is %+v", err1)
		return
	}

	queueProducer, err := NewFileQueueProducer(fileQueue1)
	if err != nil {
		fmt.Println("create file queue producer failed. error is %+v", err)
		return
	}

	seqGen, err := utils.NewSequenceGenerator()
	if err != nil {
		fmt.Println("create sequence generator failed. error is %+v", err)
		return
	}

	fqConsumer1, err := NewFileQueueConsumer(logConfig, fileQueue1, "consumer1", true)
	if err != nil {
		fmt.Println("create file queue consumer 1 failed. error is %+v", err)
		return
	}

	defer func(){
		if fqConsumer1 != nil {
			fqConsumer1.Close()
		}
	}()

	chans := make([]chan int, 15)

	for i := 0; i < 15; i++ {
		chans[i] = make(chan int)
		go produceRoutine(queueProducer, seqGen, chans[i], i, t)
	}

	for _, ch := range chans {
		<- ch
	}

	queueIterator1, err := NewFileQueueIterator(fileQueue1)
	if err != nil {
		fmt.Println("create file queue iterator failed. error is %+v", err)
		return
	}

	for i := 0; i < 15 * 200; i++ {
		if queueIterator1.HasNext() {
			msg1, err1 := queueIterator1.Next()
			if err1 != nil {
				fmt.Println("get file queue next message failed. error is %+v", err1)
				return
			}

			payloadObj1 := msg1.Payload()
			if _, ok := payloadObj1.([]byte); ok {
			} else {
				fmt.Println("read error message type from queue")
				return
			}

		} else {
			fmt.Println("No Next")
			return
		}
	}

	for i := 0; i < 15 * 200; i++ {
		msg1, err1 := fqConsumer1.ConsumeNoWait()
		if err1 != nil {
			fmt.Println("consume file queue next message from consumer1 failed. error is %+v", err1)
			return
		}

		if msg1 != nil {
			fqConsumer1.Commit()
			payloadObj1 := msg1.Payload()
			if _, ok := payloadObj1.([]byte); ok {
			} else {
				fmt.Println("read error message type from queue")
				return
			}
		} else {
			fmt.Println("Consumer nil message")
			return
		}
	}

	for i := 0; i < 15; i++ {
		chans[i] = make(chan int)
		go produceRoutine2(queueProducer, seqGen, chans[i], i, t)
	}

	for _, ch := range chans {
		<- ch
	}

	queueIterator2, err := NewFileQueueIterator(fileQueue1)
	if err != nil {
		fmt.Println("create file queue iterator failed. error is %+v", err)
		return
	}

	msgNum := 0
	for ;queueIterator2.HasNext(); {
		msg1, err1 := queueIterator2.Next()
		if err1 != nil {
			fmt.Println("get file queue next message failed. error is %+v", err1)
			return
		}

		payloadObj1 := msg1.Payload()
		if _, ok := payloadObj1.([]byte); ok {
			msgNum++
		} else {
			fmt.Println("read error message type from queue")
			return
		}
	}

	fmt.Println("Message Number is %+v", msgNum)
}

func produceRoutine(producer *FileQueueProducer, seqGen *utils.SequenceGenerator, ch chan int, rouNum int, t *testing.T)  {
	for i := 0; i < 200; i++ {
		msgBody := []byte(strconv.Itoa(rouNum) + "_abcde12_" + strconv.Itoa(i))
		bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
		if err != nil {
			fmt.Println("create bytes message failed. error is %+v", err)
			return
		}

		err = producer.Produce(bytesMsg)
		if err != nil {
			fmt.Println("send bytes message to queue failed. error is %+v", err)
			return
		}
	}

	ch <- 1
}

func produceRoutine2(producer *FileQueueProducer, seqGen *utils.SequenceGenerator, ch chan int, rouNum int, t *testing.T)  {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("error occured %+v", r)
		}

		ch <- 1
	}()

	for i := 0; i < 200; i++ {
		msgBody := []byte(strconv.Itoa(rouNum) + "_abcde12_" + strconv.Itoa(i))
		bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
		if err != nil {
			fmt.Println("create bytes message failed. error is %+v", err)
			return
		}

		err = producer.Produce(bytesMsg)
		if err != nil {
			fmt.Println("send bytes message to queue failed. error is %+v", err)
			return
		}

		if i == 100 {
			time.Sleep(time.Duration(3) * time.Second)
		}
	}
}

func TestOneProducerAndOneConsumer(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 3)

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	fileQueue1, err1 := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue3", false, 2000, 90000)

	defer func(){
		if fileQueue1 != nil {
			fileQueue1.Delete()
		}

		if fileQueue1 != nil {
			fileQueue1.Close()
		}
	}()

	if err1 != nil {
		fmt.Println("create file queue failed. error is %+v", err1)
		return
	}

	queueProducer, err := NewFileQueueProducer(fileQueue1)
	if err != nil {
		fmt.Println("create file queue producer failed. error is %+v", err)
		return
	}

	seqGen, err := utils.NewSequenceGenerator()
	if err != nil {
		fmt.Println("create sequence generator failed. error is %+v", err)
		return
	}

	fqConsumer1, err := NewFileQueueConsumer(logConfig, fileQueue1, "consumer1", true)

	defer func(){
		if fqConsumer1 != nil {
			fqConsumer1.Close()
		}
	}()

	if err != nil {
		fmt.Println("create file queue consumer 1 failed. error is %+v", err)
		return
	}

	chans := make([]chan int, 500)
	for i := 0; i < 500; i++ {
		chans[i] = make(chan int)
	}

	go func() {
		for i := 0; i < 500; i++ {
			msgBody := []byte(strconv.Itoa(1) + "_abcde21_" + strconv.Itoa(i))
			bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
			if err != nil {
				fmt.Println("create bytes message failed. error is %+v", err)
				return
			}

			err = queueProducer.Produce(bytesMsg)
			if err != nil {
				fmt.Println("send bytes message to queue failed. error is %+v", err)
				return
			}
		}
	}()

	go func() {
		for i := 0; i < 500; i++ {
			msg1, err1 := fqConsumer1.Consume()
			if err1 != nil {
				fmt.Println("consume file queue next message from consumer1 failed. error is %+v", err1)
				return
			}

			if msg1 != nil {
				fqConsumer1.Commit()
				payloadObj1 := msg1.Payload()
				if _, ok := payloadObj1.([]byte); ok {
					chans[i] <- 1
					//fmt.Logf("Received Message : " + string(payloadBytes))
				} else {
					fmt.Println("read error message type from queue")
					return
				}
			} else {
				fmt.Println("Consumer nil message")
				return
			}
		}
	}()

	for _, ch := range chans {
		<- ch
	}
}

func TestMultiProducersAndMultiDifferentConsumers(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	fileQueue, err := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue4", false, 120000, 1024*1024*1024)

	defer func(){
		if fileQueue != nil {
			fileQueue.Delete()
		}

		if fileQueue != nil {
			fileQueue.Close()
		}
	}()

	if err != nil {
		fmt.Println("create file queue failed. error is %+v", err)
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(50000)

	queueProducer, err := NewFileQueueProducer(fileQueue)
	if err != nil {
		fmt.Println("create file queue producer failed. error is %+v", err)
		return
	}

	seqGen, err := utils.NewSequenceGenerator()
	if err != nil {
		fmt.Println("create sequence generator failed. error is %+v", err)
		return
	}

	fqConsumers := [10]*FileQueueConsumer{}
	for j := 0; j < 10; j++ {
		fqConsumer1, err := NewFileQueueConsumer(logConfig, fileQueue, "consumer" + strconv.Itoa(j), false)

		defer func(){
			if fqConsumer1 != nil {
				fqConsumer1.Close()
			}
		}()

		if err != nil {
			fmt.Println("create file queue consumer 1 failed. error is %+v", err)
			return
		}

		fqConsumers[j] = fqConsumer1
	}

	for j := 0; j < 10; j++ {
		go func(k int) {
			for i := 0; i < 5000; i++ {
				msg1, err1 := fqConsumers[k].Consume()
				if err1 != nil {
					fmt.Println("consume file queue next message from consumer1 failed. error is %+v", err1)
					return
				}

				if msg1 != nil {
					fqConsumers[k].Commit()
					payloadObj1 := msg1.Payload()
					if _, ok := payloadObj1.([]byte); ok {
						wg.Done()
						//fmt.Logf("Received Message : " + string(payloadBytes))
					} else {
						fmt.Println("read error message type from queue")
						return
					}
				} else {
					fmt.Println("Consumer nil message")
					return
				}
			}
		}(j)
	}

	time.Sleep(time.Duration(3) * time.Second)

	for j := 0; j < 10; j++ {
		go func(k int) {
			for i := 0; i < 500; i++ {
				msgBody := []byte(strconv.Itoa(k) + "_abcde21_" + strconv.Itoa(i))
				bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
				if err != nil {
					fmt.Println("create bytes message failed. error is %+v", err)
					return
				}

				err = queueProducer.Produce(bytesMsg)
				if err != nil {
					fmt.Println("send bytes message to queue failed. error is %+v", err)
					return
				}
			}
		}(j)
	}

	wg.Wait()
}

func TestMultiProducersAndMultiSameConsumers(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 3)

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	fileQueue, err := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue5", false, 120000, 1024*1024*1024)

	defer func(){
		if fileQueue != nil {
			fileQueue.Delete()
		}

		if fileQueue != nil {
			fileQueue.Close()
		}
	}()

	if err != nil {
		fmt.Println("create file queue failed. error is %+v", err)
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(5000)

	fqConsumer1, err := NewFileQueueConsumer(logConfig, fileQueue, "consumer" + strconv.Itoa(1), false)

	defer func(){
		if fqConsumer1 != nil {
			fqConsumer1.Close()
		}
	}()

	if err != nil {
		fmt.Println("create file queue consumer 1 failed. error is %+v", err)
		return
	}

	for j := 0; j < 10; j++ {
		go func(k int) {
			queueProducer, err := NewFileQueueProducer(fileQueue)
			if err != nil {
				fmt.Println("create file queue producer failed. error is %+v", err)
				return
			}

			seqGen, err := utils.NewSequenceGenerator()
			if err != nil {
				fmt.Println("create sequence generator failed. error is %+v", err)
				return
			}

			for i := 0; i < 500; i++ {
				msgBody := []byte(strconv.Itoa(k) + "_abcde21_" + strconv.Itoa(i))
				bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
				if err != nil {
					fmt.Println("create bytes message failed. error is %+v", err)
					return
				}

				err = queueProducer.Produce(bytesMsg)
				if err != nil {
					fmt.Println("send bytes message to queue failed. error is %+v", err)
					return
				}
			}
		}(j)
	}

	for j := 0; j < 10; j++ {
		go func(k int) {
			for i := 0; i < 500; i++ {
				msg1, err1 := fqConsumer1.Consume()
				if err1 != nil {
					fmt.Println("consume file queue next message from consumer1 failed. error is %+v", err1)
					return
				}

				if msg1 != nil {
					fqConsumer1.Commit()
					payloadObj1 := msg1.Payload()
					if _, ok := payloadObj1.([]byte); ok {
						wg.Done()
					} else {
						fmt.Println("read error message type from queue")
						return
					}
				} else {
					fmt.Println("Consumer nil message")
					return
				}
			}
		}(j)
	}

	wg.Wait()
}

func TestConsumeWithTimeout(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 3)

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	fileQueue1, err1 := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue8", false, 2000, 90000)

	defer func() {
		if fileQueue1 != nil {
			fileQueue1.Delete()
		}

		if fileQueue1 != nil {
			fileQueue1.Close()
		}
	}()

	if err1 != nil {
		fmt.Println("create file queue failed. error is %+v", err1)
		return
	}

	fqConsumer1, err := NewFileQueueConsumer(logConfig, fileQueue1, "consumer1", true)

	defer func() {
		if fqConsumer1 != nil {
			fqConsumer1.Close()
		}
	}()

	if err != nil {
		fmt.Println("create file queue consumer 1 failed. error is %+v", err)
		return
	}

	msg1, err1 := fqConsumer1.ConsumeWithTimeout(2000)
	if err1 != nil {
		fmt.Println("consume file queue next message from consumer1 failed. error is %+v", err1)
		return
	}

	if msg1 != nil {
		fmt.Println("Consumer not nil message")
		return
	}
}

func TestMultiProducersAndMultiDifferentConsumersConsumeNoWait(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	fileQueue, err := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue6", false, 120000, 1024*1024*1024)

	defer func(){
		if fileQueue != nil {
			fileQueue.Delete()
		}

		if fileQueue != nil {
			fileQueue.Close()
		}
	}()

	if err != nil {
		fmt.Println("create file queue failed. error is %+v", err)
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(50000)

	queueProducer, err := NewFileQueueProducer(fileQueue)
	if err != nil {
		fmt.Println("create file queue producer failed. error is %+v", err)
		return
	}

	seqGen, err := utils.NewSequenceGenerator()
	if err != nil {
		fmt.Println("create sequence generator failed. error is %+v", err)
		return
	}

	fqConsumers := [10]*FileQueueConsumer{}
	for j := 0; j < 10; j++ {
		fqConsumer1, err := NewFileQueueConsumer(logConfig, fileQueue, "consumer" + strconv.Itoa(j), false)

		defer func(){
			if fqConsumer1 != nil {
				fqConsumer1.Close()
			}
		}()

		if err != nil {
			fmt.Println("create file queue consumer 1 failed. error is %+v", err)
			return
		}

		fqConsumers[j] = fqConsumer1
	}

	for j := 0; j < 10; j++ {
		go func(k int) {
			consumedMsgNumber := 0
			for {
				msg1, err1 := fqConsumers[k].ConsumeNoWait()
				if err1 != nil {
					fmt.Println("consume file queue next message from Consumer [" + strconv.Itoa(k) + "] failed. error is %+v", err1)
					return
				}

				if msg1 != nil {
					//fqConsumers[k].Commit()
					payloadObj1 := msg1.Payload()
					if _, ok := payloadObj1.([]byte); ok {
						consumedMsgNumber++
						if consumedMsgNumber >= 5000 {
							fmt.Println("Consumer [" + strconv.Itoa(k) + "] : Consumed Message Number [" + strconv.Itoa(consumedMsgNumber) + "]")
							if consumedMsgNumber == 5000 {
								wg.Done()
							}
						} else {
							wg.Done()
						}

						//fmt.Logf("Received Message : " + string(payloadBytes))
					} else {
						fmt.Println("read error message type from queue")
						return
					}
				} else {
					time.Sleep(time.Duration(50) * time.Millisecond)
				}
			}
		}(j)
	}

	time.Sleep(time.Duration(3) * time.Second)

	for j := 0; j < 10; j++ {
		go func(k int) {
			for i := 0; i < 500; i++ {
				msgBody := []byte(strconv.Itoa(k) + "_abcde21_" + strconv.Itoa(i))
				bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
				if err != nil {
					fmt.Println("create bytes message failed. error is %+v", err)
					return
				}

				err = queueProducer.Produce(bytesMsg)
				if err != nil {
					fmt.Println("send bytes message to queue failed. error is %+v", err)
					return
				}
			}

			fmt.Println("Producer [" + strconv.Itoa(k) + "] Finished")
		}(j)
	}

	wg.Wait()
}

func TestMultiProducersAndMultiDifferentConsumersWithTimeout(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	fileQueue, err := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue8", false, 120000, 1024*1024*1024)

	defer func(){
		if fileQueue != nil {
			fileQueue.Delete()
		}

		if fileQueue != nil {
			fileQueue.Close()
		}
	}()

	if err != nil {
		fmt.Println("create file queue failed. error is %+v", err)
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(50000)

	queueProducer, err := NewFileQueueProducer(fileQueue)
	if err != nil {
		fmt.Println("create file queue producer failed. error is %+v", err)
		return
	}

	seqGen, err := utils.NewSequenceGenerator()
	if err != nil {
		fmt.Println("create sequence generator failed. error is %+v", err)
		return
	}

	fqConsumers := [10]*FileQueueConsumer{}
	for j := 0; j < 10; j++ {
		fqConsumer1, err := NewFileQueueConsumer(logConfig, fileQueue, "consumer" + strconv.Itoa(j), false)

		defer func(){
			if fqConsumer1 != nil {
				fqConsumer1.Close()
			}
		}()

		if err != nil {
			fmt.Println("create file queue consumer 1 failed. error is %+v", err)
			return
		}

		fqConsumers[j] = fqConsumer1
	}

	for j := 0; j < 10; j++ {
		go func(k int) {
			consumedMsgNumber := 0
			for {
				msg1, err1 := fqConsumers[k].ConsumeWithTimeout(1000)
				if err1 != nil {
					fmt.Println("consume file queue next message from Consumer [" + strconv.Itoa(k) + "] failed. error is %+v", err1)
					return
				}

				if msg1 != nil {
					//fqConsumers[k].Commit()
					payloadObj1 := msg1.Payload()
					if _, ok := payloadObj1.([]byte); ok {
						consumedMsgNumber++
						if consumedMsgNumber >= 5000 {
							fmt.Println("Consumer [" + strconv.Itoa(k) + "] : Consumed Message Number [" + strconv.Itoa(consumedMsgNumber) + "]")
							if consumedMsgNumber == 5000 {
								wg.Done()
							}
						} else {
							wg.Done()
						}

						//fmt.Logf("Received Message : " + string(payloadBytes))
					} else {
						fmt.Println("read error message type from queue")
						return
					}
				} else {
					time.Sleep(time.Duration(50) * time.Millisecond)
				}
			}
		}(j)
	}

	time.Sleep(time.Duration(5) * time.Second)

	for j := 0; j < 10; j++ {
		go func(k int) {
			for i := 0; i < 500; i++ {
				msgBody := []byte(strconv.Itoa(k) + "_abcde21_" + strconv.Itoa(i))
				bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
				if err != nil {
					fmt.Println("create bytes message failed. error is %+v", err)
					return
				}

				err = queueProducer.Produce(bytesMsg)
				if err != nil {
					fmt.Println("send bytes message to queue failed. error is %+v", err)
					return
				}
			}

			fmt.Println("Producer [" + strconv.Itoa(k) + "] Finished")
		}(j)
	}

	wg.Wait()
}

func TestMultiProducersAndMultiSameConsumersWithTimeout(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 3)

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	fileQueue, err := NewFileQueue(logConfig, QueueRelativeFilesStoreDir + "test_queue7", false, 120000, 1024*1024*1024)

	defer func(){
		if fileQueue != nil {
			fileQueue.Delete()
		}

		if fileQueue != nil {
			fileQueue.Close()
		}
	}()

	if err != nil {
		fmt.Println("create file queue failed. error is %+v", err)
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(5000)

	fqConsumer1, err := NewFileQueueConsumer(logConfig, fileQueue, "consumer" + strconv.Itoa(1), false)

	defer func(){
		if fqConsumer1 != nil {
			fqConsumer1.Close()
		}
	}()

	if err != nil {
		fmt.Println("create file queue consumer 1 failed. error is %+v", err)
		return
	}

	for j := 0; j < 10; j++ {
		go func(k int) {
			queueProducer, err := NewFileQueueProducer(fileQueue)
			if err != nil {
				fmt.Println("create file queue producer failed. error is %+v", err)
				return
			}

			seqGen, err := utils.NewSequenceGenerator()
			if err != nil {
				fmt.Println("create sequence generator failed. error is %+v", err)
				return
			}

			for i := 0; i < 500; i++ {
				msgBody := []byte(strconv.Itoa(k) + "_abcde21_" + strconv.Itoa(i))
				bytesMsg, err := protocol.NewBytesMessage(msgBody, seqGen)
				if err != nil {
					fmt.Println("create bytes message failed. error is %+v", err)
					return
				}

				err = queueProducer.Produce(bytesMsg)
				if err != nil {
					fmt.Println("send bytes message to queue failed. error is %+v", err)
					return
				}
			}
		}(j)
	}

	for j := 0; j < 10; j++ {
		go func(k int) {
			for i := 0; i < 500; i++ {
				msg1, err1 := fqConsumer1.ConsumeWithTimeout(5000)
				if err1 != nil {
					fmt.Println("consume file queue next message from consumer1 failed. error is %+v", err1)
					return
				}

				if msg1 != nil {
					fqConsumer1.Commit()
					payloadObj1 := msg1.Payload()
					if _, ok := payloadObj1.([]byte); ok {
						wg.Done()
					} else {
						fmt.Println("read error message type from queue")
						return
					}
				} else {
					fmt.Println("Consumer nil message")
					return
				}
			}
		}(j)
	}

	wg.Wait()
}

