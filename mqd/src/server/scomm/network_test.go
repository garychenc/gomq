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
package scomm

import (
	"client/mq"
	"common/utils"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestNetworkBaseFunction(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 3)

	serverConfig := NewLongConnNetworkServerDefaultConfig()
	serverConfig.ServerListeningAddress = "127.0.0.1:26800"
	serverConfig.DeathConnectionScanningIntervalInMillis = 3000
	serverConfig.NoneActiveTimeForDeathConnectionInMillis = 6000
	serverConfig.SendServerLineTestRequestIntervalInMillis = 60000
	networkServer := NewLongConnNetworkServer(serverConfig)

	networkServer.AddRequestAction("hello1", &HelloworldRequestAction{name: "hello1"})
	networkServer.AddRequestAction("hello2", &HelloworldRequestAction{name: "hello2"})
	networkServer.AddRequestAction("hello3", &HelloworldRequestAction{name: "hello3"})

	connChangedListener := &TestConnectionChangedListener{}
	connChangedListener.connectionKeys = utils.NewConcurrentMapWithSize(512)

	networkServer.AddConnectionChangedListener(connChangedListener)

	if err := networkServer.Start(); err != nil {
		networkServer.Stop()
		fmt.Println("create and start long connection server failed. error is ", err)
		return
	}

	const clientCount = 20
	wg := &sync.WaitGroup{}
	wg.Add(clientCount)
	var networkClients [clientCount]mq.INetworkClient
	for i := 0; i < clientCount; i++ {
		clientConfig := mq.NewLongConnNetworkClientDefaultConfig()
		clientConfig.ServerAddress = "127.0.0.1:26800"
		networkClient := mq.NewLongConnNetworkClient(clientConfig)
		networkClients[i] = networkClient

		go func(client mq.INetworkClient, index int) {
			err := client.ConnectToServer("1", "1", strconv.Itoa(index))
			if err != nil {
				client.Disconnect()
				fmt.Println("connect a client to long connection server failed. error is ", err)
			}

			wg.Done()
		}(networkClient, i)
	}

	wg.Wait()

	isAllConnected := true
	for i := 0; i < clientCount; i++ {
		if !networkClients[i].IsConnected() {
			isAllConnected = false
		}
	}

	if !isAllConnected {
		t.Error("Not all client connected.")
		return
	}

	statisInfo := networkServer.GetServerStatisInfo()
	registeredConn := statisInfo["RegisteredConn"].(int)
	registeredButDisconnectedConn := statisInfo["RegisteredButDisconnectedConn"].(int)

	if registeredConn != clientCount && registeredButDisconnectedConn != 0 {
		t.Error("Not all client connected.")
		return
	}

	for i := 0; i < clientCount; i++ {
		_, err1 := networkClients[i].RPC("hello001", []byte("test1"), 5000)
		if err1 != mq.CanNotFindRpcActionError {
			t.Error("Failed.")
			return
		} else {
			fmt.Println("Error is : ", err1)
		}
	}

	for i := 0; i < clientCount; i++ {
		response2, err2 := networkClients[i].RPC("hello1", []byte("test1"), 5000)
		if err2 != nil {
			t.Error("Failed.")
			return
		}

		fmt.Println("Response is : ", string(response2))
	}

	for i := 0; i < clientCount; i++ {
		response2, err2 := networkClients[i].RPC("hello2", []byte("test2"), 5000)
		if err2 != nil {
			t.Error("Failed.")
			return
		}

		fmt.Println("Response is : ", string(response2))
	}

	for i := 0; i < clientCount; i++ {
		networkClients[i].AddRpcRequestActionIfAbsent("hello4", &HelloworldRequestActionClient{name: "hello4"})
	}

	if connChangedListener.connectionKeys.Len() != clientCount {
		t.Error("Failed.")
		return
	}

	for _, key := range connChangedListener.connectionKeys.Keys() {
		connKey := key.(int64)
		_, err1 := networkServer.RPC(connKey, "hello004", []byte("test4"), 5000)
		if err1 != CanNotFindRpcActionError {
			t.Error("Failed.")
			return
		} else {
			fmt.Println("Error is : ", err1)
		}
	}

	for _, key := range connChangedListener.connectionKeys.Keys() {
		connKey := key.(int64)
		response2, err2 := networkServer.RPC(connKey, "hello4", []byte("test40"), 5000)
		if err2 != nil {
			t.Error("Failed.")
			return
		}

		fmt.Println("Response is : ", string(response2))
	}

	for i := 0; i < clientCount; i++ {
		networkClients[i].Disconnect()
	}

	isAllConnected = false
	for i := 0; i < clientCount; i++ {
		if networkClients[i].IsConnected() {
			isAllConnected = true
		}
	}

	if isAllConnected {
		t.Error("Not all client disconnected.")
		return
	}

	time.Sleep(time.Duration(7000) * time.Millisecond)

	statisInfo = networkServer.GetServerStatisInfo()
	registeredConn = statisInfo["RegisteredConn"].(int)
	registeredButDisconnectedConn = statisInfo["RegisteredButDisconnectedConn"].(int)

	if registeredConn != 0 && registeredButDisconnectedConn != 0 {
		t.Error("Not all client disconnected.")
		return
	}

	if connChangedListener.connectionKeys.Len() != 0 {
		t.Error("Failed.")
		return
	}

	networkServer.Stop()
}

type HelloworldRequestAction struct {
	name string
}

func (requestAction *HelloworldRequestAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction MessageDeadAction, err error) {
	msg := string(requestBody)
	//fmt.Println(msg)
	return []byte(requestAction.name + " : " + msg), true, nil, nil
}

type HelloworldRequestActionClient struct {
	name string
}

func (requestAction *HelloworldRequestActionClient) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, err error) {
	msg := string(requestBody)
	//fmt.Println(msg)
	return []byte(requestAction.name + " : " + msg), nil
}

type HelloworldPushAction struct {
	wg *sync.WaitGroup
}

func (pushAction *HelloworldPushAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, err error) {
	pushAction.wg.Done()
	return nil, nil
}

type TestConnectionChangedListener struct {
	connectionKeys utils.IConcurrentMap
}

func (connChangedListener *TestConnectionChangedListener) ConnectionAdded(connKey int64) {
	connChangedListener.connectionKeys.Put(connKey, true)
}

func (connChangedListener *TestConnectionChangedListener) ConnectionDisappeared(connKey int64) {
	connChangedListener.connectionKeys.Remove(connKey)
}

func TestMultiRoutineRpcFunction(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 3)

	serverConfig := NewLongConnNetworkServerDefaultConfig()
	serverConfig.ServerListeningAddress = "127.0.0.1:26801"
	networkServer := NewLongConnNetworkServer(serverConfig)

	networkServer.AddRequestAction("hello1", &HelloworldRequestAction{name: "hello1"})
	networkServer.AddRequestAction("hello2", &HelloworldRequestAction{name: "hello2"})
	networkServer.AddRequestAction("hello3", &HelloworldRequestAction{name: "hello3"})
	networkServer.AddRequestAction("hello4", &HelloworldRequestAction{name: "hello4"})
	networkServer.AddRequestAction("hello5", &HelloworldRequestAction{name: "hello5"})

	if err := networkServer.Start(); err != nil {
		networkServer.Stop()
		fmt.Println("create and start long connection server failed. error is ", err)
		return
	}

	const clientCount = 20
	wg := &sync.WaitGroup{}
	wg.Add(clientCount)
	var networkClients [clientCount]mq.INetworkClient
	for i := 0; i < clientCount; i++ {
		clientConfig := mq.NewLongConnNetworkClientDefaultConfig()
		clientConfig.ServerAddress = "127.0.0.1:26801"
		networkClient := mq.NewLongConnNetworkClient(clientConfig)
		networkClients[i] = networkClient
		networkClients[i].AddRpcRequestActionIfAbsent("hello6", &HelloworldRequestActionClient{name: "hello6"})
		networkClients[i].AddRpcRequestActionIfAbsent("hello7", &HelloworldRequestActionClient{name: "hello7"})
		networkClients[i].AddRpcRequestActionIfAbsent("hello8", &HelloworldRequestActionClient{name: "hello8"})
		networkClients[i].AddRpcRequestActionIfAbsent("hello9", &HelloworldRequestActionClient{name: "hello9"})

		go func(client mq.INetworkClient, index int) {
			err := client.ConnectToServer("1", "1", strconv.Itoa(index))
			if err != nil {
				client.Disconnect()
				fmt.Println("connect a client to long connection server failed. error is ", err)
			}

			wg.Done()
		}(networkClient, i)
	}

	wg.Wait()

	isAllConnected := true
	for i := 0; i < clientCount; i++ {
		if !networkClients[i].IsConnected() {
			isAllConnected = false
		}
	}

	if !isAllConnected {
		t.Error("Not all client connected.")
		return
	}

	statisInfo := networkServer.GetServerStatisInfo()
	registeredConn := statisInfo["RegisteredConn"].(int)
	registeredButDisconnectedConn := statisInfo["RegisteredButDisconnectedConn"].(int)

	if registeredConn != clientCount && registeredButDisconnectedConn != 0 {
		t.Error("Not all client connected.")
		return
	}

	wg2 := &sync.WaitGroup{}
	wg2.Add(10 * clientCount * 5)

	for k := 0; k < 10; k++ {
		for i := 0; i < clientCount; i++ {
			for j := 0; j < 5; j++ {
				go func(client mq.INetworkClient, num int) {
					_, err := client.RPC("hello" + strconv.Itoa(num + 1), []byte("test - " + strconv.Itoa(num + 1)), 5000)
					if err == nil {
						wg2.Done()
					}
				}(networkClients[i], j)
			}
		}
	}

	wg3 := &sync.WaitGroup{}
	wg3.Add(10 * clientCount * 4)

	for k := 0; k < 10; k++ {
		for i := 0; i < clientCount; i++ {
			for j := 0; j < 4; j++ {
				go func(client mq.INetworkClient, num int) {
					_, err := networkServer.RPC(client.GetConnectionKey(), "hello" + strconv.Itoa(num + 6), []byte("test - " + strconv.Itoa(num + 6)), 5000)
					if err == nil {
						wg3.Done()
					}
				}(networkClients[i], j)
			}
		}
	}

	wg4 := &sync.WaitGroup{}
	wg4.Add(10 * clientCount * 5)

	for k := 0; k < 10; k++ {
		for i := 0; i < clientCount; i++ {
			go func(client mq.INetworkClient) {
				for j := 0; j < 5; j++ {
					_, err := client.RPC("hello" + strconv.Itoa(j + 1), []byte("test - " + strconv.Itoa(j + 1)), 5000)
					if err == nil {
						wg4.Done()
					}
				}
			}(networkClients[i])
		}
	}

	wg5 := &sync.WaitGroup{}
	wg5.Add(10 * clientCount * 4)

	for k := 0; k < 10; k++ {
		for i := 0; i < clientCount; i++ {
			go func(client mq.INetworkClient) {
				for j := 0; j < 4; j++ {
					_, err := networkServer.RPC(client.GetConnectionKey(), "hello" + strconv.Itoa(j + 6), []byte("test - " + strconv.Itoa(j + 6)), 5000)
					if err == nil {
						wg5.Done()
					}
				}
			}(networkClients[i])
		}
	}

	wg5.Wait()
	wg4.Wait()
	wg3.Wait()
	wg2.Wait()

	time.Sleep(time.Duration(3000) * time.Millisecond)

	wg1 := &sync.WaitGroup{}
	wg1.Add(clientCount)

	for i := 0; i < clientCount; i++ {
		go func(client mq.INetworkClient) {
			client.Disconnect()
			wg1.Done()
		}(networkClients[i])
	}

	wg1.Wait()

	isAllDisConnected := true
	for i := 0; i < clientCount; i++ {
		if networkClients[i].IsConnected() {
			isAllDisConnected = false
		}
	}

	if !isAllDisConnected {
		t.Error("Not all client disconnected.")
		return
	}

	networkServer.Stop()
}

func TestMultiRoutinePushFunction(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 3)

	serverConfig := NewLongConnNetworkServerDefaultConfig()
	serverConfig.ServerListeningAddress = "127.0.0.1:26802"
	networkServer := NewLongConnNetworkServer(serverConfig)

	if err := networkServer.Start(); err != nil {
		networkServer.Stop()
		fmt.Println("create and start long connection server failed. error is ", err)
		return
	}

	const clientCount = 20
	wg2 := &sync.WaitGroup{}
	wg2.Add(10 * clientCount * 5)

	wg := &sync.WaitGroup{}
	wg.Add(clientCount)
	var networkClients [clientCount]mq.INetworkClient
	for i := 0; i < clientCount; i++ {
		clientConfig := mq.NewLongConnNetworkClientDefaultConfig()
		clientConfig.ServerAddress = "127.0.0.1:26802"
		networkClient := mq.NewLongConnNetworkClient(clientConfig)
		networkClients[i] = networkClient
		networkClients[i].AddRpcRequestActionIfAbsent("pushHello1", &HelloworldPushAction{wg: wg2})
		networkClients[i].AddRpcRequestActionIfAbsent("pushHello2", &HelloworldPushAction{wg: wg2})
		networkClients[i].AddRpcRequestActionIfAbsent("pushHello3", &HelloworldPushAction{wg: wg2})
		networkClients[i].AddRpcRequestActionIfAbsent("pushHello4", &HelloworldPushAction{wg: wg2})
		networkClients[i].AddRpcRequestActionIfAbsent("pushHello5", &HelloworldPushAction{wg: wg2})

		go func(client mq.INetworkClient, index int) {
			err := client.ConnectToServer("1", "1", strconv.Itoa(index))
			if err != nil {
				client.Disconnect()
				fmt.Println("connect a client to long connection server failed. error is ", err)
			}

			wg.Done()
		}(networkClient, i)
	}

	wg.Wait()

	isAllConnected := true
	for i := 0; i < clientCount; i++ {
		if !networkClients[i].IsConnected() {
			isAllConnected = false
		}
	}

	if !isAllConnected {
		t.Error("Not all client connected.")
		return
	}

	statisInfo := networkServer.GetServerStatisInfo()
	registeredConn := statisInfo["RegisteredConn"].(int)
	registeredButDisconnectedConn := statisInfo["RegisteredButDisconnectedConn"].(int)

	if registeredConn != clientCount && registeredButDisconnectedConn != 0 {
		t.Error("Not all client connected.")
		return
	}

	for k := 0; k < 10; k++ {
		for i := 0; i < clientCount; i++ {
			for j := 0; j < 5; j++ {
				go func(client mq.INetworkClient, num int) {
					err := networkServer.Push(client.GetConnectionKey(), "pushHello" + strconv.Itoa(num + 1), []byte("test - " + strconv.Itoa(num + 1)))
					if err != nil {
						t.Error("Failed.")
						return
					}
				}(networkClients[i], j)
			}
		}
	}

	wg2.Wait()

	time.Sleep(time.Duration(3000) * time.Millisecond)

	statisInfo = networkServer.GetServerStatisInfo()
	reliablePushedResponse := statisInfo["ReliablePushedResponse"].(int64)
	if reliablePushedResponse != (10 * clientCount * 5) {
		t.Error("Failed.")
		return
	}

	wg1 := &sync.WaitGroup{}
	wg1.Add(clientCount)

	for i := 0; i < clientCount; i++ {
		go func(client mq.INetworkClient) {
			client.Disconnect()
			wg1.Done()
		}(networkClients[i])
	}

	wg1.Wait()

	isAllDisConnected := true
	for i := 0; i < clientCount; i++ {
		if networkClients[i].IsConnected() {
			isAllDisConnected = false
		}
	}

	if !isAllDisConnected {
		t.Error("Not all client disconnected.")
		return
	}

	networkServer.Stop()
}

func TestMultiRoutineConnectAndDisConnectFunction(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 3)

	serverConfig := NewLongConnNetworkServerDefaultConfig()
	serverConfig.ServerListeningAddress = "127.0.0.1:26803"
	networkServer := NewLongConnNetworkServer(serverConfig)

	networkServer.AddRequestAction("hello1", &HelloworldRequestAction{name: "hello1"})
	networkServer.AddRequestAction("hello2", &HelloworldRequestAction{name: "hello2"})
	networkServer.AddRequestAction("hello3", &HelloworldRequestAction{name: "hello3"})
	networkServer.AddRequestAction("hello4", &HelloworldRequestAction{name: "hello4"})
	networkServer.AddRequestAction("hello5", &HelloworldRequestAction{name: "hello5"})

	if err := networkServer.Start(); err != nil {
		networkServer.Stop()
		fmt.Println("create and start long connection server failed. error is ", err)
		return
	}

	const clientCount = 20
	wg := &sync.WaitGroup{}
	wg.Add(clientCount)

	clientConfig := mq.NewLongConnNetworkClientDefaultConfig()
	clientConfig.ServerAddress = "127.0.0.1:26803"
	networkClient := mq.NewLongConnNetworkClient(clientConfig)

	for i := 0; i < clientCount; i++ {
		go func(client mq.INetworkClient, index int) {
			err := client.ConnectToServer("1", "1", strconv.Itoa(index))
			if err != nil {
				client.Disconnect()
				fmt.Println("connect a client to long connection server failed. error is ", err)
			}

			wg.Done()
		}(networkClient, i)
	}

	wg.Wait()

	if !networkClient.IsConnected() {
		t.Error("Failed.")
		return
	}

	statisInfo := networkServer.GetServerStatisInfo()
	registeredConn := statisInfo["RegisteredConn"].(int)
	registeredButDisconnectedConn := statisInfo["RegisteredButDisconnectedConn"].(int)

	if registeredConn != 1 && registeredButDisconnectedConn != 0 {
		t.Error("Failed.")
		return
	}

	time.Sleep(time.Duration(3000) * time.Millisecond)

	wg1 := &sync.WaitGroup{}
	wg1.Add(clientCount)

	for i := 0; i < clientCount; i++ {
		go func(client mq.INetworkClient) {
			client.Disconnect()
			wg1.Done()
		}(networkClient)
	}

	wg1.Wait()

	if networkClient.IsConnected() {
		t.Error("Failed.")
		return
	}

	networkServer.Stop()
}


































