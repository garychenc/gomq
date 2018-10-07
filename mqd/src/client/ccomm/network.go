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
package ccomm

import (
	"common/config"
	"common/protocol"
	"common/utils"
	"errors"
	"github.com/astaxie/beego/logs"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type INetworkClient interface {
	ConnectToServer(appKey, appSecret, deviceId string) error
	IsConnected() bool
	Disconnect()
	GetConnectionKey() int64

	RPC(rpcActionName string, rpcRequest []byte, timeout int64) (rpcResponse []byte, err error)

	AddRpcRequestActionIfAbsent(rpcActionName string, requestAction IRequestAction)
	RemoveRpcRequestAction(rpcActionName string)
	IsRpcRequestActionRegistered(rpcActionName string) bool

	SetConnectionChangedListener(listener IConnectionChangedListener)
}

type IRequestAction interface {
	RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, err error)
}

type IConnectionChangedListener interface {
	Connected(connKey int64)
	DisConnected(connKey int64)
}

var (
	ConnectedConnectionIsNotTcpError = errors.New("connection is not tcp error")
	RegisterConnectionInServerSideFailedError = errors.New("register connection in server side failed error")
	RegisterConnectionInServerSideTimeoutError = errors.New("register connection in server side timeout error")
	RequestBodyIsNilError = errors.New("request body is nil error")
	ConnectionWasDisconnectedError = errors.New("connection was disconnected error")
	WaitForRpcResponseTimeoutError = errors.New("wait for rpc response timeout error")
	NetworkClientInternalError = errors.New("client internal error")
	CanNotFindRpcActionError = errors.New("can not find RPC action error")
	RpcActionNoResponseError = errors.New("RPC action no response error")
	RpcActionResponseFailedError = errors.New("RPC action response failed error")
)

const (
	FOR_WAITING_THRESHOLD = 10
	NIL_CONNECTION_KEY = 0x7FFFFFFFFFFFFFFF

	REGISTER_CHECKING_RESULT_LEGAL   = 1
	REGISTER_CHECKING_RESULT_ILLEGAL = 2
	REGISTER_CHECKING_GEN_CONN_KEY_ERR = 3

	RPC_FAILED_CAN_NOT_FIND_ACTION = "$$$RPC$FAILED$NO$ACTION$$$"
	RPC_FAILED_ACTION_ERROR = "$$$RPC$FAILED$ACTION$ERROR$$$"
	RPC_FAILED_ACTION_NO_RESPONSE = "$$$RPC$FAILED$ACTION$NO$RESPONSE$$$"
)

type LongConnNetworkClient struct {
	// Final Fields, initialized by New function
	logger      *logs.BeeLogger
	clientConfig *LongConnNetworkClientConfig
	serviceLock sync.RWMutex
	flowControlLock sync.Mutex
	namedRpcRequestActions utils.IConcurrentMap
	dynamicResponseActions utils.IConcurrentMap
	isDynamicResponseActionCleaningRoutineRunning bool
	connectionChangedListener IConnectionChangedListener

	// Changable Files, initialized by ConnectToServer function
	conn *net.TCPConn
	registerResponseReceived bool
	isConnRegisterSucc bool
	registerResponseReceivedCond *sync.Cond
	registerResponseReceivedLock sync.Mutex
	packageAssembler protocol.IPackageAssemble
	packageCompletedListener protocol.IPackageCompletedListener
	receivedDataAssembleChannel chan *utils.IoBuffer
	bizRequestWritingChannel chan *ForWritingBizRequest
	bizRequestWritingRetryChannel1 chan *ForWritingBizRequest
	bizRequestWritingRetryChannel2 chan *ForWritingBizRequest
	readingBufferBytes []byte
	connAppKey, connAppSecret, connDeviceId string
	messageIdGenerator *utils.SequenceGenerator

	// Changable Files
	connLastSentTime int64
	lastHeartbeatTime int64
	heartbeatLock sync.Mutex

	// Initialized by Register Response Received function
	sendHeartbeatInterval int64
	noHeartbeatResponseTimeForDeathConn int64
	reconnectInterval int64
	connectionKey int64
	isCompressPushBody bool
}

type LongConnNetworkClientConfig struct {
	LogConfig *config.LogConfig

	// Socket Config
	ServerAddress                  string
	SocketKeepAlivePeriodInSec     int64
	SocketReadingBufferSizeInBytes int
	SocketWritingBufferSizeInBytes int
	ConnectToServerTimeoutInMillis int64

	// Data Buffer Channel Config
	ReceivedDataAssembleChannelSize int
	BizRequestWritingChannelSize    int

	// Flow Control Config
	SendNextPackageWaitingTimeInMillis int64

	// Register Config
	RegisterResponseReceivedTimeoutInMillis int64

	// Message Reliable Sending Config
	MaxRetrySendMsgTimeInMillis                int64
	RetrySendMsgIntervalInMillis               int64
	WritingBizRequestConsumerCount             int
	DefaultRpcTimeoutInMillis                  int64
	DynamicResponseActionRetentionTimeInMillis int64
}

func NewLongConnNetworkClientConfig() *LongConnNetworkClientConfig {
	return &LongConnNetworkClientConfig{}
}

func NewLongConnNetworkClientDefaultConfig() *LongConnNetworkClientConfig {
	clientConfig := &LongConnNetworkClientConfig{}

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	clientConfig.LogConfig = logConfig
	clientConfig.SocketKeepAlivePeriodInSec = 2
	clientConfig.SocketReadingBufferSizeInBytes = 16 * 1024
	clientConfig.SocketWritingBufferSizeInBytes = 16 * 1024
	clientConfig.ConnectToServerTimeoutInMillis = 3000

	clientConfig.ReceivedDataAssembleChannelSize = 2000000
	clientConfig.BizRequestWritingChannelSize = 2000000

	clientConfig.SendNextPackageWaitingTimeInMillis = -1
	clientConfig.RegisterResponseReceivedTimeoutInMillis = 8000

	clientConfig.MaxRetrySendMsgTimeInMillis = 60000
	clientConfig.RetrySendMsgIntervalInMillis = 5000
	clientConfig.WritingBizRequestConsumerCount = 4
	clientConfig.DefaultRpcTimeoutInMillis = 8000
	clientConfig.DynamicResponseActionRetentionTimeInMillis = 120000

	return clientConfig
}

func NewLongConnNetworkClient(clientConfig *LongConnNetworkClientConfig) INetworkClient {
	networkClient := &LongConnNetworkClient{}
	networkClient.logger = logs.NewLogger(clientConfig.LogConfig.AsyncLogChanLength)
	networkClient.logger.SetLevel(clientConfig.LogConfig.Level)
	networkClient.logger.SetLogger(clientConfig.LogConfig.AdapterName, clientConfig.LogConfig.AdapterParameters)
	if clientConfig.LogConfig.IsAsync {
		networkClient.logger.Async(clientConfig.LogConfig.AsyncLogChanLength)
	}

	networkClient.clientConfig = clientConfig
	networkClient.namedRpcRequestActions = utils.NewConcurrentMapWithSize(512)
	networkClient.dynamicResponseActions = utils.NewConcurrentMapWithSize(16 * 1024)
	networkClient.isDynamicResponseActionCleaningRoutineRunning = false
	networkClient.connectionChangedListener = nil

	return networkClient
}

type PackageReadingCompletedListenerImpl struct {
	networkClient *LongConnNetworkClient
}

type ForWritingBizRequest struct {
	cacheLinePadding1 [utils.CACHE_LINE_SIZE]byte

	funcCode byte
	actionName string
	messageId int64
	payload []byte

	retryStartTime int64
	lastRetryTime int64

	cacheLinePadding2 [utils.CACHE_LINE_SIZE]byte
}

type DynamicResponseActionInfo struct {
	cacheLinePadding1 [utils.CACHE_LINE_SIZE]byte

	// Final Fields
	createdTime int64

	// Changable Fields, Need lock
	isWaitingForResponseReceived bool
	isResponseReceived bool
	responseBody []byte
	responseReceivedCond *sync.Cond
	responseReceivedLock sync.Mutex

	cacheLinePadding2 [utils.CACHE_LINE_SIZE]byte
}

func (networkClient *LongConnNetworkClient) ConnectToServer(appKey, appSecret, deviceId string) error {
	if appKey = strings.Trim(appKey, " "); len(appKey) == 0 {
		panic("appKey can not be empty")
	}

	if appSecret = strings.Trim(appSecret, " "); len(appSecret) == 0 {
		panic("appSecret can not be empty")
	}

	if deviceId = strings.Trim(deviceId, " "); len(deviceId) == 0 {
		panic("deviceId can not be empty")
	}

	if networkClient.conn == nil {
		err := networkClient.connectToRpcServer(appKey, appSecret, deviceId)
		if err != nil {
			return err
		}
	}

	return nil
}

func (networkClient *LongConnNetworkClient) IsConnected() bool {
	networkClient.serviceLock.RLock()
	defer networkClient.serviceLock.RUnlock()
	conn := networkClient.conn
	return conn != nil
}

func (networkClient *LongConnNetworkClient) GetConnectionKey() int64 {
	networkClient.serviceLock.RLock()
	defer networkClient.serviceLock.RUnlock()
	return networkClient.connectionKey
}

func (networkClient *LongConnNetworkClient) Disconnect() {
	networkClient.serviceLock.Lock()
	defer networkClient.serviceLock.Unlock()

	if networkClient.conn != nil {
		networkClient.conn.Close()
		networkClient.conn = nil

		receivedDataAssembleChannel := networkClient.receivedDataAssembleChannel
		bizRequestWritingChannel := networkClient.bizRequestWritingChannel
		bizRequestWritingRetryChannel1 := networkClient.bizRequestWritingRetryChannel1
		bizRequestWritingRetryChannel2 := networkClient.bizRequestWritingRetryChannel2

		if bizRequestWritingRetryChannel1 != nil {
			close(bizRequestWritingRetryChannel1)
			networkClient.bizRequestWritingRetryChannel1 = nil
		}

		if bizRequestWritingRetryChannel2 != nil {
			close(bizRequestWritingRetryChannel2)
			networkClient.bizRequestWritingRetryChannel2 = nil
		}

		if bizRequestWritingChannel != nil {
			close(bizRequestWritingChannel)
			networkClient.bizRequestWritingChannel = nil
		}

		if receivedDataAssembleChannel != nil {
			close(receivedDataAssembleChannel)
			networkClient.receivedDataAssembleChannel = nil
		}

		connectionChangedListener := networkClient.connectionChangedListener
		if connectionChangedListener != nil {
			connectionChangedListener.DisConnected(networkClient.connectionKey)
		}

		networkClient.registerResponseReceived = false
		networkClient.isConnRegisterSucc = false
		networkClient.connLastSentTime = -1
	}
}

func (networkClient *LongConnNetworkClient) connectToRpcServer(appKey, appSecret, deviceId string) error {
	networkClient.serviceLock.Lock()

	clientConfig := networkClient.clientConfig
	conn := networkClient.conn
	if conn == nil {
		socketConn, err := net.DialTimeout("tcp", clientConfig.ServerAddress, time.Duration(clientConfig.ConnectToServerTimeoutInMillis) * time.Millisecond)
		if err != nil {
			networkClient.logger.Error("Connect to server address failed. Server Network : %+v." +
				" Server Address : %+v, Error is : %+v.", "tcp", clientConfig.ServerAddress, err)
			networkClient.serviceLock.Unlock()
			networkClient.Disconnect()
			return err
		}

		tcpSocketConn, ok := socketConn.(*net.TCPConn)
		if !ok {
			networkClient.serviceLock.Unlock()
			networkClient.Disconnect()
			return ConnectedConnectionIsNotTcpError
		}

		tcpSocketConn.SetKeepAlive(true)
		tcpSocketConn.SetKeepAlivePeriod(time.Duration(clientConfig.SocketKeepAlivePeriodInSec * int64(time.Second)))
		tcpSocketConn.SetNoDelay(true)
		tcpSocketConn.SetReadBuffer(clientConfig.SocketReadingBufferSizeInBytes)
		tcpSocketConn.SetWriteBuffer(clientConfig.SocketWritingBufferSizeInBytes)

		msgIdGen, err := utils.NewSequenceGenerator()
		if err != nil {
			networkClient.logger.Error("Create message id sequence generator failed, Error is : %+v.", err)
			networkClient.serviceLock.Unlock()
			networkClient.Disconnect()
			return err
		}

		networkClient.messageIdGenerator = msgIdGen
		networkClient.packageAssembler = protocol.NewPackageAssemble(clientConfig.LogConfig)
		networkClient.packageCompletedListener = &PackageReadingCompletedListenerImpl{networkClient: networkClient}
		networkClient.receivedDataAssembleChannel = make(chan *utils.IoBuffer, clientConfig.ReceivedDataAssembleChannelSize)
		networkClient.bizRequestWritingChannel = make(chan *ForWritingBizRequest, clientConfig.BizRequestWritingChannelSize)
		networkClient.bizRequestWritingRetryChannel1 = make(chan *ForWritingBizRequest, clientConfig.BizRequestWritingChannelSize)
		networkClient.bizRequestWritingRetryChannel2 = make(chan *ForWritingBizRequest, clientConfig.BizRequestWritingChannelSize)
		networkClient.readingBufferBytes = make([]byte, clientConfig.SocketReadingBufferSizeInBytes)

		networkClient.registerResponseReceivedCond = sync.NewCond(&networkClient.registerResponseReceivedLock)

		networkClient.connAppKey = appKey
		networkClient.connAppSecret = appSecret
		networkClient.connDeviceId = deviceId
		networkClient.conn = tcpSocketConn
		networkClient.connectionKey = NIL_CONNECTION_KEY

		networkClient.registerResponseReceived = false
		networkClient.isConnRegisterSucc = false

		writingBizRequestConsumerCount := clientConfig.WritingBizRequestConsumerCount
		writingBizRequestRetryConsumerCount := writingBizRequestConsumerCount / 2
		if writingBizRequestRetryConsumerCount <= 0 {
			writingBizRequestRetryConsumerCount = 1
		}

		for i := 0; i < writingBizRequestConsumerCount; i++ {
			go networkClient.sendBizRequest2ServerTask()
		}

		networkClient.logger.Info("Consumers for channel [BizRequestWritingChannel] are ready, consumer count [%+v].", writingBizRequestConsumerCount)

		for i := 0; i < writingBizRequestRetryConsumerCount; i++ {
			go networkClient.retrySendBizRequest2ServerTask(networkClient.bizRequestWritingRetryChannel1, networkClient.bizRequestWritingRetryChannel2)
			go networkClient.retrySendBizRequest2ServerTask(networkClient.bizRequestWritingRetryChannel2, networkClient.bizRequestWritingRetryChannel1)
		}

		networkClient.logger.Info("Consumers for channel [BizRequestWritingRetryChannel] are ready, consumer count [%+v].", writingBizRequestRetryConsumerCount * 2)

		if !networkClient.isDynamicResponseActionCleaningRoutineRunning {
			go networkClient.dynamicResponseActionCleaningTask()
			networkClient.logger.Info("Dynamic response action cleaning task is running.")
		}

		go networkClient.keepPackageAssemblingTask()
		go networkClient.readDataFromConnectionTask(tcpSocketConn)

		connectionChangedListener := networkClient.connectionChangedListener
		networkClient.serviceLock.Unlock()

		writingBufferBytes := make([]byte, 0, clientConfig.SocketWritingBufferSizeInBytes)
		writingBuffer := utils.NewIoBuffer(writingBufferBytes)
		err = protocol.Initialize2LongConnRegisterRequestPackage(appKey, appSecret, deviceId, "", writingBuffer)
		if err != nil {
			networkClient.logger.Error("Write register request to server failed. Server Network : %+v." +
				" Server Address : %+v, Error is : %+v.", "tcp", clientConfig.ServerAddress, err)
			networkClient.Disconnect()
			return err
		}

		networkClient.waitForSendNextPackage()
		_, err = tcpSocketConn.Write(writingBuffer.Bytes())
		if err != nil {
			networkClient.logger.Error("Write register request to server failed. Server Network : %+v." +
				" Server Address : %+v, Error is : %+v.", "tcp", clientConfig.ServerAddress, err)
			networkClient.Disconnect()
			return err
		}

		networkClient.registerResponseReceivedLock.Lock()

		if !networkClient.registerResponseReceived {
			go func() {
				time.Sleep(time.Duration(clientConfig.RegisterResponseReceivedTimeoutInMillis) * time.Millisecond)
				networkClient.registerResponseReceivedLock.Lock()
				defer networkClient.registerResponseReceivedLock.Unlock()

				networkClient.registerResponseReceived = true
				networkClient.registerResponseReceivedCond.Broadcast()
			}()

			networkClient.registerResponseReceivedCond.Wait()
		}

		if networkClient.registerResponseReceived {
			if networkClient.isConnRegisterSucc {
				networkClient.registerResponseReceivedLock.Unlock()

				networkClient.heartbeatLock.Lock()
				networkClient.lastHeartbeatTime = utils.CurrentTimeMillis()
				networkClient.heartbeatLock.Unlock()
				go networkClient.sendHearbeatRequestRepeatedly()

				if connectionChangedListener != nil {
					connectionChangedListener.Connected(networkClient.connectionKey)
				}
			} else {
				networkClient.registerResponseReceivedLock.Unlock()

				networkClient.logger.Error("Register connection in serever side failed, AppKey: %+v, AppSecret: %+v, DeviceId: %+v, Server Address : %+v.", appKey, appSecret, deviceId, clientConfig.ServerAddress)
				networkClient.Disconnect()
				return RegisterConnectionInServerSideFailedError
			}
		} else {
			networkClient.registerResponseReceivedLock.Unlock()

			networkClient.logger.Error("No response when register connection in serever side, AppKey: %+v, AppSecret: %+v, DeviceId: %+v, Server Address : %+v.", appKey, appSecret, deviceId, clientConfig.ServerAddress)
			networkClient.Disconnect()
			return RegisterConnectionInServerSideTimeoutError
		}
	} else {
		networkClient.serviceLock.Unlock()
	}

	return nil
}

func (networkClient *LongConnNetworkClient) waitForSendNextPackage() {
	clientConfig := networkClient.clientConfig
	if clientConfig != nil {
		sendNextPackageWaitingTime := clientConfig.SendNextPackageWaitingTimeInMillis
		if sendNextPackageWaitingTime > 0 {
			networkClient.flowControlLock.Lock()

			connLastSentTime := networkClient.connLastSentTime
			if connLastSentTime > 0 {
				forWaitingTime := sendNextPackageWaitingTime - (utils.CurrentTimeMillis() - connLastSentTime)
				if forWaitingTime > FOR_WAITING_THRESHOLD {
					time.Sleep(time.Duration(forWaitingTime) * time.Millisecond)
				}
			}

			networkClient.connLastSentTime = utils.CurrentTimeMillis()

			networkClient.flowControlLock.Unlock()
		}
	}
}

func (networkClient *LongConnNetworkClient) sendHearbeatRequestRepeatedly() {
	defer func() {
		if err := recover(); err != nil {
			networkClient.logger.Info("Send heartbeat task finished. Error is %+v.", err)
		}
	}()

	networkClient.logger.Info("Send heartbeat task started. Connection Key : %+v", networkClient.connectionKey)

	for {
		conn := networkClient.conn
		sendHeartbeatInterval := networkClient.sendHeartbeatInterval
		if conn == nil {
			networkClient.logger.Error("Connection is disconnected. Connection Key : %+v", networkClient.connectionKey)
			break
		}

		time.Sleep(time.Duration(sendHeartbeatInterval) * time.Millisecond)

		networkClient.serviceLock.RLock()

		reconnectInterval := networkClient.reconnectInterval
		clientConfig := networkClient.clientConfig
		conn = networkClient.conn
		if conn == nil {
			networkClient.logger.Error("Connection is disconnected. Connection Key : %+v", networkClient.connectionKey)
			networkClient.serviceLock.RUnlock()
			break
		}

		networkClient.serviceLock.RUnlock()

		writingBufferBytes := make([]byte, 0, clientConfig.SocketWritingBufferSizeInBytes)
		writingBuffer := utils.NewIoBuffer(writingBufferBytes)
		protocol.Initialize2LongConnHeartbeatRequestPackage(writingBuffer)
		networkClient.waitForSendNextPackage()
		_, err := conn.Write(writingBuffer.Bytes())
		if err != nil {
			networkClient.logger.Error("Write heartbeat request to server failed. Server Network : %+v." +
				" Server Address : %+v, Error is : %+v.", "tcp", clientConfig.ServerAddress, err)
		}

		networkClient.heartbeatLock.Lock()
		lastHeartbeatTimeElapsed := utils.CurrentTimeMillis() - networkClient.lastHeartbeatTime
		networkClient.heartbeatLock.Unlock()

		if lastHeartbeatTimeElapsed > networkClient.noHeartbeatResponseTimeForDeathConn {
			networkClient.logger.Error("Connection is dead, go into reconnect loop. Connection Key : %+v", networkClient.connectionKey)
			networkClient.Disconnect()
			time.Sleep(time.Duration(reconnectInterval) * time.Millisecond)
			go networkClient.reconnectToRpcServer()
			break
		}
	}

	networkClient.logger.Info("Send heartbeat task finished. Connection Key : %+v", networkClient.connectionKey)
}

func (networkClient *LongConnNetworkClient) reconnectToRpcServer() {
	defer func() {
		if err := recover(); err != nil {
			networkClient.logger.Error("Reconnect to RPC Server task finished. Error is %+v.", err)
		}
	}()

	networkClient.logger.Info("Reconnect to RPC Server task started. Connection Key : %+v", networkClient.connectionKey)

	for {
		networkClient.logger.Info("Going to reconnect to RPC Server : %+v, Connection Key : %+v", networkClient.clientConfig.ServerAddress, networkClient.connectionKey)

		connAppKey := networkClient.connAppKey
		connAppSecret := networkClient.connAppSecret
		connDeviceId := networkClient.connDeviceId
		reconnectInterval := networkClient.reconnectInterval
		conn := networkClient.conn
		if conn != nil {
			networkClient.logger.Error("Connection was already connected. Connection Key : %+v", networkClient.connectionKey)
			break
		}

		err := networkClient.connectToRpcServer(connAppKey, connAppSecret, connDeviceId)
		if err != nil {
			if err == RegisterConnectionInServerSideFailedError {
				break
			} else {
				networkClient.logger.Error("Reconnect to server failed. Error is : %+v, Connection Key : %+v", err, networkClient.connectionKey)
				networkClient.Disconnect()
				time.Sleep(time.Duration(reconnectInterval) * time.Millisecond)
			}
		} else {
			networkClient.logger.Error("Connection was already connected. Connection Key : %+v", networkClient.connectionKey)
			break
		}
	}

	networkClient.logger.Info("Reconnect to RPC Server task finished. Connection Key : %+v", networkClient.connectionKey)
}

func (networkClient *LongConnNetworkClient) readDataFromConnectionTask(socketConn *net.TCPConn) {
	defer func() {
		if err := recover(); err != nil {
			networkClient.logger.Error("Reading task finished. No more connection data can be readed and processed. Error is %+v.", err)
		}
	}()

	readErrorTimes := 0

	for {
		networkClient.serviceLock.RLock()

		conn := networkClient.conn
		if conn == nil {
			networkClient.logger.Error("Connection is disconnected. Reading task finished. Connection Key : %+v", networkClient.connectionKey)
			networkClient.serviceLock.RUnlock()
			break
		}

		receivedDataAssembleChannel := networkClient.receivedDataAssembleChannel
		readingBufferBytes := networkClient.readingBufferBytes
		reconnectInterval := networkClient.reconnectInterval

		networkClient.serviceLock.RUnlock()

		readedCount, err := socketConn.Read(readingBufferBytes)
		if readedCount > 0 {
			readedBytes := make([]byte, readedCount)
			copy(readedBytes, readingBufferBytes)
			readedBuffer := utils.NewIoBuffer(readedBytes)
			receivedDataAssembleChannel <- readedBuffer
			readErrorTimes = 0
		} else {
			if err != nil {
				if err == io.EOF {
					networkClient.logger.Error("Read EOF from server side, go into reconnect loop.")
					networkClient.Disconnect()
					time.Sleep(time.Duration(reconnectInterval) * time.Millisecond)
					go networkClient.reconnectToRpcServer()
					break
				} else if opError, ok := err.(*net.OpError); ok {
					remoteAddress := socketConn.RemoteAddr()
					if opError.Timeout() {
						networkClient.logger.Info("Read timeout and no data readed. RemoteAddress : [%+v].", remoteAddress.String())
					} else {
						if readErrorTimes < 8 {
							networkClient.logger.Debug("Read data from socket encountered an error, but go on another reading. RemoteAddress : [%+v]. Error is %+v.", remoteAddress.String(), err)
							readErrorTimes++
						} else {
							networkClient.logger.Error("Reading data task encountered severe error, go into EOF process. RemoteAddress : [%+v]. Error is %+v.", remoteAddress.String(), err)
							networkClient.Disconnect()
							time.Sleep(time.Duration(reconnectInterval) * time.Millisecond)
							go networkClient.reconnectToRpcServer()
							break
						}
					}
				} else {
					remoteAddress := socketConn.RemoteAddr()
					if readErrorTimes < 8 {
						networkClient.logger.Debug("Read data from socket encountered an error, but go on another reading. RemoteAddress : [%+v]. Error is %+v.", remoteAddress.String(), err)
						readErrorTimes++
					} else {
						networkClient.logger.Error("Reading data task encountered severe error, go into EOF process. RemoteAddress : [%+v]. Error is %+v.", remoteAddress.String(), err)
						networkClient.Disconnect()
						time.Sleep(time.Duration(reconnectInterval) * time.Millisecond)
						go networkClient.reconnectToRpcServer()
						break
					}
				}
			} else {
				readErrorTimes = 0
			}
		}
	}

	remoteAddress := socketConn.RemoteAddr()
	networkClient.logger.Info("Reading task finished. No more connection data can be readed and processed. RemoteAddress : [%+v].", remoteAddress.String())
}

func (networkClient *LongConnNetworkClient) keepPackageAssemblingTask() {
	defer func() {
		if err := recover(); err != nil {
			networkClient.logger.Info("Package assemble task finished. Error is %+v.", err)
		}
	}()

	for {
		networkClient.serviceLock.RLock()

		//clientConfig := networkClient.clientConfig
		conn := networkClient.conn
		if conn == nil {
			networkClient.logger.Error("Connection is disconnected. Package assembling task finished. Connection Key : %+v", networkClient.connectionKey)
			networkClient.serviceLock.RUnlock()
			break
		}

		receivedDataAssembleChannel := networkClient.receivedDataAssembleChannel
		packageAssembler := networkClient.packageAssembler
		packageCompletedListener := networkClient.packageCompletedListener

		networkClient.serviceLock.RUnlock()

		readedData := <- receivedDataAssembleChannel
		if readedData != nil {
			packageAssembler.Assemble2CompletePackage(readedData, packageCompletedListener)
		}
	}

	networkClient.logger.Info("Package assemble task finished. Connection Key : %+v", networkClient.connectionKey)
}

func (listener *PackageReadingCompletedListenerImpl) OnCompleted(completedPackage *utils.IoBuffer) error {
	networkClient := listener.networkClient
	decodedPackage := protocol.ValueOf(completedPackage, networkClient.connectionKey)
	functionCode := decodedPackage.FunctionCode()

	switch functionCode {

		case protocol.LC_FUNC_CODE_REGISTER_RESPONSE: {
			networkClient.logger.Info("Received a register response")
			payload := decodedPackage.Payload()
			responseCode := protocol.GetResponseCodeFromPayload(payload)
			if responseCode == REGISTER_CHECKING_RESULT_LEGAL {
				connKey := decodedPackage.ConnKey()
				sendHeartbeatInterval := protocol.GetSendHeartbeatIntervalFromPayload(payload)
				noHeartbeatResponseTimeForDeathConn := protocol.GetNoHeartbeatResponseTimeForDeathConnFromPayload(payload)
				reconnectInterval := protocol.GetReconnectIntervalFromPayload(payload)
				isCompressPushBody := protocol.GetIsCompressPushBodyFromPayload(payload)

				networkClient.registerResponseReceivedLock.Lock()

				networkClient.connectionKey = connKey
				networkClient.sendHeartbeatInterval = int64(sendHeartbeatInterval)
				networkClient.noHeartbeatResponseTimeForDeathConn = int64(noHeartbeatResponseTimeForDeathConn)
				networkClient.reconnectInterval = int64(reconnectInterval)
				networkClient.isCompressPushBody = isCompressPushBody

				networkClient.isConnRegisterSucc = true
				networkClient.registerResponseReceived = true
				networkClient.registerResponseReceivedCond.Broadcast()

				networkClient.registerResponseReceivedLock.Unlock()
			} else {
				networkClient.registerResponseReceivedLock.Lock()

				networkClient.isConnRegisterSucc = false
				networkClient.registerResponseReceived = true
				networkClient.registerResponseReceivedCond.Broadcast()

				networkClient.registerResponseReceivedLock.Unlock()
			}
		}

		case protocol.LC_FUNC_CODE_HEART_BEAT_RESPONSE: {
			networkClient.heartbeatLock.Lock()
			networkClient.lastHeartbeatTime = utils.CurrentTimeMillis()
			networkClient.heartbeatLock.Unlock()
		}

		case protocol.LC_FUNC_CODE_LINE_TEST_REQUEST: {
			go networkClient.lineTestRequest(decodedPackage)
		}

		case  protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE: {
			go networkClient.client2ServerCallResponse(decodedPackage)
		}

		case  protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_REQUEST: {
			go networkClient.server2ClientCallRequest(decodedPackage)
		}

		case  protocol.LC_FUNC_CODE_SERVER_2_CLIENT_PUSH_REQUEST: {
			go networkClient.server2ClientPushRequest(decodedPackage)
		}

		default: {
			networkClient.logger.Error("Receive an illegal long connection package. Function Code : %+v.", functionCode)
		}

	}

	return nil
}

func (networkClient *LongConnNetworkClient) lineTestRequest(lineTestRequest *protocol.WiredPackage) {
	networkClient.serviceLock.RLock()

	clientConfig := networkClient.clientConfig
	conn := networkClient.conn
	if conn == nil {
		networkClient.logger.Error("Connection is disconnected. Line test request process task finished. Connection Key : %+v", networkClient.connectionKey)
		networkClient.serviceLock.RUnlock()
		return
	}

	networkClient.serviceLock.RUnlock()

	writingBufferBytes := make([]byte, 0, clientConfig.SocketWritingBufferSizeInBytes)
	writingBuffer := utils.NewIoBuffer(writingBufferBytes)
	protocol.Initialize2ServerLineTestResponsePackage(writingBuffer)
	networkClient.waitForSendNextPackage()
	_, err := conn.Write(writingBuffer.Bytes())
	if err != nil {
		networkClient.logger.Error("Write line test response to server failed. Server Network : %+v." +
			" Server Address : %+v, Error is : %+v.", "tcp", clientConfig.ServerAddress, err)
	}
}

func (networkClient *LongConnNetworkClient) client2ServerCallResponse(rpcResponse *protocol.WiredPackage) {
	networkClient.serviceLock.RLock()

	conn := networkClient.conn
	connectionKey := networkClient.connectionKey
	dynamicResponseActions := networkClient.dynamicResponseActions
	isCompressPushBody := networkClient.isCompressPushBody
	if conn == nil {
		networkClient.logger.Error("Connection is disconnected. Server to client rpc response process task finished. Connection Key : %+v", networkClient.connectionKey)
		networkClient.serviceLock.RUnlock()
		return
	}

	networkClient.serviceLock.RUnlock()

	if dynamicResponseActions.Len() > 0 {
		_, messageId, actionBody, err := protocol.GetLongConnActionInfoFromPayload(rpcResponse.Payload(), isCompressPushBody)
		if err != nil {
			networkClient.logger.Error("Get response action info from payload failed, can not process server downstream response. Long connection with key [%+v]. Error is : %+v.", connectionKey, err)
			return
		}

		action := dynamicResponseActions.Remove(messageId)
		if action != nil {
			if responseActionInfo, ok := action.(*DynamicResponseActionInfo); ok {
				if responseActionInfo.isWaitingForResponseReceived {
					responseActionInfo.responseReceivedLock.Lock()
					responseActionInfo.isResponseReceived = true
					responseActionInfo.responseBody = actionBody
					responseActionInfo.responseReceivedCond.Broadcast()
					responseActionInfo.responseReceivedLock.Unlock()
				}
			} else {
				networkClient.logger.Debug("Can not get dynamic response action by message id [%+v], can not process server downstream response, discard it.", messageId)
			}
		} else {
			networkClient.logger.Debug("Can not get dynamic response action by message id [%+v], can not process server downstream response, discard it.", messageId)
		}
	} else {
		networkClient.logger.Debug("Dynamic response actions is empty, can not process server downstream response, discard it.")
	}
}

func (networkClient *LongConnNetworkClient) server2ClientCallRequest(rpcRequest *protocol.WiredPackage) {
	networkClient.serviceLock.RLock()

	conn := networkClient.conn
	connectionKey := networkClient.connectionKey
	namedRpcRequestActions := networkClient.namedRpcRequestActions
	bizRequestWritingChannel := networkClient.bizRequestWritingChannel
	isCompressPushBody := networkClient.isCompressPushBody
	if conn == nil {
		networkClient.logger.Error("Connection is disconnected. Server to client rpc request process task finished. Connection Key : %+v", networkClient.connectionKey)
		networkClient.serviceLock.RUnlock()
		return
	}

	networkClient.serviceLock.RUnlock()

	actionName, messageId, actionBody, err := protocol.GetLongConnActionInfoFromPayload(rpcRequest.Payload(), isCompressPushBody)
	if err != nil {
		networkClient.logger.Error("Get request action info from payload failed, can not process server downstream request. Long connection with key [%+v]. Error is : %+v.", connectionKey, err)
		return
	}

	if namedRpcRequestActions.Len() > 0 {
		action := namedRpcRequestActions.Get(actionName)
		if action != nil {
			if requestAction, ok := action.(IRequestAction); ok {
				actionResponse, err := requestAction.RequestReceived(connectionKey, messageId, actionBody)
				if err != nil {
					networkClient.logger.Error("Dispatch request call package to request action failed, discard it. Error is : %+v.", err)
					bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_CALL_RESPONSE,
						actionName: actionName, messageId: messageId, payload: []byte(RPC_FAILED_ACTION_ERROR), retryStartTime: -1, lastRetryTime: -1}
					bizRequestWritingChannel <- bizRequest
				} else {
					if actionResponse != nil && len(actionResponse) > 0 {
						bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_CALL_RESPONSE,
							actionName: actionName, messageId: messageId, payload: actionResponse, retryStartTime: -1, lastRetryTime: -1}
						bizRequestWritingChannel <- bizRequest
					} else {
						bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_CALL_RESPONSE,
							actionName: actionName, messageId: messageId, payload: []byte(RPC_FAILED_ACTION_NO_RESPONSE), retryStartTime: -1, lastRetryTime: -1}
						bizRequestWritingChannel <- bizRequest
					}
				}
			} else {
				networkClient.logger.Error("Can not get RPC action by name [%+v], can not process rpc call package, discard it.", actionName)
				bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_CALL_RESPONSE,
					actionName: actionName, messageId: messageId, payload: []byte(RPC_FAILED_CAN_NOT_FIND_ACTION), retryStartTime: -1, lastRetryTime: -1}
				bizRequestWritingChannel <- bizRequest
			}
		} else {
			networkClient.logger.Error("Can not get RPC action by name [%+v], can not process rpc call package, discard it.", actionName)
			bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_CALL_RESPONSE,
				actionName: actionName, messageId: messageId, payload: []byte(RPC_FAILED_CAN_NOT_FIND_ACTION), retryStartTime: -1, lastRetryTime: -1}
			bizRequestWritingChannel <- bizRequest
		}
	} else {
		networkClient.logger.Error("Named RPC actions is empty, can not process rpc call package, discard it.")
		bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_CALL_RESPONSE,
			actionName: actionName, messageId: messageId, payload: []byte(RPC_FAILED_CAN_NOT_FIND_ACTION), retryStartTime: -1, lastRetryTime: -1}
		bizRequestWritingChannel <- bizRequest
	}
}

func (networkClient *LongConnNetworkClient) server2ClientPushRequest(pushedRequest *protocol.WiredPackage) {
	networkClient.serviceLock.RLock()

	conn := networkClient.conn
	connectionKey := networkClient.connectionKey
	namedRpcRequestActions := networkClient.namedRpcRequestActions
	dynamicResponseActions := networkClient.dynamicResponseActions
	bizRequestWritingChannel := networkClient.bizRequestWritingChannel
	isCompressPushBody := networkClient.isCompressPushBody
	if conn == nil {
		networkClient.logger.Error("Connection is disconnected. Server to client push request process task finished. Connection Key : %+v", networkClient.connectionKey)
		networkClient.serviceLock.RUnlock()
		return
	}

	networkClient.serviceLock.RUnlock()

	actionName, messageId, actionBody, err := protocol.GetLongConnActionInfoFromPayload(pushedRequest.Payload(), isCompressPushBody)
	if err != nil {
		networkClient.logger.Error("Get request action info from payload failed, can not process server push request. Long connection with key [%+v]. Error is : %+v.", connectionKey, err)
		return
	}

	if dynamicResponseActions.Len() > 0 {
		action := dynamicResponseActions.Remove(messageId)
		if action != nil {
			if responseActionInfo, ok := action.(*DynamicResponseActionInfo); ok {
				if responseActionInfo.isWaitingForResponseReceived {
					responseActionInfo.responseReceivedLock.Lock()
					responseActionInfo.isResponseReceived = true
					responseActionInfo.responseBody = actionBody
					responseActionInfo.responseReceivedCond.Broadcast()
					responseActionInfo.responseReceivedLock.Unlock()
				}

				bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
					actionName: actionName, messageId: messageId, payload: []byte("SUCC"), retryStartTime: -1, lastRetryTime: -1}
				bizRequestWritingChannel <- bizRequest
			} else {
				if namedRpcRequestActions.Len() > 0 {
					action := namedRpcRequestActions.Get(actionName)
					if action != nil {
						if requestAction, ok := action.(IRequestAction); ok {
							_, err := requestAction.RequestReceived(connectionKey, messageId, actionBody)
							if err != nil {
								networkClient.logger.Error("Dispatch push package to request action failed, discard it. Error is : %+v.", err)
							}

							bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
								actionName: actionName, messageId: messageId, payload: []byte("SUCC"), retryStartTime: -1, lastRetryTime: -1}
							bizRequestWritingChannel <- bizRequest
						} else {
							networkClient.logger.Error("Can not get RPC action by name [%+v], can not process push package, discard it.", actionName)

							bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
								actionName: actionName, messageId: messageId, payload: []byte("NOT_RECOVER"), retryStartTime: -1, lastRetryTime: -1}
							bizRequestWritingChannel <- bizRequest
						}
					} else {
						networkClient.logger.Error("Can not get RPC action by name [%+v], can not process push package, discard it.", actionName)

						bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
							actionName: actionName, messageId: messageId, payload: []byte("NOT_RECOVER"), retryStartTime: -1, lastRetryTime: -1}
						bizRequestWritingChannel <- bizRequest
					}
				} else {
					networkClient.logger.Error("Named RPC actions is empty, can not process push package, discard it.")

					bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
						actionName: actionName, messageId: messageId, payload: []byte("NOT_RECOVER"), retryStartTime: -1, lastRetryTime: -1}
					bizRequestWritingChannel <- bizRequest
				}
			}
		} else {
			if namedRpcRequestActions.Len() > 0 {
				action := namedRpcRequestActions.Get(actionName)
				if action != nil {
					if requestAction, ok := action.(IRequestAction); ok {
						_, err := requestAction.RequestReceived(connectionKey, messageId, actionBody)
						if err != nil {
							networkClient.logger.Error("Dispatch push package to request action failed, discard it. Error is : %+v.", err)
						}

						bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
							actionName: actionName, messageId: messageId, payload: []byte("SUCC"), retryStartTime: -1, lastRetryTime: -1}
						bizRequestWritingChannel <- bizRequest
					} else {
						networkClient.logger.Error("Can not get RPC action by name [%+v], can not process push package, discard it.", actionName)

						bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
							actionName: actionName, messageId: messageId, payload: []byte("NOT_RECOVER"), retryStartTime: -1, lastRetryTime: -1}
						bizRequestWritingChannel <- bizRequest
					}
				} else {
					networkClient.logger.Error("Can not get RPC action by name [%+v], can not process push package, discard it.", actionName)

					bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
						actionName: actionName, messageId: messageId, payload: []byte("NOT_RECOVER"), retryStartTime: -1, lastRetryTime: -1}
					bizRequestWritingChannel <- bizRequest
				}
			} else {
				networkClient.logger.Error("Named RPC actions is empty, can not process push package, discard it.")

				bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
					actionName: actionName, messageId: messageId, payload: []byte("NOT_RECOVER"), retryStartTime: -1, lastRetryTime: -1}
				bizRequestWritingChannel <- bizRequest
			}
		}
	} else {
		if namedRpcRequestActions.Len() > 0 {
			action := namedRpcRequestActions.Get(actionName)
			if action != nil {
				if requestAction, ok := action.(IRequestAction); ok {
					_, err := requestAction.RequestReceived(connectionKey, messageId, actionBody)
					if err != nil {
						networkClient.logger.Error("Dispatch push package to request action failed, discard it. Error is : %+v.", err)
					}

					bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
						actionName: actionName, messageId: messageId, payload: []byte("SUCC"), retryStartTime: -1, lastRetryTime: -1}
					bizRequestWritingChannel <- bizRequest
				} else {
					networkClient.logger.Error("Can not get RPC action by name [%+v], can not process push package, discard it.", actionName)

					bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
						actionName: actionName, messageId: messageId, payload: []byte("NOT_RECOVER"), retryStartTime: -1, lastRetryTime: -1}
					bizRequestWritingChannel <- bizRequest
				}
			} else {
				networkClient.logger.Error("Can not get RPC action by name [%+v], can not process push package, discard it.", actionName)

				bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
					actionName: actionName, messageId: messageId, payload: []byte("NOT_RECOVER"), retryStartTime: -1, lastRetryTime: -1}
				bizRequestWritingChannel <- bizRequest
			}
		} else {
			networkClient.logger.Error("Named RPC actions is empty, can not process push package, discard it.")

			bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE,
				actionName: actionName, messageId: messageId, payload: []byte("NOT_RECOVER"), retryStartTime: -1, lastRetryTime: -1}
			bizRequestWritingChannel <- bizRequest
		}
	}
}

func (networkClient *LongConnNetworkClient) sendBizRequest2ServerTask() {
	defer func() {
		if err := recover(); err != nil {
			networkClient.logger.Error("SendBizRequest2ServerTask finished, No more request can be send to server. Error is %+v.", err)
		}
	}()

	for {
		networkClient.serviceLock.RLock()

		conn := networkClient.conn
		connectionKey := networkClient.connectionKey
		bizRequestWritingChannel := networkClient.bizRequestWritingChannel
		bizRequestWritingRetryChannel1 := networkClient.bizRequestWritingRetryChannel1
		isCompressPushBody := networkClient.isCompressPushBody
		clientConfig := networkClient.clientConfig
		if conn == nil {
			networkClient.logger.Debug("Connection is disconnected. SendBizRequest2ServerTask finished. Connection Key : %+v", networkClient.connectionKey)
			networkClient.serviceLock.RUnlock()
			break
		}

		if connectionKey == NIL_CONNECTION_KEY {
			networkClient.serviceLock.RUnlock()
			continue
		}

		networkClient.serviceLock.RUnlock()

		bizRequest := <- bizRequestWritingChannel
		if bizRequest != nil {
			networkClient.realSendBizRequestByFuncCode(bizRequest, isCompressPushBody, bizRequestWritingRetryChannel1, clientConfig)
		}
	}

	networkClient.logger.Debug("SendBizRequest2ServerTask finished, No more request can be send to server.")
}

func (networkClient *LongConnNetworkClient) realSendBizRequestByFuncCode(bizRequest *ForWritingBizRequest, isCompressPushBody bool,
	bizRequestWritingRetryChannel chan *ForWritingBizRequest, clientConfig *LongConnNetworkClientConfig) {
	actionName := bizRequest.actionName
	messageId := bizRequest.messageId
	payload := bizRequest.payload

	networkClient.serviceLock.RLock()

	conn := networkClient.conn
	connectionKey := networkClient.connectionKey

	networkClient.serviceLock.RUnlock()

	if conn != nil {

		writingBufferBytes := make([]byte, 0, clientConfig.SocketWritingBufferSizeInBytes)
		writingBuffer := utils.NewIoBuffer(writingBufferBytes)

		switch bizRequest.funcCode {
			case protocol.LC_FUNC_CODE_CLIENT_2_SERVER_CALL_RESPONSE: {
				err := protocol.Initialize2ClientLongConnCallResponsePackage(actionName, messageId, payload, isCompressPushBody, writingBuffer)
				if err != nil {
					networkClient.processSend2ServerError(bizRequest, bizRequestWritingRetryChannel)
					networkClient.logger.Debug("Send rpc action response to connection with key [%+v] failed. Error is [%+v].", connectionKey, err)
					return
				}
			}

			case protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE: {
				err := protocol.Initialize2ClientReliablePushResponsePackage(actionName, messageId, payload, isCompressPushBody, writingBuffer)
				if err != nil {
					networkClient.processSend2ServerError(bizRequest, bizRequestWritingRetryChannel)
					networkClient.logger.Debug("Send push response to connection with key [%+v] failed. Error is [%+v].", connectionKey, err)
					return
				}
			}

			case protocol.LC_FUNC_CODE_CLIENT_2_SERVER_CALL_REQUEST: {
				err := protocol.Initialize2ClientLongConnCallRequestPackage(actionName, messageId, payload, isCompressPushBody, writingBuffer)
				if err != nil {
					networkClient.processSend2ServerError(bizRequest, bizRequestWritingRetryChannel)
					networkClient.logger.Debug("Send RPC request to connection with key [%+v] failed. Error is [%+v].", connectionKey, err)
					return
				}
			}

			default: {
				networkClient.logger.Debug("Send biz request to server with key [%+v] failed. Received wrong function code.", connectionKey)
				return
			}
		}

		networkClient.waitForSendNextPackage()
		_, err := conn.Write(writingBuffer.Bytes())
		if err != nil {
			networkClient.processSend2ServerError(bizRequest, bizRequestWritingRetryChannel)
			networkClient.logger.Debug("Send message to server with key [%+v] failed. Error is [%+v].", connectionKey, err)
			return
		}
	} else {
		networkClient.processSend2ServerError(bizRequest, bizRequestWritingRetryChannel)
		networkClient.logger.Debug("Connection was closed, message can not be sent to server.")
	}
}

func (networkClient *LongConnNetworkClient) retrySendBizRequest2ServerTask(bizRequestWritingRetryChannel1 chan *ForWritingBizRequest, bizRequestWritingRetryChannel2 chan *ForWritingBizRequest) {
	defer func() {
		if err := recover(); err != nil {
			networkClient.logger.Error("RetrySendBizRequest2ServerTask finished, No more request can be send to server. Error is %+v.", err)
		}
	}()

	for {
		networkClient.serviceLock.RLock()

		conn := networkClient.conn
		connectionKey := networkClient.connectionKey
		isCompressPushBody := networkClient.isCompressPushBody
		clientConfig := networkClient.clientConfig
		if conn == nil {
			networkClient.logger.Debug("Connection is disconnected. SendBizRequest2ServerTask finished. Connection Key : %+v", networkClient.connectionKey)
			networkClient.serviceLock.RUnlock()
			break
		}

		if connectionKey == NIL_CONNECTION_KEY {
			networkClient.serviceLock.RUnlock()
			continue
		}

		networkClient.serviceLock.RUnlock()

		bizRequest := <- bizRequestWritingRetryChannel1
		if bizRequest != nil {
			retryStartTime := bizRequest.retryStartTime
			lastRetryTime  := bizRequest.lastRetryTime
			if retryStartTime > 0 && lastRetryTime > 0 {
				currTime := utils.CurrentTimeMillis()
				retryTimeElapsedFromStart := currTime - retryStartTime
				retryTimeElapsedFromLast  := currTime - lastRetryTime
				if retryTimeElapsedFromStart >= clientConfig.MaxRetrySendMsgTimeInMillis {
					actionName := bizRequest.actionName
					messageId := bizRequest.messageId

					networkClient.logger.Error("Send message to server exchaused, exceed max retry time [%+v], will discard it, " +
						"Action Name [%+v], Connection Key [%+v], Message Id [%+v]", clientConfig.MaxRetrySendMsgTimeInMillis,
						actionName, connectionKey, messageId)
					continue
				} else {
					waitingTime := clientConfig.RetrySendMsgIntervalInMillis - retryTimeElapsedFromLast
					if waitingTime > FOR_WAITING_THRESHOLD {
						networkClient.logger.Debug("Going to sleep for retry in message retry channel, waiting time [%+v].", waitingTime)
						time.Sleep(time.Duration(waitingTime) * time.Millisecond)
					}
				}
			}

			networkClient.realSendBizRequestByFuncCode(bizRequest, isCompressPushBody, bizRequestWritingRetryChannel2, clientConfig)
		}
	}

	networkClient.logger.Debug("RetrySendBizRequest2ServerTask finished, No more request can be send to server.")
}

func (networkClient *LongConnNetworkClient) processSend2ServerError(bizRequest *ForWritingBizRequest, bizRequestWritingRetryChannel chan *ForWritingBizRequest) {
	currTime := utils.CurrentTimeMillis()
	if bizRequest.retryStartTime <= 0 {
		bizRequest.retryStartTime = currTime
	}

	bizRequest.lastRetryTime = currTime
	bizRequestWritingRetryChannel <- bizRequest
}

func (networkClient *LongConnNetworkClient) dynamicResponseActionCleaningTask() {
	defer func() {
		if err := recover(); err != nil {
			networkClient.isDynamicResponseActionCleaningRoutineRunning = false
			networkClient.logger.Info("Dynamic response action cleaning task finished. Error is %+v.", err)
		}
	}()

	networkClient.isDynamicResponseActionCleaningRoutineRunning = true

	for {
		networkClient.serviceLock.RLock()

		conn := networkClient.conn
		connectionKey := networkClient.connectionKey
		dynamicResponseActionRetentionTime := networkClient.clientConfig.DynamicResponseActionRetentionTimeInMillis
		if conn == nil {
			networkClient.logger.Error("Connection is disconnected. DynamicResponseActionCleaningTask finished. Connection Key : %+v", networkClient.connectionKey)
			networkClient.serviceLock.RUnlock()
			break
		}

		if connectionKey == NIL_CONNECTION_KEY {
			networkClient.serviceLock.RUnlock()
			continue
		}

		networkClient.serviceLock.RUnlock()

		time.Sleep(time.Duration(dynamicResponseActionRetentionTime) * time.Millisecond)

		networkClient.serviceLock.RLock()

		conn = networkClient.conn
		connectionKey = networkClient.connectionKey
		if conn == nil {
			networkClient.logger.Error("Connection is disconnected. DynamicResponseActionCleaningTask finished. Connection Key : %+v", networkClient.connectionKey)
			networkClient.serviceLock.RUnlock()
			break
		}

		if connectionKey == NIL_CONNECTION_KEY {
			networkClient.serviceLock.RUnlock()
			continue
		}

		dynamicResponseActions := networkClient.dynamicResponseActions
		responseActionKeys := dynamicResponseActions.Keys()

		for _, responseActionKey := range responseActionKeys {
			responseAction := dynamicResponseActions.Get(responseActionKey)
			if responseAction != nil {
				if responseActionInfo, ok := responseAction.(*DynamicResponseActionInfo); ok {
					actionCreatedTime := responseActionInfo.createdTime
					if (utils.CurrentTimeMillis() - actionCreatedTime) >= dynamicResponseActionRetentionTime {
						dynamicResponseActions.Remove(responseActionKey)
					}
				} else {
					dynamicResponseActions.Remove(responseActionKey)
					networkClient.logger.Info("Can not get a dynamic response action by key [%+v], romove from dynamic response actions map.", responseActionKey)
				}
			} else {
				dynamicResponseActions.Remove(responseActionKey)
				networkClient.logger.Info("Can not get a dynamic response action by key [%+v], romove from dynamic response actions map.", responseActionKey)
			}
		}

		networkClient.serviceLock.RUnlock()
	}

	networkClient.isDynamicResponseActionCleaningRoutineRunning = false
	networkClient.logger.Info("Dynamic response action cleaning task finished.")
}

func (networkClient *LongConnNetworkClient) RPC(rpcActionName string, rpcRequest []byte, timeout int64) (rpcResponse []byte, err error) {
	defer func() {
		if recover() != nil {
			rpcResponse = nil
			err = NetworkClientInternalError
		}
	}()

	if rpcRequest == nil || len(rpcRequest) <= 0 {
		return nil, RequestBodyIsNilError
	}

	networkClient.serviceLock.RLock()

	conn := networkClient.conn
	connectionKey := networkClient.connectionKey
	if conn == nil {
		networkClient.logger.Error("Connection is disconnected. Can not invoke RPC function. Connection Key : %+v, rpcActionName: %+v", connectionKey, rpcActionName)
		networkClient.serviceLock.RUnlock()
		return nil, ConnectionWasDisconnectedError
	}

	messageIdGenerator := networkClient.messageIdGenerator
	clientConfig := networkClient.clientConfig
	bizRequestWritingChannel := networkClient.bizRequestWritingChannel
	dynamicResponseActions := networkClient.dynamicResponseActions

	if timeout <= 0 {
		timeout = clientConfig.DefaultRpcTimeoutInMillis
	}

	msgId, err := messageIdGenerator.GetSequence()
	if err != nil {
		networkClient.logger.Error("Generate message Id failed. connKey: %+v, rpcActionName: %+v", connectionKey, rpcActionName)
		networkClient.serviceLock.RUnlock()
		return nil, err
	}

	networkClient.serviceLock.RUnlock()

	responseActionInfo := &DynamicResponseActionInfo{createdTime: utils.CurrentTimeMillis(), isWaitingForResponseReceived: true,
		isResponseReceived: false, responseBody: nil}
	responseActionInfo.responseReceivedCond = sync.NewCond(&responseActionInfo.responseReceivedLock)
	dynamicResponseActions.Put(msgId, responseActionInfo)

	bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_CLIENT_2_SERVER_CALL_REQUEST, actionName: rpcActionName,
		messageId: msgId, payload: rpcRequest, retryStartTime: -1, lastRetryTime: -1}
	bizRequestWritingChannel <- bizRequest

	isCheckingTimeout := false
	isTimeout := false

	responseActionInfo.responseReceivedLock.Lock()

	for ;!responseActionInfo.isResponseReceived; {
		if !isCheckingTimeout {
			go func() {
				time.Sleep(time.Duration(timeout) * time.Millisecond)
				dynamicResponseActions.Remove(msgId)

				responseActionInfo.responseReceivedLock.Lock()
				defer responseActionInfo.responseReceivedLock.Unlock()

				responseActionInfo.isResponseReceived = true
				isTimeout = true
				responseActionInfo.responseBody = nil
				responseActionInfo.responseReceivedCond.Broadcast()
			}()

			isCheckingTimeout = true
		}

		responseActionInfo.responseReceivedCond.Wait()
	}

	responseActionInfo.responseReceivedLock.Unlock()

	responseBody := responseActionInfo.responseBody
	if responseBody != nil {
		responseBodyStr := string(responseBody)
		if responseBodyStr == RPC_FAILED_CAN_NOT_FIND_ACTION {
			return nil, CanNotFindRpcActionError
		} else if responseBodyStr == RPC_FAILED_ACTION_ERROR {
			return nil, RpcActionResponseFailedError
		} else if responseBodyStr == RPC_FAILED_ACTION_NO_RESPONSE {
			return nil, RpcActionNoResponseError
		} else {
			return responseBody, nil
		}
	} else {
		if !isTimeout {
			return nil, nil
		} else {
			return nil, WaitForRpcResponseTimeoutError
		}
	}
}

func (networkClient *LongConnNetworkClient) AddRpcRequestActionIfAbsent(rpcActionName string, requestAction IRequestAction) {
	networkClient.namedRpcRequestActions.PutIfAbsent(rpcActionName, requestAction)
}

func (networkClient *LongConnNetworkClient) RemoveRpcRequestAction(rpcActionName string) {
	networkClient.namedRpcRequestActions.Remove(rpcActionName)
}

func (networkClient *LongConnNetworkClient) IsRpcRequestActionRegistered(rpcActionName string) bool {
	return networkClient.namedRpcRequestActions.Contains(rpcActionName)
}

func (networkClient *LongConnNetworkClient) SetConnectionChangedListener(listener IConnectionChangedListener) {
	networkClient.connectionChangedListener = listener
}





































