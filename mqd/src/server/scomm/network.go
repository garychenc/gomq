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
	"common/config"
	"common/protocol"
	"common/utils"
	"errors"
	"github.com/astaxie/beego/logs"
	"io"
	"net"
	"server/sutils"
	"sync"
	"time"
)

type INetworkServer interface {
	sutils.IServer

	GetServerStatisInfo() map[string]interface{}
	RPC(connKey int64, rpcActionName string, rpcRequest []byte, timeout int64) (rpcResponse []byte, err error)
	Push(connKey int64, pushedActionName string, pushedRequest []byte) error

	AddRequestAction(actionName string, action IRequestAction)
	RemoveRequestAction(actionName string)
	ClearRequestActions()

	AddConnectionChangedListener(listener IConnectionChangedListener)
	RemoveConnectionChangedListener(listener IConnectionChangedListener)
	ClearConnectionChangedListeners()
}

type IRequestAction interface {
	RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction MessageDeadAction, err error)
}

type IResponseAction interface {
	ResponseReceived(connKey int64, msgId int64, actionBody []byte) error
}

type MessageDeadAction func(connKey int64, msgId int64, msgBody []byte) bool

type IConnectionChangedListener interface {
	ConnectionAdded(connKey int64)
	ConnectionDisappeared(connKey int64)
}

type LongConnNetworkServer struct {
	// Final Fields, initialized by New function
	logger       *logs.BeeLogger
	serverConfig *LongConnNetworkServerConfig
	lifecycleLock sync.RWMutex
	namedRpcRequestActions utils.IConcurrentMap
	deviceConnectionKeys utils.IConcurrentMap
	connectionChangedListeners []IConnectionChangedListener
	connectionChangedListenersLock sync.Mutex

	//Statis Fields, initialized by New function
	processedDisconnectedRequestNumber *utils.LongAdder
	processedRegisterRequestNumber *utils.LongAdder
	sentServerLineTestRequestNumber *utils.LongAdder
	receivedKeepAliveRequestNumber *utils.LongAdder
	receivedServerLineTestResponseNumber *utils.LongAdder
	receivedButDiscardedRpcRequestNumber *utils.LongAdder
	receivedButDiscardedRpcResponseNumber *utils.LongAdder
	receivedRpcResponseNumber *utils.LongAdder
	receivedRpcRequestNumber *utils.LongAdder
	requestRetryNumber *utils.LongAdder
	sentRpcResponseNumber *utils.LongAdder
	sentRpcRequestNumber *utils.LongAdder
	pushedReliableRequestNumber *utils.LongAdder
	writingRequestRetryNumber *utils.LongAdder
	writingRequestDeadNumber *utils.LongAdder
	receivedReliablePushResponseNumber *utils.LongAdder

	// Changable Files, initialized by Start function
	serverStatus byte
	listeningTcpAddr *net.TCPAddr
	listener *net.TCPListener
	myServerDeployAddress string

	messageIdGenerator *utils.SequenceGenerator
	connectionKeyGenerator *utils.SequenceGenerator
	registeredConnections utils.IConcurrentMap
	dynamicResponseActions utils.IConcurrentMap

	receivedDataAssembleChannel chan *ReadedDataBlock
	bizRequestWritingChannel chan *ForWritingBizRequest
	bizRequestWritingRetryChannel1 chan *ForWritingBizRequest
	bizRequestWritingRetryChannel2 chan *ForWritingBizRequest
	reliablePushedRequest1stBufferChannel chan *ForWritingBizRequest
	reliablePushedRequest2ndBufferChannel chan *ForWritingBizRequest
}

type LongConnNetworkServerConfig struct {
	LogConfig *config.LogConfig

	// Socket Config
	ServerListeningAddress               string
	SocketKeepAlivePeriodInSec           int64
	SocketReadingBufferSizeInBytes       int
	SocketWritingBufferSizeInBytes       int
	SocketWaitingForRegisterTimeInMillis int64

	// Data Buffer Channel Config
	ReceivedDataAssembleChannelSize           int
	BizRequestWritingChannelSize              int
	ReliablePushedRequest1stBufferChannelSize int
	ReliablePushedRequest2ndBufferChannelSize int

	// Flow Control Config
	SendNextPackageWaitingTimeInMillis int64

	// Register Config
	SendHeartbeatIntervalInMillis               int32
	NoHeartbeatResponseTimeForDeathConnInMillis int32
	ReconnectIntervalInMillis                   int32
	IsCompressPushBody                          bool
	IsNeedSendServerLineTestAfterRegister       bool

	// Message Reliable Sending Config
	MaxRetrySendMsgTimeInMillis                    int64
	RetrySendMsgIntervalInMillis                   int64
	WritingBizRequestConsumerCount                 int
	PushedRequest1stBufferQueueConsumerCount       int
	PushedRequest2ndBufferQueueConsumerCount       int
	DefaultRpcTimeoutInMillis                      int64
	DeterminatePushResponseReceivedTimeoutInMillis int64

	// Death Connection and Cache Cleaning Config
	DeathConnectionScanningIntervalInMillis    int64
	NoneActiveTimeForDeathConnectionInMillis   int64
	SendServerLineTestRequestIntervalInMillis  int64
	DynamicResponseActionRetentionTimeInMillis int64
}

func NewLongConnNetworkServerConfig() *LongConnNetworkServerConfig {
	return &LongConnNetworkServerConfig{}
}

func NewLongConnNetworkServerDefaultConfig() *LongConnNetworkServerConfig {
	serverConfig := &LongConnNetworkServerConfig{}

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 3000000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true

	serverConfig.LogConfig = logConfig

	serverConfig.SocketKeepAlivePeriodInSec = 2
	serverConfig.SocketReadingBufferSizeInBytes = 16 * 1024
	serverConfig.SocketWritingBufferSizeInBytes = 16 * 1024
	serverConfig.SocketWaitingForRegisterTimeInMillis = 10000

	serverConfig.ReceivedDataAssembleChannelSize = 2000000
	serverConfig.BizRequestWritingChannelSize = 2000000
	serverConfig.ReliablePushedRequest1stBufferChannelSize = 2000000
	serverConfig.ReliablePushedRequest2ndBufferChannelSize = 2000000

	serverConfig.SendNextPackageWaitingTimeInMillis = -1

	serverConfig.SendHeartbeatIntervalInMillis = 5000
	serverConfig.NoHeartbeatResponseTimeForDeathConnInMillis = 30000
	serverConfig.ReconnectIntervalInMillis = 5000
	serverConfig.IsCompressPushBody = false
	serverConfig.IsNeedSendServerLineTestAfterRegister = false

	serverConfig.MaxRetrySendMsgTimeInMillis = 60000
	serverConfig.RetrySendMsgIntervalInMillis = 5000
	serverConfig.WritingBizRequestConsumerCount = 22
	serverConfig.PushedRequest1stBufferQueueConsumerCount = 20
	serverConfig.PushedRequest2ndBufferQueueConsumerCount = 15
	serverConfig.DefaultRpcTimeoutInMillis = 60000
	serverConfig.DeterminatePushResponseReceivedTimeoutInMillis = 5000

	serverConfig.DeathConnectionScanningIntervalInMillis = 180000
	serverConfig.NoneActiveTimeForDeathConnectionInMillis = 600000
	serverConfig.SendServerLineTestRequestIntervalInMillis = 60000
	serverConfig.DynamicResponseActionRetentionTimeInMillis = 180000

	return serverConfig
}

func NewLongConnNetworkServer(serverConfig *LongConnNetworkServerConfig) INetworkServer {
	networkServer := &LongConnNetworkServer{}
	networkServer.logger = logs.NewLogger(serverConfig.LogConfig.AsyncLogChanLength)
	networkServer.logger.SetLevel(serverConfig.LogConfig.Level)
	networkServer.logger.SetLogger(serverConfig.LogConfig.AdapterName, serverConfig.LogConfig.AdapterParameters)
	networkServer.logger.SetLogger(logs.AdapterConsole, "{}")
	if serverConfig.LogConfig.IsAsync {
		networkServer.logger.Async(serverConfig.LogConfig.AsyncLogChanLength)
	}

	networkServer.serverConfig = serverConfig
	networkServer.namedRpcRequestActions = utils.NewConcurrentMapWithSize(512)
	networkServer.deviceConnectionKeys = utils.NewConcurrentMapWithSize(8 * 1024)
	networkServer.connectionChangedListeners = make([]IConnectionChangedListener, 0, 32)

	networkServer.processedDisconnectedRequestNumber = utils.NewLongAdder(0)
	networkServer.processedRegisterRequestNumber = utils.NewLongAdder(0)
	networkServer.sentServerLineTestRequestNumber = utils.NewLongAdder(0)
	networkServer.receivedKeepAliveRequestNumber = utils.NewLongAdder(0)
	networkServer.receivedServerLineTestResponseNumber = utils.NewLongAdder(0)
	networkServer.receivedButDiscardedRpcRequestNumber = utils.NewLongAdder(0)
	networkServer.receivedButDiscardedRpcResponseNumber = utils.NewLongAdder(0)
	networkServer.receivedRpcResponseNumber = utils.NewLongAdder(0)
	networkServer.receivedRpcRequestNumber = utils.NewLongAdder(0)
	networkServer.requestRetryNumber = utils.NewLongAdder(0)
	networkServer.sentRpcResponseNumber = utils.NewLongAdder(0)
	networkServer.sentRpcRequestNumber = utils.NewLongAdder(0)
	networkServer.pushedReliableRequestNumber = utils.NewLongAdder(0)
	networkServer.writingRequestRetryNumber = utils.NewLongAdder(0)
	networkServer.writingRequestDeadNumber = utils.NewLongAdder(0)
	networkServer.receivedReliablePushResponseNumber = utils.NewLongAdder(0)

	return networkServer
}

var (
	NetworkServerConfigIsNilError = errors.New("server config is nil error")
	RequestBodyIsNilError = errors.New("request body is nil error")
	LongConnServerWasStoppedError = errors.New("long connection server was stopped error")
	CanNotGetConnByConnKeyError = errors.New("can not get connection error")
	WaitForRpcResponseTimeoutError = errors.New("wait for rpc response timeout error")
	NetworkServerInternalError = errors.New("server internal error")
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

type SocketConnection struct {
	cacheLinePadding1 [utils.CACHE_LINE_SIZE]byte

	writingLock sync.Mutex
	writingBuffer *utils.IoBuffer
	readingBufferBytes []byte
	connectedSocket *net.TCPConn
	packageAssembler protocol.IPackageAssemble
	packageCompletedListener protocol.IPackageCompletedListener

	connectionAcceptedTime int64
	connectionRegistedKey int64
	isConnectionResetByPeer bool
	connectionRegistedTime int64
	connectionLastSendPkgTime int64
	connectionLastHeartbeatTime int64
	connectionDisconnectedTime int64

	cacheLinePadding2 [utils.CACHE_LINE_SIZE]byte
}

type ReadedDataBlock struct {
	readedBuffer *utils.IoBuffer
	packageAssembler protocol.IPackageAssemble
	packageCompletedListener protocol.IPackageCompletedListener
}

type ForWritingBizRequest struct {
	cacheLinePadding1 [utils.CACHE_LINE_SIZE]byte

	funcCode byte
	conn *SocketConnection
	actionName string
	messageId int64
	payload []byte
	messageDeadAction MessageDeadAction

	retryStartTime int64
	lastRetryTime int64

	putInto1stBufferChannelTime int64
	putInto2ndBufferChannelTime int64

	cacheLinePadding2 [utils.CACHE_LINE_SIZE]byte
}

type DynamicResponseActionInfo struct {
	cacheLinePadding1 [utils.CACHE_LINE_SIZE]byte

	// Final Fields
	responseAction IResponseAction
	createdTime int64

	// Changable Fields, Need lock
	isWaitingForResponseReceived bool
	isPushRequest bool
	isResponseReceived bool
	responseBody []byte
	responseReceivedCond *sync.Cond
	responseReceivedLock sync.Mutex

	cacheLinePadding2 [utils.CACHE_LINE_SIZE]byte
}

type PackageReadingCompletedListenerImpl struct {
	conn *SocketConnection
	networkServer *LongConnNetworkServer
}

func (networkServer *LongConnNetworkServer) Start() error {
	networkServer.lifecycleLock.Lock()
	defer networkServer.lifecycleLock.Unlock()

	if networkServer.serverStatus == sutils.Started {
		networkServer.logger.Info("Long Connection Network Server Already Started.")
		return nil
	}

	serverConfig := networkServer.serverConfig
	if serverConfig == nil {
		networkServer.logger.Error("Can not get LongConnNetworkServerConfig object, Start failed.")
		return NetworkServerConfigIsNilError
	}

	networkServer.serverStatus = sutils.Starting
	networkServer.logger.Info("Long connection network server is starting.")

	connKeyGen, err := utils.NewSequenceGenerator()
	if err != nil {
		networkServer.logger.Error("Create connection key sequence generator failed, Error is : %+v.", err)
		return err
	}

	msgIdGen, err := utils.NewSequenceGenerator()
	if err != nil {
		networkServer.logger.Error("Create message id sequence generator failed, Error is : %+v.", err)
		return err
	}

	networkServer.connectionKeyGenerator = connKeyGen
	networkServer.messageIdGenerator = msgIdGen
	networkServer.registeredConnections = utils.NewConcurrentMapWithSize(8 * 1024)
	networkServer.dynamicResponseActions = utils.NewConcurrentMapWithSize(16 * 1024)

	networkServer.receivedDataAssembleChannel = make(chan *ReadedDataBlock, serverConfig.ReceivedDataAssembleChannelSize)
	networkServer.bizRequestWritingChannel = make(chan *ForWritingBizRequest, serverConfig.BizRequestWritingChannelSize)
	networkServer.bizRequestWritingRetryChannel1 = make(chan *ForWritingBizRequest, serverConfig.BizRequestWritingChannelSize)
	networkServer.bizRequestWritingRetryChannel2 = make(chan *ForWritingBizRequest, serverConfig.BizRequestWritingChannelSize)
	networkServer.reliablePushedRequest1stBufferChannel = make(chan *ForWritingBizRequest, serverConfig.ReliablePushedRequest1stBufferChannelSize)
	networkServer.reliablePushedRequest2ndBufferChannel = make(chan *ForWritingBizRequest, serverConfig.ReliablePushedRequest2ndBufferChannelSize)

	networkServer.logger.Info("All channels created.")

	writingBizRequestConsumerCount := serverConfig.WritingBizRequestConsumerCount
	writingBizRequestRetryConsumerCount := writingBizRequestConsumerCount / 2
	if writingBizRequestRetryConsumerCount <= 0 {
		writingBizRequestRetryConsumerCount = 1
	}

	pushedRequest1stBufferQueueConsumerCount := serverConfig.PushedRequest1stBufferQueueConsumerCount
	pushedRequest2ndBufferQueueConsumerCount := serverConfig.PushedRequest2ndBufferQueueConsumerCount

	for i := 0; i < writingBizRequestConsumerCount; i++ {
		go networkServer.sendBizRequest2ClientTask()
	}

	networkServer.logger.Info("Consumers for channel [BizRequestWritingChannel] are ready, consumer count [%+v].", writingBizRequestConsumerCount)

	for i := 0; i < writingBizRequestRetryConsumerCount; i++ {
		go networkServer.retrySendBizRequest2ClientTask(networkServer.bizRequestWritingRetryChannel1, networkServer.bizRequestWritingRetryChannel2)
		go networkServer.retrySendBizRequest2ClientTask(networkServer.bizRequestWritingRetryChannel2, networkServer.bizRequestWritingRetryChannel1)
	}

	networkServer.logger.Info("Consumers for channel [BizRequestWritingRetryChannel] are ready, consumer count [%+v].", writingBizRequestRetryConsumerCount)

	for i := 0; i < pushedRequest1stBufferQueueConsumerCount; i++ {
		go networkServer.process1stBufferQueueTask()
	}

	networkServer.logger.Info("Consumers for channel [ReliablePushedRequest1stBufferChannel] are ready, consumer count [%+v].", pushedRequest1stBufferQueueConsumerCount)

	for i := 0; i < pushedRequest2ndBufferQueueConsumerCount; i++ {
		go networkServer.process2ndBufferQueueTask()
	}

	networkServer.logger.Info("Consumers for channel [ReliablePushedRequest2ndBufferChannel] are ready, consumer count [%+v].", pushedRequest2ndBufferQueueConsumerCount)

	go networkServer.deathConnectionCleaningTask()
	networkServer.logger.Info("Death connection cleaning task is running.")

	go networkServer.dynamicResponseActionCleaningTask()
	networkServer.logger.Info("Dynamic response action cleaning task is running.")

	tcpAddr, err := net.ResolveTCPAddr("tcp", serverConfig.ServerListeningAddress)
	if err != nil {
		networkServer.logger.Error("Resolve server listening address failed. Server Listening Network : %+v." +
			" Server Listening Address : %+v, Error is : %+v.", "tcp",
			serverConfig.ServerListeningAddress, err)
		return err
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		networkServer.logger.Error("Listen on TCP address failed. Server Listening Network : %+v." +
			" Server Listening Address : %+v, Error is : %+v.", "tcp",
			serverConfig.ServerListeningAddress, err)
		return err
	}

	networkServer.listeningTcpAddr = tcpAddr
	networkServer.listener = listener
	networkServer.myServerDeployAddress = tcpAddr.String()

	go networkServer.keepPackageAssemblingTask()
	go networkServer.keepAcceptingConnectionsTask()

	networkServer.serverStatus = sutils.Started
	networkServer.logger.Info("Server socket is binding on address [%+v], waiting client to connect.", tcpAddr.String())
	networkServer.logger.Info("Long connection network server is started successfully. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	return nil
}

func (networkServer *LongConnNetworkServer) Stop() error {
	networkServer.lifecycleLock.Lock()
	defer networkServer.lifecycleLock.Unlock()

	if networkServer.serverStatus == sutils.Stopped {
		networkServer.logger.Info("Long Connection Network Server Already Stopped.")
		return nil
	}

	networkServer.serverStatus = sutils.Stopping
	networkServer.logger.Info("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)

	listener := networkServer.listener
	if listener != nil {
		err := listener.Close()
		if err != nil {
			networkServer.logger.Info("Close TCP listener failed. Error is : %+v", err)
		}

		networkServer.logger.Info("Long connection network server is stopping. Listening server socket is stopped. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}

	registeredConnections := networkServer.registeredConnections
	if registeredConnections != nil {
		registeredConnectionKeys := registeredConnections.Keys()
		for _, registeredConnKey := range registeredConnectionKeys {
			conn := registeredConnections.Get(registeredConnKey)
			if conn != nil {
				if socketConn, ok := conn.(*SocketConnection); ok {
					socketConn.writingLock.Lock()

					registeredConnections.Remove(registeredConnKey)
					networkServer.closeTcpSocket(socketConn.connectedSocket)
					networkServer.logger.Debug("Long connection network server is stopping. A registered long connection with key [%+v] was closed, remove from connection registered map. MyServerAddress [%+v].", registeredConnKey, networkServer.myServerDeployAddress)

					socketConn.writingLock.Unlock()
				} else {
					registeredConnections.Remove(registeredConnKey)
				}
			} else {
				registeredConnections.Remove(registeredConnKey)
			}
		}
	}

	dynamicResponseActions := networkServer.dynamicResponseActions
	if dynamicResponseActions != nil {
		dynamicResponseActions.Clear()
	}

	receivedDataAssembleChannel := networkServer.receivedDataAssembleChannel
	bizRequestWritingChannel := networkServer.bizRequestWritingChannel
	bizRequestWritingRetryChannel1 := networkServer.bizRequestWritingRetryChannel1
	bizRequestWritingRetryChannel2 := networkServer.bizRequestWritingRetryChannel2
	reliablePushedRequest1stBufferChannel := networkServer.reliablePushedRequest1stBufferChannel
	reliablePushedRequest2ndBufferChannel := networkServer.reliablePushedRequest2ndBufferChannel

	if reliablePushedRequest2ndBufferChannel != nil {
		close(reliablePushedRequest2ndBufferChannel)
		networkServer.reliablePushedRequest2ndBufferChannel = nil
		networkServer.logger.Info("Long connection network server is stopping. ReliablePushedRequest2ndBufferChannel is closed. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}

	if reliablePushedRequest1stBufferChannel != nil {
		close(reliablePushedRequest1stBufferChannel)
		networkServer.reliablePushedRequest1stBufferChannel = nil
		networkServer.logger.Info("Long connection network server is stopping. ReliablePushedRequest1stBufferChannel is closed. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}

	if bizRequestWritingRetryChannel1 != nil {
		close(bizRequestWritingRetryChannel1)
		networkServer.bizRequestWritingRetryChannel1 = nil
		networkServer.logger.Info("Long connection network server is stopping. BizRequestWritingRetryChannel1 is closed. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}

	if bizRequestWritingRetryChannel2 != nil {
		close(bizRequestWritingRetryChannel2)
		networkServer.bizRequestWritingRetryChannel2 = nil
		networkServer.logger.Info("Long connection network server is stopping. BizRequestWritingRetryChannel2 is closed. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}

	if bizRequestWritingChannel != nil {
		close(bizRequestWritingChannel)
		networkServer.bizRequestWritingChannel = nil
		networkServer.logger.Info("Long connection network server is stopping. BizRequestWritingChannel is closed. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}

	if receivedDataAssembleChannel != nil {
		close(receivedDataAssembleChannel)
		networkServer.receivedDataAssembleChannel = nil
		networkServer.logger.Info("Long connection network server is stopping. ReceivedDataAssembleChannel is closed. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}

	networkServer.serverStatus = sutils.Stopped
	networkServer.logger.Info("Long connection network server is stopped successfully.")

	return nil
}

func (networkServer *LongConnNetworkServer) keepAcceptingConnectionsTask() {
	defer func() {
		if err := recover(); err != nil {
			networkServer.logger.Info("Accept task finished. No more connection can connect to this server. MyServerAddress [%+v]. Error is %+v.", networkServer.myServerDeployAddress, err)
		}
	}()

	for {
		networkServer.lifecycleLock.RLock()

		lServerStatus := networkServer.serverStatus
		if lServerStatus == sutils.Stopping ||
			lServerStatus == sutils.Stopped {

			networkServer.logger.Info("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			networkServer.lifecycleLock.RUnlock()
			break
		}

		if lServerStatus != sutils.Started {

			networkServer.lifecycleLock.RUnlock()
			continue
		}

		lTCPListener  := networkServer.listener
		lServerConfig := networkServer.serverConfig

		networkServer.lifecycleLock.RUnlock()

		acceptedSocket, err := lTCPListener.AcceptTCP()
		if err != nil {
			networkServer.closeTcpSocket(acceptedSocket)
			networkServer.logger.Error("Accept failed on listener. Ignore this accept and go on next accept. MyServerAddress [" + networkServer.myServerDeployAddress + "]. Error is : %+v", err)
		} else {
			acceptedSocket.SetKeepAlive(true)
			acceptedSocket.SetKeepAlivePeriod(time.Duration(lServerConfig.SocketKeepAlivePeriodInSec * int64(time.Second)))
			acceptedSocket.SetNoDelay(true)
			acceptedSocket.SetReadBuffer(lServerConfig.SocketReadingBufferSizeInBytes)
			acceptedSocket.SetWriteBuffer(lServerConfig.SocketWritingBufferSizeInBytes)

			writingBufferBytes := make([]byte, 0, lServerConfig.SocketWritingBufferSizeInBytes)
			writingBuffer := utils.NewIoBuffer(writingBufferBytes)
			readingBufferBytes := make([]byte, lServerConfig.SocketReadingBufferSizeInBytes)
			packageAssembler := protocol.NewPackageAssemble(lServerConfig.LogConfig)

			acceptedConn := &SocketConnection{connectedSocket: acceptedSocket, writingBuffer: writingBuffer,
				readingBufferBytes: readingBufferBytes, connectionAcceptedTime: utils.CurrentTimeMillis(),
			    packageAssembler: packageAssembler, connectionRegistedKey: NIL_CONNECTION_KEY, isConnectionResetByPeer: false,
				connectionRegistedTime: -1, connectionLastSendPkgTime: -1, connectionLastHeartbeatTime: -1, connectionDisconnectedTime: -1}

			acceptedConn.packageCompletedListener = &PackageReadingCompletedListenerImpl{conn: acceptedConn, networkServer: networkServer}

			go networkServer.readDataFromConnectionTask(acceptedConn)
			go networkServer.acceptedSocketCheckingTask(acceptedConn)
			networkServer.logger.Info("## A socket with remote address [%+v] was accepted ##. MyServerAddress [%+v].", acceptedSocket.RemoteAddr().String(), networkServer.myServerDeployAddress)
		}
	}

	networkServer.logger.Info("Accept task finished. No more connection can connect to this server. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
}

func (networkServer *LongConnNetworkServer) keepPackageAssemblingTask() {
	defer func() {
		if err := recover(); err != nil {
			networkServer.logger.Info("Package assemble task finished. MyServerAddress [%+v]. Error is %+v.", networkServer.myServerDeployAddress, err)
		}
	}()

	for {
		networkServer.lifecycleLock.RLock()

		lServerStatus := networkServer.serverStatus
		if lServerStatus == sutils.Stopping ||
			lServerStatus == sutils.Stopped {

			networkServer.logger.Info("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			networkServer.lifecycleLock.RUnlock()
			break
		}

		if lServerStatus != sutils.Started {

			networkServer.lifecycleLock.RUnlock()
			continue
		}

		receivedDataAssembleChannel := networkServer.receivedDataAssembleChannel

		networkServer.lifecycleLock.RUnlock()

		readedData := <- receivedDataAssembleChannel
		if readedData != nil {
			readedData.packageAssembler.Assemble2CompletePackage(readedData.readedBuffer, readedData.packageCompletedListener)
		}
	}

	networkServer.logger.Info("Package assemble task finished. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
}

func (networkServer *LongConnNetworkServer) readDataFromConnectionTask(acceptedConn *SocketConnection) {
	defer func() {
		if err := recover(); err != nil {
			networkServer.logger.Error("Reading task finished. No more connection data can be readed and processed. Connection Key : [%+v], MyServerAddress [%+v]. Error is %+v.", acceptedConn.connectionRegistedKey, networkServer.myServerDeployAddress, err)
		}
	}()

	readErrorTimes := 0

	for {
		networkServer.lifecycleLock.RLock()

		lServerStatus := networkServer.serverStatus
		if lServerStatus == sutils.Stopping ||
			lServerStatus == sutils.Stopped {

			networkServer.logger.Debug("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			networkServer.lifecycleLock.RUnlock()
			break
		}

		if lServerStatus != sutils.Started {

			networkServer.lifecycleLock.RUnlock()
			continue
		}

		receivedDataAssembleChannel := networkServer.receivedDataAssembleChannel
		connectedSocket := acceptedConn.connectedSocket
		readingBufferBytes := acceptedConn.readingBufferBytes
		packageAssembler := acceptedConn.packageAssembler
		packageCompletedListener := acceptedConn.packageCompletedListener

		networkServer.lifecycleLock.RUnlock()

		readedCount, err := connectedSocket.Read(readingBufferBytes)
		if readedCount > 0 {
			readedBytes := make([]byte, readedCount)
			copy(readedBytes, readingBufferBytes)
			readedBuffer := utils.NewIoBuffer(readedBytes)
			readedData := &ReadedDataBlock{readedBuffer: readedBuffer, packageAssembler: packageAssembler, packageCompletedListener: packageCompletedListener}
			receivedDataAssembleChannel <- readedData
			readErrorTimes = 0
		} else {
			if err != nil {
				if err == io.EOF {
					networkServer.connResetByPeer(acceptedConn)
					break
				} else if opError, ok := err.(*net.OpError); ok {
					remoteAddress := connectedSocket.RemoteAddr()
					if opError.Timeout() {
						networkServer.logger.Info("Read timeout and no data readed. RemoteAddress : [%+v], Connection Key : [%+v], MyServerAddress [%+v].", remoteAddress.String(), acceptedConn.connectionRegistedKey, networkServer.myServerDeployAddress)
					} else {
						if readErrorTimes < 8 {
							networkServer.logger.Debug("Read data from socket encountered an error, but go on another reading. RemoteAddress : [%+v], Connection Key : [%+v], MyServerAddress [%+v]. Error is %+v.", remoteAddress.String(), acceptedConn.connectionRegistedKey, networkServer.myServerDeployAddress, err)
							readErrorTimes++
						} else {
							networkServer.logger.Error("Reading data task encountered severe error, go into EOF process. RemoteAddress : [%+v], Connection Key : [%+v], MyServerAddress [%+v]. Error is %+v.", remoteAddress.String(), acceptedConn.connectionRegistedKey, networkServer.myServerDeployAddress, err)
							networkServer.connResetByPeer(acceptedConn)
							break
						}
					}
				} else {
					remoteAddress := connectedSocket.RemoteAddr()
					if readErrorTimes < 8 {
						networkServer.logger.Debug("Read data from socket encountered an error, but go on another reading. RemoteAddress : [%+v], Connection Key : [%+v], MyServerAddress [%+v]. Error is %+v.", remoteAddress.String(), acceptedConn.connectionRegistedKey, networkServer.myServerDeployAddress, err)
						readErrorTimes++
					} else {
						networkServer.logger.Error("Reading data task encountered severe error, go into EOF process. RemoteAddress : [%+v], Connection Key : [%+v], MyServerAddress [%+v]. Error is %+v.", remoteAddress.String(), acceptedConn.connectionRegistedKey, networkServer.myServerDeployAddress, err)
						networkServer.connResetByPeer(acceptedConn)
						break
					}
				}
			} else {
				readErrorTimes = 0
			}
		}
	}

	remoteAddress := acceptedConn.connectedSocket.RemoteAddr()
	networkServer.logger.Debug("Reading task finished. No more connection data can be readed and processed. RemoteAddress : [%+v], Connection Key : [%+v], MyServerAddress [%+v].", remoteAddress.String(), acceptedConn.connectionRegistedKey, networkServer.myServerDeployAddress)
}

func (networkServer *LongConnNetworkServer) connResetByPeer(acceptedConn *SocketConnection) {
	acceptedConn.writingLock.Lock()

	acceptedConn.isConnectionResetByPeer = true
	acceptedConn.connectionDisconnectedTime = utils.CurrentTimeMillis()
	remoteAddress := acceptedConn.connectedSocket.RemoteAddr()
	networkServer.logger.Error("Connection closed by peer, EOF. RemoteAddress : [%+v], Connection Key : [%+v], MyServerAddress [%+v].", remoteAddress.String(), acceptedConn.connectionRegistedKey, networkServer.myServerDeployAddress)
	networkServer.closeSocketConnectionAndIndicateDisconnected(acceptedConn)

	acceptedConn.writingLock.Unlock()
	networkServer.processedDisconnectedRequestNumber.Increment()
}

func (networkServer *LongConnNetworkServer) closeSocketConnectionAndIndicateDisconnected(acceptedConn *SocketConnection) {
	networkServer.closeTcpSocket(acceptedConn.connectedSocket)
}

func (networkServer *LongConnNetworkServer) acceptedSocketCheckingTask(acceptedConn *SocketConnection)  {
	taskSubmitedTimestamp := utils.CurrentTimeMillis()
	lConnectedSocket := acceptedConn.connectedSocket
	lServerConfig := networkServer.serverConfig

	if lServerConfig != nil && lConnectedSocket != nil {
		socketWaitingForRegisterTime := lServerConfig.SocketWaitingForRegisterTimeInMillis
		forWaitingTime := socketWaitingForRegisterTime - (utils.CurrentTimeMillis() - taskSubmitedTimestamp)
		if forWaitingTime > FOR_WAITING_THRESHOLD {
			time.Sleep(time.Duration(forWaitingTime) * time.Millisecond)
		}

		acceptedConn.writingLock.Lock()

		if acceptedConn.connectionRegistedKey == NIL_CONNECTION_KEY {
			// unregister after waiting
			remoteAddress := lConnectedSocket.RemoteAddr()
			networkServer.closeTcpSocket(lConnectedSocket)
			acceptedConn.isConnectionResetByPeer = true
			networkServer.logger.Error("An accepted socket timeout for register. Connection was discarded. Socket Remote Address : %+v. MyServerAddress [%+v].", remoteAddress.String(), networkServer.myServerDeployAddress)
		}

		acceptedConn.writingLock.Unlock()
	}
	// Else, server is stopping
}

func (networkServer *LongConnNetworkServer) closeTcpSocket(tcpSocket *net.TCPConn) {
	if  tcpSocket != nil {
		tcpSocket.Close()
	}
}

func (listener *PackageReadingCompletedListenerImpl) OnCompleted(completedPackage *utils.IoBuffer) error {
	conn := listener.conn
	networkServer := listener.networkServer

	conn.writingLock.Lock()

	decodedPackage := protocol.ValueOf(completedPackage, conn.connectionRegistedKey)
	functionCode := decodedPackage.FunctionCode()

	switch functionCode {

		case protocol.LC_FUNC_CODE_REGISTER_REQUEST: {
			go networkServer.responseRegisterTask(conn, decodedPackage)
		}

		case protocol.LC_FUNC_CODE_HEART_BEAT_REQUEST: {
			if conn.connectionRegistedKey != NIL_CONNECTION_KEY {
				go networkServer.responseClientHeartbeatTask(conn, decodedPackage)
			}
		}

		case protocol.LC_FUNC_CODE_LINE_TEST_RESPONSE: {
			if conn.connectionRegistedKey != NIL_CONNECTION_KEY {
				go networkServer.clientResponseServerLineTestTask(conn, decodedPackage)
			}
		}

		case  protocol.LC_FUNC_CODE_CLIENT_2_SERVER_CALL_REQUEST: {
			if conn.connectionRegistedKey != NIL_CONNECTION_KEY {
				go networkServer.processClient2ServerBizRequestTask(conn, decodedPackage)
			}
		}

		case  protocol.LC_FUNC_CODE_CLIENT_2_SERVER_CALL_RESPONSE,
			  protocol.LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE: {
			if conn.connectionRegistedKey != NIL_CONNECTION_KEY {
				go networkServer.processClient2ServerBizResponseTask(conn, decodedPackage)
			}
		}

		default: {
			if conn.connectionRegistedKey != NIL_CONNECTION_KEY {
				remoteAddress := conn.connectedSocket.RemoteAddr()
				networkServer.logger.Error("Receive an illegal long connection package. Socket Remote Address : %+v, Function Code : %+v, MyServerAddress [%+v].", remoteAddress.String(), functionCode, networkServer.myServerDeployAddress)
			}
		}

	}

	conn.writingLock.Unlock()
	return nil
}

func (networkServer *LongConnNetworkServer) responseRegisterTask(conn *SocketConnection, registerRequest *protocol.WiredPackage)  {
	if conn != nil {
		lConnectedSocket := conn.connectedSocket
		lServerConfig := networkServer.serverConfig
		registeredConnections := networkServer.registeredConnections

		if lServerConfig != nil && lConnectedSocket != nil {
			conn.writingLock.Lock()

			if !conn.isConnectionResetByPeer {

				connectionKey, registerResponseCode := networkServer.checkIsRegisterRequestLegal(registerRequest)
				if registerResponseCode == REGISTER_CHECKING_RESULT_LEGAL {

					registerRequest.SetConnKey(connectionKey)
					conn.writingBuffer.Reset()
					err := protocol.Initialize2LongConnRegisterResponsePackage(registerResponseCode, connectionKey,
						lServerConfig.SendHeartbeatIntervalInMillis, lServerConfig.NoHeartbeatResponseTimeForDeathConnInMillis, lServerConfig.ReconnectIntervalInMillis,
						false, true, lServerConfig.IsCompressPushBody, networkServer.myServerDeployAddress,
						nil, conn.writingBuffer)
					if err != nil {
						networkServer.registerErrorProcess(conn, err)
						return
					}

					networkServer.waitForSendNextPackage(conn)
					_, err = lConnectedSocket.Write(conn.writingBuffer.Bytes())
					if err != nil {
						networkServer.registerErrorProcess(conn, err)
						return
					}

					conn.connectionRegistedTime = utils.CurrentTimeMillis()
					conn.connectionRegistedKey = connectionKey

					oldConn, _ := registeredConnections.Put(connectionKey, conn)
					if oldConn != nil {
						if oldSocketConnection, ok := oldConn.(*SocketConnection); ok {
							oldSocketConnection.writingLock.Lock()

							networkServer.closeTcpSocket(oldSocketConnection.connectedSocket)
							oldSocketConnection.isConnectionResetByPeer = true
							networkServer.logger.Info("An already registered long connection with key [%+v] was replaced, remove it. MyServerAddress [%+v].", connectionKey, networkServer.myServerDeployAddress)

							oldSocketConnection.writingLock.Unlock()
						}
					}

					remoteAddress := lConnectedSocket.RemoteAddr()
					networkServer.logger.Info("## A connection with connection key [%+v] and socket remote address [%+v] was registered. MyServerAddress [%+v].##", connectionKey, remoteAddress.String(), networkServer.myServerDeployAddress)

					if lServerConfig.IsNeedSendServerLineTestAfterRegister {
						go networkServer.sendServerLineTestTask(conn)
					}

					networkServer.processedRegisterRequestNumber.Increment()
					networkServer.notifyConnAdded(connectionKey)
				} else {
					networkServer.logger.Error("## Check register request failed. The response code is %+v. MyServerAddress [%+v]. ##", registerResponseCode, networkServer.myServerDeployAddress)
					networkServer.closeTcpSocket(conn.connectedSocket)
					conn.isConnectionResetByPeer = true
				}

			} else {
				registerWaitTime := utils.CurrentTimeMillis() - conn.connectionAcceptedTime
				networkServer.logger.Error("## Can not get a valid long connection, connection closed. No register response was sent. register task wait time: %+v. MyServerAddress [%+v]. ##", registerWaitTime, networkServer.myServerDeployAddress)
			}

			conn.writingLock.Unlock()
		} else {
			// Else, server is stopping
			networkServer.logger.Error("## Long connection server is stopping. No register response was sent. MyServerAddress [%+v]. ##", networkServer.myServerDeployAddress)
		}
	} else {
		networkServer.logger.Error("## Can not get a valid long connection, connection closed. No register response was sent. MyServerAddress [%+v]. ##", networkServer.myServerDeployAddress)
	}
}

func (networkServer *LongConnNetworkServer) notifyConnAdded(connectionKey int64) {
	defer func() {
		if err := recover(); err != nil {
			networkServer.logger.Error("Notify connection added for connection key [%+v] failed. MyServerAddress [%+v]. Error is %+v.", connectionKey, networkServer.myServerDeployAddress, err)
		}
	}()

	networkServer.connectionChangedListenersLock.Lock()
	connectionChangedListeners := make([]IConnectionChangedListener, len(networkServer.connectionChangedListeners))
	copy(connectionChangedListeners, networkServer.connectionChangedListeners)
	networkServer.connectionChangedListenersLock.Unlock()
	for _, l := range connectionChangedListeners {
		l.ConnectionAdded(connectionKey)
	}
}

func (networkServer *LongConnNetworkServer) notifyConnRemoved(connectionKey int64) {
	defer func() {
		if err := recover(); err != nil {
			networkServer.logger.Error("Notify connection removed for connection key [%+v] failed. MyServerAddress [%+v]. Error is %+v.", connectionKey, networkServer.myServerDeployAddress, err)
		}
	}()

	networkServer.connectionChangedListenersLock.Lock()
	connectionChangedListeners := make([]IConnectionChangedListener, len(networkServer.connectionChangedListeners))
	copy(connectionChangedListeners, networkServer.connectionChangedListeners)
	networkServer.connectionChangedListenersLock.Unlock()
	for _, l := range connectionChangedListeners {
		l.ConnectionDisappeared(connectionKey)
	}
}

func (networkServer *LongConnNetworkServer) registerErrorProcess(conn *SocketConnection, err error) {
	connKey := conn.connectionRegistedKey
	networkServer.closeTcpSocket(conn.connectedSocket)
	conn.isConnectionResetByPeer = true
	networkServer.logger.Error("Register a long connection failed. Long connection with key [%+v] was discarded. MyServerAddress [%+v]. Error is : %+v.", connKey, networkServer.myServerDeployAddress, err)
	conn.writingLock.Unlock()
}

func (networkServer *LongConnNetworkServer) checkIsRegisterRequestLegal(registerRequest *protocol.WiredPackage) (connectionKey int64, registerResponseCode byte) {
	payload := registerRequest.Payload()
	appKey, appSecret, deviceId, _, err := protocol.GetRegisterRequestInfoFromPayload(payload)
	if err != nil {
		networkServer.logger.Error("Get register request info from payload failed. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
		return NIL_CONNECTION_KEY, REGISTER_CHECKING_GEN_CONN_KEY_ERR
	}

	deviceConnectionKeys := networkServer.deviceConnectionKeys
	devConnKey := appKey + "+" + appSecret + "+" + deviceId
	connKeyObj := deviceConnectionKeys.Get(devConnKey)
	if connKeyObj != nil {
		connectionKey = connKeyObj.(int64)
		return connectionKey, REGISTER_CHECKING_RESULT_LEGAL
	} else {
		// TODO : Check whether App Key and App Secret are in the config file
		connectionKey, err = networkServer.connectionKeyGenerator.GetSequence()
		if err != nil {
			networkServer.logger.Error("Generate connection key failed. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			return NIL_CONNECTION_KEY, REGISTER_CHECKING_GEN_CONN_KEY_ERR
		}

		oldConnectionKey, ok := deviceConnectionKeys.PutIfAbsent(devConnKey, connectionKey)
		if ok {
			return connectionKey, REGISTER_CHECKING_RESULT_LEGAL
		} else {
			connectionKey = oldConnectionKey.(int64)
			return connectionKey, REGISTER_CHECKING_RESULT_LEGAL
		}
	}
}

// For Flow Control
func (networkServer *LongConnNetworkServer) waitForSendNextPackage(conn *SocketConnection) {
	lServerConfig := networkServer.serverConfig
	if lServerConfig != nil {
		sendNextPackageWaitingTime := lServerConfig.SendNextPackageWaitingTimeInMillis
		if sendNextPackageWaitingTime > 0 {
			if conn.connectionLastSendPkgTime > 0 {
				forWaitingTime := sendNextPackageWaitingTime - (utils.CurrentTimeMillis() - conn.connectionLastSendPkgTime)
				if forWaitingTime > FOR_WAITING_THRESHOLD {
					time.Sleep(time.Duration(forWaitingTime) * time.Millisecond)
				}
			}

			conn.connectionLastSendPkgTime = utils.CurrentTimeMillis()
		}
	}
}

func (networkServer *LongConnNetworkServer) sendServerLineTestTask(conn *SocketConnection)  {
	if conn != nil {
		lConnectedSocket := conn.connectedSocket
		lServerConfig := networkServer.serverConfig

		if lServerConfig != nil && lConnectedSocket != nil {
			conn.writingLock.Lock()

			if !conn.isConnectionResetByPeer {
				conn.writingBuffer.Reset()
				protocol.Initialize2ServerLineTestRequestPackage(conn.writingBuffer)
				networkServer.waitForSendNextPackage(conn)
				_, err := lConnectedSocket.Write(conn.writingBuffer.Bytes())
				if err != nil {
					networkServer.logger.Error("Sent server line test request to client with register key [%+v] failed. MyServerAddress [%+v]. Error is : %+v.", conn.connectionRegistedKey, networkServer.myServerDeployAddress, err)
				}
			}

			conn.writingLock.Unlock()

			networkServer.sentServerLineTestRequestNumber.Increment()
		}
	} else {
		networkServer.logger.Error("Can not get long connection. No server line test request was sent. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}
}

func (networkServer *LongConnNetworkServer) responseClientHeartbeatTask(conn *SocketConnection, clientHeartbeatRequest *protocol.WiredPackage)  {
	if conn != nil {
		lConnectedSocket := conn.connectedSocket
		lServerConfig := networkServer.serverConfig

		if lServerConfig != nil && lConnectedSocket != nil {
			conn.writingLock.Lock()

			if !conn.isConnectionResetByPeer {
				conn.writingBuffer.Reset()
				protocol.Initialize2LongConnHeartbeatResponsePackage(conn.writingBuffer)
				networkServer.waitForSendNextPackage(conn)
				_, err := lConnectedSocket.Write(conn.writingBuffer.Bytes())
				if err != nil {
					networkServer.logger.Error("Sent heartbeat response to client with register key [%+v] failed. MyServerAddress [%+v]. Error is : %+v.", clientHeartbeatRequest.ConnKey(), networkServer.myServerDeployAddress, err)
				}

				conn.connectionLastHeartbeatTime = utils.CurrentTimeMillis()
			} else {
				networkServer.logger.Error("Connection with connKey[%+v] was closed, keep alive request can not be sent. MyServerAddress [%+v].", conn.connectionRegistedKey, networkServer.myServerDeployAddress)
			}

			conn.writingLock.Unlock()

			networkServer.receivedKeepAliveRequestNumber.Increment()
		}
	} else {
		networkServer.logger.Error("Can not get long connection. No heartbeat response was sent. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}
}

func (networkServer *LongConnNetworkServer) clientResponseServerLineTestTask(conn *SocketConnection, serverLineTestResponse *protocol.WiredPackage)  {
	if conn != nil {
		lServerConfig := networkServer.serverConfig
		if lServerConfig != nil {
			conn.writingLock.Lock()
			conn.connectionLastHeartbeatTime = utils.CurrentTimeMillis()
			conn.writingLock.Unlock()

			networkServer.receivedServerLineTestResponseNumber.Increment()
		}
	} else {
		networkServer.logger.Error("Can not get long connection. Set last heartbeat time failed. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}
}

func (networkServer *LongConnNetworkServer) processClient2ServerBizRequestTask(conn *SocketConnection, rpcCallPackage *protocol.WiredPackage)  {
	if conn != nil {
		lConnectedSocket := conn.connectedSocket
		lServerConfig := networkServer.serverConfig
		namedRpcRequestActions := networkServer.namedRpcRequestActions
		dynamicResponseActions := networkServer.dynamicResponseActions
		bizRequestWritingChannel := networkServer.bizRequestWritingChannel

		if lServerConfig != nil && lConnectedSocket != nil && namedRpcRequestActions != nil {
			connKey := conn.connectionRegistedKey
			if !conn.isConnectionResetByPeer && connKey != NIL_CONNECTION_KEY {
				actionName, messageId, actionBody, err := protocol.GetLongConnActionInfoFromPayload(rpcCallPackage.Payload(), lServerConfig.IsCompressPushBody)
				if err != nil {
					networkServer.logger.Error("Get request action info from payload failed, can not process client upstream request. Long connection with key [%+v]. MyServerAddress [%+v]. Error is : %+v.", connKey, networkServer.myServerDeployAddress, err)
					return
				}

				if namedRpcRequestActions.Len() > 0 {
					action := namedRpcRequestActions.Get(actionName)
					if action != nil {
						if requestAction, ok := action.(IRequestAction); ok {
							actionResponse, usingReliablePush, messageDeadAction, err := requestAction.RequestReceived(connKey, messageId, actionBody)
							if err != nil {
								networkServer.logger.Error("Dispatch request call package to request action failed, discard it. MyServerAddress [%+v]. Error is : %+v.", networkServer.myServerDeployAddress, err)
								networkServer.receivedButDiscardedRpcRequestNumber.Increment()

								bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE,
									conn: conn, actionName: actionName, messageId: messageId,
									payload: []byte(RPC_FAILED_ACTION_ERROR), retryStartTime: -1, lastRetryTime: -1, putInto1stBufferChannelTime: -1,
									putInto2ndBufferChannelTime: -1, messageDeadAction: messageDeadAction}
								bizRequestWritingChannel <- bizRequest
							} else {
								if actionResponse != nil && len(actionResponse) > 0 {
									if usingReliablePush {
										responseActionInfo := &DynamicResponseActionInfo{responseAction: nil, createdTime: utils.CurrentTimeMillis(), isWaitingForResponseReceived: true,
											isPushRequest: true, isResponseReceived: false, responseBody: nil}
										responseActionInfo.responseReceivedCond = sync.NewCond(&responseActionInfo.responseReceivedLock)
										_, isPut := dynamicResponseActions.PutIfAbsent(messageId, responseActionInfo)
										if isPut {
											bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_SERVER_2_CLIENT_PUSH_REQUEST,
												conn: conn, actionName: actionName, messageId: messageId,
												payload: actionResponse, retryStartTime: -1, lastRetryTime: -1, putInto1stBufferChannelTime: -1,
												putInto2ndBufferChannelTime: -1, messageDeadAction: messageDeadAction}
											bizRequestWritingChannel <- bizRequest
										} else {
											bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE,
												conn: conn, actionName: actionName, messageId: messageId,
												payload: actionResponse, retryStartTime: -1, lastRetryTime: -1, putInto1stBufferChannelTime: -1,
												putInto2ndBufferChannelTime: -1, messageDeadAction: messageDeadAction}
											bizRequestWritingChannel <- bizRequest
										}
									} else {
										bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE,
											conn: conn, actionName: actionName, messageId: messageId,
											payload: actionResponse, retryStartTime: -1, lastRetryTime: -1, putInto1stBufferChannelTime: -1,
											putInto2ndBufferChannelTime: -1, messageDeadAction: messageDeadAction}
										bizRequestWritingChannel <- bizRequest
									}
								} else {
									bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE,
										conn: conn, actionName: actionName, messageId: messageId,
										payload: []byte(RPC_FAILED_ACTION_NO_RESPONSE), retryStartTime: -1, lastRetryTime: -1, putInto1stBufferChannelTime: -1,
										putInto2ndBufferChannelTime: -1, messageDeadAction: messageDeadAction}
									bizRequestWritingChannel <- bizRequest
								}
							}
						} else {
							networkServer.logger.Error("Can not get RPC action by name [%+v], can not process rpc call package, discard it. MyServerAddress [%+v].", actionName, networkServer.myServerDeployAddress)
							bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE,
								conn: conn, actionName: actionName, messageId: messageId,
								payload: []byte(RPC_FAILED_CAN_NOT_FIND_ACTION), retryStartTime: -1, lastRetryTime: -1, putInto1stBufferChannelTime: -1,
								putInto2ndBufferChannelTime: -1, messageDeadAction: nil}
							bizRequestWritingChannel <- bizRequest
						}
					} else {
						networkServer.logger.Error("Can not get RPC action by name [%+v], can not process rpc call package, discard it. MyServerAddress [%+v].", actionName, networkServer.myServerDeployAddress)
						bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE,
							conn: conn, actionName: actionName, messageId: messageId,
							payload: []byte(RPC_FAILED_CAN_NOT_FIND_ACTION), retryStartTime: -1, lastRetryTime: -1, putInto1stBufferChannelTime: -1,
							putInto2ndBufferChannelTime: -1, messageDeadAction: nil}
						bizRequestWritingChannel <- bizRequest
					}
				} else {
					networkServer.logger.Error("Named RPC actions is empty, can not process rpc call package, discard it. MyServerAddress [%+v]", networkServer.myServerDeployAddress)
					bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE,
						conn: conn, actionName: actionName, messageId: messageId,
						payload: []byte(RPC_FAILED_CAN_NOT_FIND_ACTION), retryStartTime: -1, lastRetryTime: -1, putInto1stBufferChannelTime: -1,
						putInto2ndBufferChannelTime: -1, messageDeadAction: nil}
					bizRequestWritingChannel <- bizRequest
				}

				networkServer.receivedRpcRequestNumber.Increment()
			} else {
				networkServer.logger.Error("Connection with connKey[%+v] was closed, Can not process client upstream request. MyServerAddress [%+v].", conn.connectionRegistedKey, networkServer.myServerDeployAddress)
			}
		} else {
			// Else, server is stopping
			networkServer.logger.Error("## Long connection server is stopping. Can not process client upstream request. MyServerAddress [%+v]. ##", networkServer.myServerDeployAddress)
		}
	} else {
		networkServer.logger.Error("Can not get long connection. Can not process client upstream request. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}
}

func (networkServer *LongConnNetworkServer) sendBizRequest2ClientTask() {
	defer func() {
		if err := recover(); err != nil {
			networkServer.logger.Error("SendBizRequest2ClientTask finished, No more request can be send to client. MyServerAddress [%+v]. Error is %+v.", networkServer.myServerDeployAddress, err)
		}
	}()

	for {
		networkServer.lifecycleLock.RLock()

		lServerStatus := networkServer.serverStatus
		if lServerStatus == sutils.Stopping ||
			lServerStatus == sutils.Stopped {

			networkServer.logger.Debug("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			networkServer.lifecycleLock.RUnlock()
			break
		}

		if lServerStatus != sutils.Started {

			networkServer.lifecycleLock.RUnlock()
			continue
		}

		lServerConfig := networkServer.serverConfig
		bizRequestWritingChannel := networkServer.bizRequestWritingChannel
		bizRequestWritingRetryChannel1 := networkServer.bizRequestWritingRetryChannel1
		reliablePushedRequest1stBufferChannel := networkServer.reliablePushedRequest1stBufferChannel

		networkServer.lifecycleLock.RUnlock()

		bizRequest := <- bizRequestWritingChannel
		if bizRequest != nil {
			networkServer.realSendBizRequestByFuncCode(bizRequest, lServerConfig, bizRequestWritingRetryChannel1, reliablePushedRequest1stBufferChannel)
		}
	}

	networkServer.logger.Debug("SendBizRequest2ClientTask finished, No more request can be send to client. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
}

func (networkServer *LongConnNetworkServer) realSendBizRequestByFuncCode(bizRequest *ForWritingBizRequest,
	lServerConfig *LongConnNetworkServerConfig, bizRequestWritingRetryChannel chan *ForWritingBizRequest,
	reliablePushedRequest1stBufferChannel chan *ForWritingBizRequest) {

	bizConn := bizRequest.conn
	actionName := bizRequest.actionName
	messageId := bizRequest.messageId
	payload := bizRequest.payload

	networkServer.lifecycleLock.RLock()

	lServerStatus := networkServer.serverStatus
	if lServerStatus != sutils.Started {
		networkServer.lifecycleLock.RUnlock()
		return
	}

	var socketConn       *SocketConnection = nil
	var lConnectedSocket *net.TCPConn = nil
	registeredConnections := networkServer.registeredConnections
	conn := registeredConnections.Get(bizConn.connectionRegistedKey)
	if conn != nil {
		if conn1, ok := conn.(*SocketConnection); ok {
			socketConn = conn1
			lConnectedSocket = socketConn.connectedSocket
		}
	}

	networkServer.lifecycleLock.RUnlock()

	if socketConn != nil && lConnectedSocket != nil {
		socketConn.writingLock.Lock()

		if !socketConn.isConnectionResetByPeer {

			socketConn.writingBuffer.Reset()
			switch bizRequest.funcCode {
			case protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE: {
				err := protocol.Initialize2ServerLongConnCallResponsePackage(actionName, messageId, payload, lServerConfig.IsCompressPushBody, socketConn.writingBuffer)
				if err != nil {
					networkServer.processSend2ClientError(bizRequest, bizRequestWritingRetryChannel, socketConn)
					networkServer.logger.Debug("Send rpc action response to connection with key [%+v] failed. MyServerAddress [%+v]. Error is [%+v].", socketConn.connectionRegistedKey, networkServer.myServerDeployAddress, err)
					return
				}
			}

			case protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_REQUEST: {
				err := protocol.Initialize2ServerLongConnCallRequestPackage(actionName, messageId, payload, lServerConfig.IsCompressPushBody, socketConn.writingBuffer)
				if err != nil {
					networkServer.processSend2ClientError(bizRequest, bizRequestWritingRetryChannel, socketConn)
					networkServer.logger.Debug("Send rpc request to connection with key [%+v] failed. MyServerAddress [%+v]. Error is [%+v].", socketConn.connectionRegistedKey, networkServer.myServerDeployAddress, err)
					return
				}
			}

			case protocol.LC_FUNC_CODE_SERVER_2_CLIENT_PUSH_REQUEST: {
				err := protocol.Initialize2ServerReliablePushRequestPackage(actionName, messageId, payload, lServerConfig.IsCompressPushBody, socketConn.writingBuffer)
				if err != nil {
					networkServer.processSend2ClientError(bizRequest, bizRequestWritingRetryChannel, socketConn)
					networkServer.logger.Debug("Send push request to connection with key [%+v] failed. MyServerAddress [%+v]. Error is [%+v].", socketConn.connectionRegistedKey, networkServer.myServerDeployAddress, err)
					return
				}
			}

			default: {
				networkServer.logger.Debug("Send biz request to client with key [%+v] failed. Received wrong function code. MyServerAddress [%+v].", socketConn.connectionRegistedKey, networkServer.myServerDeployAddress)
				socketConn.writingLock.Unlock()
				return
			}
			}

			networkServer.waitForSendNextPackage(socketConn)
			_, err := lConnectedSocket.Write(socketConn.writingBuffer.Bytes())
			if err != nil {
				networkServer.processSend2ClientError(bizRequest, bizRequestWritingRetryChannel, socketConn)
				networkServer.logger.Debug("Send message to client with key [%+v] failed. MyServerAddress [%+v]. Error is [%+v].", socketConn.connectionRegistedKey, networkServer.myServerDeployAddress, err)
				return
			}

			socketConn.writingLock.Unlock()

			switch bizRequest.funcCode {
				case protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE: {
					networkServer.sentRpcResponseNumber.Increment()
				}

				case protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_REQUEST: {
					networkServer.sentRpcRequestNumber.Increment()
				}

				case protocol.LC_FUNC_CODE_SERVER_2_CLIENT_PUSH_REQUEST: {
					networkServer.pushedReliableRequestNumber.Increment()
					bizRequest.putInto1stBufferChannelTime = utils.CurrentTimeMillis()
					reliablePushedRequest1stBufferChannel <- bizRequest
				}
			}
		} else {
			networkServer.processSend2ClientError(bizRequest, bizRequestWritingRetryChannel, socketConn)
			networkServer.logger.Debug("Connection with connKey[%+v] was closed, message can not be sent to client. MyServerAddress [%+v].", bizConn.connectionRegistedKey, networkServer.myServerDeployAddress)
		}
	} else {
		networkServer.processSend2ClientError(bizRequest, bizRequestWritingRetryChannel, nil)
		networkServer.logger.Debug("Can not get connection with connKey[%+v], message can not be sent to client. MyServerAddress [%+v].", bizConn.connectionRegistedKey, networkServer.myServerDeployAddress)
	}
}

func (networkServer *LongConnNetworkServer) retrySendBizRequest2ClientTask(bizRequestWritingRetryChannel1 chan *ForWritingBizRequest, bizRequestWritingRetryChannel2 chan *ForWritingBizRequest) {
	defer func() {
		if err := recover(); err != nil {
			networkServer.logger.Error("RetrySendBizRequest2ClientTask finished, No more request can be send to client. MyServerAddress [%+v]. Error is %+v.", networkServer.myServerDeployAddress, err)
		}
	}()

	for {
		networkServer.lifecycleLock.RLock()

		lServerStatus := networkServer.serverStatus
		if lServerStatus == sutils.Stopping ||
			lServerStatus == sutils.Stopped {

			networkServer.logger.Debug("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			networkServer.lifecycleLock.RUnlock()
			break
		}

		if lServerStatus != sutils.Started {

			networkServer.lifecycleLock.RUnlock()
			continue
		}

		lServerConfig := networkServer.serverConfig
		reliablePushedRequest1stBufferChannel := networkServer.reliablePushedRequest1stBufferChannel

		networkServer.lifecycleLock.RUnlock()

		bizRequest := <- bizRequestWritingRetryChannel1
		if bizRequest != nil {
			retryStartTime := bizRequest.retryStartTime
			lastRetryTime  := bizRequest.lastRetryTime
			if retryStartTime > 0 && lastRetryTime > 0 {
				currTime := utils.CurrentTimeMillis()
				retryTimeElapsedFromStart := currTime - retryStartTime
				retryTimeElapsedFromLast  := currTime - lastRetryTime
				if retryTimeElapsedFromStart >= lServerConfig.MaxRetrySendMsgTimeInMillis {
					networkServer.deathMessageProcess("RetryTimeout", bizRequest, lServerConfig)
					continue
				} else {
					waitingTime := lServerConfig.RetrySendMsgIntervalInMillis - retryTimeElapsedFromLast
					if waitingTime > FOR_WAITING_THRESHOLD {
						networkServer.logger.Debug("Going to sleep for retry in message retry channel, waiting time [%+v], MyServerAddress [%+v].", waitingTime, networkServer.myServerDeployAddress)
						time.Sleep(time.Duration(waitingTime) * time.Millisecond)
					}

					networkServer.writingRequestRetryNumber.Increment()
				}
			}

			networkServer.realSendBizRequestByFuncCode(bizRequest, lServerConfig, bizRequestWritingRetryChannel2, reliablePushedRequest1stBufferChannel)
		}
	}

	networkServer.logger.Debug("RetrySendBizRequest2ClientTask finished, No more request can be send to client. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
}

func (networkServer *LongConnNetworkServer) deathMessageProcess(methodPoint string, bizRequest *ForWritingBizRequest, lServerConfig *LongConnNetworkServerConfig) {
	conn := bizRequest.conn
	actionName := bizRequest.actionName
	messageId := bizRequest.messageId
	messageDeadAction := bizRequest.messageDeadAction
	msgBody := bizRequest.payload

	if messageDeadAction != nil && messageDeadAction(conn.connectionRegistedKey, messageId, msgBody) {
		networkServer.logger.Info("[%+v] Dead message had been processed by MessageDeadAction, msg exceed max retry time [%+v], "+
			"Action Name [%+v], Connection Key [%+v], Message Id [%+v]. MyServerAddress [%+v].", methodPoint, lServerConfig.MaxRetrySendMsgTimeInMillis,
			actionName, conn.connectionRegistedKey, messageId, networkServer.myServerDeployAddress)
	} else {
		networkServer.logger.Error("[%+v] Send message to client exchaused, exceed max retry time [%+v], will discard it, "+
			"Action Name [%+v], Connection Key [%+v], Message Id [%+v]. MyServerAddress [%+v].", methodPoint, lServerConfig.MaxRetrySendMsgTimeInMillis,
			actionName, conn.connectionRegistedKey, messageId, networkServer.myServerDeployAddress)
	}

	networkServer.writingRequestDeadNumber.Increment()
}

func (networkServer *LongConnNetworkServer) processSend2ClientError(bizRequest *ForWritingBizRequest, bizRequestWritingRetryChannel chan *ForWritingBizRequest, conn *SocketConnection) {
	currTime := utils.CurrentTimeMillis()
	if bizRequest.retryStartTime <= 0 {
		bizRequest.retryStartTime = currTime
	}

	bizRequest.lastRetryTime = currTime
	bizRequestWritingRetryChannel <- bizRequest
	networkServer.requestRetryNumber.Increment()

	if  conn != nil {
		conn.writingLock.Unlock()
	}
}

func (networkServer *LongConnNetworkServer) deathConnectionCleaningTask() {
	defer func() {
		if err := recover(); err != nil {
			networkServer.logger.Info("Death connection scanning task finished. MyServerAddress [%+v]. Error is %+v.", networkServer.myServerDeployAddress, err)
		}
	}()

	for {
		networkServer.lifecycleLock.RLock()

		lServerStatus := networkServer.serverStatus
		if lServerStatus == sutils.Stopping ||
			lServerStatus == sutils.Stopped {

			networkServer.logger.Info("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			networkServer.lifecycleLock.RUnlock()
			break
		}

		if lServerStatus != sutils.Started {

			networkServer.lifecycleLock.RUnlock()
			continue
		}

		lServerConfig := networkServer.serverConfig
		deathConnectionScanningInterval := lServerConfig.DeathConnectionScanningIntervalInMillis
		noneActiveTimeForDeathConnection := lServerConfig.NoneActiveTimeForDeathConnectionInMillis
		sendServerLineTestRequestInterval := lServerConfig.SendServerLineTestRequestIntervalInMillis

		if noneActiveTimeForDeathConnection < (sendServerLineTestRequestInterval * 2) {
			noneActiveTimeForDeathConnection = sendServerLineTestRequestInterval * 2
		}

		networkServer.lifecycleLock.RUnlock()

		time.Sleep(time.Duration(deathConnectionScanningInterval) * time.Millisecond)

		networkServer.lifecycleLock.RLock()

		lServerStatus = networkServer.serverStatus
		if lServerStatus == sutils.Stopping ||
			lServerStatus == sutils.Stopped {

			networkServer.logger.Info("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			networkServer.lifecycleLock.RUnlock()
			break
		}

		if lServerStatus != sutils.Started {
			networkServer.lifecycleLock.RUnlock()
			continue
		}

		registeredConnections := networkServer.registeredConnections
		registeredConnectionKeys := registeredConnections.Keys()

		for _, registeredConnKey := range registeredConnectionKeys {
			conn := registeredConnections.Get(registeredConnKey)
			if conn != nil {
				if socketConn, ok := conn.(*SocketConnection); ok {
					socketConn.writingLock.Lock()

					if socketConn.isConnectionResetByPeer {
						registeredConnections.Remove(registeredConnKey)
						networkServer.closeTcpSocket(socketConn.connectedSocket)
						networkServer.logger.Info("A registered long connection with key [%+v] was death, remove from connection registered map. MyServerAddress [%+v].", registeredConnKey, networkServer.myServerDeployAddress)
						networkServer.notifyConnRemoved(registeredConnKey.(int64))
					} else {
						forDeterminationTime := getDeterminationTime(socketConn)
						timeElapsedFromLastActive := utils.CurrentTimeMillis() - forDeterminationTime
						if timeElapsedFromLastActive > noneActiveTimeForDeathConnection {
							registeredConnections.Remove(registeredConnKey)
							networkServer.closeTcpSocket(socketConn.connectedSocket)
							networkServer.logger.Info("A registered long connection with key [%+v] was death, remove from connection registered map. MyServerAddress [%+v].", registeredConnKey, networkServer.myServerDeployAddress)
							networkServer.notifyConnRemoved(registeredConnKey.(int64))
						} else if timeElapsedFromLastActive >= sendServerLineTestRequestInterval {
							go networkServer.sendServerLineTestTask(socketConn)
						}
					}

					socketConn.writingLock.Unlock()
				} else {
					registeredConnections.Remove(registeredConnKey)
					networkServer.logger.Info("Can not get a registered long connection by key [%+v], romove from connection registered map. MyServerAddress [%+v].", registeredConnKey, networkServer.myServerDeployAddress)
					networkServer.notifyConnRemoved(registeredConnKey.(int64))
				}
			} else {
				registeredConnections.Remove(registeredConnKey)
				networkServer.logger.Info("Can not get a registered long connection by key [%+v], romove from connection registered map. MyServerAddress [%+v].", registeredConnKey, networkServer.myServerDeployAddress)
				networkServer.notifyConnRemoved(registeredConnKey.(int64))
			}
		}

		networkServer.lifecycleLock.RUnlock()
	}

	networkServer.logger.Info("Death connection scanning task finished. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
}

func (networkServer *LongConnNetworkServer) dynamicResponseActionCleaningTask() {
	defer func() {
		if err := recover(); err != nil {
			networkServer.logger.Info("Dynamic response action cleaning task finished. MyServerAddress [%+v]. Error is %+v.", networkServer.myServerDeployAddress, err)
		}
	}()

	for {
		networkServer.lifecycleLock.RLock()

		lServerStatus := networkServer.serverStatus
		if lServerStatus == sutils.Stopping ||
			lServerStatus == sutils.Stopped {

			networkServer.logger.Info("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			networkServer.lifecycleLock.RUnlock()
			break
		}

		if lServerStatus != sutils.Started {

			networkServer.lifecycleLock.RUnlock()
			continue
		}

		lServerConfig := networkServer.serverConfig
		dynamicResponseActionRetentionTime := lServerConfig.DynamicResponseActionRetentionTimeInMillis

		networkServer.lifecycleLock.RUnlock()

		time.Sleep(time.Duration(dynamicResponseActionRetentionTime) * time.Millisecond)

		networkServer.lifecycleLock.RLock()

		lServerStatus = networkServer.serverStatus
		if lServerStatus == sutils.Stopping ||
			lServerStatus == sutils.Stopped {

			networkServer.logger.Info("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			networkServer.lifecycleLock.RUnlock()
			break
		}

		if lServerStatus != sutils.Started {
			networkServer.lifecycleLock.RUnlock()
			continue
		}

		dynamicResponseActions := networkServer.dynamicResponseActions
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
				}
			} else {
				dynamicResponseActions.Remove(responseActionKey)
			}
		}

		networkServer.lifecycleLock.RUnlock()
	}

	networkServer.logger.Info("Dynamic response action cleaning task finished. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
}

func getDeterminationTime(conn *SocketConnection) int64 {
	timeArray := make([]int64, 4)
	timeArray[0] = conn.connectionAcceptedTime
	timeArray[1] = conn.connectionRegistedTime
	timeArray[2] = conn.connectionLastHeartbeatTime
	timeArray[3] = conn.connectionDisconnectedTime
	utils.Int64Sort(timeArray)
	return timeArray[3]
}

func (networkServer *LongConnNetworkServer) GetServerStatisInfo() map[string]interface{} {
	networkServer.lifecycleLock.RLock()
	defer networkServer.lifecycleLock.RUnlock()

	statisInfo := make(map[string]interface{}, 64)
	lServerStatus := networkServer.serverStatus
	if lServerStatus != sutils.Started {
		return statisInfo
	}

	currentRegisteredConnSize := networkServer.registeredConnections.Len()
	currentRegisteredButDisconnectedConnSize := networkServer.getLocalDisconnectedConnectionNum()
	writingBizRequestQueueSize := len(networkServer.bizRequestWritingChannel)
	writingBizRequestRetryQueueSize := len(networkServer.bizRequestWritingRetryChannel1) + len(networkServer.bizRequestWritingRetryChannel2)
	receivedDataAssembleQueueSize := len(networkServer.receivedDataAssembleChannel)
	receivedKeepAliveRequestNum := networkServer.receivedKeepAliveRequestNumber.Sum()
	sentServerLineTestRequestNum := networkServer.sentServerLineTestRequestNumber.Sum()
	receivedServerLineTestResponseNum := networkServer.receivedServerLineTestResponseNumber.Sum()
	writingRequestRetryNum := networkServer.writingRequestRetryNumber.Sum()
	writingRequestDeadNum := networkServer.writingRequestDeadNumber.Sum()
	sentRpcResponseNumber := networkServer.sentRpcResponseNumber.Sum()
	processedRegisterRequestNum := networkServer.processedRegisterRequestNumber.Sum()
	processedDisconnectedRequestNum := networkServer.processedDisconnectedRequestNumber.Sum()
	receivedRpcRequestNumber := networkServer.receivedRpcRequestNumber.Sum()
	receivedButDiscardedRpcRequestNumber := networkServer.receivedButDiscardedRpcRequestNumber.Sum()
	receivedRpcResponseNumber := networkServer.receivedRpcResponseNumber.Sum()
	receivedButDiscardedRpcResponseNumber := networkServer.receivedButDiscardedRpcResponseNumber.Sum()
	sentRpcRequestNumber := networkServer.sentRpcRequestNumber.Sum()
	pushedReliableRequestNumber := networkServer.pushedReliableRequestNumber.Sum()
	receivedReliablePushResponseNumber := networkServer.receivedReliablePushResponseNumber.Sum()
	dynamicResponseActionsCacheSize := networkServer.dynamicResponseActions.Len()

	statisInfo["RegisteredConn"] =  currentRegisteredConnSize
	statisInfo["RegisteredButDisconnectedConn"] =  currentRegisteredButDisconnectedConnSize

	statisInfo["KeepAliveRequest"] =  receivedKeepAliveRequestNum
	statisInfo["ServerLineTestRequest"] =  sentServerLineTestRequestNum
	statisInfo["ServerLineTestResponse"] =  receivedServerLineTestResponseNum

	statisInfo["WritingQueueSize"] =  writingBizRequestQueueSize
	statisInfo["WritingRetryQueueSize"] =  writingBizRequestRetryQueueSize
	statisInfo["ReceivedDataAssembleQueueSize"] =  receivedDataAssembleQueueSize

	statisInfo["WritingRequestRetryNumber"] =  writingRequestRetryNum
	statisInfo["WritingRequestDeadNumber"] =  writingRequestDeadNum
	statisInfo["SentRpcResponseNumber"] =  sentRpcResponseNumber

	statisInfo["ProcessedRegisterRequestNumber"] =  processedRegisterRequestNum
	statisInfo["ProcessedDisconnectedRequestNumber"] =  processedDisconnectedRequestNum
	statisInfo["ServerBindingAddressForPushing"] =  networkServer.myServerDeployAddress

	statisInfo["ServerInterOpRequest"] = receivedRpcRequestNumber
	statisInfo["DiscardedRpcRequest"] = receivedButDiscardedRpcRequestNumber
	statisInfo["RpcResponse"] = receivedRpcResponseNumber
	statisInfo["DiscardedRpcResponse"] = receivedButDiscardedRpcResponseNumber

	statisInfo["PushedRequest"] = sentRpcRequestNumber
	statisInfo["SentResponse"] = sentRpcResponseNumber

	statisInfo["ReliablePushedRequest"] = pushedReliableRequestNumber
	statisInfo["ReliablePushedResponse"] = receivedReliablePushResponseNumber

	statisInfo["RpcResponseActionsNum"] = dynamicResponseActionsCacheSize

	return statisInfo
}

func (networkServer *LongConnNetworkServer) getLocalDisconnectedConnectionNum() int {

	registeredConnections := networkServer.registeredConnections
	registeredConnectionKeys := registeredConnections.Keys()
	disconnectedConnectionNum := 0

	for _, registeredConnKey := range registeredConnectionKeys {
		conn := registeredConnections.Get(registeredConnKey)
		if conn != nil {
			if socketConn, ok := conn.(*SocketConnection); ok {
				socketConn.writingLock.Lock()
				if socketConn.isConnectionResetByPeer {
					disconnectedConnectionNum++
				}
				socketConn.writingLock.Unlock()
			}
		}
	}

	return disconnectedConnectionNum
}

func (networkServer *LongConnNetworkServer) processClient2ServerBizResponseTask(conn *SocketConnection, rpcResponsePackage *protocol.WiredPackage)  {
	if conn != nil {
		lConnectedSocket := conn.connectedSocket
		lServerConfig := networkServer.serverConfig
		dynamicResponseActions := networkServer.dynamicResponseActions

		if lServerConfig != nil && lConnectedSocket != nil && dynamicResponseActions != nil {
			connKey := conn.connectionRegistedKey
			if !conn.isConnectionResetByPeer && connKey != NIL_CONNECTION_KEY {

				if dynamicResponseActions.Len() > 0 {
					_, messageId, actionBody, err := protocol.GetLongConnActionInfoFromPayload(rpcResponsePackage.Payload(), lServerConfig.IsCompressPushBody)
					if err != nil {
						networkServer.logger.Error("Get response action info from payload failed, can not process client upstream response. Long connection with key [%+v]. MyServerAddress [%+v]. Error is : %+v.", connKey, networkServer.myServerDeployAddress, err)
						return
					}

					action := dynamicResponseActions.Get(messageId)
					if action != nil {
						if responseActionInfo, ok := action.(*DynamicResponseActionInfo); ok {
							if !responseActionInfo.isPushRequest {
								dynamicResponseActions.Remove(messageId)
							}

							responseAction := responseActionInfo.responseAction
							if responseAction != nil {
								err := responseAction.ResponseReceived(connKey, messageId, actionBody)
								if err != nil {
									networkServer.logger.Error("Dispatch rpc response package to rpc action failed, discard it. MyServerAddress [%+v]. Error is : %+v.", networkServer.myServerDeployAddress, err)
									networkServer.receivedButDiscardedRpcResponseNumber.Increment()
								}
							}

							if responseActionInfo.isWaitingForResponseReceived {
								responseActionInfo.responseReceivedLock.Lock()
								responseActionInfo.isResponseReceived = true
								responseActionInfo.responseBody = actionBody

								if !responseActionInfo.isPushRequest {
									responseActionInfo.responseReceivedCond.Broadcast()
								}
								responseActionInfo.responseReceivedLock.Unlock()
							}
						} else {
							networkServer.logger.Error("Can not get dynamic response action by message id [%+v], can not process client upstream response, discard it. MyServerAddress [%+v].", messageId, networkServer.myServerDeployAddress)
						}
					} else {
						networkServer.logger.Error("Can not get dynamic response action by message id [%+v], can not process client upstream response, discard it. MyServerAddress [%+v].", messageId, networkServer.myServerDeployAddress)
					}
				} else {
					networkServer.logger.Error("Dynamic response actions is empty, can not process client upstream response, discard it. MyServerAddress [%+v]", networkServer.myServerDeployAddress)
				}

				networkServer.receivedRpcResponseNumber.Increment()
			} else {
				networkServer.logger.Error("Connection with connKey[%+v] was closed, Can not process client upstream response. MyServerAddress [%+v].", conn.connectionRegistedKey, networkServer.myServerDeployAddress)
			}
		} else {
			// Else, server is stopping
			networkServer.logger.Error("## Long connection server is stopping. Can not process client upstream response. MyServerAddress [%+v]. ##", networkServer.myServerDeployAddress)
		}
	} else {
		networkServer.logger.Error("Can not get long connection. Can not process client upstream response. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
	}
}

func (networkServer *LongConnNetworkServer) RPC(connKey int64, rpcActionName string, rpcRequest []byte, timeout int64) (rpcResponse []byte, err error) {
	defer func() {
		if recover() != nil {
			rpcResponse = nil
			err = NetworkServerInternalError
		}
	}()

	if rpcRequest == nil || len(rpcRequest) <= 0 {
		return nil, RequestBodyIsNilError
	}

	networkServer.lifecycleLock.RLock()

	lServerStatus := networkServer.serverStatus
	if lServerStatus != sutils.Started {
		networkServer.logger.Error("Long connection server was stopped. Can not invoke RPC function. connKey: %+v, rpcActionName: %+v", connKey, rpcActionName)
		networkServer.lifecycleLock.RUnlock()
		return nil, LongConnServerWasStoppedError
	}

	messageIdGenerator := networkServer.messageIdGenerator
	serverConfig := networkServer.serverConfig
	bizRequestWritingChannel := networkServer.bizRequestWritingChannel
	registeredConnections := networkServer.registeredConnections
	dynamicResponseActions := networkServer.dynamicResponseActions

	if timeout <= 0 {
		timeout = serverConfig.DefaultRpcTimeoutInMillis
	}

	var socketConn *SocketConnection = nil
	conn := registeredConnections.Get(connKey)
	if conn != nil {
		if conn1, ok := conn.(*SocketConnection); ok {
			socketConn = conn1
		} else {
			networkServer.logger.Error("Can not get connection by connKey: %+v, rpcActionName: %+v", connKey, rpcActionName)
			networkServer.lifecycleLock.RUnlock()
			return nil, CanNotGetConnByConnKeyError
		}
	} else {
		networkServer.logger.Error("Can not get connection by connKey: %+v, rpcActionName: %+v", connKey, rpcActionName)
		networkServer.lifecycleLock.RUnlock()
		return nil, CanNotGetConnByConnKeyError
	}

	networkServer.lifecycleLock.RUnlock()

	var msgId int64 = 0
	var responseActionInfo *DynamicResponseActionInfo = nil

	for {
		msgId, err = messageIdGenerator.GetSequence()
		if err != nil {
			networkServer.logger.Error("Generate message Id failed. connKey: %+v, rpcActionName: %+v", connKey, rpcActionName)
			return nil, err
		}

		responseActionInfo = &DynamicResponseActionInfo{responseAction: nil, createdTime: utils.CurrentTimeMillis(), isWaitingForResponseReceived: true,
			isPushRequest: false, isResponseReceived: false, responseBody: nil}
		responseActionInfo.responseReceivedCond = sync.NewCond(&responseActionInfo.responseReceivedLock)
		_, ok := dynamicResponseActions.PutIfAbsent(msgId, responseActionInfo)
		if ok {
			break
		} else {
			time.Sleep(time.Duration(8) * time.Millisecond)
		}
	}

	bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_SERVER_2_CLIENT_CALL_REQUEST,
		conn: socketConn, actionName: rpcActionName, messageId: msgId,
		payload: rpcRequest, retryStartTime: -1, lastRetryTime: -1, putInto1stBufferChannelTime: -1,
		putInto2ndBufferChannelTime: -1, messageDeadAction: nil}
	bizRequestWritingChannel <- bizRequest

	isCheckingTimeout := false
	isTimeout := false

	responseActionInfo.responseReceivedLock.Lock()

	for ;!responseActionInfo.isResponseReceived; {
		if !isCheckingTimeout {
			go func() {
				time.Sleep(time.Duration(timeout) * time.Millisecond)
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

func (networkServer *LongConnNetworkServer) Push(connKey int64, pushedActionName string, pushedRequest []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = NetworkServerInternalError
		}
	}()

	if pushedRequest == nil || len(pushedRequest) <= 0 {
		return RequestBodyIsNilError
	}

	networkServer.lifecycleLock.RLock()

	lServerStatus := networkServer.serverStatus
	if lServerStatus != sutils.Started {
		networkServer.logger.Error("Long connection server was stopped. Can not invoke Push function. connKey: %+v, pushedActionName: %+v", connKey, pushedActionName)
		networkServer.lifecycleLock.RUnlock()
		return LongConnServerWasStoppedError
	}

	messageIdGenerator := networkServer.messageIdGenerator
	bizRequestWritingChannel := networkServer.bizRequestWritingChannel
	registeredConnections := networkServer.registeredConnections
	dynamicResponseActions := networkServer.dynamicResponseActions

	var socketConn *SocketConnection = nil
	conn := registeredConnections.Get(connKey)
	if conn != nil {
		if conn1, ok := conn.(*SocketConnection); ok {
			socketConn = conn1
		} else {
			networkServer.logger.Error("Can not get connection by connKey: %+v, pushedActionName: %+v", connKey, pushedActionName)
			networkServer.lifecycleLock.RUnlock()
			return CanNotGetConnByConnKeyError
		}
	} else {
		networkServer.logger.Error("Can not get connection by connKey: %+v, pushedActionName: %+v", connKey, pushedActionName)
		networkServer.lifecycleLock.RUnlock()
		return CanNotGetConnByConnKeyError
	}

	networkServer.lifecycleLock.RUnlock()

	var msgId int64 = 0
	var responseActionInfo *DynamicResponseActionInfo = nil

	for {
		msgId, err = messageIdGenerator.GetSequence()
		if err != nil {
			networkServer.logger.Error("Generate message Id failed. connKey: %+v, pushedActionName: %+v", connKey, pushedActionName)
			return err
		}

		responseActionInfo = &DynamicResponseActionInfo{responseAction: nil, createdTime: utils.CurrentTimeMillis(), isWaitingForResponseReceived: true,
			isPushRequest: true, isResponseReceived: false, responseBody: nil}
		responseActionInfo.responseReceivedCond = sync.NewCond(&responseActionInfo.responseReceivedLock)
		_, ok := dynamicResponseActions.PutIfAbsent(msgId, responseActionInfo)
		if ok {
			break
		} else {
			time.Sleep(time.Duration(8) * time.Millisecond)
		}
	}

	bizRequest := &ForWritingBizRequest{funcCode: protocol.LC_FUNC_CODE_SERVER_2_CLIENT_PUSH_REQUEST,
		conn: socketConn, actionName: pushedActionName, messageId: msgId,
		payload: pushedRequest, retryStartTime: -1, lastRetryTime: -1, putInto1stBufferChannelTime: -1,
		putInto2ndBufferChannelTime: -1, messageDeadAction: nil}
	bizRequestWritingChannel <- bizRequest
	return nil
}

func (networkServer *LongConnNetworkServer) process1stBufferQueueTask() {
	defer func() {
		if err := recover(); err != nil {
			networkServer.logger.Error("Process1stBufferQueueTask finished. MyServerAddress [%+v]. Error is %+v.", networkServer.myServerDeployAddress, err)
		}
	}()

	for {
		networkServer.lifecycleLock.RLock()

		lServerStatus := networkServer.serverStatus
		if lServerStatus == sutils.Stopping ||
			lServerStatus == sutils.Stopped {

			networkServer.logger.Debug("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			networkServer.lifecycleLock.RUnlock()
			break
		}

		if lServerStatus != sutils.Started {

			networkServer.lifecycleLock.RUnlock()
			continue
		}

		lServerConfig := networkServer.serverConfig
		reliablePushedRequest1stBufferChannel := networkServer.reliablePushedRequest1stBufferChannel
		reliablePushedRequest2ndBufferChannel := networkServer.reliablePushedRequest2ndBufferChannel
		dynamicResponseActions := networkServer.dynamicResponseActions

		networkServer.lifecycleLock.RUnlock()

		bizRequest := <- reliablePushedRequest1stBufferChannel
		if bizRequest != nil {
			messageId := bizRequest.messageId
			if dynamicResponseActions.Len() > 0 {
				action := dynamicResponseActions.Get(messageId)
				if action != nil {
					if responseActionInfo, ok := action.(*DynamicResponseActionInfo); ok {
						responseActionInfo.responseReceivedLock.Lock()

						if responseActionInfo.isResponseReceived {
							if responseActionInfo.responseBody != nil {
								responseBodyString := string(responseActionInfo.responseBody)
								if responseBodyString == "SUCC" {
									networkServer.receivedReliablePushResponseNumber.Increment()
								} else {
									networkServer.logger.Error("Received failed reliable push response in 1stBufferQueue. Response : " + responseBodyString)
									networkServer.deathMessageProcess("1stBufferQueue", bizRequest, lServerConfig)
								}
							} else {
								networkServer.logger.Error("Received nil reliable push response in 1stBufferQueue.")
								networkServer.deathMessageProcess("1stBufferQueue", bizRequest, lServerConfig)
							}

							dynamicResponseActions.Remove(messageId)
						} else {
							putIntoBufferQueueTime := bizRequest.putInto1stBufferChannelTime
							if utils.CurrentTimeMillis() - putIntoBufferQueueTime >= lServerConfig.DeterminatePushResponseReceivedTimeoutInMillis {
								bizRequest.putInto2ndBufferChannelTime = utils.CurrentTimeMillis()
								reliablePushedRequest2ndBufferChannel <- bizRequest
							} else {
								time.Sleep(time.Duration(8) * time.Millisecond)
								reliablePushedRequest1stBufferChannel <- bizRequest
							}
						}

						responseActionInfo.responseReceivedLock.Unlock()
					} else {
						networkServer.logger.Error("Can not get response action by message ID [%+v] in 1stBufferQueue, Biz request process finished.", messageId)
						networkServer.deathMessageProcess("1stBufferQueue", bizRequest, lServerConfig)
					}
				} else {
					networkServer.logger.Error("Can not get response action by message ID [%+v] in 1stBufferQueue, it is nil, Biz request process finished.", messageId)
					networkServer.deathMessageProcess("1stBufferQueue", bizRequest, lServerConfig)
				}
			}
		}
	}

	networkServer.logger.Debug("Process1stBufferQueueTask finished. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
}

func (networkServer *LongConnNetworkServer) process2ndBufferQueueTask() {
	defer func() {
		if err := recover(); err != nil {
			networkServer.logger.Error("Process2ndBufferQueueTask finished. MyServerAddress [%+v]. Error is %+v.", networkServer.myServerDeployAddress, err)
		}
	}()

	for {
		networkServer.lifecycleLock.RLock()

		lServerStatus := networkServer.serverStatus
		if lServerStatus == sutils.Stopping ||
			lServerStatus == sutils.Stopped {

			networkServer.logger.Debug("Long connection network server is stopping. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
			networkServer.lifecycleLock.RUnlock()
			break
		}

		if lServerStatus != sutils.Started {

			networkServer.lifecycleLock.RUnlock()
			continue
		}

		lServerConfig := networkServer.serverConfig
		bizRequestWritingRetryChannel1 := networkServer.bizRequestWritingRetryChannel1
		reliablePushedRequest2ndBufferChannel := networkServer.reliablePushedRequest2ndBufferChannel
		dynamicResponseActions := networkServer.dynamicResponseActions

		networkServer.lifecycleLock.RUnlock()

		bizRequest := <- reliablePushedRequest2ndBufferChannel
		if bizRequest != nil {
			messageId := bizRequest.messageId
			if dynamicResponseActions.Len() > 0 {
				action := dynamicResponseActions.Get(messageId)
				if action != nil {
					if responseActionInfo, ok := action.(*DynamicResponseActionInfo); ok {
						responseActionInfo.responseReceivedLock.Lock()

						if responseActionInfo.isResponseReceived {
							if responseActionInfo.responseBody != nil {
								responseBodyString := string(responseActionInfo.responseBody)
								if responseBodyString == "SUCC" {
									networkServer.receivedReliablePushResponseNumber.Increment()
								} else {
									networkServer.logger.Error("Received failed reliable push response. Response : " + responseBodyString)
									networkServer.deathMessageProcess("2ndBufferQueue", bizRequest, lServerConfig)
								}
							} else {
								networkServer.logger.Error("Received nil reliable push response.")
								networkServer.deathMessageProcess("2ndBufferQueue", bizRequest, lServerConfig)
							}

							dynamicResponseActions.Remove(messageId)
						} else {
							putIntoBufferQueueTime := bizRequest.putInto2ndBufferChannelTime
							timeElapsedFromPutIntoQueue := utils.CurrentTimeMillis() - putIntoBufferQueueTime
							if timeElapsedFromPutIntoQueue >= lServerConfig.DeterminatePushResponseReceivedTimeoutInMillis {
								bizRequest.retryStartTime = utils.CurrentTimeMillis()
								bizRequest.lastRetryTime = -1
								bizRequest.putInto1stBufferChannelTime = -1
								bizRequest.putInto2ndBufferChannelTime = -1
								bizRequestWritingRetryChannel1 <- bizRequest
								networkServer.requestRetryNumber.Increment()
							} else {
								forWaitingTime := lServerConfig.DeterminatePushResponseReceivedTimeoutInMillis - timeElapsedFromPutIntoQueue
								if forWaitingTime < FOR_WAITING_THRESHOLD {
									forWaitingTime = FOR_WAITING_THRESHOLD
								}

								time.Sleep(time.Duration(forWaitingTime) * time.Millisecond)

								action := dynamicResponseActions.Get(messageId)
								if action != nil {
									if responseActionInfo, ok := action.(*DynamicResponseActionInfo); ok {
										responseActionInfo.responseReceivedLock.Lock()

										if responseActionInfo.isResponseReceived {
											if responseActionInfo.responseBody != nil {
												responseBodyString := string(responseActionInfo.responseBody)
												if responseBodyString == "SUCC" {
													networkServer.receivedReliablePushResponseNumber.Increment()
												} else {
													networkServer.logger.Error("Received failed reliable push response in 2ndBufferQueue. Response : " + responseBodyString)
													networkServer.deathMessageProcess("2ndBufferQueue", bizRequest, lServerConfig)
												}
											} else {
												networkServer.logger.Error("Received nil reliable push response in 2ndBufferQueue.")
												networkServer.deathMessageProcess("2ndBufferQueue", bizRequest, lServerConfig)
											}

											dynamicResponseActions.Remove(messageId)
										} else {
											bizRequest.retryStartTime = utils.CurrentTimeMillis()
											bizRequest.lastRetryTime = -1
											bizRequest.putInto1stBufferChannelTime = -1
											bizRequest.putInto2ndBufferChannelTime = -1
											bizRequestWritingRetryChannel1 <- bizRequest
											networkServer.requestRetryNumber.Increment()
										}
									} else {
										networkServer.logger.Error("Can not get response action by message ID [%+v] in 2ndBufferQueue after sleep, Biz request process finished.", messageId)
									}
								} else {
									networkServer.logger.Error("Can not get response action by message ID [%+v] in 2ndBufferQueue after sleep, it is nil, Biz request process finished.", messageId)
								}
							}
						}

						responseActionInfo.responseReceivedLock.Unlock()
					} else {
						networkServer.logger.Error("Can not get response action by message ID [%+v] in 2ndBufferQueue, Biz request process finished.", messageId)
						networkServer.deathMessageProcess("2ndBufferQueue", bizRequest, lServerConfig)
					}
				} else {
					networkServer.logger.Error("Can not get response action by message ID [%+v] in 2ndBufferQueue, it is nil, Biz request process finished.", messageId)
					networkServer.deathMessageProcess("2ndBufferQueue", bizRequest, lServerConfig)
				}
			}
		}
	}

	networkServer.logger.Debug("Process2ndBufferQueueTask finished. MyServerAddress [%+v].", networkServer.myServerDeployAddress)
}

func (networkServer *LongConnNetworkServer) AddRequestAction(actionName string, action IRequestAction) {
	networkServer.namedRpcRequestActions.Put(actionName, action)
}

func (networkServer *LongConnNetworkServer) RemoveRequestAction(actionName string) {
	networkServer.namedRpcRequestActions.Remove(actionName)
}

func (networkServer *LongConnNetworkServer) ClearRequestActions() {
	networkServer.namedRpcRequestActions.Clear()
}

func (networkServer *LongConnNetworkServer) AddConnectionChangedListener(listener IConnectionChangedListener) {
	networkServer.connectionChangedListenersLock.Lock()
	defer networkServer.connectionChangedListenersLock.Unlock()
	networkServer.connectionChangedListeners = append(networkServer.connectionChangedListeners, listener)
}

func (networkServer *LongConnNetworkServer) RemoveConnectionChangedListener(listener IConnectionChangedListener) {
	networkServer.connectionChangedListenersLock.Lock()
	defer networkServer.connectionChangedListenersLock.Unlock()

	if len(networkServer.connectionChangedListeners) == 0 {
		return
	}

	for i, v := range networkServer.connectionChangedListeners {
		if v == listener {
			networkServer.connectionChangedListeners = append(networkServer.connectionChangedListeners[:i], networkServer.connectionChangedListeners[i+1:]...)
			networkServer.RemoveConnectionChangedListener(listener)
			break
		}
	}
}

func (networkServer *LongConnNetworkServer) ClearConnectionChangedListeners() {
	networkServer.connectionChangedListenersLock.Lock()
	defer networkServer.connectionChangedListenersLock.Unlock()

	networkServer.connectionChangedListeners = networkServer.connectionChangedListeners[:0]
}





