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
	"common/config"
	"common/utils"
	"errors"
	"github.com/astaxie/beego/logs"
	"server/queue"
	"server/scomm"
	"server/sutils"
	"sync"
)

// Thread safe
type IMqServer interface {
	sutils.IServer
}

type NetworkMqServer struct {
	// Final Fields, initialized by New function
	logger        *logs.BeeLogger
	serverConfig  *NetworkMqServerConfig
	lifecycleLock sync.RWMutex

	// Changable Files, initialized by Start function
	serverStatus       byte
	messageIdGenerator *utils.SequenceGenerator
	queueContainer     queue.IQueueContainer
	networkServer      scomm.INetworkServer
}

type NetworkMqServerConfig struct {
	LogConfig *config.LogConfig
	NetworkServerConfig *scomm.LongConnNetworkServerConfig
	IsSyncFileInEveryOpt bool
	DeployedQueueConfigs []queue.IQueueConfig
}

var (
	MqServerConfigIsNilError = errors.New("server config is nil error")
	MqServerInternalError = errors.New("mq server internal error")
	MqServerNotStartedError = errors.New("mq server not started error")
)

func NewNetworkMqServer(serverConfig *NetworkMqServerConfig) IMqServer {
	mqServer := &NetworkMqServer{}
	mqServer.logger = logs.NewLogger(serverConfig.LogConfig.AsyncLogChanLength)
	mqServer.logger.SetLevel(serverConfig.LogConfig.Level)
	mqServer.logger.SetLogger(serverConfig.LogConfig.AdapterName, serverConfig.LogConfig.AdapterParameters)
	mqServer.logger.SetLogger(logs.AdapterConsole, "{}")
	if serverConfig.LogConfig.IsAsync {
		mqServer.logger.Async(serverConfig.LogConfig.AsyncLogChanLength)
	}

	mqServer.serverConfig = serverConfig
	return mqServer
}

func NewNetworkMqServerConfig(deployedQueueNumber int) *NetworkMqServerConfig {
	mqServerConfig := &NetworkMqServerConfig{}
	mqServerConfig.LogConfig = &config.LogConfig{}
	mqServerConfig.NetworkServerConfig = scomm.NewLongConnNetworkServerDefaultConfig()
	mqServerConfig.DeployedQueueConfigs = make([]queue.IQueueConfig, deployedQueueNumber)
	return mqServerConfig
}

func (mqServer *NetworkMqServer) Start() error {
	mqServer.lifecycleLock.Lock()
	defer mqServer.lifecycleLock.Unlock()

	if mqServer.serverStatus == sutils.Started {
		mqServer.logger.Info("MQ Server Already Started.")
		return nil
	}

	serverConfig := mqServer.serverConfig
	if serverConfig == nil {
		mqServer.logger.Error("Can not get NetworkMqServerConfig object, Start failed.")
		return MqServerConfigIsNilError
	}

	mqServer.serverStatus = sutils.Starting
	mqServer.logger.Info("MQ server is starting.")

	msgIdGen, err := utils.NewSequenceGenerator()
	if err != nil {
		mqServer.logger.Error("Create message id sequence generator failed, Error is : %+v.", err)
		return err
	}

	mqServer.messageIdGenerator = msgIdGen
	mqServer.queueContainer = queue.NewFileQueueContainer(serverConfig.LogConfig, serverConfig.IsSyncFileInEveryOpt)
	mqServer.networkServer = scomm.NewLongConnNetworkServer(serverConfig.NetworkServerConfig)

	if err := mqServer.queueContainer.Start(); err != nil {
		mqServer.logger.Error("Start queue container failed in MQ server. Error is : %+v", err)
		return err
	} else {
		mqServer.logger.Info("Queue container had been started successfully.")
	}

	for _, queueConfig := range serverConfig.DeployedQueueConfigs {
		_, err := mqServer.queueContainer.DeployQueue(queueConfig)
		if err != nil {
			mqServer.logger.Error("Deploy a queue failed. Queue Config : %+v.", queueConfig.String())
			mqServer.logger.Info("Going to deploy next queue.")
		} else {
			mqServer.logger.Info("Deploy a queue successfully. Queue Config : %+v.", queueConfig.String())
		}
	}

	mqServer.networkServer.AddRequestAction(CreateProducerActionName,            &CreateProducerAction{BaseMqAction{mqServer: mqServer}})
	mqServer.networkServer.AddRequestAction(CreateConsumerActionName,            &CreateConsumerAction{BaseMqAction{mqServer: mqServer}})
	mqServer.networkServer.AddRequestAction(CloseProducerActionName,             &CloseProducerAction{BaseMqAction{mqServer: mqServer}})
	mqServer.networkServer.AddRequestAction(CloseConsumerActionName,             &CloseConsumerAction{BaseMqAction{mqServer: mqServer}})
	mqServer.networkServer.AddRequestAction(ProduceMessageActionName,            &ProduceMessageAction{BaseMqAction{mqServer: mqServer}})
	mqServer.networkServer.AddRequestAction(ConsumeMessageActionName,            &ConsumeMessageAction{BaseMqAction{mqServer: mqServer}})
	mqServer.networkServer.AddRequestAction(ConsumeMessageWithTimeoutActionName, &ConsumeMessageWithTimeoutAction{BaseMqAction{mqServer: mqServer}})
	mqServer.networkServer.AddRequestAction(ConsumeMessageNoWaitActionName,      &ConsumeMessageNoWaitAction{BaseMqAction{mqServer: mqServer}})
	mqServer.networkServer.AddRequestAction(CommitMessageActionName,             &CommitMessageAction{BaseMqAction{mqServer: mqServer}})
	mqServer.networkServer.AddRequestAction(ResetConsumerActionName,             &ResetConsumerAction{BaseMqAction{mqServer: mqServer}})

	mqServer.networkServer.AddConnectionChangedListener(&CloseProducerAction{BaseMqAction{mqServer: mqServer}})
	mqServer.networkServer.AddConnectionChangedListener(&CloseConsumerAction{BaseMqAction{mqServer: mqServer}})

	if err := mqServer.networkServer.Start(); err != nil {
		mqServer.logger.Error("Start network server failed in MQ server. Error is : %+v", err)
		return err
	} else {
		mqServer.logger.Info("Network server had been started successfully.")
	}

	mqServer.serverStatus = sutils.Started
	mqServer.logger.Info("MQ server had been started successfully.")
	return nil
}

func (mqServer *NetworkMqServer) Stop() error {
	mqServer.lifecycleLock.Lock()
	defer mqServer.lifecycleLock.Unlock()

	if mqServer.serverStatus == sutils.Stopped {
		mqServer.logger.Info("MQ server Already Stopped.")
		return nil
	}

	mqServer.serverStatus = sutils.Stopping
	mqServer.logger.Info("MQ server is stopping.")

	networkServer := mqServer.networkServer
	if networkServer != nil {
		if err := networkServer.Stop(); err != nil {
			mqServer.logger.Error("Stop network server failed in MQ server. Error is : %+v", err)
		} else {
			mqServer.logger.Info("Network server had been stopped successfully.")
		}

		networkServer.ClearConnectionChangedListeners()
		networkServer.ClearRequestActions()
		mqServer.networkServer = nil
	} else {
		mqServer.logger.Info("Network server is not started.")
	}

	queueContainer := mqServer.queueContainer
	if queueContainer != nil {
		if err := queueContainer.Stop(); err != nil {
			mqServer.logger.Error("Stop queue container failed in MQ server. Error is : %+v", err)
		} else {
			mqServer.logger.Info("Queue container had been stopped successfully.")
		}

		mqServer.queueContainer = nil
	} else {
		mqServer.logger.Info("Queue container is not started.")
	}

	mqServer.serverStatus = sutils.Stopped
	mqServer.logger.Info("MQ server is stopped successfully.")

	return nil
}




