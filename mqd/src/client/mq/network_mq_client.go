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
	"client/ccomm"
	"common/config"
	"common/protocol"
	"common/utils"
	"strconv"
	"strings"
	"sync"
)

type NetworkMqClient struct {
	serverAddress string
	sendRequestTimeout int64
	logConfig *config.LogConfig
	idGenerator *utils.SequenceGenerator
}

const (
	CreateProducerActionName            = "action.create.producer"
	CreateConsumerActionName            = "action.create.consumer"
	CloseProducerActionName             = "action.close.producer"
	CloseConsumerActionName             = "action.close.consumer"
	ProduceMessageActionName            = "action.produce.message"
	ConsumeMessageActionName            = "action.consume.message"
	ConsumeMessageWithTimeoutActionName = "action.consume.message.with.timeout"
	ConsumeMessageNoWaitActionName      = "action.consume.message.no.wait"
	CommitMessageActionName             = "action.commit.message"
	ResetConsumerActionName             = "action.reset.consumer"
)

/*
 *
 * Create a network base MQ client.
 *
 * parameters :
 * mqServerAddress : address of the MQ server. The format of the address string is IP:port. For example : 192.168.0.1:168000.
 * sendRequestTimeout : timeout for requesting MQ server data.
 *
 * return :
 * An new MQ client instance.
 *
 */
func NewNetworkMqClient(mqServerAddress string, sendRequestTimeout int64) IMqClient {
	if mqServerAddress = strings.Trim(mqServerAddress, " "); len(mqServerAddress) == 0 {
		panic("mqServerAddress can not be empty")
	}

	idGen, err := utils.NewSequenceGenerator()
	if err != nil {
		panic("Create id generator failed")
	}

	return &NetworkMqClient{serverAddress: mqServerAddress, sendRequestTimeout: sendRequestTimeout, logConfig: nil, idGenerator: idGen}
}

/*
 *
 * Create a network base MQ client.
 *
 * parameters :
 * mqServerAddress : address of the MQ server. The format of the address string is IP:port. For example : 192.168.0.1:168000.
 * sendRequestTimeout : timeout for requesting MQ server data.
 * logConfig : configure the log behavior of the MQ client.
 *
 * return :
 * An new MQ client instance.
 *
 */
func NewNetworkMqClientWithLogConfig(mqServerAddress string, sendRequestTimeout int64, logConfig *config.LogConfig) IMqClient {
	if mqServerAddress = strings.Trim(mqServerAddress, " "); len(mqServerAddress) == 0 {
		panic("mqServerAddress can not be empty")
	}

	idGen, err := utils.NewSequenceGenerator()
	if err != nil {
		panic("Create id generator failed")
	}

	return &NetworkMqClient{serverAddress: mqServerAddress, sendRequestTimeout: sendRequestTimeout, logConfig: logConfig, idGenerator: idGen}
}

func (mqClient *NetworkMqClient) CreateProducer(queueName string) (producer IProducer, err error) {
	if queueName = strings.Trim(queueName, " "); len(queueName) == 0 {
		return nil, QueueNameIsEmptyError
	}

	networkClientConfig := mqClient.createNetworkClientConfig()
	producerClientId, err := mqClient.idGenerator.GetSequence()
	if err != nil {
		return nil, err
	}

	networkClient := ccomm.NewLongConnNetworkClient(networkClientConfig)
	err = networkClient.ConnectToServer("admin", "admin", strconv.FormatInt(producerClientId, 10))
	if err != nil {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, err
	}

	cmd := &protocol.CreateProducerCommand{QueueName: queueName}
	cmdBytes, err := protocol.CommandToBytes(cmd)
	if err != nil {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, err
	}

	rpcResponse, err := networkClient.RPC(CreateProducerActionName, cmdBytes, -1)
	if err != nil {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, err
	}

	respCmd, err := protocol.BytesToCommand(rpcResponse)
	if err != nil {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, err
	}

	if _, ok := respCmd.(*protocol.SuccessfullyResponse); ok {
		return &NetworkProducer{networkClient: networkClient, mqClient: mqClient, queueName: queueName}, nil
	} else if failedResp, ok := respCmd.(*protocol.FailedResponse); ok {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, failedResp.Err
	} else {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, UnsupportedRpcResponseError
	}
}

func (mqClient *NetworkMqClient) CreateConsumer(consumerName string, queueName string) (consumer IConsumer, err error) {
	if consumerName = strings.Trim(consumerName, " "); len(consumerName) == 0 {
		return nil, ConsumerNameIsEmptyError
	}

	if queueName = strings.Trim(queueName, " "); len(queueName) == 0 {
		return nil, QueueNameIsEmptyError
	}

	networkClientConfig := mqClient.createNetworkClientConfig()
	consumerClientId, err := mqClient.idGenerator.GetSequence()
	if err != nil {
		return nil, err
	}

	networkClient := ccomm.NewLongConnNetworkClient(networkClientConfig)
	err = networkClient.ConnectToServer("admin", "admin", strconv.FormatInt(consumerClientId, 10))
	if err != nil {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, err
	}

	cmd := &protocol.CreateConsumerCommand{QueueName: queueName, ConsumerName: consumerName}
	cmdBytes, err := protocol.CommandToBytes(cmd)
	if err != nil {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, err
	}

	rpcResponse, err := networkClient.RPC(CreateConsumerActionName, cmdBytes, -1)
	if err != nil {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, err
	}

	respCmd, err := protocol.BytesToCommand(rpcResponse)
	if err != nil {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, err
	}

	if _, ok := respCmd.(*protocol.SuccessfullyResponse); ok {
		return &NetworkConsumer{networkClient: networkClient, defaultRpcTimeoutInMillis: networkClientConfig.DefaultRpcTimeoutInMillis,
			mqClient: mqClient, consumerName: consumerName, queueName: queueName}, nil
	} else if failedResp, ok := respCmd.(*protocol.FailedResponse); ok {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, failedResp.Err
	} else {
		if networkClient != nil {
			networkClient.Disconnect()
		}

		return nil, UnsupportedRpcResponseError
	}
}

func (mqClient *NetworkMqClient) createNetworkClientConfig() *ccomm.LongConnNetworkClientConfig {
	networkClientConfig := ccomm.NewLongConnNetworkClientDefaultConfig()
	networkClientConfig.ServerAddress = mqClient.serverAddress
	if mqClient.logConfig != nil {
		networkClientConfig.LogConfig = mqClient.logConfig
	}

	if mqClient.sendRequestTimeout > 0 {
		networkClientConfig.DefaultRpcTimeoutInMillis = mqClient.sendRequestTimeout
	}

	return networkClientConfig
}

type NetworkProducer struct {
	networkClient ccomm.INetworkClient
	lock          sync.RWMutex

	mqClient      *NetworkMqClient
	queueName     string
}

func (producer *NetworkProducer) Produce(message protocol.IMessage) (msgId int64, err error) {
	if message == nil {
		return -1, MessageIsNilError
	}

	for {
		producer.lock.RLock()

		networkClient := producer.networkClient
		mqClient      := producer.mqClient
		queueName     := producer.queueName
		if networkClient == nil {
			producer.lock.RUnlock()
			return -1, ProducerIsClosedError
		}

		producer.lock.RUnlock()

		cmd := &protocol.ProduceMessageCommand{Message: message}
		cmdBytes, err := protocol.CommandToBytes(cmd)
		if err != nil {
			return -1, err
		}

		rpcResponse, err := networkClient.RPC(ProduceMessageActionName, cmdBytes, -1)
		if err != nil {
			return -1, err
		}

		respCmd, err := protocol.BytesToCommand(rpcResponse)
		if err != nil {
			return -1, err
		}

		if produceMessageResponse, ok := respCmd.(*protocol.ProduceMessageResponse); ok {
			return produceMessageResponse.MsgId, nil
		} else if failedResp, ok := respCmd.(*protocol.FailedResponse); ok {
			if strings.Contains(failedResp.Err.Error(), "please create producer first.") {
				producer.Close()

				producer.lock.Lock()

				networkClient := producer.networkClient
				if networkClient == nil {
					newProducer, err := mqClient.CreateProducer(queueName)
					if err != nil {
						producer.lock.Unlock()
						return -1, err
					}

					newNetworkProducer := newProducer.(*NetworkProducer)
					producer.networkClient = newNetworkProducer.networkClient
				}

				producer.lock.Unlock()
				continue
			} else {
				return -1, failedResp.Err
			}
		} else {
			return -1, UnsupportedRpcResponseError
		}
	}
}

func (producer *NetworkProducer) Close() error {
	producer.lock.Lock()
	defer producer.lock.Unlock()

	networkClient := producer.networkClient
	if networkClient == nil {
		return nil
	}

	cmd := &protocol.CloseProducerCommand{}
	cmdBytes, _ := protocol.CommandToBytes(cmd)
	networkClient.RPC(CloseProducerActionName, cmdBytes, -1)

	networkClient.Disconnect()
	producer.networkClient = nil
	return nil
}

type NetworkConsumer struct {
	networkClient             ccomm.INetworkClient
	defaultRpcTimeoutInMillis int64
	lock                      sync.RWMutex

	mqClient     *NetworkMqClient
	consumerName string
	queueName    string
}

func (consumer *NetworkConsumer) Consume() (msg protocol.IMessage, err error) {
	for {
		consumer.lock.RLock()

		networkClient := consumer.networkClient
		mqClient      := consumer.mqClient
		consumerName  := consumer.consumerName
		queueName     := consumer.queueName
		defaultRpcTimeoutInMillis := consumer.defaultRpcTimeoutInMillis
		if networkClient == nil {
			consumer.lock.RUnlock()
			return nil, ConsumerIsClosedError
		}

		consumer.lock.RUnlock()

		cmd := &protocol.ConsumeMessageWithTimeoutCommand{Timeout: defaultRpcTimeoutInMillis}
		cmdBytes, err := protocol.CommandToBytes(cmd)
		if err != nil {
			return nil, err
		}

		rpcResponse, err := networkClient.RPC(ConsumeMessageWithTimeoutActionName, cmdBytes, defaultRpcTimeoutInMillis + 1000)
		if err != nil {
			return nil, err
		}

		respCmd, err := protocol.BytesToCommand(rpcResponse)
		if err != nil {
			return nil, err
		}

		if consumeMessageResponse, ok := respCmd.(*protocol.ConsumeMessageResponse); ok {
			return consumeMessageResponse.Message, nil
		} else if _, ok := respCmd.(*protocol.ConsumeMessageIsNilResponse); ok {
			continue
		} else if failedResp, ok := respCmd.(*protocol.FailedResponse); ok {
			if strings.Contains(failedResp.Err.Error(), "please create consumer first.") {
				consumer.Close()

				consumer.lock.Lock()

				networkClient := consumer.networkClient
				if networkClient == nil {
					newConsumer, err := mqClient.CreateConsumer(consumerName, queueName)
					if err != nil {
						consumer.lock.Unlock()
						return nil, err
					}

					newNetworkConsumer := newConsumer.(*NetworkConsumer)
					consumer.networkClient = newNetworkConsumer.networkClient
					consumer.defaultRpcTimeoutInMillis = newNetworkConsumer.defaultRpcTimeoutInMillis
				}

				consumer.lock.Unlock()
				continue
			} else {
				return nil, failedResp.Err
			}
		} else {
			return nil, UnsupportedRpcResponseError
		}
	}
}

func (consumer *NetworkConsumer) ConsumeWithTimeout(timeout int64) (msg protocol.IMessage, err error) {
	if timeout <= 0 {
		return consumer.Consume()
	} else {
		for {
			consumer.lock.RLock()

			networkClient := consumer.networkClient
			mqClient      := consumer.mqClient
			consumerName  := consumer.consumerName
			queueName     := consumer.queueName
			if networkClient == nil {
				consumer.lock.RUnlock()
				return nil, ConsumerIsClosedError
			}

			consumer.lock.RUnlock()

			cmd := &protocol.ConsumeMessageWithTimeoutCommand{Timeout: timeout}
			cmdBytes, err := protocol.CommandToBytes(cmd)
			if err != nil {
				return nil, err
			}

			rpcResponse, err := networkClient.RPC(ConsumeMessageWithTimeoutActionName, cmdBytes, timeout + 1000)
			if err != nil {
				return nil, err
			}

			respCmd, err := protocol.BytesToCommand(rpcResponse)
			if err != nil {
				return nil, err
			}

			if consumeMessageResponse, ok := respCmd.(*protocol.ConsumeMessageResponse); ok {
				return consumeMessageResponse.Message, nil
			} else if _, ok := respCmd.(*protocol.ConsumeMessageIsNilResponse); ok {
				return nil, nil
			} else if failedResp, ok := respCmd.(*protocol.FailedResponse); ok {
				if strings.Contains(failedResp.Err.Error(), "please create consumer first.") {
					consumer.Close()

					consumer.lock.Lock()

					networkClient := consumer.networkClient
					if networkClient == nil {
						newConsumer, err := mqClient.CreateConsumer(consumerName, queueName)
						if err != nil {
							consumer.lock.Unlock()
							return nil, err
						}

						newNetworkConsumer := newConsumer.(*NetworkConsumer)
						consumer.networkClient = newNetworkConsumer.networkClient
						consumer.defaultRpcTimeoutInMillis = newNetworkConsumer.defaultRpcTimeoutInMillis
					}

					consumer.lock.Unlock()
					continue
				} else {
					return nil, failedResp.Err
				}
			} else {
				return nil, UnsupportedRpcResponseError
			}
		}
	}
}

func (consumer *NetworkConsumer) ConsumeNoWait() (msg protocol.IMessage, err error) {
	for {
		consumer.lock.RLock()

		networkClient := consumer.networkClient
		mqClient      := consumer.mqClient
		consumerName  := consumer.consumerName
		queueName     := consumer.queueName
		if networkClient == nil {
			consumer.lock.RUnlock()
			return nil, ConsumerIsClosedError
		}

		consumer.lock.RUnlock()

		cmd := &protocol.ConsumeMessageNoWaitCommand{}
		cmdBytes, err := protocol.CommandToBytes(cmd)
		if err != nil {
			return nil, err
		}

		rpcResponse, err := networkClient.RPC(ConsumeMessageNoWaitActionName, cmdBytes, -1)
		if err != nil {
			return nil, err
		}

		respCmd, err := protocol.BytesToCommand(rpcResponse)
		if err != nil {
			return nil, err
		}

		if consumeMessageResponse, ok := respCmd.(*protocol.ConsumeMessageResponse); ok {
			return consumeMessageResponse.Message, nil
		} else if _, ok := respCmd.(*protocol.ConsumeMessageIsNilResponse); ok {
			return nil, nil
		} else if failedResp, ok := respCmd.(*protocol.FailedResponse); ok {
			if strings.Contains(failedResp.Err.Error(), "please create consumer first.") {
				consumer.Close()

				consumer.lock.Lock()

				networkClient := consumer.networkClient
				if networkClient == nil {
					newConsumer, err := mqClient.CreateConsumer(consumerName, queueName)
					if err != nil {
						consumer.lock.Unlock()
						return nil, err
					}

					newNetworkConsumer := newConsumer.(*NetworkConsumer)
					consumer.networkClient = newNetworkConsumer.networkClient
					consumer.defaultRpcTimeoutInMillis = newNetworkConsumer.defaultRpcTimeoutInMillis
				}

				consumer.lock.Unlock()
				continue
			} else {
				return nil, failedResp.Err
			}
		} else {
			return nil, UnsupportedRpcResponseError
		}
	}
}

func (consumer *NetworkConsumer) Commit() error {
	for {
		consumer.lock.RLock()

		networkClient := consumer.networkClient
		mqClient      := consumer.mqClient
		consumerName  := consumer.consumerName
		queueName     := consumer.queueName
		if networkClient == nil {
			consumer.lock.RUnlock()
			return ConsumerIsClosedError
		}

		consumer.lock.RUnlock()

		cmd := &protocol.CommitMessageCommand{}
		cmdBytes, err := protocol.CommandToBytes(cmd)
		if err != nil {
			return err
		}

		rpcResponse, err := networkClient.RPC(CommitMessageActionName, cmdBytes, -1)
		if err != nil {
			return err
		}

		respCmd, err := protocol.BytesToCommand(rpcResponse)
		if err != nil {
			return err
		}

		if _, ok := respCmd.(*protocol.SuccessfullyResponse); ok {
			return nil
		} else if failedResp, ok := respCmd.(*protocol.FailedResponse); ok {
			if strings.Contains(failedResp.Err.Error(), "please create consumer first.") {
				consumer.Close()

				consumer.lock.Lock()

				networkClient := consumer.networkClient
				if networkClient == nil {
					newConsumer, err := mqClient.CreateConsumer(consumerName, queueName)
					if err != nil {
						consumer.lock.Unlock()
						return err
					}

					newNetworkConsumer := newConsumer.(*NetworkConsumer)
					consumer.networkClient = newNetworkConsumer.networkClient
					consumer.defaultRpcTimeoutInMillis = newNetworkConsumer.defaultRpcTimeoutInMillis
				}

				consumer.lock.Unlock()
				continue
			} else {
				return failedResp.Err
			}
		} else {
			return UnsupportedRpcResponseError
		}
	}
}

func (consumer *NetworkConsumer) Reset() error {
	for {
		consumer.lock.RLock()

		networkClient := consumer.networkClient
		mqClient      := consumer.mqClient
		consumerName  := consumer.consumerName
		queueName     := consumer.queueName
		if networkClient == nil {
			consumer.lock.RUnlock()
			return ConsumerIsClosedError
		}

		consumer.lock.RUnlock()

		cmd := &protocol.ResetConsumerCommand{}
		cmdBytes, err := protocol.CommandToBytes(cmd)
		if err != nil {
			return err
		}

		rpcResponse, err := networkClient.RPC(ResetConsumerActionName, cmdBytes, -1)
		if err != nil {
			return err
		}

		respCmd, err := protocol.BytesToCommand(rpcResponse)
		if err != nil {
			return err
		}

		if _, ok := respCmd.(*protocol.SuccessfullyResponse); ok {
			return nil
		} else if failedResp, ok := respCmd.(*protocol.FailedResponse); ok {
			if strings.Contains(failedResp.Err.Error(), "please create consumer first.") {
				consumer.Close()

				consumer.lock.Lock()

				networkClient := consumer.networkClient
				if networkClient == nil {
					newConsumer, err := mqClient.CreateConsumer(consumerName, queueName)
					if err != nil {
						consumer.lock.Unlock()
						return err
					}

					newNetworkConsumer := newConsumer.(*NetworkConsumer)
					consumer.networkClient = newNetworkConsumer.networkClient
					consumer.defaultRpcTimeoutInMillis = newNetworkConsumer.defaultRpcTimeoutInMillis
				}

				consumer.lock.Unlock()
				continue
			} else {
				return failedResp.Err
			}
		} else {
			return UnsupportedRpcResponseError
		}
	}
}

func (consumer *NetworkConsumer) Close() error {
	consumer.lock.Lock()
	defer consumer.lock.Unlock()

	networkClient := consumer.networkClient
	if networkClient == nil {
		return nil
	}

	cmd := &protocol.CloseConsumerCommand{}
	cmdBytes, _ := protocol.CommandToBytes(cmd)
	networkClient.RPC(CloseConsumerActionName, cmdBytes, -1)

	networkClient.Disconnect()
	consumer.networkClient = nil
	return nil
}



































