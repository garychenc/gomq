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
	"common/protocol"
	"common/utils"
	"errors"
	"server/queue"
	"server/scomm"
	"server/sutils"
	"strconv"
)

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

var (
	CommandTypeMustBeCreateProducerCommandError = errors.New("command type must be CreateProducerCommand error")
	CommandTypeMustBeCreateConsumerCommandError = errors.New("command type must be CreateConsumerCommand error")
	CommandTypeMustBeCloseProducerCommandError = errors.New("command type must be CloseProducerCommand error")
	CommandTypeMustBeCloseConsumerCommandError = errors.New("command type must be CloseConsumerCommand error")
	CommandTypeMustBeProduceMessageCommandError = errors.New("command type must be ProduceMessageCommand error")
	CommandTypeMustBeConsumeMessageCommandError = errors.New("command type must be ConsumeMessageCommand error")
	CommandTypeMustBeConsumeMessageWithTimeoutCommandError = errors.New("command type must be ConsumeMessageWithTimeoutCommand error")
	CommandTypeMustBeConsumeMessageNoWaitCommandError = errors.New("command type must be ConsumeMessageNoWaitCommand error")
	CommandTypeMustBeCommitMessageCommandError = errors.New("command type must be CommitMessageCommand error")
	CommandTypeMustBeResetConsumerCommandError = errors.New("command type must be ResetConsumerCommand error")

)

type BaseMqAction struct {
	mqServer *NetworkMqServer
}

func (action *BaseMqAction) failedResponse(failedErr error) ([]byte, error) {
	resp := &protocol.FailedResponse{Err: failedErr}
	respBytes, err1 := protocol.CommandToBytes(resp)
	if err1 != nil {
		resp.Err = MqServerInternalError
		respBytes, _ = protocol.CommandToBytes(resp)
	}

	return respBytes, nil
}

func (action *BaseMqAction) succResponse() ([]byte, error) {
	resp := &protocol.SuccessfullyResponse{}
	respBytes, _ := protocol.CommandToBytes(resp)
	return respBytes, nil
}

func (action *BaseMqAction) checkMqServerIsStartedAndGetNeededFields(connKey int64, msgId int64,
	actionName string) ([]byte, error, *NetworkMqServer, queue.IQueueContainer, *utils.SequenceGenerator) {
	mqServer := action.mqServer
	mqServer.lifecycleLock.RLock()
	serverStatus := mqServer.serverStatus
	if serverStatus != sutils.Started {
		mqServer.logger.Error("Mq server is not started when invoke %+v. connKey: %+v, msgId: %+v.", actionName, connKey, msgId)
		mqServer.lifecycleLock.RUnlock()
		resp, err := action.failedResponse(MqServerNotStartedError)
		return resp, err, nil, nil, nil
	}

	queueContainer := mqServer.queueContainer
	messageIdGenerator := mqServer.messageIdGenerator
	mqServer.lifecycleLock.RUnlock()

	return nil, nil, mqServer, queueContainer, messageIdGenerator
}

func (action *BaseMqAction) convertBytesToCommand(mqServer *NetworkMqServer, requestBody []byte) ([]byte, error, protocol.IMqCommand) {
	cmd, err := protocol.BytesToCommand(requestBody)
	if err != nil {
		mqServer.logger.Error("Convert request body to mq command failed. Error is : %+v", err)
		resp, err := action.failedResponse(err)
		return resp, err, nil
	}

	return nil, nil, cmd
}

func (action *BaseMqAction) getQueueFromQueueContainer(mqServer *NetworkMqServer, queueContainer queue.IQueueContainer, queueName string) ([]byte, error, queue.IQueue) {
	deployedQueue := queueContainer.GetDeployedQueue(queueName)
	if deployedQueue == nil {
		err := errors.New("can not get deployed queue by queue name : " + queueName + ", please depoly the queue in mq server first.")
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, err, nil
	}

	return nil, nil, deployedQueue
}

func (action *BaseMqAction) getConsumerFromQueueContainer(mqServer *NetworkMqServer, queueContainer queue.IQueueContainer, connKey int64) ([]byte, error, queue.IConsumer, queue.IProducer) {
	consumer, forRecoverProducer := queueContainer.GetConsumer(connKey)
	if consumer == nil {
		err := errors.New("can not get consumer by consumer Id : " + strconv.FormatInt(connKey, 10) + ", please create consumer first.")
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, err, nil, nil
	}

	return nil, nil, consumer, forRecoverProducer
}

type CreateProducerAction struct {
	BaseMqAction
}

func (action *CreateProducerAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction scomm.MessageDeadAction, err error) {
	resp, err, mqServer, queueContainer, _ := action.checkMqServerIsStartedAndGetNeededFields(connKey, msgId, "CreateProducerAction")
	if mqServer == nil {
		return resp, false, nil, err
	}

	resp, err, cmd := action.convertBytesToCommand(mqServer, requestBody)
	if cmd == nil {
		return resp, false, nil, err
	}

	createProducerCommand, ok := cmd.(*protocol.CreateProducerCommand)
	if !ok {
		mqServer.logger.Error(CommandTypeMustBeCreateProducerCommandError.Error())
		resp, err := action.failedResponse(CommandTypeMustBeCreateProducerCommandError)
		return resp, false, nil, err
	}

	queueName := createProducerCommand.QueueName
	resp, err, deployedQueue := action.getQueueFromQueueContainer(mqServer, queueContainer, queueName)
	if deployedQueue == nil {
		return resp, false, nil, err
	}

	_, err2 := queueContainer.CreateProducer(connKey, deployedQueue)
	if err2 != nil {
		err = errors.New("create producer for queue of queue name : " + queueName + " failed, Error is " + err2.Error())
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, false, nil, err
	}

	resp, err = action.succResponse()
	return resp, false, nil, err
}

type CreateConsumerAction struct {
	BaseMqAction
}

func (action *CreateConsumerAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction scomm.MessageDeadAction, err error) {
	resp, err, mqServer, queueContainer, _ := action.checkMqServerIsStartedAndGetNeededFields(connKey, msgId, "CreateConsumerAction")
	if mqServer == nil {
		return resp, false, nil, err
	}

	resp, err, cmd := action.convertBytesToCommand(mqServer, requestBody)
	if cmd == nil {
		return resp, false, nil, err
	}

	createConsumerCommand, ok := cmd.(*protocol.CreateConsumerCommand)
	if !ok {
		mqServer.logger.Error(CommandTypeMustBeCreateConsumerCommandError.Error())
		resp, err := action.failedResponse(CommandTypeMustBeCreateConsumerCommandError)
		return resp, false, nil, err
	}

	queueName := createConsumerCommand.QueueName
	resp, err, deployedQueue := action.getQueueFromQueueContainer(mqServer, queueContainer, queueName)
	if deployedQueue == nil {
		return resp, false, nil, err
	}

	consumerName := createConsumerCommand.ConsumerName
	_, err2 := queueContainer.CreateConsumer(connKey, consumerName, deployedQueue)
	if err2 != nil {
		err = errors.New("create consumer for queue of queue name : " + queueName + " failed, Error is " + err2.Error())
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, false, nil, err
	}

	resp, err = action.succResponse()
	return resp, false, nil, err
}

type CloseProducerAction struct {
	BaseMqAction
}

func (action *CloseProducerAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction scomm.MessageDeadAction, err error) {
	resp, err, mqServer, queueContainer, _ := action.checkMqServerIsStartedAndGetNeededFields(connKey, msgId, "CloseProducerAction")
	if mqServer == nil {
		return resp, false, nil, err
	}

	resp, err, cmd := action.convertBytesToCommand(mqServer, requestBody)
	if cmd == nil {
		return resp, false, nil, err
	}

	_, ok := cmd.(*protocol.CloseProducerCommand)
	if !ok {
		mqServer.logger.Error(CommandTypeMustBeCloseProducerCommandError.Error())
		resp, err := action.failedResponse(CommandTypeMustBeCloseProducerCommandError)
		return resp, false, nil, err
	}

	queueContainer.CloseProducer(connKey)
	resp, err = action.succResponse()
	return resp, false, nil, err
}

func (action *CloseProducerAction) ConnectionAdded(connKey int64) {
}

func (action *CloseProducerAction) ConnectionDisappeared(connKey int64) {
	_, _, mqServer, queueContainer, _ := action.checkMqServerIsStartedAndGetNeededFields(connKey, -1, "CloseProducerAction")
	if mqServer == nil {
		return
	}

	queueContainer.CloseProducer(connKey)
}

type CloseConsumerAction struct {
	BaseMqAction
}

func (action *CloseConsumerAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction scomm.MessageDeadAction, err error) {
	resp, err, mqServer, queueContainer, _ := action.checkMqServerIsStartedAndGetNeededFields(connKey, msgId, "CloseConsumerAction")
	if mqServer == nil {
		return resp, false, nil, err
	}

	resp, err, cmd := action.convertBytesToCommand(mqServer, requestBody)
	if cmd == nil {
		return resp, false, nil, err
	}

	_, ok := cmd.(*protocol.CloseConsumerCommand)
	if !ok {
		mqServer.logger.Error(CommandTypeMustBeCloseConsumerCommandError.Error())
		resp, err := action.failedResponse(CommandTypeMustBeCloseConsumerCommandError)
		return resp, false, nil, err
	}

	queueContainer.CloseConsumer(connKey)
	resp, err = action.succResponse()
	return resp, false, nil, err
}

func (action *CloseConsumerAction) ConnectionAdded(connKey int64) {
}

func (action *CloseConsumerAction) ConnectionDisappeared(connKey int64) {
	_, _, mqServer, queueContainer, _ := action.checkMqServerIsStartedAndGetNeededFields(connKey, -1, "CloseConsumerAction")
	if mqServer == nil {
		return
	}

	queueContainer.CloseConsumer(connKey)
}

type ProduceMessageAction struct {
	BaseMqAction
}

func (action *ProduceMessageAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction scomm.MessageDeadAction, err error) {
	resp, err, mqServer, queueContainer, messageIdGenerator := action.checkMqServerIsStartedAndGetNeededFields(connKey, msgId, "ProduceMessageAction")
	if mqServer == nil {
		return resp, false, nil, err
	}

	resp, err, cmd := action.convertBytesToCommand(mqServer, requestBody)
	if cmd == nil {
		return resp, false, nil, err
	}

	produceMessageCommand, ok := cmd.(*protocol.ProduceMessageCommand)
	if !ok {
		mqServer.logger.Error(CommandTypeMustBeProduceMessageCommandError.Error())
		resp, err := action.failedResponse(CommandTypeMustBeProduceMessageCommandError)
		return resp, false, nil, err
	}

	msgId, msgIdErr := messageIdGenerator.GetSequence()
	if msgIdErr != nil {
		err = errors.New("generate message ID failed, Error is " + msgIdErr.Error())
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, false, nil, err
	}

	producer := queueContainer.GetProducer(connKey)
	if producer == nil {
		err := errors.New("can not get producer by producer Id : " + strconv.FormatInt(connKey, 10) + ", please create producer first.")
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, false, nil, err
	}

	msg := produceMessageCommand.Message.CloneWithNewMsgId(msgId)
	pErr := producer.Produce(msg)
	if pErr != nil {
		err = errors.New("produce message failed, Error is " + pErr.Error())
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, false, nil, err
	}

	cmdResp := &protocol.ProduceMessageResponse{MsgId: msgId}
	respBytes, _ := protocol.CommandToBytes(cmdResp)
	return respBytes, false, nil, nil
}

type ConsumeMessageAction struct {
	BaseMqAction
}

func (action *ConsumeMessageAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction scomm.MessageDeadAction, err error) {
	resp, err, mqServer, queueContainer, _ := action.checkMqServerIsStartedAndGetNeededFields(connKey, msgId, "ConsumeMessageAction")
	if mqServer == nil {
		return resp, false, nil, err
	}

	resp, err, cmd := action.convertBytesToCommand(mqServer, requestBody)
	if cmd == nil {
		return resp, false, nil, err
	}

	_, ok := cmd.(*protocol.ConsumeMessageCommand)
	if !ok {
		mqServer.logger.Error(CommandTypeMustBeConsumeMessageCommandError.Error())
		resp, err := action.failedResponse(CommandTypeMustBeConsumeMessageCommandError)
		return resp, false, nil, err
	}

	resp, err, consumer, forRecoverProducer := action.getConsumerFromQueueContainer(mqServer, queueContainer, connKey)
	if consumer == nil {
		return resp, false, nil, err
	}

	msg, consumeErr := consumer.Consume()
	if consumeErr != nil {
		err = errors.New("consume message failed, Error is " + consumeErr.Error())
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, false, nil, err
	}

	cmdResp := &protocol.ConsumeMessageResponse{Message: msg}
	respBytes, _ := protocol.CommandToBytes(cmdResp)

	if forRecoverProducer != nil {
		msgDeadRecoverFunc := func(connKey int64, msgId int64, msgBody []byte) bool {
			pErr := forRecoverProducer.Produce(msg)
			if pErr != nil {
				err = errors.New("recover a message to queue failed, Error is " + pErr.Error())
				mqServer.logger.Error(err.Error())
				return false
			}

			return true
		}

		return respBytes, true, msgDeadRecoverFunc, nil
	} else {
		return respBytes, true, nil, nil
	}
}

type ConsumeMessageWithTimeoutAction struct {
	BaseMqAction
}

func (action *ConsumeMessageWithTimeoutAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction scomm.MessageDeadAction, err error) {
	resp, err, mqServer, queueContainer, _ := action.checkMqServerIsStartedAndGetNeededFields(connKey, msgId, "ConsumeMessageWithTimeoutAction")
	if mqServer == nil {
		return resp, false, nil, err
	}

	resp, err, cmd := action.convertBytesToCommand(mqServer, requestBody)
	if cmd == nil {
		return resp, false, nil, err
	}

	consumeMessageWithTimeoutCommand, ok := cmd.(*protocol.ConsumeMessageWithTimeoutCommand)
	if !ok {
		mqServer.logger.Error(CommandTypeMustBeConsumeMessageWithTimeoutCommandError.Error())
		resp, err := action.failedResponse(CommandTypeMustBeConsumeMessageWithTimeoutCommandError)
		return resp, false, nil, err
	}

	resp, err, consumer, forRecoverProducer := action.getConsumerFromQueueContainer(mqServer, queueContainer, connKey)
	if consumer == nil {
		return resp, false, nil, err
	}

	msg, consumeErr := consumer.ConsumeWithTimeout(consumeMessageWithTimeoutCommand.Timeout)
	if consumeErr != nil {
		err = errors.New("consume message with timeout failed, Error is " + consumeErr.Error())
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, false, nil, err
	}

	if msg == nil {
		cmdResp := &protocol.ConsumeMessageIsNilResponse{}
		respBytes, _ := protocol.CommandToBytes(cmdResp)
		return respBytes, true, nil, nil
	} else {
		cmdResp := &protocol.ConsumeMessageResponse{Message: msg}
		respBytes, _ := protocol.CommandToBytes(cmdResp)

		if forRecoverProducer != nil {
			msgDeadRecoverFunc := func(connKey int64, msgId int64, msgBody []byte) bool {
				pErr := forRecoverProducer.Produce(msg)
				if pErr != nil {
					err = errors.New("recover a message to queue failed, Error is " + pErr.Error())
					mqServer.logger.Error(err.Error())
					return false
				}

				return true
			}

			return respBytes, true, msgDeadRecoverFunc, nil
		} else {
			return respBytes, true, nil, nil
		}
	}
}

type ConsumeMessageNoWaitAction struct {
	BaseMqAction
}

func (action *ConsumeMessageNoWaitAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction scomm.MessageDeadAction, err error) {
	resp, err, mqServer, queueContainer, _ := action.checkMqServerIsStartedAndGetNeededFields(connKey, msgId, "ConsumeMessageNoWaitAction")
	if mqServer == nil {
		return resp, false, nil, err
	}

	resp, err, cmd := action.convertBytesToCommand(mqServer, requestBody)
	if cmd == nil {
		return resp, false, nil, err
	}

	_, ok := cmd.(*protocol.ConsumeMessageNoWaitCommand)
	if !ok {
		mqServer.logger.Error(CommandTypeMustBeConsumeMessageNoWaitCommandError.Error())
		resp, err := action.failedResponse(CommandTypeMustBeConsumeMessageNoWaitCommandError)
		return resp, false, nil, err
	}

	resp, err, consumer, forRecoverProducer := action.getConsumerFromQueueContainer(mqServer, queueContainer, connKey)
	if consumer == nil {
		return resp, false, nil, err
	}

	msg, consumeErr := consumer.ConsumeNoWait()
	if consumeErr != nil {
		err = errors.New("consume message no wait failed, Error is " + consumeErr.Error())
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, false, nil, err
	}

	if msg == nil {
		cmdResp := &protocol.ConsumeMessageIsNilResponse{}
		respBytes, _ := protocol.CommandToBytes(cmdResp)
		return respBytes, true, nil, nil
	} else {
		cmdResp := &protocol.ConsumeMessageResponse{Message: msg}
		respBytes, _ := protocol.CommandToBytes(cmdResp)

		if forRecoverProducer != nil {
			msgDeadRecoverFunc := func(connKey int64, msgId int64, msgBody []byte) bool {
				pErr := forRecoverProducer.Produce(msg)
				if pErr != nil {
					err = errors.New("recover a message to queue failed, Error is " + pErr.Error())
					mqServer.logger.Error(err.Error())
					return false
				}

				return true
			}

			return respBytes, true, msgDeadRecoverFunc, nil
		} else {
			return respBytes, true, nil, nil
		}
	}
}

type CommitMessageAction struct {
	BaseMqAction
}

func (action *CommitMessageAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction scomm.MessageDeadAction, err error) {
	resp, err, mqServer, queueContainer, _ := action.checkMqServerIsStartedAndGetNeededFields(connKey, msgId, "CommitMessageAction")
	if mqServer == nil {
		return resp, false, nil, err
	}

	resp, err, cmd := action.convertBytesToCommand(mqServer, requestBody)
	if cmd == nil {
		return resp, false, nil, err
	}

	_, ok := cmd.(*protocol.CommitMessageCommand)
	if !ok {
		mqServer.logger.Error(CommandTypeMustBeCommitMessageCommandError.Error())
		resp, err := action.failedResponse(CommandTypeMustBeCommitMessageCommandError)
		return resp, false, nil, err
	}

	resp, err, consumer, _ := action.getConsumerFromQueueContainer(mqServer, queueContainer, connKey)
	if consumer == nil {
		return resp, false, nil, err
	}

	commitErr := consumer.Commit()
	if commitErr != nil {
		err = errors.New("commit message failed, Error is " + commitErr.Error())
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, false, nil, err
	}

	resp, err = action.succResponse()
	return resp, true, nil, err
}

type ResetConsumerAction struct {
	BaseMqAction
}

func (action *ResetConsumerAction) RequestReceived(connKey int64, msgId int64, requestBody []byte) (processResult []byte, usingReliablePush bool, messageDeadAction scomm.MessageDeadAction, err error) {
	resp, err, mqServer, queueContainer, _ := action.checkMqServerIsStartedAndGetNeededFields(connKey, msgId, "ResetConsumerAction")
	if mqServer == nil {
		return resp, false, nil, err
	}

	resp, err, cmd := action.convertBytesToCommand(mqServer, requestBody)
	if cmd == nil {
		return resp, false, nil, err
	}

	_, ok := cmd.(*protocol.ResetConsumerCommand)
	if !ok {
		mqServer.logger.Error(CommandTypeMustBeResetConsumerCommandError.Error())
		resp, err := action.failedResponse(CommandTypeMustBeResetConsumerCommandError)
		return resp, false, nil, err
	}

	resp, err, consumer, _ := action.getConsumerFromQueueContainer(mqServer, queueContainer, connKey)
	if consumer == nil {
		return resp, false, nil, err
	}

	resetErr := consumer.Reset()
	if resetErr != nil {
		err = errors.New("reset consumer failed, Error is " + resetErr.Error())
		mqServer.logger.Error(err.Error())
		resp, err := action.failedResponse(err)
		return resp, false, nil, err
	}

	resp, err = action.succResponse()
	return resp, false, nil, err
}







































