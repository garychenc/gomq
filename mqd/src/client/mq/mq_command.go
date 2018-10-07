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
	"errors"
	"strings"
)

type IMqCommand interface {
	toBytes() ([]byte, error)
}

type CreateProducerCommand struct {
	QueueName string
}

type CreateConsumerCommand struct {
	QueueName string
	ConsumerName string
}

type CloseProducerCommand struct {
}

type CloseConsumerCommand struct {
}

type SuccessfullyResponse struct {
}

type ProduceMessageCommand struct {
	Message IMessage
}

type ProduceMessageResponse struct {
	MsgId int64
}

type FailedResponse struct {
	Err error
}

type ConsumeMessageCommand struct {
}

type ConsumeMessageResponse struct {
	Message IMessage
}

type ConsumeMessageIsNilResponse struct {
}

type ConsumeMessageWithTimeoutCommand struct {
	Timeout int64
}

type ConsumeMessageNoWaitCommand struct {
}

type CommitMessageCommand struct {
}

type ResetConsumerCommand struct {
}

var (
	CommandIsNullError = errors.New("command can not be nil")
	CommandBytesIsNullError = errors.New("command bytes can not be nil")
	NotSupportedCommandTypeError = errors.New("not supported command type")
	MessageIsNullError = errors.New("message can not be nil")
	FailedReasonIsNullError = errors.New("failed reason can not be nil")
)

const (
	CommandTypeCreateProducer              = 0x01
	CommandTypeCreateConsumer              = 0x02
	CommandTypeCloseProducer               = 0x03
	CommandTypeCloseConsumer               = 0x04
	CommandTypeProduceMessage              = 0x05
	CommandTypeSuccessfullyResponse        = 0x06
	CommandTypeProduceMessageResponse      = 0x07
	CommandTypeFailedResponse              = 0x08
	CommandTypeConsumeMessage              = 0x09
	CommandTypeConsumeMessageResponse      = 0x0A
	CommandTypeConsumeMessageWithTimeout   = 0x0B
	CommandTypeConsumeMessageNoWait        = 0x0C
	CommandTypeCommitMessage               = 0x0D
	CommandTypeResetConsumer               = 0x0E
	CommandTypeConsumeMessageIsNilResponse = 0x0F
)

func CommandToBytes(command IMqCommand) ([]byte, error) {
	if command == nil {
		return nil, CommandIsNullError
	}

	return command.toBytes()
}

func BytesToCommand(bytes []byte) (IMqCommand, error) {
	if bytes == nil || len(bytes) == 0 {
		return nil, CommandBytesIsNullError
	}

	cmdBytesPackage := NewIoBuffer(bytes)
	cmdType, err := cmdBytesPackage.ReadByte()
	if err != nil {
		return nil, err
	}

	switch cmdType {

		case CommandTypeCreateProducer: {
			queueNameBytesLen, err := cmdBytesPackage.ReadByte()
			if err != nil {
				return nil, err
			}

			queueNameBytes := cmdBytesPackage.Next(int(queueNameBytesLen))
			queueName := string(queueNameBytes)
			return &CreateProducerCommand{QueueName: queueName}, nil
		}

		case CommandTypeCreateConsumer: {
			queueNameBytesLen, err := cmdBytesPackage.ReadByte()
			if err != nil {
				return nil, err
			}

			queueNameBytes := cmdBytesPackage.Next(int(queueNameBytesLen))
			queueName := string(queueNameBytes)

			consumerNameBytesLen, err := cmdBytesPackage.ReadByte()
			if err != nil {
				return nil, err
			}

			consumerNameBytes := cmdBytesPackage.Next(int(consumerNameBytesLen))
			consumerName := string(consumerNameBytes)
			return &CreateConsumerCommand{QueueName: queueName, ConsumerName: consumerName}, nil
		}

		case CommandTypeCloseProducer: {
			return &CloseProducerCommand{}, nil
		}

		case CommandTypeCloseConsumer: {
			return &CloseConsumerCommand{}, nil
		}

		case CommandTypeProduceMessage: {
			msgBytes := cmdBytesPackage.Next(len(bytes) - 1)
			msg, err := BytesToMessage(msgBytes)
			if err != nil {
				return nil, err
			}

			return &ProduceMessageCommand{Message: msg}, nil
		}

		case CommandTypeSuccessfullyResponse: {
			return &SuccessfullyResponse{}, nil
		}

		case CommandTypeFailedResponse: {
			errStrBytes := cmdBytesPackage.Next(len(bytes) - 1)
			errString := string(errStrBytes)
			err := errors.New(errString)
			return &FailedResponse{Err: err}, nil
		}

		case CommandTypeProduceMessageResponse: {
			msgIdBytes := cmdBytesPackage.Next(MsgIdBytesSize)
			msgId := Combine8BytesArray2Int64(msgIdBytes)
			return &ProduceMessageResponse{MsgId: msgId}, nil
		}

		case CommandTypeConsumeMessage: {
			return &ConsumeMessageCommand{}, nil
		}

		case CommandTypeConsumeMessageResponse: {
			msgBytes := cmdBytesPackage.Next(len(bytes) - 1)
			msg, err := BytesToMessage(msgBytes)
			if err != nil {
				return nil, err
			}

			return &ConsumeMessageResponse{Message: msg}, nil
		}

		case CommandTypeConsumeMessageWithTimeout: {
			timeoutBytes := cmdBytesPackage.Next(8)
			timeout := Combine8BytesArray2Int64(timeoutBytes)
			return &ConsumeMessageWithTimeoutCommand{Timeout: timeout}, nil
		}

		case CommandTypeConsumeMessageNoWait: {
			return &ConsumeMessageNoWaitCommand{}, nil
		}

		case CommandTypeCommitMessage: {
			return &CommitMessageCommand{}, nil
		}

		case CommandTypeResetConsumer: {
			return &ResetConsumerCommand{}, nil
		}

		case CommandTypeConsumeMessageIsNilResponse: {
			return &ConsumeMessageIsNilResponse{}, nil
		}

		default: {
			return nil, NotSupportedCommandTypeError
		}
	}
}

func (cmd *CreateProducerCommand) toBytes() ([]byte, error) {
	queueName := cmd.QueueName
	if len(strings.Trim(queueName, " ")) <= 0 {
		return nil, QueueNameIsEmptyError
	}

	queueNameBytes := []byte(queueName)
	cmdBytes := make([]byte, len(queueNameBytes) + 2)
	cmdBytes[0] = CommandTypeCreateProducer
	cmdBytes[1] = byte(len(queueNameBytes))
	cmdBytesForCopy := cmdBytes[2:]
	copy(cmdBytesForCopy, queueNameBytes)
	return cmdBytes, nil
}

func (cmd *CreateConsumerCommand) toBytes() ([]byte, error) {
	queueName := cmd.QueueName
	consumerName := cmd.ConsumerName
	if len(strings.Trim(queueName, " ")) <= 0 {
		return nil, QueueNameIsEmptyError
	}

	if len(strings.Trim(consumerName, " ")) <= 0 {
		return nil, ConsumerNameIsEmptyError
	}

	queueNameBytes := []byte(queueName)
	consumerNameBytes := []byte(consumerName)
	cmdBytes := make([]byte, len(queueNameBytes) + len(consumerNameBytes) + 3)
	cmdBytes[0] = CommandTypeCreateConsumer
	cmdBytes[1] = byte(len(queueNameBytes))
	cmdBytesForCopy := cmdBytes[2:len(queueNameBytes) + 2]
	copy(cmdBytesForCopy, queueNameBytes)
	cmdBytes[len(queueNameBytes) + 2] = byte(len(consumerNameBytes))
	cmdBytesForCopy = cmdBytes[len(queueNameBytes) + 3:]
	copy(cmdBytesForCopy, consumerNameBytes)
	return cmdBytes, nil
}

func (cmd *CloseProducerCommand) toBytes() ([]byte, error) {
	cmdBytes := make([]byte, 1)
	cmdBytes[0] = CommandTypeCloseProducer
	return cmdBytes, nil
}

func (cmd *CloseConsumerCommand) toBytes() ([]byte, error) {
	cmdBytes := make([]byte, 1)
	cmdBytes[0] = CommandTypeCloseConsumer
	return cmdBytes, nil
}

func (cmd *ProduceMessageCommand) toBytes() ([]byte, error) {
	message := cmd.Message
	if message == nil {
		return nil, MessageIsNullError
	}

	msgBytes, err := MessageToBytes(message)
	if err != nil {
		return nil, err
	}

	cmdBytes := make([]byte, len(msgBytes) + 1)
	cmdBytes[0] = CommandTypeProduceMessage
	cmdBytesForCopy := cmdBytes[1:]
	copy(cmdBytesForCopy, msgBytes)
	return cmdBytes, nil
}

func (cmd *SuccessfullyResponse) toBytes() ([]byte, error) {
	cmdBytes := make([]byte, 1)
	cmdBytes[0] = CommandTypeSuccessfullyResponse
	return cmdBytes, nil
}

func (cmd *ProduceMessageResponse) toBytes() ([]byte, error) {
	cmdBytes := make([]byte, 1 + MsgIdBytesSize)
	cmdBytes[0] = CommandTypeProduceMessageResponse
	msgIdBytes := cmdBytes[1:]
	SplitInt64To8BytesArrayStoreWithAlign(cmd.MsgId, msgIdBytes)
	return cmdBytes, nil
}

func (cmd *FailedResponse) toBytes() ([]byte, error) {
	err := cmd.Err
	if err == nil {
		return nil, FailedReasonIsNullError
	}

	reason := err.Error()
	if len(strings.Trim(reason, " ")) <= 0 {
		return nil, FailedReasonIsNullError
	}

	errReasonBytes := []byte(reason)
	cmdBytes := make([]byte, len(errReasonBytes) + 1)
	cmdBytes[0] = CommandTypeFailedResponse
	cmdBytesForCopy := cmdBytes[1:]
	copy(cmdBytesForCopy, errReasonBytes)
	return cmdBytes, nil
}

func (cmd *ConsumeMessageCommand) toBytes() ([]byte, error) {
	cmdBytes := make([]byte, 1)
	cmdBytes[0] = CommandTypeConsumeMessage
	return cmdBytes, nil
}

func (cmd *ConsumeMessageResponse) toBytes() ([]byte, error) {
	message := cmd.Message
	if message == nil {
		return nil, MessageIsNullError
	}

	msgBytes, err := MessageToBytes(message)
	if err != nil {
		return nil, err
	}

	cmdBytes := make([]byte, len(msgBytes) + 1)
	cmdBytes[0] = CommandTypeConsumeMessageResponse
	cmdBytesForCopy := cmdBytes[1:]
	copy(cmdBytesForCopy, msgBytes)
	return cmdBytes, nil
}

func (cmd *ConsumeMessageWithTimeoutCommand) toBytes() ([]byte, error) {
	cmdBytes := make([]byte, 9)
	cmdBytes[0] = CommandTypeConsumeMessageWithTimeout
	msgIdBytes := cmdBytes[1:]
	SplitInt64To8BytesArrayStoreWithAlign(cmd.Timeout, msgIdBytes)
	return cmdBytes, nil
}

func (cmd *ConsumeMessageNoWaitCommand) toBytes() ([]byte, error) {
	cmdBytes := make([]byte, 1)
	cmdBytes[0] = CommandTypeConsumeMessageNoWait
	return cmdBytes, nil
}

func (cmd *CommitMessageCommand) toBytes() ([]byte, error) {
	cmdBytes := make([]byte, 1)
	cmdBytes[0] = CommandTypeCommitMessage
	return cmdBytes, nil
}

func (cmd *ResetConsumerCommand) toBytes() ([]byte, error) {
	cmdBytes := make([]byte, 1)
	cmdBytes[0] = CommandTypeResetConsumer
	return cmdBytes, nil
}

func (cmd *ConsumeMessageIsNilResponse) toBytes() ([]byte, error) {
	cmdBytes := make([]byte, 1)
	cmdBytes[0] = CommandTypeConsumeMessageIsNilResponse
	return cmdBytes, nil
}




































