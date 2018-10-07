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
	"common/protocol"
	"errors"
	"io"
)

// Thread safe
type IQueue interface {

	io.Closer
	QueueName() string
	Peek() (protocol.IMessage, error)
	IsEmpty() bool

}

type QueueRelatedResource interface {
	QueueClosed(queueName string)
	QueueDeleted(queueName string)
	MessageAdded(queueName string)
}

var (
	MsgIsNullError = errors.New("message can not be nil")
	StoreFileMagicNumberError = errors.New("store file magic number error")
	StoreFileVersionNumberError = errors.New("store file version number error")
	StoreFileHeaderChecksumError = errors.New("store file header checksum error")
	QueueNodeChecksumError = errors.New("queue node checksum error")
	StoreFileAlreadyClosedError = errors.New("store file already closed error")
)











