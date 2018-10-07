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
	"io"
	"errors"
)

// Thread safe
type IConsumer interface {

	io.Closer
	Ready() bool
	Consume() (msg protocol.IMessage, err error)
	ConsumeWithTimeout(timeout int64) (msg protocol.IMessage, err error)
	ConsumeNoWait() (msg protocol.IMessage, err error)

	Commit() error
	Reset() error

}

var (
	ConsumersMetadataStoreFileMagicNumberError = errors.New("consumers metadata store file magic number error")
	ConsumersMetadataStoreFileVersionNumberError = errors.New("consumers metadata store file version number error")
	ConsumerMetadataStoreBlockChecksumError = errors.New("consumer metadata store block checksum error")
	ConsumersMetadataStoreFileAlreadyClosedError = errors.New("consumers metadata store file already closed error")
	ConsumedQueuePathNotMatchError = errors.New("consumed queue path is not match with previous queue path")
	ConsumerAlreadyClosedError = errors.New("consumer already closed error")
)

