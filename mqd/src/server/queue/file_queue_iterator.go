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

import "common/protocol"

// Not thread safe
type FileQueueIterator struct {
	cursor int64
	fileQueue *FileQueue
}

func NewFileQueueIterator(fileQueue *FileQueue) (*FileQueueIterator, error) {
	if fileQueue == nil {
		panic("parameter fileQueue can not be null")
	}

	headNodeOffset, err := fileQueue.getHeadNodeOffset()
	if err != nil {
		return nil, err
	}

	return &FileQueueIterator{fileQueue: fileQueue, cursor: headNodeOffset}, nil
}

func (iterator *FileQueueIterator) HasNext() bool {
	cursor := iterator.cursor
	lastNodeOffset, err := iterator.fileQueue.getLastNodeOffset()
	if err != nil {
		return false
	}

	return cursor != lastNodeOffset
}

func (iterator *FileQueueIterator) Next() (msg protocol.IMessage, err error) {

	readNodeChecksumErrorTimes := 0

	for {
		msg, nextNodeOffset, err := iterator.fileQueue.getNextMessageFromOffset(iterator.cursor)
		if err == nil {
			if msg != nil {
				iterator.cursor = nextNodeOffset
				return msg, nil
			} else {
				return nil, nil
			}
		} else if err == QueueNodeChecksumError {
			readNodeChecksumErrorTimes++
			if readNodeChecksumErrorTimes >= moveToTailNodeAfterResetTimes {
				headNodeOffset, err := iterator.fileQueue.getHeadNodeOffset()
				if err != nil {
					return nil, err
				}

				iterator.cursor = headNodeOffset
			}
		} else {
			return nil, err
		}
	}
}

