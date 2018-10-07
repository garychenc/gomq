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
package utils

import (
	"fmt"
	"sync"
)

type SequenceGenerator struct {
	workerId, sequence, maxWorkerId                               int64
	workerIdBits, sequenceBits, timestampLeftShift, workerIdShift uint64
	sequenceMask, twepoch, lastTimestamp                          int64

	lock sync.Mutex
}

func NewSequenceGenerator() (*SequenceGenerator, error) {
	serverIps, err := GetLocalServerIps()
	if err != nil {
		return nil, err
	}

	var workerId int64 = 0
	var sequence int64 = 0
	var workerIdBits uint64 = 10
	var maxWorkerId int64 = -1 ^ (-1 << workerIdBits)
	var sequenceBits uint64 = 12
	var sequenceMask int64 = -1 ^ (-1 << sequenceBits)
	var workerIdShift = sequenceBits
	var timestampLeftShift = sequenceBits + workerIdBits
	var twepoch int64 = 1469520702761
	var lastTimestamp int64 = -1

	if len(serverIps) > 0 {
		firstIp := serverIps[0]
		ipNumbers, err := ParseStringIp(firstIp)
		if err != nil {
			return nil, err
		}

		workerId = int64(ipNumbers[3] & 0xFF)
	} else {
		workerId = 2
	}

	if workerId > maxWorkerId || workerId < 0 {
		return nil, fmt.Errorf("worker Id can't be greater than %v or less than 0", maxWorkerId)
	}

	return &SequenceGenerator{workerId: workerId, sequence: sequence, maxWorkerId: maxWorkerId, workerIdBits: workerIdBits,
		sequenceBits: sequenceBits, timestampLeftShift: timestampLeftShift, workerIdShift: workerIdShift,
		sequenceMask: sequenceMask, twepoch: twepoch, lastTimestamp: lastTimestamp}, nil
}

func (generator *SequenceGenerator) GetSequence() (int64, error) {
	generator.lock.Lock()
	defer generator.lock.Unlock()

	timestamp := CurrentTimeMillis()
	if timestamp < generator.lastTimestamp {
		return -1, fmt.Errorf("Clock moved backwards.  Refusing to generate id for %v milliseconds", generator.lastTimestamp-timestamp)
	}

	if generator.lastTimestamp == timestamp {
		generator.sequence = (generator.sequence + 1) & generator.sequenceMask
		if generator.sequence == 0 {
			timestamp = generator.tilNextMillis(generator.lastTimestamp)
		}
	} else {
		generator.sequence = 0
	}

	generator.lastTimestamp = timestamp
	return ((timestamp - generator.twepoch) << generator.timestampLeftShift) | (generator.workerId << generator.workerIdShift) | generator.sequence, nil
}

func (generator *SequenceGenerator) tilNextMillis(lastTimestamp int64) int64 {
	timestamp := CurrentTimeMillis()
	for ; timestamp <= lastTimestamp; {
		timestamp = CurrentTimeMillis()
	}

	return timestamp
}

