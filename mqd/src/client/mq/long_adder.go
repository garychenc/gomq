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
	"sync/atomic"
	"unsafe"
)

type LongAdder struct {
	// For avoid cache line visit
	valueData[CACHE_LINE_SIZE * 2 + 16]byte
}

func NewLongAdder(initValue int64) *LongAdder {
	adder := &LongAdder{}
	vPointer := adder.valuePointer()
	atomic.StoreInt64(vPointer, initValue)
	return adder
}

func (adder *LongAdder) valuePointer() *int64 {
	startPointerRemainder := uintptr(unsafe.Pointer(&adder.valueData)) % 8
	if startPointerRemainder == 0 {
		return (*int64)(unsafe.Pointer(&adder.valueData[CACHE_LINE_SIZE]))
	} else {
		startPointerRemainder = 8 - startPointerRemainder
		return (*int64)(unsafe.Pointer(&adder.valueData[CACHE_LINE_SIZE + startPointerRemainder]))
	}
}

func (adder *LongAdder) Increment() {
	vPointer := adder.valuePointer()
	atomic.AddInt64(vPointer, 1)
}

func (adder *LongAdder) Decrement() {
	vPointer := adder.valuePointer()
	atomic.AddInt64(vPointer, -1)
}

func (adder *LongAdder) Add(x int64) {
	vPointer := adder.valuePointer()
	atomic.AddInt64(vPointer, x)
}

func (adder *LongAdder) Sum() int64 {
	vPointer := adder.valuePointer()
	return atomic.LoadInt64(vPointer)
}