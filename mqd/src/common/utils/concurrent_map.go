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

import "sync"

type IConcurrentMap interface {

	// 获取给定键值对应的元素值。若没有对应元素值则返回nil。
	Get(key interface{}) interface{}

	// 添加键值对，并返回与给定键值对应的旧的元素值。若没有旧元素值则返回(nil, true)。
	Put(key interface{}, value interface{}) (interface{}, bool)

	// 假如 key 不存在则添加键值对，并返回与给定键值对应的旧的元素值。若没有旧元素值则返回(nil, true)。
	// 假如 key 存在则返回与给定键值对应的旧的元素值。bool 值为 false
	PutIfAbsent(key interface{}, value interface{}) (interface{}, bool)

	// 删除与给定键值对应的键值对，并返回旧的元素值。若没有旧元素值则返回nil。
	Remove(key interface{}) interface{}

	// 清除所有的键值对。
	Clear()

	// 获取键值对的数量。
	Len() int

	// 判断是否包含给定的键值。
	Contains(key interface{}) bool

	// 获取所有的键值所组成的切片值。
	Keys() []interface{}

	// 获取所有的元素值所组成的切片值。
	Values() []interface{}

}

type ConcurrentMapImpl struct {
	store map[interface{}]interface{}
	initialMapSize int
	rwLock sync.RWMutex
}

func NewConcurrentMap() IConcurrentMap {
	return NewConcurrentMapWithSize(-1)
}

func NewConcurrentMapWithSize(size int) IConcurrentMap {
	concurrentMap := &ConcurrentMapImpl{}
	concurrentMap.initialMapSize = size

	if size <= 0 {
		concurrentMap.store = make(map[interface{}]interface{})
	} else {
		concurrentMap.store = make(map[interface{}]interface{}, size)
	}

	return concurrentMap
}

func (concurrentMap *ConcurrentMapImpl) Get(key interface{}) interface{} {
	concurrentMap.rwLock.RLock()
	defer concurrentMap.rwLock.RUnlock()

	v, ok := concurrentMap.store[key]
	if ok {
		return v
	} else {
		return nil
	}
}

func (concurrentMap *ConcurrentMapImpl) Put(key interface{}, value interface{}) (interface{}, bool) {
	concurrentMap.rwLock.Lock()
	defer concurrentMap.rwLock.Unlock()

	oldValue, ok := concurrentMap.store[key]
	concurrentMap.store[key] = value
	if ok {
		return oldValue, true
	} else {
		return nil, true
	}
}

func (concurrentMap *ConcurrentMapImpl) PutIfAbsent(key interface{}, value interface{}) (interface{}, bool) {
	concurrentMap.rwLock.Lock()
	defer concurrentMap.rwLock.Unlock()

	oldValue, exist := concurrentMap.store[key]
	if exist {
		return oldValue, false
	} else {
		concurrentMap.store[key] = value
		return nil, true
	}
}

func (concurrentMap *ConcurrentMapImpl) Remove(key interface{}) interface{} {
	concurrentMap.rwLock.Lock()
	defer concurrentMap.rwLock.Unlock()

	oldValue, ok := concurrentMap.store[key]
	delete(concurrentMap.store, key)
	if ok {
		return oldValue
	} else {
		return nil
	}
}

func (concurrentMap *ConcurrentMapImpl) Clear() {
	concurrentMap.rwLock.Lock()
	defer concurrentMap.rwLock.Unlock()

	if concurrentMap.initialMapSize <= 0 {
		concurrentMap.store = make(map[interface{}]interface{})
	} else {
		concurrentMap.store = make(map[interface{}]interface{}, concurrentMap.initialMapSize)
	}
}

func (concurrentMap *ConcurrentMapImpl) Len() int {
	concurrentMap.rwLock.RLock()
	defer concurrentMap.rwLock.RUnlock()

	return len(concurrentMap.store)
}

func (concurrentMap *ConcurrentMapImpl) Contains(key interface{}) bool {
	concurrentMap.rwLock.RLock()
	defer concurrentMap.rwLock.RUnlock()

	_, contains := concurrentMap.store[key]
	return contains
}

func (concurrentMap *ConcurrentMapImpl) Keys() []interface{} {
	concurrentMap.rwLock.RLock()
	defer concurrentMap.rwLock.RUnlock()

	mapSize := len(concurrentMap.store)
	keys := make([]interface{}, mapSize)
	index := 0
	for key, _ := range concurrentMap.store {
		keys[index] = key
		index++
	}

	return keys
}

func (concurrentMap *ConcurrentMapImpl) Values() []interface{} {
	concurrentMap.rwLock.RLock()
	defer concurrentMap.rwLock.RUnlock()

	mapSize := len(concurrentMap.store)
	values := make([]interface{}, mapSize)
	index := 0
	for _, value := range concurrentMap.store {
		values[index] = value
		index++
	}

	return values
}

