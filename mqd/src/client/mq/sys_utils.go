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
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func GetLocalServerIps() ([]string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	localIps := make([]string, 0, 8)
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				localIps = append(localIps, ipnet.IP.String())
			}

		}
	}

	return localIps, nil
}

func ParseStringIp(bindingIp string) ([]byte, error) {

	ip := make([]byte, 4)
	for i, ipSeg := range strings.Split(bindingIp, ".") {
		ipInt, errIp := strconv.Atoi(ipSeg)
		if errIp != nil {
			return nil, fmt.Errorf("parse comm addr [%v] failed, error is %+v", bindingIp, errIp)
		}

		ip[i] = (byte)(ipInt & 0X00FF)
	}

	return ip, nil
}

func CurrentTimeMillis() int64 {
	now := time.Now()
	return now.UnixNano() / 1000000
}

type WaitingCondition struct {
	Cond *sync.Cond
	Lock *sync.Mutex
	Done bool
}

type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Sort is a convenience method.
func (p Int64Slice) Sort() { sort.Sort(p) }

func Int64Sort(a []int64) { sort.Sort(Int64Slice(a)) }

