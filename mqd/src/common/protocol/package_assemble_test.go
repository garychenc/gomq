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
package protocol

import (
	"testing"
	"common/utils"
	"common/config"
	"github.com/astaxie/beego/logs"
)

func TestPackageAssembleRegisterRequest(t *testing.T) {
	appKey := "123hakjsdhsaidsa777"
	appSecret := "1232hh&&$$"
	deviceId := "12d333"
	extInfo := ""
	requestPackage := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))

	err := Initialize2LongConnRegisterRequestPackage(appKey, appSecret, deviceId, extInfo, requestPackage)
	if err != nil {
		t.Errorf("get error when init register request. error is %+v", err)
	}

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 1000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true
	packageAssembler := NewPackageAssemble(logConfig)

	completedListener := func(completedPackage *utils.IoBuffer) error {
		wriedPackage := ValueOf(completedPackage, -1)
		t.Logf("Received a completed package")

		if wriedPackage.ConnKey() != -1 {
			t.Errorf("expected ConnKey %v, actual %v ", -1, wriedPackage.ConnKey())
		}

		if wriedPackage.FunctionCode() != LC_FUNC_CODE_REGISTER_REQUEST {
			t.Errorf("expected FunctionCode %v, actual %v ", LC_FUNC_CODE_REGISTER_REQUEST, wriedPackage.FunctionCode())
		}

		if wriedPackage.PayloadLength() <= 0 {
			t.Errorf("expected PayloadLength > %v, actual %v ", 0, wriedPackage.PayloadLength())
		}

		payload := wriedPackage.Payload()
		appKey1, appSecret1, deviceId1, extInfo1, _ := GetRegisterRequestInfoFromPayload(payload)
		if appKey1 != appKey {
			t.Errorf("expected appKey %v, actual %v ", appKey, appKey1)
		}

		if appSecret1 != appSecret {
			t.Errorf("expected appSecret %v, actual %v ", appSecret, appSecret1)
		}

		if deviceId1 != deviceId {
			t.Errorf("expected deviceId %v, actual %v ", deviceId, deviceId1)
		}

		if extInfo1 != extInfo {
			t.Errorf("expected extInfo %v, actual %v ", extInfo, extInfo1)
		}

		return nil
	}

	packageAssembler.Assemble2CompletePackageFunc(requestPackage, completedListener)

	requestPackage1 := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))
	err1 := Initialize2LongConnRegisterRequestPackage(appKey, appSecret, deviceId, extInfo, requestPackage1)
	if err1 != nil {
		t.Errorf("get error when init register request. error is %+v", err1)
	}

	requestPackageBytes1 := requestPackage1.Bytes()
	requestPackageBytes2 := requestPackageBytes1[0:10]
	requestPackageBytes3 := requestPackageBytes1[10:13]
	requestPackageBytes4 := requestPackageBytes1[13:]

	requestPackage2 := utils.NewIoBuffer(requestPackageBytes2)
	requestPackage3 := utils.NewIoBuffer(requestPackageBytes3)
	requestPackage4 := utils.NewIoBuffer(requestPackageBytes4)

	packageAssembler.Assemble2CompletePackageFunc(requestPackage2, completedListener)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage3, completedListener)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage4, completedListener)

	requestPackage5 := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))
	err2 := Initialize2LongConnRegisterRequestPackage(appKey, appSecret, deviceId, extInfo, requestPackage5)
	if err2 != nil {
		t.Errorf("get error when init register request. error is %+v", err2)
	}

	requestPackage6 := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))
	err3 := Initialize2LongConnRegisterRequestPackage(appKey, appSecret, deviceId, extInfo, requestPackage6)
	if err3 != nil {
		t.Errorf("get error when init register request. error is %+v", err3)
	}

	requestPackageBytes5 := requestPackage5.Bytes()
	requestPackageBytes6 := requestPackage6.Bytes()

	requestPackageBytes5 = append(requestPackageBytes5, requestPackageBytes6...)
	requestPackage7 := utils.NewIoBuffer(requestPackageBytes5)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage7, completedListener)

	requestPackageBytes6 = append(requestPackageBytes6, requestPackageBytes2...)
	requestPackage8 := utils.NewIoBuffer(requestPackageBytes6)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage8, completedListener)
	requestPackage9 := utils.NewIoBuffer(requestPackageBytes3)
	requestPackage10 := utils.NewIoBuffer(requestPackageBytes4)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage9, completedListener)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage10, completedListener)
}

func TestPackageAssembleRegisterResponse(t *testing.T) {
	var connKey int64 = 1234352646436
	var sendHeartbeatInterval int32 = 123
	var noHeartbeatResponseTimeForDeathConn int32 = 12443
	var reconnectInterval int32 = 2223
	isRedirect := false
	isUsingContinuousHeartbeat := true
	isCompressPushBody := true
	serverAddr := "192.168.1.100:11132"
	serversAddr := []string{}
	requestPackage := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))

	err := Initialize2LongConnRegisterResponsePackage(SUCC, connKey, sendHeartbeatInterval, noHeartbeatResponseTimeForDeathConn, reconnectInterval,
		isRedirect, isUsingContinuousHeartbeat, isCompressPushBody, serverAddr, serversAddr, requestPackage)
	if err != nil {
		t.Errorf("get error when init register response. error is %+v", err)
	}

	logConfig := &config.LogConfig{}
	logConfig.AsyncLogChanLength = 1000
	logConfig.Level = logs.LevelInformational
	logConfig.AdapterName = logs.AdapterConsole
	logConfig.AdapterParameters = "{}"
	logConfig.IsAsync = true
	packageAssembler := NewPackageAssemble(logConfig)

	completedListener := func(completedPackage *utils.IoBuffer) error {
		wriedPackage := ValueOf(completedPackage, -1)
		t.Logf("Received a completed package")

		if wriedPackage.ConnKey() != connKey {
			t.Errorf("expected ConnKey %v, actual %v ", connKey, wriedPackage.ConnKey())
		}

		if wriedPackage.FunctionCode() != LC_FUNC_CODE_REGISTER_RESPONSE {
			t.Errorf("expected FunctionCode %v, actual %v ", LC_FUNC_CODE_REGISTER_RESPONSE, wriedPackage.FunctionCode())
		}

		if wriedPackage.PayloadLength() <= 0 {
			t.Errorf("expected PayloadLength > %v, actual %v ", 0, wriedPackage.PayloadLength())
		}

		payload := wriedPackage.Payload()
		sendHeartbeatInterval1 := GetSendHeartbeatIntervalFromPayload(payload)
		if sendHeartbeatInterval1 != sendHeartbeatInterval {
			t.Errorf("expected sendHeartbeatInterval %v, actual %v ", sendHeartbeatInterval, sendHeartbeatInterval1)
		}

		noHeartbeatResponseTimeForDeathConn1 := GetNoHeartbeatResponseTimeForDeathConnFromPayload(payload)
		reconnectInterval1 := GetReconnectIntervalFromPayload(payload)
		isRedirect1 := GetIsRedirectFromPayload(payload)
		isUsingContinuousHeartbeat1 := GetIsUsingContinuousHeartbeatFromPayload(payload)
		isCompressPushBody1 := GetIsCompressPushBodyFromPayload(payload)
		serverAddr1 := GetRegisterDestServerAddrFromPayload(payload)
		serversAddr1 := GetRegisterServerNetworkFromPayload(payload)

		if noHeartbeatResponseTimeForDeathConn1 != noHeartbeatResponseTimeForDeathConn {
			t.Errorf("expected noHeartbeatResponseTimeForDeathConn %v, actual %v ", noHeartbeatResponseTimeForDeathConn, noHeartbeatResponseTimeForDeathConn1)
		}

		if reconnectInterval1 != reconnectInterval {
			t.Errorf("expected reconnectInterval %v, actual %v ", reconnectInterval, reconnectInterval1)
		}

		if isRedirect1 != isRedirect {
			t.Errorf("expected isRedirect %v, actual %v ", isRedirect, isRedirect1)
		}

		if isUsingContinuousHeartbeat1 != isUsingContinuousHeartbeat {
			t.Errorf("expected isUsingContinuousHeartbeat %v, actual %v ", isUsingContinuousHeartbeat, isUsingContinuousHeartbeat1)
		}

		if isCompressPushBody1 != isCompressPushBody1 {
			t.Errorf("expected noHeartbeatResponseTimeForDeathConn %v, actual %v ", isCompressPushBody1, isCompressPushBody1)
		}

		if serverAddr1 != serverAddr {
			t.Errorf("expected serverAddr %v, actual %v ", serverAddr, serverAddr1)
		}

		if len(serversAddr1) != 0 {
			t.Errorf("expected serversAddr len %v, actual %v ", 0, len(serversAddr1))
		}

		return nil
	}

	packageAssembler.Assemble2CompletePackageFunc(requestPackage, completedListener)

	requestPackage1 := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))
	err1 := Initialize2LongConnRegisterResponsePackage(SUCC, connKey, sendHeartbeatInterval, noHeartbeatResponseTimeForDeathConn, reconnectInterval,
		isRedirect, isUsingContinuousHeartbeat, isCompressPushBody, serverAddr, serversAddr, requestPackage1)
	if err1 != nil {
		t.Errorf("get error when init register request. error is %+v", err1)
	}

	requestPackageBytes1 := requestPackage1.Bytes()
	requestPackageBytes2 := requestPackageBytes1[0:10]
	requestPackageBytes3 := requestPackageBytes1[10:13]
	requestPackageBytes4 := requestPackageBytes1[13:]

	requestPackage2 := utils.NewIoBuffer(requestPackageBytes2)
	requestPackage3 := utils.NewIoBuffer(requestPackageBytes3)
	requestPackage4 := utils.NewIoBuffer(requestPackageBytes4)

	packageAssembler.Assemble2CompletePackageFunc(requestPackage2, completedListener)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage3, completedListener)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage4, completedListener)

	requestPackage5 := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))
	err2 := Initialize2LongConnRegisterResponsePackage(SUCC, connKey, sendHeartbeatInterval, noHeartbeatResponseTimeForDeathConn, reconnectInterval,
		isRedirect, isUsingContinuousHeartbeat, isCompressPushBody, serverAddr, serversAddr, requestPackage5)
	if err2 != nil {
		t.Errorf("get error when init register request. error is %+v", err2)
	}

	requestPackage6 := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))
	err3 := Initialize2LongConnRegisterResponsePackage(SUCC, connKey, sendHeartbeatInterval, noHeartbeatResponseTimeForDeathConn, reconnectInterval,
		isRedirect, isUsingContinuousHeartbeat, isCompressPushBody, serverAddr, serversAddr, requestPackage6)
	if err3 != nil {
		t.Errorf("get error when init register request. error is %+v", err3)
	}

	requestPackageBytes5 := requestPackage5.Bytes()
	requestPackageBytes6 := requestPackage6.Bytes()

	requestPackageBytes5 = append(requestPackageBytes5, requestPackageBytes6...)
	requestPackage7 := utils.NewIoBuffer(requestPackageBytes5)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage7, completedListener)

	requestPackageBytes6 = append(requestPackageBytes6, requestPackageBytes2...)
	requestPackage8 := utils.NewIoBuffer(requestPackageBytes6)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage8, completedListener)
	requestPackage9 := utils.NewIoBuffer(requestPackageBytes3)
	requestPackage10 := utils.NewIoBuffer(requestPackageBytes4)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage9, completedListener)
	packageAssembler.Assemble2CompletePackageFunc(requestPackage10, completedListener)
}
