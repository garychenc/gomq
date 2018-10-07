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
)

func TestRegisterRequestAndResponse(t *testing.T) {
	appKey := "123hakjsdhsaidsa777"
	appSecret := "1232hh&&$$"
	deviceId := "12d333"
	extInfo := ""
	requestPackage := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))

	err := Initialize2LongConnRegisterRequestPackage(appKey, appSecret, deviceId, extInfo, requestPackage)
	if err != nil {
		t.Errorf("get error when init register request. error is %+v", err)
	}

	wriedPackage := ValueOf(requestPackage, -1)
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
	appKey1, appSecret1, deviceId1, extInfo1, err := GetRegisterRequestInfoFromPayload(payload)
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

	var connKey int64 = 1234352646436
	var sendHeartbeatInterval int32 = 123
	var noHeartbeatResponseTimeForDeathConn int32 = 12443
	var reconnectInterval int32 = 2223
	isRedirect := false
	isUsingContinuousHeartbeat := true
	isCompressPushBody := true
	serverAddr := "192.168.1.100:11132"
	serversAddr := []string{}
	requestPackage = utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))

	err = Initialize2LongConnRegisterResponsePackage(SUCC, connKey, sendHeartbeatInterval, noHeartbeatResponseTimeForDeathConn, reconnectInterval,
		isRedirect, isUsingContinuousHeartbeat, isCompressPushBody, serverAddr, serversAddr, requestPackage)
	if err != nil {
		t.Errorf("get error when init register response. error is %+v", err)
	}

	wriedPackage = ValueOf(requestPackage, -1)
	if wriedPackage.ConnKey() != connKey {
		t.Errorf("expected ConnKey %v, actual %v ", connKey, wriedPackage.ConnKey())
	}


	if wriedPackage.FunctionCode() != LC_FUNC_CODE_REGISTER_RESPONSE {
		t.Errorf("expected FunctionCode %v, actual %v ", LC_FUNC_CODE_REGISTER_RESPONSE, wriedPackage.FunctionCode())
	}

	if wriedPackage.PayloadLength() <= 0 {
		t.Errorf("expected PayloadLength > %v, actual %v ", 0, wriedPackage.PayloadLength())
	}

	payload = wriedPackage.Payload()
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
}

func TestHeartbeatRequestAndResponse(t *testing.T) {
	requestPackage := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))
	Initialize2LongConnHeartbeatRequestPackage(requestPackage)
	wriedPackage := ValueOf(requestPackage, -1)
	if wriedPackage.ConnKey() != -1 {
		t.Errorf("expected ConnKey %v, actual %v ", -1, wriedPackage.ConnKey())
	}

	if wriedPackage.FunctionCode() != LC_FUNC_CODE_HEART_BEAT_REQUEST {
		t.Errorf("expected FunctionCode %v, actual %v ", LC_FUNC_CODE_HEART_BEAT_REQUEST, wriedPackage.FunctionCode())
	}

	if wriedPackage.PayloadLength() <= 0 {
		t.Errorf("expected PayloadLength > %v, actual %v ", 0, wriedPackage.PayloadLength())
	}

	payload := wriedPackage.Payload()
	if payload[0] != MEANING_LESS_PAYLOAD {
		t.Errorf("expected payload %v, actual %v ", MEANING_LESS_PAYLOAD, payload[0])
	}

	requestPackage = utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))
	Initialize2LongConnHeartbeatResponsePackage(requestPackage)
	wriedPackage = ValueOf(requestPackage, -1)
	if wriedPackage.ConnKey() != -1 {
		t.Errorf("expected ConnKey %v, actual %v ", -1, wriedPackage.ConnKey())
	}

	if wriedPackage.FunctionCode() != LC_FUNC_CODE_HEART_BEAT_RESPONSE {
		t.Errorf("expected FunctionCode %v, actual %v ", LC_FUNC_CODE_HEART_BEAT_RESPONSE, wriedPackage.FunctionCode())
	}

	if wriedPackage.PayloadLength() <= 0 {
		t.Errorf("expected PayloadLength > %v, actual %v ", 0, wriedPackage.PayloadLength())
	}

	payload = wriedPackage.Payload()
	if payload[0] != MEANING_LESS_PAYLOAD {
		t.Errorf("expected payload %v, actual %v ", MEANING_LESS_PAYLOAD, payload[0])
	}
}

func TestLineTestRequestAndResponse(t *testing.T) {
	requestPackage := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))
	Initialize2ServerLineTestRequestPackage(requestPackage)
	wriedPackage := ValueOf(requestPackage, -1)
	if wriedPackage.ConnKey() != -1 {
		t.Errorf("expected ConnKey %v, actual %v ", -1, wriedPackage.ConnKey())
	}

	if wriedPackage.FunctionCode() != LC_FUNC_CODE_LINE_TEST_REQUEST {
		t.Errorf("expected FunctionCode %v, actual %v ", LC_FUNC_CODE_LINE_TEST_REQUEST, wriedPackage.FunctionCode())
	}

	if wriedPackage.PayloadLength() <= 0 {
		t.Errorf("expected PayloadLength > %v, actual %v ", 0, wriedPackage.PayloadLength())
	}

	payload := wriedPackage.Payload()
	if payload[0] != MEANING_LESS_PAYLOAD {
		t.Errorf("expected payload %v, actual %v ", MEANING_LESS_PAYLOAD, payload[0])
	}

	requestPackage = utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))
	Initialize2ServerLineTestResponsePackage(requestPackage)
	wriedPackage = ValueOf(requestPackage, -1)
	if wriedPackage.ConnKey() != -1 {
		t.Errorf("expected ConnKey %v, actual %v ", -1, wriedPackage.ConnKey())
	}

	if wriedPackage.FunctionCode() != LC_FUNC_CODE_LINE_TEST_RESPONSE {
		t.Errorf("expected FunctionCode %v, actual %v ", LC_FUNC_CODE_LINE_TEST_RESPONSE, wriedPackage.FunctionCode())
	}

	if wriedPackage.PayloadLength() <= 0 {
		t.Errorf("expected PayloadLength > %v, actual %v ", 0, wriedPackage.PayloadLength())
	}

	payload = wriedPackage.Payload()
	if payload[0] != MEANING_LESS_PAYLOAD {
		t.Errorf("expected payload %v, actual %v ", MEANING_LESS_PAYLOAD, payload[0])
	}
}

func TestServerReliablePushRequestAndResponse(t *testing.T) {
	actionName := "push/a/root/test"
	var msgId int64 = 122333333333
	isCompressBody := true
	actionBody := []byte("ce测试数据123ndauunnnn99$$%%%{}测试数据001")
	requestPackage := utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))

	err := Initialize2ServerReliablePushRequestPackage(actionName, msgId, actionBody, isCompressBody, requestPackage)
	if err != nil {
		t.Errorf("get error when init reliable push request. error is %+v", err)
	}

	wriedPackage := ValueOf(requestPackage, -1)
	if wriedPackage.FunctionCode() != LC_FUNC_CODE_SERVER_2_CLIENT_PUSH_REQUEST {
		t.Errorf("expected FunctionCode %v, actual %v ", LC_FUNC_CODE_SERVER_2_CLIENT_PUSH_REQUEST, wriedPackage.FunctionCode())
	}

	payload := wriedPackage.Payload()
	actionName1, msgId1, actionBody1, err1 := GetLongConnActionInfoFromPayload(payload, isCompressBody)
	if err1 != nil {
		t.Errorf("get error when get info from reliable push request body. error is %+v", err1)
	}

	if actionName1 != actionName {
		t.Errorf("expected actionName %v, actual %v ", actionName, actionName1)
	}

	if msgId1 != msgId {
		t.Errorf("expected msgId %v, actual %v ", msgId, msgId1)
	}

	actionBodyStr1 := string (actionBody1)
	if actionBodyStr1 != "ce测试数据123ndauunnnn99$$%%%{}测试数据001" {
		t.Errorf("expected actionBodyStr %v, actual %v ", "ce测试数据123ndauunnnn99$$%%%{}测试数据001", actionBodyStr1)
	}

	requestPackage = utils.NewIoBuffer(make([]byte, 0, utils.DEFAULT_IO_BUFFER_SIZE))

	err = Initialize2ClientReliablePushResponsePackage(actionName, msgId, actionBody, isCompressBody, requestPackage)
	if err != nil {
		t.Errorf("get error when init reliable push response. error is %+v", err)
	}

	wriedPackage = ValueOf(requestPackage, -1)
	if wriedPackage.FunctionCode() != LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE {
		t.Errorf("expected FunctionCode %v, actual %v ", LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE, wriedPackage.FunctionCode())
	}

	payload = wriedPackage.Payload()
	actionName2, msgId2, actionBody2, err2 := GetLongConnActionInfoFromPayload(payload, isCompressBody)
	if err2 != nil {
		t.Errorf("get error when get info from reliable push request body. error is %+v", err2)
	}

	if actionName2 != actionName {
		t.Errorf("expected actionName %v, actual %v ", actionName, actionName2)
	}

	if msgId2 != msgId {
		t.Errorf("expected msgId %v, actual %v ", msgId, msgId2)
	}

	actionBodyStr2 := string (actionBody2)
	if actionBodyStr2 != "ce测试数据123ndauunnnn99$$%%%{}测试数据001" {
		t.Errorf("expected actionBodyStr %v, actual %v ", "ce测试数据123ndauunnnn99$$%%%{}测试数据001", actionBodyStr2)
	}


}

func TestServerRpcCallRequestAndResponse(t *testing.T) {

}










































