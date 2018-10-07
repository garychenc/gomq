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
	"common/utils"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const LC_PACKAGE_START_IDENTIFY_BYTE_NUMBER = 2
const LC_PACKAGE_START_IDENTIFY_BYTE_1 = 0xB2
const LC_PACKAGE_START_IDENTIFY_BYTE_2 = 0x97

const LC_APP_KEY_BYTE_NUMBER = 32
const LC_APP_SECRET_BYTE_NUMBER = 32
const LC_DEVICE_ID_BYTE_NUMBER = 128

const LC_REGISTER_INFO_BYTE_NUMBER = 14
const LC_REGISTER_RESPONSE_CODE_START_ADDR = 0
const LC_REGISTER_HEARTBEAT_IVL_START_ADDR = 1
const LC_REGISTER_NO_HEARTBEAT_TIME_START_ADDR = 5
const LC_REGISTER_RECONNECT_IVL_START_ADDR = 9
const LC_REGISTER_IS_CON_HEARTBEAT_START_ADDR = 13
const LC_REGISTER_IS_COMPRESS_PUSH_BODY_START_ADDR = 13
const LC_REGISTER_IS_REDIRECT_START_ADDR = 13

const LC_REGISTER_IS_CON_HEARTBEAT_MASK = 0x01
const LC_REGISTER_IS_CON_HEARTBEAT_UN_MASK = 0xFE
const LC_REGISTER_IS_COMPRESS_PUSH_BODY_MASK = 0x02
const LC_REGISTER_IS_COMPRESS_PUSH_BODY_UN_MASK = 0xFD
const LC_REGISTER_IS_REDIRECT_MASK = 0x04
const LC_REGISTER_IS_REDIRECT_UN_MASK = 0xFB

const LC_SERVER_BIND_IP_BYTE_NUMBER = 4
const LC_SERVER_BIND_PORT_BYTE_NUMBER = 2
const LC_SERVER_BIND_ADDR_BYTE_NUMBER = LC_SERVER_BIND_IP_BYTE_NUMBER + LC_SERVER_BIND_PORT_BYTE_NUMBER

const LC_CONN_KEY_BYTE_NUMBER = 8
const LC_PKG_FUNC_CODE_BYTE_NUMBER = 1
const LC_PKG_LENGTH_BYTE_NUMBER = 2
const LC_PKG_CHECK_SUM_BYTE_NUMBER = 1
const LC_PKG_HEADER_BYTE_NUMBER = LC_PACKAGE_START_IDENTIFY_BYTE_NUMBER + LC_CONN_KEY_BYTE_NUMBER + LC_PKG_FUNC_CODE_BYTE_NUMBER + LC_PKG_LENGTH_BYTE_NUMBER
const LC_PKG_HEADER_BYTE_NUMBER_WITHOUT_START_IDENTIFY = LC_CONN_KEY_BYTE_NUMBER + LC_PKG_FUNC_CODE_BYTE_NUMBER + LC_PKG_LENGTH_BYTE_NUMBER
const LC_PKG_HEADER_BYTE_NUMBER_WITHOUT_CONN_KEY = LC_PKG_FUNC_CODE_BYTE_NUMBER + LC_PKG_LENGTH_BYTE_NUMBER
const LC_PKG_HEADER_BYTE_NUMBER_WITHOUT_FUNC_CODE = LC_CONN_KEY_BYTE_NUMBER + LC_PKG_LENGTH_BYTE_NUMBER
const LC_ACTION_NAME_MAX_BYTE_NUMBER = 50
const LC_MESSAGE_ID_BYTE_NUMBER = 8

const STRING_BLOCK_END_BYTE_NUMBER = 1
const STRING_BLOCK_END_BYTE = 0x00

const TRUE_BYTE = 0x01
const FALSE_BYTE = 0x00

const LC_FUNC_CODE_HEART_BEAT_REQUEST = 0x01
const LC_FUNC_CODE_HEART_BEAT_RESPONSE = 0x02
const LC_FUNC_CODE_REGISTER_REQUEST = 0x03
const LC_FUNC_CODE_REGISTER_RESPONSE = 0x04
const LC_FUNC_CODE_LINE_TEST_REQUEST = 0x05
const LC_FUNC_CODE_LINE_TEST_RESPONSE = 0x06
const LC_FUNC_CODE_SERVER_2_CLIENT_CALL_REQUEST = 0x07
const LC_FUNC_CODE_CLIENT_2_SERVER_CALL_RESPONSE = 0x08
const LC_FUNC_CODE_CLIENT_2_SERVER_CALL_REQUEST = 0x09
const LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE = 0x0A
const LC_FUNC_CODE_SERVER_2_CLIENT_PUSH_REQUEST = 0x0B
const LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE = 0x0C

const STRING_2_BYTES_DEFAULT_ENCODING string = "US-ASCII"
const MEANING_LESS_PAYLOAD = 0x01

const (
	SUCC                  = iota
	APP_KEY_INVALID
	APP_SECRET_INVALID
	DEVICE_ID_INVALID
	SERVER_INTERNAL_ERROR
)

type WiredPackage struct {
	connKey       int64
	functionCode  byte
	payloadLength uint16
	payload       []byte
	checksum      byte
}

func create(connKey int64, functionCode byte, payloadLength uint16, payload []byte, checksum byte) *WiredPackage {
	return &WiredPackage{connKey, functionCode, payloadLength, payload, checksum}
}

func (wp *WiredPackage) ConnKey() int64 {
	return wp.connKey
}

func (wp *WiredPackage) SetConnKey(connKey int64) {
	wp.connKey = connKey
}

func (wp *WiredPackage) FunctionCode() byte {
	return wp.functionCode
}

func (wp *WiredPackage) PayloadLength() uint16 {
	return wp.payloadLength
}

func (wp *WiredPackage) Payload() []byte {
	return wp.payload
}

func (wp *WiredPackage) Checksum() byte {
	return wp.checksum
}

func ValueOf(rawData *utils.IoBuffer, connKey int64) *WiredPackage {
	if rawData == nil {
		panic("Error format of raw data.")
	}

	// 2 bytes, package start prefix
	rawData.Next(2)
	// 1 byte, function code
	funCode, err := rawData.ReadByte()
	if err != nil {
		panic(io.EOF)
	}

	if funCode == LC_FUNC_CODE_REGISTER_RESPONSE {
		connKey = rawData.ReadInt64()
	}

	// 2 bytes, package length
	pkgDataLength := rawData.ReadUint16()

	// payload data
	payloadData := rawData.Next(int(pkgDataLength) & 0xFFFF)

	// 1 byte, checksum
	checksum, err := rawData.ReadByte()
	if err != nil {
		panic(io.EOF)
	}

	return create(connKey, funCode, pkgDataLength, payloadData, checksum)
}

func putStartAndId(connKey int64, responsePackage *utils.IoBuffer) {
	responsePackage.WriteByte(LC_PACKAGE_START_IDENTIFY_BYTE_1)
	responsePackage.WriteByte(LC_PACKAGE_START_IDENTIFY_BYTE_2)

	responsePackage.WriteByte((byte)(connKey >> 56 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 48 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 40 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 32 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 24 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 16 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 8 & 0xff))
	responsePackage.WriteByte((byte)(connKey & 0xff))
}

func putStart(responsePackage *utils.IoBuffer) {
	responsePackage.WriteByte(LC_PACKAGE_START_IDENTIFY_BYTE_1)
	responsePackage.WriteByte(LC_PACKAGE_START_IDENTIFY_BYTE_2)
}

func putLengthAndPayload(payload []byte, responsePackage *utils.IoBuffer) uint16 {
	pkgDataLength := uint16(len(payload))
	responsePackage.WriteByte((byte)((pkgDataLength >> 8) & 0x00FF))
	responsePackage.WriteByte((byte)(pkgDataLength & 0x00FF))
	responsePackage.Write(payload)
	return pkgDataLength
}

func checksumStartAndId(connKey int64) int64 {
	var checksum int64
	checksum = int64(LC_PACKAGE_START_IDENTIFY_BYTE_1 + LC_PACKAGE_START_IDENTIFY_BYTE_2)

	checksum = checksum + (connKey >> 56 & 0xff)
	checksum = checksum + (connKey >> 48 & 0xff)
	checksum = checksum + (connKey >> 40 & 0xff)
	checksum = checksum + (connKey >> 32 & 0xff)
	checksum = checksum + (connKey >> 24 & 0xff)
	checksum = checksum + (connKey >> 16 & 0xff)
	checksum = checksum + (connKey >> 8 & 0xff)
	checksum = checksum + (connKey & 0xff)
	return checksum
}

func checksumStart() int64 {
	return (int64)(LC_PACKAGE_START_IDENTIFY_BYTE_1 + LC_PACKAGE_START_IDENTIFY_BYTE_2)
}

func checksumDataLengthAndPayload(payload []byte, pkgDataLength uint16, checksum int64) int64 {
	checksum = checksum + int64((pkgDataLength>>8)&0x00FF)
	checksum = checksum + int64(pkgDataLength&0x00FF)
	for _, aPayload := range payload {
		checksum = checksum + int64(aPayload&0x00FF)
	}

	return checksum
}

func putChecksumAndFlip(responsePackage *utils.IoBuffer, checksum int64) {
	responsePackage.WriteByte((byte)(checksum & 0x00FF))
}

func checkLongConnActionBody(longConnActionBody []byte) {
	if longConnActionBody == nil || len(longConnActionBody) == 0 {
		panic("No long connection action body.")
	}
}

func createLongConnActionPayload(longConnActionName string, msgId int64, longConnActionBody []byte, isCompressBody bool) ([]byte, error) {
	longConnActionNameBytes := []byte(longConnActionName)
	longConnActionNameLength := len(longConnActionNameBytes)
	longConnActionBodyLength := len(longConnActionBody)
	if longConnActionNameLength > LC_ACTION_NAME_MAX_BYTE_NUMBER {
		panic("Long connection action name [" + longConnActionName + "] too large. Current bytes length is " + strconv.Itoa(longConnActionNameLength))
	}

	payload := make([]byte, longConnActionNameLength+STRING_BLOCK_END_BYTE_NUMBER+LC_MESSAGE_ID_BYTE_NUMBER+longConnActionBodyLength)

	copy(payload, longConnActionNameBytes)
	payload[longConnActionNameLength] = STRING_BLOCK_END_BYTE

	payload[longConnActionNameLength+STRING_BLOCK_END_BYTE_NUMBER+0] = (byte)(msgId >> 56 & 0xff)
	payload[longConnActionNameLength+STRING_BLOCK_END_BYTE_NUMBER+1] = (byte)(msgId >> 48 & 0xff)
	payload[longConnActionNameLength+STRING_BLOCK_END_BYTE_NUMBER+2] = (byte)(msgId >> 40 & 0xff)
	payload[longConnActionNameLength+STRING_BLOCK_END_BYTE_NUMBER+3] = (byte)(msgId >> 32 & 0xff)
	payload[longConnActionNameLength+STRING_BLOCK_END_BYTE_NUMBER+4] = (byte)(msgId >> 24 & 0xff)
	payload[longConnActionNameLength+STRING_BLOCK_END_BYTE_NUMBER+5] = (byte)(msgId >> 16 & 0xff)
	payload[longConnActionNameLength+STRING_BLOCK_END_BYTE_NUMBER+6] = (byte)(msgId >> 8 & 0xff)
	payload[longConnActionNameLength+STRING_BLOCK_END_BYTE_NUMBER+7] = (byte)(msgId & 0xff)

	payloadForCopy := payload[longConnActionNameLength+STRING_BLOCK_END_BYTE_NUMBER+LC_MESSAGE_ID_BYTE_NUMBER:]
	copy(payloadForCopy, longConnActionBody)

	if isCompressBody {
		zipData, err := utils.GZip(payload)
		if err != nil {
			return nil, err
		} else {
			return zipData, nil
		}
	} else {
		return payload, nil
	}
}

func Initialize2LongConnRegisterResponsePackage(responseCode byte, connKey int64, sendHeartbeatInterval int32, noHeartbeatResponseTimeForDeathConn int32, reconnectInterval int32,
	isRedirect bool, isUsingContinuousHeartbeat bool, isCompressPushBody bool, serverAddr string, serversAddr []string, responsePackage *utils.IoBuffer) error {
	if serversAddr == nil {
		serversAddr = make([]string, 0)
	}

	putStart(responsePackage)
	responsePackage.WriteByte(LC_FUNC_CODE_REGISTER_RESPONSE)
	responsePackage.WriteByte((byte)(connKey >> 56 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 48 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 40 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 32 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 24 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 16 & 0xff))
	responsePackage.WriteByte((byte)(connKey >> 8 & 0xff))
	responsePackage.WriteByte((byte)(connKey & 0xff))
	payload := make([]byte, LC_REGISTER_INFO_BYTE_NUMBER+LC_SERVER_BIND_ADDR_BYTE_NUMBER+LC_SERVER_BIND_ADDR_BYTE_NUMBER*len(serversAddr))
	payload[LC_REGISTER_RESPONSE_CODE_START_ADDR] = responseCode

	payload[LC_REGISTER_HEARTBEAT_IVL_START_ADDR+0] = (byte)((sendHeartbeatInterval >> 24) & 0x00FF)
	payload[LC_REGISTER_HEARTBEAT_IVL_START_ADDR+1] = (byte)((sendHeartbeatInterval >> 16) & 0x00FF)
	payload[LC_REGISTER_HEARTBEAT_IVL_START_ADDR+2] = (byte)((sendHeartbeatInterval >> 8) & 0x00FF)
	payload[LC_REGISTER_HEARTBEAT_IVL_START_ADDR+3] = (byte)(sendHeartbeatInterval & 0x00FF)
	payload[LC_REGISTER_NO_HEARTBEAT_TIME_START_ADDR+0] = (byte)((noHeartbeatResponseTimeForDeathConn >> 24) & 0x00FF)
	payload[LC_REGISTER_NO_HEARTBEAT_TIME_START_ADDR+1] = (byte)((noHeartbeatResponseTimeForDeathConn >> 16) & 0x00FF)
	payload[LC_REGISTER_NO_HEARTBEAT_TIME_START_ADDR+2] = (byte)((noHeartbeatResponseTimeForDeathConn >> 8) & 0x00FF)
	payload[LC_REGISTER_NO_HEARTBEAT_TIME_START_ADDR+3] = (byte)(noHeartbeatResponseTimeForDeathConn & 0x00FF)
	payload[LC_REGISTER_RECONNECT_IVL_START_ADDR+0] = (byte)((reconnectInterval >> 24) & 0x00FF)
	payload[LC_REGISTER_RECONNECT_IVL_START_ADDR+1] = (byte)((reconnectInterval >> 16) & 0x00FF)
	payload[LC_REGISTER_RECONNECT_IVL_START_ADDR+2] = (byte)((reconnectInterval >> 8) & 0x00FF)
	payload[LC_REGISTER_RECONNECT_IVL_START_ADDR+3] = (byte)(reconnectInterval & 0x00FF)
	payload[LC_REGISTER_IS_CON_HEARTBEAT_START_ADDR] = 0x00
	if isUsingContinuousHeartbeat {
		payload[LC_REGISTER_IS_CON_HEARTBEAT_START_ADDR] = (byte)(payload[LC_REGISTER_IS_CON_HEARTBEAT_START_ADDR] | LC_REGISTER_IS_CON_HEARTBEAT_MASK)
	} else {
		payload[LC_REGISTER_IS_CON_HEARTBEAT_START_ADDR] = (byte)(payload[LC_REGISTER_IS_CON_HEARTBEAT_START_ADDR] & LC_REGISTER_IS_CON_HEARTBEAT_UN_MASK)
	}

	if isCompressPushBody {
		payload[LC_REGISTER_IS_COMPRESS_PUSH_BODY_START_ADDR] = (byte)(payload[LC_REGISTER_IS_COMPRESS_PUSH_BODY_START_ADDR] | LC_REGISTER_IS_COMPRESS_PUSH_BODY_MASK)
	} else {
		payload[LC_REGISTER_IS_COMPRESS_PUSH_BODY_START_ADDR] = (byte)(payload[LC_REGISTER_IS_COMPRESS_PUSH_BODY_START_ADDR] & LC_REGISTER_IS_COMPRESS_PUSH_BODY_UN_MASK)
	}

	if isRedirect {
		payload[LC_REGISTER_IS_REDIRECT_START_ADDR] = (byte)(payload[LC_REGISTER_IS_REDIRECT_START_ADDR] | LC_REGISTER_IS_REDIRECT_MASK)
	} else {
		payload[LC_REGISTER_IS_REDIRECT_START_ADDR] = (byte)(payload[LC_REGISTER_IS_REDIRECT_START_ADDR] & LC_REGISTER_IS_REDIRECT_UN_MASK)
	}

	err := convertServerAndNetworkInfo2Payload(serverAddr, serversAddr, payload)
	if err != nil {
		return err
	}

	pkgDataLength := putLengthAndPayload(payload, responsePackage)
	checksum := checksumStartAndId(connKey)
	checksum = checksum + LC_FUNC_CODE_REGISTER_RESPONSE
	checksum = checksumDataLengthAndPayload(payload, pkgDataLength, checksum)
	putChecksumAndFlip(responsePackage, checksum)

	return nil
}

func convertServerAndNetworkInfo2Payload(destServerAddr string, serversAddr []string, payload []byte) error {
	if len(strings.Trim(destServerAddr, " ")) > 0 {
		err := parseStringAddr2Payload(destServerAddr, payload, LC_REGISTER_INFO_BYTE_NUMBER)
		if err != nil {
			return err
		}
	} else {
		payload[LC_REGISTER_INFO_BYTE_NUMBER+0] = 0x00
		payload[LC_REGISTER_INFO_BYTE_NUMBER+1] = 0x00
		payload[LC_REGISTER_INFO_BYTE_NUMBER+2] = 0x00
		payload[LC_REGISTER_INFO_BYTE_NUMBER+3] = 0x00
		payload[LC_REGISTER_INFO_BYTE_NUMBER+4] = 0x00
		payload[LC_REGISTER_INFO_BYTE_NUMBER+5] = 0x00
	}

	for index, serverAddr := range serversAddr {
		err := parseStringAddr2Payload(serverAddr, payload, LC_REGISTER_INFO_BYTE_NUMBER+LC_SERVER_BIND_ADDR_BYTE_NUMBER*index)
		if err != nil {
			return err
		}
	}

	return nil
}

func parseStringAddr2Payload(addr string, payload []byte, payloadStart int) error {
	addrInfo := strings.Split(addr, ":")

	bindingIp := addrInfo[0]
	bindingPort := addrInfo[1]

	for i, ipSeg := range strings.Split(bindingIp, ".") {
		ipInt, errIp := strconv.Atoi(ipSeg)
		if errIp != nil {
			return fmt.Errorf("parse comm addr [%v] failed, error is %+v", addr, errIp)
		}

		payload[payloadStart+i] = (byte)(ipInt & 0X00FF)
	}

	portInt, errPort := strconv.Atoi(bindingPort)
	if errPort != nil {
		return fmt.Errorf("parse comm addr [%v] failed, error is %+v", addr, errPort)
	}

	payload[payloadStart+4] = (byte)((portInt >> 8) & 0X00FF) // port最大支持65535
	payload[payloadStart+5] = (byte)(portInt & 0X00FF)
	return nil
}

func Initialize2LongConnHeartbeatRequestPackage(requestPackage *utils.IoBuffer) {
	putStart(requestPackage)
	requestPackage.WriteByte(LC_FUNC_CODE_HEART_BEAT_REQUEST)
	pkgDataLength := putLengthAndPayload([]byte{MEANING_LESS_PAYLOAD}, requestPackage)
	checksum := checksumStart()
	checksum = checksum + LC_FUNC_CODE_HEART_BEAT_REQUEST
	checksum = checksumDataLengthAndPayload([]byte{MEANING_LESS_PAYLOAD}, pkgDataLength, checksum)
	putChecksumAndFlip(requestPackage, checksum)
}

func Initialize2LongConnRegisterRequestPackage(appKey string, appSecret string, deviceId string, extDevInfo string, requestPackage *utils.IoBuffer) error {
	putStart(requestPackage) // connKey不传,用 0 占位
	requestPackage.WriteByte(LC_FUNC_CODE_REGISTER_REQUEST)

	payload, errPayload := createRegisterPayload(appKey, appSecret, deviceId, extDevInfo)
	if errPayload != nil {
		return errPayload
	}

	pkgDataLength := putLengthAndPayload(payload, requestPackage)

	checksum := checksumStart()
	checksum = checksum + LC_FUNC_CODE_REGISTER_REQUEST
	checksum = checksumDataLengthAndPayload(payload, pkgDataLength, checksum)
	putChecksumAndFlip(requestPackage, checksum)

	return nil
}

func createRegisterPayload(appKey string, appSecret string, deviceId string, deviceExtInfo string) ([]byte, error) {
	appKeyLen := 0
	appSecretLen := 0
	deviceIdLen := 0
	extDevInfoLen := 0
	appKeyBytes := []byte(appKey)
	appKeyLen = len(appKeyBytes)
	if appKeyLen > LC_APP_KEY_BYTE_NUMBER {
		return nil, fmt.Errorf("appKey [%v] too large", appKey)
	}

	appSecretBytes := []byte(appSecret)
	appSecretLen = len(appSecretBytes)
	if appSecretLen > LC_APP_SECRET_BYTE_NUMBER {
		return nil, fmt.Errorf("appSecret [%v] too large", appSecret)
	}

	deviceIdBytes := []byte(deviceId)
	deviceIdLen = len(deviceIdBytes)
	if deviceIdLen > LC_DEVICE_ID_BYTE_NUMBER {
		return nil, fmt.Errorf("deviceId [%v] too large", deviceId)
	}

	var extDevInfo []byte = nil
	if len(strings.Trim(deviceExtInfo, " ")) > 0 {
		extDevInfo = []byte(deviceExtInfo)
		extDevInfoLen = len(extDevInfo)
	}

	payload := make([]byte, appKeyLen+STRING_BLOCK_END_BYTE_NUMBER+appSecretLen+STRING_BLOCK_END_BYTE_NUMBER+deviceIdLen+STRING_BLOCK_END_BYTE_NUMBER+extDevInfoLen)

	copy(payload, appKeyBytes)
	payload[appKeyLen] = STRING_BLOCK_END_BYTE
	payloadForCopy := payload[appKeyLen+STRING_BLOCK_END_BYTE_NUMBER:]
	copy(payloadForCopy, appSecretBytes)

	payload[appKeyLen+STRING_BLOCK_END_BYTE_NUMBER+appSecretLen] = STRING_BLOCK_END_BYTE
	payloadForCopy = payload[appKeyLen+STRING_BLOCK_END_BYTE_NUMBER+appSecretLen+STRING_BLOCK_END_BYTE_NUMBER:]
	copy(payloadForCopy, deviceIdBytes)

	payload[appKeyLen+STRING_BLOCK_END_BYTE_NUMBER+appSecretLen+STRING_BLOCK_END_BYTE_NUMBER+deviceIdLen] = STRING_BLOCK_END_BYTE
	if extDevInfo != nil && len(extDevInfo) > 0 {
		payloadForCopy = payload[appKeyLen+STRING_BLOCK_END_BYTE_NUMBER+appSecretLen+STRING_BLOCK_END_BYTE_NUMBER+deviceIdLen+STRING_BLOCK_END_BYTE_NUMBER:]
		copy(payloadForCopy, extDevInfo)

	}

	return payload, nil
}

func Initialize2LongConnHeartbeatResponsePackage(responsePackage *utils.IoBuffer) {
	putStart(responsePackage)
	responsePackage.WriteByte(LC_FUNC_CODE_HEART_BEAT_RESPONSE)
	pkgDataLength := putLengthAndPayload([]byte{MEANING_LESS_PAYLOAD}, responsePackage)
	checksum := checksumStart()
	checksum = checksum + LC_FUNC_CODE_HEART_BEAT_RESPONSE
	checksum = checksumDataLengthAndPayload([]byte{MEANING_LESS_PAYLOAD}, pkgDataLength, checksum)
	putChecksumAndFlip(responsePackage, checksum)
}

func Initialize2ServerLineTestRequestPackage(requestPackage *utils.IoBuffer) {
	putStart(requestPackage)
	requestPackage.WriteByte(LC_FUNC_CODE_LINE_TEST_REQUEST)
	pkgDataLength := putLengthAndPayload([]byte{MEANING_LESS_PAYLOAD}, requestPackage)
	checksum := checksumStart()
	checksum = checksum + LC_FUNC_CODE_LINE_TEST_REQUEST
	checksum = checksumDataLengthAndPayload([]byte{MEANING_LESS_PAYLOAD}, pkgDataLength, checksum)
	putChecksumAndFlip(requestPackage, checksum)
}

func Initialize2ServerLineTestResponsePackage(responsePackage *utils.IoBuffer) {
	putStart(responsePackage)
	responsePackage.WriteByte(LC_FUNC_CODE_LINE_TEST_RESPONSE)
	pkgDataLength := putLengthAndPayload([]byte{MEANING_LESS_PAYLOAD}, responsePackage)
	checksum := checksumStart()
	checksum = checksum + LC_FUNC_CODE_LINE_TEST_RESPONSE
	checksum = checksumDataLengthAndPayload([]byte{MEANING_LESS_PAYLOAD}, pkgDataLength, checksum)
	putChecksumAndFlip(responsePackage, checksum)
}

func GetResponseCodeFromPayload(payload []byte) byte {
	return payload[LC_REGISTER_RESPONSE_CODE_START_ADDR] & 0x00FF
}

func getIntFromPayload(payload []byte, offset int) int32 {
	byte1 := payload[offset+0]
	byte2 := payload[offset+1]
	byte3 := payload[offset+2]
	byte4 := payload[offset+3]
	return utils.Combine4Bytes2Int32(uint32(byte1), uint32(byte2), uint32(byte3), uint32(byte4))
}

func GetSendHeartbeatIntervalFromPayload(payload []byte) int32 {
	return getIntFromPayload(payload, LC_REGISTER_HEARTBEAT_IVL_START_ADDR)
}

func GetNoHeartbeatResponseTimeForDeathConnFromPayload(payload []byte) int32 {
	return getIntFromPayload(payload, LC_REGISTER_NO_HEARTBEAT_TIME_START_ADDR)
}

func GetReconnectIntervalFromPayload(payload []byte) int32 {
	return getIntFromPayload(payload, LC_REGISTER_RECONNECT_IVL_START_ADDR)
}

func GetIsUsingContinuousHeartbeatFromPayload(payload []byte) bool {
	return getBooleanFromPayloadByMask(payload, LC_REGISTER_IS_CON_HEARTBEAT_START_ADDR, LC_REGISTER_IS_CON_HEARTBEAT_MASK)
}

func GetIsCompressPushBodyFromPayload(payload []byte) bool {
	return getBooleanFromPayloadByMask(payload, LC_REGISTER_IS_COMPRESS_PUSH_BODY_START_ADDR, LC_REGISTER_IS_COMPRESS_PUSH_BODY_MASK)
}

func getBooleanFromPayload(payload []byte, offset int) bool {
	byte1 := payload[offset]
	return byte1 != FALSE_BYTE
}

func getBooleanFromPayloadByMask(payload []byte, offset int, mask byte) bool {
	byte1 := payload[offset]
	return (byte1 & mask) == (mask & 0x00FF)
}

func GetLongConnActionInfoFromPayload(payload []byte, isCompressBody bool) (actionName string, messageId int64, longConnBody []byte, ex error) {
	if isCompressBody {
		unzipPayload, err := utils.UnGZip(payload)
		if err != nil {
			return "", -1, nil, err
		}

		payload = unzipPayload
	}

	payloadBuffer := utils.NewIoBuffer(payload)

	// 50 字节，Long Connection Action Name
	var actionNameStr strings.Builder
	actionNameStr.Grow(LC_ACTION_NAME_MAX_BYTE_NUMBER)
	for i := 0; i < LC_ACTION_NAME_MAX_BYTE_NUMBER; i++ {
		aActionNameChar, err := payloadBuffer.ReadByte()
		if err != nil {
			return "", -1, nil, err
		}

		if aActionNameChar != STRING_BLOCK_END_BYTE {
			actionNameStr.WriteByte(aActionNameChar)
		} else {
			break
		}
	}

	msgId := payloadBuffer.ReadInt64()
	body := payloadBuffer.Bytes()
	bodyCopy := make([]byte, len(body))
	copy(bodyCopy, body)
	return actionNameStr.String(), msgId, bodyCopy, nil
}

func GetRegisterRequestInfoFromPayload(payload []byte) (appKey string, appSecret string, deviceId string, deviceExtInfo string, ex error) {
	payloadBuffer := utils.NewIoBuffer(payload)

	// 最大 32 字节，APPKEY
	var appKeyStr strings.Builder
	appKeyStr.Grow(LC_APP_KEY_BYTE_NUMBER)
	for i := 0; i < LC_APP_KEY_BYTE_NUMBER; i++ {
		appKeyChar, err := payloadBuffer.ReadByte()
		if err != nil {
			return "", "", "", "", err
		}

		if appKeyChar != STRING_BLOCK_END_BYTE {
			appKeyStr.WriteByte(appKeyChar)
		} else {
			break
		}
	}

	// 最大 32 字节，APPSECRET
	var appSecretStr strings.Builder
	appSecretStr.Grow(LC_APP_SECRET_BYTE_NUMBER)
	for i := 0; i < LC_APP_SECRET_BYTE_NUMBER; i++ {
		appSecretChar, err := payloadBuffer.ReadByte()
		if err != nil {
			return "", "", "", "", err
		}

		if appSecretChar != STRING_BLOCK_END_BYTE {
			appSecretStr.WriteByte(appSecretChar)
		} else {
			break
		}
	}

	// 最大 128 字节，DEVICEID
	var deviceIdStr strings.Builder
	deviceIdStr.Grow(LC_DEVICE_ID_BYTE_NUMBER)
	for i := 0; i < LC_DEVICE_ID_BYTE_NUMBER; i++ {
		deviceIdChar, err := payloadBuffer.ReadByte()
		if err != nil {
			return "", "", "", "", err
		}

		if deviceIdChar != STRING_BLOCK_END_BYTE {
			deviceIdStr.WriteByte(deviceIdChar)
		} else {
			break
		}
	}

	var devExtInfo string
	if payloadBuffer.HasRemaining() {
		devExtInfo = payloadBuffer.String()
	}

	return appKeyStr.String(), appSecretStr.String(), deviceIdStr.String(), devExtInfo, nil
}

func GetIsRedirectFromPayload(payload []byte) bool {
	return getBooleanFromPayloadByMask(payload, LC_REGISTER_IS_REDIRECT_START_ADDR, LC_REGISTER_IS_REDIRECT_MASK)
}

func GetRegisterDestServerAddrFromPayload(payload []byte) string {
	var serverAddr strings.Builder
	serverAddr.Grow(LC_SERVER_BIND_IP_BYTE_NUMBER * LC_SERVER_BIND_IP_BYTE_NUMBER)
	for i := 0; i < LC_SERVER_BIND_IP_BYTE_NUMBER; i++ {
		ip := payload[LC_REGISTER_INFO_BYTE_NUMBER+i]
		serverAddr.WriteString(strconv.Itoa(int(ip)))

		if i < LC_SERVER_BIND_IP_BYTE_NUMBER-1 {
			serverAddr.WriteString(".")
		}
	}

	serverAddr.WriteString(":")

	portH := uint32(payload[LC_REGISTER_INFO_BYTE_NUMBER+LC_SERVER_BIND_IP_BYTE_NUMBER])
	portL := uint32(payload[LC_REGISTER_INFO_BYTE_NUMBER+LC_SERVER_BIND_IP_BYTE_NUMBER+1])
	port := (portH << 8 & 0xFF00) | (portL & 0x00FF)

	serverAddr.WriteString(strconv.Itoa(int(port)))
	return serverAddr.String()
}

func GetRegisterServerNetworkFromPayload(payload []byte) []string {
	var registerInfoLen = LC_REGISTER_INFO_BYTE_NUMBER + LC_SERVER_BIND_ADDR_BYTE_NUMBER
	if len(payload) == registerInfoLen {
		return []string{}
	}

	serverNetwork := make([]string, 0, 10)

	for i := registerInfoLen; i <= (len(payload) - LC_SERVER_BIND_ADDR_BYTE_NUMBER); i += LC_SERVER_BIND_ADDR_BYTE_NUMBER {
		var serverAddr strings.Builder
		serverAddr.Grow(LC_SERVER_BIND_IP_BYTE_NUMBER * LC_SERVER_BIND_IP_BYTE_NUMBER)
		for j := 0; j < LC_SERVER_BIND_IP_BYTE_NUMBER; j++ {
			ip := payload[i+j]
			serverAddr.WriteString(strconv.Itoa(int(ip)))

			if j < LC_SERVER_BIND_IP_BYTE_NUMBER-1 {
				serverAddr.WriteString(".")
			}
		}

		serverAddr.WriteString(":")

		portH := uint32(payload[i+LC_SERVER_BIND_IP_BYTE_NUMBER])
		portL := uint32(payload[i+LC_SERVER_BIND_IP_BYTE_NUMBER+1])
		port := (portH << 8 & 0xFF00) | (portL & 0x00FF)

		serverAddr.WriteString(strconv.Itoa(int(port)))
		serverNetwork = append(serverNetwork, serverAddr.String())
	}

	return serverNetwork
}

func CreateUploadDeviceAliasActionBody(connKey int64, deviceAlias string) []byte {
	deviceAliasBytes := []byte(deviceAlias)
	actionBody := make([]byte, LC_CONN_KEY_BYTE_NUMBER+len(deviceAliasBytes))

	actionBody[0] = (byte)(connKey >> 56 & 0xff)
	actionBody[1] = (byte)(connKey >> 48 & 0xff)
	actionBody[2] = (byte)(connKey >> 40 & 0xff)
	actionBody[3] = (byte)(connKey >> 32 & 0xff)
	actionBody[4] = (byte)(connKey >> 24 & 0xff)
	actionBody[5] = (byte)(connKey >> 16 & 0xff)
	actionBody[6] = (byte)(connKey >> 8 & 0xff)
	actionBody[7] = (byte)(connKey & 0xff)

	actionBodyForCopy := actionBody[LC_CONN_KEY_BYTE_NUMBER:]
	copy(actionBodyForCopy, deviceAliasBytes)

	return actionBody
}

func GetLongConnRegisterDeviceAliasInfoFromActionBody(actionBody []byte) (int64, string) {
	payloadBuffer := utils.NewIoBuffer(actionBody)

	connKey := payloadBuffer.ReadInt64()
	deviceAlias := string(actionBody[LC_CONN_KEY_BYTE_NUMBER:])
	return connKey, deviceAlias
}

func Initialize2ServerLongConnCallRequestPackage(longConnActionName string, msgId int64, longConnActionBody []byte, isCompressBody bool, requestPackage *utils.IoBuffer) error {
	checkLongConnActionBody(longConnActionBody)
	putStart(requestPackage)
	requestPackage.WriteByte(LC_FUNC_CODE_SERVER_2_CLIENT_CALL_REQUEST)
	payload, err := createLongConnActionPayload(longConnActionName, msgId, longConnActionBody, isCompressBody)
	if err != nil {
		return err
	}

	pkgDataLength := putLengthAndPayload(payload, requestPackage)
	checksum := checksumStart()
	checksum = checksum + LC_FUNC_CODE_SERVER_2_CLIENT_CALL_REQUEST
	checksum = checksumDataLengthAndPayload(payload, pkgDataLength, checksum)
	putChecksumAndFlip(requestPackage, checksum)
	return nil
}

func Initialize2ServerLongConnCallResponsePackage(longConnActionName string, msgId int64, longConnActionBody []byte, isCompressBody bool, requestPackage *utils.IoBuffer) error {
	checkLongConnActionBody(longConnActionBody)
	putStart(requestPackage)
	requestPackage.WriteByte(LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE)
	payload, err := createLongConnActionPayload(longConnActionName, msgId, longConnActionBody, isCompressBody)
	if err != nil {
		return err
	}

	pkgDataLength := putLengthAndPayload(payload, requestPackage)
	checksum := checksumStart()
	checksum = checksum + LC_FUNC_CODE_SERVER_2_CLIENT_CALL_RESPONSE
	checksum = checksumDataLengthAndPayload(payload, pkgDataLength, checksum)
	putChecksumAndFlip(requestPackage, checksum)
	return nil
}

func Initialize2ClientLongConnCallRequestPackage(longConnActionName string, msgId int64, longConnActionBody []byte, isCompressBody bool, requestPackage *utils.IoBuffer) error {
	checkLongConnActionBody(longConnActionBody)
	putStart(requestPackage)
	requestPackage.WriteByte(LC_FUNC_CODE_CLIENT_2_SERVER_CALL_REQUEST)
	payload, err := createLongConnActionPayload(longConnActionName, msgId, longConnActionBody, isCompressBody)
	if err != nil {
		return err
	}

	pkgDataLength := putLengthAndPayload(payload, requestPackage)
	checksum := checksumStart()
	checksum = checksum + LC_FUNC_CODE_CLIENT_2_SERVER_CALL_REQUEST
	checksum = checksumDataLengthAndPayload(payload, pkgDataLength, checksum)
	putChecksumAndFlip(requestPackage, checksum)
	return nil
}

func Initialize2ClientLongConnCallResponsePackage(longConnActionName string, msgId int64, longConnActionBody []byte, isCompressBody bool, requestPackage *utils.IoBuffer) error {
	checkLongConnActionBody(longConnActionBody)
	putStart(requestPackage)
	requestPackage.WriteByte(LC_FUNC_CODE_CLIENT_2_SERVER_CALL_RESPONSE)
	payload, err := createLongConnActionPayload(longConnActionName, msgId, longConnActionBody, isCompressBody)
	if err != nil {
		return err
	}

	pkgDataLength := putLengthAndPayload(payload, requestPackage)
	checksum := checksumStart()
	checksum = checksum + LC_FUNC_CODE_CLIENT_2_SERVER_CALL_RESPONSE
	checksum = checksumDataLengthAndPayload(payload, pkgDataLength, checksum)
	putChecksumAndFlip(requestPackage, checksum)
	return nil
}

func Initialize2ServerReliablePushRequestPackage(actionName string, msgId int64, actionBody []byte, isCompressBody bool, requestPackage *utils.IoBuffer) error {
	checkLongConnActionBody(actionBody)
	putStart(requestPackage)
	requestPackage.WriteByte(LC_FUNC_CODE_SERVER_2_CLIENT_PUSH_REQUEST)
	payload, err := createLongConnActionPayload(actionName, msgId, actionBody, isCompressBody)
	if err != nil {
		return err
	}

	pkgDataLength := putLengthAndPayload(payload, requestPackage)
	checksum := checksumStart()
	checksum = checksum + LC_FUNC_CODE_SERVER_2_CLIENT_PUSH_REQUEST
	checksum = checksumDataLengthAndPayload(payload, pkgDataLength, checksum)
	putChecksumAndFlip(requestPackage, checksum)
	return nil
}

func Initialize2ClientReliablePushResponsePackage(actionName string, msgId int64, actionBody []byte, isCompressBody bool, requestPackage *utils.IoBuffer) error {
	checkLongConnActionBody(actionBody)
	putStart(requestPackage)
	requestPackage.WriteByte(LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE)
	payload, err := createLongConnActionPayload(actionName, msgId, actionBody, isCompressBody)
	if err != nil {
		return err
	}

	pkgDataLength := putLengthAndPayload(payload, requestPackage)
	checksum := checksumStart()
	checksum = checksum + LC_FUNC_CODE_CLIENT_2_SERVER_PUSH_RESPONSE
	checksum = checksumDataLengthAndPayload(payload, pkgDataLength, checksum)
	putChecksumAndFlip(requestPackage, checksum)
	return nil
}
