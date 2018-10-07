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
	"github.com/astaxie/beego/logs"
)

const DEFAULT_UCP_BUFFER_SIZE = 16

type IPackageCompletedListener interface {
	OnCompleted(completedPackage *IoBuffer) error
}

type PackageCompletedListenerFunc func(completedPackage *IoBuffer) error

func (f PackageCompletedListenerFunc) OnCompleted(completedPackage *IoBuffer) error {
	return f(completedPackage)
}

type IPackageAssemble interface {
	Assemble2CompletePackage(receivedMessage *IoBuffer, completedListener IPackageCompletedListener) error
	Assemble2CompletePackageFunc(receivedMessage *IoBuffer, completedListener func(completedPackage *IoBuffer) error) error
}

type RpcPackageAssembleImpl struct {
	uncompletedPackages []*IoBuffer
	logger              *logs.BeeLogger
}

func NewPackageAssemble(logConfig *LogConfig) IPackageAssemble {
	logger := logs.NewLogger(logConfig.AsyncLogChanLength)
	logger.SetLevel(logConfig.Level)
	logger.SetLogger(logConfig.AdapterName, logConfig.AdapterParameters)
	if logConfig.IsAsync {
		logger.Async(logConfig.AsyncLogChanLength)
	}

	uncompletedPackages := make([]*IoBuffer, 0, DEFAULT_UCP_BUFFER_SIZE)
	return &RpcPackageAssembleImpl{uncompletedPackages, logger}
}

func (packageAssemble *RpcPackageAssembleImpl) Assemble2CompletePackageFunc(receivedMessage *IoBuffer, completedListener func(completedPackage *IoBuffer) error) error {
	return packageAssemble.Assemble2CompletePackage(receivedMessage, PackageCompletedListenerFunc(completedListener))
}

func (packageAssemble *RpcPackageAssembleImpl) Assemble2CompletePackage(receivedMessage *IoBuffer, completedListener IPackageCompletedListener) error {
	if receivedMessage == nil || completedListener == nil {
		panic("Parameter error")
	}

	startPosition := receivedMessage.Position()

	if receivedMessage.Len() >= LC_PACKAGE_START_IDENTIFY_BYTE_NUMBER {
		if isPackageStart(receivedMessage) {
			if receivedMessage.Len() >= LC_PKG_FUNC_CODE_BYTE_NUMBER {
				// 1 字节，功能码
				funCode, err1 := receivedMessage.ReadByte()
				if err1 != nil {
					return err1
				}

				if funCode == LC_FUNC_CODE_REGISTER_RESPONSE {
					if receivedMessage.Len() >= LC_PKG_HEADER_BYTE_NUMBER_WITHOUT_FUNC_CODE {
						// 8 字节，Connection Key
						for i := 0; i < LC_CONN_KEY_BYTE_NUMBER; i++ {
							_, err1 := receivedMessage.ReadByte()
							if err1 != nil {
								return err1
							}
						}

						// 2 字节，数据长度
						pkgDataLength := receivedMessage.ReadUint16()
						// 1 字节校验码
						remainingLength := pkgDataLength + LC_PKG_CHECK_SUM_BYTE_NUMBER
						if receivedMessage.Len() >= int(remainingLength) {
							// 帧头（2）+ CONN_KEY（8）+ 功能码（1）+ 数据长度（2）= 13
							pkgTotalLength := remainingLength + LC_PKG_HEADER_BYTE_NUMBER
							if startPosition == 0 && receivedMessage.Limit() == int(pkgTotalLength) {
								var checksum int64 = 0
								receivedMessage.SetPosition(startPosition)
								for i := 0; i < int(pkgTotalLength-1); i++ {
									pkgByte, err2 := receivedMessage.ReadByte()
									if err2 != nil {
										return err2
									}

									checksum = checksum + int64(pkgByte&0xFF)
								}

								pkgChecksum, err2 := receivedMessage.ReadByte()
								if err2 != nil {
									return err2
								}

								if byte(checksum&0xFF) == pkgChecksum {
									receivedMessage.SetPosition(startPosition)
									callbackErr := completedListener.OnCompleted(receivedMessage)
									if callbackErr != nil {
										packageAssemble.logger.Error("Process completed package failed. Error is %+v", callbackErr)
									}
								} else {
									packageAssemble.logger.Warn("Receive a corruption package, checksum is not right. Discard it.")
								}
							} else {
								var checksum int64 = 0
								receivedMessage.SetPosition(startPosition)
								for i := 0; i < int(pkgTotalLength-1); i++ {
									pkgByte, err2 := receivedMessage.ReadByte()
									if err2 != nil {
										return err2
									}

									checksum = checksum + int64(pkgByte&0xFF)
								}

								pkgChecksum, err2 := receivedMessage.ReadByte()
								if err2 != nil {
									return err2
								}

								if byte(checksum&0xFF) == pkgChecksum {
									nextPkgStartPos := receivedMessage.Position()
									receivedMessage.SetPosition(startPosition)
									callbackErr := completedListener.OnCompleted(receivedMessage)
									if callbackErr != nil {
										packageAssemble.logger.Error("Process completed package failed. Error is %+v", callbackErr)
									}

									receivedMessage.SetPosition(nextPkgStartPos)
								} else {
									packageAssemble.logger.Warn("Receive a corruption package, checksum is not right. Discard it.")
								}

								if receivedMessage.Len() > 0 {
									packageAssemble.Assemble2CompletePackage(receivedMessage, completedListener)
								}
							}
						} else {
							receivedMessage.SetPosition(startPosition)
							packageAssemble.uncompletedPackages = append(packageAssemble.uncompletedPackages, receivedMessage)
						}
					} else {
						receivedMessage.SetPosition(startPosition)
						packageAssemble.uncompletedPackages = append(packageAssemble.uncompletedPackages, receivedMessage)
					}
				} else {
					if receivedMessage.Len() >= LC_PKG_LENGTH_BYTE_NUMBER {
						// 2 字节，数据长度
						pkgDataLength := receivedMessage.ReadUint16()
						// 1 字节校验码
						remainingLength := pkgDataLength + LC_PKG_CHECK_SUM_BYTE_NUMBER
						if receivedMessage.Len() >= int(remainingLength) {
							// 帧头（2）+ 功能码（1）+ 数据长度（2）= 5
							pkgTotalLength := remainingLength + LC_PACKAGE_START_IDENTIFY_BYTE_NUMBER + LC_PKG_FUNC_CODE_BYTE_NUMBER + LC_PKG_LENGTH_BYTE_NUMBER
							if startPosition == 0 && receivedMessage.Limit() == int(pkgTotalLength) {
								var checksum int64 = 0
								receivedMessage.SetPosition(startPosition)
								for i := 0; i < int(pkgTotalLength-1); i++ {
									pkgByte, err2 := receivedMessage.ReadByte()
									if err2 != nil {
										return err2
									}

									checksum = checksum + int64(pkgByte&0xFF)
								}

								pkgChecksum, err2 := receivedMessage.ReadByte()
								if err2 != nil {
									return err2
								}

								if byte(checksum&0xFF) == pkgChecksum {
									receivedMessage.SetPosition(startPosition)
									callbackErr := completedListener.OnCompleted(receivedMessage)
									if callbackErr != nil {
										packageAssemble.logger.Error("Process completed package failed. Error is %+v", callbackErr)
									}
								} else {
									packageAssemble.logger.Warn("Receive a corruption package, checksum is not right. Discard it.")
								}
							} else {
								var checksum int64 = 0
								receivedMessage.SetPosition(startPosition)
								for i := 0; i < int(pkgTotalLength-1); i++ {
									pkgByte, err2 := receivedMessage.ReadByte()
									if err2 != nil {
										return err2
									}

									checksum = checksum + int64(pkgByte&0xFF)
								}

								pkgChecksum, err2 := receivedMessage.ReadByte()
								if err2 != nil {
									return err2
								}

								if byte(checksum&0xFF) == pkgChecksum {
									nextPkgStartPos := receivedMessage.Position()
									receivedMessage.SetPosition(startPosition)
									callbackErr := completedListener.OnCompleted(receivedMessage)
									if callbackErr != nil {
										packageAssemble.logger.Error("Process completed package failed. Error is %+v", callbackErr)
									}

									receivedMessage.SetPosition(nextPkgStartPos)
								} else {
									packageAssemble.logger.Warn("Receive a corruption package, checksum is not right. Discard it.")
								}

								if receivedMessage.Len() > 0 {
									packageAssemble.Assemble2CompletePackage(receivedMessage, completedListener)
								}
							}
						} else {
							receivedMessage.SetPosition(startPosition)
							packageAssemble.uncompletedPackages = append(packageAssemble.uncompletedPackages, receivedMessage)
						}
					} else {
						receivedMessage.SetPosition(startPosition)
						packageAssemble.uncompletedPackages = append(packageAssemble.uncompletedPackages, receivedMessage)
					}
				}
			} else {
				receivedMessage.SetPosition(startPosition)
				packageAssemble.uncompletedPackages = append(packageAssemble.uncompletedPackages, receivedMessage)
			}
		} else {
			receivedMessage.SetPosition(startPosition)
			if len(packageAssemble.uncompletedPackages) > 0 {
				packageAssemble.mergeAndProcess(receivedMessage, completedListener)
			}
		}
	} else {
		receivedMessage.SetPosition(startPosition)
		if len(packageAssemble.uncompletedPackages) > 0 {
			packageAssemble.mergeAndProcess(receivedMessage, completedListener)
		} else {
			if maybePackageStart(receivedMessage) {
				receivedMessage.SetPosition(startPosition)
				packageAssemble.uncompletedPackages = append(packageAssemble.uncompletedPackages, receivedMessage)
			}
		}
	}

	return nil
}

func (packageAssemble *RpcPackageAssembleImpl) mergeAndProcess(receivedMessage *IoBuffer, completedListener IPackageCompletedListener) {
	mergedMessage := NewIoBuffer(make([]byte, 0, DEFAULT_IO_BUFFER_SIZE*2))

	for _, uncompletedPackage := range packageAssemble.uncompletedPackages {
		mergedMessage.Write(uncompletedPackage.Bytes())
	}

	packageAssemble.uncompletedPackages = packageAssemble.uncompletedPackages[:0]
	mergedMessage.Write(receivedMessage.Bytes())

	packageAssemble.Assemble2CompletePackage(mergedMessage, completedListener)
}

func isPackageStart(receivedMessage *IoBuffer) bool {
	identifyByte1, err1 := receivedMessage.ReadByte()
	if err1 != nil {
		panic("Received message error, read start identify byte 1 error.")
	}

	identifyByte2, err2 := receivedMessage.ReadByte()
	if err2 != nil {
		panic("Received message error, read start identify byte 2 error.")
	}

	return (identifyByte1 == LC_PACKAGE_START_IDENTIFY_BYTE_1) && (identifyByte2 == LC_PACKAGE_START_IDENTIFY_BYTE_2)
}

func maybePackageStart(receivedMessage *IoBuffer) bool {
	if receivedMessage.Len() >= 1 {
		identifyByte1, err1 := receivedMessage.ReadByte()
		if err1 != nil {
			panic("Received message error, read start identify byte 1 error.")
		}

		return identifyByte1 == LC_PACKAGE_START_IDENTIFY_BYTE_1
	} else {
		return false
	}
}
