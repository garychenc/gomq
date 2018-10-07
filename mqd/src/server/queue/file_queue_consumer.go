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
	"common/config"
	"common/protocol"
	"common/utils"
	"errors"
	"github.com/astaxie/beego/logs"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
)

// Thread safe
type FileQueueConsumer struct {
	cacheLinePadding1 [utils.CACHE_LINE_SIZE]byte

	cursor                 int64
	lastCommitPoint        int64
	waitingForMessageAdded bool
	messageHadBeenAdded    bool

	// Unchanged value
	consumedFileQueue         *FileQueue
	consumerMetadataStoreFile *os.File
	logger                    *logs.BeeLogger

	isSyncFileInEveryOpt        bool
	consumerName                string
	consumedQueuePath           string
	consumerMetadataBlockOffset int64

	lifecycleLock    sync.Mutex
	messageAddedCond *sync.Cond

	cacheLinePadding2 [utils.CACHE_LINE_SIZE]byte
}

var newFileQueueConsumerLock sync.Mutex
var consumerMetadataStoreFiles map[string]*os.File = nil
var namedFileQueueConsumers map[string]*FileQueueConsumer = nil

const consumerMetadataFilePostfix = "-consumer.meta"
const consumerMetadataFileMagicNumber uint32 = 0x784978F2
const consumerMetadataFileVersion uint16 = 1

const consumerMetadataFileMagicNumberSize = 4
const consumerMetadataFileVersionSize = 2
const consumedQueuePathLengthHeaderSize = 1
const consumerMetadataFileHeaderChecksumSize = 1
const consumerMetadataFileHeaderSize = consumerMetadataFileMagicNumberSize + consumerMetadataFileVersionSize + consumedQueuePathLengthHeaderSize +
	consumerMetadataFileHeaderChecksumSize

const consumerCursorHeaderSize = 8
const consumerMetadataBlockChecksumSize = 1
const consumerNameLengthHeaderSize = 1
const consumerMetadataBlockHeaderSize = consumerCursorHeaderSize + consumerMetadataBlockChecksumSize +
	consumerNameLengthHeaderSize

const consumerNameMaxSize = 50
const moveToTailNodeAfterResetTimes = 1024

func NewFileQueueConsumer(logConfig *config.LogConfig, fileQueue *FileQueue, consumerName string, isSyncFileInEveryOpt bool) (*FileQueueConsumer, error) {
	if paraErr := checkParameters(fileQueue, consumerName); paraErr != nil {
		return nil, paraErr
	}

	newFileQueueConsumerLock.Lock()
	defer newFileQueueConsumerLock.Unlock()

	loadingLogger := logs.NewLogger(logConfig.AsyncLogChanLength)
	loadingLogger.SetLevel(logConfig.Level)
	loadingLogger.SetLogger(logConfig.AdapterName, logConfig.AdapterParameters)
	loadingLogger.SetLogger(logs.AdapterConsole, "{}")
	if logConfig.IsAsync {
		loadingLogger.Async(logConfig.AsyncLogChanLength)
	}

	if err := loadNamedFileQueueConsumersIfNecessary(loadingLogger, logConfig, isSyncFileInEveryOpt); err != nil {
		return nil, err
	}

	consumer, err := getFileQueueConsumerFromCache(loadingLogger, fileQueue, consumerName)
	if err != nil {
		return nil, err
	}

	if consumer != nil {
		return consumer, nil
	}

	consumerMetadataFileName := fileQueue.QueueName() + consumerMetadataFilePostfix
	consumerMetadataFile, ok := consumerMetadataStoreFiles[consumerMetadataFileName]
	if ok {
		queueLastNodeOffset, consumerMetadataBlockBuf, err := newConsumerMetadataStoreBlock(fileQueue, consumerName)
		if err != nil {
			return nil, err
		}

		consumerMetadataBlockOffset, err := writeConsumerMetadataBlock2File(consumerMetadataFileName, consumerMetadataFile, consumerMetadataBlockBuf)
		if err != nil {
			loadingLogger.Error("Write consumer metadata store block to file failed, consumer name : %+v, file path : %+v. Error is %+v", consumerName, consumerMetadataFileName, err)
			return nil, err
		}

		consumer := &FileQueueConsumer{consumedFileQueue: fileQueue, consumerName: consumerName,
			consumedQueuePath: fileQueue.QueueName(), cursor: queueLastNodeOffset, lastCommitPoint: queueLastNodeOffset,
		    consumerMetadataBlockOffset: consumerMetadataBlockOffset, consumerMetadataStoreFile: consumerMetadataFile,
		    isSyncFileInEveryOpt: isSyncFileInEveryOpt, waitingForMessageAdded: false, messageHadBeenAdded: false}
		consumer.logger = logs.NewLogger(logConfig.AsyncLogChanLength)
		consumer.logger.SetLevel(logConfig.Level)
		consumer.logger.SetLogger(logConfig.AdapterName, logConfig.AdapterParameters)
		consumer.logger.SetLogger(logs.AdapterConsole, "{}")
		if logConfig.IsAsync {
			consumer.logger.Async(logConfig.AsyncLogChanLength)
		}

		consumer.messageAddedCond = sync.NewCond(&consumer.lifecycleLock)
		consumer.consumedFileQueue.addQueueRelatedResource(consumer)
		namedFileQueueConsumers[fileQueue.QueueName() + "_" + consumerName] = consumer
		return consumer, nil
	} else {
		consumerMetadataFile, err := os.OpenFile(consumerMetadataFileName, os.O_CREATE | os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, err
		}

		consumerMetadataFileHeaderBuf := createConsumerMetadataFileHeader(fileQueue)
		wErr := writeConsumerMetadataFileHeader2File(consumerMetadataFile, consumerMetadataFileHeaderBuf)
		if wErr != nil {
			loadingLogger.Error("Write consumer metadata file header to file failed, consumer name : %+v, file path : %+v. Error is %+v", consumerName, consumerMetadataFileName, wErr)
			return nil, wErr
		}

		queueLastNodeOffset, consumerMetadataBlockBuf, err := newConsumerMetadataStoreBlock(fileQueue, consumerName)
		if err != nil {
			return nil, err
		}

		consumerMetadataBlockOffset, err := writeConsumerMetadataBlock2File(consumerMetadataFileName, consumerMetadataFile, consumerMetadataBlockBuf)
		if err != nil {
			loadingLogger.Error("Write consumer metadata store block to file failed, consumer name : %+v, file path : %+v. Error is %+v", consumerName, consumerMetadataFileName, err)
			return nil, err
		}

		consumer := &FileQueueConsumer{consumedFileQueue: fileQueue, consumerName: consumerName,
			consumedQueuePath: fileQueue.QueueName(), cursor: queueLastNodeOffset, lastCommitPoint: queueLastNodeOffset,
		    consumerMetadataBlockOffset: consumerMetadataBlockOffset, consumerMetadataStoreFile: consumerMetadataFile,
		    isSyncFileInEveryOpt: isSyncFileInEveryOpt, waitingForMessageAdded: false, messageHadBeenAdded: false}
		consumer.logger = logs.NewLogger(logConfig.AsyncLogChanLength)
		consumer.logger.SetLevel(logConfig.Level)
		consumer.logger.SetLogger(logConfig.AdapterName, logConfig.AdapterParameters)
		consumer.logger.SetLogger(logs.AdapterConsole, "{}")
		if logConfig.IsAsync {
			consumer.logger.Async(logConfig.AsyncLogChanLength)
		}

		consumer.messageAddedCond = sync.NewCond(&consumer.lifecycleLock)
		consumer.consumedFileQueue.addQueueRelatedResource(consumer)
		namedFileQueueConsumers[fileQueue.QueueName() + "_" + consumerName] = consumer
		consumerMetadataStoreFiles[consumerMetadataFileName] = consumerMetadataFile
		return consumer, nil
	}
}

func writeConsumerMetadataFileHeader2File(consumerMetadataFile *os.File, consumerMetadataFileHeaderBuf *utils.IoBuffer) error {
	if _, err := consumerMetadataFile.Seek(0, 0); err != nil {
		return err
	}

	if _, wErr := consumerMetadataFile.Write(consumerMetadataFileHeaderBuf.Bytes()); wErr != nil {
		return wErr
	}

	if err := consumerMetadataFile.Sync(); err != nil {
		return err
	}

	return nil
}

func createConsumerMetadataFileHeader(fileQueue *FileQueue) *utils.IoBuffer {
	queueNameBytes := []byte(fileQueue.QueueName())
	queueNameBytesLength := byte(len(queueNameBytes))

	var headerChecksum int64 = 0
	headerChecksum = utils.AddUint32ToChecksum(consumerMetadataFileMagicNumber, headerChecksum)
	headerChecksum = utils.AddUint16ToChecksum(consumerMetadataFileVersion, headerChecksum)
	headerChecksum = utils.AddByteToChecksum(queueNameBytesLength, headerChecksum)
	headerChecksum = utils.AddByteArrayToChecksum(queueNameBytes, headerChecksum)

	consumerMetadataFileHeaderBytes := make([]byte, 0, consumerMetadataFileHeaderSize+queueNameBytesLength)
	consumerMetadataFileHeaderBuf := utils.NewIoBuffer(consumerMetadataFileHeaderBytes)
	consumerMetadataFileHeaderBuf.WriteUint32(consumerMetadataFileMagicNumber)
	consumerMetadataFileHeaderBuf.WriteUint16(consumerMetadataFileVersion)
	consumerMetadataFileHeaderBuf.WriteByte(queueNameBytesLength)
	consumerMetadataFileHeaderBuf.WriteByte(byte(headerChecksum & 0xFF))
	consumerMetadataFileHeaderBuf.Write(queueNameBytes)

	return consumerMetadataFileHeaderBuf
}

func writeConsumerMetadataBlock2File(consumerMetadataFileName string, consumerMetadataFile *os.File, consumerMetadataBlockBuf *utils.IoBuffer) (int64, error) {
	consumerMetadataFileSize := utils.FileSize(consumerMetadataFileName)
	if _, err := consumerMetadataFile.Seek(consumerMetadataFileSize, 0); err != nil {
		return -1, err
	}

	_, wErr := consumerMetadataFile.Write(consumerMetadataBlockBuf.Bytes())
	if wErr != nil {
		return -1, wErr
	}

	if err := consumerMetadataFile.Sync(); err != nil {
		return -1, err
	}

	return consumerMetadataFileSize, nil
}

func newConsumerMetadataStoreBlock(fileQueue *FileQueue, consumerName string) (int64, *utils.IoBuffer,  error)  {
	queueLastNodeOffset, err := fileQueue.getLastNodeOffset()
	if err != nil {
		return -1, nil, err
	}

	consumerNameBytes := []byte(consumerName)
	consumerNameLength := byte(len(consumerNameBytes))

	var headerChecksum int64 = 0
	headerChecksum = utils.AddInt64ToChecksum(queueLastNodeOffset, headerChecksum)
	headerChecksum = utils.AddByteToChecksum(consumerNameLength, headerChecksum)
	headerChecksum = utils.AddByteArrayToChecksum(consumerNameBytes, headerChecksum)

	consumerMetadataBlockBytes := make([]byte, 0, consumerMetadataBlockHeaderSize + consumerNameLength)
	consumerMetadataBlockBuf := utils.NewIoBuffer(consumerMetadataBlockBytes)
	consumerMetadataBlockBuf.WriteInt64(queueLastNodeOffset)
	consumerMetadataBlockBuf.WriteByte(byte(headerChecksum & 0xFF))
	consumerMetadataBlockBuf.WriteByte(consumerNameLength)
	consumerMetadataBlockBuf.Write(consumerNameBytes)

	return queueLastNodeOffset, consumerMetadataBlockBuf, nil
}

func getFileQueueConsumerFromCache(loadingLogger *logs.BeeLogger, fileQueue *FileQueue, consumerName string) (*FileQueueConsumer,  error)  {
	consumer, ok := namedFileQueueConsumers[fileQueue.QueueName() + "_" + consumerName]
	if ok {
		if consumer.consumedQueuePath == fileQueue.QueueName() {
			if consumer.consumedFileQueue == nil {
				consumer.consumedFileQueue = fileQueue
				consumer.consumedFileQueue.addQueueRelatedResource(consumer)
			}

			return consumer, nil
		} else {
			loadingLogger.Error("Consumed queue path does not equals to file queue path. Consumed queue path: %+v, file queue path : %+v, consumer name : %+v.", consumer.consumedQueuePath, fileQueue.QueueName(), consumerName)
			return nil, ConsumedQueuePathNotMatchError
		}
	}

	return nil, nil
}

func checkParameters(fileQueue *FileQueue, consumerName string) error  {
	if fileQueue == nil {
		panic("parameter fileQueue can not be null")
	}

	consumerName = strings.Trim(consumerName, " ")
	if len(consumerName) == 0 {
		return errors.New("consumerName can not be nil or empty")
	}

	if len(consumerName) > consumerNameMaxSize {
		return errors.New("consumerName exceed max size")
	}

	return nil
}

func loadNamedFileQueueConsumersIfNecessary(loadingLogger *logs.BeeLogger, logConfig *config.LogConfig, isSyncFileInEveryOpt bool) error  {
	if namedFileQueueConsumers == nil {
		namedFileQueueConsumers = make(map[string]*FileQueueConsumer)
		consumerMetadataStoreFiles = make(map[string]*os.File)

		consumerMetadataFileNames, err := utils.ListDir(QueueRelativeFilesStoreDir, consumerMetadataFilePostfix)
		if err != nil {
			loadingLogger.Error("List files by postfix [%+v] in directory [%+v] failed, Error is : %+v", QueueRelativeFilesStoreDir, consumerMetadataFilePostfix, err)
			return err
		}

		for _, consumerMetadataFileName := range consumerMetadataFileNames {
			consumerMetadataFile, err := os.OpenFile(consumerMetadataFileName, os.O_RDWR, os.ModePerm)
			if err != nil {
				if consumerMetadataFile != nil {
					consumerMetadataFile.Close()
				}

				loadingLogger.Error("Open consumer metadata file with path : %+v failed. Can not load consumers stored in this file. Error is %+v", consumerMetadataFileName, err)
				continue
			}

			fileContent, ok := readAllFileContent(loadingLogger, consumerMetadataFile, consumerMetadataFileName)
			if !ok {
				continue
			}

			fileBuffer := utils.NewIoBuffer(fileContent)

			if !checkMagicNumIsMatch(loadingLogger, fileBuffer, consumerMetadataFile, consumerMetadataFileName) {
				continue
			}

			if !checkFileVersionIsMatch(loadingLogger, fileBuffer, consumerMetadataFile, consumerMetadataFileName) {
				continue
			}

			loadedConsumedQueuePath, ok := readFileHeaderAndCheckChecksum(loadingLogger, fileBuffer, consumerMetadataFile, consumerMetadataFileName)
			if !ok {
				continue
			}

			readConsumerMetadataBlocksAndAdd2Cache(loadingLogger, logConfig, fileBuffer, consumerMetadataFile, loadedConsumedQueuePath, isSyncFileInEveryOpt, consumerMetadataFileName)
			consumerMetadataStoreFiles[consumerMetadataFileName] = consumerMetadataFile
		}
	}

	return nil
}

func readConsumerMetadataBlocksAndAdd2Cache(loadingLogger *logs.BeeLogger, logConfig *config.LogConfig, fileBuffer *utils.IoBuffer, consumerMetadataFile *os.File, loadedConsumedQueuePath string,
	isSyncFileInEveryOpt bool, consumerMetadataFileName string)  {
	for ;fileBuffer.HasRemaining(); {
		readConsumerMetadataBlockAndAdd2Cache(loadingLogger, logConfig, fileBuffer, consumerMetadataFile, loadedConsumedQueuePath, isSyncFileInEveryOpt, consumerMetadataFileName)
	}
}

func readConsumerMetadataBlockAndAdd2Cache(loadingLogger *logs.BeeLogger, logConfig *config.LogConfig, fileBuffer *utils.IoBuffer, consumerMetadataFile *os.File,
	loadedConsumedQueuePath string, isSyncFileInEveryOpt bool, consumerMetadataFileName string)  {
	consumerMetadataBlockOffset := int64(fileBuffer.Position())
	consumerCursor := fileBuffer.ReadInt64()
	consumerMetadataBlockChecksum, _ := fileBuffer.ReadByte()
	consumerNameLength, _ := fileBuffer.ReadByte()
	consumerNameBytes := fileBuffer.Next(int(consumerNameLength))

	var headerChecksum int64 = 0
	headerChecksum = utils.AddInt64ToChecksum(consumerCursor, headerChecksum)
	headerChecksum = utils.AddByteToChecksum(consumerNameLength, headerChecksum)
	headerChecksum = utils.AddByteArrayToChecksum(consumerNameBytes, headerChecksum)

	if byte(headerChecksum & 0xFF) == consumerMetadataBlockChecksum {
		loadedConsumerName := string(consumerNameBytes)
		consumer := &FileQueueConsumer{consumedFileQueue: nil, consumerName: loadedConsumerName,
			consumedQueuePath: loadedConsumedQueuePath, cursor: consumerCursor, lastCommitPoint: consumerCursor,
		    consumerMetadataBlockOffset: consumerMetadataBlockOffset, consumerMetadataStoreFile: consumerMetadataFile,
		    isSyncFileInEveryOpt: isSyncFileInEveryOpt, waitingForMessageAdded: false, messageHadBeenAdded: false}
		consumer.logger = logs.NewLogger(logConfig.AsyncLogChanLength)
		consumer.logger.SetLevel(logConfig.Level)
		consumer.logger.SetLogger(logConfig.AdapterName, logConfig.AdapterParameters)
		consumer.logger.SetLogger(logs.AdapterConsole, "{}")
		if logConfig.IsAsync {
			consumer.logger.Async(logConfig.AsyncLogChanLength)
		}

		consumer.messageAddedCond = sync.NewCond(&consumer.lifecycleLock)
		namedFileQueueConsumers[loadedConsumedQueuePath + "_" + loadedConsumerName] = consumer
	} else {
		loadingLogger.Error("Consumer metadata store block checksum error, file path : %+v. Can not load this consumer in this offset.", consumerMetadataFileName)
	}
}

func readFileHeaderAndCheckChecksum(loadingLogger *logs.BeeLogger, fileBuffer *utils.IoBuffer, consumerMetadataFile *os.File, consumerMetadataFileName string) (string, bool) {
	consumedQueuePathLength, _ := fileBuffer.ReadByte()
	consumerMetadataFileHeaderChecksum, _ := fileBuffer.ReadByte()
	consumedQueuePathBytes := fileBuffer.Next(int(consumedQueuePathLength))

	var headerChecksum int64 = 0
	headerChecksum = utils.AddUint32ToChecksum(consumerMetadataFileMagicNumber, headerChecksum)
	headerChecksum = utils.AddUint16ToChecksum(consumerMetadataFileVersion, headerChecksum)
	headerChecksum = utils.AddByteToChecksum(consumedQueuePathLength, headerChecksum)
	headerChecksum = utils.AddByteArrayToChecksum(consumedQueuePathBytes, headerChecksum)

	if byte(headerChecksum & 0xFF) != consumerMetadataFileHeaderChecksum {
		if consumerMetadataFile != nil {
			consumerMetadataFile.Close()
		}

		os.Remove(consumerMetadataFileName)
		loadingLogger.Error("Consumer metadata file checksum error, file path : %+v. Can not load consumers stored in this file.", consumerMetadataFileName)
		return "", false
	}

	loadedConsumedQueuePath := string(consumedQueuePathBytes)
	return loadedConsumedQueuePath, true
}

func checkFileVersionIsMatch(loadingLogger *logs.BeeLogger, fileBuffer *utils.IoBuffer, consumerMetadataFile *os.File, consumerMetadataFileName string) bool {
	fVersion := fileBuffer.ReadUint16()
	if fVersion != consumerMetadataFileVersion {
		if consumerMetadataFile != nil {
			consumerMetadataFile.Close()
		}

		os.Remove(consumerMetadataFileName)
		loadingLogger.Error("Check consumer metadata file version failed, file path : %+v. Can not load consumers stored in this file.", consumerMetadataFileName)
		return false
	}

	return true
}

func checkMagicNumIsMatch(loadingLogger *logs.BeeLogger, fileBuffer *utils.IoBuffer, consumerMetadataFile *os.File, consumerMetadataFileName string) bool {
	magicNum := fileBuffer.ReadUint32()
	if magicNum != consumerMetadataFileMagicNumber {
		if consumerMetadataFile != nil {
			consumerMetadataFile.Close()
		}

		os.Remove(consumerMetadataFileName)
		loadingLogger.Error("Check consumer metadata file magic number failed, file path : %+v. Can not load consumers stored in this file.", consumerMetadataFileName)
		return false
	}

	return true
}

func readAllFileContent(loadingLogger *logs.BeeLogger, consumerMetadataFile *os.File, consumerMetadataFileName string) ([]byte, bool) {
	fileContent, err := ioutil.ReadAll(consumerMetadataFile)
	if err != nil {
		if consumerMetadataFile != nil {
			consumerMetadataFile.Close()
		}

		loadingLogger.Error("Read consumer metadata file content with path : %+v failed. Can not load consumers stored in this file. Error is %+v", consumerMetadataFileName, err)
		return nil, false
	}

	return fileContent, true
}

func (consumer *FileQueueConsumer) Ready() bool {
	return true
}

func (consumer *FileQueueConsumer) Close() error {
	consumer.lifecycleLock.Lock()
	defer consumer.lifecycleLock.Unlock()

	lConsumerName := consumer.consumerName
	lConsumedQueuePath := consumer.consumedQueuePath
	lConsumerMetadataStoreFile := consumer.consumerMetadataStoreFile
	consumer.consumerMetadataStoreFile = nil
	consumer.consumedFileQueue = nil

	if lConsumerMetadataStoreFile == nil {
		return nil
	}

	if err := lConsumerMetadataStoreFile.Sync(); err != nil {
		return err
	}

	newFileQueueConsumerLock.Lock()
	delete(namedFileQueueConsumers, lConsumedQueuePath + "_" + lConsumerName)
	newFileQueueConsumerLock.Unlock()

	return nil
}

func (consumer *FileQueueConsumer) QueueClosed(queueName string) {
	newFileQueueConsumerLock.Lock()
	defer newFileQueueConsumerLock.Unlock()

	consumerMetadataFileName := queueName + consumerMetadataFilePostfix
	consumerMetadataFile, ok := consumerMetadataStoreFiles[consumerMetadataFileName]
	if ok {
		consumerMetadataFile.Sync()
		consumerMetadataFile.Close()
		delete(consumerMetadataStoreFiles, consumerMetadataFileName)
		consumer.logger.Info("Consumer related queue was closed, consumer metadata store file was closed. Queue path : " + queueName)
	}
}

func (consumer *FileQueueConsumer) QueueDeleted(queueName string) {
	newFileQueueConsumerLock.Lock()
	defer newFileQueueConsumerLock.Unlock()

	consumerMetadataFileName := queueName + consumerMetadataFilePostfix
	consumerMetadataFile, ok := consumerMetadataStoreFiles[consumerMetadataFileName]
	if ok {
		consumerMetadataFile.Sync()
		consumerMetadataFile.Close()
		delete(consumerMetadataStoreFiles, consumerMetadataFileName)
		consumer.logger.Info("Consumer related queue was deleted, consumer metadata store file was deleted. Queue path : " + queueName)
	}

	os.Remove(consumerMetadataFileName)
}

func (consumer *FileQueueConsumer) Consume() (msg protocol.IMessage, err error) {
	consumer.lifecycleLock.Lock()
	defer consumer.lifecycleLock.Unlock()

	readNodeChecksumErrorTimes := 0

	for {
		lConsumerCursor := consumer.cursor
		lConsumedFileQueue := consumer.consumedFileQueue
		lConsumerMetadataStoreFile := consumer.consumerMetadataStoreFile

		if lConsumerMetadataStoreFile == nil {
			return nil, ConsumerAlreadyClosedError
		}

		msg, nextNodeOffset, err := lConsumedFileQueue.getNextMessageFromOffset(lConsumerCursor)
		if err == nil {
			if msg != nil {
				consumer.cursor = nextNodeOffset
				return msg, nil
			} else {
				consumer.waitingForMessageAdded = false
				consumer.messageHadBeenAdded = false
				for ;!consumer.messageHadBeenAdded; {
					consumer.waitingForMessageAdded = true
					consumer.messageAddedCond.Wait()
				}
				consumer.waitingForMessageAdded = false
			}
		} else if err == QueueNodeChecksumError {
			readNodeChecksumErrorTimes++
			if readNodeChecksumErrorTimes >= moveToTailNodeAfterResetTimes {
				lastNodeOffset, err := lConsumedFileQueue.getLastNodeOffset()
				if err != nil {
					consumer.cursor = consumer.lastCommitPoint
					consumer.logger.Error("[Consume] Read queue node from offset [%+v] failed. Occur checksum error. Set to lastCommitPoint offset [%+v].", lConsumerCursor, consumer.lastCommitPoint)
				} else {
					consumer.cursor = lastNodeOffset
					consumer.logger.Error("[Consume] Read queue node from offset [%+v] failed. Occur checksum error. Set to lastNodeOffset offset [%+v].", lConsumerCursor, lastNodeOffset)
				}
			}
		} else {
			return nil, err
		}
	}
}

func (consumer *FileQueueConsumer) MessageAdded(queueName string) {
	if consumer.waitingForMessageAdded {
		consumer.lifecycleLock.Lock()
		defer consumer.lifecycleLock.Unlock()

		if consumer.waitingForMessageAdded {
			consumer.messageHadBeenAdded = true
			consumer.messageAddedCond.Broadcast()
		}
	}
}

func (consumer *FileQueueConsumer) ConsumeWithTimeout(timeout int64) (msg protocol.IMessage, err error) {
	consumer.lifecycleLock.Lock()
	defer consumer.lifecycleLock.Unlock()

	isTimeout := false
	isCheckingTimeout := false
	readNodeChecksumErrorTimes := 0

	for {
		lConsumerCursor := consumer.cursor
		lConsumedFileQueue := consumer.consumedFileQueue
		lConsumerMetadataStoreFile := consumer.consumerMetadataStoreFile

		if lConsumerMetadataStoreFile == nil {
			return nil, ConsumerAlreadyClosedError
		}

		msg, nextNodeOffset, err := lConsumedFileQueue.getNextMessageFromOffset(lConsumerCursor)
		if err == nil {
			if msg != nil {
				consumer.cursor = nextNodeOffset
				return msg, nil
			} else {
				if isTimeout {
					return nil, nil
				} else {
					consumer.waitingForMessageAdded = false
					consumer.messageHadBeenAdded = false
					for ;!consumer.messageHadBeenAdded; {
						if !isCheckingTimeout {
							go func() {
								time.Sleep(time.Duration(timeout) * time.Millisecond)
								consumer.lifecycleLock.Lock()
								defer consumer.lifecycleLock.Unlock()
								consumer.messageHadBeenAdded = true
								isTimeout = true
								consumer.messageAddedCond.Broadcast()
							}()

							isCheckingTimeout = true
						}

						consumer.waitingForMessageAdded = true
						consumer.messageAddedCond.Wait()
					}
					consumer.waitingForMessageAdded = false
				}
			}
		} else if err == QueueNodeChecksumError {
			readNodeChecksumErrorTimes++
			if readNodeChecksumErrorTimes >= moveToTailNodeAfterResetTimes {
				lastNodeOffset, err := lConsumedFileQueue.getLastNodeOffset()
				if err != nil {
					consumer.cursor = consumer.lastCommitPoint
					consumer.logger.Error("[ConsumeWithTimeout] Read queue node from offset [%+v] failed. Occur checksum error. Set to lastCommitPoint offset [%+v].", lConsumerCursor, consumer.lastCommitPoint)
				} else {
					consumer.cursor = lastNodeOffset
					consumer.logger.Error("[ConsumeWithTimeout] Read queue node from offset [%+v] failed. Occur checksum error. Set to lastNodeOffset offset [%+v].", lConsumerCursor, lastNodeOffset)
				}
			}
		} else {
			return nil, err
		}
	}
}

func (consumer *FileQueueConsumer) ConsumeNoWait() (msg protocol.IMessage, err error) {
	consumer.lifecycleLock.Lock()
	defer consumer.lifecycleLock.Unlock()

	readNodeChecksumErrorTimes := 0

	for {
		lConsumerCursor := consumer.cursor
		lConsumedFileQueue := consumer.consumedFileQueue
		lConsumerMetadataStoreFile := consumer.consumerMetadataStoreFile

		if lConsumerMetadataStoreFile == nil {
			return nil, ConsumerAlreadyClosedError
		}

		msg, nextNodeOffset, err := lConsumedFileQueue.getNextMessageFromOffset(lConsumerCursor)
		if err == nil {
			if msg != nil {
				consumer.cursor = nextNodeOffset
				return msg, nil
			} else {
				return nil, nil
			}
		} else if err == QueueNodeChecksumError {
			readNodeChecksumErrorTimes++
			if readNodeChecksumErrorTimes >= moveToTailNodeAfterResetTimes {
				lastNodeOffset, err := lConsumedFileQueue.getLastNodeOffset()
				if err != nil {
					consumer.cursor = consumer.lastCommitPoint
					consumer.logger.Error("[ConsumeNoWait] Read queue node from offset [%+v] failed. Occur checksum error. Set to lastCommitPoint offset [%+v].", lConsumerCursor, consumer.lastCommitPoint)
				} else {
					consumer.cursor = lastNodeOffset
					consumer.logger.Error("[ConsumeNoWait] Read queue node from offset [%+v] failed. Occur checksum error. Set to lastNodeOffset offset [%+v].", lConsumerCursor, lastNodeOffset)
				}
			}
		} else {
			return nil, err
		}
	}
}

func (consumer *FileQueueConsumer) Commit() error {
	consumer.lifecycleLock.Lock()
	defer consumer.lifecycleLock.Unlock()

	lConsumerName   := consumer.consumerName
	lConsumerCursor := consumer.cursor
	lConsumerMetadataStoreFile := consumer.consumerMetadataStoreFile
	lConsumerMetadataBlockOffset := consumer.consumerMetadataBlockOffset
	lIsSyncFileInEveryOpt := consumer.isSyncFileInEveryOpt

	if lConsumerMetadataStoreFile == nil {
		return ConsumerAlreadyClosedError
	}

	if err := writeNewCursor2ConsumerMetadataStoreFile(lConsumerName, lConsumerCursor, lConsumerMetadataStoreFile,
		lConsumerMetadataBlockOffset, lIsSyncFileInEveryOpt); err != nil {
		consumer.logger.Error("Write consumer cursor to consumer metadata file error. Consumer Name : [%+v], Error is %+v.", lConsumerName, err)
		return err
	}

	consumer.lastCommitPoint = lConsumerCursor
	return nil
}

func (consumer *FileQueueConsumer) Reset() error {
	consumer.lifecycleLock.Lock()
	defer consumer.lifecycleLock.Unlock()

	lConsumerMetadataStoreFile := consumer.consumerMetadataStoreFile
	if lConsumerMetadataStoreFile == nil {
		return ConsumerAlreadyClosedError
	}

	consumer.cursor = consumer.lastCommitPoint
	return nil
}

func writeNewCursor2ConsumerMetadataStoreFile(lConsumerName string, nextNodeOffset int64, lConsumerMetadataStoreFile *os.File,
	lConsumerMetadataBlockOffset int64, lIsSyncFileInEveryOpt bool) error {
	consumerNameBytes := []byte(lConsumerName)
	consumerNameLength := byte(len(consumerNameBytes))

	var headerChecksum int64 = 0
	headerChecksum = utils.AddInt64ToChecksum(nextNodeOffset, headerChecksum)
	headerChecksum = utils.AddByteToChecksum(consumerNameLength, headerChecksum)
	headerChecksum = utils.AddByteArrayToChecksum(consumerNameBytes, headerChecksum)

	consumerMetadataBlockBytes := make([]byte, 0, consumerMetadataBlockHeaderSize)
	consumerMetadataBlockBuf := utils.NewIoBuffer(consumerMetadataBlockBytes)
	consumerMetadataBlockBuf.WriteInt64(nextNodeOffset)
	consumerMetadataBlockBuf.WriteByte(byte(headerChecksum & 0xFF))

	if _, err := lConsumerMetadataStoreFile.WriteAt(consumerMetadataBlockBuf.Bytes(), lConsumerMetadataBlockOffset); err != nil {
		return err
	}

	if lIsSyncFileInEveryOpt {
		if err := lConsumerMetadataStoreFile.Sync(); err != nil {
			return err
		}
	}

	return nil
}