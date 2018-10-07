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
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Thread safe
type FileQueue struct {
	// For avoid cache line visit
	headNodeAndlastNodeMetadata [utils.CACHE_LINE_SIZE * 3]byte

	// Unchanged value
	isSyncFileInEveryOpt    bool
	messageStoreTimeInMs    int64
	storeFileMinSizeInBytes int64
	queuePath               string
	logger                  *logs.BeeLogger

	storeFile      *os.File
	lifecycleLock  sync.RWMutex
	fileHeaderLock sync.Mutex
	offerLock      sync.Mutex

	queueRelatedResources []QueueRelatedResource
}

const QueueRelativeFilesStoreDir = "." + string(filepath.Separator) + "store" + string(filepath.Separator)

const queueNodeLengthHeaderSize = 4
const queueNodeCreatedTimeSize = 8
const queueNodeHeaderChecksumSize = 1
const queueNodeChecksumSize = 1
const queueNodeHeaderSize = queueNodeLengthHeaderSize + queueNodeCreatedTimeSize + queueNodeHeaderChecksumSize + queueNodeChecksumSize

type QueueNode struct {
	nodeLength     uint32
	createdTime    int64
	headerChecksum byte
	nodeChecksum   byte
	nodeContent    []byte
}

const queueStoreFilePostfix = ".qc"
const fileMagicNumber uint32 = 0x7849550D
const fileVersion uint16 = 1

const fileMagicNumberSize = 4
const fileVersionSize = 2
const headNodeOffsetSize = 8
const lastNodeOffsetSize = 8
const fileHeaderChecksumSize = 1
const fileHeaderSize = fileMagicNumberSize + fileVersionSize + headNodeOffsetSize + lastNodeOffsetSize + fileHeaderChecksumSize
const fileMetaDataHeaderSize = headNodeOffsetSize + lastNodeOffsetSize + fileHeaderChecksumSize
const fileMetaDataHeaderOffset = fileMagicNumberSize + fileVersionSize

const moveToHeadAfterReadEofTimes = 1024

const DefaultStoreFileMinSizeInBytes = 268435456

var newFileQueueLock sync.Mutex
var namedFileQueues map[string]*FileQueue = nil

func NewFileQueue(logConfig *config.LogConfig, queuePath string, isSyncFileInEveryOpt bool, messageStoreTimeInMs int64,
	storeFileMinSizeInBytes int64) (*FileQueue, error) {
	queuePath = strings.Trim(queuePath, " ")
	if len(queuePath) == 0 {
		return nil, errors.New("queuePath can not be nil or empty")
	}

	newFileQueueLock.Lock()
	defer newFileQueueLock.Unlock()

	queuePath = queuePath + queueStoreFilePostfix
	if namedFileQueues == nil {
		namedFileQueues = make(map[string]*FileQueue)
	}

	fQueue, ok := namedFileQueues[queuePath]
	if ok {
		return fQueue, nil
	}

	if isExist := utils.IsFileExist(queuePath); isExist {
		file, err := os.OpenFile(queuePath, os.O_RDWR, os.ModePerm)
		if err != nil {
			if file != nil {
				file.Close()
			}

			return nil, err
		}

		headerBytes, err := readFileHeader(file)
		if err != nil {
			if file != nil {
				file.Close()
			}

			return nil, err
		}

		fileQueue, err := parseHeaderAndCreateFileQueue(logConfig, file, headerBytes, isSyncFileInEveryOpt, messageStoreTimeInMs, storeFileMinSizeInBytes, queuePath)
		if err != nil {
			if file != nil {
				file.Close()
			}

			return nil, err
		}

		namedFileQueues[queuePath] = fileQueue
		return fileQueue, nil
	} else {
		file, err := os.OpenFile(queuePath, os.O_CREATE | os.O_RDWR, os.ModePerm)
		if err != nil {
			if file != nil {
				file.Close()
			}

			return nil, err
		}

		headNodeOffset, lastNodeOffset, headerBytesBuf := createFileHeader()
		fileQueue, err := writeHeaderAndCreateFileQueue(logConfig, file, headNodeOffset, lastNodeOffset, headerBytesBuf,
			isSyncFileInEveryOpt, messageStoreTimeInMs, storeFileMinSizeInBytes, queuePath)
		if err != nil {
			if file != nil {
				file.Close()
			}

			return nil, err
		}

		namedFileQueues[queuePath] = fileQueue
		return fileQueue, nil
	}
}

func (queue *FileQueue) headNodeOffsetPointer() *int64 {
	startPointerRemainder := uintptr(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata)) % 8
	if startPointerRemainder == 0 {
		return (*int64)(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata[utils.CACHE_LINE_SIZE]))
	} else {
		startPointerRemainder = 8 - startPointerRemainder
		return (*int64)(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata[utils.CACHE_LINE_SIZE + startPointerRemainder]))
	}
}

func (queue *FileQueue) headNodeCreatedTimePointer() *int64 {
	startPointerRemainder := uintptr(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata)) % 8
	if startPointerRemainder == 0 {
		return (*int64)(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata[utils.CACHE_LINE_SIZE + 8]))
	} else {
		startPointerRemainder = 8 - startPointerRemainder
		return (*int64)(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata[utils.CACHE_LINE_SIZE + startPointerRemainder + 8]))
	}
}

func (queue *FileQueue) lastNodeOffsetPointer() *int64 {
	startPointerRemainder := uintptr(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata)) % 8
	if startPointerRemainder == 0 {
		return (*int64)(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata[utils.CACHE_LINE_SIZE + 16]))
	} else {
		startPointerRemainder = 8 - startPointerRemainder
		return (*int64)(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata[utils.CACHE_LINE_SIZE + startPointerRemainder + 16]))
	}
}

func (queue *FileQueue) lastNodeLengthPointer() *int64 {
	startPointerRemainder := uintptr(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata)) % 8
	if startPointerRemainder == 0 {
		return (*int64)(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata[utils.CACHE_LINE_SIZE + 24]))
	} else {
		startPointerRemainder = 8 - startPointerRemainder
		return (*int64)(unsafe.Pointer(&queue.headNodeAndlastNodeMetadata[utils.CACHE_LINE_SIZE + startPointerRemainder + 24]))
	}
}

func readFileHeader(file *os.File) ([]byte, error) {
	headerBytes := make([]byte, fileHeaderSize)
	if _, err := file.Seek(0, 0); err != nil {
		return nil, err
	}

	readedNum, err := file.Read(headerBytes)
	if err != nil {
		return nil, err
	}

	for ; readedNum < fileHeaderSize; {
		readingBytes := headerBytes[readedNum:]
		n, err := file.Read(readingBytes)
		if err != nil {
			return nil, err
		}

		readedNum += n
	}

	return headerBytes, nil
}

func parseHeaderAndCreateFileQueue(logConfig *config.LogConfig, file *os.File, headerBytes []byte, isSyncFileInEveryOpt bool,
	messageStoreTimeInMs int64, storeFileMinSizeInBytes int64, queuePath string) (*FileQueue, error) {
	headerBytesBuf := utils.NewIoBuffer(headerBytes)
	magicNum := headerBytesBuf.ReadUint32()
	if magicNum != fileMagicNumber {
		return nil, StoreFileMagicNumberError
	}

	fVersion := headerBytesBuf.ReadUint16()
	if fVersion != fileVersion {
		return nil, StoreFileVersionNumberError
	}

	headNodeOffset := headerBytesBuf.ReadInt64()
	lastNodeOffset := headerBytesBuf.ReadInt64()

	readedHeaderChecksum, err := headerBytesBuf.ReadByte()
	if err != nil {
		return nil, err
	}

	calHeaderChecksum := calculateFileHeaderChecksum(magicNum, fVersion, headNodeOffset, lastNodeOffset)
	if byte(calHeaderChecksum & 0xFF) != readedHeaderChecksum {
		return nil, StoreFileHeaderChecksumError
	}

	_, _, headNodeCreatedTime, err := readQueueNodeHeaderFromFile(file, headNodeOffset)
	if err != nil {
		return nil, err
	}

	_, lastNodeLength, _, err := readQueueNodeHeaderFromFile(file, lastNodeOffset)
	if err != nil {
		return nil, err
	}

	fq := &FileQueue{storeFile: file, isSyncFileInEveryOpt: isSyncFileInEveryOpt, messageStoreTimeInMs: messageStoreTimeInMs,
	    storeFileMinSizeInBytes: storeFileMinSizeInBytes, queuePath: queuePath, queueRelatedResources: make([]QueueRelatedResource, 0, 1024)}
	fq.logger = logs.NewLogger(logConfig.AsyncLogChanLength)
	fq.logger.SetLevel(logConfig.Level)
	fq.logger.SetLogger(logConfig.AdapterName, logConfig.AdapterParameters)
	fq.logger.SetLogger(logs.AdapterConsole, "{}")
	if logConfig.IsAsync {
		fq.logger.Async(logConfig.AsyncLogChanLength)
	}

	headNodeOffsetPointer       := fq.headNodeOffsetPointer()
	headNodeCreatedTimePointer  := fq.headNodeCreatedTimePointer()
	lastNodeOffsetPointer       := fq.lastNodeOffsetPointer()
	lastNodeLengthPointer       := fq.lastNodeLengthPointer()
	atomic.StoreInt64(headNodeOffsetPointer, headNodeOffset)
	atomic.StoreInt64(headNodeCreatedTimePointer, headNodeCreatedTime)
	atomic.StoreInt64(lastNodeOffsetPointer, lastNodeOffset)
	atomic.StoreInt64(lastNodeLengthPointer, int64(lastNodeLength))

	return fq, nil
}

func createFileHeader() (headNodeOffset, lastNodeOffset int64, headerBytesBuf *utils.IoBuffer) {
	headNodeOffset = int64(fileHeaderSize)
	lastNodeOffset = int64(fileHeaderSize)
	headerChecksum := calculateFileHeaderChecksum(fileMagicNumber, fileVersion, headNodeOffset, lastNodeOffset)

	headerBytes := make([]byte, 0, fileHeaderSize)
	headerBytesBuf = utils.NewIoBuffer(headerBytes)
	headerBytesBuf.WriteUint32(fileMagicNumber)
	headerBytesBuf.WriteUint16(fileVersion)
	headerBytesBuf.WriteInt64(headNodeOffset)
	headerBytesBuf.WriteInt64(lastNodeOffset)
	headerBytesBuf.WriteByte(byte(headerChecksum & 0xFF))
	return headNodeOffset, lastNodeOffset, headerBytesBuf
}

func createFileMetaDataHeader(headNodeOffset, lastNodeOffset int64) (metaDataHeaderBytesBuf *utils.IoBuffer) {
	headerChecksum := calculateFileHeaderChecksum(fileMagicNumber, fileVersion, headNodeOffset, lastNodeOffset)
	headerBytes := make([]byte, 0, fileMetaDataHeaderSize)
	metaDataHeaderBytesBuf = utils.NewIoBuffer(headerBytes)
	metaDataHeaderBytesBuf.WriteInt64(headNodeOffset)
	metaDataHeaderBytesBuf.WriteInt64(lastNodeOffset)
	metaDataHeaderBytesBuf.WriteByte(byte(headerChecksum & 0xFF))
	return metaDataHeaderBytesBuf
}

func calculateFileHeaderChecksum(magicNum uint32, fVersion uint16, headNodeOffset, lastNodeOffset int64) int64 {
	var headerChecksum int64 = 0
	headerChecksum = utils.AddUint32ToChecksum(magicNum, headerChecksum)
	headerChecksum = utils.AddUint16ToChecksum(fVersion, headerChecksum)
	headerChecksum = utils.AddInt64ToChecksum(headNodeOffset, headerChecksum)
	headerChecksum = utils.AddInt64ToChecksum(lastNodeOffset, headerChecksum)
	return headerChecksum
}

func writeHeaderAndCreateFileQueue(logConfig *config.LogConfig, file *os.File, headNodeOffset, lastNodeOffset int64, headerBytesBuf *utils.IoBuffer,
	isSyncFileInEveryOpt bool, messageStoreTimeInMs int64, storeFileMinSizeInBytes int64, queuePath string) (*FileQueue, error) {
	if _, err := file.Seek(0, 0); err != nil {
		return nil, err
	}

	_, wErr := file.Write(headerBytesBuf.Bytes())
	if wErr != nil {
		return nil, wErr
	}

	seqGen, err := utils.NewSequenceGenerator()
	if err != nil {
		return nil, err
	}

	headMsg, err := protocol.NewEmptyBytesMessage(seqGen)
	if err != nil {
		return nil, err
	}

	headNode, err := createQueueNode(headMsg)
	if err != nil {
		return nil, err
	}

	headNodeBytes, err := queueNodeToBytes(headNode)
	if err != nil {
		return nil, err
	}

	_, err1 := file.WriteAt(headNodeBytes, lastNodeOffset)
	if err1 != nil {
		return nil, err1
	}

	if err := file.Sync(); err != nil {
		return nil, err
	}

	fq := &FileQueue{storeFile: file, isSyncFileInEveryOpt: isSyncFileInEveryOpt, messageStoreTimeInMs: messageStoreTimeInMs,
	    storeFileMinSizeInBytes: storeFileMinSizeInBytes, queuePath: queuePath, queueRelatedResources: make([]QueueRelatedResource, 0, 1024)}
	fq.logger = logs.NewLogger(logConfig.AsyncLogChanLength)
	fq.logger.SetLevel(logConfig.Level)
	fq.logger.SetLogger(logConfig.AdapterName, logConfig.AdapterParameters)
	fq.logger.SetLogger(logs.AdapterConsole, "{}")
	if logConfig.IsAsync {
		fq.logger.Async(logConfig.AsyncLogChanLength)
	}

	headNodeOffsetPointer       := fq.headNodeOffsetPointer()
	headNodeCreatedTimePointer  := fq.headNodeCreatedTimePointer()
	lastNodeOffsetPointer       := fq.lastNodeOffsetPointer()
	lastNodeLengthPointer       := fq.lastNodeLengthPointer()
	atomic.StoreInt64(headNodeOffsetPointer, headNodeOffset)
	atomic.StoreInt64(headNodeCreatedTimePointer, headNode.createdTime)
	atomic.StoreInt64(lastNodeOffsetPointer, lastNodeOffset)
	atomic.StoreInt64(lastNodeLengthPointer, int64(headNode.nodeLength))

	return fq, nil
}

func createQueueNode(message protocol.IMessage) (*QueueNode, error) {
	if message == nil {
		return nil, MsgIsNullError
	}

	msgBytes, err := protocol.MessageToBytes(message)
	if err != nil {
		return nil, err
	}

	createdTime := utils.CurrentTimeMillis()
	nodeLength := uint32(queueNodeHeaderSize + len(msgBytes))
	hChecksum := calculateQueueNodeHeaderChecksum(nodeLength, createdTime)
	nChecksum := calculateQueueNodeChecksum(hChecksum, msgBytes)

	var headerChecksum = byte(hChecksum & 0xFF)
	var nodeChecksum = byte(nChecksum & 0xFF)

	return &QueueNode{nodeLength: nodeLength, createdTime: createdTime, headerChecksum: headerChecksum, nodeChecksum: nodeChecksum, nodeContent: msgBytes}, nil
}

func calculateQueueNodeChecksum(hChecksum int64, msgBytes []byte) int64 {
	var nChecksum = hChecksum
	nChecksum = utils.AddByteArrayToChecksum(msgBytes, nChecksum)
	return nChecksum
}

func calculateQueueNodeHeaderChecksum(nodeLength uint32, createdTime int64) int64 {
	var hChecksum int64 = 0
	hChecksum = utils.AddUint32ToChecksum(nodeLength, hChecksum)
	hChecksum = utils.AddInt64ToChecksum(createdTime, hChecksum)
	return hChecksum
}

func queueNodeToBytes(node *QueueNode) ([]byte, error) {
	msgBytesPackage := utils.NewIoBuffer(make([]byte, 0, node.nodeLength))
	msgBytesPackage.WriteUint32(node.nodeLength)
	msgBytesPackage.WriteInt64(node.createdTime)
	msgBytesPackage.WriteByte(node.headerChecksum)
	msgBytesPackage.WriteByte(node.nodeChecksum)
	msgBytesPackage.Write(node.nodeContent)

	return msgBytesPackage.Bytes(), nil
}

func bytesToQueueNode(bytes []byte) (*QueueNode, error) {
	nodeBytesPackage := utils.NewIoBuffer(bytes)
	nodeLength := nodeBytesPackage.ReadUint32()
	createdTime := nodeBytesPackage.ReadInt64()
	headerChecksum, err := nodeBytesPackage.ReadByte()
	if err != nil {
		return nil, err
	}

	nodeChecksum, err := nodeBytesPackage.ReadByte()
	if err != nil {
		return nil, err
	}

	nodeContent := nodeBytesPackage.Next(int(nodeLength - queueNodeHeaderSize) & 0xFFFFFF)
	hChecksum := calculateQueueNodeHeaderChecksum(nodeLength, createdTime)
	nChecksum := calculateQueueNodeChecksum(hChecksum, nodeContent)

	if byte(hChecksum & 0xFF) != headerChecksum || byte(nChecksum & 0xFF) != nodeChecksum {
		return nil, QueueNodeChecksumError
	}

	return &QueueNode{nodeLength: nodeLength, createdTime: createdTime, headerChecksum: headerChecksum, nodeChecksum: nodeChecksum, nodeContent: nodeContent}, nil
}

func (queue *FileQueue) QueueName() string {
	queue.lifecycleLock.RLock()
	defer queue.lifecycleLock.RUnlock()

	localStoreFile := queue.storeFile
	if localStoreFile == nil {
		return ""
	}

	return queue.queuePath
}

func (queue *FileQueue) Peek() (protocol.IMessage, error) {
	queue.lifecycleLock.RLock()
	defer queue.lifecycleLock.RUnlock()

	localStoreFile := queue.storeFile
	if localStoreFile == nil {
		return nil, StoreFileAlreadyClosedError
	}

	headNodeOffsetPointer       := queue.headNodeOffsetPointer()
	lastNodeOffsetPointer       := queue.lastNodeOffsetPointer()

	for {
		localHeadNodeOffset := atomic.LoadInt64(headNodeOffsetPointer)
		localLastNodeOffset := atomic.LoadInt64(lastNodeOffsetPointer)

		if localHeadNodeOffset == localLastNodeOffset {
			// Empty Queue
			return nil, nil
		}

		_, headNodeLength, _, err := readQueueNodeHeaderFromFile(localStoreFile, localHeadNodeOffset)
		if err != nil && err != QueueNodeChecksumError {
			queue.logger.Error("Read head node header from offset [%+v] failed when peeking message from file queue. Queue Path : %+v, Error is %+v", localHeadNodeOffset, queue.queuePath, err)
			return nil, err
		}

		if err == nil {
			localHeadNodeOffset += int64(headNodeLength)
			msg, err := getMessageFromNode(localStoreFile, localHeadNodeOffset)
			if err != nil {
				if err == io.EOF {
					// Read head node
					msg, err := getMessageFromNode(localStoreFile, int64(fileHeaderSize))
					if err != nil && err != QueueNodeChecksumError {
						queue.logger.Error("Read first node from offset [%+v] failed when peeking message from file queue. Queue Path : %+v, Error is %+v", fileHeaderSize, queue.queuePath, err)
						return nil, err
					}

					if err == nil {
						return msg, nil
					}
				} else if err != QueueNodeChecksumError {
					queue.logger.Error("Read node from offset [%+v] failed when peeking message from file queue. Queue Path : %+v, Error is %+v", localHeadNodeOffset, queue.queuePath, err)
					return nil, err
				}
			} else {
				return msg, nil
			}
		}
	}
}

func (queue *FileQueue) getNextMessageFromOffset(nodeOffset int64) (message protocol.IMessage, nextNodeOffset int64, err error) {
	queue.lifecycleLock.RLock()
	defer queue.lifecycleLock.RUnlock()

	localStoreFile := queue.storeFile
	if localStoreFile == nil {
		return nil, -1, StoreFileAlreadyClosedError
	}

	lastNodeOffsetPointer := queue.lastNodeOffsetPointer()
	localLastNodeOffset   := atomic.LoadInt64(lastNodeOffsetPointer)
	if nodeOffset == localLastNodeOffset {
		// Have No Node for Reading
		return nil, nodeOffset, nil
	}

	_, nodeLength, _, err := readQueueNodeHeaderFromFile(localStoreFile, nodeOffset)
	if err != nil {
		queue.logger.Error("Read node header from offset [%+v] failed when get message from file queue. Queue Path : %+v, Error is %+v", nodeOffset, queue.queuePath, err)
		return nil, -1, err
	}

	nodeOffset += int64(nodeLength)
	readEofTimes := 0

	for {
		msg, err := getMessageFromNode(localStoreFile, nodeOffset)
		if err != nil {
			if err == io.EOF {
				readEofTimes++
				if readEofTimes >= moveToHeadAfterReadEofTimes {
					// Really read the end of file, Set to head node
					msg, err := getMessageFromNode(localStoreFile, int64(fileHeaderSize))
					if err != nil {
						queue.logger.Error("Read first node from offset [%+v] failed when get message from file queue. Queue Path : %+v, Error is %+v", fileHeaderSize, queue.queuePath, err)
						return nil, -1, err
					}

					return msg, int64(fileHeaderSize), nil
				}
			} else {
				queue.logger.Error("Read node from offset [%+v] failed when get message from file queue. Queue Path : %+v, Error is %+v", nodeOffset, queue.queuePath, err)
				return nil, -1, err
			}
		} else {
			return msg, nodeOffset, nil
		}
	}
}

func getMessageFromNode(storeFile *os.File, nodeOffset int64) (protocol.IMessage, error) {
	node, err := readQueueNodeFromFile(storeFile, nodeOffset)
	if err != nil {
		return nil, err
	}

	msg, err := protocol.BytesToMessage(node.nodeContent)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (queue *FileQueue) getHeadNodeOffset() (int64, error) {
	queue.lifecycleLock.RLock()
	defer queue.lifecycleLock.RUnlock()

	localStoreFile := queue.storeFile
	if localStoreFile == nil {
		return 0, StoreFileAlreadyClosedError
	}

	headNodeOffsetPointer := queue.headNodeOffsetPointer()
	return atomic.LoadInt64(headNodeOffsetPointer), nil
}

func (queue *FileQueue) getLastNodeOffset() (int64, error) {
	queue.lifecycleLock.RLock()
	defer queue.lifecycleLock.RUnlock()

	localStoreFile := queue.storeFile
	if localStoreFile == nil {
		return 0, StoreFileAlreadyClosedError
	}

	lastNodeOffsetPointer := queue.lastNodeOffsetPointer()
	return atomic.LoadInt64(lastNodeOffsetPointer), nil
}

func (queue *FileQueue) IsEmpty() bool {
	queue.lifecycleLock.RLock()
	defer queue.lifecycleLock.RUnlock()

	localStoreFile := queue.storeFile
	if localStoreFile == nil {
		return true
	}

	headNodeOffsetPointer := queue.headNodeOffsetPointer()
	lastNodeOffsetPointer := queue.lastNodeOffsetPointer()
	localHeadNodeOffset := atomic.LoadInt64(headNodeOffsetPointer)
	localLastNodeOffset := atomic.LoadInt64(lastNodeOffsetPointer)

	return localHeadNodeOffset == localLastNodeOffset
}

func readQueueNodeFromFile(storeFile *os.File, nodeOffset int64) (*QueueNode, error) {
	nodeHeaderBytes, nodeLength, _, err := readQueueNodeHeaderFromFile(storeFile, nodeOffset)
	if err != nil {
		return nil, err
	}

	nodeContentBytes := make([]byte, nodeLength - queueNodeHeaderSize)
	_, err1 := storeFile.ReadAt(nodeContentBytes, nodeOffset + queueNodeHeaderSize)
	if err1 != nil {
		return nil, err1
	}

	nodeBytes := make([]byte, nodeLength)
	copy(nodeBytes, nodeHeaderBytes)
	nodeBytesForCopy := nodeBytes[queueNodeHeaderSize:]
	copy(nodeBytesForCopy, nodeContentBytes)

	node, err := bytesToQueueNode(nodeBytes)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func readQueueNodeHeaderFromFile(storeFile *os.File, nodeOffset int64) (header []byte, nodeLength uint32, createdTime int64, err error) {
	nodeHeaderBytes := make([]byte, queueNodeHeaderSize)
	_, err = storeFile.ReadAt(nodeHeaderBytes, nodeOffset)
	if err != nil {
		return nil, 0, 0, err
	}

	nodeLength = utils.Combine4Bytes2UInt32(uint32(nodeHeaderBytes[0]), uint32(nodeHeaderBytes[1]), uint32(nodeHeaderBytes[2]), uint32(nodeHeaderBytes[3]))
	createdTime = utils.Combine8Bytes2Int64(uint64(nodeHeaderBytes[4]), uint64(nodeHeaderBytes[5]), uint64(nodeHeaderBytes[6]), uint64(nodeHeaderBytes[7]),
		uint64(nodeHeaderBytes[8]), uint64(nodeHeaderBytes[9]), uint64(nodeHeaderBytes[10]), uint64(nodeHeaderBytes[11]))
	headerChecksum := nodeHeaderBytes[12]
	hChecksum := calculateQueueNodeHeaderChecksum(nodeLength, createdTime)
	if byte(hChecksum & 0xFF) != headerChecksum {
		return nil, 0, 0, QueueNodeChecksumError
	}

	return nodeHeaderBytes, nodeLength, createdTime, nil
}

func (queue *FileQueue) writeQueueNodeToFile(storeFile *os.File, nodeOffset int64, node *QueueNode, headNodeOffset, lastNodeOffset int64) error {
	nodeBytes, err := queueNodeToBytes(node)
	if err != nil {
		return err
	}

	_, err1 := storeFile.WriteAt(nodeBytes, nodeOffset)
	if err1 != nil {
		return err1
	}

	headNodeOffsetPointer := queue.headNodeOffsetPointer()
	lastNodeOffsetPointer := queue.lastNodeOffsetPointer()

	queue.fileHeaderLock.Lock()
	defer queue.fileHeaderLock.Unlock()

	localHeadNodeOffset := atomic.LoadInt64(headNodeOffsetPointer)
	localLastNodeOffset  := atomic.LoadInt64(lastNodeOffsetPointer)
	if localHeadNodeOffset == headNodeOffset && localLastNodeOffset == lastNodeOffset {
		fileHeaderBytesBuf := createFileMetaDataHeader(headNodeOffset, lastNodeOffset)
		_, err2 := storeFile.WriteAt(fileHeaderBytesBuf.Bytes(), fileMetaDataHeaderOffset)
		if err2 != nil {
			return err2
		}
	}

	return nil
}

func (queue *FileQueue) offer(message protocol.IMessage) error {
	if message == nil {
		return MsgIsNullError
	}

	queue.lifecycleLock.RLock()
	defer queue.lifecycleLock.RUnlock()

	localStoreFile := queue.storeFile
	localStoreFileMinSizeInBytes := queue.storeFileMinSizeInBytes
	localMessageStoreTimeInMs := queue.messageStoreTimeInMs
	localIsSyncFileInEveryOpt := queue.isSyncFileInEveryOpt
	localQueuePath := queue.queuePath
	lQueueRelatedResources := queue.queueRelatedResources

	if localStoreFile == nil {
		return StoreFileAlreadyClosedError
	}

	var newLastNodeOffset int64 = 0
	var newHeadNodeOffset int64 = 0
	var newLastNode *QueueNode = nil

	headNodeOffsetPointer       := queue.headNodeOffsetPointer()
	headNodeCreatedTimePointer  := queue.headNodeCreatedTimePointer()
	lastNodeOffsetPointer       := queue.lastNodeOffsetPointer()
	lastNodeLengthPointer       := queue.lastNodeLengthPointer()

	queue.offerLock.Lock()

	localHeadNodeOffset       := atomic.LoadInt64(headNodeOffsetPointer)
	localHeadNodeCreatedTime  := atomic.LoadInt64(headNodeCreatedTimePointer)
	localLastNodeOffset       := atomic.LoadInt64(lastNodeOffsetPointer)
	localLastNodeLength       := atomic.LoadInt64(lastNodeLengthPointer)

	newLastNodeOffset = localLastNodeOffset + int64(localLastNodeLength)
	if newLastNodeOffset >= utils.FileSize(localQueuePath) &&
		newLastNodeOffset > (localStoreFileMinSizeInBytes + fileHeaderSize) &&
		(utils.CurrentTimeMillis() - localHeadNodeCreatedTime) > localMessageStoreTimeInMs {
		newLastNodeOffset = int64(fileHeaderSize)
	}

	var err error = nil
	newLastNode, err = createQueueNode(message)
	if err != nil {
		queue.offerLock.Unlock()
		return err
	}

	if newLastNodeOffset <= localHeadNodeOffset {
		// Move Head Node Offset
		for ; localHeadNodeOffset < (newLastNodeOffset + int64(newLastNode.nodeLength)); {
			_, headNodeLength, _, err := readQueueNodeHeaderFromFile(localStoreFile, localHeadNodeOffset)
			if err != nil {
				queue.offerLock.Unlock()
				queue.logger.Error("Read head node header from file failed. Offset : %+v, Queue Path : %+v, Error is %+v", localHeadNodeOffset, queue.queuePath, err)
				return err
			}

			localHeadNodeOffset += int64(headNodeLength)
			if localHeadNodeOffset >= utils.FileSize(localQueuePath) {
				localHeadNodeOffset = int64(fileHeaderSize)
				break
			}
		}

		_, _, headNodeCreatedTime, err := readQueueNodeHeaderFromFile(localStoreFile, localHeadNodeOffset)
		if err != nil {
			queue.offerLock.Unlock()
			queue.logger.Error("Read head node from file failed. Offset : %+v, Queue Path : %+v, Error is %+v", localHeadNodeOffset, queue.queuePath, err)
			return err
		}

		localHeadNodeCreatedTime = headNodeCreatedTime
	}

	newHeadNodeOffset = localHeadNodeOffset
	atomic.StoreInt64(headNodeOffsetPointer, newHeadNodeOffset)
	atomic.StoreInt64(headNodeCreatedTimePointer, localHeadNodeCreatedTime)
	atomic.StoreInt64(lastNodeOffsetPointer, newLastNodeOffset)
	atomic.StoreInt64(lastNodeLengthPointer, int64(newLastNode.nodeLength))

	queue.offerLock.Unlock()

	err1 := queue.writeQueueNodeToFile(localStoreFile, newLastNodeOffset, newLastNode, newHeadNodeOffset, newLastNodeOffset)
	if err1 != nil {
		queue.logger.Error("Write node info to file failed. Offset : %+v, Queue Path : %+v, Error is %+v", newLastNodeOffset, queue.queuePath, err1)
		return err1
	}

	if localIsSyncFileInEveryOpt {
		if err := localStoreFile.Sync(); err != nil {
			queue.logger.Error("Write node info to file failed. Offset : %+v, Queue Path : %+v, Error is %+v", newLastNodeOffset, queue.queuePath, err)
			return err
		}
	}

	for _, res := range lQueueRelatedResources {
		res.MessageAdded(localQueuePath)
	}

	return nil
}

func (queue *FileQueue) addQueueRelatedResource(res QueueRelatedResource) {
	if res == nil {
		panic("parameter res can not be nil")
	}

	queue.lifecycleLock.Lock()
	defer queue.lifecycleLock.Unlock()

	queue.queueRelatedResources = append(queue.queueRelatedResources, res)
}

func (queue *FileQueue) Close() error {
	queue.lifecycleLock.Lock()
	defer queue.lifecycleLock.Unlock()

	queuePath := queue.queuePath
	localStoreFile := queue.storeFile
	lQueueRelatedResources := queue.queueRelatedResources
	queue.storeFile = nil

	if localStoreFile == nil {
		return nil
	}

	if err := localStoreFile.Sync(); err != nil {
		return err
	}

	if err := localStoreFile.Close(); err != nil {
		return err
	}

	for _, res := range lQueueRelatedResources {
		res.QueueClosed(queuePath)
	}

	queue.queueRelatedResources = queue.queueRelatedResources[:0]

	newFileQueueLock.Lock()
	delete(namedFileQueues, queuePath)
	newFileQueueLock.Unlock()

	return nil
}

func (queue *FileQueue) Delete() error {
	queue.lifecycleLock.Lock()
	defer queue.lifecycleLock.Unlock()

	queuePath := queue.queuePath
	localStoreFile := queue.storeFile
	lQueueRelatedResources := queue.queueRelatedResources
	queue.storeFile = nil

	if localStoreFile == nil {
		if err := os.Remove(queuePath); err != nil {
			return err
		}

		for _, res := range lQueueRelatedResources {
			res.QueueDeleted(queuePath)
		}

		queue.queueRelatedResources = queue.queueRelatedResources[:0]

		return nil
	} else {
		if err := localStoreFile.Sync(); err != nil {
			return err
		}

		if err := localStoreFile.Close(); err != nil {
			return err
		}

		if err := os.Remove(queuePath); err != nil {
			return err
		}

		for _, res := range lQueueRelatedResources {
			res.QueueDeleted(queuePath)
		}

		queue.queueRelatedResources = queue.queueRelatedResources[:0]

		newFileQueueLock.Lock()
		delete(namedFileQueues, queuePath)
		newFileQueueLock.Unlock()

		return nil
	}
}

