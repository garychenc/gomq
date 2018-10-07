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
	"bytes"
	"compress/gzip"
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"path/filepath"
)

const DEFAULT_IO_BUFFER_SIZE = 1024 * 16
const CACHE_LINE_SIZE = 64

func GZip(source []byte) ([]byte, error) {
	var zipBuffer bytes.Buffer
	zipWriter := gzip.NewWriter(&zipBuffer)
	defer func() {
		if  zipWriter != nil {
			zipWriter.Close()
		}
	}()

	_, err := zipWriter.Write(source)
	if err != nil {
		return nil, err
	}

	err = zipWriter.Flush()
	if err != nil {
		return nil, err
	}

	err = zipWriter.Close()
	if err != nil {
		return nil, err
	}

	return zipBuffer.Bytes(), nil
}

func UnGZip(source []byte) ([]byte, error) {
	zipBuffer := bytes.NewBuffer(source)
	zipReader, err := gzip.NewReader(zipBuffer)
	defer func() {
		if  zipReader != nil {
			zipReader.Close()
		}
	}()

	if err != nil {
		return nil, err
	}

	unzipData, err1 := ioutil.ReadAll(zipReader)
	if err1 != nil {
		return nil, err1
	}

	err1 = zipReader.Close()
	if err1 != nil {
		return nil, err1
	}

	return unzipData, nil
}

func Combine2Bytes2Int16(byte1, byte2 uint16) int16 {
	return int16(((byte1 << 8) & 0xFF00) | (byte2 & 0x00FF))
}

func Combine2BytesArray2Int16(bytes []byte) int16 {
	return Combine2Bytes2Int16(uint16(bytes[0]), uint16(bytes[1]))
}

func Combine2Bytes2UInt16(byte1, byte2 uint16) uint16 {
	return ((byte1 << 8) & 0xFF00) | (byte2 & 0x00FF)
}

func Combine2BytesArray2UInt16(bytes []byte) uint16 {
	return Combine2Bytes2UInt16(uint16(bytes[0]), uint16(bytes[1]))
}

func Combine4Bytes2Int32(byte1, byte2, byte3, byte4 uint32) int32 {
	return int32(((byte1 << 24) & 0xFF000000) |
		((byte2 << 16) & 0x00FF0000) |
		((byte3 << 8) & 0x0000FF00) |
		(byte4 & 0x000000FF))
}

func Combine4BytesArray2Int32(bytes []byte) int32 {
	return Combine4Bytes2Int32(uint32(bytes[0]), uint32(bytes[1]), uint32(bytes[2]), uint32(bytes[3]))
}

func Combine4Bytes2UInt32(byte1, byte2, byte3, byte4 uint32) uint32 {
	return ((byte1 << 24) & 0xFF000000) |
		((byte2 << 16) & 0x00FF0000) |
		((byte3 << 8) & 0x0000FF00) |
		(byte4 & 0x000000FF)
}

func Combine4BytesArray2UInt32(bytes []byte) uint32 {
	return Combine4Bytes2UInt32(uint32(bytes[0]), uint32(bytes[1]), uint32(bytes[2]), uint32(bytes[3]))
}

func SplitUInt32To4BytesArray(v uint32) []byte {
	valueArray := make([]byte, 4)

	valueArray[0] = (byte)(v >> 24 & 0xff)
	valueArray[1] = (byte)(v >> 16 & 0xff)
	valueArray[2] = (byte)(v >> 8 & 0xff)
	valueArray[3] = (byte)(v & 0xff)

	return valueArray
}

func SplitUInt32To4BytesArrayStore(v uint32, store []byte) {
	store[0] = (byte)(v >> 24 & 0xff)
	store[1] = (byte)(v >> 16 & 0xff)
	store[2] = (byte)(v >> 8 & 0xff)
	store[3] = (byte)(v & 0xff)
}

func SplitUInt32To4BytesArrayStoreReversed(v uint32, store []byte) {
	store[3] = (byte)(v >> 24 & 0xff)
	store[2] = (byte)(v >> 16 & 0xff)
	store[1] = (byte)(v >> 8 & 0xff)
	store[0] = (byte)(v & 0xff)
}

func Combine8Bytes2Int64(byte1, byte2, byte3, byte4, byte5, byte6, byte7, byte8 uint64) int64 {
	return int64(((byte1 << 56) & 0xFF00000000000000) |
		((byte2 << 48) & 0x00FF000000000000) |
		((byte3 << 40) & 0x0000FF0000000000) |
		((byte4 << 32) & 0x000000FF00000000) |
		((byte5 << 24) & 0x00000000FF000000) |
		((byte6 << 16) & 0x0000000000FF0000) |
		((byte7 << 8)  & 0x000000000000FF00) |
		( byte8        & 0x00000000000000FF))
}

func Combine8BytesArray2Int64(bytes []byte) int64 {
	return Combine8Bytes2Int64(uint64(bytes[0]), uint64(bytes[1]), uint64(bytes[2]), uint64(bytes[3]), uint64(bytes[4]), uint64(bytes[5]), uint64(bytes[6]), uint64(bytes[7]))
}

func SplitInt64To8BytesArray(v int64) []byte {
	valueArray := make([]byte, 8)

	valueArray[0] = (byte)(v >> 56 & 0xff)
	valueArray[1] = (byte)(v >> 48 & 0xff)
	valueArray[2] = (byte)(v >> 40 & 0xff)
	valueArray[3] = (byte)(v >> 32 & 0xff)
	valueArray[4] = (byte)(v >> 24 & 0xff)
	valueArray[5] = (byte)(v >> 16 & 0xff)
	valueArray[6] = (byte)(v >> 8 & 0xff)
	valueArray[7] = (byte)(v & 0xff)

	return valueArray
}

func SplitInt64To8BytesArrayStoreWithAlign(v int64, store []byte) {
	store[0] = (byte)(v >> 56 & 0xff)
	store[1] = (byte)(v >> 48 & 0xff)
	store[2] = (byte)(v >> 40 & 0xff)
	store[3] = (byte)(v >> 32 & 0xff)
	store[4] = (byte)(v >> 24 & 0xff)
	store[5] = (byte)(v >> 16 & 0xff)
	store[6] = (byte)(v >> 8 & 0xff)
	store[7] = (byte)(v & 0xff)
}

func SplitInt64To8BytesArrayStoreWithAlignReversed(v int64, store []byte) {
	store[7] = (byte)(v >> 56 & 0xff)
	store[6] = (byte)(v >> 48 & 0xff)
	store[5] = (byte)(v >> 40 & 0xff)
	store[4] = (byte)(v >> 32 & 0xff)
	store[3] = (byte)(v >> 24 & 0xff)
	store[2] = (byte)(v >> 16 & 0xff)
	store[1] = (byte)(v >> 8 & 0xff)
	store[0] = (byte)(v & 0xff)
}

func IsFileExist(path string) bool {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			// file does not exist
			return false
		} else {
			// other error
			return false
		}
	} else {
		//exist
		return true
	}
}

func FileSize(path string) int64 {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return -1
	}

	return fileInfo.Size()
}

func AddByteToChecksum(v byte, checksum int64) int64 {
	return checksum + int64(v & 0xFF)
}

func AddUint16ToChecksum(v uint16, checksum int64) int64 {
	checksum = checksum + int64(v >> 8 & 0xff)
	checksum = checksum + int64(v & 0xff)
	return checksum
}

func AddInt16ToChecksum(v int16, checksum int64) int64 {
	checksum = checksum + int64(v >> 8 & 0xff)
	checksum = checksum + int64(v & 0xff)
	return checksum
}

func AddUint32ToChecksum(v uint32, checksum int64) int64 {
	checksum = checksum + int64(v >> 24 & 0xff)
	checksum = checksum + int64(v >> 16 & 0xff)
	checksum = checksum + int64(v >> 8 & 0xff)
	checksum = checksum + int64(v & 0xff)
	return checksum
}

func AddInt32ToChecksum(v int32, checksum int64) int64 {
	checksum = checksum + int64(v >> 24 & 0xff)
	checksum = checksum + int64(v >> 16 & 0xff)
	checksum = checksum + int64(v >> 8 & 0xff)
	checksum = checksum + int64(v & 0xff)
	return checksum
}

func AddInt64ToChecksum(v int64, checksum int64) int64 {
	checksum = checksum + (v >> 56 & 0xff)
	checksum = checksum + (v >> 48 & 0xff)
	checksum = checksum + (v >> 40 & 0xff)
	checksum = checksum + (v >> 32 & 0xff)
	checksum = checksum + (v >> 24 & 0xff)
	checksum = checksum + (v >> 16 & 0xff)
	checksum = checksum + (v >> 8 & 0xff)
	checksum = checksum + (v & 0xff)
	return checksum
}

func AddByteArrayToChecksum(payload []byte, checksum int64) int64 {
	for _, aPayload := range payload {
		checksum = checksum + int64(aPayload & 0xFF)
	}

	return checksum
}

func ListDir(dirPth string, suffix string) (files []string, err error) {
	files = make([]string, 0, 1024)
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return nil, err
	}

	pthSep := string(os.PathSeparator)
	suffix = strings.ToUpper(suffix) //忽略后缀匹配的大小写

	for _, file := range dir {
		if file.IsDir() { // 忽略目录
			continue
		}

		if strings.HasSuffix(strings.ToUpper(file.Name()), suffix) { //匹配文件
			if strings.HasSuffix(dirPth, pthSep) {
				files = append(files, dirPth + file.Name())
			} else {
				files = append(files, dirPth + pthSep + file.Name())
			}
		}
	}

	return files, nil
}

func WalkDir(dirPth, suffix string) (files []string, err error) {
	files = make([]string, 0, 1024)
	suffix = strings.ToUpper(suffix) //忽略后缀匹配的大小写

	err = filepath.Walk(dirPth, func(filename string, fi os.FileInfo, err error) error { //遍历目录
		//if err != nil { //忽略错误
		// return err
		//}

		if fi.IsDir() { // 忽略目录
			return nil
		}

		if strings.HasSuffix(strings.ToUpper(fi.Name()), suffix) {
			files = append(files, filename)
		}

		return nil
	})

	return files, err
}

func GetCurrentPath() (string, error) {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}

	path, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}

	i := strings.LastIndex(path, "/")
	if i < 0 {
		i = strings.LastIndex(path, "\\")
	}

	if i < 0 {
		return "", errors.New(`error: Can't find "/" or "\".`)
	}

	return string(path[0 : i+1]), nil
}



