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
	"common/utils"
	"github.com/astaxie/beego/logs"
	"server/sutils"
	"strconv"
	"strings"
	"sync"
)

type FileQueueContainer struct {
	logger               *logs.BeeLogger
	logConfig            *config.LogConfig
	isSyncFileInEveryOpt bool

	lifecycleLock sync.RWMutex
	serverStatus byte
	queues    utils.IConcurrentMap
	producers utils.IConcurrentMap
	consumers utils.IConcurrentMap
	forRecoverProducers utils.IConcurrentMap
}

type FileQueueConfig struct {
	Name string
	QueueStorePath string
	MessageStoreTimeInMs int64
	StoreFileMinSizeInBytes int64
}

func NewFileQueueConfig() *FileQueueConfig {
	return &FileQueueConfig{}
}

func NewFileQueueContainer(logConfig *config.LogConfig, isSyncFileInEveryOpt bool) *FileQueueContainer {
	fileQueueContainer := &FileQueueContainer{}
	fileQueueContainer.logger = logs.NewLogger(logConfig.AsyncLogChanLength)
	fileQueueContainer.logger.SetLevel(logConfig.Level)
	fileQueueContainer.logger.SetLogger(logConfig.AdapterName, logConfig.AdapterParameters)
	fileQueueContainer.logger.SetLogger(logs.AdapterConsole, "{}")
	if logConfig.IsAsync {
		fileQueueContainer.logger.Async(logConfig.AsyncLogChanLength)
	}

	fileQueueContainer.logConfig = logConfig
	fileQueueContainer.isSyncFileInEveryOpt = isSyncFileInEveryOpt
	fileQueueContainer.queues = utils.NewConcurrentMapWithSize(1024 * 8)
	fileQueueContainer.producers = utils.NewConcurrentMapWithSize(1024 * 8)
	fileQueueContainer.consumers = utils.NewConcurrentMapWithSize(1024 * 8)
	fileQueueContainer.forRecoverProducers = utils.NewConcurrentMapWithSize(1024 * 8)
	return fileQueueContainer
}

func (queueConfig *FileQueueConfig) QueueName() string {
	return queueConfig.Name
}

func (queueConfig *FileQueueConfig) String() string {
	var configDesc strings.Builder
	configDesc.WriteString("[FileQueueConfig : Name = ")
	configDesc.WriteString(queueConfig.Name)
	configDesc.WriteString(", QueueStorePath = ")
	configDesc.WriteString(queueConfig.QueueStorePath)
	configDesc.WriteString(", MessageStoreTimeInMs = ")
	configDesc.WriteString(strconv.Itoa(int(queueConfig.MessageStoreTimeInMs)))
	configDesc.WriteString(", StoreFileMinSizeInBytes = ")
	configDesc.WriteString(strconv.Itoa(int(queueConfig.StoreFileMinSizeInBytes)))
	configDesc.WriteString("]")
	return configDesc.String()
}

func (queueContainer *FileQueueContainer) Start() error {
	queueContainer.lifecycleLock.Lock()
	defer queueContainer.lifecycleLock.Unlock()

	queueContainer.serverStatus = sutils.Started
	return nil
}

func (queueContainer *FileQueueContainer) Stop() error {
	queueContainer.lifecycleLock.Lock()
	defer queueContainer.lifecycleLock.Unlock()

	if queueContainer.serverStatus == sutils.Stopped {
		queueContainer.logger.Info("File queue container already stopped.")
		return nil
	}

	queueContainer.serverStatus = sutils.Stopping
	queueContainer.logger.Info("File queue container is stopping.")

	producers := queueContainer.producers
	if producers != nil {
		producerKeys := producers.Keys()
		for _, producerKey := range producerKeys {
			obj := producers.Get(producerKey)
			if obj != nil {
				if producer, ok := obj.(*FileQueueProducer); ok {
					if err := producer.Close(); err != nil {
						queueContainer.logger.Error("Close producer with key [%+v] failed.", producerKey)
					}
				}
			}
		}

		producers.Clear()
	}

	queueContainer.logger.Info("All file queue producers had been closed.")

	forRecoverProducers := queueContainer.forRecoverProducers
	if forRecoverProducers != nil {
		producerKeys := forRecoverProducers.Keys()
		for _, producerKey := range producerKeys {
			obj := forRecoverProducers.Get(producerKey)
			if obj != nil {
				if producer, ok := obj.(*FileQueueProducer); ok {
					if err := producer.Close(); err != nil {
						queueContainer.logger.Error("Close Recover producer with key [%+v] failed.", producerKey)
					}
				}
			}
		}

		forRecoverProducers.Clear()
	}

	queueContainer.logger.Info("All recover producers had been closed.")

	consumers := queueContainer.consumers
	if consumers != nil {
		consumerKeys := consumers.Keys()
		for _, consumerKey := range consumerKeys {
			obj := consumers.Get(consumerKey)
			if obj != nil {
				if consumer, ok := obj.(*FileQueueConsumer); ok {
					if err := consumer.Close(); err != nil {
						queueContainer.logger.Error("Close consumer with key [%+v] failed.", consumerKey)
					}
				}
			}
		}

		consumers.Clear()
	}

	queueContainer.logger.Info("All file queue consumers had been closed.")

	queues := queueContainer.queues
	if queues != nil {
		queueNames := queues.Keys()
		for _, queueName := range queueNames {
			obj := queues.Get(queueName)
			if obj != nil {
				if fQueue, ok := obj.(*FileQueue); ok {
					if err := fQueue.Close(); err != nil {
						queueContainer.logger.Error("Close queue with name [%+v] failed.", queueName)
					}
				}
			}
		}

		queues.Clear()
	}

	queueContainer.logger.Info("All file queues had been closed.")

	queueContainer.serverStatus = sutils.Stopped
	queueContainer.logger.Info("File queue container is stopped successfully.")

	return nil
}

func (queueContainer *FileQueueContainer) DeployQueue(queueConfig IQueueConfig) (queue IQueue, err error) {
	fileQueueConfig, ok := queueConfig.(*FileQueueConfig)
	if !ok {
		panic("parameter 'queueConfig' must be the type of *FileQueueConfig")
	}

	fileQueue, err := NewFileQueue(queueContainer.logConfig, fileQueueConfig.QueueStorePath, queueContainer.isSyncFileInEveryOpt,
		fileQueueConfig.MessageStoreTimeInMs, fileQueueConfig.StoreFileMinSizeInBytes)
	if err != nil {
		queueContainer.logger.Error("Create file queue error, can not deploy this queue. Queue Config : %+v, Error is : %+v", fileQueueConfig.String(), err)
		return nil, err
	}

	_, notExist := queueContainer.queues.PutIfAbsent(fileQueueConfig.Name, fileQueue)
	if notExist {
		queueContainer.logger.Info("File queue with name [%+v] had been deployed. Queue Config : %+v.", fileQueue.QueueName(), fileQueueConfig.String())
	} else {
		queueContainer.logger.Info("File queue with name [%+v] already exist in container. Queue Config : %+v.", fileQueue.QueueName(), fileQueueConfig.String())
	}

	return fileQueue, nil
}

func (queueContainer *FileQueueContainer) GetDeployedQueue(queueName string) IQueue {
	obj := queueContainer.queues.Get(queueName)
	if obj != nil {
		if fQueue, ok := obj.(*FileQueue); ok {
			return fQueue
		}
	}

	return nil
}

func (queueContainer *FileQueueContainer) CloseQueue(queueName string) {
	obj := queueContainer.queues.Get(queueName)
	if obj != nil {
		if fQueue, ok := obj.(*FileQueue); ok {
			if err := fQueue.Close(); err != nil {
				queueContainer.logger.Error("Close queue with name [%+v] failed.", queueName)
			} else {
				queueContainer.logger.Info("File queue with name [%+v] had been closed.", queueName)
				queueContainer.queues.Remove(queueName)
			}
		}
	}
}

func (queueContainer *FileQueueContainer) CreateProducer(producerId int64, queue IQueue) (producer IProducer, err error) {
	fileQueue, ok := queue.(*FileQueue)
	if !ok {
		panic("parameter 'queue' must be the type of *FileQueue")
	}

	producer, err = NewFileQueueProducer(fileQueue)
	if err != nil {
		queueContainer.logger.Error("Create file queue producer error. Queue Name : %+v, Error is : %+v", fileQueue.QueueName(), err)
		return nil, err
	}

	oldProducerObj, ok := queueContainer.producers.PutIfAbsent(producerId, producer)
	if ok {
		queueContainer.logger.Info("File queue producer with ID [%+v] had been created and added to container.", producerId)
		return producer, nil
	} else {
		producer.Close()
		producer, _ = oldProducerObj.(*FileQueueProducer)
		queueContainer.logger.Info("File queue producer with ID [%+v] already exist in container.", producerId)
		return producer, nil
	}
}

func (queueContainer *FileQueueContainer) GetProducer(producerId int64) IProducer {
	obj := queueContainer.producers.Get(producerId)
	if obj != nil {
		if producer, ok := obj.(*FileQueueProducer); ok {
			return producer
		}
	}

	return nil
}

func (queueContainer *FileQueueContainer) CloseProducer(producerId int64) {
	obj := queueContainer.producers.Get(producerId)
	if obj != nil {
		if producer, ok := obj.(*FileQueueProducer); ok {
			if err := producer.Close(); err != nil {
				queueContainer.logger.Error("Close producer with key [%+v] failed.", producerId)
			} else {
				queueContainer.logger.Error("File queue producer with key [%+v] had been closed.", producerId)
				queueContainer.producers.Remove(producerId)
			}
		}
	}
}

func (queueContainer *FileQueueContainer) CreateConsumer(consumerId int64, consumerName string, queue IQueue) (consumer IConsumer, err error) {
	fileQueue, ok := queue.(*FileQueue)
	if !ok {
		panic("parameter 'queue' must be the type of *FileQueue")
	}

	recoverProducer, err := NewFileQueueProducer(fileQueue)
	if err != nil {
		queueContainer.logger.Error("Create recover producer error. Queue Name : %+v, Error is : %+v", fileQueue.QueueName(), err)
		return nil, err
	}

	_, ok = queueContainer.forRecoverProducers.PutIfAbsent(consumerId, recoverProducer)
	if ok {
		queueContainer.logger.Info("Recover producer with ID [%+v] had been created and added to container.", consumerId)
	} else {
		recoverProducer.Close()
		queueContainer.logger.Info("Recover producer with ID [%+v] already exist in container.", consumerId)
	}

	consumer, err = NewFileQueueConsumer(queueContainer.logConfig, fileQueue, consumerName, queueContainer.isSyncFileInEveryOpt)
	if err != nil {
		queueContainer.logger.Error("Create file queue consumer error. Queue Name : %+v, Error is : %+v, Consumer Name : %+v.", fileQueue.QueueName(), err, consumerName)
		return nil, err
	}

	_, notExist := queueContainer.consumers.PutIfAbsent(consumerId, consumer)
	if notExist {
		queueContainer.logger.Info("File queue consumer with ID [%+v] and Name [%+v] had been created and added to container.", consumerId, consumerName)
	} else {
		queueContainer.logger.Info("File queue consumer with ID [%+v] and Name [%+v] already exist in container.", consumerId, consumerName)
	}

	return consumer, nil
}

func (queueContainer *FileQueueContainer) GetConsumer(consumerId int64) (consumer IConsumer, forRecoverProducer IProducer) {
	obj := queueContainer.consumers.Get(consumerId)
	if obj != nil {
		if consumer, ok := obj.(*FileQueueConsumer); ok {
			var recoverProducer IProducer = nil
			obj := queueContainer.forRecoverProducers.Get(consumerId)
			if obj != nil {
				if producer, ok := obj.(*FileQueueProducer); ok {
					recoverProducer = producer
				}
			}

			return consumer, recoverProducer
		}
	}

	return nil, nil
}

func (queueContainer *FileQueueContainer) CloseConsumer(consumerId int64) {
	obj := queueContainer.consumers.Get(consumerId)
	if obj != nil {
		if _, ok := obj.(*FileQueueConsumer); ok {
			// TODO : Find a good way to close the server side consumer, if there is no client consumer connects to it (with the same consumer name)
			//if err := consumer.Close(); err != nil {
			//	queueContainer.logger.Error("Close consumer with key [%+v] failed.", consumerId)
			//} else {
			//	queueContainer.logger.Error("File queue consumer with key [%+v] had been closed.", consumerId)
			//	queueContainer.consumers.Remove(consumerId)
			//}

			queueContainer.logger.Error("File queue consumer with key [%+v] had been closed.", consumerId)
			queueContainer.consumers.Remove(consumerId)
		}
	}

	obj = queueContainer.forRecoverProducers.Get(consumerId)
	if obj != nil {
		if producer, ok := obj.(*FileQueueProducer); ok {
			if err := producer.Close(); err != nil {
				queueContainer.logger.Error("Close recover producer with key [%+v] failed.", consumerId)
			} else {
				queueContainer.logger.Error("Recover producer with key [%+v] had been closed.", consumerId)
				queueContainer.forRecoverProducers.Remove(consumerId)
			}
		}
	}
}

func (queueContainer *FileQueueContainer) CreateIterator(queue IQueue) (iterator IIterator, err error) {
	fileQueue, ok := queue.(*FileQueue)
	if !ok {
		panic("parameter 'queue' must be the type of *FileQueue")
	}

	iterator, err = NewFileQueueIterator(fileQueue)
	if err != nil {
		queueContainer.logger.Error("Create file queue iterator error. Queue Name : %+v, Error is : %+v.", fileQueue.QueueName(), err)
		return nil, err
	}

	queueContainer.logger.Info("File queue iterator had been created.")
	return iterator, nil
}






















