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
	"server/sutils"
)

type IQueueContainer interface {
	sutils.IServer

	DeployQueue(queueConfig IQueueConfig) (queue IQueue, err error)
	GetDeployedQueue(queueName string) IQueue
	CloseQueue(queueName string)

	CreateProducer(producerId int64, queue IQueue) (producer IProducer, err error)
	GetProducer(producerId int64) IProducer
	CloseProducer(producerId int64)

	CreateConsumer(consumerId int64, consumerName string, queue IQueue) (consumer IConsumer, err error)
	GetConsumer(consumerId int64) (consumer IConsumer, forRecoverProducer IProducer)
	CloseConsumer(consumerId int64)

	CreateIterator(queue IQueue) (iterator IIterator, err error)
}

type IQueueConfig interface {
	QueueName() string
	String() string
}
