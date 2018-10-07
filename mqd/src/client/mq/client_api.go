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
	"errors"
	"io"
)

/*
 *   This file defines API of the MQ client. Business system use the API to operate the MQ server, send a message to the server
 *   or consume a message from the server.
 *
 *   The MQ client consists of three interfaces :
 *
 *   1. IMqClient : This is the entry for the MQ client. First, create an instance of the IMqClient interface implementation.
 *
 *      	mqClient := mq.NewNetworkMqClient("127.0.0.1:16800", -1)
 *
 *   The NewNetworkMqClient function has two parameters, the first parameter is the server address that the MQ client needs to connect to,
 *   in the format: IP:Port. The second parameter is the timeout period for the MQ client to communicate with the MQ server.
 *
 *   After creating an instance of IMqClient, you can use IMqClient's method to create producers and consumers.
 *   When creating a producer or consumer, you need to specify the name of the queue to be operated.
 *
 *   So, before creating a producer or consumer, you need to configure queues in the server-side configuration file.
 *   For details, please refer to the instruction in the server.yml configuration file.
 *
 *   -------------------------------------------------------------------------------------------------------------------
 *
 *   2. IProducer : This is the producer interface for the MQ client to send messages to the MQ server.
 *   Create an instance of this interface using the CreateProducer function of IMqClient.
 *
 *      	producer, err := mqClient.CreateProducer("ForProducingQueueName")
 *          if err != nil {
 *          // Process Error
 *          }
 *
 *   The only parameter for the CreateProducer function is the name of the queue to send messages to. After the instance of IProducer was created,
 *   You can use the function of the IProducer interface, for example :
 *
 *   		msg, _ := mq.NewBytesMessageWithoutId([]byte("test-msg-1"))
 *			msgId, err := producer.Produce(msg)
 *			if err != nil {
 *      	// Process Error
 *			}
 *
 *   a. Create a bytes message by the NewBytesMessageWithoutId function
 *   b. Send a message to MQ server and store in the specified queue by Produce function
 *
 *   After using the producer, the producer's Close() function must be called to close it.
 *
 *   -------------------------------------------------------------------------------------------------------------------
 *
 *   3. IConsumer : This is the consumer interface for the MQ client to consume messages from the MQ server.
 *   Create an instance of this interface using the CreateConsumer function of IMqClient.
 *
 *			consumer, err := mqClient.CreateConsumer("ConsumerName", "ForConsumingQueueName")
 *			if err != nil {
 *      	// Process Error
 *			}
 *
 *   Two parameters are required to create a consumer.
 *
 *   First parameter is consumer name. The consumer name is used to identify
 *   a consumer in a queue. For the same queue, each consumer with a different name has an own offset that identifies it's location
 *   of the last message currently consumed in the queue.
 *
 *   If multiple consumer instances consume the same queue using the same consumer name, each consumer instance will consume a different message.
 *   For example, consumers a, b, and c all use the consumer name N1 to consume the same queue Q1. If there are messages 1, 2, 3, 4, 5, 6 in
 *   the queue, each consumer consumes two messages, possibly: a consumption 1,4, b consumption 2,5, c consumption 4,6
 *
 *   If multiple consumer instances consume the same queue using different consumer names, then each message in the queue will send to every consumer instance.
 *   For example, consumers a, b, and c consume the same queue Q1 with different consumer names N1, N2, N3. If there are messages 1, 2, 3, 4, 5, 6 in the queue, each consumer
 *   will consume all the messages in the queue.
 *
 *   Second parameter is the name of the queue to send messages to.
 *
 *   After the instance of IConsumer was created, you can use the function of the IConsumer interface, for example :
 *
 *			msg, err := consumer.Consume()
 *			if err != nil {
 *      	// Process Error
 *			}
 *
 *          // Process the message
 *
 *			err = consumer.Commit()
 *			if err != nil {
 *      	// Process Error
 *			}
 *
 *   a. Consume a message from the specified queue by Consume function
 *   b. After the message is processed successfully, commit the consumer offset to it's latest consumed message location by Commit function
 *
 *   After using the consumer, the consumer's Close() function must be called to close it.
 *
 *   For more details of each interface, please refer to the API description for each interface below.
 *
 */

/*
 * Possible errors with the MQ Client API
 */
var (
	//Error : The queue name parameter is empty
	QueueNameIsEmptyError       = errors.New("queueName can not be empty")
	//Error : The consumer name parameter is empty
	ConsumerNameIsEmptyError    = errors.New("consumerName can not be empty")
	//Error : The message to produce is nil
	MessageIsNilError           = errors.New("message can not be nil")
	//Error : Server internal error, if the client and server are the same version, it should not happen
	UnsupportedRpcResponseError = errors.New("receive unsupported rpc response")
	//Error : The producer is closed, should not call any method after the close operation
	ProducerIsClosedError       = errors.New("producer had been closed")
	//Error : The consumer is closed, should not call any method after the close operation
	ConsumerIsClosedError       = errors.New("consumer had been closed")
)

/*
 * Entry for the MQ client API, create an instance of this interface first, then you can create a producer or a consumer.
 * Instance of this interface is thread safe, can be used in mutil-thread environment.
 *
 * Use the following functions to create a MQ client instance :
 *
 * 1. NewNetworkMqClient
 *    Create a network base MQ client, the first parameter is address of the MQ server. The format of the address string is IP:port.
 *    For example : 192.168.0.1:168000, the second parameter is timeout for requesting MQ server data.
 *
 * 2. NewNetworkMqClientWithLogConfig
 *    Create a network base MQ client, the first parameter is address of the MQ server. The format of the address string is IP:port.
 *    For example : 192.168.0.1:168000, the second parameter is timeout for requesting MQ server data.
 *    The third parameter is used to configure the log behavior of the MQ client.
 *
 */
type IMqClient interface {

	/*
	 * Use this function to create a message producer for sending messages to the MQ server.
	 * Each producer creates and keeps a long connection to the MQ server for communication.
	 * So after using the producer, the producer's Close() function must be called to close it.
	 *
	 * parameters :
	 * queueName : the name of the queue to send messages to. You need to configure queues in the server-side configuration file first.
	 *
	 * return :
	 * producer : the created producer for using
	 * err : if there is any error when creating the producer, the error will save in this return value, otherwise this value is nil
	 *
	 */
	CreateProducer(queueName string) (producer IProducer, err error)

	/*
	 * Use this function to create a message consumer for receiving messages from the MQ server.
	 * Each consumer creates and keeps a long connection to the MQ server for communication.
	 * So after using the consumer, the consumer's Close() function must be called to close it.
	 *
	 * parameters :
	 * consumerName : the name to identify a consumer in a queue.
	 * queueName : the name of the queue to receive messages from. You need to configure queues in the server-side configuration file first.
	 *
	 * return :
	 * consumer : the created consumer for using
	 * err : if there is any error when creating the consumer, the error will save in this return value, otherwise this value is nil
	 *
	 */
	CreateConsumer(consumerName string, queueName string) (consumer IConsumer, err error)
}

/*
 * The consumer interface for the MQ client to consume messages from the MQ server.
 * Instance of this interface is thread safe, can be used in mutil-thread environment.
 *
 */
type IConsumer interface {

	/*
	 * Call the Close function to clear all resources and close the connection with MQ server. No error will be returned.
	 */
	io.Closer

	/*
	 * Use the Consume function to receive a message from the MQ server.
	 * If the queue is empty, the call will be blocked until there is a message in the queue, then returns the first message placed in the queue.
	 * Each time the Consume function is called, the consumer offset will be moved forward by one message.
	 *
	 * return :
	 * msg : the received message
	 * err : if there is any error, the error will save in this return value, otherwise this value is nil
	 *
	 */
	Consume() (msg IMessage, err error)

	/*
	 * Use the ConsumeWithTimeout function to receive a message from the MQ server.
	 * If a message can not be retrieved after the time specified in timeout parameter, this function will return nil.
	 * Each time the ConsumeWithTimeout function is called and return not nil, the consumer offset will be moved forward by one message.
	 *
	 * parameters :
	 * timeout : If this function cannot get a message from the MQ server within the time specified by timeout, then nil is returned.
	 *           unit of timeout is milliseconds.
	 *
	 * return :
	 * msg : the received message or nil if timeout occured
	 * err : if there is any error, the error will save in this return value, otherwise this value is nil
	 *
	 */
	ConsumeWithTimeout(timeout int64) (msg IMessage, err error)

	/*
	 * Use the ConsumeNoWait function to receive a message from the MQ server immediately.
	 * If a message can not be retrieved immediately, this function will return nil.
	 * Each time the ConsumeNoWait function is called and return not nil, the consumer offset will be moved forward by one message.
	 *
	 * return :
	 * msg : the received message or nil if there is no message
	 * err : if there is any error, the error will save in this return value, otherwise this value is nil
	 *
	 */
	ConsumeNoWait() (msg IMessage, err error)

	/*
	 * Use the Commit function to write the consumer's current offset to persistent file.
	 *
	 * return :
	 * err : if there is any error, the error will save in this return value, otherwise this value is nil
	 *
	 */
	Commit() error

	/*
	 * Use the Reset function to set the consumer's current offset to the position of the last Commit
	 *
	 * return :
	 * err : if there is any error, the error will save in this return value, otherwise this value is nil
	 *
	 */
	Reset() error
}

/*
 * The producer interface for the MQ client to send messages to the MQ server.
 * Instance of this interface is thread safe, can be used in mutil-thread environment.
 *
 */
type IProducer interface {

	/*
	 * Call the Close function to clear all resources and close the connection with MQ server. No error will be returned.
	 */
	io.Closer

	/*
	 * Use the Produce function to send a message to the MQ server.
	 * Create a message using the mq.NewBytesMessageWithoutId function before calling the function.
	 *
	 * parameters :
	 * message : The message to be sent.
	 *
	 * return :
	 * msgId : the message ID generated by the server side. Will be -1 when there is any error.
	 * err : if there is any error, the error will save in this return value, otherwise this value is nil
	 *
	 */
	Produce(message IMessage) (msgId int64, err error)
}

