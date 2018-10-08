# 简介

## GOMQ 是一个由 GO 语言编写的开源消息中间件，它具有以下特性 ：

+ 作为消息服务器运行，在服务器上部署消息队列，由生产者客户端向服务器的队列发送消息或消费者客户端从服务器的队列接收消息。

![MQServer](https://github.com/garychenc/gomq/blob/master/doc/img/MQ-Server.png "MQ服务器示意图")

+ 消息队列使用文件持久化消息，保证已经发送到服务器端的消息的可靠存储，假如服务器重启或者突然崩溃，不会导致消息丢失。

+ 队列持久化文件采用 Append Only 的形式存储消息，消息发送到队列之后会被添加到持久化文件中，即使消息被消费了也不会从文件中删除，保证新的消费者可从头重新读取队列中的所有消息（待开发功能）。可设置消息在队列中最长的保存时间，到达最长保存时间之后，新写入的消息会被写在文件的开头位置，覆盖掉过期的消息。所以，文件会循环写入，不会无止境的增长。

+ 每个消费者都有一个在当前队列最后消费位置的指针，指向最后一个消费的消息，并且独立存储在消费者元数据文件中。每次消费者从队列中消费一个消息，处理完毕之后，调用 Commit 函数，即可将最后消费指针写入文件，保证无论消费者客户端重启，或者服务器重启，再次从队列中消费消息的时候都从下一个消息的位置开始读取新的消息。

+ 消费者可 Reset 最后消费位置的指针，让其重新指向最后一次 Commit 的消息位置。也即是说可以从队列中接收多个消息，进行处理，假如消息处理失败可以重新接收相同的消息再次进行处理。

+ 创建消费者的时候需要指定消费者的名称，当多个名称**相同**的消费者消费同个队列的时候，它们共享相同的消息指针。因此，接收消息的时候，队列中的消息采用分发的形式分发给多个消费者，每个消费者接收到一个不同的消息。当多个名称**不同**的消费者消费同个队列的时候，由于它们有各自的消息指针，所以，每个消费者都可以接收到队列中的所有消息。

![Consumer-Offset](https://github.com/garychenc/gomq/blob/master/doc/img/consumer-offset.png "消费者指针示意图")

+ 生产者、消息队列服务器、消费者可分别处于不同服务器上，它们之间通过 TCP 长连接进行通讯。客户端与服务器端之间由独立的心跳来保持长连接，并且进行对端的健康检查。该长连接具有可靠通讯的功能，保证消息能够可靠的发给对端，消除由于网络闪断等带来的各种不可靠传输问题。

+ 该消息中间件通过可靠的网络传输、保存到文件的消息存储、Receive - Process - Commit的消息消费模式等机制，保证消息从发生到存储到消费各个环节的可靠性。

## GOMQ 目前还有以下特性待开发 ：

+ 目前队列还不支持多个 Partition，有多个生产者或消费者的情况下只能往同个 Partition 发送消息或从同个 Partition 中竞争接收消息。这样会影响性能和消息接收的可靠性。

+ 目前消息服务器还不支持集群，只能运行单个消息服务器。接下来会采用 Master Server - Data Server 的模式设计并开发消息服务器的集群功能。

+ 目前还没有完善的监控和管理系统可以监控服务器的各种运行指标，并且对服务器进行管理。

+ 目前只有 GO 语言开发的生产者和消费者客户端，还不能支持其它语言。

# 快速入门

接下来将会从服务器构建、配置服务器、部署消息队列、启动服务器、创建消息生产者、创建消息消费者等步骤介绍如何快速使用 GOMQ，本入门假设你熟悉 GO 语言开发。

+ 由于 GOMQ 是采用 GO 语言编写，并且没有提供已编译版本，所以，在使用之前需要先 checkout 源代码，并且在目标操作系统中使用 GO 编译器编译源代码。

+ 安装 GO 开发环境，[Windows & Linux 安装说明](https://www.jianshu.com/p/b6f34ae55c90)，[MAC 安装说明](https://www.jianshu.com/p/ae872a26b658)（安装过程可能需要翻墙）。

+ 由于 GOMQ 使用 DEP 进行依赖管理，所以需要安装 DEP，用于下载项目中用到的第三方组件。[DEP 安装说明](https://studygolang.com/articles/10589) 。

+ 使用 GIT Checkout 源代码，代码路径：https://github.com/garychenc/gomq.git 。假设 Checkout 之后源代码的存放根目录（目录下有 README.md 文件的那个目录）的路径为 CODE_ROOT 。

+ 将 CODE_ROOT/mqd 路径添加到 GOPATH 环境变量中。

+ 在命令行控制台中进入 CODE_ROOT/mqd/src/client 目录，运行 dep ensure 命令。稍微等待之后，可看到 CODE_ROOT/mqd/src/client 目录多了一个 vendor 目录，即表示 dep ensure 命令运行成功，拉取到 client 所需的第三方组件（beego 日志组件）。

+ 在命令行控制台中进入 CODE_ROOT/mqd/src/common 目录，运行 dep ensure 命令。稍微等待之后，可看到 CODE_ROOT/mqd/src/common 目录多了一个 vendor 目录，即表示 dep ensure 命令运行成功。

+ 在命令行控制台中进入 CODE_ROOT/mqd/src/server 目录，运行 dep ensure 命令。稍微等待之后，可看到 CODE_ROOT/mqd/src/server 目录多了一个 vendor 目录，即表示 dep ensure 命令运行成功，拉取到 server 所需的第三方组件（beego 日志组件）。

+ 在命令行控制台中进入 CODE_ROOT/mqd 目录，运行 go build -o ./bin/gomqd server/main 命令，编译可执行文件到 bin 目录，文件名为 gomqd 。

+ 将 CODE_ROOT/mqd/src/server/main 中的 config，logs，store 目录拷贝到 CODE_ROOT/mqd/bin 目录。

+ 打开 CODE_ROOT/mqd/bin/config/server.yml 配置文件，将 ListeningAddress 配置项的服务器监听 IP 改为本机 IP，或者不改动亦可。从配置文件中可以看到服务器默认部署了三条测试队列，部署新队列的方法请参考配置文件的注释。

+ 在命令行控制台中进入 CODE_ROOT/mqd/bin 目录，运行 ./gomqd 命令，看到 “-- MQ SERVER STARTED !! --” 日志则 GOMQ 服务器启动正常。

+ 查看 CODE_ROOT/mqd/bin/logs 目录，可以看到服务器运行时日志存放在该目录。包含：mq.log 和 main.log 两个日志文件，mq.log 记录消息服务器运行时的各种日志，main.log 记录 main 函数的启动日志。

+ 查看 CODE_ROOT/mqd/bin/store 目录，可以看到所有队列的消息持久化文件存放在该目录。消费者的元数据文件也会存放在该目录，目前没有消费者，所以没有任何消费者元数据文件。

+ 至此 GOMQ 服务器已成功编译并启动，可将 CODE_ROOT/mqd/bin 目录下的所有文件和子目录移到其它任何不包含中文路径的目录，也可正常启动。接下来将介绍客户端示例，通过这个示例可了解如何使用 GOMQ 的客户端与 GOMQ 服务器进行交互。

+ 暂时将 CODE_ROOT/mqd 路径从 GOPATH 环境变量中移除，然后将 CODE_ROOT/client-example 路径添加到 GOPATH 环境变量中。

+ 在命令行控制台中进入 CODE_ROOT/client-example/src/pe 目录，运行 dep ensure 命令。

+ 在命令行控制台中进入 CODE_ROOT/client-example/src/ce 目录，运行 dep ensure 命令。

+ 在命令行控制台中进入 CODE_ROOT/client-example 目录，运行 go test pe 命令。然后在 CODE_ROOT/client-example/src/pe 目录下可看到 producer.log 文件，记录着生产者示例函数的消息发送日志。

+ 在命令行控制台中进入 CODE_ROOT/client-example 目录，运行 go test ce 命令。然后在 CODE_ROOT/client-example/src/ce 目录下可看到 consumer.log 文件，记录着消费者示例函数的消息接收日志。

+ 可通过查看 CODE_ROOT/client-example/src 目录下的源代码，了解 GOMQ 客户端的使用方法和 GOMQ 客户端各接口的详细 API 说明。

# 联系方式

Gary CHEN : email : gary.chen.c@qq.com

# 系统架构简介

**系统整体架构图如下 ：**

![WholeArchitecture](https://github.com/garychenc/gomq/blob/master/doc/img/whole-architecture.png "系统整体架构图")

系统从整体上分成服务器端和客户端，服务器端从下至上包含以下组件：

+ Server Side Long Connection Network Layer，该组件用于对客户端 TCP 长连接进行管理，接收所有进入服务器的网络请求，结合 Network Transfer Protocol Process 组件对请求进行解码，然后将请求分发到对应的处理器进行处理。该组件同时还用于维持客户端与服务器端之间的长连接心跳。

+ MQ Command & Message Protocol Process，在下层组件对网络传输包进行解码之后，就将解码之后的请求发送到该组件对应的处理器进行请求处理。在处理过程中，需要对请求中包含的命令类型、消息内容等进行解码，然后执行相应的命令，例如：将消息添加到队列的末尾或者从队列中取出消息并且发送到客户端。

+ Queue Container，服务器端创建的队列、生产者、消费者对象的容器，每个客户端创建的生产者或消费者都对应着一个服务器端的生产者或消费者，并且通过远程调用的形式调用到服务器端对象的相应方法。这些服务器端的对象由 Queue Container 管理其生命周期。从队列的部署，生产者、消费者的创建到队列、生产者、消费者的关闭。

+ Producer，服务器端对应的生产者对象，用于执行将消息发送到队列的逻辑。

+ Consumer，服务器端对应的消费者对象，用于执行从队列中取出消息，管理最后消费消息指针等逻辑。

+ Queue，基于文件实现的循环数组列表（Circle Array List）。把一个大文件看成整块的大内存，在此基础上基于文件操作 API 实现：空间分配管理、空间循环使用、空间写入、空间数据读取、列表元素持久化、反持久化等逻辑。

+ Message & Consumer MetaData Storage，该组件属于 Queue 组件和 Consumer 组件的一部分，封装对文件操作的逻辑。

+ MQ Server，服务器端所有对象的容器，管理着整个服务器的启动、停止，以及所需的各种资源的申请和销毁。

客户端包含以下组件：

+ Client Side Long Connection Network Layer、Network Transfer Protocol Process、MQ Command & Message Protocol Process 这些组件与服务器端的 Server Side Long Connection Network Layer、Network Transfer Protocol Process、MQ Command & Message Protocol Process 组件一一对应，完成客户端的长连接管理、网络请求协议编解码、网络请求发送、网络请求处理等逻辑。

+ MQ Client，实现 MQ 客户端业务逻辑的封装。

+ MQ Client API，对外提供操作 MQ 服务器的 API 接口。

**基于文件的队列、消费者名称、消息指针示意图 ：**

![Consumer-Offset](https://github.com/garychenc/gomq/blob/master/doc/img/consumer-offset.png "消费者指针示意图")

+ 每个客户端创建的消费者对象都对应着一个服务器端的消费者对象，根据消费者名称形成对应关系。假如多个客户端的消费者对象采用相同的消费者名称，则这些客户端的消费者对象都对应着同个服务器端的消费者对象，它们共享着相同的消息指针，所以当一个客户端消费者接收一个消息之后，服务器端消费者的消息指针向前，另外一个客户端消费者只能接收下一个消息。而不同名称的客户端消费者对应着不同的服务器端消费者，使用不同的消息指针，所以各自都可以接收队列中所有的消息。

+ 每个服务器端消费者包含两个消息指针，1. 当前已接收消息指针，指向最后一个已成功获取的消息的位置，2. 最后 Commit 消息指针，指向最后一次调用 Commit 时的当前已接收消息指针。Reset 消费者的时候将当前已接收消息指针重新指向最后 Commit 消息指针。

+ 队列只是一个按顺序存储消息的文件，对外的表现形式就像一个循环数组列表（Circle Array List）一样，不删除消息，不更新消息。消费者自己保存当前消费指针，通过指针指向的位置查询队列中的消息内容。

# 客户端 API 说明

CODE_ROOT/mqd/src/client/mq/client_api.go 文件为主要的对外 API 函数的声明文件，文件说明如下：

```

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

```

配合 client_api.go 文件使用时需要用到 message.go 文件中声明的某些接口和函数，用于创建消息，获取消息内容等，需要用到的接口和函数说明如下：

```

/*
 * Create a message object whose message body type is a byte array
 *
 * parameters :
 * payload : a byte array message body
 *
 * return :
 * a new message pointer
 *
 */
func NewBytesMessageWithoutId(payload []byte) *BytesMessage {
	return &BytesMessage{msgId: -1, payload: payload}
}


package mq

import (
	"errors"
)

const MsgIdBytesSize = 8
const MsgPayloadFlagBytesSize = 1
const MsgPayloadLengthBytesSize = 3
const MsgPayloadNotNilFlag      = 0x00
const MsgPayloadNilFlag         = 0x01
const MsgPayloadIsBytesFlag     = 0x02
const MsgPayloadIsObjectFlag    = 0x04

/*
 * Message object, message sent to or received from the MQ server must be an instance of the interface.
 * The implementation of this interface is not thread safe
 *
 * Create a message using the function NewBytesMessageWithoutId, NewBytesMessage and NewEmptyBytesMessage are not recommended
 */
type IMessage interface {

	/*
	 * Get the message ID.
	 * This function can be called only on the message received by the consumer.
	 * When message sent by the producer, the message ID will be returned by Produce function, it is generated by the server side.
	 *
	 * return :
	 * message ID
	 *
	 */
	MsgId() int64

	/*
	 * Get the message body, the message body type is determined by the message type
	 *
	 * return :
	 * message body
	 *
	 */
	Payload() interface{}

	/*
	 * Clone a new message with the same message body with the new message ID
	 *
	 * parameters :
	 * newMsgId : new message ID
	 *
	 * return :
	 * a new message
	 *
	 */
	CloneWithNewMsgId(newMsgId int64) IMessage

	/*
	 * Convert a message to a byte array
	 */
	toBytes() ([]byte, error)
}

```

使用 IMqClient 之前，需要先创建 IMqClient 的实例，创建 IMqClient 实例的函数说明如下：

```

/*
 *
 * Create a network base MQ client.
 *
 * parameters :
 * mqServerAddress : address of the MQ server. The format of the address string is IP:port. For example : 192.168.0.1:168000.
 * sendRequestTimeout : timeout for requesting MQ server data.
 *
 * return :
 * An new MQ client instance.
 *
 */
func NewNetworkMqClient(mqServerAddress string, sendRequestTimeout int64) IMqClient {
	if mqServerAddress = strings.Trim(mqServerAddress, " "); len(mqServerAddress) == 0 {
		panic("mqServerAddress can not be empty")
	}

	idGen, err := NewSequenceGenerator()
	if err != nil {
		panic("Create id generator failed")
	}

	return &NetworkMqClient{serverAddress: mqServerAddress, sendRequestTimeout: sendRequestTimeout, logConfig: nil, idGenerator: idGen}
}

/*
 *
 * Create a network base MQ client.
 *
 * parameters :
 * mqServerAddress : address of the MQ server. The format of the address string is IP:port. For example : 192.168.0.1:168000.
 * sendRequestTimeout : timeout for requesting MQ server data.
 * logConfig : configure the log behavior of the MQ client.
 *
 * return :
 * An new MQ client instance.
 *
 */
func NewNetworkMqClientWithLogConfig(mqServerAddress string, sendRequestTimeout int64, logConfig *LogConfig) IMqClient {
	if mqServerAddress = strings.Trim(mqServerAddress, " "); len(mqServerAddress) == 0 {
		panic("mqServerAddress can not be empty")
	}

	idGen, err := NewSequenceGenerator()
	if err != nil {
		panic("Create id generator failed")
	}

	return &NetworkMqClient{serverAddress: mqServerAddress, sendRequestTimeout: sendRequestTimeout, logConfig: logConfig, idGenerator: idGen}
}

```

# 服务器端配置文件说明

GOMQ 服务器运行时配置文件说明如下，文件所在位置 ： CODE_ROOT/mqd/src/server/main/config/server.yml

```

# Config File For The MQ Server
# This configuration file is in YAML format

# Configuration items for log behavior of the MQ server
# Log level configuration item. Legal values :
# DEBUG : Set the log to debug level
# INFO : Set the log to information level
# WARN : Set the log to warn level
# ERROR : Set the log to error level
LogLevel: INFO

# The log is a rotation log, will generate a log file every day
# This configuration item configurates the max file size for each log file, a new log file will be generated after the log file exceed the size
# This configuration item must be ended with G or M, 1G = 1024M
LogMaxSizePerFile: 1G
# This configuration item configurates max day for the MQ server to keep a log file
LogMaxRetainDay: 30

# The network address that the MQ server listens on
# The format of the address string is IP:port. For example : 192.168.0.1:168000
ListeningAddress: 127.0.0.1:16800

# Configure whether to flush the file cache to disk each time a message is sent to or consume from the MQ server.
# If the configuration is false, the file cache refresh mechanism is left to the operating system, or wait for the MQ server to close to refresh.
FlushFileCacheEveryOperation: false

# Message queues to be deployed on the MQ server ：
# Each queue configuration item consists of two parts:
#   1. the queue name
#   2. the maximum storage time for every queue message
#
#   For example :
#
# - QueueName: TestQueue-1
#   MessageStoreTimeInMs: 259200000
#
# configure a queue named TestQueue-1 and message storage time of 3 days. Unit of message storage time is milliseconds
# The queue name is used in CreateProducer function and CreateConsumer function. Queue name must be unique.
#
Queues:
  - QueueName: TestQueue-1
    MessageStoreTimeInMs: 259200000

  - QueueName: TestQueue-2
    MessageStoreTimeInMs: 259200000

  - QueueName: TestQueue-3
    MessageStoreTimeInMs: 259200000

```
