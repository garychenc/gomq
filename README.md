# 简介

## GOMQ 是一个由 GO 语言编写的开源消息中间件，它具有以下特性 ：

+ 作为消息服务器运行，在服务器上部署消息队列，由生产者客户端通过网络向服务器的队列发送消息或消费者客户端通过网络从服务器的队列接收消息。

+ 消息队列使用文件持久化消息，保证已经发送到服务器端的消息的可靠存储，假如服务器重启或者突然崩溃，不会导致消息丢失。

+ 队列持久化文件采用 Append Only 的形式存储消息，消息发送到队列之后会被添加到持久化文件中，即使消息被消费了也不会从文件中删除，保证新的消费者可从头重新读取队列中的所有消息（待开发功能）。可设置消息在队列中最长的保存时间，到达最长保存时间之后，新写入的消息会被写在文件的开头位置，覆盖掉过期的消息。所以，文件会循环写入，不会无止境的增长。

+ 每个消费者都有一个在当前队列最后消费位置的指针，指向最后一个消费的消息，并且独立存储在消费者元数据文件中。每次消费者从队列中消费一个消息，处理完毕之后，调用 Commit 函数，即可将最后消费指针写入文件，保证无论消费者客户端重启，或者服务器重启，再次从队列中消费消息的时候都从下一个消息的位置开始读取新的消息。

+ 消费者可 Reset 最后消费位置的指针，让其重新指向最后一次 Commit 的消息位置。也即是说可以从队列中接收多个消息，进行处理，假如消息处理失败可以重新接收相同的消息再次进行处理。

+ 创建消费者的时候需要指定消费者的名称，当多个名称**相同**的消费者消费同个队列的时候，它们共享相同的消息指针。因此，接收消息的时候，队列中的消息采用分发的形式分发给多个消费者，每个消费者接收到一个不同的消息。当多个名称**不同**的消费者消费同个队列的时候，由于它们有各自的消息指针，所以，每个消费者都可以接收到队列中的所有消息。

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

# 参与开发

TODO

# 架构简介

TODO
