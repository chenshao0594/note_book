Kafka Controller 是 Kafka 的核心组件，在前面的文章中，已经详细讲述过 Controller 部分的内容。在过去的几年根据大家在生产环境中应用的反馈，Controller 也积累了一些比较大的问题，而针对这些问题的修复，代码的改动量都是非常大的，无疑是一次重构，因此，社区准备在新版的系统里对 Controller 做一些相应的优化（0.11.0及以后的版本），相应的设计方案见：[Kafka Controller Redesign](https://docs.google.com/document/d/1rLDmzDOGQQeSiMANP0rC2RYp_L7nUGHzFD9MQISgXYM/edit#heading=h.pxfjarumuhko)，本文的内容就是结合这篇文章做一个简单的总结。   
 ## Controller 功能 
 在一个 Kafka 中，Controller 要处理的事情总结如下表所示：   
| 功能| 详情 | 
| -----| ----- | 
 | cluster metadata updates | producer 或 consumer 可以通过 MetadataRequest 请求从集群任何一台 broker 上查询到某个 Partition 的 metadata 信息，如果一个 Partition 的 leader 或 isr 等信息变化，Controller 会广播到集群的所有 broker 上，这样每台 Broker 都会有该 Partition 的最新 Metadata 信息 | 
 | topic creation | 用户可以通过多种方式创建一个 topic，最终的结果都是在 zk 的 /brokers/topics 目录下新建一个 topic 节点信息，controller 通过监控这个目录来判断是否有新的 topic 需要创建 | 
 | topic deletion | Controller 通过监控 zk 的 /admin/delete_topics 节点来触发 topic 删除操作 | 
 | partition reassignment | Controller 通过监控 zk 的 /admin/reassign_partitions 节点来触发 Partition 的副本迁移操作 | 
 | preferred replica leader election | Controller 通过监控 zk 的 /admin/preferred_replica_election 节点来触发最优 leader 选举操作，该操作的目的选举 Partition 的第一个 replica 作为 leader | 
 | topic partition expansion | Controller 通过监控 zk 的 /brokers/topics/<topic> 数据内容的变化，来触发 Topic 的 Partition 扩容操作 | 
 | broker join | Controller 通过监控 zk 的 /brokers/ids 目录变化，就会知道哪些 broker 是最新加入的，进而触发 broker 的上线操作 | 
 | broker failure | 同样，Controller 通过监控 zk 的 /brokers/ids 目录变化，就会知道哪些 broker 掉线了，进而触发 broker 的下线操作 |  | 
 | controlled shutdown | Controller 通过处理 ControlledShudownRequest 请求来优雅地关闭一个 broker 节点，主动关闭与直接 kill 的区别，它可以减少 Partition 的不可用时间，因为一个 broker 的 zk 临时节点消失是需要一定时间的 | 
 | controller leader election | 集群中所有 broker 会监听 zk 的 /controller 节点，如果该节点消失，所有的 broker 都回去抢占 controller 节点，抢占成功的，就成了最新的 controller | 
 
 ## Controller 目前存在的问题 
 之所以要重新设计 Controller，是因为现在的 Controller 积累了一些比较难解决的问题，这些问题解决起来，代码改动量都是巨大的，甚至需要改变 controller 部门的设计，基本就跟重构差不多了，下面我们先来了看一下 controller 之前（主要是 0.11.0 之前的版本）存在的一些问题。   目前遇到的比较大的问题有以下几个：    
 - Partition 级别同步 zk 写； 
 - sequential per-partition controller-to-broker requests； 
 - Controller 复杂的并发语义； 
 - 代码组织混乱； 
 - 控制类请求与数据类请求未分离； 
 - Controller 给 broker 的请求中没有 broker 的 generation信息； 
 - ZkClient 阻碍 Client 的状态管理。  
 ### Partition 级别同步 zk 写 
 zookeeper 的同步写意味着在下次写之前需要等待前面整个过程的结束，而且由于它们都是 partition 粒度的（一个 Partition 一个 Partition 的去执行写操作），对于 Partition 非常多的集群来说，需要等待的时间会更长，Controller 通常会在下面这两个地方做 Partition 级别 zookeeper 同步写操作：    
 - PartitionStateMachine 在进行触发 leader 选举（partition 目的状态是 OnlinePartition），将会触发上面的操作； 
 - ReplicaStateMachine 更新 LeaderAndIsr 信息到 zk（replica 状态转变为 OfflineReplica），这种情况也触发这种情况，它既阻碍了 Controller 进程，也有可能会 zk 造成压力。  
 ### sequential per-partition controller-to-broker requests 
 Controller 在向 Broker 发送请求，有些情况下也是 Partition 粒度去发送的，效率非常低，比如在 Controller 处理 broker shutdown 请求时，这里是按 Partition 级别处理，每处理一个 Partition 都会执行 Partition、Replica 状态变化以及 Metadata 更新，并且调用 sendRequestsToBrokers() 向 broker 发送请求，这样的话，效率将变得非常低。   
 ### Controller 复杂的并发语义 
 Controller 需要在多个线程之间共享状态信息，这些线程有：    
 - IO threads handling controlled shutdown requests 
 - The ZkClient org.I0Itec.zkclient.ZkEventThread processing zookeeper callbacks sequentially； 
 - The TopicDeletionManager kafka.controller.DeleteTopicsThread； 
 - Per-broker RequestSendThread within ControllerChannelManager.  所有这些线程都需要访问或修改状态信息（ControllerContext），现在它们是通过 ControllerContext 的 controllerLock（排它锁）实现的，Controller 的并发变得虚弱无力。   
 ### 代码组织混乱 
 KafkaController 部分的代码组织（KafkaController、PartitionStateMachine 和 ReplicaStateMachine）不是很清晰，比如，下面的问题就很难回答：    
 - where and when does zookeeper get updated? 
 - where and when does a controller-to-broker request get formed? 
 - what impact does a failing zookeeper update or controller-to-broker request have on the cluster state?  这也导致了这部分很多开发者不敢轻易去改动。   
 ### 控制类请求与数据类请求未分离 
 现在 broker 收到的请求，有来自 client、broker 和 controller 的请求，这些请求都会被放到同一个 requestQueue 中，它们有着同样的优先级，所以来自 client 的请求很可能会影响来自 controller 请求的处理（如果是 leader 变动的请求，ack 设置的不是 all，这种情况有可能会导致数据丢失）。   
 ### Controller 给 broker 的请求中没有 broker 的 generation信息 
 这里的 Broker generation 代表着一个标识，每当它重新加入集群时，这个标识都会变化。如果 Controller 的请求没有这个信息的话，可能会导致一个重启的 Broker 收到之前的请求，让 Broker 进入到一个错误的状态。   比如，Broker 收到之前的 StopReplica 请求，可能会导致副本同步线程退出。   
 ### ZkClient 阻碍 Client 的状态管理 
 这里的状态管理指的是当 Client 发生重连或会话过期时，Client 可以监控这种状态变化，并做出一些处理，因为开源版的 ZKClient 在处理 notification 时，是线性处理的，一些 notification 会被先放到 ZkEventThread’s queue 中，这样会导致一些最新的 notification 不能及时被处理，特别是与 zk 连接断开重连的情况。   
 ## Controller 改进方案 
 关于上述问题，Kafka 提出了一些改进方案，有些已经在最新版的系统中实现，有的还在规划中。   
 ### 使用异步的 zk api 
 Zookeeper 的 client 提供三种执行请求的方式：    
 - 同步调用，意味着下次请求需要等待当前当前请求的完成； 
 - 异步调用，意味着不需要等待当前请求的完成就可以开始下次请求的执行，并且我们可以通过回调机制去处理请求返回的结果； 
 - 单请求的 batch 调用，意味着 batch 内的所有请求都会在一次事务处理中完成，这里需要关注的是 zookeeper 的 server 对单请求的大小是有限制的（jute.maxbuffer）。  文章中给出了三种请求的测试结果，Kafka 最后选取的是异步处理机制，因为对于单请求处理，异步处理更加简洁，并且相比于同步处理还可以保持一个更好的写性能。   
 ### improve controller-to-broker request batching 
 这个在设计文档还是 TODO 状态，具体的方案还没确定，不过基本可以猜测一下，因为目的是提高 batch 发送能力，那么只能是在调用对每个 broker 的 RequestSenderThread 线程发送请求之前，做一下检测，而不是来一个请求立马就发送，这是一个性能与时间的权衡，如果不是立马发送请求，那么可能会带来 broker 短时 metadata 信息的不一致，这个不一致时间不同的应用场景要求是不一样的。   
 ### 单线程的事件处理模型 
 采用单线程的时间处理模型将极大简化 Controller 的并发实现，只允许这个线程访问和修改 Controller 的本地状态信息，因此在 Controller 部分也就不需要到处加锁来保证线程安全了。   目前 1.1.0 的实现中，Controller 使用了一个 ControllerEventThread 线程来处理所有的 event，目前可以支持13种不同类型事件：    
 - Idle：代表当前 ControllerEventThread 处理空闲状态； 
 - ControllerChange：Controller 切换处理； 
 - BrokerChange：Broker 变动处理，broker 可能有上线或掉线； 
 - TopicChange：Topic 新增处理； 
 - TopicDeletion：Topic 删除处理； 
 - PartitionReassignment：Partition 副本迁移处理； 
 - AutoLeaderBalance：自动 rebalance 处理； 
 - ManualLeaderBalance：最优 leader 选举处理，这里叫做手动 rebalance，手动去切流量； 
 - ControlledShutdown：优雅关闭 broker； 
 - IsrChange：Isr 变动处理； 
 - LeaderAndIsrResponseReceived； 
 - LogDirChange：Broker 某个目录失败后的处理（比如磁盘坏掉等）； 
 - ControllerShutdown：ControllerEventThread 处理这个事件时，会关闭当前线程。  
 ### 重构集群状态管理 
 这部分的改动，目前社区也没有一个很好的解决思路，重构这部分的目的是希望 Partition、Replica 的状态管理变得更清晰一些，让我们从代码中可以清楚地明白状态是在什么时间、什么地方、什么条件下被触发的。这个优化其实是跟上面那个有很大关联，采用单线程的事件处理模型，可以让状态管理也变得更清晰。   
 #### prioritize controller requests 
 我们想要把控制类请求与数据类请求分开，提高 controller 请求的优先级，这样的话即使 Broker 中请求有堆积，Broker 也会优先处理控制类的请求。   这部分的优化可以在网络层的 RequestChannel 中做，RequestChannel 可以根据请求的 id 信息把请求分为正常的和优先的，如果请求是 UpdateMetadataRequest、LeaderAndIsrRequest 或者 StopReplicaRequest，那么这个请求的优先级应该提高。实现方案有以下两种：    
 - 在请求队列中增加一个优先级队列，优先级高的请求放到 the prioritized request queue 中，优先级低的放到普通请求队列中，但是无论使用一个定时拉取（poll）还是2个定时拉取，都会带来其他的问题，要么是增大普通请求的处理延迟，要么是增大了优先级高请求的延迟； 
 - 直接使用优先级队列代替现在的普通队列，设计上更倾向与这一种。  目前这部分在1.1.0中还未实现。   
 ### Controller 发送请求中添加 broker 的 generation 信息 
 generation 信息是用来标识当前 broker 加入集群 epoch 信息，每当 broker 重新加入集群中，该 broker.id 对应的 generation 都应该变化（要求递增），目前有两种实现方案：    
 - 为 broker 分配的一个全局唯一的 id，由 controller 广播给其他 broker； 
 - 直接使用 zookeeper 的 zxid 信息（broker.id 注册时的 zxid）。  
 ### 直接使用原生的 Zookeeper client 
 Client 端的状态管理意味着当 Client 端发生状态变化（像连接中断或回话超时）时，我们有能力做一些操作。其中，zookeeper client 有效的状态（目前的 client 比下面又多了几种状态，这里先不深入）是:    
 - NOT_CONNECTED： the initial state of the client； 
 - CONNECTING： the client is establishing a connection to zookeeper； 
 - CONNECTED： the client has established a connection and session to zookeeper； 
 - CLOSED： the session has closed or expired。  有效的状态转移是：    
 - NOT_CONNECTED &gt; CONNECTING 
 - CONNECTING &gt; CONNECTED 
 - CONNECTING &gt; CLOSED 
 - CONNECTED &gt; CONNECTING 
 - CONNECTED &gt; CLOSED  最开始的设想是直接使用原生 Client 的异步调用方式，这样的话依然可以通过回调方法监控到状态的变化（像连接中断或回话超时），同样，在每次事件处理时，可以通过检查状态信息来监控到 Client 状态的变化，及时做一些处理。   当一个 Client 接收到连接中断的 notification（Client 状态变成了 CONNECTING 状态），它意味着 Client 不能再从 zookeeper 接收到任何 notification 了。如果断开连接，对于 Controller 而言，无论它现在正在做什么它都应该先暂停，因为可能集群的 Controller 已经切换到其他机器上了，只是它还没接收到通知，它如果还在工作，可能会导致集群状态不一致。当连接断开后，Client 可以重新建立连接（re-establish，状态变为 CONNECTED）或者会话过期（状态变为 CLOSED，会话过期是由 zookeeper Server 来决定的）。如果变成了 CONNECTED 状态，Controller 应该重新开始这些暂停的操作，而如果状态变成了 CLOSED 状态，旧的 Controller 就会知道它不再是 controller，应该丢弃掉这些任务。   参考：    
 - [Kafka Controller Redesign](https://docs.google.com/document/d/1rLDmzDOGQQeSiMANP0rC2RYp_L7nUGHzFD9MQISgXYM/edit#heading=h.pxfjarumuhko)； 
 - [Kafka controller重设计](https://www.cnblogs.com/huxi2b/p/6980045.html)。