### Why are duplicates introduced

- Broker can fail
- Producer-to-Broker RPC can fail
- Producer or Consumer client can fail.

### kafka事务

#### Producer角度

- 跨会话的幂等性写入：即使中间故障，恢复后依然可以保持幂等性
- 跨会话的事务恢复: 如果一个应用实例挂掉，启动的下一个实例依然可以保证上个事务完成(commit或者abort)
- 跨多个topic-partition的幂等写入(要么成功，要么失败)

#### Consumer角度

- compacted topic，在一个事务中写入的数据可能被新的值覆盖；
- 一个事务内的数据，可能会跨多个log segment，如果旧的segment数据由于过期被删除，那么这个事务的部分数据就无法被消费。
- Consumer在消费时候可以通过seek机制，随机从一个位置开始消费，也会导致一个事务内的部分数据无法被消费。
- Consumer可能没有订阅这个事务涉及的全部partition。

#### 总结

- Atomic writes across multiple partitions.
- All messages in a transaction are made visible together, or none.
- Consumers must be configured to skip uncommitted messages.

### Key Pionts

#### Atomic

- 涉及到多个 Topic-Partition 数据的写入，

- 如果是一个 long transaction 操作，可能会涉及到非常多的数据

  如何才能保证这个事务操作的原子性（要么全部完成，要么全部失败）呢？

1. 关于这点，最容易想到的应该是引用 **2PC 协议**（它主要是解决分布式系统数据一致性的问题）中协调者的角色，它的作用是统计所有参与者的投票结果，如果大家一致认为可以 commit，那么就执行 commit，否则执行 abort：
   - 我们来想一下，Kafka 是不是也可以引入一个类似的角色来管理事务的状态，只有当 Producer 真正 commit 时，事务才会提交，否则事务会还在进行中（实际的实现中还需要考虑 timeout 的情况），不会处于完成状态；
   - Producer 在开始一个事务时，告诉【协调者】事务开始，然后开始向多个 Topic-Partition 写数据，只有这批数据全部写完（中间没有出现异常），Producer 会调用 commit 接口进行 commit，然后事务真正提交，否则如果中间出现异常，那么事务将会被 abort（Producer 通过 abort 接口告诉【协调者】执行 abort 操作）；
   - 这里的协调者与 2PC 中的协调者略有不同，主要为了管理事务相关的状态信息，这就是 Kafka Server 端的 **TransactionCoordinator** 角色；
2. TransactionCoordinator 如何实现高可用？
   - TransactionCoordinator 需要管理事务的状态信息，如果一个事务的 TransactionCoordinator 挂的话，需要转移到其他的机器上，这里关键是在 **事务状态信息如何恢复？** 也就是事务的状态信息需要**很强的容错性、一致性**；
   - 关于数据的强容错性、一致性，存储的容错性方案基本就是多副本机制，而对于一致性，就有很多的机制实现，其实这个在 Kafka 内部已经实现（不考虑数据重复问题），那就是 `min.isr + ack` 机制；
   - 分析到这里，对于 Kafka 熟悉的同学应该就知道，这个是不是跟 `__consumer_offset` 这个内部的 topic 很像，TransactionCoordinator 也跟 GroupCoordinator 类似，而对应事务数据（transaction log）就是 `__transaction_state` 这个内部 topic，所有事务状态信息都会持久化到这个 topic，TransactionCoordinator 在做故障恢复也是从这个 topic 中恢复数据；
3. 有了上面的机制，就够了么？我们再来考虑一种情况，我们期望一个 Producer 在 Fail 恢复后能主动 abort 上次未完成的事务（接上之前未完成的事务），然后重新开始一个事务，这种情况应该怎么办？之前幂等性引入的 PID 是无法解决这个问题的，因为每次 Producer 在重启时，PID 都会更新为一个新值：
   - Kafka 在 Producer 端引入了一个 **TransactionalId** 来解决这个问题，这个 txn.id 是由应用来配置的；
   - TransactionalId 的引入还有一个好处，就是跟 consumer group 类似，它可以用来标识一个事务操作，便于这个事务的所有操作都能在一个地方（同一个 TransactionCoordinator）进行处理；
4. 再来考虑一个问题，在具体的实现时，我们应该如何标识一个事务操作的开始、进行、完成的状态？正常来说，一个事务操作是由很多操作组成的一个操作单元，对于 TransactionCoordinator 而言，是需要准确知道当前的事务操作处于哪个阶段，这样在容错恢复时，新选举的 TransactionCoordinator 才能恢复之前的状态：
   - 这个就是**事务状态转移**，一个事务从开始，都会有一个相应的状态标识，直到事务完成，有了事务的状态转移关系之后，TransactionCoordinator 对于事务的管理就会简单很多，TransactionCoordinator 会将当前事务的状态信息都会缓存起来，每当事务需要进行转移，就更新缓存中事务的状态（前提是这个状态转移是有效的）。

#### TransactionCoordinator

- hash(txn.id) % number(_transaction_state) 找到对应_ transaction_state的partition leader.
- 处理事务相关的请求
- 维护事务的状态信息
- 向其他Broker发送Transaction Marker数据。

#### Transaction Log(__transaction_state)

一个事务应该由哪个 TransactionCoordinator 来处理，是根据其 txn.id 的 hash 值与 `__transaction_state` 的 partition 数取模得到，`__transaction_state` Partition 默认是50个，假设取模之后的结果是2，那么这个 txn.id 应该由 `__transaction_state` Partition 2 的 leader 来处理。

#### Transaction Marker

也称为Control message.作用告诉这个事务操作涉及的所有Topic-Partition的leaders当前事务已经完成，可以执行Commit或者Abort。

marker数据是由该事务的TransactionCoordinator发送。

- 没有Transaction Marker的情况下， 一个事务完成后，如何执行commit操作。
  - Producer进行commit时候，第一步：先通知TransactionCoordinator这个事务可以commit(由于TransactionCoordinate记录该事务状态信息)。第二步:通知涉及到的Topic-Partition的leader进行commit。
  - 涉及到的Topic-Partition收到commit之后，如何操作？
    - 才把数据持久化到磁盘? 如果是个long transaction操作，写数据非常多(内存中无法存放)，数据需要持久化到磁盘中。如果持久化到磁盘，这个时候收到一个abort请求，需要从磁盘中清除数据？这个方案有以下问题：
      - 持久化到本身的日志文件，还是额外的其他文件？
        - 本身日志文件---consumer会消费到一个未commit的数据，而这些数据是有可能被abort。
        - 额外的其他文件--- 涉及到数据多次写磁盘，会影响到server端的性能。
- 采用Transaction Marker机制的情况
  - step-1 : Producer通知TransactionCoordinate当前事务可以commit。
  - step-2 : TransactionCoordinate向涉及到的Topic-Partition的Leaders发送Transaction Marker数据。好处：1-减轻Client的压力；2-如果目标Broker涉及到多个事务操作，可以共享同一个TCP连接。
  - step-3: 涉及到的Topic-Partition的Leader：第一：Producer写数据的时候，与普通消息一样， 按照条件持久化到磁盘(数据有一个额外的标识位:标识这条或者这批数据是否是事务写入，通过消息协议中的attribute实现)。第二：当收到Transaction Marker时候，把这个Transaction Marker数据也直接写入Partition中，处理consumer消费时，可以根据marker信息做相应处理。

 ### 事务状态

| 状态              | 说明                                                         |
| :---------------- | ------------------------------------------------------------ |
| Empty             | Transaction has not existed yet                              |
| Ongoing           | Transaction has started and ongoing                          |
| PrepareCommit     | Group is preparing to commit                                 |
| PrepareAbort      | Group is preparing to abort                                  |
| CompleteCommit    | Group has completed commit                                   |
| CompleteAbort     | Group has completed abort                                    |
| Dead              | TransactionalId has expired and is about to be removed from the transaction cache |
| PrepareEpochFence | We are in the middle of bumping the epoch and fencing out older producers |

### 事务性的整体流程

https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging#KIP-98-ExactlyOnceDeliveryandTransactionalMessaging-DataFlow

![](C:\Users\user\Desktop\txn-data-flow.png)



1. Finding a transaction coordinator

   找到这个事务 txn.id 对应的 TransactionCoordinator，Transaction Producer 会向 Broker （随机选择一台 broker，一般选择本地连接最少的这台 broker）发送 FindCoordinatorRequest 请求，获取其 TransactionCoordinator。

2. Getting a producer Id

   

3. Starting a Transaction

4. The consume-transform-produce loop

   1. AddPartitionsToTxnRequest

   2. ProduceRequest

      这一步与正常 Producer 写入基本上一样，就是相应的 Leader 在持久化数据时会在头信息中标识这条数据是不是来自事务 Producer 的写入（主要是数据协议有变动，Server 处理并不需要做额外的处理）。

   3. AddOffsetCommitsToTxnRequest

5. Committing or Aborting a Transaction

   1. EndTxnRequest

      TransactionCoordinator 在收到 EndTxnRequest 请求后，会做以下处理：

      1. 更新事务的 meta 信息，状态转移成 PREPARE_COMMIT 或 PREPARE_ABORT，并将事务状态信息持久化到事务日志中；
      2. 根据事务 meta 信息，向其涉及到的所有 Topic-Partition 的 leader 发送 Transaction Marker 信息（也就是 WriteTxnMarkerRquest 请求，见下面的 2 分析）；
      3. 最后将事务状态更新为 COMMIT 或者 ABORT，并将事务的 meta 持久化到事务日志中，也就是 5.3 步骤。

   2. WriteTxnMarkerRequest

      WriteTxnMarkerRquest 是 TransactionCoordinator 收到 Producer 的 EndTxnRequest 请求后向其他 Broker 发送的请求，主要是告诉它们事务已经完成。不论是普通的 Topic-Partition 还是 `__consumer_offsets`，在收到这个请求后，都会把事务结果（Transaction Marker 的格数据式见前面）持久化到对应的日志文件中，这样下游 Consumer 在消费这个数据时，就知道这个事务是 commit 还是 abort。

   3. Writing the Final Commit or Abort Message

      当这个事务涉及到所有 Topic-Partition 都已经把这个 marker 信息持久化到日志文件之后，TransactionCoordinator 会将这个事务的状态置为 COMMIT 或 ABORT，并持久化到事务日志文件中，到这里，这个事务操作就算真正完成了，TransactionCoordinator 缓存的很多关于这个事务的数据可以被清除了。

### Questions

#### 如果多个 Producer 使用同一个 txn.id 会出现什么情况？

对于这个情况，我们这里直接做了一个相应的实验，两个 Producer 示例都使用了同一个 txn.id（为 test-transactional-matt），Producer 1 先启动，然后过一会再启动 Producer 2，这时候会发现一个现象，那就是 Producer 1 进程会抛出异常退出进程，其异常信息为：

```
org.apache.kafka.common.KafkaException: Cannot execute transactional method because we are in an error state	at org.apache.kafka.clients.producer.internals.TransactionManager.maybeFailWithError(TransactionManager.java:784)	at org.apache.kafka.clients.producer.internals.TransactionManager.beginTransaction(TransactionManager.java:215)	at org.apache.kafka.clients.producer.KafkaProducer.beginTransaction(KafkaProducer.java:606)	at com.matt.test.kafka.producer.ProducerTransactionExample.main(ProducerTransactionExample.java:68)Caused by: org.apache.kafka.common.errors.ProducerFencedException: Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.
```

这里抛出了 ProducerFencedException 异常，如果打开相应的 Debug 日志，在 Producer 1 的日志文件会看到下面的日志信息

```
[2018-11-03 12:48:52,495] DEBUG [Producer clientId=ProducerTransactionExample, transactionalId=test-transactional-matt] Transition from state COMMITTING_TRANSACTION to error state FATAL_ERROR (org.apache.kafka.clients.producer.internals.TransactionManager)org.apache.kafka.common.errors.ProducerFencedException: Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.[2018-11-03 12:48:52,498] ERROR [Producer clientId=ProducerTransactionExample, transactionalId=test-transactional-matt] Aborting producer batches due to fatal error (org.apache.kafka.clients.producer.internals.Sender)org.apache.kafka.common.errors.ProducerFencedException: Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.[2018-11-03 12:48:52,599] INFO [Producer clientId=ProducerTransactionExample, transactionalId=test-transactional-matt] Closing the Kafka producer with timeoutMillis = 9223372036854775807 ms. (org.apache.kafka.clients.producer.KafkaProducer)[2018-11-03 12:48:52,599] DEBUG [Producer clientId=ProducerTransactionExample, transactionalId=test-transactional-matt] Beginning shutdown of Kafka producer I/O thread, sending remaining records. (org.apache.kafka.clients.producer.internals.Sender)[2018-11-03 12:48:52,601] DEBUG Removed sensor with name connections-closed: (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,601] DEBUG Removed sensor with name connections-created: (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,602] DEBUG Removed sensor with name successful-authentication: (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,602] DEBUG Removed sensor with name failed-authentication: (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,602] DEBUG Removed sensor with name bytes-sent-received: (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,603] DEBUG Removed sensor with name bytes-sent: (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,603] DEBUG Removed sensor with name bytes-received: (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,604] DEBUG Removed sensor with name select-time: (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,604] DEBUG Removed sensor with name io-time: (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,604] DEBUG Removed sensor with name node--1.bytes-sent (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,605] DEBUG Removed sensor with name node--1.bytes-received (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,605] DEBUG Removed sensor with name node--1.latency (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,605] DEBUG Removed sensor with name node-33.bytes-sent (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,606] DEBUG Removed sensor with name node-33.bytes-received (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,606] DEBUG Removed sensor with name node-33.latency (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,606] DEBUG Removed sensor with name node-35.bytes-sent (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,606] DEBUG Removed sensor with name node-35.bytes-received (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,606] DEBUG Removed sensor with name node-35.latency (org.apache.kafka.common.metrics.Metrics)[2018-11-03 12:48:52,607] DEBUG [Producer clientId=ProducerTransactionExample, transactionalId=test-transactional-matt] Shutdown of Kafka producer I/O thread has completed. (org.apache.kafka.clients.producer.internals.Sender)[2018-11-03 12:48:52,607] DEBUG [Producer clientId=ProducerTransactionExample, transactionalId=test-transactional-matt] Kafka producer has been closed (org.apache.kafka.clients.producer.KafkaProducer)[2018-11-03 12:48:52,808] ERROR Forcing producer close! (com.matt.test.kafka.producer.ProducerTransactionExample)[2018-11-03 12:48:52,808] INFO [Producer clientId=ProducerTransactionExample, transactionalId=test-transactional-matt] Closing the Kafka producer with timeoutMillis = 9223372036854775807 ms. (org.apache.kafka.clients.producer.KafkaProducer)[2018-11-03 12:48:52,808] DEBUG [Producer clientId=ProducerTransactionExample, transactionalId=test-transactional-matt] Kafka producer has been closed (org.apache.kafka.clients.producer.KafkaProducer)
```

Producer 1 本地事务状态从 COMMITTING_TRANSACTION 变成了 FATAL_ERROR 状态，导致 Producer 进程直接退出了，出现这个异常的原因，就是抛出的 ProducerFencedException 异常，简单来说 Producer 1 被 Fencing 了（这是 Producer Fencing 的情况）。因此，这个问题的答案就很清除了，如果多个 Producer 共用一个 txn.id，那么最后启动的 Producer 会成功运行，会它之前启动的 Producer 都 Fencing 掉（至于为什么会 Fencing 下一小节会做分析）。

#### Fencing

关于 Fencing 这个机制，在分布式系统还是很常见的，我第一个见到这个机制是在 HDFS 中，可以参考我之前总结的一篇文章 [HDFS NN 脑裂问题](http://matt33.com/2018/07/15/hdfs-architecture-learn/#HDFS-脑裂问题)，Fencing 机制解决的主要也是这种类型的问题 —— 脑裂问题，简单来说就是，本来系统这个组件在某个时刻应该只有一个处于 active 状态的，但是在实际生产环境中，特别是切换期间，可能会同时出现两个组件处于 active 状态，这就是脑裂问题，在 Kafka 的事务场景下，用到 Fencing 机制有两个地方：

1. TransactionCoordinator Fencing；
2. Producer Fencing；

#### TransactionCoordinator Fencing

TransactionCoordinator 在遇到上 long FGC 时，可能会导致 脑裂 问题，FGC 时会 stop-the-world，这时候可能会与 zk 连接超时导致临时节点消失进而触发 leader 选举，如果 `__transaction_state` 发生了 leader 选举，TransactionCoordinator 就会切换，如果此时旧的 TransactionCoordinator FGC 完成，在还没来得及同步到最细 meta 之前，会有一个短暂的时刻，对于一个 txn.id 而言就是这个时刻可能出现了两个 TransactionCoordinator。

相应的解决方案就是 TransactionCoordinator Fencing，这里 Fencing 策略不像离线场景 HDFS 这种直接 Kill 旧的 NN 进程或者强制切换状态这么暴力，而是通过 CoordinatorEpoch 来判断，每个 TransactionCoordinator 都有其 CoordinatorEpoch 值，这个值就是对应 `__transaction_state` Partition 的 Epoch 值（每当 leader 切换一次，该值就会自增1）。

明白了 TransactionCoordinator 脑裂问题发生情况及解决方案之后，来分析下，Fencing 机制会在哪里发挥作用？仔细想想，是可以推断出来的，只可能是 TransactionCoordinator 向别人发请求时影响才会比较严重（特别是乱发 admin 命令）。有了 CoordinatorEpoch 之后，其他 Server 在收到请求时做相应的判断，如果发现 CoordinatorEpoch 值比缓存的最新的值小，那么 Fencing 就生效，拒绝这个请求，也就是 TransactionCoordinator 发送 WriteTxnMarkerRequest 时可能会触发这一机制。

#### Producer Fencing

Producer Fencing 与前面的类似，如果对于相同 PID 和 txn.id 的 Producer，Server 端会记录最新的 Epoch 值，拒绝来自 zombie Producer （Epoch 值小的 Producer）的请求。前面第一个问题的情况，Producer 2 在启动时，会向 TransactionCoordinator 发送 InitPIDRequest 请求，此时 TransactionCoordinator 已经有了这个 txn.id 对应的 meta，会返回之前分配的 PID，并把 Epoch 自增 1 返回，这样 Producer 2 就被认为是最新的 Producer，而 Producer 1 就会被认为是 zombie Producer，因此，TransactionCoordinator 在处理 Producer 1 的事务请求时，会返回相应的异常信息。

#### Consumer 端如何消费事务数据

在讲述这个问题之前，需要先介绍一下事务场景下，Consumer 的消费策略，Consumer 有一个 `isolation.level` 配置，这个是配置对于事务性数据的消费策略，有以下两种可选配置：

1. `read_committed`: only consume non-­transactional messages or transactional messages that are already committed, in offset ordering.
2. `read_uncommitted`: consume all available messages in offset ordering. This is the **default value**.

简单来说就是，read_committed 只会读取 commit 的数据，而 abort 的数据不会向 consumer 显现，对于 read_uncommitted 这种模式，consumer 可以读取到所有数据（control msg 会过滤掉），这种模式与普通的消费机制基本没有区别，就是做了一个 check，过滤掉 control msg（也就是 marker 数据），这部分的难点在于 read_committed 机制的实现。

#### Last Stable Offset（LSO）

在事务机制的实现中，Kafka 又设置了一个新的 offset 概念，那就是 Last Stable Offset，简称 LSO（其他的 Offset 概念可参考 [Kafka Offset 那些事](http://matt33.com/2017/01/16/kafka-group/#offset-那些事)），先看下 LSO 的定义：

> The LSO is defined as the latest offset such that the status of all transactional messages at lower offsets have been determined (i.e. committed or aborted).

对于一个 Partition 而言，offset 小于 LSO 的数据，全都是已经确定的数据，这个主要是对于事务操作而言，在这个 offset 之前的事务操作都是已经完成的事务（已经 commit 或 abort），如果这个 Partition 没有涉及到事务数据，那么 LSO 就是其 HW（水位）。

#### Server 处理 read_committed 类型的 Fetch 请求

如果 Consumer 的消费策略设置的是 read_committed，其在向 Server 发送 Fetch 请求时，Server 端**只会返回 LSO 之前的数据**，在 LSO 之后的数据不会返回。

这种机制有没有什么问题呢？我现在能想到的就是如果有一个 long transaction，比如其 first offset 是 1000，另外有几个已经完成的小事务操作，比如：txn1（offset：1100~1200）、txn2（offset：1400~1500），假设此时的 LSO 是 1000，也就是说这个 long transaction 还没有完成，那么已经完成的 txn1、txn2 也会对 consumer 不可见（假设都是 commit 操作），此时**受 long transaction 的影响可能会导致数据有延迟**。

那么我们再来想一下，如果不设计 LSO，又会有什么问题呢？可能分两种情况：

1. 允许读未完成的事务：那么 Consumer 可以直接读取到 Partition 的 HW 位置，对于未完成的事务，因为设置的是 read_committed 机制，所以不能对用户可见，需要在 Consumer 端做缓存，这个缓存应该设置多大？（不限制肯定会出现 OOM 的情况，当然也可以现在 client 端持久化到硬盘，这样的设计太过于复杂，还需要考虑 client 端 IO、磁盘故障等风险），明显这种设计方案是不可行的；
2. 如果不允许读未完成的事务：相当于还是在 Server 端处理，与前面的区别是，这里需要先把示例中的 txn1、txn2 的数据发送给 Consumer，这样的设计会带来什么问题呢？
   1. 假设这个 long transaction commit 了，其 end offset 是 2000，这时候有两种方案：第一种是把 1000-2000 的数据全部读出来（可能是磁盘读），把这个 long transaction 的数据过滤出来返回给 Consumer；第二种是随机读，只读这个 long transaction 的数据，无论哪种都有多触发一次磁盘读的风险，可能影响影响 Server 端的性能；
   2. Server 端需要维护每个 consumer group 有哪些事务读了、哪些事务没读的 meta 信息，因为 consumer 是随机可能挂掉，需要接上次消费的，这样实现就复杂很多了；
   3. 还有一个问题是，消费的顺序性无法保证，两次消费其读取到的数据顺序可能是不同的（两次消费启动时间不一样）；

从这些分析来看，个人认为 LSO 机制还是一种相当来说 实现起来比较简单、而且不影响原来 server 端性能、还能保证顺序性的一种设计方案，它不一定是最好的，但也不会差太多。在实际的生产场景中，尽量避免 long transaction 这种操作，而且 long transaction可能也会容易触发事务超时。

#### Consumer 如何过滤 abort 的事务数据

Consumer 在拉取到相应的数据之后，后面该怎么处理呢？它拉取到的这批数据并不能保证都是完整的事务数据，很有可能是拉取到一个事务的部分数据（marker 数据还没有拉取到），这时候应该怎么办？难道 Consumer 先把这部分数据缓存下来，等后面的 marker 数据到来时再确认数据应该不应该丢弃？（还是又OOM 的风险）有没有更好的实现方案？

Kafka 的设计总是不会让我们失望，这部分做的优化也是非常高明，Broker 会追踪每个 Partition 涉及到的 abort transactions，**Partition 的每个 log segment 都会有一个单独只写的文件（append-only file）来存储 abort transaction 信息**，因为 abort transaction 并不是很多，所以这个开销是可以可以接受的，之所以要持久化到磁盘，主要是为了故障后快速恢复，要不然 Broker 需要把这个 Partition 的所有数据都读一遍，才能直到哪些事务是 abort 的，这样的话，开销太大（如果这个 Partition 没有事务操作，就不会生成这个文件）。这个持久化的文件是以 `.txnindex` 做后缀，前面依然是这个 log segment 的 offset 信息，存储的数据格式如下：

```
TransactionEntry =>    Version => int16    PID => int64    FirstOffset => int64    LastOffset => int64    LastStableOffset => int64
```

有了这个设计，Consumer 在拉取数据时，Broker 会把这批数据涉及到的所有 abort transaction 信息都返回给 Consumer，Server 端会根据拉取的 offset 范围与 abort transaction 的 offset 做对比，返回涉及到的 abort transaction 集合，其实现如下：

```scala
def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): 
TxnIndexSearchResult = {  
    val abortedTransactions = ListBuffer.empty[AbortedTxn]  
    for ((abortedTxn, _) <- iterator()) {    
        if (abortedTxn.lastOffset >= fetchOffset && abortedTxn.firstOffset < upperBoundOffset)      
        abortedTransactions += abortedTxn //note: 这个 abort 的事务有在在这个范围内，就返回    if (abortedTxn.lastStableOffset >= upperBoundOffset)      
        return TxnIndexSearchResult(abortedTransactions.toList, isComplete = true)  
    }  
    TxnIndexSearchResult(abortedTransactions.toList, isComplete = false)
}
```

Consumer 在拿到这些数据之后，会进行相应的过滤，大概的判断逻辑如下（Server 端返回的 abort transaction 列表就保存在 `abortedTransactions` 集合中，`abortedProducerIds` 最开始时是为空的）：

1. 如果这个数据是 control msg（也即是 marker 数据），是 ABORT 的话，那么与这个事务相关的 PID 信息从 `abortedProducerIds` 集合删掉，是 COMMIT 的话，就忽略（每个这个 PID 对应的 marker 数据收到之后，就从 `abortedProducerIds` 中清除这个 PID 信息）；
2. 如果这个数据是正常的数据，把它的 PID 和 offset 信息与 `abortedTransactions` 队列（有序队列，头部 transaction 的 first offset 最小）第一个 transaction 做比较，如果 PID 相同，并且 offset 大于等于这个 transaction 的 first offset，就将这个 PID 信息添加到 `abortedProducerIds` 集合中，同时从 `abortedTransactions` 队列中删除这个 transaction，最后再丢掉这个 batch（它是 abort transaction 的数据）；
3. 检查这个 batch 的 PID 是否在 `abortedProducerIds` 集合中，在的话，就丢弃，不在的话就返回上层应用。

这部分的实现确实有些绕（有兴趣的可以慢慢咀嚼一下），它严重依赖了 Kafka 提供的下面两种保证：

1. Consumer 拉取到的数据，在处理时，其 offset 是严格有序的；
2. 同一个 txn.id（PID 相同）在某一个时刻最多只能有一个事务正在进行；

这部分代码实现如下：

```java
private Record nextFetchedRecord() {
    while (true) {
        if (records == null || !records.hasNext()) { //note: records 为空（数据全部丢掉了），records 没有数据（是 control msg）
            maybeCloseRecordStream();

            if (!batches.hasNext()) {
                // Message format v2 preserves the last offset in a batch even if the last record is removed
                // through compaction. By using the next offset computed from the last offset in the batch,
                // we ensure that the offset of the next fetch will point to the next batch, which avoids
                // unnecessary re-fetching of the same batch (in the worst case, the consumer could get stuck
                // fetching the same batch repeatedly).
                if (currentBatch != null)
                    nextFetchOffset = currentBatch.nextOffset();
                drain();
                return null;
            }

            currentBatch = batches.next();
            maybeEnsureValid(currentBatch);

            if (isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                //note: 需要做相应的判断
                // remove from the aborted transaction queue all aborted transactions which have begun
                // before the current batch's last offset and add the associated producerIds to the
                // aborted producer set
                //note: 如果这个 batch 的 offset 已经大于等于 abortedTransactions 中第一事务的 first offset
                //note: 那就证明下个 abort transaction 的数据已经开始到来，将 PID 添加到 abortedProducerIds 中
                consumeAbortedTransactionsUpTo(currentBatch.lastOffset());

                long producerId = currentBatch.producerId();
                if (containsAbortMarker(currentBatch)) {
                    abortedProducerIds.remove(producerId); //note: 这个 PID（当前事务）涉及到的数据已经处理完
                } else if (isBatchAborted(currentBatch)) { //note: 丢弃这个数据
                    log.debug("Skipping aborted record batch from partition {} with producerId {} and " +
                                  "offsets {} to {}",
                              partition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset());
                    nextFetchOffset = currentBatch.nextOffset();
                    continue;
                }
            }

            records = currentBatch.streamingIterator(decompressionBufferSupplier);
        } else {
            Record record = records.next();
            // skip any records out of range
            if (record.offset() >= nextFetchOffset) {
                // we only do validation when the message should not be skipped.
                maybeEnsureValid(record);

                // control records are not returned to the user
                if (!currentBatch.isControlBatch()) { //note: 过滤掉 marker 数据
                    return record;
                } else {
                    // Increment the next fetch offset when we skip a control batch.
                    nextFetchOffset = record.offset() + 1;
                }
            }
        }
    }
}
```

#### Consumer 消费数据时，其顺序如何保证

有了前面的分析，这个问题就很好回答了，顺序性还是严格按照 offset 的，只不过遇到 abort trsansaction 的数据时就丢弃掉，其他的与普通 Consumer 并没有区别。

#### 如果 txn.id 长期不使用，server 端怎么处理？

Producer 在开始一个事务操作时，可以设置其事务超时时间（参数是 `transaction.timeout.ms`，默认60s），而且 Server 端还有一个最大可允许的事务操作超时时间（参数是 `transaction.timeout.ms`，默认是15min），Producer 设置超时时间不能超过 Server，否则的话会抛出异常。

上面是关于事务操作的超时设置，而对于 txn.id，我们知道 TransactionCoordinator 会缓存 txn.id 的相关信息，如果没有超时机制，这个 meta 大小是无法预估的，Server 端提供了一个 `transaction.id.expiration.ms` 参数来配置这个超时时间（默认是7天），如果超过这个时间没有任何事务相关的请求发送过来，那么 TransactionCoordinator 将会使这个 txn.id 过期。

#### PID Snapshot 是做什么的？用来解决什么问题？

对于每个 Topic-Partition，Broker 都会在内存中维护其 PID 与 sequence number（最后成功写入的 msg 的 sequence number）的对应关系（这个在上面幂等性文章应讲述过，主要是为了不丢补充的实现）。

Broker 重启时，如果想恢复上面的状态信息，那么它读取所有的 log 文件。相比于之下，定期对这个 state 信息做 checkpoint（Snapshot），明显收益是非常大的，此时如果 Broker 重启，只需要读取最近一个 Snapshot 文件，之后的数据再从 log 文件中恢复即可。

这个 PID Snapshot 样式如 00000000000235947656.snapshot，以 `.snapshot` 作为后缀，其数据格式如下：

```
[matt@XXX-35 app.matt_test_transaction_json_3-2]$ /usr/local/java18/bin/java -Djava.ext.dirs=/XXX/kafka/libs kafka.tools.DumpLogSegments --files 00000000000235947656.snapshotDumping 00000000000235947656.snapshotproducerId: 2000 producerEpoch: 1 coordinatorEpoch: 4 currentTxnFirstOffset: None firstSequence: 95769510 lastSequence: 95769511 lastOffset: 235947654 offsetDelta: 1 timestamp: 1541325156503producerId: 3000 producerEpoch: 5 coordinatorEpoch: 6 currentTxnFirstOffset: None firstSequence: 91669662 lastSequence: 91669666 lastOffset: 235947651 offsetDelta: 4 timestamp: 1541325156454
```

在实际的使用中，这个 snapshot 文件一般只会保存最近的两个文件。

#### 中间流程故障如何恢复

对于上面所讲述的一个事务操作流程，实际生产环境中，任何一个地方都有可能出现的失败：
1. Producer 在发送 `beginTransaction()` 时，如果出现 timeout 或者错误：Producer 只需要重试即可；
2. Producer 在发送数据时出现错误：Producer 应该 abort 这个事务，如果 Produce 没有 abort（比如设置了重试无限次，并且 batch 超时设置得非常大），TransactionCoordinator 将会在这个事务超时之后 abort 这个事务操作；
3. Producer 发送 `commitTransaction()` 时出现 timeout 或者错误：Producer 应该重试这个请求；
4. Coordinator Failure：如果 Transaction Coordinator 发生切换（事务 topic leader 切换），Coordinator 可以从日志中恢复。如果发送事务有处于 PREPARE_COMMIT 或 PREPARE_ABORT 状态，那么直接执行 commit 或者 abort 操作，如果是一个正在进行的事务，Coordinator 的失败并不需要 abort 事务，producer 只需要向新的 Coordinator 发送请求即可。
### Tips
- Compaction Topic.   
默认的删除规则之外，提供了另外一种删除过期数据的策略:对于相关key的不同数据，只保留最后一条数据。