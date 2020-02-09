在上篇文章中讲述了 Fetch 请求是如何处理的，其中包括来自副本同步的 Fetch 请求和 Consumer 的 Fetch 请求，副本同步是 Kafka 多副本机制（可靠性）实现的基础，它也是通过向 leader replica 发送 Fetch 请求来实现数据同步的。本篇文章我们就来看一下 Kafka 副本同步这块的内容，对于每个 broker 来说，它上面的 replica 对象，除了 leader 就是 follower，只要这台 broker 有 follower replica，broker 就会启动副本同步流程从 leader 同步数据，副本同步机制的实现是 Kafka Server 端非常重要的内容，在这篇文章中，主要会从以下几块来讲解：    
 - Kafka 在什么情况下会启动副本同步线程？ 
 - Kafka 副本同步线程启动流程及付副本同步流程的处理逻辑； 
 - Kafka 副本同步需要解决的问题以及 Kafka 是如何解决这些问题的？ 
 - Kafka 在什么情况下会关闭一个副本同步线程。  
<blockquote> 小插曲：本来想先介绍一下与 LeaderAndIsr 请求相关的，因为副本同步线程的启动与这部分是息息相关的，但是发现涉及到了很多 controller 端的内容，而 controller 这部分还没开始涉及，所以本篇文章涉及到 LeaderAndIsr 请求的部分先简单讲述一下其处理逻辑，在 controller 这块再详细介绍。   
</blockquote> 
 ## 整体流程 
 Kafka Server 端的副本同步，是由 replica fetcher 线程来负责的，而它又是由 ReplicaManager 来控制的。关于 ReplicaManger，不知道大家还记不记得在 [Kafka 源码解析之 Server 端如何处理 Produce 请求（十二）](http://matt33.com/2018/03/18/kafka-server-handle-produce-request/) 有一个简单的表格，如下所示。ReplicaManager 通过对 Partition 对象的管理，来控制着 Partition 对应的 Replica 实例，而 Replica 实例又是通过 Log 对象实例来管理着其底层的存储内容。   
| | 管理对象| 组成部分 |
| -----| -----| ----- |
| 日志管理器（LogManager） | 日志（Log） | 日志分段（LogSegment） |
| 副本管理器（ReplicaManager） | 分区（Partition） | 副本（Replica） |
 关于 ReplicaManager 的内容准备专门写一篇文章来介绍，刚好也作为对 Kafka 存储层内容的一个总结。   下面回到这篇文章的主题 —— 副本同步机制，在 ReplicaManager 中有一个实例变量 replicaFetcherManager，它负责管理所有副本同步线程，副本同步线程的启动和关闭都是由这个实例来操作的，关于副本同步相关处理逻辑，下面这张图可以作为一个整体流程，包括了 replica fetcher 线程的启动、工作流程、关闭三个部分，如下图所示：   
![副本同步机制](./images/kafka/fetcher_thread.png)
   后面的讲述会围绕着这张图开始，这里看不懂或不理解也没有关系，后面会一一讲解。   
 ## replica fetcher 线程何时启动 
 Broker 会在什么情况下启动副本同步线程呢？简单想一下这部分的逻辑：首先 broker 分配的任何一个 partition 都是以 Replica 对象实例的形式存在，而 Replica 在 Kafka 上是有两个角色： leader 和 follower，只要这个 Replica 是 follower，它便会向 leader 进行数据同步。   反应在 ReplicaManager 上就是如果 Broker 的本地副本被选举为 follower，那么它将会启动副本同步线程，其具体实现如下所示：   
``` scala
//note: 对于给定的这些副本，将本地副本设置为 follower
//note: 1. 从 leader partition 集合移除这些 partition；
//note: 2. 将这些 partition 标记为 follower，之后这些 partition 就不会再接收 produce 的请求了；
//note: 3. 停止对这些 partition 的副本同步，这样这些副本就不会再有（来自副本请求线程）的数据进行追加了；
//note: 4. 对这些 partition 的 offset 进行 checkpoint，如果日志需要截断就进行截断操作；
//note: 5. 清空 purgatory 中的 produce 和 fetch 请求；
//note: 6. 如果 broker 没有掉线，向这些 partition 的新 leader 启动副本同步线程；
//note: 上面这些操作的顺序性，保证了这些副本在 offset checkpoint 之前将不会接收新的数据，这样的话，在 checkpoint 之前这些数据都可以保证刷到磁盘
private def makeFollowers(controllerId: Int,
                          epoch: Int,
                          partitionState: Map[Partition, PartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Short],
                          metadataCache: MetadataCache) : Set[Partition] = {
  partitionState.keys.foreach { partition =>
    stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
      "starting the become-follower transition for partition %s")
      .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
  }

  for (partition <- partitionState.keys)
    responseMap.put(partition.topicPartition, Errors.NONE.code)

  //note: 统计 follower 的集合
  val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()

  try {

    // TODO: Delete leaders from LeaderAndIsrRequest
    partitionState.foreach{ case (partition, partitionStateInfo) =>
      val newLeaderBrokerId = partitionStateInfo.leader
      metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match { //note: leader 是可用的
        // Only change partition state when the leader is available
        case Some(_) => //note: partition 的本地副本设置为 follower
          if (partition.makeFollower(controllerId, partitionStateInfo, correlationId))
            partitionsToMakeFollower += partition
          else //note: 这个 partition 的本地副本已经是 follower 了
            stateChangeLogger.info(("Broker %d skipped the become-follower state change after marking its partition as follower with correlation id %d from " +
              "controller %d epoch %d for partition %s since the new leader %d is the same as the old leader")
              .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
              partition.topicPartition, newLeaderBrokerId))
        case None =>
          // The leader broker should always be present in the metadata cache.
          // If not, we should record the error message and abort the transition process for this partition
          stateChangeLogger.error(("Broker %d received LeaderAndIsrRequest with correlation id %d from controller" +
            " %d epoch %d for partition %s but cannot become follower since the new leader %d is unavailable.")
            .format(localBrokerId, correlationId, controllerId, partitionStateInfo.controllerEpoch,
            partition.topicPartition, newLeaderBrokerId))
          // Create the local replica even if the leader is unavailable. This is required to ensure that we include
          // the partition's high watermark in the checkpoint file (see KAFKA-1647)
          partition.getOrCreateReplica()
      }
    }

    //note: 删除对这些 partition 的副本同步线程
    replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
    partitionsToMakeFollower.foreach { partition =>
      stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +
        "%d epoch %d with correlation id %d for partition %s")
        .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
    }

    //note: Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
    logManager.truncateTo(partitionsToMakeFollower.map { partition =>
      (partition.topicPartition, partition.getOrCreateReplica().highWatermark.messageOffset)
    }.toMap)
    //note: 完成那些延迟请求的处理
    partitionsToMakeFollower.foreach { partition =>
      val topicPartitionOperationKey = new TopicPartitionOperationKey(partition.topicPartition)
      tryCompleteDelayedProduce(topicPartitionOperationKey)
      tryCompleteDelayedFetch(topicPartitionOperationKey)
    }

    partitionsToMakeFollower.foreach { partition =>
      stateChangeLogger.trace(("Broker %d truncated logs and checkpointed recovery boundaries for partition %s as part of " +
        "become-follower request with correlation id %d from controller %d epoch %d").format(localBrokerId,
        partition.topicPartition, correlationId, controllerId, epoch))
    }

    if (isShuttingDown.get()) {
      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d skipped the adding-fetcher step of the become-follower state change with correlation id %d from " +
          "controller %d epoch %d for partition %s since it is shutting down").format(localBrokerId, correlationId,
          controllerId, epoch, partition.topicPartition))
      }
    }
    else {
      // we do not need to check if the leader exists again since this has been done at the beginning of this process
      //note: 启动副本同步线程
      val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
        partition.topicPartition -> BrokerAndInitialOffset(
          metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get.getBrokerEndPoint(config.interBrokerListenerName),
          partition.getReplica().get.logEndOffset.messageOffset)).toMap //note: leader 信息+本地 replica 的 offset
      replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(("Broker %d started fetcher to new leader as part of become-follower request from controller " +
          "%d epoch %d with correlation id %d for partition %s")
          .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
      }
    }
  } catch {
    case e: Throwable =>
      val errorMsg = ("Error on broker %d while processing LeaderAndIsr request with correlationId %d received from controller %d " +
        "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
      stateChangeLogger.error(errorMsg, e)
      // Re-throw the exception for it to be caught in KafkaApis
      throw e
  }

  partitionState.keys.foreach { partition =>
    stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
      "for the become-follower transition for partition %s")
      .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
  }

  partitionsToMakeFollower
}
```
 简单来说，makeFollowers() 的处理过程如下：    
 - 先从本地记录 leader partition 的集合中将这些 partition 移除，因为这些 partition 已经被选举为了 follower； 
 - 将这些 partition 的本地副本设置为 follower，后面就不会接收关于这个 partition 的 Produce 请求了，如果依然有 client 在向这台 broker 发送数据，那么它将会返回相应的错误； 
 - 先停止关于这些 partition 的副本同步线程（如果本地副本之前是 follower 现在还是 follower，先关闭的原因是：这个 partition 的 leader 发生了变化，如果 leader 没有发生变化，那么 <code>makeFollower</code> 方法返回的是 False，这个 Partition 就不会被添加到 partitionsToMakeFollower 集合中），这样的话可以保证这些 partition 的本地副本将不会再有新的数据追加； 
 - 对这些 partition 本地副本日志文件进行截断操作并进行 checkpoint 操作； 
 - 完成那些延迟处理的 Produce 和 Fetch 请求； 
 - 如果本地的 broker 没有掉线，那么向这些 partition 新选举出来的 leader 启动副本同步线程。  关于第6步，并不一定会为每一个 partition 都启动一个 fetcher 线程，对于一个目的 broker，只会启动 num.replica.fetchers 个线程，具体这个 topic-partition 会分配到哪个 fetcher 线程上，是根据 topic 名和 partition id 进行计算得到，实现所示：   
``` scala
//note: 获取分配到这个 topic-partition 的 fetcher 线程 id
private def getFetcherId(topic: String, partitionId: Int) : Int = {
  Utils.abs(31 * topic.hashCode() + partitionId) % numFetchers
}
```

 ### replica fetcher 线程参数设置 
 关于副本同步线程有一些参数配置，具体如下表所示：   
| 参数| 说明| 默认值 |
| -----| -----| ----- |
| num.replica.fetchers | 从一个 broker 同步数据的 fetcher 线程数，增加这个值时也会增加该 broker 的 Io 并行度（也就是说：从一台 broker 同步数据，最多能开这么大的线程数） | 1 |
| replica.fetch.wait.max.ms | 对于 follower replica 而言，每个 Fetch 请求的最大等待时间，这个值应该比 replica.lag.time.max.ms 要小，否则对于那些吞吐量特别低的 topic 可能会导致 isr 频繁抖动 | 500 |
| replica.high.watermark.checkpoint.interval.ms | hw 刷到磁盘频率 | 500 |
| replica.lag.time.max.ms | 如果一个 follower 在这个时间内没有发送任何 fetch 请求或者在这个时间内没有追上 leader 当前的 log end offset，那么将会从 isr 中移除 | 10000 |
| replica.fetch.min.bytes | 每次 fetch 请求最少拉取的数据量，如果不满足这个条件，那么要等待 replicaMaxWaitTimeMs | 1 |
| replica.fetch.backoff.ms | 拉取时，如果遇到错误，下次拉取等待的时间 | 1000 |
| replica.fetch.max.bytes | 在对每个 partition 拉取时，最大的拉取数量，这并不是一个绝对值，如果拉取的第一条 msg 的大小超过了这个值，只要不超过这个 topic 设置（defined via message.max.bytes (broker config) or max.message.bytes (topic config)）的单条大小限制，依然会返回。 | 1048576 |
| replica.fetch.response.max.bytes | 对于一个 fetch 请求，返回的最大数据量（可能会涉及多个 partition），这并不是一个绝对值，如果拉取的第一条 msg 的大小超过了这个值，只要不超过这个 topic 设置（defined via message.max.bytes (broker config) or max.message.bytes (topic config)）的单条大小限制，依然会返回。 | 10MB |

 ## replica fetcher 线程启动 
 如上面的图所示，在 ReplicaManager 调用 makeFollowers() 启动 replica fetcher 线程后，它实际上是通过 ReplicaFetcherManager 实例进行相关 topic-partition 同步线程的启动和关闭，其启动过程分为下面两步：    
 - ReplicaFetcherManager 调用 <code>addFetcherForPartitions()</code> 添加对这些 topic-partition 的数据同步流程； 
 - ReplicaFetcherManager 调用 <code>createFetcherThread()</code> 初始化相应的 ReplicaFetcherThread 线程。  
 ### addFetcherForPartitions 
 addFetcherForPartitions() 的具体实现如下所示：   
``` scala
//note: 为一个 topic-partition 添加 replica-fetch 线程
def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, BrokerAndInitialOffset]) {
  mapLock synchronized {
    //note: 为这些 topic-partition 分配相应的 fetch 线程 id
    val partitionsPerFetcher = partitionAndOffsets.groupBy { case(topicPartition, brokerAndInitialOffset) =>
      BrokerAndFetcherId(brokerAndInitialOffset.broker, getFetcherId(topicPartition.topic, topicPartition.partition))}
    for ((brokerAndFetcherId, partitionAndOffsets) <- partitionsPerFetcher) {
      //note: 为 BrokerAndFetcherId 构造 fetcherThread 线程
      var fetcherThread: AbstractFetcherThread = null
      fetcherThreadMap.get(brokerAndFetcherId) match {
        case Some(f) => fetcherThread = f
        case None =>
          //note: 创建 fetcher 线程
          fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
          fetcherThreadMap.put(brokerAndFetcherId, fetcherThread)
          fetcherThread.start
      }

      //note: 添加 topic-partition 列表
      fetcherThreadMap(brokerAndFetcherId).addPartitions(partitionAndOffsets.map { case (tp, brokerAndInitOffset) =>
        tp -> brokerAndInitOffset.initOffset
      })
    }
  }

  info("Added fetcher for partitions %s".format(partitionAndOffsets.map { case (topicPartition, brokerAndInitialOffset) =>
    "[" + topicPartition + ", initOffset " + brokerAndInitialOffset.initOffset + " to broker " + brokerAndInitialOffset.broker + "] "}))
}
```
 这个方法其实是做了下面这几件事：    
 - 先计算这个 topic-partition 对应的 fetcher id； 
 - 根据 leader 和 fetcher id 获取对应的 replica fetcher 线程，如果没有找到，就调用 <code>createFetcherThread()</code> 创建一个新的 fetcher 线程； 
 - 如果是新启动的 replica fetcher 线程，那么就启动这个线程； 
 - 将 topic-partition 记录到 <code>fetcherThreadMap</code> 中，这个变量记录每个 replica fetcher 线程要同步的 topic-partition 列表。  
 ### createFetcherThread 
 ReplicaFetcherManager 创建 replica Fetcher 线程的实现如下：   
``` scala
//note: 创建 replica-fetch 线程
override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
  val threadName = threadNamePrefix match {
    case None =>
      "ReplicaFetcherThread-%d-%d".format(fetcherId, sourceBroker.id)
    case Some(p) =>
      "%s:ReplicaFetcherThread-%d-%d".format(p, fetcherId, sourceBroker.id)
  }
  new ReplicaFetcherThread(threadName, fetcherId, sourceBroker, brokerConfig,
    replicaMgr, metrics, time, quotaManager) //note: replica-fetch 线程
}
```

 ## replica fetcher 线程处理过程 
 replica fetcher 线程在启动之后就开始进行正常数据同步流程了，在文章最开始流程图中的第二部分（线程处理过程）已经给出了大概的处理过程，这节会详细介绍一下，这个过程都是在 ReplicaFetcherThread 线程中实现的。   
 ### doWoker 
 ReplicaFetcherThread 的 doWork() 方法是一直在这个线程中的 run() 中调用的，实现方法如下：   
``` scala
override def run(): Unit = {
  info("Starting ")
  try{
    while(isRunning.get()){
      doWork()
    }
  } catch{
    case e: Throwable =>
      if(isRunning.get())
        error("Error due to ", e)
  }
  shutdownLatch.countDown()
  info("Stopped ")
}

override def doWork() {
  //note: 构造 fetch request
  val fetchRequest = inLock(partitionMapLock) {
    val fetchRequest = buildFetchRequest(partitionStates.partitionStates.asScala.map { state =>
      state.topicPartition -> state.value
    })
    if (fetchRequest.isEmpty) { //note: 如果没有活跃的 partition，在下次调用之前，sleep fetchBackOffMs 时间
      trace("There are no active partitions. Back off for %d ms before sending a fetch request".format(fetchBackOffMs))
      partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
    }
    fetchRequest
  }
  if (!fetchRequest.isEmpty)
    processFetchRequest(fetchRequest) //note: 发送 fetch 请求，处理 fetch 的结果
}
```
 在 doWork() 方法中主要做了两件事：    
 - 构造相应的 Fetch 请求（<code>buildFetchRequest()</code>）； 
 - 通过 <code>processFetchRequest()</code> 方法发送 Fetch 请求，并对其结果进行相应的处理。  
 ### buildFetchRequest 
 通过 buildFetchRequest() 方法构造相应的 Fetcher 请求时，会设置 replicaId，该值会代表了这个 Fetch 请求是来自副本同步，而不是来自 consumer。   
``` scala
//note: 构造 Fetch 请求
protected def buildFetchRequest(partitionMap: Seq[(TopicPartition, PartitionFetchState)]): FetchRequest = {
  val requestMap = new util.LinkedHashMap[TopicPartition, JFetchRequest.PartitionData]

  partitionMap.foreach { case (topicPartition, partitionFetchState) =>
    // We will not include a replica in the fetch request if it should be throttled.
    if (partitionFetchState.isActive && !shouldFollowerThrottle(quota, topicPartition))
      requestMap.put(topicPartition, new JFetchRequest.PartitionData(partitionFetchState.offset, fetchSize))
  }
  //note: 关键在于 setReplicaId 方法,设置了 replicaId, 对于 consumer, 该值为 CONSUMER_REPLICA_ID（-1）
  val requestBuilder = new JFetchRequest.Builder(maxWait, minBytes, requestMap).
      setReplicaId(replicaId).setMaxBytes(maxBytes)
  requestBuilder.setVersion(fetchRequestVersion)
  new FetchRequest(requestBuilder)
}
```

 ### processFetchRequest 
 processFetchRequest() 这个方法的作用是发送 Fetch 请求，并对返回的结果进行处理，最终写入到本地副本的 Log 实例中，其具体实现：   
``` scala
private def processFetchRequest(fetchRequest: REQ) {
  val partitionsWithError = mutable.Set[TopicPartition]()

  def updatePartitionsWithError(partition: TopicPartition): Unit = {
    partitionsWithError += partition
    partitionStates.moveToEnd(partition)
  }

  var responseData: Seq[(TopicPartition, PD)] = Seq.empty

  try {
    trace("Issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
    responseData = fetch(fetchRequest) //note: 发送 fetch 请求，获取 fetch 结果
  } catch {
    case t: Throwable =>
      if (isRunning.get) {
        warn(s"Error in fetch $fetchRequest", t)
        inLock(partitionMapLock) { //note: fetch 时发生错误，sleep 一会
          partitionStates.partitionSet.asScala.foreach(updatePartitionsWithError)
          // there is an error occurred while fetching partitions, sleep a while
          // note that `ReplicaFetcherThread.handlePartitionsWithError` will also introduce the same delay for every
          // partition with error effectively doubling the delay. It would be good to improve this.
          partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
        }
      }
  }
  fetcherStats.requestRate.mark()

  if (responseData.nonEmpty) { //note: fetch 结果不为空
    // process fetched data
    inLock(partitionMapLock) {

      responseData.foreach { case (topicPartition, partitionData) =>
        val topic = topicPartition.topic
        val partitionId = topicPartition.partition
        Option(partitionStates.stateValue(topicPartition)).foreach(currentPartitionFetchState =>
          // we append to the log if the current offset is defined and it is the same as the offset requested during fetch
          //note: 如果 fetch 的 offset 与返回结果的 offset 相同，并且返回没有异常，那么就将拉取的数据追加到对应的 partition 上
          if (fetchRequest.offset(topicPartition) == currentPartitionFetchState.offset) {
            Errors.forCode(partitionData.errorCode) match {
              case Errors.NONE =>
                try {
                  val records = partitionData.toRecords
                  val newOffset = records.shallowEntries.asScala.lastOption.map(_.nextOffset).getOrElse(
                    currentPartitionFetchState.offset)

                  fetcherLagStats.getAndMaybePut(topic, partitionId).lag = Math.max(0L, partitionData.highWatermark - newOffset)
                  // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                  //note: 将 fetch 的数据追加到日志文件中
                  processPartitionData(topicPartition, currentPartitionFetchState.offset, partitionData)

                  val validBytes = records.validBytes
                  if (validBytes > 0) {
                    // Update partitionStates only if there is no exception during processPartitionData
                    //note: 更新 fetch 的 offset 位置
                    partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset))
                    fetcherStats.byteRate.mark(validBytes) //note: 更新 metrics
                  }
                } catch {
                  case ime: CorruptRecordException =>
                    // we log the error and continue. This ensures two things
                    // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                    // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                    // should get fixed in the subsequent fetches
                    //note: CRC 验证失败时，打印日志，并继续进行（这个线程还会有其他的 tp 拉取，防止影响其他副本同步）
                    logger.error("Found invalid messages during fetch for partition [" + topic + "," + partitionId + "] offset " + currentPartitionFetchState.offset  + " error " + ime.getMessage)
                    updatePartitionsWithError(topicPartition);
                  case e: Throwable =>
                    //note: 这里还会抛出异常，是 RUNTimeException
                    throw new KafkaException("error processing data for partition [%s,%d] offset %d"
                      .format(topic, partitionId, currentPartitionFetchState.offset), e)
                }
              case Errors.OFFSET_OUT_OF_RANGE => //note: Out-of-range 的情况处理
                try {
                  val newOffset = handleOffsetOutOfRange(topicPartition)
                  partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset))
                  error("Current offset %d for partition [%s,%d] out of range; reset offset to %d"
                    .format(currentPartitionFetchState.offset, topic, partitionId, newOffset))
                } catch { //note: 处理 out-of-range 是抛出的异常
                  case e: Throwable =>
                    error("Error getting offset for partition [%s,%d] to broker %d".format(topic, partitionId, sourceBroker.id), e)
                    updatePartitionsWithError(topicPartition)
                }
              case _ => //note: 其他的异常情况
                if (isRunning.get) {
                  error("Error for partition [%s,%d] to broker %d:%s".format(topic, partitionId, sourceBroker.id,
                    partitionData.exception.get))
                  updatePartitionsWithError(topicPartition)
                }
            }
          })
      }
    }
  }

  //note: 处理拉取遇到的错误读的 tp
  if (partitionsWithError.nonEmpty) {
    debug("handling partitions with error for %s".format(partitionsWithError))
    handlePartitionsWithErrors(partitionsWithError)
  }
}
```
 其处理过程简单总结一下：    
 - 通过 <code>fetch()</code> 方法，发送 Fetch 请求，获取相应的 response（如果遇到异常，那么在下次发送 Fetch 请求之前，会 sleep 一段时间再发）； 
 - 如果返回的结果 不为空，并且 Fetch 请求的 offset 信息与返回结果的 offset 信息对得上，那么就会调用 <code>processPartitionData()</code> 方法将拉取到的数据追加本地副本的日志文件中，如果返回结果有错误信息，那么就对相应错误进行相应的处理； 
 - 对在 Fetch 过程中遇到异常或返回错误的 topic-partition，会进行 delay 操作，下次 Fetch 请求的发生至少要间隔 <code>replica.fetch.backoff.ms</code> 时间。  
 #### fetch 
 fetch() 方法作用是发送 Fetch 请求，并返回相应的结果，其具体的实现，如下：   
``` scala
//note: 发送 fetch 请求，获取拉取结果
protected def fetch(fetchRequest: FetchRequest): Seq[(TopicPartition, PartitionData)] = {
  val clientResponse = sendRequest(fetchRequest.underlying)
  val fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse]
  fetchResponse.responseData.asScala.toSeq.map { case (key, value) =>
    key -> new PartitionData(value)
  }
}

//note: 发送请求
private def sendRequest(requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): ClientResponse = {
  import kafka.utils.NetworkClientBlockingOps._
  try {
    if (!networkClient.blockingReady(sourceNode, socketTimeout)(time))
      throw new SocketTimeoutException(s"Failed to connect within $socketTimeout ms")
    else {
      val clientRequest = networkClient.newClientRequest(sourceBroker.id.toString, requestBuilder,
        time.milliseconds(), true)
      networkClient.blockingSendAndReceive(clientRequest)(time) //note: 阻塞直到获取返回结果
    }
  }
  catch {
    case e: Throwable =>
      networkClient.close(sourceBroker.id.toString)
      throw e
  }

}
```

 #### processPartitionData 
 这个方法的作用是，处理 Fetch 请求的具体数据内容，简单来说就是：检查一下数据大小是否超过限制、将数据追加到本地副本的日志文件中、更新本地副本的 hw 值。   
``` scala
// process fetched data
//note: 处理 fetch 的数据，将 fetch 的数据追加的日志文件中
def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PartitionData) {
  try {
    val replica = replicaMgr.getReplica(topicPartition).get
    val records = partitionData.toRecords

    //note: 检查 records
    maybeWarnIfOversizedRecords(records, topicPartition)

    if (fetchOffset != replica.logEndOffset.messageOffset)
      throw new RuntimeException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(topicPartition, fetchOffset, replica.logEndOffset.messageOffset))
    if (logger.isTraceEnabled)
      trace("Follower %d has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
        .format(replica.brokerId, replica.logEndOffset.messageOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))
    replica.log.get.append(records, assignOffsets = false) //note: 将 fetch 的数据追加到 log 中
    if (logger.isTraceEnabled)
      trace("Follower %d has replica log end offset %d after appending %d bytes of messages for partition %s"
        .format(replica.brokerId, replica.logEndOffset.messageOffset, records.sizeInBytes, topicPartition))
    //note: 更新 replica 的 hw（logEndOffset 在追加数据后也会立马进行修改)
    val followerHighWatermark = replica.logEndOffset.messageOffset.min(partitionData.highWatermark)
    // for the follower replica, we do not need to keep
    // its segment base offset the physical position,
    // these values will be computed upon making the leader
    //note: 这个值主要是用在 leader replica 上的
    replica.highWatermark = new LogOffsetMetadata(followerHighWatermark)
    if (logger.isTraceEnabled)
      trace(s"Follower ${replica.brokerId} set replica high watermark for partition $topicPartition to $followerHighWatermark")
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)
  } catch {
    case e: KafkaStorageException =>
      fatal(s"Disk error while replicating data for $topicPartition", e)
      Runtime.getRuntime.halt(1)
  }
}
```

 ## 副本同步异常情况的处理 
 在副本同步的过程中，会遇到哪些异常情况呢？   大家一定会想到关于 offset 的问题，在 Kafka 中，关于 offset 的处理，无论是 producer 端、consumer 端还是其他地方，offset 似乎都是一个形影不离的问题。在副本同步时，关于 offset，会遇到什么问题呢？下面举两个异常的场景：    
 - 假如当前本地（id：1）的副本现在是 leader，其 LEO 假设为1000，而另一个在 isr 中的副本（id：2）其 LEO 为800，此时出现网络抖动，id 为1 的机器掉线后又上线了，但是此时副本的 leader 实际上已经变成了 2，而2的 LEO 为800，这时候1启动副本同步线程去2上拉取数据，希望从 offset=1000 的地方开始拉取，但是2上最大的 offset 才是800，这种情况该如何处理呢？ 
 - 假设一个 replica （id：1）其 LEO 是10，它已经掉线好几天，这个 partition leader 的 offset 范围是 [100, 800]，那么 1 重启启动时，它希望从 offset=10 的地方开始拉取数据时，这时候发生了 OutOfRange，不过跟上面不同的是这里是小于了 leader offset 的范围，这种情况又该怎么处理？  以上两种情况都是 offset OutOfRange 的情况，只不过：一是 Fetch Offset 超过了 leader 的 LEO，二是 Fetch Offset 小于 leader 最小的 offset，在介绍 Kafka 解决方案之前，我们先来自己思考一下这两种情况应该怎么处理？    
 - 如果 fetch offset 超过 leader 的 offset，这时候副本应该是回溯到 leader 的 LEO 位置（超过这个值的数据删除），然后再去进行副本同步，当然这种解决方案其实是无法保证 leader 与 follower 数据的完全一致，再次发生 leader 切换时，可能会导致数据的可见性不一致，但既然用户允许了脏选举的发生，其实我们是可以认为用户是可以接收这种情况发生的； 
 - 这种就比较容易处理，首先清空本地的数据，因为本地的数据都已经过期了，然后从 leader 的最小 offset 位置开始拉取数据。  上面是我们比较容易想出的解决方案，而在 Kafka 中，其解决方案也很类似，不过遇到情况比上面我们列出的两种情况多了一些复杂，其解决方案如下：   
``` scala
/**
   * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
   * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
   * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
   * and it may discover that the current leader's end offset is behind its own end offset.
   *
   * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
   *
   * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
   */
  //note: 脏选举的发生
  //note: 获取最新的 offset
  val leaderEndOffset: Long = earliestOrLatestOffset(topicPartition, ListOffsetRequest.LATEST_TIMESTAMP,
    brokerConfig.brokerId)

  if (leaderEndOffset < replica.logEndOffset.messageOffset) { //note: leaderEndOffset 小于 副本 LEO 的情况
    // Prior to truncating the follower's log, ensure that doing so is not disallowed by the configuration for unclean leader election.
    // This situation could only happen if the unclean election configuration for a topic changes while a replica is down. Otherwise,
    // we should never encounter this situation since a non-ISR leader cannot be elected if disallowed by the broker configuration.
    //note: 这种情况只是发生在 unclear election 的情况下
    if (!LogConfig.fromProps(brokerConfig.originals, AdminUtils.fetchEntityConfig(replicaMgr.zkUtils,
      ConfigType.Topic, topicPartition.topic)).uncleanLeaderElectionEnable) { //note: 不允许 unclear elect 时,直接退出进程
      // Log a fatal error and shutdown the broker to ensure that data loss does not unexpectedly occur.
      fatal("Exiting because log truncation is not allowed for partition %s,".format(topicPartition) +
        " Current leader %d's latest offset %d is less than replica %d's latest offset %d"
        .format(sourceBroker.id, leaderEndOffset, brokerConfig.brokerId, replica.logEndOffset.messageOffset))
      System.exit(1)
    }

    //note: warn 日志信息
    warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's latest offset %d"
      .format(brokerConfig.brokerId, topicPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderEndOffset))
    //note: 进行截断操作,将offset 大于等于targetOffset 的数据和索引删除
    replicaMgr.logManager.truncateTo(Map(topicPartition -> leaderEndOffset))
    leaderEndOffset
  } else { //note: leader 的 LEO 大于 follower 的 LEO 的情况下,还发生了 OutOfRange
    //note: 1. follower 下线了很久,其 LEO 已经小于了 leader 的 StartOffset;
    //note: 2. 脏选举发生时, 如果 old leader 的 HW 大于 new leader 的 LEO,此时 old leader 回溯到 HW,并且这个位置开始拉取数据发生了 Out of range
    //note:    当这个方法调用时,随着 produce 持续产生数据,可能出现 leader LEO 大于 Follower LEO 的情况（不做任何处理,重试即可解决,但
    //note:    无法保证数据的一致性）。
    /**
     * If the leader's log end offset is greater than the follower's log end offset, there are two possibilities:
     * 1. The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
     * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
     * 2. When unclean leader election occurs, it is possible that the old leader's high watermark is greater than
     * the new leader's log end offset. So when the old leader truncates its offset to its high watermark and starts
     * to fetch from the new leader, an OffsetOutOfRangeException will be thrown. After that some more messages are
     * produced to the new leader. While the old leader is trying to handle the OffsetOutOfRangeException and query
     * the log end offset of the new leader, the new leader's log end offset becomes higher than the follower's log end offset.
     *
     * In the first case, the follower's current log end offset is smaller than the leader's log start offset. So the
     * follower should truncate all its logs, roll out a new segment and start to fetch from the current leader's log
     * start offset.
     * In the second case, the follower should just keep the current log segments and retry the fetch. In the second
     * case, there will be some inconsistency of data between old and new leader. We are not solving it here.
     * If users want to have strong consistency guarantees, appropriate configurations needs to be set for both
     * brokers and producers.
     *
     * Putting the two cases together, the follower should fetch from the higher one of its replica log end offset
     * and the current leader's log start offset.
     *
     */
    val leaderStartOffset: Long = earliestOrLatestOffset(topicPartition, ListOffsetRequest.EARLIEST_TIMESTAMP,
      brokerConfig.brokerId)
    warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's start offset %d"
      .format(brokerConfig.brokerId, topicPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderStartOffset))
    val offsetToFetch = Math.max(leaderStartOffset, replica.logEndOffset.messageOffset)
    // Only truncate log when current leader's log start offset is greater than follower's log end offset.
    if (leaderStartOffset > replica.logEndOffset.messageOffset) //note: 如果 leader 的 startOffset 大于副本的最大 offset
      //note: 将这个 log 的数据全部清空,并且从 leaderStartOffset 开始拉取数据
      replicaMgr.logManager.truncateFullyAndStartAt(topicPartition, leaderStartOffset)
    offsetToFetch
  }
}
```
 针对第一种情况，在 Kafka 中，实际上还会发生这样一种情况，1 在收到 OutOfRange 错误时，这时去 leader 上获取的 LEO 值与最小的 offset 值，这时候却发现 leader 的 LEO 已经从 800 变成了 1100（这个 topic-partition 的数据量增长得比较快），再按照上面的解决方案就不太合理，Kafka 这边的解决方案是：遇到这种情况，进行重试就可以了，下次同步时就会正常了，但是依然会有上面说的那个问题。   
 ## replica fetcher 线程的关闭 
 最后我们再来介绍一下 replica fetcher 线程在什么情况下会关闭，同样，看一下最开始那张图的第三部分，图中已经比较清晰地列出了 replica fetcher 线程关闭的条件，在三种情况下会关闭对这个 topic-partition 的拉取操作（becomeLeaderOrFollower() 这个方法会在对 LeaderAndIsr 请求处理的文章中讲解，这里先忽略）：    
 - <code>stopReplica()</code>：broker 收到了 controller 发来的 StopReplica 请求，这时会开始关闭对指定 topic-partition 的同步线程； 
 - <code>makeLeaders</code>：这些 partition 的本地副本被选举成了 leader，这时候就会先停止对这些 topic-partition 副本同步线程； 
 - <code>makeFollowers()</code>：前面已经介绍过，这里实际上停止副本同步，然后再开启副本同步线程，因为这些 topic-partition 的 leader 可能发生了切换。  
<blockquote> 这里直接说线程关闭，其实不是很准确，因为每个 replica fetcher 线程操作的是多个 topic-partition，而在关闭的粒度是 partition 级别，只有这个线程分配的 partition 全部关闭后，这个线程才会真正被关闭。   
</blockquote> 
 ### 关闭副本同步 
 看下 ReplicaManager 中触发 replica fetcher 线程关闭的三个方法。   
 #### stopReplica 
 StopReplica 的请求实际上是 Controller 发送过来的，这个在 controller 部分会讲述，它触发的条件有多种，比如：broker 下线、partition replica 迁移等等，ReplicaManager 这里的实现如下：   
``` scala
//note: 获取 tp 的 leader replica
def getLeaderReplicaIfLocal(topicPartition: TopicPartition): Replica =  {
  val partitionOpt = getPartition(topicPartition) //note: 获取对应的 Partiion 对象
  partitionOpt match {
    case None =>
      throw new UnknownTopicOrPartitionException(s"Partition $topicPartition doesn't exist on $localBrokerId")
    case Some(partition) =>
      partition.leaderReplicaIfLocal match {
        case Some(leaderReplica) => leaderReplica //note: 返回 leader 对应的副本
        case None =>
          throw new NotLeaderForPartitionException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
      }
  }
}
```

 #### makeLeaders 
 makeLeaders() 方法的调用是在 broker 上这个 partition 的副本被设置为 leader 时触发的，其实现如下：   
``` scala
/*
 * Make the current broker to become leader for a given set of partitions by:
 *
 * 1. Stop fetchers for these partitions
 * 2. Update the partition metadata in cache
 * 3. Add these partitions to the leader partitions set
 *
 * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
 * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
 * return the set of partitions that are made leader due to this method
 *
 *  TODO: the above may need to be fixed later
 */
//note: 选举当前副本作为 partition 的 leader，处理过程：
//note: 1. 停止这些 partition 的 副本同步请求；
//note: 2. 更新缓存中的 partition metadata；
//note: 3. 将这些 partition 添加到 leader partition 集合中。
private def makeLeaders(controllerId: Int,
                        epoch: Int,
                        partitionState: Map[Partition, PartitionState],
                        correlationId: Int,
                        responseMap: mutable.Map[TopicPartition, Short]): Set[Partition] = {
  partitionState.keys.foreach { partition =>
    stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +
      "starting the become-leader transition for partition %s")
      .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
  }

  for (partition <- partitionState.keys)
    responseMap.put(partition.topicPartition, Errors.NONE.code)

  val partitionsToMakeLeaders: mutable.Set[Partition] = mutable.Set()

  try {
    // First stop fetchers for all the partitions
    //note: 停止这些副本同步请求
    replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(_.topicPartition))
    // Update the partition information to be the leader
    //note: 更新这些 partition 的信息（这些 partition 成为 leader 了）
    partitionState.foreach{ case (partition, partitionStateInfo) =>
      //note: 在 partition 对象将本地副本设置为 leader
      if (partition.makeLeader(controllerId, partitionStateInfo, correlationId))
        partitionsToMakeLeaders += partition //note: 成功选为 leader 的 partition 集合
      else
        //note: 本地 replica 已经是 leader replica，可能是接收了重试的请求
        stateChangeLogger.info(("Broker %d skipped the become-leader state change after marking its partition as leader with correlation id %d from " +
          "controller %d epoch %d for partition %s since it is already the leader for the partition.")
          .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
    }
    partitionsToMakeLeaders.foreach { partition =>
      stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-leader request from controller " +
        "%d epoch %d with correlation id %d for partition %s")
        .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
    }
  } catch {
    case e: Throwable =>
      partitionState.keys.foreach { partition =>
        val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d" +
          " epoch %d for partition %s").format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition)
        stateChangeLogger.error(errorMsg, e)
      }
      // Re-throw the exception for it to be caught in KafkaApis
      throw e
  }

  //note: LeaderAndIsr 请求处理完成
  partitionState.keys.foreach { partition =>
    stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +
      "for the become-leader transition for partition %s")
      .format(localBrokerId, correlationId, controllerId, epoch, partition.topicPartition))
  }

  partitionsToMakeLeaders
}
```
 简单来说，这个方法的过程逻辑如下：    
 - 先停止对这些 partition 的副本同步流程，因为这些 partition 的本地副本已经被选举成为了 leader； 
 - 将这些 partition 的本地副本设置为 leader，并且开始更新相应 meta 信息（主要是记录其他 follower 副本的相关信息）； 
 - 将这些 partition 添加到本地记录的 leader partition 集合中。  
 #### makeFollowers 
 这个在前面已经讲述过了，参考前面的讲述。   
 ### removeFetcherForPartitions 
 调用 ReplicaFetcherManager 的 removeFetcherForPartitions() 删除对这些 topic-partition 的副本同步设置，这里在实现时，会遍历所有的 replica fetcher 线程，都执行 removePartitions() 方法来移除对应的 topic-partition 集合。   
``` scala
//note: 删除一个 partition 的 replica-fetch 线程
def removeFetcherForPartitions(partitions: Set[TopicPartition]) {
  mapLock synchronized {
    for (fetcher <- fetcherThreadMap.values) //note: 遍历所有的 fetchThread 去移除这个 topic-partition 集合
      fetcher.removePartitions(partitions)
  }
  info("Removed fetcher for partitions %s".format(partitions.mkString(",")))
}
```

 ### removePartitions 
 这个方法的作用是：ReplicaFetcherThread 将这些 topic-partition 从自己要拉取的 partition 列表中移除。   
``` scala
def removePartitions(topicPartitions: Set[TopicPartition]) {
  partitionMapLock.lockInterruptibly()
  try {
    topicPartitions.foreach { topicPartition =>
      partitionStates.remove(topicPartition)
      fetcherLagStats.unregister(topicPartition.topic, topicPartition.partition)
    }
  } finally partitionMapLock.unlock()
}
```

 ### ReplicaFetcherThread 的关闭 
 前面介绍那么多，似乎还是没有真正去关闭，那么 ReplicaFetcherThread 真正关闭是哪里操作的呢？   实际上 ReplicaManager 每次处理完 LeaderAndIsr 请求后，都会调用 ReplicaFetcherManager 的 shutdownIdleFetcherThreads() 方法，如果 fetcher 线程要拉取的 topic-partition 集合为空，那么就会关闭掉对应的 fetcher 线程。   
``` scala
//note: 关闭没有拉取 topic-partition 任务的拉取线程
def shutdownIdleFetcherThreads() {
  mapLock synchronized {
    val keysToBeRemoved = new mutable.HashSet[BrokerAndFetcherId]
    for ((key, fetcher) <- fetcherThreadMap) {
      if (fetcher.partitionCount <= 0) { //note: 如果该线程拉取的 partition 数小于 0
        fetcher.shutdown()
        keysToBeRemoved += key
      }
    }
    fetcherThreadMap --= keysToBeRemoved
  }
}
```
 关于 Replica Fetcher 线程这部分的内容终于讲解完了，希望能对大家有所帮助，有问题欢迎通过留言、微博或邮件进行交流。