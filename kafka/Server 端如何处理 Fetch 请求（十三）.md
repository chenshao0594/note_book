上一篇讲述完 Kafka 如何处理 Produce 请求以及日志写操作之后，这篇文章开始讲述 Kafka 如何处理 Fetch 请求以及日志读操作。日志的读写操作是 Kafka 存储层最重要的内容，本文会以 Server 端处理 Fetch 请求的过程为入口，一步步深入到底层的 Log 实例部分。与 Produce 请求不一样的地方是，对于 Fetch 请求，是有两种不同的来源：consumer 和 follower，consumer 读取数据与副本同步数据都是通过向 leader 发送 Fetch 请求来实现的，在对这两种不同情况处理过程中，其底层的实现是统一的，只是实现方法的参数不同而已，在本文中会详细讲述对这两种不同情况的处理。   
 ## Fetch 请求处理的整体流程 
 Fetch 请求（读请求）的处理与 Produce 请求（写请求）的整体流程非常类似，读和写由最上面的抽象层做入口，最终还是在存储层的 Log 对象实例进行真正的读写操作，在这一点上，Kafka 封装的非常清晰，这样的系统设计是非常值得学习的，甚至可以作为分布式系统的模范系统来学习。   Fetch 请求处理的整体流程如下图所示，与 Produce 请求的处理流程非常相似。   
![Server 端处理 Fetch 请求的总体过程](./images/kafka/kafka_fetch_request.png)
   
 ### Fetch 请求的来源 
 那 Server 要处理的 Fetch 请求有几种类型呢？来自于哪里呢？第一个来源肯定是 Consumer，Consumer 在消费数据时会向 Server 端发送 Fetch 请求，那么是不是还没有其他的类型，对 Kafka 比较熟悉的同学大概会猜到，还有一种就是：副本同步，follower 在从 leader 同步数据时，也是发送的 Fetch 请求，下面看下这两种情况的具体实现（代码会进行简化，并不完全与源码一致，便于理解）。   
 #### Consumer Fetch 请求 
 Consumer 的 Fetch 请求是在 poll 方法中调用的，Fetcher 请求的构造过程及发送如下所示：   
``` scala
/**
 * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
 * an in-flight fetch or pending fetch data.
 * @return number of fetches sent
 */
//note: 向订阅的所有 partition （只要该 leader 暂时没有拉取请求）所在 leader 发送 fetch请求
public int sendFetches() {
    //note: 1 创建 Fetch Request
    Map<Node, FetchRequest.Builder> fetchRequestMap = createFetchRequests();
    for (Map.Entry<Node, FetchRequest.Builder> fetchEntry : fetchRequestMap.entrySet()) {
        final FetchRequest.Builder request = fetchEntry.getValue();
        final Node fetchTarget = fetchEntry.getKey();

        log.debug("Sending fetch for partitions {} to broker {}", request.fetchData().keySet(), fetchTarget);
        //note: 2 发送 Fetch Request
        client.send(fetchTarget, request)
                .addListener(new RequestFutureListener<ClientResponse>() {
                    @Override
                    public void onSuccess(ClientResponse resp) {
                        ...
                    }

                    @Override
                    public void onFailure(RuntimeException e) {
                        ...
                    }
                });
    }
    return fetchRequestMap.size();
}

/**
 * Create fetch requests for all nodes for which we have assigned partitions
 * that have no existing requests in flight.
 */
//note: 为所有 node 创建 fetch request
private Map<Node, FetchRequest.Builder> createFetchRequests() {
    // create the fetch info
    Cluster cluster = metadata.fetch();
    Map<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> fetchable = new LinkedHashMap<>();
    for (TopicPartition partition : fetchablePartitions()) {
        Node node = cluster.leaderFor(partition);
        if (node == null) {
            metadata.requestUpdate();
        } else if (this.client.pendingRequestCount(node) == 0) {
            // if there is a leader and no in-flight requests, issue a new fetch
            LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetch = fetchable.get(node);
            if (fetch == null) {
                fetch = new LinkedHashMap<>();
                fetchable.put(node, fetch);
            }

            long position = this.subscriptions.position(partition);
            //note: 要 fetch 的 position 以及 fetch 的大小
            fetch.put(partition, new FetchRequest.PartitionData(position, this.fetchSize));
            log.trace("Added fetch request for partition {} at offset {} to node {}", partition, position, node);
        } else {
            log.trace("Skipping fetch for partition {} because there is an in-flight request to {}", partition, node);
        }
    }

    // create the fetches
    Map<Node, FetchRequest.Builder> requests = new HashMap<>();
    for (Map.Entry<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> entry : fetchable.entrySet()) {
        Node node = entry.getKey();
        // 构造 Fetch 请求
        FetchRequest.Builder fetch = new FetchRequest.Builder(this.maxWaitMs, this.minBytes, entry.getValue()).
                setMaxBytes(this.maxBytes);//note: 构建 Fetch Request
        requests.put(node, fetch);
    }
    return requests;
}
```
 从上面可以看出，Consumer 的 Fetcher 请求构造为：   
``` scala
FetchRequest.Builder fetch = new FetchRequest.Builder(this.maxWaitMs, this.minBytes, entry.getValue()).
                setMaxBytes(this.maxBytes);//note: 构建 Fetch Request
```
 
 #### Replica 同步 Fetch 请求 
 在 Replica 同步（Replica 同步流程的讲解将会在下篇文章中详细展开）的 Fetch 请求中，其 Fetch 请求的构造如下所示：   
``` scala
//note: 构造 Fetch 请求
protected def buildFetchRequest(partitionMap: Seq[(TopicPartition, PartitionFetchState)]): FetchRequest = {
  val requestMap = new util.LinkedHashMap[TopicPartition, JFetchRequest.PartitionData]

  partitionMap.foreach { case (topicPartition, partitionFetchState) =>
    // We will not include a replica in the fetch request if it should be throttled.
    if (partitionFetchState.isActive && !shouldFollowerThrottle(quota, topicPartition))
      requestMap.put(topicPartition, new JFetchRequest.PartitionData(partitionFetchState.offset, fetchSize))
  }
  //note: 关键在于 setReplicaId 方法,设置了 replicaId, consumer 的该值为 CONSUMER_REPLICA_ID（-1）
  val requestBuilder = new JFetchRequest.Builder(maxWait, minBytes, requestMap).
      setReplicaId(replicaId).setMaxBytes(maxBytes)
  requestBuilder.setVersion(fetchRequestVersion)
  new FetchRequest(requestBuilder)
}
```
 与 Consumer Fetch 请求进行对比，这里区别仅在于在构造 FetchRequest 时，调用了 setReplicaId() 方法设置了对应的 replicaId，而 Consumer 在构造时则没有进行设置，该值默认为 CONSUMER_REPLICA_ID，即 
<strong>-1</strong>，这个值是作为 Consumer 的 Fetch 请求与 Replica 同步的 Fetch 请求的区分。   
 ## Server 端的处理 
 这里开始真正讲解 Fetch 请求的处理过程，会按照前面图中的处理流程开始讲解，本节主要是 Server 端抽象层的内容。   
 ### KafkaApis 如何处理 Fetch 请求 
 关于 Fetch 请求的处理，如下所示：   
``` scala
/**
 * Handle a fetch request
 */
def handleFetchRequest(request: RequestChannel.Request) {
  val fetchRequest = request.body.asInstanceOf[FetchRequest]
  val versionId = request.header.apiVersion
  val clientId = request.header.clientId

  //note: 判断 tp 是否存在以及是否有 Describe 权限
  val (existingAndAuthorizedForDescribeTopics, nonExistingOrUnauthorizedForDescribeTopics) = fetchRequest.fetchData.asScala.toSeq.partition {
    case (tp, _) => authorize(request.session, Describe, new Resource(auth.Topic, tp.topic)) && metadataCache.contains(tp.topic)
  }

  //note: 判断 tp 是否有 Read 权限
  val (authorizedRequestInfo, unauthorizedForReadRequestInfo) = existingAndAuthorizedForDescribeTopics.partition {
    case (tp, _) => authorize(request.session, Read, new Resource(auth.Topic, tp.topic))
  }

  //note: 不存在或没有 Describe 权限的 topic 返回 UNKNOWN_TOPIC_OR_PARTITION 错误
  val nonExistingOrUnauthorizedForDescribePartitionData = nonExistingOrUnauthorizedForDescribeTopics.map {
    case (tp, _) => (tp, new FetchResponse.PartitionData(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, -1, MemoryRecords.EMPTY))
  }

  //note: 没有 Read 权限的 topic 返回 TOPIC_AUTHORIZATION_FAILED 错误
  val unauthorizedForReadPartitionData = unauthorizedForReadRequestInfo.map {
    case (tp, _) => (tp, new FetchResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED.code, -1, MemoryRecords.EMPTY))
  }

  // the callback for sending a fetch response
  def sendResponseCallback(responsePartitionData: Seq[(TopicPartition, FetchPartitionData)]) {
    ....
    def fetchResponseCallback(delayTimeMs: Int) {
      trace(s"Sending fetch response to client $clientId of " +
        s"${convertedPartitionData.map { case (_, v) => v.records.sizeInBytes }.sum} bytes")
      val fetchResponse = if (delayTimeMs > 0) new FetchResponse(versionId, fetchedPartitionData, delayTimeMs) else response
      requestChannel.sendResponse(new RequestChannel.Response(request, fetchResponse))
    }

    // When this callback is triggered, the remote API call has completed
    request.apiRemoteCompleteTimeMs = time.milliseconds

    //note: 配额情况的处理
    if (fetchRequest.isFromFollower) {
      // We've already evaluated against the quota and are good to go. Just need to record it now.
      val responseSize = sizeOfThrottledPartitions(versionId, fetchRequest, mergedPartitionData, quotas.leader)
      quotas.leader.record(responseSize)
      fetchResponseCallback(0)
    } else {
      quotas.fetch.recordAndMaybeThrottle(request.session.sanitizedUser, clientId, response.sizeOf, fetchResponseCallback)
    }
  }

  if (authorizedRequestInfo.isEmpty)
    sendResponseCallback(Seq.empty)
  else {
    // call the replica manager to fetch messages from the local replica
    //note: 从 replica 上拉取数据,满足条件后调用回调函数进行返回
    replicaManager.fetchMessages(
      fetchRequest.maxWait.toLong, //note: 拉取请求最长的等待时间
      fetchRequest.replicaId, //note: Replica 编号，Consumer 的为 -1
      fetchRequest.minBytes, //note: 拉取请求设置的最小拉取字节
      fetchRequest.maxBytes, //note: 拉取请求设置的最大拉取字节
      versionId <= 2,
      authorizedRequestInfo,
      replicationQuota(fetchRequest),
      sendResponseCallback)
  }
}
```
 Fetch 请求处理的真正实现是在 replicaManager 的 fetchMessages() 方法中，在这里，可以看出，无论是 Fetch 请求还是 Produce 请求，都是通过副本管理器来实现的，副本管理器（ReplicaManager）管理的对象是分区实例（Partition），而每个分区都会与相应的副本实例对应（Replica），在这个节点上的副本又会与唯一的 Log 实例对应，正如流程图的上半部分一样，Server 就是通过这几部分抽象概念来管理真正存储层的内容。   
 ### ReplicaManager 如何处理 Fetch 请求 
 ReplicaManger 处理 Fetch 请求的入口在 fetchMessages() 方法。   
 #### fetchMessages 
 fetchMessages() 方法的具体如下：   
``` scala
/**
 * Fetch messages from the leader replica, and wait until enough data can be fetched and return;
 * the callback function will be triggered either when timeout or required fetch info is satisfied
 */
//note: 从 leader 拉取数据,等待拉取到足够的数据或者达到 timeout 时间后返回拉取的结果
def fetchMessages(timeout: Long,
                  replicaId: Int,
                  fetchMinBytes: Int,
                  fetchMaxBytes: Int,
                  hardMaxBytesLimit: Boolean,
                  fetchInfos: Seq[(TopicPartition, PartitionData)],
                  quota: ReplicaQuota = UnboundedQuota,
                  responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit) {
  val isFromFollower = replicaId >= 0 //note: 判断请求是来自 consumer （这个值为 -1）还是副本同步
  //note: 默认都是从 leader 拉取，推测这个值只是为了后续能从 follower 消费数据而设置的
  val fetchOnlyFromLeader: Boolean = replicaId != Request.DebuggingConsumerId
  //note: 如果拉取请求来自 consumer（true）,只拉取 HW 以内的数据,如果是来自 Replica 同步,则没有该限制（false）。
  val fetchOnlyCommitted: Boolean = ! Request.isValidBrokerId(replicaId)

  // read from local logs
  //note：获取本地日志
  val logReadResults = readFromLocalLog(
    replicaId = replicaId,
    fetchOnlyFromLeader = fetchOnlyFromLeader,
    readOnlyCommitted = fetchOnlyCommitted,
    fetchMaxBytes = fetchMaxBytes,
    hardMaxBytesLimit = hardMaxBytesLimit,
    readPartitionInfo = fetchInfos,
    quota = quota)

  // if the fetch comes from the follower,
  // update its corresponding log end offset
  //note: 如果 fetch 来自 broker 的副本同步,那么就更新相关的 log end offset
  if(Request.isValidBrokerId(replicaId))
    updateFollowerLogReadResults(replicaId, logReadResults)

  // check if this fetch request can be satisfied right away
  val logReadResultValues = logReadResults.map { case (_, v) => v }
  val bytesReadable = logReadResultValues.map(_.info.records.sizeInBytes).sum
  val errorReadingData = logReadResultValues.foldLeft(false) ((errorIncurred, readResult) =>
    errorIncurred || (readResult.error != Errors.NONE))

  // respond immediately if 1) fetch request does not want to wait
  //                        2) fetch request does not require any data
  //                        3) has enough data to respond
  //                        4) some error happens while reading data
  //note: 如果满足以下条件的其中一个,将会立马返回结果:
  //note: 1. timeout 达到; 2. 拉取结果为空; 3. 拉取到足够的数据; 4. 拉取是遇到 error
  if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData) {
    val fetchPartitionData = logReadResults.map { case (tp, result) =>
      tp -> FetchPartitionData(result.error, result.hw, result.info.records)
    }
    responseCallback(fetchPartitionData)
  } else {
    //note： 其他情况下,延迟发送结果
    // construct the fetch results from the read results
    val fetchPartitionStatus = logReadResults.map { case (topicPartition, result) =>
      val fetchInfo = fetchInfos.collectFirst {
        case (tp, v) if tp == topicPartition => v
      }.getOrElse(sys.error(s"Partition $topicPartition not found in fetchInfos"))
      (topicPartition, FetchPartitionStatus(result.info.fetchOffsetMetadata, fetchInfo))
    }
    val fetchMetadata = FetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit, fetchOnlyFromLeader,
      fetchOnlyCommitted, isFromFollower, replicaId, fetchPartitionStatus)
    val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, responseCallback)

    // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
    val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => new TopicPartitionOperationKey(tp) }

    // try to complete the request immediately, otherwise put it into the purgatory;
    // this is because while the delayed fetch operation is being created, new requests
    // may arrive and hence make this operation completable.
    delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
  }
}
```
 整体来说，分为以下几步：    
 - <code>readFromLocalLog()</code>：调用该方法，从本地日志拉取相应的数据； 
 - 判断 Fetch 请求来源，如果来自副本同步，那么更新该副本的 the end offset 记录，如果该副本不在 isr 中，并判断是否需要更新 isr； 
 - 返回结果，满足条件的话立马返回，否则的话，通过延迟操作，延迟返回结果。  
 #### readFromLocalLog 
 readFromLocalLog() 方法的实现如下：   
``` scala
/**
 * Read from multiple topic partitions at the given offset up to maxSize bytes
 */
//note: 按 offset 从 tp 列表中读取相应的数据
def readFromLocalLog(replicaId: Int,
                     fetchOnlyFromLeader: Boolean,
                     readOnlyCommitted: Boolean,
                     fetchMaxBytes: Int,
                     hardMaxBytesLimit: Boolean,
                     readPartitionInfo: Seq[(TopicPartition, PartitionData)],
                     quota: ReplicaQuota): Seq[(TopicPartition, LogReadResult)] = {

  def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
    val offset = fetchInfo.offset
    val partitionFetchSize = fetchInfo.maxBytes

    BrokerTopicStats.getBrokerTopicStats(tp.topic).totalFetchRequestRate.mark()
    BrokerTopicStats.getBrokerAllTopicsStats().totalFetchRequestRate.mark()

    try {
      trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
        s"remaining response limit $limitBytes" +
        (if (minOneMessage) s", ignoring response/partition size limits" else ""))

      // decide whether to only fetch from leader
      //note: 根据决定 [是否只从 leader 读取数据] 来获取相应的副本
      //note: 根据 tp 获取 Partition 对象, 在获取相应的 Replica 对象
      val localReplica = if (fetchOnlyFromLeader)
        getLeaderReplicaIfLocal(tp)
      else
        getReplicaOrException(tp)

      // decide whether to only fetch committed data (i.e. messages below high watermark)
      //note: 获取 hw 位置，副本同步不设置这个值
      val maxOffsetOpt = if (readOnlyCommitted)
        Some(localReplica.highWatermark.messageOffset)
      else
        None

      /* Read the LogOffsetMetadata prior to performing the read from the log.
       * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not.
       * Using the log end offset after performing the read can lead to a race condition
       * where data gets appended to the log immediately after the replica has consumed from it
       * This can cause a replica to always be out of sync.
       */
      val initialLogEndOffset = localReplica.logEndOffset.messageOffset //note: the end offset
      val initialHighWatermark = localReplica.highWatermark.messageOffset //note: hw
      val fetchTimeMs = time.milliseconds
      val logReadInfo = localReplica.log match {
        case Some(log) =>
          val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)

          // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
          //note: 从指定的 offset 位置开始读取数据，副本同步不需要 maxOffsetOpt
          val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage)

          // If the partition is being throttled, simply return an empty set.
          if (shouldLeaderThrottle(quota, tp, replicaId)) //note: 如果被限速了,那么返回 空 集合
            FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
          // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
          // progress in such cases and don't need to report a `RecordTooLargeException`
          else if (!hardMaxBytesLimit && fetch.firstEntryIncomplete)
            FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
          else fetch

        case None =>
          error(s"Leader for partition $tp does not have a local log")
          FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY)
      }

      //note: 返回最后的结果,返回的都是 LogReadResult 对象
      LogReadResult(info = logReadInfo,
                    hw = initialHighWatermark,
                    leaderLogEndOffset = initialLogEndOffset,
                    fetchTimeMs = fetchTimeMs,
                    readSize = partitionFetchSize,
                    exception = None)
    } catch {
      // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
      // is supposed to indicate un-expected failure of a broker in handling a fetch request
      case e@ (_: UnknownTopicOrPartitionException |
               _: NotLeaderForPartitionException |
               _: ReplicaNotAvailableException |
               _: OffsetOutOfRangeException) =>
        LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                      hw = -1L,
                      leaderLogEndOffset = -1L,
                      fetchTimeMs = -1L,
                      readSize = partitionFetchSize,
                      exception = Some(e))
      case e: Throwable =>
        BrokerTopicStats.getBrokerTopicStats(tp.topic).failedFetchRequestRate.mark()
        BrokerTopicStats.getBrokerAllTopicsStats().failedFetchRequestRate.mark()
        error(s"Error processing fetch operation on partition $tp, offset $offset", e)
        LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                      hw = -1L,
                      leaderLogEndOffset = -1L,
                      fetchTimeMs = -1L,
                      readSize = partitionFetchSize,
                      exception = Some(e))
    }
  }

  var limitBytes = fetchMaxBytes
  val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
  var minOneMessage = !hardMaxBytesLimit
  readPartitionInfo.foreach { case (tp, fetchInfo) =>
    val readResult = read(tp, fetchInfo, limitBytes, minOneMessage) //note: 读取该 tp 的数据
    val messageSetSize = readResult.info.records.sizeInBytes
    // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
    if (messageSetSize > 0)
      minOneMessage = false
    limitBytes = math.max(0, limitBytes - messageSetSize)
    result += (tp -> readResult)
  }
  result
}
```
 readFromLocalLog() 方法的处理过程：    
 - 先根据要拉取的 topic-partition 获取对应的 Partition 对象，根据 Partition 对象获取对应的 Replica 对象； 
 - 根据 Replica 对象找到对应的 Log 对象，然后调用其 <code>read()</code> 方法从指定的位置读取数据。  
 ## 存储层对 Fetch 请求的处理 
 接着前面的流程开始往下走。   
 ### Log 对象 
 每个 Replica 会对应一个 log 对象，而每个 log 对象会管理相应的 LogSegment 实例。   
 #### read() 
 Log 对象的 read() 方法的实现如下所示：   
``` scala
//note: 从指定 offset 开始读取数据
def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None, minOneMessage: Boolean = false): FetchDataInfo = {
  trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))

  // Because we don't use lock for reading, the synchronization is a little bit tricky.
  // We create the local variables to avoid race conditions with updates to the log.
  val currentNextOffsetMetadata = nextOffsetMetadata
  val next = currentNextOffsetMetadata.messageOffset
  if(startOffset == next)
    return FetchDataInfo(currentNextOffsetMetadata, MemoryRecords.EMPTY)

  //note: 先查找对应的日志分段（segment）
  var entry = segments.floorEntry(startOffset)

  // attempt to read beyond the log end offset is an error
  if(startOffset > next || entry == null)
    throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, segments.firstKey, next))

  // Do the read on the segment with a base offset less than the target offset
  // but if that segment doesn't contain any messages with an offset greater than that
  // continue to read from successive segments until we get some messages or we reach the end of the log
  while(entry != null) {
    // If the fetch occurs on the active segment, there might be a race condition where two fetch requests occur after
    // the message is appended but before the nextOffsetMetadata is updated. In that case the second fetch may
    // cause OffsetOutOfRangeException. To solve that, we cap the reading up to exposed position instead of the log
    // end of the active segment.
    //note: 如果 Fetch 请求刚好发生在 the active segment 上,当多个 Fetch 请求同时处理,如果 nextOffsetMetadata 更新不及时,可能会导致
    //note: 发送 OffsetOutOfRangeException 异常; 为了解决这个问题, 这里能读取的最大位置是对应的物理位置（exposedPos）
    //note: 而不是 the log end of the active segment.
    val maxPosition = {
      if (entry == segments.lastEntry) {
        //note: nextOffsetMetadata 对应的实际物理位置
        val exposedPos = nextOffsetMetadata.relativePositionInSegment.toLong
        // Check the segment again in case a new segment has just rolled out.
        if (entry != segments.lastEntry) //note: 可能会有新的 segment 产生,所以需要再次判断
          // New log segment has rolled out, we can read up to the file end.
          entry.getValue.size
        else
          exposedPos
      } else {
        entry.getValue.size
      }
    }
    //note: 从 segment 中读取相应的数据
    val fetchInfo = entry.getValue.read(startOffset, maxOffset, maxLength, maxPosition, minOneMessage)
    if(fetchInfo == null) { //note: 如果该日志分段没有读取到数据,则读取更高的日志分段
      entry = segments.higherEntry(entry.getKey)
    } else {
      return fetchInfo
    }
  }

  // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
  // this can happen when all messages with offset larger than start offsets have been deleted.
  // In this case, we will return the empty set with log end offset metadata
  FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
}
```
 从实现可以看出，该方法会先查找对应的 Segment 对象（日志分段），然后循环直到读取到数据结束，如果当前的日志分段没有读取到相应的数据，那么会更新日志分段及对应的最大位置。   日志分段实际上是逻辑概念，它管理了物理概念的一个数据文件、一个时间索引文件和一个 offset 索引文件，读取日志分段时，会先读取 offset 索引文件再读取数据文件，具体步骤如下：    
 - 根据要读取的起始偏移量（startOffset）读取 offset 索引文件中对应的物理位置； 
 - 查找 offset 索引文件最后返回：起始偏移量对应的最近物理位置（startPosition）； 
 - 根据 startPosition 直接定位到数据文件，然后读取数据文件内容； 
 - 最多能读到数据文件的结束位置（maxPosition）。  
 ### LogSegment 
 关乎 数据文件、offset 索引文件和时间索引文件真正的操作都是在 LogSegment 对象中的，日志读取也与这个方法息息相关。   
 #### read() 
 read() 方法的实现如下：   
``` scala
//note: 读取日志分段（副本同步不会设置 maxSize）
def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int, maxPosition: Long = size,
         minOneMessage: Boolean = false): FetchDataInfo = {
  if (maxSize < 0)
    throw new IllegalArgumentException("Invalid max size for log read (%d)".format(maxSize))

  //note: log 文件物理长度
  val logSize = log.sizeInBytes // this may change, need to save a consistent copy
  //note: 将起始的 offset 转换为起始的实际物理位置
  val startOffsetAndSize = translateOffset(startOffset)

  // if the start position is already off the end of the log, return null
  if (startOffsetAndSize == null)
    return null

  val startPosition = startOffsetAndSize.position.toInt
  val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

  val adjustedMaxSize =
    if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
    else maxSize

  // return a log segment but with zero size in the case below
  if (adjustedMaxSize == 0)
    return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

  // calculate the length of the message set to read based on whether or not they gave us a maxOffset
  //note: 计算读取的长度
  val length = maxOffset match {
    //note: 副本同步时的计算方式
    case None =>
      // no max offset, just read until the max position
      min((maxPosition - startPosition).toInt, adjustedMaxSize) //note: 直接读取到最大的位置
    //note: consumer 拉取时,计算方式
    case Some(offset) =>
      // there is a max offset, translate it to a file position and use that to calculate the max read size;
      // when the leader of a partition changes, it's possible for the new leader's high watermark to be less than the
      // true high watermark in the previous leader for a short window. In this window, if a consumer fetches on an
      // offset between new leader's high watermark and the log end offset, we want to return an empty response.
      if (offset < startOffset)
        return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY, firstEntryIncomplete = false)
      val mapping = translateOffset(offset, startPosition)
      val endPosition =
        if (mapping == null)
          logSize // the max offset is off the end of the log, use the end of the file
        else
          mapping.position
      min(min(maxPosition, endPosition) - startPosition, adjustedMaxSize).toInt
  }

  //note: 根据起始的物理位置和读取长度读取数据文件
  FetchDataInfo(offsetMetadata, log.read(startPosition, length),
    firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
}
```
 从上面的实现来看，上述过程分为以下三部分：    
 - 根据 startOffset 得到实际的物理位置（<code>translateOffset()</code>）； 
 - 计算要读取的实际物理长度； 
 - 根据实际起始物理位置和要读取实际物理长度读取数据文件。  
 #### translateOffset() 
 translateOffset() 方法的实现过程主要分为两部分：    
 - 查找 offset 索引文件：调用 offset 索引文件的 <code>lookup()</code> 查找方法，获取离 startOffset 最接近的物理位置； 
 - 调用数据文件的 <code>searchFor()</code> 方法，从指定的物理位置开始读取每条数据，知道找到对应 offset 的物理位置。  
``` scala
private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogEntryPosition = {
  //note: 获取离 offset 最新的物理位置,返回包括 offset 和物理位置（不是准确值）
  val mapping = index.lookup(offset)
  //note: 从指定的位置开始消费,直到找到 offset 对应的实际物理位置,返回包括 offset 和物理位置（准确值）
  log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))
}
```
 
 ##### 查找 offset 索引文件 
 offset 索引文件是使用内存映射（不了解的，可以阅读 [操作系统之共享对象学习](http://matt33.com/2018/02/04/linux-mmap/)）的方式加载到内存中的，在查询的过程中，内存映射是会发生变化，所以在 lookup() 中先拷贝出来了一个（idx），然后再进行查询，具体实现如下：   
``` scala
//note: 查找小于等于指定 offset 的最大 offset,并且返回对应的 offset 和实际物理位置
def lookup(targetOffset: Long): OffsetPosition = {
  maybeLock(lock) {
    val idx = mmap.duplicate //note: 查询时,mmap 会发生变化,先复制出来一个
    val slot = indexSlotFor(idx, targetOffset, IndexSearchType.KEY) //note: 二分查找
    if(slot == -1)
      OffsetPosition(baseOffset, 0)
    else
      //note: 先计算绝对偏移量,再计算物理位置
      parseEntry(idx, slot).asInstanceOf[OffsetPosition]
  }
}

override def parseEntry(buffer: ByteBuffer, n: Int): IndexEntry = {
    OffsetPosition(baseOffset + relativeOffset(buffer, n), physical(buffer, n))
}

private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize)

private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 4)
```
 关于 relativeOffset 和 physical 的计算方法，可以参考下面这张图（来自《Kafka 计算内幕》）：   
![根据索引条目编号查找偏移量的值和物理位置的值](./images/kafka/offset-physical.png)
   
 ##### 搜索数据文件获取准确的物理位置 
 前面通过 offset 索引文件获取的物理位置是一个接近值，下面通过实际读取数据文件将会得到一个真正的准确值，它是通过遍历数据文件实现的。   
``` scala
/**
 * Search forward for the file position of the last offset that is greater than or equal to the target offset
 * and return its physical position and the size of the message (including log overhead) at the returned offset. If
 * no such offsets are found, return null.
 *
 * @param targetOffset The offset to search for.
 * @param startingPosition The starting position in the file to begin searching from.
 */
public LogEntryPosition searchForOffsetWithSize(long targetOffset, int startingPosition) {
    for (FileChannelLogEntry entry : shallowEntriesFrom(startingPosition)) {
        long offset = entry.offset();
        if (offset >= targetOffset)
            return new LogEntryPosition(offset, entry.position(), entry.sizeInBytes());
    }
    return null;
}
```
 到这里，一个 Fetch 请求的处理过程算是完成了。