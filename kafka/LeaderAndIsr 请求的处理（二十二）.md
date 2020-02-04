本篇算是 Controller 部分的最后一篇，在前面讲述 ReplicaManager 时，留一个地方没有讲解，是关于 Broker 对 Controller 发送的 LeaderAndIsr 请求的处理，这个请求的处理实现会稍微复杂一些，本篇文章主要就是讲述 Kafka Server 是如何处理 LeaderAndIsr 请求的。   
 ## LeaderAndIsr 请求 
 LeaderAndIsr 请求是在一个 Topic Partition 的 leader、isr、assignment replicas 变动时，Controller 向 Broker 发送的一种请求，有时候是向这个 Topic Partition 的所有副本发送，有时候是其中的某个副本，跟具体的触发情况有关系。在一个 LeaderAndIsr 请求中，会封装多个 Topic Partition 的信息，每个 Topic Partition 会对应一个 PartitionState 对象，这个对象主要成员变量如下：   
``` scala
public class PartitionState {
    public final int controllerEpoch;
    public final int leader;
    public final int leaderEpoch;
    public final List<Integer> isr;
    public final int zkVersion;
    public final Set<Integer> replicas;
}
```
 由此可见，在 LeaderAndIsr 请求中，会包含一个 Partition 的以下信息：    
 - 当前 Controller 的 epoch（Broker 收到这个请求后，如果发现是过期的 Controller 请求，就会拒绝这个请求）； 
 - leader，Partition 的 leader 信息； 
 - leader epoch，Partition leader epoch 信息（leader、isr、AR 变动时，这个 epoch 都会加1）； 
 - isr 列表； 
 - zkVersion，； 
 - AR，所有的 replica 列表。  
 ### LeaderAndIsr 请求处理 
 
 ### 处理整体流程 
 LeaderAndIsr 请求可谓是包含了一个 Partition 的所有 metadata 信息，Server 在接收到 Controller 发送的这个请求后，其处理的逻辑如下：   
``` scala
//KafkaApis
//note: LeaderAndIsr 请求的处理
def handleLeaderAndIsrRequest(request: RequestChannel.Request) {
  // ensureTopicExists is only for client facing requests
  // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
  // stop serving data to clients for the topic being deleted
  val correlationId = request.header.correlationId
  val leaderAndIsrRequest = request.body.asInstanceOf[LeaderAndIsrRequest]

  try {
    def onLeadershipChange(updatedLeaders: Iterable[Partition], updatedFollowers: Iterable[Partition]) {
      // for each new leader or follower, call coordinator to handle consumer group migration.
      // this callback is invoked under the replica state change lock to ensure proper order of
      // leadership changes
      //note: __consumer_offset 是 leader 的情况，读取相应 group 的 offset 信息
      updatedLeaders.foreach { partition =>
        if (partition.topic == Topic.GroupMetadataTopicName)
          coordinator.handleGroupImmigration(partition.partitionId)
      }
      //note: __consumer_offset 是 follower 的情况，如果之前是 leader，那么移除这个 partition 对应的信息
      updatedFollowers.foreach { partition =>
        if (partition.topic == Topic.GroupMetadataTopicName)
          coordinator.handleGroupEmigration(partition.partitionId)
      }
    }

    val leaderAndIsrResponse =
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {//note: 有权限的情况下
        //note: replicaManager 进行相应的处理
        val result = replicaManager.becomeLeaderOrFollower(correlationId, leaderAndIsrRequest, metadataCache, onLeadershipChange)
        new LeaderAndIsrResponse(result.errorCode, result.responseMap.mapValues(new JShort(_)).asJava)
      } else {
        val result = leaderAndIsrRequest.partitionStates.asScala.keys.map((_, new JShort(Errors.CLUSTER_AUTHORIZATION_FAILED.code))).toMap
        new LeaderAndIsrResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.code, result.asJava)
      }

    requestChannel.sendResponse(new Response(request, leaderAndIsrResponse))
  } catch {
    case e: KafkaStorageException =>
      fatal("Disk error during leadership change.", e)
      Runtime.getRuntime.halt(1)
  }
}
```
 上述处理逻辑分为以下两步：    
 - ReplicaManager 调用 <code>becomeLeaderOrFollower()</code> 方法对这个请求进行相应的处理； 
 - 如果请求中包含 <code>__consumer_offset</code> 的 Partition（对应两种情况：之前是 fllower 现在变成了 leader、之前是 leader 现在变成了 follower），那么还需要调用这个方法中定义的 <code>onLeadershipChange()</code> 方法进行相应的处理。  becomeLeaderOrFollower() 的整体处理流程如下：   
![LeaderAndIsr 请求的处理](./images/kafka/leader-and-isr.png)
   
 ### becomeLeaderOrFollower 
 这里先看下 ReplicaManager 的 becomeLeaderOrFollower() 方法，它是 LeaderAndIsr 请求处理的实现，如下所示：   
``` scala
//note: 处理 LeaderAndIsr 请求
def becomeLeaderOrFollower(correlationId: Int,leaderAndISRRequest: LeaderAndIsrRequest,
                           metadataCache: MetadataCache,
                           onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): BecomeLeaderOrFollowerResult = {
  leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
    stateChangeLogger.trace("Broker %d received LeaderAndIsr request %s correlation id %d from controller %d epoch %d for partition [%s,%d]"
                              .format(localBrokerId, stateInfo, correlationId,
                                      leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch, topicPartition.topic, topicPartition.partition))
  }
  replicaStateChangeLock synchronized {
    val responseMap = new mutable.HashMap[TopicPartition, Short]
    //note: 1. 验证 controller 的 epoch，如果是来自旧的 controller，就拒绝这个请求
    if (leaderAndISRRequest.controllerEpoch < controllerEpoch) {
      stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d since " +
        "its controller epoch %d is old. Latest known controller epoch is %d").format(localBrokerId, leaderAndISRRequest.controllerId,
        correlationId, leaderAndISRRequest.controllerEpoch, controllerEpoch))
      BecomeLeaderOrFollowerResult(responseMap, Errors.STALE_CONTROLLER_EPOCH.code)
    } else { //note: 当前 controller 的请求
      val controllerId = leaderAndISRRequest.controllerId
      controllerEpoch = leaderAndISRRequest.controllerEpoch

      // First check partition's leader epoch
      //note: 2. 检查 leader epoch，得到一个 partitionState map，epoch 满足条件并且有副本在本地的集合
      val partitionState = new mutable.HashMap[Partition, PartitionState]()
      leaderAndISRRequest.partitionStates.asScala.foreach { case (topicPartition, stateInfo) =>
        val partition = getOrCreatePartition(topicPartition) //note: 对应的 tp 如果没有 Partition 实例的话,就新建一个
        val partitionLeaderEpoch = partition.getLeaderEpoch //note: 更新 leader epoch
        // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
        // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
        if (partitionLeaderEpoch < stateInfo.leaderEpoch) {
          if(stateInfo.replicas.contains(localBrokerId))
            partitionState.put(partition, stateInfo)  //note: 更新 replica 的 stateInfo
          else {
            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
              "epoch %d for partition [%s,%d] as itself is not in assigned replica list %s")
              .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
                topicPartition.topic, topicPartition.partition, stateInfo.replicas.asScala.mkString(",")))
            responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
          }
        } else {  //note: 忽略这个请求，因为请求的 leader epoch 小于缓存的 epoch
          // Otherwise record the error code in response
          stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +
            "epoch %d for partition [%s,%d] since its associated leader epoch %d is not higher than the current leader epoch %d")
            .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
              topicPartition.topic, topicPartition.partition, stateInfo.leaderEpoch, partitionLeaderEpoch))
          responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH.code)
        }
      }

      //note: 3. 过滤出本地副本设置为 leader 的 Partition 列表
      val partitionsTobeLeader = partitionState.filter { case (_, stateInfo) =>
        stateInfo.leader == localBrokerId
      }
      //note: 4. 过滤出本地副本设置为 follower 的 Partition 列表
      val partitionsToBeFollower = partitionState -- partitionsTobeLeader.keys //note: 这些 tp 设置为了 follower

      //note: 5. 将为 leader 的副本设置为 leader
      val partitionsBecomeLeader = if (partitionsTobeLeader.nonEmpty)
        makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, correlationId, responseMap)
      else
        Set.empty[Partition]

      //note: 6. 将为 follower 的副本设置为 follower
      val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
        makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap, metadataCache)
      else
        Set.empty[Partition]

      //note: 7. 如果 hw checkpoint 的线程没有初始化，这里需要进行一次初始化
      // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
      // have been completely populated before starting the checkpointing there by avoiding weird race conditions
      if (!hwThreadInitialized) {
        startHighWaterMarksCheckPointThread()
        hwThreadInitialized = true
      }
      //note: 8. 检查 replica fetcher 是否需要关闭（有些副本需要关闭因为可能从 follower 变为 leader）
      replicaFetcherManager.shutdownIdleFetcherThreads()

      //note: 9. 检查是否 __consumer_offset 的 Partition 的 leaderAndIsr 信息，有的话进行相应的操作
      onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
      BecomeLeaderOrFollowerResult(responseMap, Errors.NONE.code)
    }
  }
}
```
 上述实现，其处理逻辑总结如下：    
 - 检查 Controller 的 epoch，如果是来自旧的 Controller，那么就拒绝这个请求； 
 - 获取请求的 Partition 列表的 PartitionState 信息，在遍历的过程中，会进行一个检查，如果 leader epoch 小于缓存中的 epoch 值，那么就过滤掉这个 Partition 信息，如果这个 Partition 在本地不存在，那么会初始化这个 Partition 的对象（这时候并不会初始化本地副本）； 
 - 获取出本地副本为 leader 的 Partition 列表（partitionsTobeLeader）； 
 - 获取出本地副本为 follower 的 Partition 列表（partitionsToBeFollower）； 
 - 调用 <code>makeLeaders()</code> 方法将 leader 的副本设置为 leader； 
 - 调用 <code>makeFollowers()</code> 方法将 leader 的副本设置为 follower； 
 - 检查 HW checkpoint 的线程是否初始化，如果没有，这里需要进行一次初始化； 
 - 检查 ReplicaFetcherManager 是否有线程需要关闭（如果这个线程上没有分配要拉取的 Topic Partition，那么在这里这个线程就会被关闭，下次需要时会再次启动）； 
 - 检查是否有 <code>__consumer_offset</code> Partition 的 leaderAndIsr 信息，有的话进行相应的操作。  这其中，比较复杂的部分是第 5、6、9步，也前面图中标出的 1、2、4步，文章下面接着分析这三部分。   
 ### makeLeaders 
 ReplicaManager 的 makeLeaders() 的作用是将指定的这批 Partition 列表设置为 Leader，并返回是新 leader 对应的 Partition 列表（之前不是 leader，现在选举为了 leader），其实实现如下：   
``` scala
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
    //note: 1. 停止这些副本同步请求
    replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(_.topicPartition))
    // Update the partition information to be the leader
    //note: 2. 更新这些 partition 的信息（这些 partition 成为 leader 了）
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
 实现逻辑如下：    
 - 调用 ReplicaFetcherManager 的 <code>removeFetcherForPartitions()</code> 方法移除这些 Partition 的副本同步线程； 
 - 遍历这些 Partition，通过 Partition 的 <code>makeLeader()</code> 方法将这个 Partition 设置为 Leader，如果设置成功（如果 leader 没有变化，证明这个 Partition 之前就是 leader，这个方法返回的是 false，这种情况下不会更新到缓存中），那么将 leader 信息更新到缓存中。  下面来看下在 Partition 中是如何真正初始化一个 Partition 的 leader？其实现如下：   
``` scala
//note: 将本地副本设置为 leader, 如果 leader 不变,向 ReplicaManager 返回 false
def makeLeader(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
  val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
    val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
    // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
    // to maintain the decision maker controller's epoch in the zookeeper path
    controllerEpoch = partitionStateInfo.controllerEpoch
    // add replicas that are new
    //note: 为了新的 replica 创建副本实例
    allReplicas.foreach(replica => getOrCreateReplica(replica))
    //note: 获取新的 isr 列表
    val newInSyncReplicas = partitionStateInfo.isr.asScala.map(r => getOrCreateReplica(r)).toSet
    // remove assigned replicas that have been removed by the controller
    //note: 将已经在不在 AR 中的副本移除
    (assignedReplicas.map(_.brokerId) -- allReplicas).foreach(removeReplica)
    inSyncReplicas = newInSyncReplicas
    leaderEpoch = partitionStateInfo.leaderEpoch
    zkVersion = partitionStateInfo.zkVersion
    //note: 判断是否是新的 leader
    val isNewLeader =
      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == localBrokerId) {//note: leader 没有更新
        false
      } else {
        leaderReplicaIdOpt = Some(localBrokerId)
        true
      }
    val leaderReplica = getReplica().get //note: 获取在当前上的副本,也就是 leader replica
    val curLeaderLogEndOffset = leaderReplica.logEndOffset.messageOffset //note: 获取 leader replica 的 the end offset
    val curTimeMs = time.milliseconds
    // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
    (assignedReplicas - leaderReplica).foreach { replica => //note: 对于 isr 中的 replica,更新 LastCaughtUpTime
      val lastCaughtUpTimeMs = if (inSyncReplicas.contains(replica)) curTimeMs else 0L
      replica.resetLastCaughtUpTime(curLeaderLogEndOffset, curTimeMs, lastCaughtUpTimeMs)
    }
    // we may need to increment high watermark since ISR could be down to 1
    if (isNewLeader) {  //note: 如果是新的 leader,那么需要
      // construct the high watermark metadata for the new leader replica
      //note: 为新的 leader 构造 replica 的 HW metadata
      leaderReplica.convertHWToLocalOffsetMetadata()
      // reset log end offset for remote replicas
      //note: 更新远程副本的副本同步信息（设置为 unKnown）
      assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
    }
    //note: 如果满足更新 isr 的条件,就更新 HW 信息
    (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
  }
  // some delayed operations may be unblocked after HW changed
  if (leaderHWIncremented) //note: HW 更新的情况下
    tryCompleteDelayedRequests()
  isNewLeader
}
```
 简单总结一下上述的实现：    
 - 首先更新这个 Partition 的相应信息，包括：isr、AR、leader epoch、zkVersion 等，并为每个副本创建一个 Replica 对象（如果不存在该对象的情况下才会创建，只有本地副本才会初始化相应的日志对象）； 
 - 如果这个 Partition 的 leader 本来就是本地副本，那么返回的结果设置为 false，证明这个 leader 并不是新的 leader； 
 - 对于 isr 中的所有 Replica，更新 LastCaughtUpTime 值，即最近一次赶得上 leader 的时间； 
 - 如果是新的 leader，那么为 leader 初始化相应的 HighWatermarkMetadata 对象，并将所有副本的副本同步信息更新为 UnknownLogReadResult； 
 - 检查一下是否需要更新 HW 值。  如果这个本地副本是新选举的 leader，那么它所做的事情就是初始化 Leader 应该记录的相关信息。   
 ### makeFollowers 
 ReplicaManager 的 makeFollowers() 方法，是将哪些 Partition 设置为 Follower，返回的结果是那些新的 follower 对应的 Partition 列表（之前是 leader，现在变成了 follower），其实现如下：   
``` scala
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

  //note: 1. 统计 follower 的集合
  val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()

  try {

    // TODO: Delete leaders from LeaderAndIsrRequest
    partitionState.foreach{ case (partition, partitionStateInfo) =>
      val newLeaderBrokerId = partitionStateInfo.leader
      metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match { //note: leader 是可用的 Partition
        // Only change partition state when the leader is available
        case Some(_) => //note: 2. 将 Partition 的本地副本设置为 follower
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

    //note: 3. 移除这些 Partition 的副本同步线程,这样在 MakeFollower 期间,这些 Partition 就不会进行副本同步了
    replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
    partitionsToMakeFollower.foreach { partition =>
      stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +
        "%d epoch %d with correlation id %d for partition %s")
        .format(localBrokerId, controllerId, epoch, correlationId, partition.topicPartition))
    }

    //note: 4. Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
    logManager.truncateTo(partitionsToMakeFollower.map { partition =>
      (partition.topicPartition, partition.getOrCreateReplica().highWatermark.messageOffset)
    }.toMap)
    //note: 5. 完成那些延迟请求的处理（Produce 和 FetchConsumer 请求）
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
      //note: 6. 启动副本同步线程
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

//note: 遍历所有的 partition 对象,检查其 isr 是否需要抖动
private def maybeShrinkIsr(): Unit = {
  trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")
  allPartitions.values.foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs))
}

private def updateFollowerLogReadResults(replicaId: Int, readResults: Seq[(TopicPartition, LogReadResult)]) {
  debug("Recording follower broker %d log read results: %s ".format(replicaId, readResults))
  readResults.foreach { case (topicPartition, readResult) =>
    getPartition(topicPartition) match {
      case Some(partition) =>
        //note: 更新副本的相关信息
        partition.updateReplicaLogReadResult(replicaId, readResult)

        // for producer requests with ack > 1, we need to check
        // if they can be unblocked after some follower's log end offsets have moved
        tryCompleteDelayedProduce(new TopicPartitionOperationKey(topicPartition))
      case None =>
        warn("While recording the replica LEO, the partition %s hasn't been created.".format(topicPartition))
    }
  }
}
```
 简单总结一下上述的逻辑过程：    
 - 首先遍历所有的 Partition，获到那些 leader 可用、并且 Partition 可以成功设置为 Follower 的 Partition 列表（partitionsToMakeFollower）； 
 - 在上面遍历的过程中，会调用 Partition 的 <code>makeFollower()</code> 方法将 Partition 设置为 Follower（在这里，如果该 Partition 的本地副本不存在，会初始化相应的日志对象，如果该 Partition 的 leader 已经存在，并且没有变化，那么就返回 false，只有 leader 变化的 Partition，才会返回 true，才会加入到 partitionsToMakeFollower 集合中，这是因为 leader 没有变化的 Partition 是不需要变更副本同步线程的）； 
 - 移除这些 Partition 的副本同步线程，这样在 MakeFollower 期间，这些 Partition 就不会进行副本同步了； 
 - Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset，因为前面已经移除了这个 Partition 的副本同步线程，所以这里在 checkpoint 后可以保证所有缓存的数据都可以刷新到磁盘； 
 - 完成那些延迟请求的处理（Produce 和 FetchConsumer 请求）； 
 - 启动相应的副本同步线程。  到这里 LeaderAndIsr 请求的大部分处理已经完成，但是有一个比较特殊的 topic（__consumer_offset），如果这 Partition 的 leader 发生变化，是需要一些额外的处理。   
 ## <code>__consumer_offset</code> leader 切换处理 
 __consumer_offset 这个 Topic 如果发生了 leader 切换，GroupCoordinator 需要进行相应的处理，其处理过程如下：   
``` scala
def onLeadershipChange(updatedLeaders: Iterable[Partition], updatedFollowers: Iterable[Partition]) {
  // for each new leader or follower, call coordinator to handle consumer group migration.
  // this callback is invoked under the replica state change lock to ensure proper order of
  // leadership changes
  //note: __consumer_offset 是 leader 的情况，读取相应 group 的 offset 信息
  updatedLeaders.foreach { partition =>
    if (partition.topic == Topic.GroupMetadataTopicName)
      coordinator.handleGroupImmigration(partition.partitionId)
  }
  //note: __consumer_offset 是 follower 的情况，如果之前是 leader，那么移除这个 partition 对应的信息
  updatedFollowers.foreach { partition =>
    if (partition.topic == Topic.GroupMetadataTopicName)
      coordinator.handleGroupEmigration(partition.partitionId)
  }
}
```
 
 ### 成为 leader 
 如果当前节点这个 __consumer_offset 有 Partition 成为 leader，GroupCoordinator 通过 handleGroupImmigration() 方法进行相应的处理。   
``` scala
//note: 加载这个 Partition 对应的 group offset 信息
def handleGroupImmigration(offsetTopicPartitionId: Int) {
  groupManager.loadGroupsForPartition(offsetTopicPartitionId, onGroupLoaded)
}

//note: 异步地加载这个 offset Partition 的信息
def loadGroupsForPartition(offsetsPartition: Int, onGroupLoaded: GroupMetadata => Unit) {
  val topicPartition = new TopicPartition(Topic.GroupMetadataTopicName, offsetsPartition)

  def doLoadGroupsAndOffsets() {
    info(s"Loading offsets and group metadata from $topicPartition")

    //note: 添加到  loadingPartitions 集合中
    inLock(partitionLock) {
      if (loadingPartitions.contains(offsetsPartition)) {
        info(s"Offset load from $topicPartition already in progress.")
        return
      } else {
        loadingPartitions.add(offsetsPartition)
      }
    }

    //note: 开始加载，加载成功的话，将该 Partition 从 loadingPartitions 集合中移除，添加到 ownedPartition 集合中
    try {
      loadGroupsAndOffsets(topicPartition, onGroupLoaded)
    } catch {
      case t: Throwable => error(s"Error loading offsets from $topicPartition", t)
    } finally {
      inLock(partitionLock) {
        ownedPartitions.add(offsetsPartition)
        loadingPartitions.remove(offsetsPartition)
      }
    }
  }

  scheduler.schedule(topicPartition.toString, doLoadGroupsAndOffsets)
}
```
 这个方法的做的事情是：    
 - 将正在处理的 Partition 添加到 loadingPartitions 集合中，这个集合内都是当前正在加载的 Partition（特指 <code>__consumer_offset</code> Topic）； 
 - 通过 <code>loadGroupsAndOffsets()</code> 加载这个 Partition 的数据，处理完成后，该 Partition 从 loadingPartitions 中清除，并添加到 ownedPartitions 集合中。  loadGroupsAndOffsets() 的实现如下：   
``` scala
//note: 读取该 group offset Partition 数据
private[coordinator] def loadGroupsAndOffsets(topicPartition: TopicPartition, onGroupLoaded: GroupMetadata => Unit) {
  //note: 这个必然有本地副本，现获取 hw（如果本地是 leader 的情况，否则返回-1）
  def highWaterMark = replicaManager.getHighWatermark(topicPartition).getOrElse(-1L)

  val startMs = time.milliseconds()
  replicaManager.getLog(topicPartition) match {
    case None =>
      warn(s"Attempted to load offsets and group metadata from $topicPartition, but found no log")

    case Some(log) =>
      var currOffset = log.logStartOffset //note: 这副本最起始的 offset
      val buffer = ByteBuffer.allocate(config.loadBufferSize) //note: 默认5MB
      // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
      //note: group 与 offset 的对应关系
      val loadedOffsets = mutable.Map[GroupTopicPartition, OffsetAndMetadata]()
      val removedOffsets = mutable.Set[GroupTopicPartition]()
      //note: Group 对应的 meta 信息
      val loadedGroups = mutable.Map[String, GroupMetadata]()
      val removedGroups = mutable.Set[String]()

      while (currOffset < highWaterMark && !shuttingDown.get()) { //note: 直到读取到 hw 位置，或服务关闭
        buffer.clear()
        val fileRecords = log.read(currOffset, config.loadBufferSize, maxOffset = None, minOneMessage = true)
          .records.asInstanceOf[FileRecords]
        val bufferRead = fileRecords.readInto(buffer, 0)

        MemoryRecords.readableRecords(bufferRead).deepEntries.asScala.foreach { entry =>
          val record = entry.record
          require(record.hasKey, "Group metadata/offset entry key should not be null")

          GroupMetadataManager.readMessageKey(record.key) match {
            case offsetKey: OffsetKey => //note: GroupTopicPartition，有 group 和 topic-partition
              // load offset
              //note: 加载 offset 信息
              val key = offsetKey.key
              if (record.hasNullValue) { //note: value 为空
                loadedOffsets.remove(key)
                removedOffsets.add(key)
              } else { //note: 有 commit offset 信息
                val value = GroupMetadataManager.readOffsetMessageValue(record.value)
                loadedOffsets.put(key, value)
                removedOffsets.remove(key)
              }

            case groupMetadataKey: GroupMetadataKey =>
              // load group metadata
              //note: 加载 group metadata 信息
              val groupId = groupMetadataKey.key
              val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, record.value)
              if (groupMetadata != null) {
                trace(s"Loaded group metadata for group $groupId with generation ${groupMetadata.generationId}")
                removedGroups.remove(groupId)
                loadedGroups.put(groupId, groupMetadata)
              } else { //note: 更新最新的信息
                loadedGroups.remove(groupId)
                removedGroups.add(groupId)
              }

            case unknownKey =>
              throw new IllegalStateException(s"Unexpected message key $unknownKey while loading offsets and group metadata")
          }

          currOffset = entry.nextOffset
        }
      }

      val (groupOffsets, emptyGroupOffsets) = loadedOffsets
        .groupBy(_._1.group)
        .mapValues(_.map { case (groupTopicPartition, offset) => (groupTopicPartition.topicPartition, offset)} )
        .partition { case (group, _) => loadedGroups.contains(group) } //note: 把集合根据条件分两个部分

      loadedGroups.values.foreach { group =>
        val offsets = groupOffsets.getOrElse(group.groupId, Map.empty[TopicPartition, OffsetAndMetadata])
        loadGroup(group, offsets) //note: 在缓存中添加 group 和初始化 offset 信息
        onGroupLoaded(group) //note: 设置 group 下一次心跳超时时间
      }

      // load groups which store offsets in kafka, but which have no active members and thus no group
      // metadata stored in the log
      //note: 加载哪些有 offset 信息但是当前没有活跃的 member 信息的 group
      emptyGroupOffsets.foreach { case (groupId, offsets) =>
        val group = new GroupMetadata(groupId)
        loadGroup(group, offsets)
        onGroupLoaded(group)
      }

      removedGroups.foreach { groupId =>
        // if the cache already contains a group which should be removed, raise an error. Note that it
        // is possible (however unlikely) for a consumer group to be removed, and then to be used only for
        // offset storage (i.e. by "simple" consumers)
        if (groupMetadataCache.contains(groupId) && !emptyGroupOffsets.contains(groupId))
          throw new IllegalStateException(s"Unexpected unload of active group $groupId while " +
            s"loading partition $topicPartition")
      }

      if (!shuttingDown.get())
        info("Finished loading offsets from %s in %d milliseconds."
          .format(topicPartition, time.milliseconds() - startMs))
  }
}
```
 上面方法的实现虽然比较长，但是处理逻辑还是比较简单的，实现结果如下：    
 - 获取这个 Partition 的 HW 值（如果 leader 不在本地，那么返回-1）； 
 - 初始化 loadedOffsets 和 removedOffsets、loadedGroups 和 removedGroups 集合，它们就是 group offset 信息以及 consumer member 信息； 
 - 从这个 Partition 第一条数据开始读取，直到读取到 HW 位置，加载相应的 commit offset、consumer member 信息，因为是顺序读取的，所以会新的值会覆盖前面的值； 
 - 通过 <code>loadGroup()</code> 加载到 GroupCoordinator 的缓存中。  经过上面这些步骤，这个 Partition 的数据就被完整加载缓存中了。   
 ### 变成 follower 
 如果 __consumer_offset 有 Partition 变成了 follower（之前是 leader，如果之前不是 leader，不会走到这一步的），GroupCoordinator 通过 handleGroupEmigration() 移除这个 Partition 相应的缓存信息。   
``` scala
//note: 移除这个 Partition 对应的 group offset 信息
def handleGroupEmigration(offsetTopicPartitionId: Int) {
  groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
}
```
 removeGroupsForPartition() 的实现如下：   
``` scala
//note: 当一个 broker 变成一个 follower 时，清空这个 partition 的相关缓存信息
def removeGroupsForPartition(offsetsPartition: Int,
                             onGroupUnloaded: GroupMetadata => Unit) {
  val topicPartition = new TopicPartition(Topic.GroupMetadataTopicName, offsetsPartition)
  scheduler.schedule(topicPartition.toString, removeGroupsAndOffsets)

  def removeGroupsAndOffsets() {
    var numOffsetsRemoved = 0
    var numGroupsRemoved = 0

    inLock(partitionLock) {
      // we need to guard the group removal in cache in the loading partition lock
      // to prevent coordinator's check-and-get-group race condition
      ownedPartitions.remove(offsetsPartition)

      for (group <- groupMetadataCache.values) {
        if (partitionFor(group.groupId) == offsetsPartition) {
          onGroupUnloaded(group) //note: 将 group 状态转移成 dead
          groupMetadataCache.remove(group.groupId, group) //note: 清空 group 的信息
          numGroupsRemoved += 1
          numOffsetsRemoved += group.numOffsets
        }
      }
    }

    if (numOffsetsRemoved > 0)
      info(s"Removed $numOffsetsRemoved cached offsets for $topicPartition on follower transition.")

    if (numGroupsRemoved > 0)
      info(s"Removed $numGroupsRemoved cached groups for $topicPartition on follower transition.")
  }
}

private def onGroupUnloaded(group: GroupMetadata) {
  group synchronized {
    info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
    val previousState = group.currentState
    group.transitionTo(Dead) //note: 状态转移成 dead

    previousState match {
      case Empty | Dead =>
      case PreparingRebalance =>
        for (member <- group.allMemberMetadata) { //note: 如果有 member 信息返回异常
          if (member.awaitingJoinCallback != null) {
            member.awaitingJoinCallback(joinError(member.memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
            member.awaitingJoinCallback = null
          }
        }
        joinPurgatory.checkAndComplete(GroupKey(group.groupId))

      case Stable | AwaitingSync =>
        for (member <- group.allMemberMetadata) { //note: 如果有 member 信息，返回异常
          if (member.awaitingSyncCallback != null) {
            member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR_FOR_GROUP.code)
            member.awaitingSyncCallback = null
          }
          heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
        }
    }
  }
}
```
 对于在这个 Partition 上的所有 Group，会按下面的步骤执行：    
 - 通过 <code>onGroupUnloaded()</code> 方法先将这个 Group 的状态转换为 dead，如果 Group 处在 PreparingRebalance/Stable/AwaitingSync 状态，并且设置了相应的回调函数，那么就在回调函数中返回带有 NOT_COORDINATOR_FOR_GROUP 异常信息的响应，consumer 在收到这个异常信息会重新加入 group； 
 - 从缓存中移除这个 Group 的信息。  这个遍历执行完成之后，这个 Topic Partition 就从 Leader 变成了 follower 状态。