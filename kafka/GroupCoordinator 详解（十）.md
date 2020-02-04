突然发现距离上一篇文章，已经过去两个多月了，有两个月没有写博客了，之前定的是年前把这个系列写完，现在看来只能往后拖了，后面估计还有五篇文章左右，尽量在春节前完成吧。继续之前的内容开始讲解，这篇文章，主要是想把 GroupCoordinator 的内容总结一下，也算是开始了 Kafka Server 端的讲解，Kafka 的 Server 端主要有三块内容：GroupCoordinator、Controller 和 ReplicaManager，其中，GroupCoordinator 的内容是与 Consumer 端紧密结合在一起的，有一部分内容在前面已经断断续续介绍过，这里会做一个总结。   关于 GroupCoordinator，代码中有一段注释介绍得比较清晰，这里引用一下：   
<blockquote> GroupCoordinator handles general group membership and offset management.   Each Kafka server instantiates a coordinator which is responsible for a set of groups. Groups are assigned to coordinators based on their group names.   
</blockquote> 简单来说就是，GroupCoordinator 是负责进行 consumer 的 group 成员与 offset 管理（但每个 GroupCoordinator 只是管理一部分的 consumer group member 和 offset 信息），那它是怎么管理的呢？这个从 GroupCoordinator 处理的 client 端请求类型可以看出来，它处理的请求类型主要有以下几种：    
 - ApiKeys.OFFSET_COMMIT; 
 - ApiKeys.OFFSET_FETCH; 
 - ApiKeys.JOIN_GROUP; 
 - ApiKeys.LEAVE_GROUP; 
 - ApiKeys.SYNC_GROUP; 
 - ApiKeys.DESCRIBE_GROUPS; 
 - ApiKeys.LIST_GROUPS; 
 - ApiKeys.HEARTBEAT;  而 Kafka Server 端要处理的请求总共有以下 21 种，其中有 8 种是由 GroupCoordinator 来完成的。   
``` scala
ApiKeys.forId(request.requestId) match {
  case ApiKeys.PRODUCE => handleProducerRequest(request)
  case ApiKeys.FETCH => handleFetchRequest(request)
  case ApiKeys.LIST_OFFSETS => handleOffsetRequest(request)
  case ApiKeys.METADATA => handleTopicMetadataRequest(request)
  case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
  case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
  case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request)
  case ApiKeys.CONTROLLED_SHUTDOWN_KEY => handleControlledShutdownRequest(request)
  case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
  case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
  case ApiKeys.GROUP_COORDINATOR => handleGroupCoordinatorRequest(request)
  case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request)
  case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
  case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
  case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
  case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupRequest(request)
  case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request)
  case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
  case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
  case ApiKeys.CREATE_TOPICS => handleCreateTopicsRequest(request)
  case ApiKeys.DELETE_TOPICS => handleDeleteTopicsRequest(request)
  case requestId => throw new KafkaException("Unknown api code " + requestId)
}
```
 
 ## GroupCoordinator 简介 
 这里先简单看下 GroupCoordinator 的基本内容。   
 ### GroupCoordinator 的启动 
 Broker 在启动时，也就是 KafkaServer 在 startup() 方法中会有以下一段内容，它表示每个 Broker 在启动是都会启动 GroupCoordinator 服务。   
``` scala
/* start group coordinator */
// Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
groupCoordinator = GroupCoordinator(config, zkUtils, replicaManager, Time.SYSTEM)
groupCoordinator.startup()//note: 启动 groupCoordinator
```
 GroupCoordinator 服务在调用 setup() 方法启动后，进行的操作如下，实际上只是把一个标志变量值 isActive 设置为 true，并且启动了一个后台线程来删除过期的 group metadata。   
``` scala
/**
* Startup logic executed at the same time when the server starts up.
*/
def startup(enableMetadataExpiration: Boolean = true) {
  info("Starting up.")
  if (enableMetadataExpiration)
    groupManager.enableMetadataExpiration()
  isActive.set(true)
  info("Startup complete.")
}
```
 
 ### group 如何选择相应的 GroupCoordinator 
 要说这个，就必须介绍一下这个 __consumer_offsets topic 了，它是 Kafka 内部使用的一个 topic，专门用来存储 group 消费的情况，默认情况下有50个 partition，每个 partition 默认有三个副本，而具体的一个 group 的消费情况要存储到哪一个 partition 上，是根据 abs(GroupId.hashCode()) % NumPartitions 来计算的（其中，NumPartitions 是 __consumer_offsets 的 partition 数，默认是50个）。   对于 consumer group 而言，是根据其 group.id 进行 hash 并计算得到其具对应的 partition 值，该 partition leader 所在 Broker 即为该 Group 所对应的 GroupCoordinator，GroupCoordinator 会存储与该 group 相关的所有的 Meta 信息。   
 ### GroupCoordinator 的 metadata 
 对于 consumer group 而言，其对应的 metadata 信息主要包含一下内容：   
``` scala
/**
 * Group contains the following metadata:
 *
 *  Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 *  State metadata:
 *  1. group state
 *  2. generation id
 *  3. leader id
 */
@nonthreadsafe
//NOTE: group 的 meta 信息,对 group 级别而言,每个 group 都会有一个实例对象
private[coordinator] class GroupMetadata(val groupId: String, initialState: GroupState = Empty) {

  private var state: GroupState = initialState // group 的状态
  private val members = new mutable.HashMap[String, MemberMetadata] // group 的 member 信息
  private val offsets = new mutable.HashMap[TopicPartition, OffsetAndMetadata] //对应的 commit offset
  private val pendingOffsetCommits = new mutable.HashMap[TopicPartition, OffsetAndMetadata] // commit offset 成功后更新到上面的 map 中

  var protocolType: Option[String] = None
  var generationId = 0 // generation id
  var leaderId: String = null // leader consumer id
  var protocol: String = null
}
```
 而对于每个 consumer 而言，其 metadata 信息主要包括以下内容：   
``` scala
/**
 * Member metadata contains the following metadata:
 *
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout 心跳超时时间
 * 2. timestamp of the latest heartbeat 上次发送心跳的时间
 *
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference) 支持的 partition reassign 协议
 * 2. the metadata associated with each protocol
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
 * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
 */
@nonthreadsafe
//NOTE: 记录 group 中每个成员的状态信息
private[coordinator] class MemberMetadata(val memberId: String,
                                          val groupId: String,
                                          val clientId: String,
                                          val clientHost: String,
                                          val rebalanceTimeoutMs: Int,
                                          val sessionTimeoutMs: Int,
                                          val protocolType: String,
                                          var supportedProtocols: List[(String, Array[Byte])]) {}
```
 
 ## GroupCoordinator 请求处理 
 正如前面所述，Kafka Server 端可以介绍的21种请求中，其中有8种是由 GroupCoordinator 来处理的，这里主要介绍一下，GroupCoordinator 如何处理这些请求的。   
 ### Offset 请求的处理 
 关于 Offset 请求的处理，有两个：    
 - OFFSET_FETCH：查询 offset； 
 - OFFSET_COMMIT：提供 offset；  
 #### OFFSET_FETCH 请求处理 
 关于 OFFSET_FETCH 请求，Server 端的处理如下，新版 offset 默认是保存在 Kafka 中，这里也以保存在 Kafka 中为例，从下面的实现中也可以看出，在 fetch commit 是分两种情况：    
 - 获取 group 所消费的所有 topic-partition 的 offset； 
 - 获取指定 topic-partition 的 offset。  两种情况都是调用 coordinator.handleFetchOffsets() 方法实现的。   
``` scala
/**
 * Handle an offset fetch request
 */
def handleOffsetFetchRequest(request: RequestChannel.Request) {
  val header = request.header
  val offsetFetchRequest = request.body.asInstanceOf[OffsetFetchRequest]

  def authorizeTopicDescribe(partition: TopicPartition) =
    authorize(request.session, Describe, new Resource(auth.Topic, partition.topic)) //note: 验证 Describe 权限

  val offsetFetchResponse =
    // reject the request if not authorized to the group
    if (!authorize(request.session, Read, new Resource(Group, offsetFetchRequest.groupId)))
      offsetFetchRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED)
    else {
      if (header.apiVersion == 0) {
        val (authorizedPartitions, unauthorizedPartitions) = offsetFetchRequest.partitions.asScala
          .partition(authorizeTopicDescribe)

        // version 0 reads offsets from ZK
        val authorizedPartitionData = authorizedPartitions.map { topicPartition =>
          val topicDirs = new ZKGroupTopicDirs(offsetFetchRequest.groupId, topicPartition.topic)
          try {
            if (!metadataCache.contains(topicPartition.topic))
              (topicPartition, OffsetFetchResponse.UNKNOWN_PARTITION)
            else {
              val payloadOpt = zkUtils.readDataMaybeNull(s"${topicDirs.consumerOffsetDir}/${topicPartition.partition}")._1
              payloadOpt match {
                case Some(payload) =>
                  (topicPartition, new OffsetFetchResponse.PartitionData(
                      payload.toLong, OffsetFetchResponse.NO_METADATA, Errors.NONE))
                case None =>
                  (topicPartition, OffsetFetchResponse.UNKNOWN_PARTITION)
              }
            }
          } catch {
            case e: Throwable =>
              (topicPartition, new OffsetFetchResponse.PartitionData(
                  OffsetFetchResponse.INVALID_OFFSET, OffsetFetchResponse.NO_METADATA, Errors.forException(e)))
          }
        }.toMap

        val unauthorizedPartitionData = unauthorizedPartitions.map(_ -> OffsetFetchResponse.UNKNOWN_PARTITION).toMap
        new OffsetFetchResponse(Errors.NONE, (authorizedPartitionData ++ unauthorizedPartitionData).asJava, header.apiVersion)
      } else {
        // versions 1 and above read offsets from Kafka
        if (offsetFetchRequest.isAllPartitions) {//note: 获取这个 group 消费的所有 tp offset
          val (error, allPartitionData) = coordinator.handleFetchOffsets(offsetFetchRequest.groupId)
          if (error != Errors.NONE)
            offsetFetchRequest.getErrorResponse(error)
          else {
            // clients are not allowed to see offsets for topics that are not authorized for Describe
            //note: 如果没有 Describe 权限的话,不能查看相应的 offset
            val authorizedPartitionData = allPartitionData.filter { case (topicPartition, _) => authorizeTopicDescribe(topicPartition) }
            new OffsetFetchResponse(Errors.NONE, authorizedPartitionData.asJava, header.apiVersion)
          }
        } else { //note: 获取指定列表的 tp offset
          val (authorizedPartitions, unauthorizedPartitions) = offsetFetchRequest.partitions.asScala
            .partition(authorizeTopicDescribe)
          val (error, authorizedPartitionData) = coordinator.handleFetchOffsets(offsetFetchRequest.groupId,
            Some(authorizedPartitions))
          if (error != Errors.NONE)
            offsetFetchRequest.getErrorResponse(error)
          else {
            val unauthorizedPartitionData = unauthorizedPartitions.map(_ -> OffsetFetchResponse.UNKNOWN_PARTITION).toMap
            new OffsetFetchResponse(Errors.NONE, (authorizedPartitionData ++ unauthorizedPartitionData).asJava, header.apiVersion)
          }
        }
      }
    }

  trace(s"Sending offset fetch response $offsetFetchResponse for correlation id ${header.correlationId} to client ${header.clientId}.")
  requestChannel.sendResponse(new Response(request, offsetFetchResponse))
}
```
 在 coordinator.handleFetchOffsets() 的实现中，主要是调用了 groupManager.getOffsets() 获取相应的 offset 信息，在查询时加锁的原因应该是为了避免在查询的过程中 offset 不断更新。   
``` scala
def getOffsets(groupId: String, topicPartitionsOpt: Option[Seq[TopicPartition]]): Map[TopicPartition, OffsetFetchResponse.PartitionData] = {
  trace("Getting offsets of %s for group %s.".format(topicPartitionsOpt.getOrElse("all partitions"), groupId))
  val group = groupMetadataCache.get(groupId)
  if (group == null) {
    topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
      (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NONE))
    }.toMap
  } else {
    group synchronized {
      if (group.is(Dead)) { //note: group 状态已经变成 dead, offset 返回 -1（INVALID_OFFSET）
        topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
          (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NONE))
        }.toMap
      } else {
          topicPartitionsOpt match {
            case None => //note: 返回 group 消费的所有 tp 的 offset 信息（只返回这边已有 offset 的 tp）
              // Return offsets for all partitions owned by this consumer group. (this only applies to consumers
              // that commit offsets to Kafka.)
              group.allOffsets.map { case (topicPartition, offsetAndMetadata) =>
                topicPartition -> new OffsetFetchResponse.PartitionData(offsetAndMetadata.offset, offsetAndMetadata.metadata, Errors.NONE)
              }

            case Some(topicPartitions) =>
              topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
                val partitionData = group.offset(topicPartition) match {
                  case None => //note: offset 没有的话就返回-1
                    new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NONE)
                  case Some(offsetAndMetadata) =>
                    new OffsetFetchResponse.PartitionData(offsetAndMetadata.offset, offsetAndMetadata.metadata, Errors.NONE)
                }
                topicPartition -> partitionData
              }.toMap
          }
      }
    }
  }
}
```
 
 #### OFFSET_COMMIT 请求处理 
 对 OFFSET_COMMIT 请求的处理，部分内容已经介绍过，可以参考 [commit offset 请求处理](http://matt33.com/2017/11/18/consumer-subscribe/#commit-offset-%E8%AF%B7%E6%B1%82%E5%A4%84%E7%90%86)，处理过程如下：   
``` scala
private def doCommitOffsets(group: GroupMetadata,
                    memberId: String,
                    generationId: Int,
                    offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                    responseCallback: immutable.Map[TopicPartition, Short] => Unit) {
  var delayedOffsetStore: Option[DelayedStore] = None

  group synchronized {
    if (group.is(Dead)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
    } else if (generationId < 0 && group.is(Empty)) {//note: 来自 assign 的情况
      // the group is only using Kafka to store offsets
      delayedOffsetStore = groupManager.prepareStoreOffsets(group, memberId, generationId,
        offsetMetadata, responseCallback)
    } else if (group.is(AwaitingSync)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS.code))
    } else if (!group.has(memberId)) {//note: 有可能 simple 与 high level 的冲突了,这里就直接拒绝相应的请求
      responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
    } else if (generationId != group.generationId) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
    } else {
      val member = group.get(memberId)
      completeAndScheduleNextHeartbeatExpiration(group, member)//note: 更新下次需要的心跳时间
      delayedOffsetStore = groupManager.prepareStoreOffsets(group, memberId, generationId,
        offsetMetadata, responseCallback) //note: commit offset
    }
  }

  // store the offsets without holding the group lock
  delayedOffsetStore.foreach(groupManager.store)
}
```
 这里主要介绍一下 groupManager.prepareStoreOffsets() 方法，处理逻辑如下，这里简单说一下其 offset 存储的过程：    
 - 首先过滤掉那些 offset 超过范围的 metadata； 
 - 将 offset 信息追加到 replicated log 中； 
 - 调用 <code>prepareOffsetCommit()</code> 方法，先将 offset 信息更新到 group 的 pendingOffsetCommits 中（这时还没有真正提交，后面如果失败的话，是可以撤回的）； 
 - 在 <code>putCacheCallback</code> 回调函数中，如果 offset 信息追加到 replicated log 成功，那么就更新缓存（将 group 的 pendingOffsetCommits 中的信息更新到 offset 变量中）。  
``` scala
/**
 * Store offsets by appending it to the replicated log and then inserting to cache
 */
//note: 记录 commit 的 offset
def prepareStoreOffsets(group: GroupMetadata,
                        consumerId: String,
                        generationId: Int,
                        offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                        responseCallback: immutable.Map[TopicPartition, Short] => Unit): Option[DelayedStore] = {
  // first filter out partitions with offset metadata size exceeding limit
  //note: 首先过滤掉 offset 信息超过范围的 metadata
  val filteredOffsetMetadata = offsetMetadata.filter { case (_, offsetAndMetadata) =>
    validateOffsetMetadataLength(offsetAndMetadata.metadata)
  }

  // construct the message set to append
  //note: 构造一个 msg set 追加
  getMagicAndTimestamp(partitionFor(group.groupId)) match {
    case Some((magicValue, timestampType, timestamp)) =>
      val records = filteredOffsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
        Record.create(magicValue, timestampType, timestamp,
          GroupMetadataManager.offsetCommitKey(group.groupId, topicPartition), //note: key是一个三元组: group、topic、partition
          GroupMetadataManager.offsetCommitValue(offsetAndMetadata))
      }.toSeq

      val offsetTopicPartition = new TopicPartition(Topic.GroupMetadataTopicName, partitionFor(group.groupId))

      //note: 将 offset 信息追加到 replicated log 中
      val entries = Map(offsetTopicPartition -> MemoryRecords.withRecords(timestampType, compressionType, records:_*))

      // set the callback function to insert offsets into cache after log append completed
      def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
        // the append response should only contain the topics partition
        if (responseStatus.size != 1 || ! responseStatus.contains(offsetTopicPartition))
          throw new IllegalStateException("Append status %s should only have one partition %s"
            .format(responseStatus, offsetTopicPartition))

        // construct the commit response status and insert
        // the offset and metadata to cache if the append status has no error
        val status = responseStatus(offsetTopicPartition)

        val responseCode =
          group synchronized {
            if (status.error == Errors.NONE) { //note: 如果已经追加到了 replicated log 中了,那么就更新其缓存
              if (!group.is(Dead)) { //note: 更新到 group 的 offset 中
                filteredOffsetMetadata.foreach { case (topicPartition, offsetAndMetadata) =>
                  group.completePendingOffsetWrite(topicPartition, offsetAndMetadata)
                }
              }
              Errors.NONE.code
            } else {
              if (!group.is(Dead)) {
                filteredOffsetMetadata.foreach { case (topicPartition, offsetAndMetadata) =>
                  group.failPendingOffsetWrite(topicPartition, offsetAndMetadata)
                }
              }

              debug(s"Offset commit $filteredOffsetMetadata from group ${group.groupId}, consumer $consumerId " +
                s"with generation $generationId failed when appending to log due to ${status.error.exceptionName}")

              // transform the log append error code to the corresponding the commit status error code
              val responseError = status.error match {
                case Errors.UNKNOWN_TOPIC_OR_PARTITION
                     | Errors.NOT_ENOUGH_REPLICAS
                     | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                  Errors.GROUP_COORDINATOR_NOT_AVAILABLE

                case Errors.NOT_LEADER_FOR_PARTITION =>
                  Errors.NOT_COORDINATOR_FOR_GROUP

                case Errors.MESSAGE_TOO_LARGE
                     | Errors.RECORD_LIST_TOO_LARGE
                     | Errors.INVALID_FETCH_SIZE =>
                  Errors.INVALID_COMMIT_OFFSET_SIZE

                case other => other
              }

              responseError.code
            }
          }

        // compute the final error codes for the commit response
        val commitStatus = offsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
          if (validateOffsetMetadataLength(offsetAndMetadata.metadata))
            (topicPartition, responseCode)
          else
            (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE.code)
        }

        // finally trigger the callback logic passed from the API layer
        responseCallback(commitStatus)
      }

      group synchronized {
        group.prepareOffsetCommit(offsetMetadata) //note: 添加到 group 的 pendingOffsetCommits 中
      }

      Some(DelayedStore(entries, putCacheCallback)) //note:

    case None =>
      val commitStatus = offsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
        (topicPartition, Errors.NOT_COORDINATOR_FOR_GROUP.code)
      }
      responseCallback(commitStatus)
      None
  }
}
```
 
 ### group 相关的处理 
 这一小节主要介绍 GroupCoordinator 处理 group 相关的请求。   
 #### JOIN_GROUP 和 SYNC_GROUP请求处理 
 这两个请求的处理实际上在 [Kafka 源码解析之 Consumer 如何加入一个 Group（六）](http://matt33.com/2017/10/22/consumer-join-group/) 中已经详细介绍过，这里就不再陈述。   
 #### DESCRIBE_GROUPS 请求处理 
 关于 DESCRIBE_GROUPS 请求处理实现如下，主要是返回 group 中各个 member 的详细信息，包含的变量信息为 memberId, clientId, clientHost, metadata(protocol), assignment。   
``` scala
def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
  if (!isActive.get) {
    (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, GroupCoordinator.EmptyGroup)
  } else if (!isCoordinatorForGroup(groupId)) {
    (Errors.NOT_COORDINATOR_FOR_GROUP, GroupCoordinator.EmptyGroup)
  } else if (isCoordinatorLoadingInProgress(groupId)) {
    (Errors.GROUP_LOAD_IN_PROGRESS, GroupCoordinator.EmptyGroup)
  } else {
    groupManager.getGroup(groupId) match { //note: 返回 group 详细信息,主要是 member 的详细信息
      case None => (Errors.NONE, GroupCoordinator.DeadGroup)
      case Some(group) =>
        group synchronized {
          (Errors.NONE, group.summary)
        }
    }
  }
}
```
 
 #### LEAVE_GROUP 请求处理 
 在什么情况下，Server 会收到 LEAVE_GROUP 的请求呢？一般来说是：    
 - consumer 调用 <code>unsubscribe()</code> 方法，取消了对所有 topic 的订阅时； 
 - consumer 的心跳线程超时时，这时 consumer 会主动发送 LEAVE_GROUP 请求； 
 - 在 server 端，如果在给定的时间没收到 client 的心跳请求，这时候会自动触发 LEAVE_GROUP 操作。  
``` scala
def handleLeaveGroup(groupId: String, memberId: String, responseCallback: Short => Unit) {
  if (!isActive.get) {
    responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
  } else if (!isCoordinatorForGroup(groupId)) {
    responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
  } else if (isCoordinatorLoadingInProgress(groupId)) {
    responseCallback(Errors.GROUP_LOAD_IN_PROGRESS.code)
  } else {
    groupManager.getGroup(groupId) match {
      case None =>
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
        responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

      case Some(group) =>
        group synchronized {
          if (group.is(Dead) || !group.has(memberId)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
          } else {
            val member = group.get(memberId)
            removeHeartbeatForLeavingMember(group, member)//NOTE: 认为心跳完成
            onMemberFailure(group, member)//NOTE: 从 group 移除当前 member,并进行 rebalance
            responseCallback(Errors.NONE.code)
          }
        }
    }
  }
}

private def onMemberFailure(group: GroupMetadata, member: MemberMetadata) {
  trace("Member %s in group %s has failed".format(member.memberId, group.groupId))
  group.remove(member.memberId)//NOTE: 从 Group 移除当前 member 信息
  group.currentState match {
    case Dead | Empty =>
    case Stable | AwaitingSync => maybePrepareRebalance(group)//NOTE: 进行 rebalance
    case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))//NOTE: 检查 join-group 是否可以完成
  }
}
```
 从上面可以看出，GroupCoordinator 在处理 LEAVE_GROUP 请求时，实际上就是调用了 onMemberFailure() 方法，从 group 移除了失败的 member 的，并且将进行相应的状态转换：    
 - 如果 group 原来是在 Dead 或 Empty 时，那么由于 group 本来就没有 member，就不再进行任何操作； 
 - 如果 group 原来是在 Stable 或 AwaitingSync 时，那么将会执行 <code>maybePrepareRebalance()</code> 方法，进行 rebalance 操作（后面的过程就跟最开始 join-group 时一样，参考源码分析六）； 
 - 如果 group 已经在 PreparingRebalance 状态了，那么这里将检查一下 join-group 的延迟操作是否完成了，如果操作完成了，那么 GroupCoordinator 就会向 group 的 member 发送 join-group response，然后将状态更新为 AwaitingSync.  
 ### HEARTBEAT 心跳请求处理 
 心跳请求是非常重要的请求之一：    
 - 对于 Server 端来说，它是 GroupCoordinator 判断一个 consumer member 是否存活的重要条件，如果其中一个 consumer 在给定的时间没有发送心跳请求，那么就会将这个 consumer 从这个 group 中移除，并执行 rebalance 操作； 
 - 对于 Client 端而言，心跳请求是 client 感应 group 状态变化的一个重要中介，比如：此时有一个新的 consumer 加入到 consumer group 中了，这时候会进行 rebalace 操作，group 端的状态会发送变化，当 group 其他 member 发送心跳请求，GroupCoordinator 就会通知 client 此时这个 group 正处于 rebalance 阶段，让它们 rejoin group。  GroupCoordinator 处理心跳请求的过程如下所示。   
``` scala
//NOTE: Server 端处理心跳请求
def handleHeartbeat(groupId: String,
                  memberId: String,
                  generationId: Int,
                  responseCallback: Short => Unit) {
if (!isActive.get) {//NOTE: GroupCoordinator 已经失败
  responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
} else if (!isCoordinatorForGroup(groupId)) {//NOTE: 当前的 GroupCoordinator 不包含这个 group
  responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
} else if (isCoordinatorLoadingInProgress(groupId)) {//NOTE: group 的状态信息正在 loading,直接返回成功结果
  // the group is still loading, so respond just blindly
  responseCallback(Errors.NONE.code)
} else {
  groupManager.getGroup(groupId) match {
    case None => //NOTE: 当前 GroupCoordinator 不包含这个 group
      responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

    case Some(group) => //NOTE: 包含这个 group
      group synchronized {
        group.currentState match {
          case Dead => //NOTE: group 的状态已经变为 dead,意味着 group 的 meta 已经被清除,返回 UNKNOWN_MEMBER_ID 错误
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

          case Empty => //NOTE: group 的状态为 Empty, 意味着 group 的成员为空,返回 UNKNOWN_MEMBER_ID 错误
            responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

          case AwaitingSync => //NOTE: group 状态为 AwaitingSync, 意味着 group 刚 rebalance 结束
            if (!group.has(memberId)) //NOTE: group 不包含这个 member,返回 UNKNOWN_MEMBER_ID 错误
              responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
            else //NOTE: 返回当前 group 正在进行 rebalance,要求 client rejoin 这个 group
              responseCallback(Errors.REBALANCE_IN_PROGRESS.code)

          case PreparingRebalance => //NOTE: group 状态为 PreparingRebalance
            if (!group.has(memberId)) { //NOTE: group 不包含这个 member,返回 UNKNOWN_MEMBER_ID 错误
              responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
            } else if (generationId != group.generationId) {
              responseCallback(Errors.ILLEGAL_GENERATION.code)
            } else { //NOTE: 正常处理心跳信息,并返回 REBALANCE_IN_PROGRESS 错误
              val member = group.get(memberId)
              //note: 更新心跳时间,认为心跳完成,并监控下次的调度情况（超时的话,会把这个 member 从 group 中移除）
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.REBALANCE_IN_PROGRESS.code)
            }

          case Stable =>
            if (!group.has(memberId)) {
              responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
            } else if (generationId != group.generationId) {
              responseCallback(Errors.ILLEGAL_GENERATION.code)
            } else { //NOTE: 正确处理心跳信息
              val member = group.get(memberId)
              //note: 更新心跳时间,认为心跳完成,并监控下次的调度情况（超时的话,会把这个 member 从 group 中移除）
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.NONE.code)
            }
        }
      }
  }
}
```
 
 ## group 的状态机 
 GroupCoordinator 在进行 group 和 offset 相关的管理操作时，有一项重要的工作就是处理和维护 group 状态的变化，一个 Group 状态机如下如所示。   
![Group 状态机](./images/kafka/group.png)
   在这个状态机中，最核心就是 rebalance 操作，简单说一下 rebalance 过程：    
 - 当一些条件发生时将 group 从 <strong>Stable</strong> 状态变为 <strong>PreparingRebalance</strong>； 
 - 然后就是等待 group 中的所有 consumer member 发送 join-group 请求加入 group，如果都已经发送 join-group 请求，此时 GroupCoordinator 会向所有 member 发送 join-group response，那么 group 的状态变为 <strong>AwaitingSync</strong>； 
 - leader consumer 会收到各个 member 订阅的 topic 详细信息，等待其分配好 partition 后，通过 sync-group 请求将结果发给 GroupCoordinator（非 leader consumer 发送的 sync-group 请求的 data 是为空的）； 
 - 如果 GroupCoordinator 收到了 leader consumer 发送的 response，获取到了这个 group 各个 member 所分配的 topic-partition 列表，group 的状态就会变成 <strong>Stable</strong>。  这就是一次完整的 rebalance 过程。