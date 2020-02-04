本篇主要讲述 Controller 向各个 Broker 发送请求的模型，算是对 [Controller Channel Manager](http://matt33.com/2018/06/15/kafka-controller-start/#Controller-Channel-Manager) 部分的一个补充，在这篇文章中，将会看到 Controller 在处理 leader 切换、ShutDown 请求时如何向 Broker 发送相应的请求。   Kafka Controller 向 Broker 发送的请求类型主要分为三种：LeaderAndIsr、UpdateMetadata、StopReplica 请求，正如 [Controller Channel Manager](http://matt33.com/2018/06/15/kafka-controller-start/#Controller-Channel-Manager) 这里介绍的，Controller 会为每台 Broker 初始化为一个 ControllerBrokerStateInfo 对象，该对象主要包含以下四个内容：    
 - NetworkClient：与 Broker 的网络连接对象； 
 - Node：Broker 的节点信息； 
 - MessageQueue：每个 Broker 对应的请求队列，Controller 向 Broker 发送的请求会想放在这个队列里； 
 - RequestSendThread：每台 Broker 对应的请求发送线程。  
 ## Controller 的请求发送模型 
 在讲述 Controller 发送模型之前，先看下 Controller 是如何向 Broker 发送请求的，这里以发送 metadata 更新请求为例，简略的代码如下：   
``` scala
//note: 创建新的批量请求
brokerRequestBatch.newBatch()
//note: 为目标 Broker 添加相应的请求
brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
//note: 发送请求，实际上只是把请求添加发送线程的 request queue 中
brokerRequestBatch.sendRequestsToBrokers(epoch)
```
 这里有一个比较重要的对象，就是 ControllerBrokerRequestBatch 对象，可以认为它是一个专门用于批量请求发送的对象，在这个对象中有几个重要成员变量：    
 - leaderAndIsrRequestMap：记录每个 broker 与要发送的 LeaderAndIsr 请求集合的 map； 
 - stopReplicaRequestMap：记录每个 broker 与要发送的 StopReplica 集合的 map； 
 - updateMetadataRequestBrokerSet：记录要发送的 update-metadata 请求的 broker 集合； 
 - updateMetadataRequestPartitionInfoMap：记录 update-metadata 请求要更新的 Topic Partition 集合。  Controller 可以通过下面这三方法向这些集合添加相应的请求：    
 - <code>addLeaderAndIsrRequestForBrokers()</code>：向给定的 Broker 发送某个 Topic Partition 的 LeaderAndIsr 请求； 
 - <code>addStopReplicaRequestForBrokers()</code>：向给定的 Broker 发送某个 Topic Partition 的 StopReplica 请求； 
 - <code>addUpdateMetadataRequestForBrokers()</code>：向给定的 Broker 发送某一批 Partitions 的 UpdateMetadata 请求。  Controller 整体的请求模型概况如下图所示：   
![Controller 的请求发送模型](./images/kafka/controller-request-model.png)
   上述三个方法将相应的请求添加到对应的集合中后，然后通过 sendRequestsToBrokers() 方法将该请求添加到该 Broker 对应的请求队列中，接着再由该 Broker 对应的 RequestSendThread 去发送相应的请求。   
 ## ControllerBrokerRequestBatch 
 这节详细讲述一下关于 ControllerBrokerRequestBatch 的一些方法实现。   
 ### newBatch 方法 
 Controller 在添加请求前，都会先调用 newBatch() 方法，该方法的实现如下：   
``` scala
//note: 创建新的请求前,确保前一批请求全部发送完毕
def newBatch() {
  // raise error if the previous batch is not empty
  if (leaderAndIsrRequestMap.nonEmpty)
    throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
      "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
  if (stopReplicaRequestMap.nonEmpty)
    throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
      "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
  if (updateMetadataRequestBrokerSet.nonEmpty)
    throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
      "new one. Some UpdateMetadata state changes to brokers %s with partition info %s might be lost ".format(
        updateMetadataRequestBrokerSet.toString(), updateMetadataRequestPartitionInfoMap.toString()))
}
```
 这个方法的主要作用是检查上一波的 LeaderAndIsr、UpdateMetadata、StopReplica 请求是否已经发送，正常情况下，Controller 在调用 sendRequestsToBrokers() 方法之后，这些集合中的请求都会被发送，发送之后，会将相应的请求集合清空，当然在异常情况可能会导致部分集合没有被清空，导致无法 newBatch()，这种情况下，通常策略是重启 controller，因为现在 Controller 的设计还是有些复杂，在某些情况下还是可能会导致异常发生，并且有些异常还是无法恢复的。   
 ### 添加 LeaderAndIsr 请求 
 Controller 可以通过 addLeaderAndIsrRequestForBrokers() 向指定 Broker 列表添加某个 Topic Partition 的 LeaderAndIsr 请求，其具体实现如下：   
``` scala
//note: 将 LeaderAndIsr 添加到对应的 broker 中,还未开始发送数据
def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                     leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                     replicas: Seq[Int], callback: AbstractResponse => Unit = null) {
  val topicPartition = new TopicPartition(topic, partition)

  //note: 将请求添加到对应的 broker 上
  brokerIds.filter(_ >= 0).foreach { brokerId =>
    val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
    result.put(topicPartition, PartitionStateInfo(leaderIsrAndControllerEpoch, replicas.toSet))
  }

  //note: 在更新 LeaderAndIsr 信息时,主题的 metadata 相当于也进行了更新,需要发送这个 topic 的 metadata 给所有存活的 broker
  addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq,
                                     Set(TopicAndPartition(topic, partition)))
}
```
 这个方法的处理流程如下：    
 - 向对应的 Broker 添加 LeaderAndIsr 请求，请求会被添加到 leaderAndIsrRequestMap 集合中； 
 - 并通过 <code>addUpdateMetadataRequestForBrokers()</code> 方法向所有的 Broker 添加这个 Topic-Partition 的 UpdateMatedata 请求，leader 或 isr 变动时，会向所有 broker 同步这个 Partition 的 metadata 信息，这样可以保证每台 Broker 上都有最新的 metadata 信息。  
 ### 添加 UpdateMetadata 请求 
 Controller 可以通过 addUpdateMetadataRequestForBrokers() 向指定 Broker 列表添加某批 Partitions 的 UpdateMetadata 请求，其具体实现如下：   
``` scala
//note: 向给行的 Broker 发送 UpdateMetadataRequest 请求
def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                       partitions: collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition],
                                       callback: AbstractResponse => Unit = null) {
  //note: 将 Topic-Partition 添加到对应的 map 中
  def updateMetadataRequestPartitionInfo(partition: TopicAndPartition, beingDeleted: Boolean) {
    val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition)
    leaderIsrAndControllerEpochOpt match {
      case Some(leaderIsrAndControllerEpoch) =>
        val replicas = controllerContext.partitionReplicaAssignment(partition).toSet
        val partitionStateInfo = if (beingDeleted) { //note: 正在删除的 Partition,设置 leader 为-2
          val leaderAndIsr = new LeaderAndIsr(LeaderAndIsr.LeaderDuringDelete, leaderIsrAndControllerEpoch.leaderAndIsr.isr)
          PartitionStateInfo(LeaderIsrAndControllerEpoch(leaderAndIsr, leaderIsrAndControllerEpoch.controllerEpoch), replicas)
        } else {
          PartitionStateInfo(leaderIsrAndControllerEpoch, replicas)
        }
        //note: 添加到对应的 request map 中
        updateMetadataRequestPartitionInfoMap.put(new TopicPartition(partition.topic, partition.partition), partitionStateInfo)
      case None =>
        info("Leader not yet assigned for partition %s. Skip sending UpdateMetadataRequest.".format(partition))
    }
  }

  //note:过滤出要发送的 partition
  val filteredPartitions = {
    val givenPartitions = if (partitions.isEmpty)
      controllerContext.partitionLeadershipInfo.keySet //note: Partitions 为空时，就过滤出所有的 topic
    else
      partitions
    if (controller.deleteTopicManager.partitionsToBeDeleted.isEmpty)
      givenPartitions
    else
      givenPartitions -- controller.deleteTopicManager.partitionsToBeDeleted //note: 将要删除的 topic 过滤掉
  }

  updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0) //note: 将 broker 列表更新到要发送的集合中
  //note: 对于要更新 metadata 的 Partition,设置 beingDeleted 为 False
  filteredPartitions.foreach(partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = false))
  //note: 要删除的 Partition 设置 BeingDeleted 为 True
  controller.deleteTopicManager.partitionsToBeDeleted.foreach(partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = true))
}
```
 这个方法的实现逻辑如下：    
 - 首先过滤出要发送的 Partition 列表，如果没有指定要发送 partitions 列表，那么默认就是发送全局的 metadata 信息； 
 - 接着将已经标记为删除的 Partition 从上面的列表中移除； 
 - 将要发送的 Broker 列表添加到 updateMetadataRequestBrokerSet 集合中； 
 - 将前面过滤的 Partition 列表对应的 metadata 信息添加到对应的 updateMetadataRequestPartitionInfoMap 集合中; 
 - 将当前设置为删除的所有 Partition 的 metadata 信息也添加到 updateMetadataRequestPartitionInfoMap 集合中，添加前会把其 leader 设置为-2，这样 Broker 收到这个 Partition 的 metadata 信息之后就会知道这个 Partition 是设置删除标志。  
 ### 添加 StopReplica 请求 
 Controller 可以通过 addStopReplicaRequestForBrokers() 向指定 Broker 列表添加某个 Topic Partition 的 StopReplica 请求，其具体实现如下：   
``` scala
//note: 将 StopReplica 添加到对应的 Broker 中,还未开始发送数据
def addStopReplicaRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int, deletePartition: Boolean,
                                    callback: (AbstractResponse, Int) => Unit = null) {
  brokerIds.filter(b => b >= 0).foreach { brokerId =>
    stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[StopReplicaRequestInfo])
    val v = stopReplicaRequestMap(brokerId)
    if(callback != null)
      stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
        deletePartition, (r: AbstractResponse) => callback(r, brokerId))
    else
      stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
        deletePartition)
  }
}
```
 这个方法的实现逻辑比较简单，直接将 StopReplica 添加到 stopReplicaRequestMap 中。   
 ### 向 Broker 发送请求 
 Controller 在添加完相应的请求后，最后一步都会去调用 sendRequestsToBrokers() 方法构造相应的请求，并把请求添加到 Broker 对应的 RequestQueue 中。   
``` scala
//note: 发送请求给 broker（只是将对应处理后放入到对应的 queue 中）
def sendRequestsToBrokers(controllerEpoch: Int) {
  try {
    //note: LeaderAndIsr 请求
    leaderAndIsrRequestMap.foreach { case (broker, partitionStateInfos) =>
      partitionStateInfos.foreach { case (topicPartition, state) =>
        val typeOfRequest = if (broker == state.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader" else "become-follower"
        stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request %s to broker %d " +
                                 "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest,
                                                                 state.leaderIsrAndControllerEpoch, broker,
                                                                 topicPartition.topic, topicPartition.partition))
      }
      //note: leader id 集合
      val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet
      val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
        _.getNode(controller.config.interBrokerListenerName)
      }
      //note: requests.PartitionState
      val partitionStates = partitionStateInfos.map { case (topicPartition, partitionStateInfo) =>
        val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
        val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
          leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
          partitionStateInfo.allReplicas.map(Integer.valueOf).asJava)
        topicPartition -> partitionState
      }
      //note: 构造 LeaderAndIsr 请求,并添加到对应的 queue 中
      val leaderAndIsrRequest = new LeaderAndIsrRequest.
          Builder(controllerId, controllerEpoch, partitionStates.asJava, leaders.asJava)
      controller.sendRequest(broker, ApiKeys.LEADER_AND_ISR, leaderAndIsrRequest, null)
    }
    leaderAndIsrRequestMap.clear() //note: 清空 leaderAndIsr 集合

    //note: update-metadata 请求
    updateMetadataRequestPartitionInfoMap.foreach(p => stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request %s " +
      "to brokers %s for partition %s").format(controllerId, controllerEpoch, p._2.leaderIsrAndControllerEpoch,
      updateMetadataRequestBrokerSet.toString(), p._1)))
    val partitionStates = updateMetadataRequestPartitionInfoMap.map { case (topicPartition, partitionStateInfo) =>
      val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
      val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
        leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
        partitionStateInfo.allReplicas.map(Integer.valueOf).asJava)
      topicPartition -> partitionState
    }

    val version: Short =
      if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_2_IV0) 3
      else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2
      else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
      else 0

    //note: 构造 update-metadata 请求
    val updateMetadataRequest = {
      val liveBrokers = if (version == 0) {
        // Version 0 of UpdateMetadataRequest only supports PLAINTEXT.
        controllerContext.liveOrShuttingDownBrokers.map { broker =>
          val securityProtocol = SecurityProtocol.PLAINTEXT
          val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
          val node = broker.getNode(listenerName)
          val endPoints = Seq(new EndPoint(node.host, node.port, securityProtocol, listenerName))
          new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
        }
      } else {
        controllerContext.liveOrShuttingDownBrokers.map { broker =>
          val endPoints = broker.endPoints.map { endPoint =>
            new UpdateMetadataRequest.EndPoint(endPoint.host, endPoint.port, endPoint.securityProtocol, endPoint.listenerName)
          }
          new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
        }
      }
      new UpdateMetadataRequest.Builder(
        controllerId, controllerEpoch, partitionStates.asJava, liveBrokers.asJava).
        setVersion(version)
    }

    //note: 将请求添加到对应的 queue
    updateMetadataRequestBrokerSet.foreach { broker =>
      controller.sendRequest(broker, ApiKeys.UPDATE_METADATA_KEY, updateMetadataRequest, null)
    }
    updateMetadataRequestBrokerSet.clear() //note: 清空对应的请求记录
    updateMetadataRequestPartitionInfoMap.clear()

    //note: StopReplica 请求的处理
    stopReplicaRequestMap.foreach { case (broker, replicaInfoList) =>
      val stopReplicaWithDelete = replicaInfoList.filter(_.deletePartition).map(_.replica).toSet
      val stopReplicaWithoutDelete = replicaInfoList.filterNot(_.deletePartition).map(_.replica).toSet
      debug("The stop replica request (delete = true) sent to broker %d is %s"
        .format(broker, stopReplicaWithDelete.mkString(",")))
      debug("The stop replica request (delete = false) sent to broker %d is %s"
        .format(broker, stopReplicaWithoutDelete.mkString(",")))

      val (replicasToGroup, replicasToNotGroup) = replicaInfoList.partition(r => !r.deletePartition && r.callback == null)

      // Send one StopReplicaRequest for all partitions that require neither delete nor callback. This potentially
      // changes the order in which the requests are sent for the same partitions, but that's OK.
      val stopReplicaRequest = new StopReplicaRequest.Builder(controllerId, controllerEpoch, false,
        replicasToGroup.map(r => new TopicPartition(r.replica.topic, r.replica.partition)).toSet.asJava)
      controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest)

      replicasToNotGroup.foreach { r =>
        val stopReplicaRequest = new StopReplicaRequest.Builder(
            controllerId, controllerEpoch, r.deletePartition,
            Set(new TopicPartition(r.replica.topic, r.replica.partition)).asJava)
        controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest, r.callback)
      }
    }
    stopReplicaRequestMap.clear()
  } catch {
    case e: Throwable =>
      if (leaderAndIsrRequestMap.nonEmpty) {
        error("Haven't been able to send leader and isr requests, current state of " +
            s"the map is $leaderAndIsrRequestMap. Exception message: $e")
      }
      if (updateMetadataRequestBrokerSet.nonEmpty) {
        error(s"Haven't been able to send metadata update requests to brokers $updateMetadataRequestBrokerSet, " +
              s"current state of the partition info is $updateMetadataRequestPartitionInfoMap. Exception message: $e")
      }
      if (stopReplicaRequestMap.nonEmpty) {
        error("Haven't been able to send stop replica requests, current state of " +
            s"the map is $stopReplicaRequestMap. Exception message: $e")
      }
      throw new IllegalStateException(e)
  }
}
```
 上面这个方法看着很复杂，其实做的事情很明确，就是将三个集合中的请求发送对应 Broker 的请求队列中，这里简单作一个总结：    
 - 从 leaderAndIsrRequestMap 集合中构造相应的 LeaderAndIsr 请求，通过 Controller 的 <code>sendRequest()</code> 方法将请求添加到 Broker 对应的 MessageQueue 中，最后清空 leaderAndIsrRequestMap 集合； 
 - 从 updateMetadataRequestPartitionInfoMap 集合中构造相应的 UpdateMetadata 请求，，通过 Controller 的 <code>sendRequest()</code> 方法将请求添加到 Broker 对应的 MessageQueue 中，最后清空 updateMetadataRequestBrokerSet 和 updateMetadataRequestPartitionInfoMap 集合； 
 - 从 stopReplicaRequestMap 集合中构造相应的 StopReplica 请求，在构造时会根据是否设置删除标志将要涉及的 Partition 分成两类，构造对应的请求，对于要删除数据的 StopReplica 会设置相应的回调函数，然后通过 Controller 的 <code>sendRequest()</code> 方法将请求添加到 Broker 对应的 MessageQueue 中，最后清空 stopReplicaRequestMap 集合。  走到这一步，Controller 要发送的请求算是都添加到对应 Broker 的 MessageQueue 中，后台的 RequestSendThread 线程会从这个请求队列中遍历相应的请求，发送给对应的 Broker。