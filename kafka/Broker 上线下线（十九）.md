本篇接着讲述 Controller 对于监听器的处理内容 —— Broker 节点上下线的处理流程。每台 Broker 在上线时，都会与 ZK 建立一个建立一个 session，并在 /brokers/ids 下注册一个节点，节点名字就是 broker id，这个节点是临时节点，该节点内部会有这个 Broker 的详细节点信息。Controller 会监听 /brokers/ids 这个路径下的所有子节点，如果有新的节点出现，那么就代表有新的 Broker 上线，如果有节点消失，就代表有 broker 下线，Controller 会进行相应的处理，Kafka 就是利用 ZK 的这种 watch 机制及临时节点的特性来完成集群 Broker 的上下线，本文将会深入讲解这一过程。   
 ## BrokerChangeListener 
 KafkaController 在启动时，会通过副本状态机注册一个监控 broker 上下线的监听器，通过 ReplicaStateMachine 的 registerListeners() 方法实现的，该方法的实现如下：   
``` scala
// register ZK listeners of the replica state machine
 def registerListeners() {
   // register broker change listener
   registerBrokerChangeListener() //note: 监听【/brokers/ids】，broker 的上线下线
 }

private def registerBrokerChangeListener() = {
  zkUtils.zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
}
```
 BrokerChangeListener 是监听 /brokers/ids 节点的监听器，当该节点有变化时会触发 doHandleChildChange() 方法，具体实现如下：   
``` scala
//note: 如果 【/brokers/ids】 目录下子节点有变化将会触发这个操作
class BrokerChangeListener(protected val controller: KafkaController) extends ControllerZkChildListener {

  protected def logName = "BrokerChangeListener"

  def doHandleChildChange(parentPath: String, currentBrokerList: Seq[String]) {
    info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.sorted.mkString(",")))
    inLock(controllerContext.controllerLock) {
      if (hasStarted.get) {
        ControllerStats.leaderElectionTimer.time {
          try {
            //note: 当前 zk 的 broker 列表
            val curBrokers = currentBrokerList.map(_.toInt).toSet.flatMap(zkUtils.getBrokerInfo)
            //note: ZK 中的 broker id 列表
            val curBrokerIds = curBrokers.map(_.id)
            //note: Controller 缓存中的 broker 列表
            val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
            //note: 新上线的 broker id 列表
            val newBrokerIds = curBrokerIds -- liveOrShuttingDownBrokerIds
            //note: 掉线的 broker id 列表
            val deadBrokerIds = liveOrShuttingDownBrokerIds -- curBrokerIds
            //note: 新上线的 Broker 列表
            val newBrokers = curBrokers.filter(broker => newBrokerIds(broker.id))
            controllerContext.liveBrokers = curBrokers //note: 更新缓存中当前 broker 列表
            val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
            val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
            val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
            info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
              .format(newBrokerIdsSorted.mkString(","), deadBrokerIdsSorted.mkString(","), liveBrokerIdsSorted.mkString(",")))
            //note: Broker 上线, 在 Controller Channel Manager 中添加该 broker
            newBrokers.foreach(controllerContext.controllerChannelManager.addBroker)
            //note: Broker 下线处理, 在 Controller Channel Manager 移除该 broker
            deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)
            if(newBrokerIds.nonEmpty) //note: 启动该 Broker
              controller.onBrokerStartup(newBrokerIdsSorted)
            if(deadBrokerIds.nonEmpty) //note: broker 掉线后开始 leader 选举
              controller.onBrokerFailure(deadBrokerIdsSorted)
          } catch {
            case e: Throwable => error("Error while handling broker changes", e)
          }
        }
      }
    }
  }
}
```
 这里需要重点关注 doHandleChildChange() 方法的实现，该方法处理逻辑如下：    
 - 从 ZK 获取当前的 Broker 列表（<code>curBrokers</code>）及 broker id 的列表（<code>curBrokerIds</code>）； 
 - 获取当前 Controller 中缓存的 broker id 列表（<code>liveOrShuttingDownBrokerIds</code>）； 
 - 获取新上线 broker id 列表：<code>newBrokerIds</code> = <code>curBrokerIds</code> – <code>liveOrShuttingDownBrokerIds</code>； 
 - 获取掉线的 broker id 列表：<code>deadBrokerIds</code> = <code>liveOrShuttingDownBrokerIds</code> – <code>curBrokerIds</code>； 
 - 对于新上线的 broker，先在 ControllerChannelManager 中添加该 broker（即建立与该 Broker 的连接、初始化相应的发送线程和请求队列），最后 Controller 调用 <code>onBrokerStartup()</code> 上线该 Broker； 
 - 对于掉线的 broker，先在 ControllerChannelManager 中移除该 broker（即关闭与 Broker 的连接、关闭相应的发送线程和清空请求队列），最后 Controller 调用 <code>onBrokerFailure()</code> 下线该 Broker。  整体的处理流程如下图所示：   
![Broker 上线下线处理过程](./images/kafka/broker_online_offline.png)
   
 ## Broker 上线 
 本节主要讲述一台 Broker 上线的过程，如前面图中所示，一台 Broker 上线主要有以下两步：   
``` scala
controllerContext.controllerChannelManager.addBroker
controller.onBrokerStartup(newBrokerIdsSorted)
```
  
 - 在 Controller Channel Manager 中添加该 Broker 节点，主要的内容是：Controller 建立与该 Broker 的连接、初始化相应的请求发送线程与请求队列； 
 - 调用 Controller 的 <code>onBrokerStartup()</code> 方法上线该节点。  Controller Channel Manager 添加 Broker 的实现如下，这里就不重复讲述了，前面讲述 Controller 服务初始化的文章（ [Controller Channel Manager](http://matt33.com/2018/06/15/kafka-controller-start/#Controller-Channel-Manager)）已经讲述过这部分的内容。   
``` scala
def addBroker(broker: Broker) {
  // be careful here. Maybe the startup() API has already started the request send thread
  brokerLock synchronized {
    if(!brokerStateInfo.contains(broker.id)) {
      addNewBroker(broker)
      startRequestSendThread(broker.id)
    }
  }
}
```
 下面再看下 Controller 如何在 onBrokerStartup() 方法中实现 Broker 上线操作的，具体实现如下所示：   
``` scala
//note: 这个是被 副本状态机触发的
//note: 1. 发送 update-metadata 请求给所有存活的 broker;
//note: 2. 对于所有 new/offline partition 触发选主操作, 选举成功的, Partition 状态设置为 Online
//note: 3. 检查是否有分区的重新副本分配分配到了这个台机器上, 如果有, 就进行相应的操作
//note: 4. 检查这台机器上是否有 Topic 被设置为了删除标志, 如果是, 那么机器启动完成后, 重新尝试删除操作
def onBrokerStartup(newBrokers: Seq[Int]) {
  info("New broker startup callback for %s".format(newBrokers.mkString(",")))
  val newBrokersSet = newBrokers.toSet //note: 新启动的 broker
  // send update metadata request to all live and shutting down brokers. Old brokers will get to know of the new
  // broker via this update.
  // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
  // common controlled shutdown case, the metadata will reach the new brokers faster
  //note: 发送 metadata 更新给所有的 broker, 这样的话旧的 broker 将会知道有机器新上线了
  sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
  // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
  // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
  //note:  获取这个机器上的所有 replica 请求
  val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
  //note: 将这些副本的状态设置为 OnlineReplica
  replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers, OnlineReplica)
  // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
  // to see if these brokers can become leaders for some/all of those
  //note: 新的 broker 上线也会触发所有处于 new/offline 的 partition 进行 leader 选举
  partitionStateMachine.triggerOnlinePartitionStateChange()
  // check if reassignment of some partitions need to be restarted
  //note: 检查是否副本的重新分配分配到了这台机器上
  val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter {
    case (_, reassignmentContext) => reassignmentContext.newReplicas.exists(newBrokersSet.contains(_))
  }
  //note: 如果需要副本进行迁移的话,就执行副本迁移操作
  partitionsWithReplicasOnNewBrokers.foreach(p => onPartitionReassignment(p._1, p._2))
  // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
  // on the newly restarted brokers, there is a chance that topic deletion can resume
  //note: 检查 topic 删除操作是否需要重新启动
  val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
  if(replicasForTopicsToBeDeleted.nonEmpty) {
    info(("Some replicas %s for topics scheduled for deletion %s are on the newly restarted brokers %s. " +
      "Signaling restart of topic deletion for these topics").format(replicasForTopicsToBeDeleted.mkString(","),
      deleteTopicManager.topicsToBeDeleted.mkString(","), newBrokers.mkString(",")))
    deleteTopicManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
  }
}
```
 onBrokerStartup() 方法在实现的逻辑上分为以下几步：    
 - 调用 <code>sendUpdateMetadataRequest()</code> 方法向当前集群所有存活的 Broker 发送 Update Metadata 请求，这样的话其他的节点就会知道当前的 Broker 已经上线了； 
 - 获取当前节点分配的所有的 Replica 列表，并将其状态转移为 OnlineReplica 状态； 
 - 触发 PartitionStateMachine 的 <code>triggerOnlinePartitionStateChange()</code> 方法，为所有处于 NewPartition/OfflinePartition 状态的 Partition 进行 leader 选举，如果 leader 选举成功，那么该 Partition 的状态就会转移到 OnlinePartition 状态，否则状态转移失败； 
 - 如果副本迁移中有新的 Replica 落在这台新上线的节点上，那么开始执行副本迁移操作（见[Kafka 源码解析之 Partition 副本迁移实现](http://matt33.com/2018/06/16/partition-reassignment/)）; 
 - 如果之前由于这个 Topic 设置为删除标志，但是由于其中有 Replica 掉线而导致无法删除，这里在节点启动后，尝试重新执行删除操作。  到此为止，一台 Broker 算是真正加入到了 Kafka 的集群中，在上述过程中，涉及到 leader 选举的操作，都会触发 LeaderAndIsr 请求及 Metadata 请求的发送。   
 ## Broker 掉线 
 本节主要讲述一台 Broker 掉线后的处理过程，正如前面图中所示，一台 Broker 掉线后主要有以下两步：   
``` scala
controllerContext.controllerChannelManager.removeBroker
controller.onBrokerFailure(deadBrokerIdsSorted)
```
  
 - 首先在 Controller Channel Manager 中移除该 Broker 节点，主要的内容是：关闭 Controller 与 Broker 的连接和相应的请求发送线程，并清空请求队列； 
 - 调用 Controller 的 <code>onBrokerFailure()</code> 方法下线该节点。  Controller Channel Manager 下线 Broker 的处理如下所示：   
``` scala
def removeBroker(brokerId: Int) {
  brokerLock synchronized {
    removeExistingBroker(brokerStateInfo(brokerId))
  }
}

//note: 移除旧的 broker（关闭网络连接、关闭请求发送线程）
private def removeExistingBroker(brokerState: ControllerBrokerStateInfo) {
  try {
    brokerState.networkClient.close()
    brokerState.messageQueue.clear()
    brokerState.requestSendThread.shutdown()
    brokerStateInfo.remove(brokerState.brokerNode.id)
  } catch {
    case e: Throwable => error("Error while removing broker by the controller", e)
  }
}
```
 在 Controller Channel Manager 处理完掉线的 Broker 节点后，下面 KafkaController 将会调用 onBrokerFailure() 进行相应的处理，其实现如下：   
``` scala
//note: 这个方法会被副本状态机调用（进行 broker 节点下线操作）
//note: 1. 将 leader 在这台机器上的分区设置为 Offline
//note: 2. 通过 OfflinePartitionLeaderSelector 为 new/offline partition 选举新的 leader
//note: 3. leader 选举后,发送 LeaderAndIsr 请求给该分区所有存活的副本;
//note: 4. 分区选举 leader 后,状态更新为 Online
//note: 5. 要下线的 broker 上的所有 replica 改为 Offline 状态
def onBrokerFailure(deadBrokers: Seq[Int]) {
  info("Broker failure callback for %s".format(deadBrokers.mkString(",")))
  //note: 从正在下线的 broker 集合中移除已经下线的机器
  val deadBrokersThatWereShuttingDown =
    deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
  info("Removed %s from list of shutting down brokers.".format(deadBrokersThatWereShuttingDown))
  val deadBrokersSet = deadBrokers.toSet
  // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers
  //note: 1. 将 leader 在这台机器上的、并且未设置删除的分区状态设置为 Offline
  val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
    deadBrokersSet.contains(partitionAndLeader._2.leaderAndIsr.leader) &&
      !deleteTopicManager.isTopicQueuedUpForDeletion(partitionAndLeader._1.topic)).keySet
  partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition)
  // trigger OnlinePartition state changes for offline or new partitions
  //note: 2. 选举 leader, 选举成功后设置为 Online 状态
  partitionStateMachine.triggerOnlinePartitionStateChange()
  // filter out the replicas that belong to topics that are being deleted
  //note: 过滤出 replica 在这个机器上、并且没有被设置为删除的 topic 列表
  var allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokersSet)
  val activeReplicasOnDeadBrokers = allReplicasOnDeadBrokers.filterNot(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
  // handle dead replicas
  //note: 将这些 replica 状态转为 Offline
  replicaStateMachine.handleStateChanges(activeReplicasOnDeadBrokers, OfflineReplica)
  // check if topic deletion state for the dead replicas needs to be updated
  //note: 过滤设置为删除的 replica
  val replicasForTopicsToBeDeleted = allReplicasOnDeadBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic))
  if(replicasForTopicsToBeDeleted.nonEmpty) { //note: 将上面这个 topic 列表的 topic 标记为删除失败
    // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
    // deleted when the broker is down. This will prevent the replica from being in TopicDeletionStarted state indefinitely
    // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
    deleteTopicManager.failReplicaDeletion(replicasForTopicsToBeDeleted)
  }

  // If broker failure did not require leader re-election, inform brokers of failed broker
  // Note that during leader re-election, brokers update their metadata
  if (partitionsWithoutLeader.isEmpty) {
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
  }
}
```
 Controller 对于掉线 Broker 的处理过程主要有以下几步：    
 - 首先找到 Leader 在该 Broker 上所有 Partition 列表，然后将这些 Partition 的状态全部转移为 OfflinePartition 状态； 
 - 触发 PartitionStateMachine 的 <code>triggerOnlinePartitionStateChange()</code> 方法，为所有处于 NewPartition/OfflinePartition 状态的 Partition 进行 Leader 选举，如果 Leader 选举成功，那么该 Partition 的状态就会迁移到 OnlinePartition 状态，否则状态转移失败（Broker 上线/掉线、Controller 初始化时都会触发这个方法）； 
 - 获取在该 Broker 上的所有 Replica 列表，将其状态转移成 OfflineReplica 状态； 
 - 过滤出设置为删除、并且有副本在该节点上的 Topic 列表，先将该 Replica 的转移成 ReplicaDeletionIneligible 状态，然后再将该 Topic 标记为非法删除，即因为有 Replica 掉线导致该 Topic 无法删除； 
 - 如果 leader 在该 Broker 上所有 Partition 列表不为空，证明有 Partition 的 leader 需要选举，在最后一步会触发全局 metadata 信息的更新。  到这里，一台掉线的 Broker 算是真正下线完成了。   
 ## Broker 优雅下线 
 前面部分是关于通过监听节点变化来实现对 Broker 的上下线，这也是 Kafka 上下线 Broker 的主要流程，但是还有一种情况是：主动关闭 Kafka 服务，这种情况又被称为 Broker 的优雅关闭。   优雅关闭的节点会向 Controller 发送 ControlledShutdownRequest 请求，Controller 在收到这个情况会进行相应的处理，如下所示：   
``` scala
def handleControlledShutdownRequest(request: RequestChannel.Request) {
  // ensureTopicExists is only for client facing requests
  // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
  // stop serving data to clients for the topic being deleted
  val controlledShutdownRequest = request.requestObj.asInstanceOf[ControlledShutdownRequest]

  //note: 判断该连接是否经过认证
  authorizeClusterAction(request)

  //note: 处理该请求
  val partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId)
  //note: 返回的 response
  val controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
    Errors.NONE.code, partitionsRemaining)
  requestChannel.sendResponse(new Response(request, new RequestOrResponseSend(request.connectionId, controlledShutdownResponse)))
}
```
 Controller 在接收这个关闭服务的请求，通过 shutdownBroker() 方法进行处理，实现如下所示：   
``` scala
//note: 优雅地关闭 Broker
//note: controller 首先决定将这个 broker 上的 leader 迁移到其他可用的机器上
//note: 返回还没有 leader 的迁移的 TopicPartition 集合
def shutdownBroker(id: Int): Set[TopicAndPartition] = {

  if (!isActive) {
    throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
  }

  controllerContext.brokerShutdownLock synchronized { //note: 拿到 broker shutdown 的唯一锁
    info("Shutting down broker " + id)

    inLock(controllerContext.controllerLock) { //note: 拿到 controllerLock 的排它锁
      if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
        throw new BrokerNotAvailableException("Broker id %d does not exist.".format(id))

      controllerContext.shuttingDownBrokerIds.add(id) //note: 将 broker id 添加到正在关闭的 broker 列表中
      debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds.mkString(","))
      debug("Live brokers: " + controllerContext.liveBrokerIds.mkString(","))
    }

    //note: 获取这个 broker 上所有 Partition 与副本数的 map
    val allPartitionsAndReplicationFactorOnBroker: Set[(TopicAndPartition, Int)] =
      inLock(controllerContext.controllerLock) {
        controllerContext.partitionsOnBroker(id)
          .map(topicAndPartition => (topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition).size))
      }

    //note: 处理这些 TopicPartition，更新 Partition 或 Replica 的状态，必要时进行 leader 选举
    allPartitionsAndReplicationFactorOnBroker.foreach {
      case(topicAndPartition, replicationFactor) =>
        // Move leadership serially to relinquish lock.
        inLock(controllerContext.controllerLock) {
          controllerContext.partitionLeadershipInfo.get(topicAndPartition).foreach { currLeaderIsrAndControllerEpoch =>
            if (replicationFactor > 1) { //note: 副本数大于1
              if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) { //note: leader 正好是下线的节点
                // If the broker leads the topic partition, transition the leader and update isr. Updates zk and
                // notifies all affected brokers
                //todo: 这种情况下 Replica 的状态不需要修改么？（Replica 的处理还是通过监听器还实现的,这里只是在服务关闭前进行 leader 切换和停止副本同步）
                //note: 状态变化（变为 OnlinePartition，并且进行 leader 选举，使用 controlledShutdownPartitionLeaderSelector 算法）
                partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition,
                  controlledShutdownPartitionLeaderSelector)
              } else {
                // Stop the replica first. The state change below initiates ZK changes which should take some time
                // before which the stop replica request should be completed (in most cases)
                try { //note: 要下线的机器停止副本迁移，发送 StopReplica 请求
                  brokerRequestBatch.newBatch()
                  brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), topicAndPartition.topic,
                    topicAndPartition.partition, deletePartition = false)
                  brokerRequestBatch.sendRequestsToBrokers(epoch)
                } catch {
                  case e : IllegalStateException => {
                    // Resign if the controller is in an illegal state
                    error("Forcing the controller to resign")
                    brokerRequestBatch.clear()
                    controllerElector.resign()

                    throw e
                  }
                }
                // If the broker is a follower, updates the isr in ZK and notifies the current leader
                //note: 更新这个副本的状态，变为 OfflineReplica
                replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic,
                  topicAndPartition.partition, id)), OfflineReplica)
              }
            }
          }
        }
    }
    //note: 返回 leader 在这个要下线节点上并且副本数大于 1 的 TopicPartition 集合
    //note: 在已经进行前面 leader 迁移后
    def replicatedPartitionsBrokerLeads() = inLock(controllerContext.controllerLock) {
      trace("All leaders = " + controllerContext.partitionLeadershipInfo.mkString(","))
      controllerContext.partitionLeadershipInfo.filter {
        case (topicAndPartition, leaderIsrAndControllerEpoch) =>
          leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment(topicAndPartition).size > 1
      }.keys
    }
    replicatedPartitionsBrokerLeads().toSet
  }
}
```
 上述方法的处理逻辑如下：    
 - 先将要下线的 Broker 添加到 shuttingDownBrokerIds 集合中，该集合记录了当前正在进行关闭的 broker 列表； 
 - 获取副本在该节点上的所有 Partition 的列表集合； 
 - 遍历上述 Partition 列表进行处理：如果该 Partition 的 leader 是要下线的节点，那么通过 PartitionStateMachine 进行状态转移（OnlinePartition –&gt; OnlinePartition）触发 leader 选举，使用的 leader 选举方法是 [ControlledShutdownLeaderSelector](http://matt33.com/2018/06/15/kafka-controller-start/#ControlledShutdownLeaderSelector)，它会选举 isr 中第一个没有正在关闭的 Replica 作为 leader，否则抛出 StateChangeFailedException 异常； 
 - 否则的话，即要下线的节点不是 leader，那么就向要下线的节点发送 StopReplica 请求停止副本同步，并将该副本设置为 OfflineReplica 状态，这里对 Replica 进行处理的原因是为了让要下线的机器关闭副本同步流程，这样 Kafka 服务才能正常关闭。  我在看这部分的代码是有一个疑问的，那就是如果要下线的节点是 Partition leader 的情况下，并没有对 Replica 进行相应的处理，这里的原因是，这部分 Replica 的处理可以放在 onBrokerFailure() 方法中处理，即使通过优雅下线的方法下线了 Broker，但是监听 ZK 的 BrokerChangeListener 监听器还是会被触发的。