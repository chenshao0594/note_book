本篇接着讲述 Controller 的功能方面的内容，在 Kafka 中，一个 Topic 的新建、扩容或者删除都是由 Controller 来操作的，本篇文章也是主要聚焦在 Topic 的操作处理上（新建、扩容、删除），实际上 Topic 的创建在 [Kafka 源码解析之 topic 创建过程（三）](http://matt33.com/2017/07/21/kafka-topic-create/) 中已经讲述过了，本篇与前面不同的是，本篇主要是从 Controller 角度来讲述，而且是把新建、扩容、删除这三个 Topic 级别的操作放在一起做一个总结。   
 ## Topic 新建与扩容 
 这里把 Topic 新建与扩容放在一起讲解，主要是因为无论 Topic 是新建还是扩容，在 Kafka 内部其实都是 Partition 的新建，底层的实现机制是一样的，Topic 的新建与扩容的整体流程如下图所示：   
![Topic 新建与扩容流程](./images/kafka/topic-create-alter.png)
   Topic 新建与扩容触发条件的不同如下所示：    
 - 对于 Topic 扩容，监控的节点是 <code>/brokers/topics/TOPIC_NAME</code>，监控的是具体的 Topic 节点，通过 PartitionStateMachine 的 <code>registerPartitionChangeListener(topic)</code> 方法注册的相应 listener； 
 - 对于 Topic 新建，监控的节点是 <code>/brokers/topics</code>，监控的是 Topic 列表，通过 PartitionStateMachine 的 <code>registerTopicChangeListener()</code> 方法注册的相应 listener。  下面开始详细讲述这两种情况。   
 ### Topic 扩容 
 Kafka 提供了 Topic 扩容工具，假设一个 Topic（topic_test）只有一个 partition，这时候我们想把它扩容到两个 Partition，可以通过下面两个命令来实现：   
``` scala
./bin/kafka-topics.sh --zookeeper zk01:2181/kafka --topic topic_test --alter --partitions 2
./bin/kafka-topics.sh --zookeeper zk01:2181/kafka --topic topic_test --alter --replica-assignment 1:2,2:1 --partitions 2
```
 这两种方法的区别是：第二种方法直接指定了要扩容的 Partition 2 的副本需要分配到哪台机器上，这样的话我们可以精确控制到哪些 Topic 放下哪些机器上。   无论是使用哪种方案，上面两条命令产生的结果只有一个，将 Topic 各个 Partition 的副本写入到 ZK 对应的节点上，这样的话 /brokers/topics/topic_test 节点的内容就会发生变化，PartitionModificationsListener 监听器就会被触发，该监听器的处理流程如下：   
``` scala
//note: Partition change 监听器,主要是用于 Partition 扩容的监听
class PartitionModificationsListener(protected val controller: KafkaController, topic: String) extends ControllerZkDataListener {

  protected def logName = "AddPartitionsListener"

  def doHandleDataChange(dataPath: String, data: AnyRef) {
    inLock(controllerContext.controllerLock) {
      try {
        info(s"Partition modification triggered $data for path $dataPath")
        val partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(List(topic))
        //note: 获取新增的 partition 列表及其对应的分配副本列表
        val partitionsToBeAdded = partitionReplicaAssignment.filter(p =>
          !controllerContext.partitionReplicaAssignment.contains(p._1))
        //note: 如果该 topic 被标记为删除,那么直接跳过,不再处理,否则创建该 Partition
        if(controller.deleteTopicManager.isTopicQueuedUpForDeletion(topic))
          error("Skipping adding partitions %s for topic %s since it is currently being deleted"
                .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))
        else {
          if (partitionsToBeAdded.nonEmpty) {
            info("New partitions to be added %s".format(partitionsToBeAdded))
            controllerContext.partitionReplicaAssignment.++=(partitionsToBeAdded)
            controller.onNewPartitionCreation(partitionsToBeAdded.keySet)//note: 创建新的 partition
          }
        }
      } catch {
        case e: Throwable => error("Error while handling add partitions for data path " + dataPath, e)
      }
    }
  }

  // this is not implemented for partition change
  def doHandleDataDeleted(parentPath: String): Unit = {}
}
```
 其 doHandleDataChange() 方法的处理流程如下：    
 - 首先获取该 Topic 在 ZK 的 Partition 副本列表，跟本地的缓存做对比，获取新增的 Partition 列表； 
 - 检查这个 Topic 是否被标记为删除，如果被标记了，那么直接跳过，不再处理这个 Partition 扩容的请求； 
 - 调用 KafkaController 的 <code>onNewPartitionCreation()</code> 新建该 Partition。  下面我们看下 onNewPartitionCreation() 方法，其实现如下：   
``` scala
//note: 用于 Topic Partition 的新建
//note: 1. 将新创建的 partition 状态置为 NewPartition 状态;
//note: 2. 将新创建的 Replica 状态置为 NewReplica 状态;
//note: 3. 将该 Partition 从 NewPartition 改为 OnlinePartition 状态,这期间会 为该 Partition 选举 leader 和 isr，更新到 zk 和 controller的缓存中
//note: 4. 将副本状态从 NewReplica 改为 OnlineReplica 状态。
def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]) {
  info("New partition creation callback for %s".format(newPartitions.mkString(",")))
  partitionStateMachine.handleStateChanges(newPartitions, NewPartition)
  replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), NewReplica)
  partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector)
  replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), OnlineReplica)
}
```
 关于 Partition 的新建，总共分了以下四步：    
 - 将新创建的 Partition 状态置为 NewPartition 状态，此时 Partition 刚刚创建，只是分配了相应的 Replica 但是还没有 leader 和 isr，不能正常工作; 
 - 将该 Partition 对应的 Replica 列表状态设置为 NewReplica 状态，这部分只是将 Replica 的状态设置为了 NewReplica，并没有做其他的处理; 
 - 将该 Partition 的状态从 NewPartition 改为 OnlinePartition 状态，这期间会为该 Partition 选举 leader 和 isr，并将结果更新到 ZK 和 Controller 的缓存中，并向该 Partition 的所有副本发送对应的 LeaderAndIsr 信息（发送 LeaderAndIsr 请求的同时也会向所有 Broker 发送该 Topic 的 leader、isr metadata 信息）； 
 - 将副本状态从 NewReplica 转移为 OnlineReplica 状态。  经过上面几个阶段，一个 Partition 算是真正创建出来，可以正常进行读写工作了，当然上面只是讲述了 Controller 端做的内容，Partition 副本所在节点对 LeaderAndIsr 请求会做更多的工作，这部分会在后面关于 LeaderAndIsr 请求的处理中只能够详细讲述。   
 ### Topic 新建 
 Kafka 也提供了 Topic 创建的工具，假设我们要创建一个名叫 topic_test，Partition 数为2的 Topic，创建的命令如下：   
``` scala
./bin/kafka-topics.sh --zookeeper zk01:2181/kafka --topic topic_test --create --partitions 2 --replication-factor 2
./bin/kafka-topics.sh --zookeeper zk01:2181/kafka --topic topic_test --create --replica-assignment 1:2,2:1 --partitions 2
```
 跟前面的类似，方法二是可以精确控制新建 Topic 每个 Partition 副本所在位置，Topic 创建的本质上是在 /brokers/topics 下新建一个节点信息，并将 Topic 的分区详情写入进去，当 /brokers/topics 有了新增的 Topic 节点后，会触发 TopicChangeListener 监听器，其实现如下：   
``` scala
//note: 监控 zk 上 Topic 子节点的变化 ,KafkaController 会进行相应的处理
class TopicChangeListener(protected val controller: KafkaController) extends ControllerZkChildListener {

  protected def logName = "TopicChangeListener"

  //note: 当 zk 上 topic 节点上有变更时,这个方法就会调用
  def doHandleChildChange(parentPath: String, children: Seq[String]) {
    inLock(controllerContext.controllerLock) {
      if (hasStarted.get) {
        try {
          val currentChildren = {
            debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
            children.toSet
          }
          //note: 新创建的 topic 列表
          val newTopics = currentChildren -- controllerContext.allTopics
          //note: 已经删除的 topic 列表
          val deletedTopics = controllerContext.allTopics -- currentChildren
          controllerContext.allTopics = currentChildren

          //note: 新创建 topic 对应的 partition 列表及副本列表添加到 Controller 的缓存中
          val addedPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(newTopics.toSeq)
          //note: Controller 从缓存中把已经删除 partition 过滤掉
          controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
            !deletedTopics.contains(p._1.topic))
          controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)//note: 将新增的 tp-replicas 更新到缓存中
          info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
            deletedTopics, addedPartitionReplicaAssignment))
          if (newTopics.nonEmpty)//note: 处理新建的 topic
            controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet)
        } catch {
          case e: Throwable => error("Error while handling new topic", e)
        }
      }
    }
  }
}
```
 只要 /brokers/topics 下子节点信息有变化（topic 新增或者删除），TopicChangeListener 都会被触发，其 doHandleChildChange() 方法的处理流程如下：    
 - 获取 ZK 当前的所有 Topic 列表，根据本地缓存的 Topic 列表记录，可以得到新增的 Topic 记录与已经删除的 Topic 列表； 
 - 将新增 Topic 的相信信息更新到 Controller 的缓存中，将已经删除的 Topic 从 Controller 的副本缓存中移除； 
 - 调用 KafkaController 的 <code>onNewTopicCreation()</code> 方法创建该 topic。  接着看下 onNewTopicCreation() 方法实现   
``` scala
//note: 当 partition state machine 监控到有新 topic 或 partition 时,这个方法将会被调用
//note: 1. 注册 partition change listener, 监听 Parition 变化;
//note: 2. 触发 the new partition, 也即是 onNewPartitionCreation()
//note: 3. 发送 metadata 请求给所有的 Broker
def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]) {
  info("New topic creation callback for %s".format(newPartitions.mkString(",")))
  // subscribe to partition changes
  topics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
  onNewPartitionCreation(newPartitions)
}
```
 上述方法主要做了两件事：    
 - 注册这个 topic 的 PartitionModificationsListener 监听器； 
 - 通过 <code>onNewPartitionCreation()</code> 创建该 Topic 的所有 Partition。  onNewPartitionCreation() 的实现在前面 Topic 扩容部分已经讲述过，这里不再重复，最好参考前面流程图来梳理 Topic 扩容和新建的整个过程。   
 ## Topic 删除 
 Kafka Topic 删除这部分的逻辑是一个单独线程去做的，这个线程是在 Controller 启动时初始化和启动的。   
 ### TopicDeletionManager 初始化 
 TopicDeletionManager 启动实现如下所示：   
``` scala
/**
 * Invoked at the end of new controller initiation
 */
//note: Controller 初始化完成,触发这个操作,删除 topic 线程启动
def start() {
  if (isDeleteTopicEnabled) {
    deleteTopicsThread = new DeleteTopicsThread()
    if (topicsToBeDeleted.nonEmpty)
      deleteTopicStateChanged.set(true)
    deleteTopicsThread.start() //note: 启动 DeleteTopicsThread
  }
}
```
 TopicDeletionManager 启动时只是初始化了一个 DeleteTopicsThread 线程，并启动该线程。TopicDeletionManager 这个类从名字上去看，它是 Topic 删除的管理器，它是如何实现 Topic 删除管理呢，这里先看下该类的几个重要的成员变量：    
 - topicsToBeDeleted：需要删除的 Topic 列表，每当有新的 topic 需要删除时，Controller 就通过 <code>enqueueTopicsForDeletion()</code> 方法将 Topic 添加到这个列表中，而 DeleteTopicsThread 线程则会从列表拿到需要进行删除的 Topic 信息； 
 - partitionsToBeDeleted：需要删除的 Partition 列表，跟上面的 Topic 列表保持一致，只不过纬度不同； 
 - topicsIneligibleForDeletion：非法删除的 Topic 列表，当一个 Topic 正在进行副本迁移、leader 选举或者有副本 dead 的情况下，该 Topic 都会设置被非法删除状态，只有恢复正常后，这个状态才会解除，处在这个状态的 Topic 是无法删除的。  
 ### Topic 删除整体流程 
 前面一小节，简单介绍了 TopicDeletionManager、DeleteTopicsThread 的启动以及它们之间的关系，这里我们看下一个 Topic 被设置删除后，其处理的整理流程，简单做了一个小图，如下所示：   
![Topic 删除整理流程](./images/kafka/topic-delete.png)
   这里先简单讲述上面的流程，当一个 Topic 设置为删除后：    
 - 首先 DeleteTopicsListener 会被触发，然后通过 <code>enqueueTopicsForDeletion()</code> 方法将 Topic 添加到要删除的 Topic 列表中； 
 - DeleteTopicsThread 这个线程会不断调用 <code>doWork()</code> 方法，这个方法被调用时，它会遍历 <code>topicsToBeDeleted</code> 中的所有 Topic 列表； 
 - 对于之前没有处理过的 Topic（之前还没有开始删除），会通过 TopicDeletionManager 的 <code>onTopicDeletion()</code> 方法执行删除操作； 
 - 如果 Topic 删除完成（所有 Replica 的状态都变为 ReplicaDeletionSuccessful 状态），那么就执行 TopicDeletionManager 的 <code>completeDeleteTopic()</code> 完成删除流程，即更新状态信息，并将 Topic 的 meta 信息从缓存和 ZK 中清除。  
 ### Topic 删除详细实现 
 先看下 DeleteTopicsListener 的实现，如下：   
``` scala
//note: 删除 Topic 包括以下操作:
//note: 1. 如果要删除的 topic 存在,将 Topic 添加到 Topic 将要删除的缓存中;
//note: 2. 如果有 Topic 将要被删除,那么将触发 Topic 删除线程
class DeleteTopicsListener(protected val controller: KafkaController) extends ControllerZkChildListener {
  private val zkUtils = controllerContext.zkUtils

  protected def logName = "DeleteTopicsListener"

  /**
   * Invoked when a topic is being deleted
   * @throws Exception On any error.
   */
  //note: 当 topic 需要被删除时,才会触发
  @throws[Exception]
  def doHandleChildChange(parentPath: String, children: Seq[String]) {
    inLock(controllerContext.controllerLock) {
      var topicsToBeDeleted = children.toSet
      debug("Delete topics listener fired for topics %s to be deleted".format(topicsToBeDeleted.mkString(",")))
      //note: 不存在的、需要删除的 topic, 直接清除 zk 上的记录
      val nonExistentTopics = topicsToBeDeleted -- controllerContext.allTopics
      if (nonExistentTopics.nonEmpty) {
        warn("Ignoring request to delete non-existing topics " + nonExistentTopics.mkString(","))
        nonExistentTopics.foreach(topic => zkUtils.deletePathRecursive(getDeleteTopicPath(topic)))
      }
      topicsToBeDeleted --= nonExistentTopics
      if (controller.config.deleteTopicEnable) { //note: 如果允许 topic 删除
        if (topicsToBeDeleted.nonEmpty) { //note: 有 Topic 需要删除
          info("Starting topic deletion for topics " + topicsToBeDeleted.mkString(","))
          // mark topic ineligible for deletion if other state changes are in progress
          topicsToBeDeleted.foreach { topic => //note: 如果 topic 正在最优 leader 选举或正在迁移,那么将 topic 标记为非法删除状态
            val preferredReplicaElectionInProgress =
              controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic).contains(topic)
            val partitionReassignmentInProgress =
              controllerContext.partitionsBeingReassigned.keySet.map(_.topic).contains(topic)
            if (preferredReplicaElectionInProgress || partitionReassignmentInProgress)
              controller.deleteTopicManager.markTopicIneligibleForDeletion(Set(topic))
          }
          // add topic to deletion list
          //note: 将要删除的 topic 添加到待删除的 topic
          controller.deleteTopicManager.enqueueTopicsForDeletion(topicsToBeDeleted)
        }
      } else {
        // If delete topic is disabled remove entries under zookeeper path : /admin/delete_topics
        for (topic <- topicsToBeDeleted) {
          info("Removing " + getDeleteTopicPath(topic) + " since delete topic is disabled")
          zkUtils.zkClient.delete(getDeleteTopicPath(topic))
        }
      }
    }
  }

  def doHandleDataDeleted(dataPath: String) {}
}
```
 其 doHandleChildChange() 的实现逻辑如下：    
 - 根据要删除的 Topic 列表，过滤出那些不存在的 Topic 列表，直接从 ZK 中清除（只是从 <code>/admin/delete_topics</code> 中移除）； 
 - 如果集群不允许 Topic 删除，直接从 ZK 中清除（只是从 <code>/admin/delete_topics</code> 中移除）这些 Topic 列表，结束流程； 
 - 如果这个列表中有正在进行副本迁移或 leader 选举的 Topic，那么先将这些 Topic 加入到 <code>topicsIneligibleForDeletion</code> 中，即标记为非法删除； 
 - 通过 <code>enqueueTopicsForDeletion()</code> 方法将 Topic 添加到要删除的 Topic 列表（<code>topicsToBeDeleted</code>）、将 Partition 添加到要删除的 Partition 列表中（<code>partitionsToBeDeleted</code>）。  接下来，看下 Topic 删除线程 DeleteTopicsThread 的实现，如下所示：   
``` scala
/note: topic 删除线程
class DeleteTopicsThread() extends ShutdownableThread(name = "delete-topics-thread-" + controller.config.brokerId, isInterruptible = false) {
  val zkUtils = controllerContext.zkUtils
  override def doWork() {
    awaitTopicDeletionNotification()

    if (!isRunning.get)
      return

    inLock(controllerContext.controllerLock) {
      //note: 要删除的 topic 列表
      val topicsQueuedForDeletion = Set.empty[String] ++ topicsToBeDeleted

      if(topicsQueuedForDeletion.nonEmpty)
        info("Handling deletion for topics " + topicsQueuedForDeletion.mkString(","))

      topicsQueuedForDeletion.foreach { topic =>
      // if all replicas are marked as deleted successfully, then topic deletion is done
        if(controller.replicaStateMachine.areAllReplicasForTopicDeleted(topic)) {//note: 如果 Topic 所有副本都删除成功的情况下
          // clear up all state for this topic from controller cache and zookeeper
          //note: 从 controller 的缓存和 zk 中清除这个 topic 的所有记录,这个 topic 彻底删除成功了
          completeDeleteTopic(topic)
          info("Deletion of topic %s successfully completed".format(topic))
        } else {
          if(controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)) {
            //note: Topic 的副本至少有一个状态为 ReplicaDeletionStarted 时
            // ignore since topic deletion is in progress
            //note: 过滤出 Topic 中副本状态为 ReplicaDeletionStarted 的 Partition 列表
            val replicasInDeletionStartedState = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionStarted)
            //note: 表明了上面这些副本正在删除中
            val replicaIds = replicasInDeletionStartedState.map(_.replica)
            val partitions = replicasInDeletionStartedState.map(r => TopicAndPartition(r.topic, r.partition))
            info("Deletion for replicas %s for partition %s of topic %s in progress".format(replicaIds.mkString(","),
              partitions.mkString(","), topic))
          } else { //note:副本既没有全部删除完成、也没有一个副本是在删除过程中，证明这个 topic 还没有开始删除或者删除完成但是至少一个副本删除失败
            // if you come here, then no replica is in TopicDeletionStarted and all replicas are not in
            // TopicDeletionSuccessful. That means, that either given topic haven't initiated deletion
            // or there is at least one failed replica (which means topic deletion should be retried).
            if(controller.replicaStateMachine.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
              //note: 如果有副本删除失败,那么进行重试操作
              // mark topic for deletion retry
              markTopicForDeletionRetry(topic)
            }
          }
        }
        // Try delete topic if it is eligible for deletion.
        if(isTopicEligibleForDeletion(topic)) { //note: 如果 topic 可以被删除
          info("Deletion of topic %s (re)started".format(topic))
          // topic deletion will be kicked off
          //note: 开始删除 topic
          onTopicDeletion(Set(topic))
        } else if(isTopicIneligibleForDeletion(topic)) {
          info("Not retrying deletion of topic %s at this time since it is marked ineligible for deletion".format(topic))
        }
      }
    }
  }
}
```
 doWork() 方法处理逻辑如下：    
 - 遍历所有要删除的 Topic，进行如下处理； 
 - 如果该 Topic 的所有副本都下线成功（状态为 ReplicaDeletionSuccessful）时，那么执行 <code>completeDeleteTopic()</code> 方法完成 Topic 的删除； 
 - 否则，如果 Topic 在删除过程有失败的副本（状态为 ReplicaDeletionIneligible），那么执行 <code>markTopicForDeletionRetry()</code> 将失败的 Replica 状态设置为 OfflineReplica； 
 - 判断 Topic 是否允许删除（不在非法删除的集合中就代表运允许），调用 <code>onTopicDeletion()</code> 执行 Topic 删除。  先看下 onTopicDeletion() 方法，这是 Topic 最开始删除时的实现，如下所示：   
``` scala
//note: Topic 删除
private def onTopicDeletion(topics: Set[String]) {
  info("Topic deletion callback for %s".format(topics.mkString(",")))
  // send update metadata so that brokers stop serving data for topics to be deleted
  val partitions = topics.flatMap(controllerContext.partitionsForTopic) //note: topic 的所有 Partition
  controller.sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions) //note: 更新meta
  val partitionReplicaAssignmentByTopic = controllerContext.partitionReplicaAssignment.groupBy(p => p._1.topic)
  topics.foreach { topic => //note:  删除 topic 的每一个 Partition
    onPartitionDeletion(partitionReplicaAssignmentByTopic(topic).keySet)
  }
}

//note: 这个方法是用于 delete-topic, 用于删除 topic 的所有 partition
private def onPartitionDeletion(partitionsToBeDeleted: Set[TopicAndPartition]) {
  info("Partition deletion callback for %s".format(partitionsToBeDeleted.mkString(",")))
  val replicasPerPartition = controllerContext.replicasForPartition(partitionsToBeDeleted)
  startReplicaDeletion(replicasPerPartition)
}
```
 Topic 的删除的真正实现方法还是在 startReplicaDeletion() 方法中，Topic 删除时，会先调用 onPartitionDeletion() 方法删除所有的 Partition，然后在 Partition 删除时，执行 startReplicaDeletion() 方法删除该 Partition 的副本，该方法的实现如下：   
``` scala
//note: 被 onPartitionDeletion 方法触发,删除副本具体的实现的地方
private def startReplicaDeletion(replicasForTopicsToBeDeleted: Set[PartitionAndReplica]) {
  replicasForTopicsToBeDeleted.groupBy(_.topic).keys.foreach { topic =>
    //note: topic 所有存活的 replica
    val aliveReplicasForTopic = controllerContext.allLiveReplicas().filter(p => p.topic == topic)
    //note: topic 的 dead replica
    val deadReplicasForTopic = replicasForTopicsToBeDeleted -- aliveReplicasForTopic
    //note: topic 中已经处于 ReplicaDeletionSuccessful 状态的副本
    val successfullyDeletedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
    //note: 还没有成功删除的、存活的副本
    val replicasForDeletionRetry = aliveReplicasForTopic -- successfullyDeletedReplicas
    // move dead replicas directly to failed state
    //note: 将 dead replica 设置为 ReplicaDeletionIneligible（删除无效的状态）
    replicaStateMachine.handleStateChanges(deadReplicasForTopic, ReplicaDeletionIneligible)
    // send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader
    //note: 将 replicasForDeletionRetry 设置为 OfflineReplica（发送 StopReplica 请求）
    replicaStateMachine.handleStateChanges(replicasForDeletionRetry, OfflineReplica)
    debug("Deletion started for replicas %s".format(replicasForDeletionRetry.mkString(",")))
    //note: 将 replicasForDeletionRetry 设置为 ReplicaDeletionStarted 状态
    controller.replicaStateMachine.handleStateChanges(replicasForDeletionRetry, ReplicaDeletionStarted,
      new Callbacks.CallbackBuilder().stopReplicaCallback(deleteTopicStopReplicaCallback).build)
    if(deadReplicasForTopic.nonEmpty) { //note: 将 topic 标记为不能删除
      debug("Dead Replicas (%s) found for topic %s".format(deadReplicasForTopic.mkString(","), topic))
      markTopicIneligibleForDeletion(Set(topic))
    }
  }
}
```
 该方法的执行逻辑如下：    
 - 首先获取当前集群所有存活的 broker 信息，根据这个信息可以知道 Topic 哪些副本所在节点是处于 dead 状态； 
 - 找到那些已经成功删除的 Replica 列表（状态为 ReplicaDeletionSuccessful），进而可以得到那些还没有成功删除、并且存活的 Replica 列表（<code>replicasForDeletionRetry</code>）； 
 - 将处于 dead 节点上的 Replica 的状态设置为 ReplicaDeletionIneligible 状态； 
 - 然后重新删除 replicasForDeletionRetry 列表中的副本，先将其状态转移为 OfflineReplica，再转移为 ReplicaDeletionStarted 状态（真正从发送 StopReplica +从物理上删除数据）； 
 - 如果有 Replica 所在的机器处于 dead 状态，那么将 Topic 设置为非法删除状态。  在将副本状态从 OfflineReplica 转移成 ReplicaDeletionStarted 时，会设置一个回调方法 deleteTopicStopReplicaCallback()，该方法会将删除成功的 Replica 设置为 ReplicaDeletionSuccessful 状态，删除失败的 Replica 设置为 ReplicaDeletionIneligible 状态（需要根据 StopReplica 请求处理的过程，看下哪些情况下 Replica 会删除失败，这个会在后面讲解）。   下面看下这个方法 completeDeleteTopic()，当一个 Topic 的所有 Replica 都删除成功时，即其状态都在 ReplicaDeletionSuccessful 时，会调用这个方法，如下所示：   
``` scala
//note: topic 删除后,从 controller 缓存、状态机以及 zk 移除这个 topic 相关记录
private def completeDeleteTopic(topic: String) {
  // deregister partition change listener on the deleted topic. This is to prevent the partition change listener
  // firing before the new topic listener when a deleted topic gets auto created
  //note: 1. 取消 zk 对这个 topic 的 partition-modify-listener
  partitionStateMachine.deregisterPartitionChangeListener(topic)
  //note: 2. 过滤出副本状态为 ReplicaDeletionSuccessful 的副本列表
  val replicasForDeletedTopic = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful)
  // controller will remove this replica from the state machine as well as its partition assignment cache
  //note: controller 将会从副本状态机移除这些副本
  replicaStateMachine.handleStateChanges(replicasForDeletedTopic, NonExistentReplica)
  val partitionsForDeletedTopic = controllerContext.partitionsForTopic(topic)
  // move respective partition to OfflinePartition and NonExistentPartition state
  //note: 3. 从分区状态机中下线并移除这个 topic 的分区
  partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, OfflinePartition)
  partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, NonExistentPartition)
  topicsToBeDeleted -= topic //note: 删除成功,从删除 topic 列表中移除
  partitionsToBeDeleted.retain(_.topic != topic) //note: 从 partitionsToBeDeleted 移除这个 topic
  val zkUtils = controllerContext.zkUtils
  //note: 4. 删除 zk 上关于这个 topic 的相关记录
  zkUtils.zkClient.deleteRecursive(getTopicPath(topic))
  zkUtils.zkClient.deleteRecursive(getEntityConfigPath(ConfigType.Topic, topic))
  zkUtils.zkClient.delete(getDeleteTopicPath(topic))
  //note: 5. 从 controller 的所有缓存中再次移除关于这个 topic 的信息
  controllerContext.removeTopic(topic)
}
```
 当一个 Topic 所有副本都删除后，会进行如下处理：    
 - 取消对该 Topic 的 partition-modify-listener 监听器； 
 - 将状态为 ReplicaDeletionSuccessful 的副本状态都转移成 NonExistentReplica； 
 - 将该 Topic Partition 状态先后转移成 OfflinePartition、NonExistentPartition 状态，正式下线了该 Partition； 
 - 从分区状态机和副本状态机中移除这个 Topic 记录； 
 - 从 Controller 缓存和 ZK 中清除这个 Topic 的相关记录。  至此，一个 Topic 算是真正删除完成。