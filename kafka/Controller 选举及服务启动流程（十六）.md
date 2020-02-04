#  

从本篇文章开始，Kafka 源码解析就正式进入了 Controller 部分，Controller 作为 Kafka Server 端一个重要的组件，它的角色类似于其他分布式系统 Master 的角色，跟其他系统不一样的是，Kafka 集群的任何一台 Broker 都可以作为 Controller，但是在一个集群中同时只会有一个 Controller 是 alive 状态。Controller 在集群中负责的事务很多，比如：集群 meta 信息的一致性保证、Partition leader 的选举、broker 上下线等都是由 Controller 来具体负责。Controller 部分的内容还是比较多的，计划分5篇左右的文章讲述，本文先来看下 Controller 的简介、Controller 的选举、Controller 选举后服务的启动流程以及 Controller 的四种不同 leader 选举机制。分区状态机、副本副本状态机以及对各种 listener 的处理将在后续的文章中展开。   

 ## Controller 简介 
 在于分布式系统中，总会有一个地方需要对全局 meta 做一个统一的维护，Kafka 的 Controller 就是充当这个角色的。Kafka 简单的框架图如下所示   
![Kafka架构简图](./images/kafka/kafka-framwoker.png)
   Controller 是运行在 Broker 上的，任何一台 Broker 都可以作为 Controller，但是一个集群同时只能存在一个 Controller，也就意味着 Controller 与数据节点是在一起的，Controller 做的主要事情如下：    
 - Broker 的上线、下线处理； 
 - 新创建的 topic 或已有 topic 的分区扩容，处理分区副本的分配、leader 选举； 
 - 管理所有副本的状态机和分区的状态机，处理状态机的变化事件； 
 - topic 删除、副本迁移、leader 切换等处理。  
 ## Controller 选举过程 
 Kafka 的每台 Broker 在启动过程中，都会启动 Controller 服务，相关代码如下：   
``` scala
def startup() {
  info("starting")
  val canStartup = isStartingUp.compareAndSet(false, true)
  if (canStartup) {
    /* start kafka controller */
    //note: 启动 controller
    kafkaController = new KafkaController(config, zkUtils, brokerState, time, metrics, threadNamePrefix)
    kafkaController.startup()
  }
}
```

 ### Controller 启动 
 Kafka Server 在启动的过程中，都会去启动 Controller 服务，Controller 启动方法如下：   
``` scala
//NOTE: 当 broker 的 controller 模块启动时触发,它比并不保证当前 broker 是 controller,它仅仅是注册 registerSessionExpirationListener 和启动 controllerElector
def startup() = {
  inLock(controllerContext.controllerLock) {
    info("Controller starting up")
    registerSessionExpirationListener() // note: 注册回话失效的监听器
    isRunning = true
    controllerElector.startup //note: 启动选举过程
    info("Controller startup complete")
  }
}
```
 Controller 在 startup() 方法中主要实现以下两部分功能：    
 - <code>registerSessionExpirationListener()</code> 方法注册连接 zk 的超时监听器； 
 - <code>controllerElector.startup()</code> 方法，监听 zk 上 controller 节点的变化，并触发 controller 选举方法。  
 ### Controller 选举 
 Controller 在启动时，会初始化 ZookeeperLeaderElector 对象，并调用其 startup() 启动相应的流程，具体过程如下：    
``` scala
def startup {
  inLock(controllerContext.controllerLock) {
    controllerContext.zkUtils.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
    elect
  }
}
```
 在 startup() 方法中，主要做了下面两件事情：    
 - 监听 zk 的 <code>/controller</code> 节点的数据变化，一旦节点有变化，立刻通过 LeaderChangeListener 的方法进行相应的处理； 
 - <code>elect</code> 在 controller 不存在的情况下选举 controller，存在的话，就是从 zk 获取当前的 controller 节点信息。  
 #### Controller 选举方法 elect 
 ZookeeperLeaderElector 的 elect 方法实现如下：   
``` scala
//note: 从 zk 获取当前的 controller 信息
def getControllerID(): Int = {
  controllerContext.zkUtils.readDataMaybeNull(electionPath)._1 match {
     case Some(controller) => KafkaController.parseControllerId(controller)
     case None => -1
  }
}

//note: 进行 controller 选举
def elect: Boolean = {
  val timestamp = time.milliseconds.toString
  val electString = Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp))

 leaderId = getControllerID
  /*
   * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition,
   * it's possible that the controller has already been elected when we get here. This check will prevent the following
   * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
   */
  if(leaderId != -1) {
     debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
     return amILeader
  }

  try {
    val zkCheckedEphemeral = new ZKCheckedEphemeral(electionPath,
                                                    electString,
                                                    controllerContext.zkUtils.zkConnection.getZookeeper,
                                                    JaasUtils.isZkSecurityEnabled())
    zkCheckedEphemeral.create() //note: 没有异常的话就是创建成功了
    info(brokerId + " successfully elected as leader")
    leaderId = brokerId
    onBecomingLeader() //note: 成为了 controller
  } catch {
    case _: ZkNodeExistsException => //note: 在创建时,发现已经有 broker 提前注册成功
      // If someone else has written the path, then
      leaderId = getControllerID

      if (leaderId != -1)
        debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
      else
        warn("A leader has been elected but just resigned, this will result in another round of election")

    case e2: Throwable => //note: 抛出了其他异常，那么重新选举 controller
      error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
      resign()
  }
  amILeader
}

def amILeader : Boolean = leaderId == brokerId
```
 其实现逻辑如下：    
 - 先获取 zk 的 <code>/cotroller</code> 节点的信息，获取 controller 的 broker id，如果该节点不存在（比如集群刚创建时），那么获取的 controller id 为-1； 
 - 如果 controller id 不为-1，即 controller 已经存在，直接结束流程； 
 - 如果 controller id 为-1，证明 controller 还不存在，这时候当前 broker 开始在 zk 注册 controller； 
 - 如果注册成功，那么当前 broker 就成为了 controller，这时候开始调用 <code>onBecomingLeader()</code> 方法，正式初始化 controller（注意：<strong>controller 节点是临时节点</strong>，如果当前 controller 与 zk 的 session 断开，那么 controller 的临时节点会消失，会触发 controller 的重新选举）； 
 - 如果注册失败（刚好 controller 被其他 broker 创建了、抛出异常等），那么直接返回。  在这里 controller 算是成功被选举出来了，controller 选举过程实际上就是各个 Broker 抢占式注册该节点，注册成功的便为 Controller。   
 #### controller 节点监听 LeaderChangeListener 
 LeaderChangeListener 主要是监听 zk 上的 Controller 节点变化，如果该节点内容变化或者节点被删除，那么会触发 handleDataChange() 和 handleDataDeleted() 方法，具体实现如下：   
``` scala
//note: 监控 controller 内容的变化
class LeaderChangeListener extends IZkDataListener with Logging {
  /**
   * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
   * @throws Exception On any error.
   */
  @throws[Exception]
  def handleDataChange(dataPath: String, data: Object) {
    val shouldResign = inLock(controllerContext.controllerLock) {
      val amILeaderBeforeDataChange = amILeader
      leaderId = KafkaController.parseControllerId(data.toString)
      info("New leader is %d".format(leaderId))
      // The old leader needs to resign leadership if it is no longer the leader
      amILeaderBeforeDataChange && !amILeader
    }

    //note: 之前是 controller,现在不是了
    if (shouldResign)
      onResigningAsLeader() //note: 关闭 controller 服务
  }

  /**
   * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
   * @throws Exception
   *             On any error.
   */
  //note: 如果之前是 controller,现在这个节点被删除了,那么首先退出 controller 进程,然后开始重新选举 controller
  @throws[Exception]
  def handleDataDeleted(dataPath: String) {
    val shouldResign = inLock(controllerContext.controllerLock) {
      debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
        .format(brokerId, dataPath))
      amILeader
    }

    if (shouldResign)
      onResigningAsLeader()

    inLock(controllerContext.controllerLock) {
      elect
    }
  }
}
```
 处理过程如下：    
 - 如果 <code>/controller</code> 节点内容变化，那么更新一下 controller 最新的节点信息，如果该节点刚好之前是 controller，现在不是了，那么需要执行 controller 关闭操作，即 <code>onResigningAsLeader()</code> 方法； 
 - 如果 <code>/controller</code> 节点被删除，如果该节点刚好之前是 controller，那么需要执行 controller 关闭操作，即 <code>onResigningAsLeader()</code> 方法，然后再执行 <code>elect</code> 方法重新去选举 controller；  
 ## Controller 服务启动流程 
 Controller 节点选举出来之后，ZookeeperLeaderElector 就会调用 onBecomingLeader() 方法初始化 KafkaController 的相关内容，在 KafkaController 对 ZookeeperLeaderElector 的初始化中可以看到 onBecomingLeader() 这个方法实际上是 KafkaController 的 onControllerFailover() 方法。   
``` scala
class KafkaController{
    private val controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover,
                                                               onControllerResignation, config.brokerId, time) //note: controller 通过 zk 选举
}

//note: controller 临时节点监控及 controller 选举
class ZookeeperLeaderElector(controllerContext: ControllerContext,
                             electionPath: String, //note: 路径是 /controller
                             onBecomingLeader: () => Unit, //note: onControllerFailover() 方法
                             onResigningAsLeader: () => Unit, //note: onControllerResignation() 方法
                             brokerId: Int,
                             time: Time)
```

 ### onControllerFailover 启动及初始化 
 下面开始进入 KafkaController 正式初始化的讲解过程中，onControllerFailover() 方法实现如下：   
``` scala
//note: 如果当前 Broker 被选为 controller 时, 当被选为 controller,它将会做以下操作
//note: 1. 注册 controller epoch changed listener;
//note: 2. controller epoch 自增加1;
//note: 3. 初始化 KafkaController 的上下文信息 ControllerContext,它包含了当前的 topic、存活的 broker 以及已经存在的 partition 的 leader;
//note: 4. 启动 controller 的 channel 管理: 建立与其他 broker 的连接的,负责与其他 broker 之间的通信;
//note: 5. 启动 ReplicaStateMachine（副本状态机,管理副本的状态）;
//note: 6. 启动 PartitionStateMachine（分区状态机,管理分区的状态）;
//note: 如果在 Controller 服务初始化的过程中，出现了任何不可预期的 异常/错误，它将会退出当前的进程，这确保了可以再次触发 controller 的选举
def onControllerFailover() {
  if(isRunning) {
    info("Broker %d starting become controller state transition".format(config.brokerId))
    readControllerEpochFromZookeeper() //note: 从 zk 获取 controllrt 的 epoch 和 zkVersion 值
    incrementControllerEpoch(zkUtils.zkClient) //note: 更新 Controller 的 epoch 和 zkVersion 值，可能会抛出异常

    // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
    //note: 再从 zk 获取数据初始化前，注册一些关于 broker/topic 的回调监听器
    registerReassignedPartitionsListener() //note: 监控路径【/admin/reassign_partitions】，分区迁移监听
    registerIsrChangeNotificationListener() //note: 监控路径【/isr_change_notification】，isr 变动监听
    registerPreferredReplicaElectionListener() //note: 监听路径【/admin/preferred_replica_election】，最优 leader 选举
    partitionStateMachine.registerListeners()//note: 监听 Topic 的创建与删除
    replicaStateMachine.registerListeners() //note: 监听 broker 的上下线

    //note: 初始化 controller 相关的变量信息:包括 alive broker 列表、partition 的详细信息等
    initializeControllerContext() //note: 初始化 controller 相关的变量信息

    // We need to send UpdateMetadataRequest after the controller context is initialized and before the state machines
    // are started. The is because brokers need to receive the list of live brokers from UpdateMetadataRequest before
    // they can process the LeaderAndIsrRequests that are generated by replicaStateMachine.startup() and
    // partitionStateMachine.startup().
    //note: 在 controller contest 初始化之后,我们需要发送 UpdateMetadata 请求在状态机启动之前,这是因为 broker 需要从 UpdateMetadata 请求
    //note: 获取当前存活的 broker list, 因为它们需要处理来自副本状态机或分区状态机启动发送的 LeaderAndIsr 请求
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

    //note: 初始化 replica 的状态信息: replica 是存活状态时是 OnlineReplica, 否则是 ReplicaDeletionIneligible
    replicaStateMachine.startup() //note: 初始化 replica 的状态信息
    //note: 初始化 partition 的状态信息:如果 leader 所在 broker 是 alive 的,那么状态为 OnlinePartition,否则为 OfflinePartition
    //note: 并状态为 OfflinePartition 的 topic 选举 leader
    partitionStateMachine.startup() //note: 初始化 partition 的状态信息

    // register the partition change listeners for all existing topics on failover
    //note: 为所有的 topic 注册 partition change 监听器
    controllerContext.allTopics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
    info("Broker %d is ready to serve as the new controller with epoch %d".format(config.brokerId, epoch))
    maybeTriggerPartitionReassignment() //note: 触发一次分区副本迁移的操作
    maybeTriggerPreferredReplicaElection() //note: 触发一次分区的最优 leader 选举操作
    if (config.autoLeaderRebalanceEnable) { //note: 如果开启自动均衡
      info("starting the partition rebalance scheduler")
      autoRebalanceScheduler.startup()
      autoRebalanceScheduler.schedule("partition-rebalance-thread", checkAndTriggerPartitionRebalance,
        5, config.leaderImbalanceCheckIntervalSeconds.toLong, TimeUnit.SECONDS) //note: 发送最新的 meta 信息
    }
    deleteTopicManager.start() //note: topic 删除线程启动
  }
  else
    info("Controller has been shut down, aborting startup/failover")
}
```
 简单来说，onControllerFailover() 所做的事情如下：    
 - <code>readControllerEpochFromZookeeper()</code> 方法更新 controller 的 epoch 及 zkVersion 信息，<code>incrementControllerEpoch()</code> 方法将 controller 的 epoch 字增加1，并更新到 zk 中； 
 - 在控制器中注册相关的监听器，主要有6类类型，如下面表格中所列； 
 - 通过 <code>initializeControllerContext()</code> 方法初始化 Controller 的上下文信息，更新 Controller 的相关缓存信息、并启动 ControllerChannelManager 等； 
 - 向所有 alive 的 broker 发送 Update-Metadata 请求，broker 通过这个请求获取当前集群中 alive 的 broker 列表； 
 - 启动副本状态机，初始化所有 Replica 的状态信息，如果 Replica 所在节点是 alive 的，那么状态更新为 OnlineReplica, 否则更新为 ReplicaDeletionIneligible； 
 - 启动分区状态机，初始化所有 Partition 的状态信息，如果 leader 所在 broker 是 alive 的，那么状态更新为 OnlinePartition，否则更新为 OfflinePartition； 
 - 为当前所有 topic 注册一个 PartitionModificationsListener 监听器，监听所有 Topic 分区数的变化； 
 - KafkaController 初始化完成，正式启动； 
 - KafkaController 启动后，触发一次副本迁移，如果需要的情况下； 
 - KafkaController 启动后，触发一次最优 leader 选举操作，如果需要的情况下； 
 - KafkaController 启动后，如果开启了自动 leader 均衡，启动自动 leader 均衡线程，它会根据配置的信息定期运行。  KafkaController 需要监听的 zk 节点、触发的监听方法及作用如下：   
| 监听方法| 监听路径| 作用 |
| -----| -----| ----- |
| registerReassignedPartitionsListener | /admin/reassign_partitions | 用于分区副本迁移 |
| registerIsrChangeNotificationListener | /isr_change_notification | 用于 Partition ISR 变动 |
| registerPreferredReplicaElectionListener | /admin/preferred_replica_election | 用于 Partition 最优 leader 选举 |
| partitionStateMachine.registerTopicChangeListener() | /brokers/topics | 用于 Topic 新建的监听 |
| partitionStateMachine.registerDeleteTopicListener() | /admin/delete_topics | 用于 Topic 删除的监听 |
| replicaStateMachine.registerBrokerChangeListener() | /brokers/ids | 用于 broker 上下线的监听 |
| partitionStateMachine.registerPartitionChangeListener(topic) | /brokers/topics/TOPIC_NAME | 用于 Topic Partition 扩容的监听 |
 在 KafkaController 中    
 - 有两个状态机：分区状态机和副本状态机； 
 - 一个管理器：Channel 管理器，负责管理所有的 Broker 通信； 
 - 相关缓存：Partition 信息、Topic 信息、broker id 信息等； 
 - 四种 leader 选举机制：分别是用 leader offline、broker 掉线、partition reassign、最优 leader 选举时触发；  如下图所示：   
![Kafka Controller 的重要内容](./images/kafka/controller-cache.png)
   
 ### initializeControllerContext 初始化 Controller 上下文信息 
 在 initializeControllerContext() 初始化 KafkaController 上下文信息的方法中，主要做了以下事情：    
 - 从 zk 获取所有 alive broker 列表，记录到 <code>liveBrokers</code>； 
 - 从 zk 获取所有的 topic 列表，记录到 <code>allTopic</code> 中； 
 - 从 zk 获取所有 Partition 的 replica 信息，更新到 <code>partitionReplicaAssignment</code> 中； 
 - 从 zk 获取所有 Partition 的 LeaderAndIsr 信息，更新到 <code>partitionLeadershipInfo</code> 中； 
 - 调用 <code>startChannelManager()</code> 启动 Controller 的 Channel Manager； 
 - 通过 <code>initializePreferredReplicaElection()</code> 初始化需要最优 leader 选举的 Partition 列表，记录到 <code>partitionsUndergoingPreferredReplicaElection</code> 中； 
 - 通过 <code>initializePartitionReassignment()</code> 方法初始化需要进行副本迁移的 Partition 列表，记录到 <code>partitionsBeingReassigned</code> 中； 
 - 通过 <code>initializeTopicDeletion()</code> 方法初始化需要删除的 topic 列表及 TopicDeletionManager 对象；  综上，这个方法最主要的作用就是相关的 meta 信息及启动 Channel 管理器，其具体实现如下所示：   
``` scala
//note: 初始化 KafkaController 的上下文数据
private def initializeControllerContext() {
  // update controller cache with delete topic information
  controllerContext.liveBrokers = zkUtils.getAllBrokersInCluster().toSet //note: 初始化 zk 的 broker_list 信息
  controllerContext.allTopics = zkUtils.getAllTopics().toSet //note: 初始化所有的 topic 信息
  //note: 初始化所有 topic 的所有 partition 的 replica 分配
  controllerContext.partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(controllerContext.allTopics.toSeq)
  //note: 下面两个都是新创建的空集合
  controllerContext.partitionLeadershipInfo = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
  controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]
  // update the leader and isr cache for all existing partitions from Zookeeper
  updateLeaderAndIsrCache() //note: 获取 topic-partition 的详细信息,更新到 partitionLeadershipInfo 中
  // start the channel manager
  startChannelManager() //note: 启动连接所有的 broker 的线程, 根据 broker/ids 的临时去判断要连接哪些 broker
  initializePreferredReplicaElection() //note: 初始化需要进行最优 leader 选举的 partition
  initializePartitionReassignment() //note: 初始化需要进行分区副本迁移的 partition
  initializeTopicDeletion() //note: 初始化要删除的 topic 及后台的 topic 删除线程,还有不能删除的 topic 集合
  info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
  info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds))
  info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
}
```

<blockquote> 最优 leader 选举：就是默认选择 Replica 分配中第一个 replica 作为 leader，为什么叫做最优 leader 选举呢？因为 Kafka 在给每个 Partition 分配副本时，它会保证分区的主副本会均匀分布在所有的 broker 上，这样的话只要保证第一个 replica 被选举为 leader，读写流量就会均匀分布在所有的 Broker 上，当然这是有一个前提的，那就是每个 Partition 的读写流量相差不多，但是在实际的生产环境，这是不太可能的，所以一般情况下，大集群是不建议开自动 leader 均衡的，可以通过额外的算法计算、手动去触发最优 leader 选举。   
</blockquote> 
 ### Controller Channel Manager 
 initializeControllerContext() 方法会通过 startChannelManager() 方法初始化 ControllerChannelManager 对象，如下所示：   
``` scala
//note: 启动 ChannelManager 线程
private def startChannelManager() {
  controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics, threadNamePrefix)
  controllerContext.controllerChannelManager.startup()
}
```
 ControllerChannelManager 在初始化时，会为集群中的每个节点初始化一个 ControllerBrokerStateInfo 对象，该对象包含四个部分：    
 - NetworkClient：网络连接对象； 
 - Node：节点信息； 
 - BlockingQueue：请求队列； 
 - RequestSendThread：请求的发送线程。  其具体实现如下所示：   
``` scala
//note: 控制所有已经存活 broker 的网络连接
class ControllerChannelManager(controllerContext: ControllerContext, config: KafkaConfig, time: Time, metrics: Metrics, threadNamePrefix: Option[String] = None) extends Logging {
  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  controllerContext.liveBrokers.foreach(addNewBroker) //note: 获取目前已经存活的所有 broker
  //note: 添加一个新的 broker（初始化时,这个方法相当于连接当前存活的所有 broker）
  //note: 建立网络连接、启动请求发送线程
  private def addNewBroker(broker: Broker) {
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug("Controller %d trying to connect to broker %d".format(config.brokerId, broker.id))
    val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerListenerName)
    val brokerNode = new Node(broker.id, brokerEndPoint.host, brokerEndPoint.port)
    val networkClient = { //note: 初始化 NetworkClient
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        config.interBrokerSecurityProtocol,
        LoginType.SERVER,
        config.values,
        config.saslMechanismInterBrokerProtocol,
        config.saslInterBrokerHandshakeRequestEnable
      )
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> broker.id.toString).asJava,
        false,
        channelBuilder
      )
      new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        time,
        false
      )
    }
    val threadName = threadNamePrefix match {
      case None => "Controller-%d-to-broker-%d-send-thread".format(config.brokerId, broker.id)
      case Some(name) => "%s:Controller-%d-to-broker-%d-send-thread".format(name, config.brokerId, broker.id)
    }

    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, threadName) //note: 初始化 requestThread
    requestThread.setDaemon(false) //note: 非守护进程
    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue, requestThread))
  }
}
```
 清楚了上面的逻辑，再来看 KafkaController 部分是如何向 Broker 发送请求的？   
``` scala
sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                callback: AbstractResponse => Unit = null) = {
  controllerContext.controllerChannelManager.sendRequest(brokerId, apiKey, request, callback)
}
```
 KafkaController 实际上是调用的 ControllerChannelManager 的 sendRequest() 方法向 Broker 发送请求信息，其实现如下所示：   
``` scala
//note: 向 broker 发送请求（并没有真正发送,只是添加到对应的 queue 中）
def sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                callback: AbstractResponse => Unit = null) {
  brokerLock synchronized {
    val stateInfoOpt = brokerStateInfo.get(brokerId)
    stateInfoOpt match {
      case Some(stateInfo) =>
        stateInfo.messageQueue.put(QueueItem(apiKey, request, callback))
      case None =>
        warn("Not sending request %s to broker %d, since it is offline.".format(request, brokerId))
    }
  }
}
```
 它实际上只是把对应的请求添加到该 Broker 对应的 MessageQueue 中，并没有真正的去发送请求，请求的的发送是在 每台 Broker 对应的 RequestSendThread 中处理的。   
 ## Controller 原生的四种 leader 选举机制 
 KafkaController 在初始化时，也会初始化四种不同的 leader 选举机制，如下所示：   
``` scala
//note: partition leader 挂掉时，选举 leader
val offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext, config)
//note: 重新分配分区时，leader 选举
private val reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext)
//note: 使用最优的副本作为 leader
private val preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext)
//note: broker 掉线时，重新选举 leader
private val controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext)
```
 四种 leader 选举实现类及对应触发条件如下所示：   
| 实现| 触发条件 |
| -----| ----- |
| OfflinePartitionLeaderSelector | leader 掉线时触发 |
| ReassignedPartitionLeaderSelector | 分区的副本重新分配数据同步完成后触发的 |
| PreferredReplicaPartitionLeaderSelector | 最优 leader 选举，手动触发或自动 leader 均衡调度时触发 |
| ControlledShutdownLeaderSelector | broker 发送 ShutDown 请求主动关闭服务时触发 |

 ### OfflinePartitionLeaderSelector 
 OfflinePartitionLeaderSelector Partition leader 选举的逻辑是：    
 - 如果 isr 中至少有一个副本是存活的，那么从该 Partition 存活的 isr 中选举第一个副本作为新的 leader，存活的 isr 作为新的 isr； 
 - 否则，如果脏选举（unclear elect）是禁止的，那么就抛出 NoReplicaOnlineException 异常； 
 - 否则，即允许脏选举的情况下，从存活的、所分配的副本（不在 isr 中的副本）中选出一个副本作为新的 leader 和新的 isr 集合； 
 - 否则，即是 Partition 分配的副本没有存活的，抛出 NoReplicaOnlineException 异常；  一旦 leader 被成功注册到 zk 中，它将会更新到 KafkaController 缓存中的 allLeaders 中。   
``` scala
//note: 对于 LeaderAndIsrRequest， 选举一个新的 leader、isr 和 receiving replicas
//note: 1.如果 isr 中至少有一个副本是存活的，那么存活的 isr 中选举一个副本作为新的 leader，存活的 isr 作为新的 isr；
//note: 2.否则，如果脏选举（unclear elect）是禁止的，那么就抛出 NoReplicaOnlineException 异常；
//note: 3.否则，从存活的、所分配的副本中选出一个副本作为新的 leader 和新的 isr 集合；
//note: 4.否则，partition 分配的副本没有存活的，抛出 NoReplicaOnlineException 异常；
//note: 一旦 leader 被成功注册到 zk 中，它将更新缓存中的 allLeaders。
class OfflinePartitionLeaderSelector(controllerContext: ControllerContext, config: KafkaConfig)
  extends PartitionLeaderSelector with Logging {
  this.logIdent = "[OfflinePartitionLeaderSelector]: "

  //note: leader 选举，过程如上面所述
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    controllerContext.partitionReplicaAssignment.get(topicAndPartition) match {
      case Some(assignedReplicas) =>
        //note: AR 中还存活的副本
        val liveAssignedReplicas = assignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
        //note: 当前 isr 中还存活的副本
        val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => controllerContext.liveBrokerIds.contains(r))
        val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch //note: epoch
        val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion //note: zkVersion
        //note: 选取新的 leader 和 isr
        val newLeaderAndIsr =
          if (liveBrokersInIsr.isEmpty) { //note: 当前 isr 中副本都挂了
            // Prior to electing an unclean (i.e. non-ISR) leader, ensure that doing so is not disallowed by the configuration
            // for unclean leader election.
            if (!LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(controllerContext.zkUtils,
              ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) { //note: 不允许脏选举的话，抛异常
              throw new NoReplicaOnlineException(("No broker in ISR for partition " +
                "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                " ISR brokers are: [%s]".format(currentLeaderAndIsr.isr.mkString(",")))
            }
            debug("No broker in ISR is alive for %s. Pick the leader from the alive assigned replicas: %s"
              .format(topicAndPartition, liveAssignedReplicas.mkString(",")))
            if (liveAssignedReplicas.isEmpty) { //note: 副本全挂了，抛异常
              throw new NoReplicaOnlineException(("No replica for partition " +
                "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                " Assigned replicas are: [%s]".format(assignedReplicas))
            } else { //note: 从存活的副本中选举 leader（不能保证选举的是 LEO 最大的副本），并将该副本作为 isr
              ControllerStats.uncleanLeaderElectionRate.mark()
              val newLeader = liveAssignedReplicas.head //note: 选择第一个作为 leader
              warn("No broker in ISR is alive for %s. Elect leader %d from live brokers %s. There's potential data loss."
                .format(topicAndPartition, newLeader, liveAssignedReplicas.mkString(",")))
              new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, List(newLeader), currentLeaderIsrZkPathVersion + 1)
            }
          } else { //note: 当前 isr 中还有副本存活
            val liveReplicasInIsr = liveAssignedReplicas.filter(r => liveBrokersInIsr.contains(r))
            val newLeader = liveReplicasInIsr.head //note: 第一个作为 leader
            debug("Some broker in ISR is alive for %s. Select %d from ISR %s to be the leader."
              .format(topicAndPartition, newLeader, liveBrokersInIsr.mkString(",")))
            new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr.toList, currentLeaderIsrZkPathVersion + 1)
          }
        info("Selected new leader and ISR %s for offline partition %s".format(newLeaderAndIsr.toString(), topicAndPartition))
        (newLeaderAndIsr, liveAssignedReplicas)
      case None =>
        throw new NoReplicaOnlineException("Partition %s doesn't have replicas assigned to it".format(topicAndPartition))
    }
  }
}
`
```

 ### ReassignedPartitionLeaderSelector 
 ReassignedPartitionLeaderSelector 是在 Partition 副本迁移后，副本同步完成（RAR 都处在 isr 中，RAR 指的是该 Partition 新分配的副本）后触发的，其 leader 选举逻辑如下：    
 - leader 选择存活的 RAR 中的第一个副本，此时 RAR 都在 isr 中了； 
 - new isr 是所有存活的 RAR 副本列表；  
``` scala
//note: 重新分配分区时，partition 的 leader 选举策略
//note: new leader = 新分配并且在 isr 中的一个副本
//note: new isr = 当前的 isr
//note: 接收 LeaderAndIsr request 的副本 = reassigned replicas
class ReassignedPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {
  this.logIdent = "[ReassignedPartitionLeaderSelector]: "

  /**
   * The reassigned replicas are already in the ISR when selectLeader is called.
   */
  //note: 当这个方法被调用时，要求新分配的副本已经在 isr 中了
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    //note: 新分配的 replica 列表
    val reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned(topicAndPartition).newReplicas
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    //note: 当前的 zk version
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
    //note: 新分配的 replica 列表，并且其 broker 存活、且在 isr 中
    val aliveReassignedInSyncReplicas = reassignedInSyncReplicas.filter(r => controllerContext.liveBrokerIds.contains(r) &&
                                                                             currentLeaderAndIsr.isr.contains(r))
    //note: 选择第一个作为新的 leader
    val newLeaderOpt = aliveReassignedInSyncReplicas.headOption
    newLeaderOpt match {
      case Some(newLeader) => (new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, currentLeaderAndIsr.isr,
        currentLeaderIsrZkPathVersion + 1), reassignedInSyncReplicas)
      case None =>
        reassignedInSyncReplicas.size match {
          case 0 =>
            throw new NoReplicaOnlineException("List of reassigned replicas for partition " +
              " %s is empty. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
          case _ =>
            throw new NoReplicaOnlineException("None of the reassigned replicas for partition " +
              "%s are in-sync with the leader. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
        }
    }
  }
}
```

 ### PreferredReplicaPartitionLeaderSelector 
 PreferredReplicaPartitionLeaderSelector 是最优 leader 选举，选择 AR（assign replica）中的第一个副本作为 leader，前提是该 replica 在是存活的、并且在 isr 中，否则会抛出 StateChangeFailedException 的异常。   
``` scala
//note: 最优的 leader 选举策略（主要用于自动 leader 均衡，选择 AR 中第一个为 leader，前提是它在 isr 中，这样整个集群的 leader 是均衡的,否则抛出异常）
//note: new leader = 第一个 replica（alive and in isr）
//note: new isr = 当前 isr
//note: 接收 LeaderAndIsr request 的 replica = AR
class PreferredReplicaPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector
with Logging {
  this.logIdent = "[PreferredReplicaPartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    //note: Partition 的 AR
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    //note: preferredReplica，第一个 replica
    val preferredReplica = assignedReplicas.head
    // check if preferred replica is the current leader
    //note: 当前的 leader
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    if (currentLeader == preferredReplica) {
      throw new LeaderElectionNotNeededException("Preferred replica %d is already the current leader for partition %s"
                                                   .format(preferredReplica, topicAndPartition))
    } else { //note: 当前 leader 不是 preferredReplica 的情况
      info("Current leader %d for partition %s is not the preferred replica.".format(currentLeader, topicAndPartition) +
        " Triggering preferred replica leader election")
      // check if preferred replica is not the current leader and is alive and in the isr
      //note: preferredReplica 是 alive 并且在 isr 中
      if (controllerContext.liveBrokerIds.contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
        (new LeaderAndIsr(preferredReplica, currentLeaderAndIsr.leaderEpoch + 1, currentLeaderAndIsr.isr,
          currentLeaderAndIsr.zkVersion + 1), assignedReplicas)
      } else {
        throw new StateChangeFailedException("Preferred replica %d for partition ".format(preferredReplica) +
          "%s is either not alive or not in the isr. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
      }
    }
  }
}
```

 ### ControlledShutdownLeaderSelector 
 ControlledShutdownLeaderSelector 是在处理 broker 下线时调用的 leader 选举方法，它会选举 isr 中第一个没有正在关闭的 replica 作为 leader，否则抛出 StateChangeFailedException 异常。   
``` scala
//note: Broker 掉线时，重新选举 leader 调用的 leader 选举方法
//note: new leader = 在 isr 中，并且没有正在 shutdown 的 replica
//note: new isr = 当前 isr 除去关闭的 replica
//note: 接收 LeaderAndIsr request 的 replica = 存活的 AR
class ControlledShutdownLeaderSelector(controllerContext: ControllerContext)
        extends PartitionLeaderSelector
        with Logging {

  this.logIdent = "[ControlledShutdownLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion

    val currentLeader = currentLeaderAndIsr.leader

    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
    //note: 存活的 AR
    val liveAssignedReplicas = assignedReplicas.filter(r => liveOrShuttingDownBrokerIds.contains(r))

    //note: 从当前 isr 中过滤掉正在 shutdown 的 broker
    val newIsr = currentLeaderAndIsr.isr.filter(brokerId => !controllerContext.shuttingDownBrokerIds.contains(brokerId))
    liveAssignedReplicas.find(newIsr.contains) match { //note: find 方法返回的是第一满足条件的元素，AR 中第一个在 newIsr 集合中的元素被选为 leader
      case Some(newLeader) =>
        debug("Partition %s : current leader = %d, new leader = %d".format(topicAndPartition, currentLeader, newLeader))
        (LeaderAndIsr(newLeader, currentLeaderEpoch + 1, newIsr, currentLeaderIsrZkPathVersion + 1), liveAssignedReplicas)
      case None =>
        throw new StateChangeFailedException(("No other replicas in ISR %s for %s besides" +
          " shutting down brokers %s").format(currentLeaderAndIsr.isr.mkString(","), topicAndPartition, controllerContext.shuttingDownBrokerIds.mkString(",")))
    }
  }
}
```