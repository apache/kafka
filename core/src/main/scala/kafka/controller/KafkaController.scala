/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller

import java.util
import java.util.concurrent.TimeUnit

import kafka.admin.AdminOperationException
import kafka.api._
import kafka.common._
import kafka.controller.KafkaController.AlterIsrCallback
import kafka.cluster.Broker
import kafka.controller.KafkaController.{AlterReassignmentsCallback, ElectLeadersCallback, ListReassignmentsCallback, UpdateFeaturesCallback}
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server._
import kafka.utils._
import kafka.utils.Implicits._
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zk.{FeatureZNodeStatus, _}
import kafka.zookeeper.{StateChangeHandler, ZNodeChangeHandler, ZNodeChildChangeHandler}
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException, StaleBrokerEpochException}
import org.apache.kafka.common.message.{AlterIsrRequestData, AlterIsrResponseData}
import org.apache.kafka.common.feature.{Features, FinalizedVersionRange}
import org.apache.kafka.common.message.UpdateFeaturesRequestData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractControlRequest, ApiError, LeaderAndIsrResponse, UpdateFeaturesRequest, UpdateMetadataResponse}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.{Map, Seq, Set, immutable, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

/**
 * 主题分区副本选举触发器，为分区选举Leader副本
 */
sealed trait ElectionTrigger

final case object AutoTriggered extends ElectionTrigger

final case object ZkTriggered extends ElectionTrigger

final case object AdminClientTriggered extends ElectionTrigger

object KafkaController extends Logging {
  val InitialControllerEpoch = 0
  val InitialControllerEpochZkVersion = 0

  type ElectLeadersCallback = Map[TopicPartition, Either[ApiError, Int]] => Unit
  type ListReassignmentsCallback = Either[Map[TopicPartition, ReplicaAssignment], ApiError] => Unit
  type AlterReassignmentsCallback = Either[Map[TopicPartition, ApiError], ApiError] => Unit
  type AlterIsrCallback = Either[Map[TopicPartition, Either[Errors, LeaderAndIsr]], Errors] => Unit
  type UpdateFeaturesCallback = Either[ApiError, Map[String, ApiError]] => Unit
}

/**
 * Controller 发送请求类型：它会给集群中所有Broker（包括它自己所在的Broker）机器发送网络请求。
 * Controller只会向Broker发送三类请求RPC：
 * ① LeaderAndIsr：告诉Broker相关主题各个分区的Leader副本位于哪台Broker、ISR位于哪些Broker。
 * 这是非常重要的且优先级最高的控制类请求。
 * ② StopReplica：告知指定的Broker停止它上面的副本对象，甚至还能删除副本底层的日志数据。
 * 主要的使用场景是：①分区副本迁移;②删除主题。这两个场景都涉及到停掉Broker上副本操作。
 * ③ UpdateMetadata：会更新Broker上的元数据缓存。集群上的所有元数据变更，都首先发生在Controller端，然后
 * 再经由请求广播给集群上的所有Broker。
 * Controller会为集群中的每个Broker创建一个对应的RequestSendTWhread线程，这个线程不断从阻塞队列中获取待发送的请求。
 *
 * Controller单线程事件队列处理模型及基础组件：
 * ZookeeperWatcher线程/KafkaReqeustHandler线程/定时任务线程/其它线程->事件队列（一个）->ControllerEventThread线程
 *
 * @param config             当前Broker配置信息
 * @param zkClient           Zookeeper客户端
 * @param time               时间工具类
 * @param metrics            指标监控类
 * @param initialBrokerInfo  初始的Broker详情（从ZK中获取）
 * @param initialBrokerEpoch 初始的Broker版本号，用来隔离旧的Controller发送的数据，确
 *                           保数据一致性
 * @param tokenManager
 * @param brokerFeatures
 * @param featureCache
 * @param threadNamePrefix
 */
class KafkaController(val config: KafkaConfig,
                      zkClient: KafkaZkClient,
                      time: Time,
                      metrics: Metrics,
                      initialBrokerInfo: BrokerInfo,
                      initialBrokerEpoch: Long,
                      tokenManager: DelegationTokenManager,
                      brokerFeatures: BrokerFeatures,
                      featureCache: FinalizedFeatureCache,
                      threadNamePrefix: Option[String] = None)
  extends ControllerEventProcessor with Logging with KafkaMetricsGroup {

  this.logIdent = s"[Controller id=${config.brokerId}] "

  /**
   * Broker相关信息
   */
  @volatile private var brokerInfo = initialBrokerInfo
  @volatile private var _brokerEpoch = initialBrokerEpoch

  private val isAlterIsrEnabled = config.interBrokerProtocolVersion.isAlterIsrSupported
  private val stateChangeLogger = new StateChangeLogger(config.brokerId, inControllerContext = true, None)

  /**
   * Controller 上下文：保存Controller元数据的容器，所有的元数据信息都封装在这个类中
   * 特别重要的一个类
   */
  val controllerContext = new ControllerContext

  /**
   * 底层更接近网络层，可以理解为用于管理Controller和Broker连接的SocketChannel
   * 是一个微小的生产者-消费者模型（也就是有阻塞队列用于解耦）
   */
  var controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics, stateChangeLogger, threadNamePrefix)

  // have a separate scheduler for the controller to be able to start and stop independently of the kafka server
  /**
   * 线程调度器：当前唯一负责定期执行分区重平衡Leader选举
   * 有一个单独的调度程序，Controller 能够独立于 kafka 服务器启动和停止
   */
  private[controller] val kafkaScheduler = new KafkaScheduler(1)

  /**
   * Controller事件管理器，是事件处理环境所构建的生产者-消费者模型的一环
   * 主要是提供相关方法用于添加Controller事件（内部有一个队列存放Controller相关的事件）
   * 当其它线程（组件）感知到某个Controller事件发生，就会通过ControllerEventManager的API
   * 将事件放入到任务队列中，内部有一个单线程不断轮询并处理事件。
   */
  private[controller] val eventManager = new ControllerEventManager(config.brokerId, this, time,
    controllerContext.stats.rateAndTimeMetrics)

  /**
   *
   */
  private val brokerRequestBatch =
    new ControllerBrokerRequestBatch(config, controllerChannelManager, eventManager, controllerContext, stateChangeLogger)

  /**
   * 副本状态机：负责副本状态转换
   */
  val replicaStateMachine: ReplicaStateMachine = new ZkReplicaStateMachine(config, stateChangeLogger, controllerContext, zkClient,
    new ControllerBrokerRequestBatch(config, controllerChannelManager, eventManager, controllerContext, stateChangeLogger))

  /**
   * 分区状态机：负责分区状态转换
   */
  val partitionStateMachine: PartitionStateMachine = new ZkPartitionStateMachine(config, stateChangeLogger, controllerContext, zkClient,
    new ControllerBrokerRequestBatch(config, controllerChannelManager, eventManager, controllerContext, stateChangeLogger))

  /**
   * 主题移除管理器：负责删除主题及日志文件
   */
  val topicDeletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
    partitionStateMachine, new ControllerDeletionClient(this, zkClient))

  /**
   * 定义各种Handler，这些Handler会注册到Zookeeper的监听器。
   * Zookeeper第一时间感知节点发生变化，然后就会触发下面对应的Handler的执行
   */
  // controller变更 ZK监听器：监听 /controller节点，包括创建、删除和数据变更等情况
  private val controllerChangeHandler = new ControllerChangeHandler(eventManager)

  // 「/brokers/ids」节点变更处理器
  private val brokerChangeHandler = new BrokerChangeHandler(eventManager)

  /**
   * Broker信息变更 ZK监听器，每个Broker对应一个BrokerModificationsHandler
   * 比如Broker的配置信息发生变化就会触发相关事件执行
   */
  private val brokerModificationsHandlers: mutable.Map[Int, BrokerModificationsHandler] = mutable.Map.empty

  // 主题数量变更 ZK监听器
  private val topicChangeHandler = new TopicChangeHandler(eventManager)

  // 主题删除 ZK监听器：监听 /admin/delete_topics的子节点数量变更情况
  private val topicDeletionHandler = new TopicDeletionHandler(eventManager)

  // 分区变更 ZK监听器：监听主题分区数据变更的监听器，比如新增副本、分区Leader变更
  private val partitionModificationsHandlers: mutable.Map[String, PartitionModificationsHandler] = mutable.Map.empty

  /**
   * 分区重分配 ZK监听器：监听 /admin/reassign_partitions 节点是否被创建，如果创建则触发handler执行，
   * 当我们需要对Kakfa集群进行扩容以应对即将到来的大流量和业务尖峰，扩容后的Broker默认是不会有任何的分区副本，所以需要手动使用
   * kafka-reassign-partition.sh脚本将现有的分区副本挪一小部分到新创建的Broker中，使整个集群处于稳定的状态
   */
  private val partitionReassignmentHandler = new PartitionReassignmentHandler(eventManager)

  /**
   * Preferred Replica选举 ZK监听器
   * 一旦发现新提交的任务，就为目标主题执行Preferred Leader选举
   */
  private val preferredReplicaElectionHandler = new PreferredReplicaElectionHandler(eventManager)

  /**
   * ISR副本集合变更 ZK监听器：监听ISR副本集合变更，一旦触发，就需要获取ISR发生变更的分工列表，
   * 然后更新Controller端对应的Leader和ISR缓存元数据
   */
  private val isrChangeNotificationHandler = new IsrChangeNotificationHandler(eventManager)

  // 日志路径变更 ZK监听器
  private val logDirEventNotificationHandler = new LogDirEventNotificationHandler(eventManager)

  // 统计相关的字段，有些监控是非常重要的
  // 当前Controller所在的BrokerID
  @volatile private var activeControllerId = -1
  // 离线分区数量
  @volatile private var offlinePartitionCount = 0
  // 满足Preferred Leader选举条件的总分区数量
  @volatile private var preferredReplicaImbalanceCount = 0
  // 集群主题总数
  @volatile private var globalTopicCount = 0
  // 集群分区总数
  @volatile private var globalPartitionCount = 0
  // 集群中待删除的主题总数
  @volatile private var topicsToDeleteCount = 0
  // 集群中待删除的副本总数
  @volatile private var replicasToDeleteCount = 0
  // 集群中暂时无法删除的主题总数
  @volatile private var ineligibleTopicsToDeleteCount = 0
  // 集群中暂时无法删除的副本总数
  @volatile private var ineligibleReplicasToDeleteCount = 0

  /**
   * 单线程调度器：用来定时删除过期的tokens
   */
  private val tokenCleanScheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "delegation-token-cleaner")

  newGauge("ActiveControllerCount", () => if (isActive) 1 else 0)
  newGauge("OfflinePartitionsCount", () => offlinePartitionCount)
  newGauge("PreferredReplicaImbalanceCount", () => preferredReplicaImbalanceCount)
  newGauge("ControllerState", () => state.value)
  newGauge("GlobalTopicCount", () => globalTopicCount)
  newGauge("GlobalPartitionCount", () => globalPartitionCount)
  newGauge("TopicsToDeleteCount", () => topicsToDeleteCount)
  newGauge("ReplicasToDeleteCount", () => replicasToDeleteCount)
  newGauge("TopicsIneligibleToDeleteCount", () => ineligibleTopicsToDeleteCount)
  newGauge("ReplicasIneligibleToDeleteCount", () => ineligibleReplicasToDeleteCount)

  /**
   * Returns true if this broker is the current controller.
   */
  def isActive: Boolean = activeControllerId == config.brokerId

  def brokerEpoch: Long = _brokerEpoch

  def epoch: Int = controllerContext.epoch

  /**
   * Kafka Broker Server启动时调用此方法启动Controller。
   * 它不会假设Controller位于当前Broker中，仅仅是注册会话过期监听器并且启动controller leader
   *
   *
   * Invoked when the controller module of a Kafka server is started up.
   *
   * This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  def startup() = {
    // #1 注册用于session过期后触发重新选举的handler
    zkClient.registerStateChangeHandler(new StateChangeHandler {
      override val name: String = StateChangeHandlers.ControllerHandler

      /**
       * 在重新初始化session后做的操作：添加「RegisterBrokerAndReelect」事件
       */
      override def afterInitializingSession(): Unit = {
        eventManager.put(RegisterBrokerAndReelect)
      }

      /**
       * 在重新初始化session前做的操作：添加「Expire」事件
       */
      override def beforeInitializingSession(): Unit = {
        val queuedEvent = eventManager.clearAndPut(Expire)

        // Block initialization of the new session until the expiration event is being handled,
        // which ensures that all pending events have been processed before creating the new session
        // 阻塞等待时间被处理结束,session过期触发重新选举,必须等待选举这个时间完成Controller才能正常工作
        queuedEvent.awaitProcessing()
      }
    })

    // #2 将启动事件放入事件管理器中
    eventManager.put(Startup)

    // #3 启动事件管理器
    eventManager.start()
  }

  /**
   *
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown(): Unit = {
    // #1 关闭事件管理器
    eventManager.close()
    onControllerResignation()
  }

  /**
   * On controlled shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   *
   * @param id          Id of the broker to shutdown.
   * @param brokerEpoch The broker epoch in the controlled shutdown request
   * @return The number of partitions that the broker still leads.
   */
  def controlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    val controlledShutdownEvent = ControlledShutdown(id, brokerEpoch, controlledShutdownCallback)
    eventManager.put(controlledShutdownEvent)
  }

  private[kafka] def updateBrokerInfo(newBrokerInfo: BrokerInfo): Unit = {
    this.brokerInfo = newBrokerInfo
    zkClient.updateBrokerInfo(newBrokerInfo)
  }

  private[kafka] def enableDefaultUncleanLeaderElection(): Unit = {
    eventManager.put(UncleanLeaderElectionEnable)
  }

  private[kafka] def enableTopicUncleanLeaderElection(topic: String): Unit = {
    if (isActive) {
      eventManager.put(TopicUncleanLeaderElectionEnable(topic))
    }
  }

  private def state: ControllerState = eventManager.state

  /**
   * 成功当选Controller后所执行的方法逻辑，主要包含：
   * ① 注册各类ZK监听器
   * ② 删除日志路径变更和ISR副本变更通知事件
   * ③ 启动Controller通道管理器（即 {@link ControllerChannelManager}）
   * ③ 启动副本状态机{@link ReplicaStateMachine}和分区状态机 {@link PartitionStateMachine}
   *
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   * It does the following things on the become-controller state change -
   *
   * 如果这个方法出现任何异常，那么会放弃controller身份。这确保整个集群有可用的controller对象。
   * 1. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   * leaders for all existing partitions.
   * 2. Starts the controller's channel manager
   * 3. Starts the replica state machine
   * 4. Starts the partition state machine
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   */
  private def onControllerFailover(): Unit = {
    maybeSetupFeatureVersioning()

    info("Registering handlers")

    // #1 将以下Handler添加到ZkClient本地缓存中，
    //    后续在初始化ControllerContext根据这个集合判断是否需要对节点注册Zookeeper监听器
    val childChangeHandlers = Seq(brokerChangeHandler,
      topicChangeHandler,
      topicDeletionHandler,
      logDirEventNotificationHandler,
      isrChangeNotificationHandler)
    childChangeHandlers.foreach(zkClient.registerZNodeChildChangeHandler)
    val nodeChangeHandlers = Seq(preferredReplicaElectionHandler, partitionReassignmentHandler)
    nodeChangeHandlers.foreach(zkClient.registerZNodeChangeHandlerAndCheckExistence)

    // #2 删除ZK中的日志路径变更通知事件
    info("Deleting log dir event notifications")
    zkClient.deleteLogDirEventNotifications(controllerContext.epochZkVersion)

    // #3 删除ZK中ISR副本变更通知事件
    info("Deleting isr change notifications")
    zkClient.deleteIsrChangeNotifications(controllerContext.epochZkVersion)

    // #4 初始化ControllerContext上下文，里面包含Controller关于集群的一切元数据
    //    注册Broker监听器，一旦Broker发生变更，当前Controller可以第一时间感知并执行handler处理逻辑
    initializeControllerContext()

    // #5 从Zookeeper获取正在删除的主题列表
    info("Fetching topic deletions in progress")
    val (topicsToBeDeleted, topicsIneligibleForDeletion) = fetchTopicDeletionsInProgress()

    // #6 初始化主题删除管理器
    info("Initializing topic deletion manager")
    topicDeletionManager.init(topicsToBeDeleted, topicsIneligibleForDeletion)

    // #7 向集群广播「UpdateMetadataRequest」为什么要做这一步呢?目的是接下来副本状态机和分区状态机启动做铺垫，这两个状态机会向
    //    有关Broker发送「LeaderAndIsrRequests」，这里Broker就因为有集群元数据就可以处理这个请求了
    info("Sending update metadata request")
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)

    // #8 启动副本状态机
    replicaStateMachine.startup()

    // #9 启动分区状态机
    partitionStateMachine.startup()


    // #10 初始化分区重分配管理器
    info(s"Ready to serve as the new controller with epoch $epoch")
    initializePartitionReassignments()

    // #11 恢复主题删除操作
    topicDeletionManager.tryTopicDeletion()

    // #12
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    // #13 尝试为给定的分区选出一个副本Leader
    onReplicaElection(pendingPreferredReplicaElections, ElectionType.PREFERRED, ZkTriggered)

    // #14 启动controller调度器
    info("Starting the controller scheduler")
    kafkaScheduler.startup()
    if (config.autoLeaderRebalanceEnable) {
      // #15 每5秒钟自动执行Leader重平衡
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
    }

    // #16 定时清理过期的token
    if (config.tokenAuthEnabled) {
      info("starting the token expiry check scheduler")
      tokenCleanScheduler.startup()
      tokenCleanScheduler.schedule(name = "delete-expired-tokens",
        fun = () => tokenManager.expireTokens(),
        period = config.delegationTokenExpiryCheckIntervalMs,
        unit = TimeUnit.MILLISECONDS)
    }
  }

  private def createFeatureZNode(newNode: FeatureZNode): Int = {
    info(s"Creating FeatureZNode at path: ${FeatureZNode.path} with contents: $newNode")
    zkClient.createFeatureZNode(newNode)
    val (_, newVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    newVersion
  }

  private def updateFeatureZNode(updatedNode: FeatureZNode): Int = {
    info(s"Updating FeatureZNode at path: ${FeatureZNode.path} with contents: $updatedNode")
    zkClient.updateFeatureZNode(updatedNode)
  }

  /**
   * This method enables the feature versioning system (KIP-584).
   *
   * Development in Kafka (from a high level) is organized into features. Each feature is tracked by
   * a name and a range of version numbers. A feature can be of two types:
   *
   * 1. Supported feature:
   * A supported feature is represented by a name (string) and a range of versions (defined by a
   * SupportedVersionRange). It refers to a feature that a particular broker advertises support for.
   * Each broker advertises the version ranges of its own supported features in its own
   * BrokerIdZNode. The contents of the advertisement are specific to the particular broker and
   * do not represent any guarantee of a cluster-wide availability of the feature for any particular
   * range of versions.
   *
   * 2. Finalized feature:
   * A finalized feature is represented by a name (string) and a range of version levels (defined
   * by a FinalizedVersionRange). Whenever the feature versioning system (KIP-584) is
   * enabled, the finalized features are stored in the cluster-wide common FeatureZNode.
   * In comparison to a supported feature, the key difference is that a finalized feature exists
   * in ZK only when it is guaranteed to be supported by any random broker in the cluster for a
   * specified range of version levels. Also, the controller is the only entity modifying the
   * information about finalized features.
   *
   * This method sets up the FeatureZNode with enabled status, which means that the finalized
   * features stored in the FeatureZNode are active. The enabled status should be written by the
   * controller to the FeatureZNode only when the broker IBP config is greater than or equal to
   * KAFKA_2_7_IV0.
   *
   * There are multiple cases handled here:
   *
   * 1. New cluster bootstrap:
   * A new Kafka cluster (i.e. it is deployed first time) is almost always started with IBP config
   * setting greater than or equal to KAFKA_2_7_IV0. We would like to start the cluster with all
   * the possible supported features finalized immediately. Assuming this is the case, the
   * controller will start up and notice that the FeatureZNode is absent in the new cluster,
   * it will then create a FeatureZNode (with enabled status) containing the entire list of
   * supported features as its finalized features.
   *
   * 2. Broker binary upgraded, but IBP config set to lower than KAFKA_2_7_IV0:
   * Imagine there was an existing Kafka cluster with IBP config less than KAFKA_2_7_IV0, and the
   * broker binary has now been upgraded to a newer version that supports the feature versioning
   * system (KIP-584). But the IBP config is still set to lower than KAFKA_2_7_IV0, and may be
   * set to a higher value later. In this case, we want to start with no finalized features and
   * allow the user to finalize them whenever they are ready i.e. in the future whenever the
   * user sets IBP config to be greater than or equal to KAFKA_2_7_IV0, then the user could start
   * finalizing the features. This process ensures we do not enable all the possible features
   * immediately after an upgrade, which could be harmful to Kafka.
   * This is how we handle such a case:
   *      - Before the IBP config upgrade (i.e. IBP config set to less than KAFKA_2_7_IV0), the
   *        controller will start up and check if the FeatureZNode is absent.
   *        - If the node is absent, it will react by creating a FeatureZNode with disabled status
   *          and empty finalized features.
   *        - Otherwise, if a node already exists in enabled status then the controller will just
   *          flip the status to disabled and clear the finalized features.
   *      - After the IBP config upgrade (i.e. IBP config set to greater than or equal to
   *        KAFKA_2_7_IV0), when the controller starts up it will check if the FeatureZNode exists
   *        and whether it is disabled.
   *         - If the node is in disabled status, the controller won’t upgrade all features immediately.
   *           Instead it will just switch the FeatureZNode status to enabled status. This lets the
   *           user finalize the features later.
   *         - Otherwise, if a node already exists in enabled status then the controller will leave
   *           the node umodified.
   *
   * 3. Broker binary upgraded, with existing cluster IBP config >= KAFKA_2_7_IV0:
   * Imagine there was an existing Kafka cluster with IBP config >= KAFKA_2_7_IV0, and the broker
   * binary has just been upgraded to a newer version (that supports IBP config KAFKA_2_7_IV0 and
   * higher). The controller will start up and find that a FeatureZNode is already present with
   * enabled status and existing finalized features. In such a case, the controller leaves the node
   * unmodified.
   *
   * 4. Broker downgrade:
   * Imagine that a Kafka cluster exists already and the IBP config is greater than or equal to
   * KAFKA_2_7_IV0. Then, the user decided to downgrade the cluster by setting IBP config to a
   * value less than KAFKA_2_7_IV0. This means the user is also disabling the feature versioning
   * system (KIP-584). In this case, when the controller starts up with the lower IBP config, it
   * will switch the FeatureZNode status to disabled with empty features.
   */
  private def enableFeatureVersioning(): Unit = {
    val (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(FeatureZNode.path)
    if (version == ZkVersion.UnknownVersion) {
      val newVersion = createFeatureZNode(new FeatureZNode(FeatureZNodeStatus.Enabled,
        brokerFeatures.defaultFinalizedFeatures))
      featureCache.waitUntilEpochOrThrow(newVersion, config.zkConnectionTimeoutMs)
    } else {
      val existingFeatureZNode = FeatureZNode.decode(mayBeFeatureZNodeBytes.get)
      val newFeatures = existingFeatureZNode.status match {
        case FeatureZNodeStatus.Enabled => existingFeatureZNode.features
        case FeatureZNodeStatus.Disabled =>
          if (!existingFeatureZNode.features.empty()) {
            warn(s"FeatureZNode at path: ${FeatureZNode.path} with disabled status" +
              s" contains non-empty features: ${existingFeatureZNode.features}")
          }
          Features.emptyFinalizedFeatures
      }
      val newFeatureZNode = new FeatureZNode(FeatureZNodeStatus.Enabled, newFeatures)
      if (!newFeatureZNode.equals(existingFeatureZNode)) {
        val newVersion = updateFeatureZNode(newFeatureZNode)
        featureCache.waitUntilEpochOrThrow(newVersion, config.zkConnectionTimeoutMs)
      }
    }
  }

  /**
   * Disables the feature versioning system (KIP-584).
   *
   * Sets up the FeatureZNode with disabled status. This status means the feature versioning system
   * (KIP-584) is disabled, and, the finalized features stored in the FeatureZNode are not relevant.
   * This status should be written by the controller to the FeatureZNode only when the broker
   * IBP config is less than KAFKA_2_7_IV0.
   *
   * NOTE:
   * 1. When this method returns, existing finalized features (if any) will be cleared from the
   * FeatureZNode.
   * 2. This method, unlike enableFeatureVersioning() need not wait for the FinalizedFeatureCache
   * to be updated, because, such updates to the cache (via FinalizedFeatureChangeListener)
   * are disabled when IBP config is < than KAFKA_2_7_IV0.
   */
  private def disableFeatureVersioning(): Unit = {
    val newNode = FeatureZNode(FeatureZNodeStatus.Disabled, Features.emptyFinalizedFeatures())
    val (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(FeatureZNode.path)
    if (version == ZkVersion.UnknownVersion) {
      createFeatureZNode(newNode)
    } else {
      val existingFeatureZNode = FeatureZNode.decode(mayBeFeatureZNodeBytes.get)
      if (existingFeatureZNode.status == FeatureZNodeStatus.Disabled &&
        !existingFeatureZNode.features.empty()) {
        warn(s"FeatureZNode at path: ${FeatureZNode.path} with disabled status" +
          s" contains non-empty features: ${existingFeatureZNode.features}")
      }
      if (!newNode.equals(existingFeatureZNode)) {
        updateFeatureZNode(newNode)
      }
    }
  }

  private def maybeSetupFeatureVersioning(): Unit = {
    if (config.isFeatureVersioningSupported) {
      enableFeatureVersioning()
    } else {
      disableFeatureVersioning()
    }
  }

  private def scheduleAutoLeaderRebalanceTask(delay: Long, unit: TimeUnit): Unit = {
    kafkaScheduler.schedule("auto-leader-rebalance-task", () => eventManager.put(AutoPreferredReplicaLeaderElection),
      delay = delay, unit = unit)
  }

  /**
   * 当controller退位时，就会调用此方法。这是清理内部controller数据结果必要操作
   */
  private def onControllerResignation(): Unit = {
    debug("Resigning")
    // #1 注销相关的Handler处理器（监听器）
    zkClient.unregisterZNodeChildChangeHandler(isrChangeNotificationHandler.path)
    zkClient.unregisterZNodeChangeHandler(partitionReassignmentHandler.path)
    zkClient.unregisterZNodeChangeHandler(preferredReplicaElectionHandler.path)
    zkClient.unregisterZNodeChildChangeHandler(logDirEventNotificationHandler.path)
    unregisterBrokerModificationsHandler(brokerModificationsHandlers.keySet)

    // #2 关闭Leader重平衡（leader rebalance）调度器
    kafkaScheduler.shutdown()

    // #3 相关统计信息清零
    offlinePartitionCount = 0
    preferredReplicaImbalanceCount = 0
    globalTopicCount = 0
    globalPartitionCount = 0
    topicsToDeleteCount = 0
    replicasToDeleteCount = 0
    ineligibleTopicsToDeleteCount = 0
    ineligibleReplicasToDeleteCount = 0

    // #4 关闭token过期调度器
    if (tokenCleanScheduler.isStarted)
      tokenCleanScheduler.shutdown()

    // de-register partition ISR listener for on-going partition reassignment task
    // #5
    unregisterPartitionReassignmentIsrChangeHandlers()

    // #6 关闭分区状态机
    partitionStateMachine.shutdown()
    zkClient.unregisterZNodeChildChangeHandler(topicChangeHandler.path)
    unregisterPartitionModificationsHandlers(partitionModificationsHandlers.keys.toSeq)
    zkClient.unregisterZNodeChildChangeHandler(topicDeletionHandler.path)

    // #7 关闭副本状态机
    replicaStateMachine.shutdown()
    zkClient.unregisterZNodeChildChangeHandler(brokerChangeHandler.path)

    // #8 关闭controller通道管理器
    controllerChannelManager.shutdown()

    // #9 重置controller上下文
    controllerContext.resetContext()

    info("Resigned")
  }

  /**
   * This callback is invoked by the controller's LogDirEventNotificationListener with the list of broker ids who
   * have experienced new log directory failures. In response the controller should send LeaderAndIsrRequest
   * to all these brokers to query the state of their replicas. Replicas with an offline log directory respond with
   * KAFKA_STORAGE_ERROR, which will be handled by the LeaderAndIsrResponseReceived event.
   */
  private def onBrokerLogDirFailure(brokerIds: Seq[Int]): Unit = {
    // send LeaderAndIsrRequest for all replicas on those brokers to see if they are still online.
    info(s"Handling log directory failure for brokers ${brokerIds.mkString(",")}")
    // 获取brokers的所有副本
    val replicasOnBrokers = controllerContext.replicasOnBrokers(brokerIds.toSet)

    // 副本状态机更改副本为「OnlineReplica」
    replicaStateMachine.handleStateChanges(replicasOnBrokers.toSeq, OnlineReplica)
  }

  /**
   *
   *
   * 此回调是由副本状态机的broker改变监听器调用，入参是新的brokers id 列表。
   * 这个方法执行以下步骤：
   * ① 向所有存活的或正在关闭的broker发送「updatemetadata」请求
   * ② 触发所有新的/离线的分区的 OnlinePartition 状态更改。
   * ③ 检查是否重分配的副本分配给任何新启动的brokers。如果是这样的话，它会为每个每个主题/分区执行重分配逻辑。
   * 请注意，此时我们不需要为所有主题/分区刷新leader/isr缓存，原因有两点：
   * ① 当分区状态机触发在线状态变更时，将仅刷新当前新分区的leader和ISR或离线（而不是该控制器知道的每个分区）(rather than every partition this controller is aware of)
   * ② 即使我们确实刷新了缓存，也不能保证在领导者和 ISR 请求到达每个代理时它仍然有效。 broker通过检查leader epoch以推断请求的合法性。
   */

  /**
   * 首先，我们应该对入参「newBrokers」有一个清楚的认识：它不代表是新创建的Broker，有可能之前处于离线状态，然后经过一段时间重启并重新加入集群中，它也属于newBrokers。
   * 所以，为什么一开始需要从「replicasOnOfflineDirs」移除缓存就是因为它们之前存在过，所以这一步是有必要的。
   * #2 元数据更新请求「updatemetadata」的发送也是有技巧的，kakfa将broker分为两大类，
   * 一类是已存在的，和新增的。前者不需要携带完整的分区状态信息，只需要发个通知告知broker有新的成员加入，
   * 而后者需要携带完整信息，这样就可以以最快速度让所有的broker都有完整的元数据信息。
   * 「newBrokers」对象可能包含副本，我们需要将这些副本对象变更为「online」以对外提供服务。
   * 即Leader副本对外提供读写服务，follower副本自动向Leader副本拉取消息。
   *
   * @param newBrokers
   */
  private def onBrokerStartup(newBrokers: Seq[Int]): Unit = {
    info(s"New broker startup callback for ${newBrokers.mkString(",")}")
    // 首先从replicasOnOfflineDirs缓存中移除数据，因为broker现在处于「可用」状态了
    newBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
    val newBrokersSet = newBrokers.toSet

    // #1 获取「已存在」的brokers
    val existingBrokers = controllerContext.liveOrShuttingDownBrokerIds.diff(newBrokersSet)

    // #2 向已存在的brokers发送「updatemetadata」更新元数据信息请求，以至于让这些broker知道有新成员加入
    //    由于没有分区状态的变更，所以不需要在请求中包含任何分区状态
    //    这一步目的是让「已存在」的broker感知到有新的broker加入
    sendUpdateMetadataRequest(existingBrokers.toSeq, Set.empty)

    // #3 向新增的brokers发送「updatemetadata」更新元数据信息请求，请求携带集群完整的分区状态信息
    //    令这些新增的brokers同步集群当前所有分区数据
    //    当可控关闭的情况下，当一个新的broker出现时，leader不会被选出。因此，至少在觉的可控关闭情况下，元数据可以最快到达新的broker节点
    sendUpdateMetadataRequest(newBrokers, controllerContext.partitionsWithLeaders)

    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)

    // #4 将新增broker上的所有副本设置为「online」在线状态
    replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers.toSeq, OnlineReplica)
    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those

    // #5 分区状态机变更分区状态
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // check if reassignment of some partitions need to be restarted

    // #6 重启之前暂停副本迁移工作
    maybeResumeReassignments { (_, assignment) =>
      assignment.targetReplicas.exists(newBrokersSet.contains)
    }

    // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
    // on the newly restarted brokers, there is a chance that topic deletion can resume
    val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))

    // #7 重启之前暂停主题删除操作
    if (replicasForTopicsToBeDeleted.nonEmpty) {
      info(s"Some replicas ${replicasForTopicsToBeDeleted.mkString(",")} for topics scheduled for deletion " +
        s"${controllerContext.topicsToBeDeleted.mkString(",")} are on the newly restarted brokers " +
        s"${newBrokers.mkString(",")}. Signaling restart of topic deletion for these topics")
      topicDeletionManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }

    // #8 为新增Brokers注册「BrokerModificationsHandler」监听器，目的是监听「/brokers/ids/[broker id]」节点数据的变化
    registerBrokerModificationsHandler(newBrokers)
  }

  private def maybeResumeReassignments(shouldResume: (TopicPartition, ReplicaAssignment) => Boolean): Unit = {
    controllerContext.partitionsBeingReassigned.foreach { tp =>
      val currentAssignment = controllerContext.partitionFullReplicaAssignment(tp)
      if (shouldResume(tp, currentAssignment))
        onPartitionReassignment(tp, currentAssignment)
    }
  }

  private def registerBrokerModificationsHandler(brokerIds: Iterable[Int]): Unit = {
    debug(s"Register BrokerModifications handler for $brokerIds")
    brokerIds.foreach { brokerId =>
      val brokerModificationsHandler = new BrokerModificationsHandler(eventManager, brokerId)
      zkClient.registerZNodeChangeHandlerAndCheckExistence(brokerModificationsHandler)
      brokerModificationsHandlers.put(brokerId, brokerModificationsHandler)
    }
  }

  private def unregisterBrokerModificationsHandler(brokerIds: Iterable[Int]): Unit = {
    debug(s"Unregister BrokerModifications handler for $brokerIds")
    brokerIds.foreach { brokerId =>
      brokerModificationsHandlers.remove(brokerId).foreach(handler => zkClient.unregisterZNodeChangeHandler(handler.path))
    }
  }

  /**
   *
   * This callback is invoked by the replica state machine's broker change listener
   * with the list of failed brokers
   * as input.
   * It will call onReplicaBecomeOffline(...)
   * with the list of replicas on those failed brokers as input.
   */

  /**
   * 当一个broker关闭时，controller需要关注以下事情：
   * ① broker管理的副本对象 -- 置为「离线状态」
   * ② ZK监听器 -- 移除监听器
   * ③ 相关缓存 -- 移除相关缓存数据
   *
   * @param deadBrokers 已终止运行的Broker ID列表
   */
  private def onBrokerFailure(deadBrokers: Seq[Int]): Unit = {
    info(s"Broker failure callback for ${deadBrokers.mkString(",")}")
    // #1「更新Controller元数据」：将给定的Broker从「replicasOnOfflineDirs」缓存中移除
    deadBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)

    // #2 从「正在关闭中（shuttingDownBrokerIds）」移除deadBrokers
    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    if (deadBrokersThatWereShuttingDown.nonEmpty)
      info(s"Removed ${deadBrokersThatWereShuttingDown.mkString(",")} from list of shutting down brokers.")

    // #3 执行副本清理工作
    // #3-1 获取死亡brokers中所有的副本对象，由于副本所在的broker已被关闭，所以这些副本的状态需要变更为「离线」状态
    val allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokers.toSet)
    // #3-2 变更副本的状态为「离线」
    onReplicasBecomeOffline(allReplicasOnDeadBrokers)

    // #4 注销死亡brokers注册的「BrokerModificationsHandler」监听器，
    // 这个监听器用来监听「/brokers/ids/[broker id]」节点数据变更的，比如broker重启就会触发此handler相关回调方法
    unregisterBrokerModificationsHandler(deadBrokers)
  }

  private def onBrokerUpdate(updatedBrokerId: Int): Unit = {
    info(s"Broker info update callback for $updatedBrokerId")
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)
  }

  /**
   * 该方法将给定的副本状态变更为离线状态。它做了以下工作
   * ① 将给定的分区标记为「Offline」
   * ② 对所有new/offline分区触发OnlinePartition状态变化
   * ③ 在新的离线复制的输入列表上调用OfflineReplica状态变化
   * ④ 如果没有分区受到影响，则向存活的或正在关闭的broker发送UpdateMetadataRequest请求
   *
   * This method marks the given replicas as offline. It does the following -
   * 1. Marks the given partitions as offline
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. Invokes the OfflineReplica state change on the input list of newly offline replicas
   * 4. If no partitions are affected then send UpdateMetadataRequest to live or shutting down brokers
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point. This is because
   * the partition state machine will refresh our cache for us when performing leader election for all new/offline
   * partitions coming online.
   */
  private def onReplicasBecomeOffline(newOfflineReplicas: Set[PartitionAndReplica]): Unit = {
    // #1 对「newOfflineReplicas」副本根据主题是否被删除做区分
    val (newOfflineReplicasForDeletion, newOfflineReplicasNotForDeletion) =
      newOfflineReplicas.partition(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))

    val partitionsWithOfflineLeader = controllerContext.partitionsWithOfflineLeader

    // trigger OfflinePartition state for all partitions whose current leader is one amongst the newOfflineReplicas
    partitionStateMachine.handleStateChanges(partitionsWithOfflineLeader.toSeq, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // trigger OfflineReplica state change for those newly offline replicas
    replicaStateMachine.handleStateChanges(newOfflineReplicasNotForDeletion.toSeq, OfflineReplica)

    // fail deletion of topics that are affected by the offline replicas
    if (newOfflineReplicasForDeletion.nonEmpty) {
      // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
      // deleted when its log directory is offline. This will prevent the replica from being in TopicDeletionStarted state indefinitely
      // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
      topicDeletionManager.failReplicaDeletion(newOfflineReplicasForDeletion)
    }

    // If replica failure did not require leader re-election, inform brokers of the offline brokers
    // Note that during leader re-election, brokers update their metadata
    if (partitionsWithOfflineLeader.isEmpty) {
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)
    }
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  private def onNewPartitionCreation(newPartitions: Set[TopicPartition]): Unit = {
    info(s"New partition creation callback for ${newPartitions.mkString(",")}")
    partitionStateMachine.handleStateChanges(newPartitions.toSeq, NewPartition)
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, NewReplica)
    partitionStateMachine.handleStateChanges(
      newPartitions.toSeq,
      OnlinePartition,
      Some(OfflinePartitionLeaderElectionStrategy(false))
    )
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, OnlineReplica)
  }

  /**
   * 这个方法会被以下方法所回调：
   * ① AlterPartitionReassignments
   * ② 当zookeeper节点 /admin/reassign/partitions 被创建时触发监听器而引发的回调
   * ③ 当一个正在进行的重分区完成时（通过监控分区的ISR Zookeeper节点变更）
   * ④ 每当一个新的broker出现，这个正在进行重分配的一部分
   * ⑤ controller 启动/故障转移
   *
   * 为分区重分配副本需要经历以下步骤：
   * ① RS  = 当前已分配的副本集合（replica set）
   * ② ORS = 分区原始副本集合（Original replica set）
   * ③ TRS = 重分配后目标副本集合（Target replica set）
   * ④ AR  = 作为重分配的一部分添加的副本（Adding as part of this reassignment）
   * ⑤ RR  = 作为重分配的一部分我们要删除的副本（Removing as part of this reassignment）
   *
   * 重分区过程可以分为三个大的阶段：
   * ① Assignment Update：无论是什么类型的触发操作，第一步总是更新现有分区的分配状态。我们总量在更新内存之前更新 Zookeeper 中的状态，
   * 以便它可以在故障转移时恢复，如果先更新缓存，再更新Zookeeper时broker宕机，那么集群的部分数据就丢失了。
   *    1. 更新Zookeeper数据：RS = ORS + TRS, AR = TRS - ORS, RR = ORS - TRS.
   *       2. 更新本地缓存数据：RS = ORS + TRS, AR = TRS - ORS, RR = ORS - TRS.
   *       3. 如果我们需要取消或替换现有的分区分配，如果它们不在分配的TRS集合中，则将「StopReplica」请求发送到原始的所有的AR集合成员中。
   *       为了完成分区重分配，我们需要使新副本加入同步。根据ISR的状态，我们将会执行以下步骤中的其中一个：
   *       ② 当 TRS != ISR，意味着重分配操作还未完成。
   *       A1. 增加leader epoch值，并向RS集合中的Broker发送「LeaderAndIsr」更新请求。
   *       A2. 将AR集合中的副本的状态变更为「NewReplica」，从而开始新的副本 AR
   *
   * ③ 当 TRS != ISR，意味着重分配操作已完成。
   * 1.将AR中所有副本状态变更为「OnlineReplica」状态
   * 2.更新本地缓存：RS = TRS, AR = [], RR = []
   * 3.发送「LeaderAndIsr」请求并附带RS=TRS数据。
   *
   * B3. Send a LeaderAndIsr request with RS = TRS.这能阻止将 TRS-ORS 中任何副本添加回 ISR中。如果当前的leader副本并未
   * 在 TRS 或离线，我们从TRS中选出一个副本作为leader副本。
   * 由于分区状态机的工作方式（它从ZK中读取副本），我们可能会将L eaderAndIsr 请求发送到多于TRS的副本中。
   *
   * B4. 将所有在RR的副本的状态变更为「OfflineReplica」。作为OfflineReplica状态改变的一部分，我们缩小isr以移除ZooKeeper中的RR，并向Leader发送LeaderAndIsr ONLY以通知它缩小后的isr。
   * 之后，我们向RR中的副本发送「StopReplica（delete=false）」请求，使得broker暂停该副本一切行为。
   *
   * B5. 将所有在RR的副本的状态变更为「NonExistentReplica」。这一步将会给RR的副本发送一个「StopReplica（delete=true）」的请求，broker收到此请求会删除物理日志文件。
   * B6. 更新ZK数据： RS=TRS, AR=[], RR=[].
   *
   * B7. 移除iSR的重分配监听器，并且有可能更新zookeeper节点 /admin/reassign_partitions，如果主题名称存在此路径的话，那么就会被移除
   * B8. 当副本leader选举结束，副本和isr信息会被修改，因此需要向集群广播元数据更新请求。
   *
   * 总之，我们有两个目标：
   * ①
   * In general, there are two goals we want to aim for:
   * 1. Every replica present in the replica set of a LeaderAndIsrRequest gets the request sent to it
   * 2. Replicas that are removed from a partition's assignment get StopReplica sent to them
   *
   * For example, if ORS = {1,2,3} and TRS = {4,5,6}, the values in the topic and leader/isr paths in ZK
   * may go through the following transitions.
   * RS                AR          RR          leader     isr
   * {1,2,3}           {}          {}          1          {1,2,3}           (initial state)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     1          {1,2,3}           (step A2)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     1          {1,2,3,4,5,6}     (phase B)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     4          {1,2,3,4,5,6}     (step B3)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     4          {4,5,6}           (step B4)
   * {4,5,6}           {}          {}          4          {4,5,6}           (step B6)
   *
   * Note that we have to update RS in ZK with TRS last since it's the only place where we store ORS persistently.
   * This way, if the controller crashes before that step, we can still recover.
   * @param topicPartition   主题分区
   * @param reassignment     重分配目标方案
   */
  private def onPartitionReassignment(topicPartition: TopicPartition, reassignment: ReplicaAssignment): Unit = {
    // 当分区重分配进行时，主题和分区的删除操作都是禁止的，即分区重分配的优先级比删除操作高
    topicDeletionManager.markTopicIneligibleForDeletion(Set(topicPartition.topic), reason = "topic reassignment in progress")

    // 更新Zookeeper和本地缓存的元数据，遇到需要删除的副本则进入副本删除流程
    updateCurrentReassignment(topicPartition, reassignment)

    // 正在新增的副本列表
    val addingReplicas = reassignment.addingReplicas
    // 正在被移除的副本列表
    val removingReplicas = reassignment.removingReplicas

    // 首先，判断是否完成重分配过程
    if (!isReassignmentComplete(topicPartition, reassignment)) {
      // 没有，向所有ORS+TRS副本所在的Broker发送「LeaderAndIsr」请求
      updateLeaderEpochAndSendRequest(topicPartition, reassignment)
      // 使用副本状态机将新增的副本状态变更为「NewReplica」
      startNewReplicasForReassignedPartition(topicPartition, addingReplicas)
    } else {
      // 已经完成副本重平衡

      // 1. 将处于AR状态的副本状态变更为「OnlineReplica」
      replicaStateMachine.handleStateChanges(addingReplicas.map(PartitionAndReplica(topicPartition, _)), OnlineReplica)
      // 2. 更新本地缓存，RS = TRS, AR = [], RR = []
      val completedReassignment = ReplicaAssignment(reassignment.targetReplicas)
      controllerContext.updatePartitionFullReplicaAssignment(topicPartition, completedReassignment)
      // B3. Send LeaderAndIsr request with a potential new leader (if current leader not in TRS) and
      //   a new RS (using TRS) and same isr to every broker in ORS + TRS or TRS
      moveReassignedPartitionLeaderIfRequired(topicPartition, completedReassignment)
      // B4. replicas in RR -> Offline (force those replicas out of isr)
      // B5. replicas in RR -> NonExistentReplica (force those replicas to be deleted)
      stopRemovedReplicasOfReassignedPartition(topicPartition, removingReplicas)
      // B6. Update ZK with RS = TRS, AR = [], RR = [].
      updateReplicaAssignmentForPartition(topicPartition, completedReassignment)
      // B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it.
      removePartitionFromReassigningPartitions(topicPartition, completedReassignment)
      // B8. After electing a leader in B3, the replicas and isr information changes, so resend the update metadata request to every broker
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
      // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
      topicDeletionManager.resumeDeletionForTopics(Set(topicPartition.topic))
    }
  }

  /**
   * 更新Zookeeper和内存中的当前分配状态。
   * 如果一个重新分配已经在进行中，那么新的重新分配将取代它，一些副本将被关闭。
   * 注意，由于我们计算原始副本集的方式，我们不能保证取消会恢复原始副本的顺序。
   *
   * 目标复制总是按照所需的顺序在复制集中列在第一位，这意味着如果重新分配与当前分配重叠，我们没有办法回到原来的顺序。
   * 例如，初始分配为[1, 2, 3]，重新分配为[3, 4, 2]，那么在重新分配的过程中，副本将被编码为[3, 4, 2, 1]。
   * 如果重新分配被取消了，就没有办法恢复原来的顺序。
   *
   * Update the current assignment state in Zookeeper and in memory. If a reassignment is already in
   * progress, then the new reassignment will supplant it and some replicas will be shutdown.
   *
   * Note that due to the way we compute the original replica set, we cannot guarantee that a
   * cancellation will restore the original replica order.
   *
   * Target replicas are always listed
   * first in the replica set in the desired order, which means we have no way to get to the
   * original order if the reassignment overlaps with the current assignment.
   *
   * For example,
   * with an initial assignment of [1, 2, 3] and a reassignment of [3, 4, 2], then the replicas
   * will be encoded as [3, 4, 2, 1] while the reassignment is in progress. If the reassignment
   * is cancelled, there is no way to restore the original order.
   * 旧的：[1,2,3] 新的：「3,4,2」
   *
   * 旧-新=1
   *
   *
   * 1.更新Zookeeper状态
   * 2.更新分区的本地缓存元数据
   * 3.遇到需要删除的副本，则对这些副本进行删除流程
   * 4.将分区详情加入到「partitionsBeingReassigned」，表示分区正处于重平衡过程
   *
   * @param topicPartition 待分区的分区
   * @param reassignment   目标分配方案
   */
  private def updateCurrentReassignment(topicPartition: TopicPartition, reassignment: ReplicaAssignment): Unit = {
    // #1 从缓存中获取分区的所有副本详情
    val currentAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)

    // 不相等，才会进行重分配操作
    if (currentAssignment != reassignment) {
      debug(s"Updating assignment of partition $topicPartition from $currentAssignment to $reassignment")

      // #2 先更新Zookeeper的分区状态（/brokers/topics/<topic name>）
      updateReplicaAssignmentForPartition(topicPartition, reassignment)

      // #3 更新本地缓存
      controllerContext.updatePartitionFullReplicaAssignment(topicPartition, reassignment)

      // #4 获取待移除的副本
      // 如果有一个分区重分配已经在进行中，那么当前添加的一些副本可能符合立即删除的条件，在这种情况下，我们需要停止这些副本。
      val unneededReplicas = currentAssignment.replicas.diff(reassignment.replicas)
      if (unneededReplicas.nonEmpty) {
        // 及时停止待删除副本
        stopRemovedReplicasOfReassignedPartition(topicPartition, unneededReplicas)
      }
    }

    if (!isAlterIsrEnabled) {
      // 注册监听器，监听 /brokers/topics/<topic name>/partitions/<partition id>/state 节点
      val reassignIsrChangeHandler = new PartitionReassignmentIsrChangeHandler(eventManager, topicPartition)
      zkClient.registerZNodeChangeHandler(reassignIsrChangeHandler)
    }

    // 加入缓存中，表示分区现正处于重平衡过程
    controllerContext.partitionsBeingReassigned.add(topicPartition)
  }

  /**
   * 1. 判断主题是否被删除，如果删除，返回错误码
   * 2. 执行分区重分配操作 {@link KafkaController.onPartitionReassignment}
   *
   * 触发分区重分配操作。需要判断分区的主题是否处于被删除状态。
   *
   * This is called when a reassignment is initially received either through Zookeeper or through the
   * AlterPartitionReassignments API
   *
   * The `partitionsBeingReassigned` field in the controller context will be updated by this
   * call after the reassignment completes validation and is successfully stored in the topic
   * assignment zNode.
   *
   * @param reassignments 待进行重分配的分区副本详情 <分区, 副本详情>
   * @return 分区分配结果或错误标识：<分区,错误标志>，如果错误标志为NONE，说明分区重分配结果提交成功
   */
  private def maybeTriggerPartitionReassignment(reassignments: Map[TopicPartition, ReplicaAssignment]): Map[TopicPartition, ApiError] = {
    reassignments.map { case (tp, reassignment) =>
      val topic = tp.topic

      val apiError = if (topicDeletionManager.isTopicQueuedUpForDeletion(topic)) {
        // 判断主题是否已经被删除
        info(s"Skipping reassignment of $tp since the topic is currently being deleted")
        new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist.")
      } else {
        // 从缓存中获取主题原先的副本分配情况
        val assignedReplicas = controllerContext.partitionReplicaAssignment(tp)
        if (assignedReplicas.nonEmpty) {
          try {
            // 执行分区重分配操作
            onPartitionReassignment(tp, reassignment)
            ApiError.NONE
          } catch {
            case e: ControllerMovedException =>
              info(s"Failed completing reassignment of partition $tp because controller has moved to another broker")
              throw e
            case e: Throwable =>
              error(s"Error completing reassignment of partition $tp", e)
              new ApiError(Errors.UNKNOWN_SERVER_ERROR)
          }
        } else {
          new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist.")
        }
      }

      tp -> apiError
    }
  }

  /**
   * Attempt to elect a replica as leader for each of the given partitions.
   *
   * @param partitions      The partitions to have a new leader elected
   * @param electionType    The type of election to perform
   * @param electionTrigger The reason for tigger this election
   * @return A map of failed and successful elections. The keys are the topic partitions and the corresponding values are
   *         either the exception that was thrown or new leader & ISR.
   */
  private[this] def onReplicaElection(
                                       partitions: Set[TopicPartition],
                                       electionType: ElectionType,
                                       electionTrigger: ElectionTrigger
                                     ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    info(s"Starting replica leader election ($electionType) for partitions ${partitions.mkString(",")} triggered by $electionTrigger")
    try {
      val strategy = electionType match {
        case ElectionType.PREFERRED => PreferredReplicaPartitionLeaderElectionStrategy
        case ElectionType.UNCLEAN =>
          /* Let's be conservative and only trigger unclean election if the election type is unclean and it was
           * triggered by the admin client
           */
          OfflinePartitionLeaderElectionStrategy(allowUnclean = electionTrigger == AdminClientTriggered)
      }

      val results = partitionStateMachine.handleStateChanges(
        partitions.toSeq,
        OnlinePartition,
        Some(strategy)
      )
      if (electionTrigger != AdminClientTriggered) {
        results.foreach {
          case (tp, Left(throwable)) =>
            if (throwable.isInstanceOf[ControllerMovedException]) {
              info(s"Error completing replica leader election ($electionType) for partition $tp because controller has moved to another broker.", throwable)
              throw throwable
            } else {
              error(s"Error completing replica leader election ($electionType) for partition $tp", throwable)
            }
          case (_, Right(_)) => // Ignored; No need to log or throw exception for the success cases
        }
      }

      results
    } finally {
      if (electionTrigger != AdminClientTriggered) {
        removePartitionsFromPreferredReplicaElection(partitions, electionTrigger == AutoTriggered)
      }
    }
  }

  /**
   * 初始化Controller上下文对象：从Zookeeper获取相关数据填充 {@link ControllerContext} 相关字段
   * ① Broker详情：从/brokers/ids/[broker id] 节点下获取集群所有brokers的元信息，比如监听器名称、host、port、安全协议等。
   * ② 主题详情：从/brokers/topics/[主题名称]节点下获取主题名称列表，放入Controller上下文
   * ③
   */
  private def initializeControllerContext(): Unit = {
    // #1 获取集群中所有存活的Broker信息
    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster

    // #2
    val (compatibleBrokerAndEpochs, incompatibleBrokerAndEpochs) = partitionOnFeatureCompatibility(curBrokerAndEpochs)
    if (!incompatibleBrokerAndEpochs.isEmpty) {
      warn("Ignoring registration of new brokers due to incompatibilities with finalized features: " +
        incompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
    }

    // #3 更新存活的Broker对象（兼容的Broker）
    controllerContext.setLiveBrokers(compatibleBrokerAndEpochs)
    info(s"Initialized broker epochs cache: ${controllerContext.liveBrokerIdAndEpochs}")

    // #4 从ZK中获取所有的主题列表，并更新到Controller缓存中
    controllerContext.setAllTopics(zkClient.getAllTopicsInCluster(true))

    // 注册处理器，处理topic变动
    registerPartitionModificationsHandlers(controllerContext.allTopics.toSeq)

    // #5 获取主题的副本详情
    // Set[TopicIdReplicaAssignment]
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(controllerContext.allTopics.toSet)

    // #6 如果当前Kafka版本>=2.8，则需要为每个主题设置topic_id
    processTopicIds(replicaAssignmentAndTopicIds)

    // #7 更新controller上下文缓存
    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(_, _, assignments) =>
      assignments.foreach { case (topicPartition, replicaAssignment) =>
        controllerContext.updatePartitionFullReplicaAssignment(topicPartition, replicaAssignment)
        if (replicaAssignment.isBeingReassigned)
          controllerContext.partitionsBeingReassigned.add(topicPartition)
      }
    }
    // #8 清除「partitionLeadershipInfo」缓存
    controllerContext.clearPartitionLeadershipInfo()

    // #9
    controllerContext.shuttingDownBrokerIds.clear()

    // #10 注册「BrokerModifications」处理器，一旦ZK监听Broker发生变更，则触发处理器执行
    registerBrokerModificationsHandler(controllerContext.liveOrShuttingDownBrokerIds)

    // #11 更新「partitionLeadershipInfo」缓存
    updateLeaderAndIsrCache()

    // #12 启动Controller Channel管理器，它是负责维护和其它Broker的Socket连接
    controllerChannelManager.startup()
    info(s"Currently active brokers in the cluster: ${controllerContext.liveBrokerIds}")
    info(s"Currently shutting brokers in the cluster: ${controllerContext.shuttingDownBrokerIds}")
    info(s"Current list of topics in the cluster: ${controllerContext.allTopics}")
  }

  private def fetchPendingPreferredReplicaElections(): Set[TopicPartition] = {
    val partitionsUndergoingPreferredReplicaElection = zkClient.getPreferredReplicaElection
    // check if they are already completed or topic was deleted
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      val topicDeleted = replicas.isEmpty
      val successful =
        if (!topicDeleted) controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader == replicas.head else false
      successful || topicDeleted
    }
    val pendingPreferredReplicaElectionsIgnoringTopicDeletion = partitionsUndergoingPreferredReplicaElection -- partitionsThatCompletedPreferredReplicaElection
    val pendingPreferredReplicaElectionsSkippedFromTopicDeletion = pendingPreferredReplicaElectionsIgnoringTopicDeletion.filter(partition => topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
    val pendingPreferredReplicaElections = pendingPreferredReplicaElectionsIgnoringTopicDeletion -- pendingPreferredReplicaElectionsSkippedFromTopicDeletion
    info(s"Partitions undergoing preferred replica election: ${partitionsUndergoingPreferredReplicaElection.mkString(",")}")
    info(s"Partitions that completed preferred replica election: ${partitionsThatCompletedPreferredReplicaElection.mkString(",")}")
    info(s"Skipping preferred replica election for partitions due to topic deletion: ${pendingPreferredReplicaElectionsSkippedFromTopicDeletion.mkString(",")}")
    info(s"Resuming preferred replica election for partitions: ${pendingPreferredReplicaElections.mkString(",")}")
    pendingPreferredReplicaElections
  }

  /**
   * 初始化待分区重分配
   * 这些字符串是 znode 名称而不是绝对 znode 路径。 ...
   * 这包括通过/admin/reassign_partition发送的重新分配，这将取代任何已经进行的API重新分配。
   * This includes reassignments sent through /admin/reassign_partitions,
   * which will supplant any API reassignments already in progress.
   */
  private def initializePartitionReassignments(): Unit = {
    // New reassignments may have been submitted through Zookeeper while the controller was failing over
    val zkPartitionsResumed = processZkPartitionReassignment()
    // We may also have some API-based reassignments that need to be restarted
    maybeResumeReassignments { (tp, _) =>
      !zkPartitionsResumed.contains(tp)
    }
  }

  /**
   * 获取
   *
   * @return
   */
  private def fetchTopicDeletionsInProgress(): (Set[String], Set[String]) = {
    // 从 /admin/delete_topics获取待删除的主题
    val topicsToBeDeleted = zkClient.getTopicDeletions.toSet

    // 获取存在离线副本的主题列表
    val topicsWithOfflineReplicas = controllerContext.allTopics.filter { topic => {
      // 根据主题名称获取该主题的所有副本
      val replicasForTopic = controllerContext.replicasForTopic(topic)

      // 判断副本是否离线
      replicasForTopic.exists(r => !controllerContext.isReplicaOnline(r.replica, r.topicPartition))
    }
    }

    // 获取正在处于分区重分配过程的主题
    val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsBeingReassigned.map(_.topic)

    // 获取无删除资格的主题，因为可能这些主题处于重分配过程
    val topicsIneligibleForDeletion = topicsWithOfflineReplicas | topicsForWhichPartitionReassignmentIsInProgress
    info(s"List of topics to be deleted: ${topicsToBeDeleted.mkString(",")}")
    info(s"List of topics ineligible for deletion: ${topicsIneligibleForDeletion.mkString(",")}")
    (topicsToBeDeleted, topicsIneligibleForDeletion)
  }

  private def updateLeaderAndIsrCache(partitions: Seq[TopicPartition] = controllerContext.allPartitions.toSeq): Unit = {
    val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
    leaderIsrAndControllerEpochs.forKeyValue { (partition, leaderIsrAndControllerEpoch) =>
      controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
    }
  }

  /**
   * 判断分区重分配过程是否结束
   *
   * @param partition     分区
   * @param assignment    目标副本分配方案
   * @return
   */
  private def isReassignmentComplete(partition: TopicPartition, assignment: ReplicaAssignment): Boolean = {
    if (!assignment.isBeingReassigned) {
      true
    } else {
      // 从ZK中获取分区详情
      zkClient.getTopicPartitionStates(Seq(partition)).get(partition).exists { leaderIsrAndControllerEpoch =>
        // 获取ZK中ISR数据
        val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr.toSet
        val targetReplicas = assignment.targetReplicas.toSet
        // 判断目标副本是否是ZK ISR的子集，如果是，则返回true，说明分区重分配完成，否则返回false
        targetReplicas.subsetOf(isr)
      }
    }
  }

  private def moveReassignedPartitionLeaderIfRequired(topicPartition: TopicPartition,
                                                      newAssignment: ReplicaAssignment): Unit = {
    val reassignedReplicas = newAssignment.replicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicPartition).get.leaderAndIsr.leader

    if (!reassignedReplicas.contains(currentLeader)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is not in the new list of replicas ${reassignedReplicas.mkString(",")}. Re-electing leader")
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
    } else if (controllerContext.isReplicaOnline(currentLeader, topicPartition)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} and is alive")
      // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
      updateLeaderEpochAndSendRequest(topicPartition, newAssignment)
    } else {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} but is dead")
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
    }
  }

  /**
   * 走完需要删除的副本的剩余流程：OfflineReplica->ReplicaDeletionStarted->ReplicaDeletionSuccessful->NonExistentReplica
   * @param topicPartition   分区
   * @param removedReplicas  待移除的副本对象
   */
  private def stopRemovedReplicasOfReassignedPartition(topicPartition: TopicPartition,
                                                       removedReplicas: Seq[Int]): Unit = {
    // 待删除的副本
    val replicasToBeDeleted = removedReplicas.map(PartitionAndReplica(topicPartition, _))
    // 副本状态机将副本变更为「OfflineReplica」状态，并通知相关Broker进行处理
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica)

    // 将副本状态变更为「ReplicaDeletionStarted」，并通知相关Broker
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted)

    // 将副本状态变更为「ReplicaDeletionSuccessful」，并通知相关Broker
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful)

    // 将副本状态变更为「NonExistentReplica」，并通知相关Broker
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica)
  }

  private def updateReplicaAssignmentForPartition(topicPartition: TopicPartition, assignment: ReplicaAssignment): Unit = {
    val topicAssignment = mutable.Map() ++=
      controllerContext.partitionFullReplicaAssignmentForTopic(topicPartition.topic) +=
      (topicPartition -> assignment)

    val setDataResponse = zkClient.setTopicAssignmentRaw(topicPartition.topic,
      controllerContext.topicIds.get(topicPartition.topic),
      topicAssignment, controllerContext.epochZkVersion)
    setDataResponse.resultCode match {
      case Code.OK =>
        info(s"Successfully updated assignment of partition $topicPartition to $assignment")
      case Code.NONODE =>
        throw new IllegalStateException(s"Failed to update assignment for $topicPartition since the topic " +
          "has no current assignment")
      case _ => throw new KafkaException(setDataResponse.resultException.get)
    }
  }

  private def startNewReplicasForReassignedPartition(topicPartition: TopicPartition, newReplicas: Seq[Int]): Unit = {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Seq(PartitionAndReplica(topicPartition, replica)), NewReplica)
    }
  }

  private def updateLeaderEpochAndSendRequest(topicPartition: TopicPartition,
                                              assignment: ReplicaAssignment): Unit = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    updateLeaderEpoch(topicPartition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        try {
          brokerRequestBatch.newBatch()
          // the isNew flag, when set to true, makes sure that when a replica possibly resided
          // in a logDir that is offline, we refrain from just creating a new replica in a good
          // logDir. This is exactly the behavior we want for the original replicas, but not
          // for the replicas we add in this reassignment. For new replicas, want to be able
          // to assign to one of the good logDirs.
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(assignment.originReplicas, topicPartition,
            updatedLeaderIsrAndControllerEpoch, assignment, isNew = false)
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(assignment.addingReplicas, topicPartition,
            updatedLeaderIsrAndControllerEpoch, assignment, isNew = true)
          brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        } catch {
          case e: IllegalStateException =>
            handleIllegalState(e)
        }
        stateChangeLog.info(s"Sent LeaderAndIsr request $updatedLeaderIsrAndControllerEpoch with " +
          s"new replica assignment $assignment to leader ${updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader} " +
          s"for partition being reassigned $topicPartition")

      case None => // fail the reassignment
        stateChangeLog.error(s"Failed to send LeaderAndIsr request with new replica assignment " +
          s"$assignment to leader for partition being reassigned $topicPartition")
    }
  }

  /**
   * 为新增主题注册 {@link PartitionModificationsHandler}
   *
   * @param topics 新增主题集合列表
   */
  private def registerPartitionModificationsHandlers(topics: Seq[String]) = {
    topics.foreach { topic =>
      // #1 创建新的「PartitionModificationsHandler」对象，并绑定topic
      //    ZK节点发生变动->通知Broker->触发PartitionModificationsHandler相关回调方法->生产Controller事件->交给ControllerEventManager分发并后续处理
      val partitionModificationsHandler = new PartitionModificationsHandler(eventManager, topic)

      // #2 放入全局变量中
      partitionModificationsHandlers.put(topic, partitionModificationsHandler)
    }

    // #3 注册监听器
    partitionModificationsHandlers.values.foreach(zkClient.registerZNodeChangeHandler)
  }

  private[controller] def unregisterPartitionModificationsHandlers(topics: Seq[String]) = {
    topics.foreach { topic =>
      partitionModificationsHandlers.remove(topic).foreach(handler => zkClient.unregisterZNodeChangeHandler(handler.path))
    }
  }

  private def unregisterPartitionReassignmentIsrChangeHandlers(): Unit = {
    if (!isAlterIsrEnabled) {
      controllerContext.partitionsBeingReassigned.foreach { tp =>
        val path = TopicPartitionStateZNode.path(tp)
        zkClient.unregisterZNodeChangeHandler(path)
      }
    }
  }

  private def removePartitionFromReassigningPartitions(topicPartition: TopicPartition,
                                                       assignment: ReplicaAssignment): Unit = {
    if (controllerContext.partitionsBeingReassigned.contains(topicPartition)) {
      if (!isAlterIsrEnabled) {
        val path = TopicPartitionStateZNode.path(topicPartition)
        zkClient.unregisterZNodeChangeHandler(path)
      }
      maybeRemoveFromZkReassignment((tp, replicas) => tp == topicPartition && replicas == assignment.replicas)
      controllerContext.partitionsBeingReassigned.remove(topicPartition)
    } else {
      throw new IllegalStateException("Cannot remove a reassigning partition because it is not present in memory")
    }
  }

  /**
   * Remove partitions from an active zk-based reassignment (if one exists).
   *
   * @param shouldRemoveReassignment Predicate indicating which partition reassignments should be removed
   */
  private def maybeRemoveFromZkReassignment(shouldRemoveReassignment: (TopicPartition, Seq[Int]) => Boolean): Unit = {
    if (!zkClient.reassignPartitionsInProgress)
      return

    val reassigningPartitions = zkClient.getPartitionReassignment
    val (removingPartitions, updatedPartitionsBeingReassigned) = reassigningPartitions.partition { case (tp, replicas) =>
      shouldRemoveReassignment(tp, replicas)
    }
    info(s"Removing partitions $removingPartitions from the list of reassigned partitions in zookeeper")

    // write the new list to zookeeper
    if (updatedPartitionsBeingReassigned.isEmpty) {
      info(s"No more partitions need to be reassigned. Deleting zk path ${ReassignPartitionsZNode.path}")
      zkClient.deletePartitionReassignment(controllerContext.epochZkVersion)
      // Ensure we detect future reassignments
      eventManager.put(ZkPartitionReassignment)
    } else {
      try {
        zkClient.setOrCreatePartitionReassignment(updatedPartitionsBeingReassigned, controllerContext.epochZkVersion)
      } catch {
        case e: KeeperException => throw new AdminOperationException(e)
      }
    }
  }

  private def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicPartition],
                                                           isTriggeredByAutoRebalance: Boolean): Unit = {
    for (partition <- partitionsToBeRemoved) {
      // check the status
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if (currentLeader == preferredReplica) {
        info(s"Partition $partition completed preferred replica leader election. New leader is $preferredReplica")
      } else {
        warn(s"Partition $partition failed to complete preferred replica leader election to $preferredReplica. " +
          s"Leader is still $currentLeader")
      }
    }
    if (!isTriggeredByAutoRebalance) {
      zkClient.deletePreferredReplicaElection(controllerContext.epochZkVersion)
      // Ensure we detect future preferred replica leader elections
      eventManager.put(ReplicaLeaderElection(None, ElectionType.PREFERRED, ZkTriggered))
    }
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   *
   * @param brokers The brokers that the update metadata request should be sent to
   */
  private[controller] def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicPartition]): Unit = {
    try {
      brokerRequestBatch.newBatch()
      brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   *
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  private def updateLeaderEpoch(partition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    debug(s"Updating leader epoch for partition $partition")
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      zkWriteCompleteOrUnnecessary = zkClient.getTopicPartitionStates(Seq(partition)).get(partition) match {
        case Some(leaderIsrAndControllerEpoch) =>
          val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
          if (controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably " +
              s"means the current controller with epoch $epoch went through a soft failure and another " +
              s"controller was elected with epoch $controllerEpoch. Aborting state change by this controller")
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = leaderAndIsr.newEpochAndZkVersion
          // update the new leadership decision in zookeeper or retry
          val UpdateLeaderAndIsrResult(finishedUpdates, _) =
            zkClient.updateLeaderAndIsr(immutable.Map(partition -> newLeaderAndIsr), epoch, controllerContext.epochZkVersion)

          finishedUpdates.get(partition) match {
            case Some(Right(leaderAndIsr)) =>
              val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, epoch)
              controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
              finalLeaderIsrAndControllerEpoch = Some(leaderIsrAndControllerEpoch)
              info(s"Updated leader epoch for partition $partition to ${leaderAndIsr.leaderEpoch}")
              true
            case Some(Left(e)) => throw e
            case None => false
          }
        case None =>
          throw new IllegalStateException(s"Cannot update leader epoch for partition $partition as " +
            "leaderAndIsr path is empty. This could mean we somehow tried to reassign a partition that doesn't exist")
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  private def checkAndTriggerAutoLeaderRebalance(): Unit = {
    trace("Checking need to trigger auto leader balancing")
    val preferredReplicasForTopicsByBrokers: Map[Int, Map[TopicPartition, Seq[Int]]] =
      controllerContext.allPartitions.filterNot {
        tp => topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
      }.map { tp =>
        (tp, controllerContext.partitionReplicaAssignment(tp))
      }.toMap.groupBy { case (_, assignedReplicas) => assignedReplicas.head }

    // for each broker, check if a preferred replica election needs to be triggered
    preferredReplicasForTopicsByBrokers.forKeyValue { (leaderBroker, topicPartitionsForBroker) =>
      val topicsNotInPreferredReplica = topicPartitionsForBroker.filter { case (topicPartition, _) =>
        val leadershipInfo = controllerContext.partitionLeadershipInfo(topicPartition)
        leadershipInfo.exists(_.leaderAndIsr.leader != leaderBroker)
      }
      debug(s"Topics not in preferred replica for broker $leaderBroker $topicsNotInPreferredReplica")

      val imbalanceRatio = topicsNotInPreferredReplica.size.toDouble / topicPartitionsForBroker.size
      trace(s"Leader imbalance ratio for broker $leaderBroker is $imbalanceRatio")

      // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
      // that need to be on this broker
      if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
        // do this check only if the broker is live and there are no partitions being reassigned currently
        // and preferred replica election is not in progress
        val candidatePartitions = topicsNotInPreferredReplica.keys.filter(tp =>
          controllerContext.partitionsBeingReassigned.isEmpty &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic) &&
            controllerContext.allTopics.contains(tp.topic) &&
            canPreferredReplicaBeLeader(tp)
        )
        onReplicaElection(candidatePartitions.toSet, ElectionType.PREFERRED, AutoTriggered)
      }
    }
  }

  private def canPreferredReplicaBeLeader(tp: TopicPartition): Boolean = {
    val assignment = controllerContext.partitionReplicaAssignment(tp)
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, tp))
    val isr = controllerContext.partitionLeadershipInfo(tp).get.leaderAndIsr.isr
    PartitionLeaderElectionAlgorithms
      .preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
      .nonEmpty
  }

  private def processAutoPreferredReplicaLeaderElection(): Unit = {
    if (!isActive) return
    try {
      info("Processing automatic preferred replica leader election")
      checkAndTriggerAutoLeaderRebalance()
    } finally {
      scheduleAutoLeaderRebalanceTask(delay = config.leaderImbalanceCheckIntervalSeconds, unit = TimeUnit.SECONDS)
    }
  }

  private def processUncleanLeaderElectionEnable(): Unit = {
    if (!isActive) return
    info("Unclean leader election has been enabled by default")
    partitionStateMachine.triggerOnlinePartitionStateChange()
  }

  private def processTopicUncleanLeaderElectionEnable(topic: String): Unit = {
    if (!isActive) return
    info(s"Unclean leader election has been enabled for topic $topic")
    partitionStateMachine.triggerOnlinePartitionStateChange(topic)
  }

  private def processControlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    val controlledShutdownResult = Try {
      doControlledShutdown(id, brokerEpoch)
    }
    controlledShutdownCallback(controlledShutdownResult)
  }

  private def doControlledShutdown(id: Int, brokerEpoch: Long): Set[TopicPartition] = {
    if (!isActive) {
      throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
    }

    // broker epoch in the request is unknown if the controller hasn't been upgraded to use KIP-380
    // so we will keep the previous behavior and don't reject the request
    if (brokerEpoch != AbstractControlRequest.UNKNOWN_BROKER_EPOCH) {
      val cachedBrokerEpoch = controllerContext.liveBrokerIdAndEpochs(id)
      if (brokerEpoch < cachedBrokerEpoch) {
        val stateBrokerEpochErrorMessage = "Received controlled shutdown request from an old broker epoch " +
          s"$brokerEpoch for broker $id. Current broker epoch is $cachedBrokerEpoch."
        info(stateBrokerEpochErrorMessage)
        throw new StaleBrokerEpochException(stateBrokerEpochErrorMessage)
      }
    }

    info(s"Shutting down broker $id")

    if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
      throw new BrokerNotAvailableException(s"Broker id $id does not exist.")

    controllerContext.shuttingDownBrokerIds.add(id)
    debug(s"All shutting down brokers: ${controllerContext.shuttingDownBrokerIds.mkString(",")}")
    debug(s"Live brokers: ${controllerContext.liveBrokerIds.mkString(",")}")

    val partitionsToActOn = controllerContext.partitionsOnBroker(id).filter { partition =>
      controllerContext.partitionReplicaAssignment(partition).size > 1 &&
        controllerContext.partitionLeadershipInfo(partition).isDefined &&
        !topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic)
    }
    val (partitionsLedByBroker, partitionsFollowedByBroker) = partitionsToActOn.partition { partition =>
      controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader == id
    }
    partitionStateMachine.handleStateChanges(partitionsLedByBroker.toSeq, OnlinePartition, Some(ControlledShutdownPartitionLeaderElectionStrategy))
    try {
      brokerRequestBatch.newBatch()
      partitionsFollowedByBroker.foreach { partition =>
        brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), partition, deletePartition = false)
      }
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
    // If the broker is a follower, updates the isr in ZK and notifies the current leader
    replicaStateMachine.handleStateChanges(partitionsFollowedByBroker.map(partition =>
      PartitionAndReplica(partition, id)).toSeq, OfflineReplica)
    trace(s"All leaders = ${controllerContext.partitionsLeadershipInfo.mkString(",")}")
    controllerContext.partitionLeadersOnBroker(id)
  }

  private def processUpdateMetadataResponseReceived(updateMetadataResponse: UpdateMetadataResponse, brokerId: Int): Unit = {
    if (!isActive) return

    if (updateMetadataResponse.error != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${updateMetadataResponse.error} in UpdateMetadata " +
        s"response $updateMetadataResponse from broker $brokerId")
    }
  }

  private def processLeaderAndIsrResponseReceived(leaderAndIsrResponse: LeaderAndIsrResponse, brokerId: Int): Unit = {
    if (!isActive) return

    if (leaderAndIsrResponse.error != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${leaderAndIsrResponse.error} in LeaderAndIsr " +
        s"response $leaderAndIsrResponse from broker $brokerId")
      return
    }

    val offlineReplicas = new ArrayBuffer[TopicPartition]()
    val onlineReplicas = new ArrayBuffer[TopicPartition]()

    leaderAndIsrResponse.partitionErrors(controllerContext.topicNames.asJava).forEach { case (tp, error) =>
      if (error.code() == Errors.KAFKA_STORAGE_ERROR.code)
        offlineReplicas += tp
      else if (error.code() == Errors.NONE.code)
        onlineReplicas += tp
    }

    val previousOfflineReplicas = controllerContext.replicasOnOfflineDirs.getOrElse(brokerId, Set.empty[TopicPartition])
    val currentOfflineReplicas = mutable.Set() ++= previousOfflineReplicas --= onlineReplicas ++= offlineReplicas
    controllerContext.replicasOnOfflineDirs.put(brokerId, currentOfflineReplicas)
    val newOfflineReplicas = currentOfflineReplicas.diff(previousOfflineReplicas)

    if (newOfflineReplicas.nonEmpty) {
      stateChangeLogger.info(s"Mark replicas ${newOfflineReplicas.mkString(",")} on broker $brokerId as offline")
      onReplicasBecomeOffline(newOfflineReplicas.map(PartitionAndReplica(_, brokerId)))
    }
  }

  private def processTopicDeletionStopReplicaResponseReceived(replicaId: Int,
                                                              requestError: Errors,
                                                              partitionErrors: Map[TopicPartition, Errors]): Unit = {
    if (!isActive) return
    debug(s"Delete topic callback invoked on StopReplica response received from broker $replicaId: " +
      s"request error = $requestError, partition errors = $partitionErrors")

    val partitionsInError = if (requestError != Errors.NONE)
      partitionErrors.keySet
    else
      partitionErrors.filter { case (_, error) => error != Errors.NONE }.keySet

    val replicasInError = partitionsInError.map(PartitionAndReplica(_, replicaId))
    // move all the failed replicas to ReplicaDeletionIneligible
    topicDeletionManager.failReplicaDeletion(replicasInError)
    if (replicasInError.size != partitionErrors.size) {
      // some replicas could have been successfully deleted
      val deletedReplicas = partitionErrors.keySet.diff(partitionsInError)
      topicDeletionManager.completeReplicaDeletion(deletedReplicas.map(PartitionAndReplica(_, replicaId)))
    }
  }

  /**
   * Controller启动事件会触发此方法执行
   * ① 注册 {@link ControllerChangeHandler} 处理器
   * ② 执行选举
   */
  private def processStartup(): Unit = {
    // #1 向ZookeeperClient注册Controller变更处理器，
    // 如果后续Zookeeper节点/controller变更，则会触发这个handler执行
    zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
    // #2 执行选举
    elect()
  }

  private def updateMetrics(): Unit = {
    offlinePartitionCount =
      if (!isActive) {
        0
      } else {
        controllerContext.offlinePartitionCount
      }

    preferredReplicaImbalanceCount =
      if (!isActive) {
        0
      } else {
        controllerContext.preferredReplicaImbalanceCount
      }

    globalTopicCount = if (!isActive) 0 else controllerContext.allTopics.size

    globalPartitionCount = if (!isActive) 0 else controllerContext.partitionWithLeadersCount

    topicsToDeleteCount = if (!isActive) 0 else controllerContext.topicsToBeDeleted.size

    replicasToDeleteCount = if (!isActive) 0 else controllerContext.topicsToBeDeleted.map { topic =>
      // For each enqueued topic, count the number of replicas that are not yet deleted
      controllerContext.replicasForTopic(topic).count { replica =>
        controllerContext.replicaState(replica) != ReplicaDeletionSuccessful
      }
    }.sum

    ineligibleTopicsToDeleteCount = if (!isActive) 0 else controllerContext.topicsIneligibleForDeletion.size

    ineligibleReplicasToDeleteCount = if (!isActive) 0 else controllerContext.topicsToBeDeleted.map { topic =>
      // For each enqueued topic, count the number of replicas that are ineligible
      controllerContext.replicasForTopic(topic).count { replica =>
        controllerContext.replicaState(replica) == ReplicaDeletionIneligible
      }
    }.sum
  }

  // visible for testing
  private[controller] def handleIllegalState(e: IllegalStateException): Nothing = {
    // Resign if the controller is in an illegal state
    error("Forcing the controller to resign")
    brokerRequestBatch.clear()
    triggerControllerMove()
    throw e
  }

  private def triggerControllerMove(): Unit = {
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    if (!isActive) {
      warn("Controller has already moved when trying to trigger controller movement")
      return
    }
    try {
      val expectedControllerEpochZkVersion = controllerContext.epochZkVersion
      activeControllerId = -1
      onControllerResignation()
      zkClient.deleteController(expectedControllerEpochZkVersion)
    } catch {
      case _: ControllerMovedException =>
        warn("Controller has already moved when trying to trigger controller movement")
    }
  }

  /**
   * controller可能退位：如果ZK的「/controller」节点的controller_id不等于当前broker id，
   * 说明controller易主，当前broker就需要执行退位操作，否则，controller仍在当前broker上，不做任何处理
   */
  private def maybeResign(): Unit = {
    val wasActiveBeforeChange = isActive
    // #1 注册「ControllerChangeHandler」处理器
    zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)

    // #2 从ZK中获取「/controller」节点的controller_id值
    activeControllerId = zkClient.getControllerId.getOrElse(-1)

    // #3 如果确认controller已经变更，则当前broker执行controller退位逻辑
    if (wasActiveBeforeChange && !isActive) {
      onControllerResignation()
    }
  }

  /**
   * Controller选举核心方法：
   * ① 首先确认「/controller」的broker id 是否>=0，
   * 如果是，则说明Controller已存在，如果为-1，则需要进行接下来的选举操作（注意，并非100%竞选成功）
   * ② 再尝试获取两个版本号，分别是/controller_epoch的值和这个节点对应的ZK的version的值，称为zkVersion
   * ③
   */
  private def elect(): Unit = {
    // #1 从Zookeeper获取Controller Id，即所在的Broker ID（/controller）。
    //    如果没有则为-1，意味着当前此刻集群没有选出确切的controller
    activeControllerId = zkClient.getControllerId.getOrElse(-1)

    /**
     * We can get here during the initial startup and the handleDeleted ZK callback.
     * Because of the potential race condition,
     * it's possible that the controller has already been elected when we get here.
     *
     * This check will prevent the following
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    // #2 id 不等于-1，表示集群已经选出Controller，直接返回
    if (activeControllerId != -1) {
      debug(s"Broker $activeControllerId has been elected as the controller, so stopping the election process.")
      return
    }

    try {
      // #3 此刻broker还没有确认controller，所以参与controller选举
      //    ① 注册controller相关信息，创建/controller节点
      val (epoch, epochZkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(config.brokerId)
      controllerContext.epoch = epoch
      controllerContext.epochZkVersion = epochZkVersion
      activeControllerId = config.brokerId

      info(s"${config.brokerId} successfully elected as the controller. Epoch incremented to ${controllerContext.epoch} " +
        s"and epoch zk version is now ${controllerContext.epochZkVersion}")

      // #4 执行成功当选controller的后续逻辑（failover：故障转移）
      onControllerFailover()
    } catch {
      case e: ControllerMovedException =>
        maybeResign()

        if (activeControllerId != -1)
          debug(s"Broker $activeControllerId was elected as controller instead of broker ${config.brokerId}", e)
        else
          warn("A controller has been elected but just resigned, this will result in another round of election", e)
      case t: Throwable =>
        error(s"Error while electing or becoming controller on broker ${config.brokerId}. " +
          s"Trigger controller movement immediately", t)
        triggerControllerMove()
    }
  }

  /**
   * Partitions the provided map of brokers and epochs into 2 new maps:
   *  - The first map contains only those brokers whose features were found to be compatible with
   *    the existing finalized features.
   *  - The second map contains only those brokers whose features were found to be incompatible with
   *    the existing finalized features.
   *
   * @param brokersAndEpochs the map to be partitioned
   * @return two maps: first contains compatible brokers and second contains
   *         incompatible brokers as explained above
   */
  private def partitionOnFeatureCompatibility(brokersAndEpochs: Map[Broker, Long]): (Map[Broker, Long], Map[Broker, Long]) = {
    // There can not be any feature incompatibilities when the feature versioning system is disabled
    // or when the finalized feature cache is empty. Otherwise, we check if the non-empty contents
    //  of the cache are compatible with the supported features of each broker.
    brokersAndEpochs.partition {
      case (broker, _) =>
        !config.isFeatureVersioningSupported ||
          !featureCache.get.exists(
            latestFinalizedFeatures =>
              BrokerFeatures.hasIncompatibleFeatures(broker.features, latestFinalizedFeatures.features))
    }
  }

  /**
   * 处理brokers数量变更，比如新增、删除broker
   */
  private def processBrokerChange(): Unit = {
    if (!isActive) return

    // #1 集群「在线」broker列表（最新、最权威数据）：获取「/brokers/ids」节点的详情
    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster

    val curBrokerIdAndEpochs = curBrokerAndEpochs map { case (broker, epoch) => (broker.id, epoch) }
    val curBrokerIds = curBrokerIdAndEpochs.keySet
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds

    // #2 本轮「新增」broker列表
    val newBrokerIds = curBrokerIds.diff(liveOrShuttingDownBrokerIds)

    // #3 本轮「离线」broker列表
    val deadBrokerIds = liveOrShuttingDownBrokerIds.diff(curBrokerIds)

    // #4 本轮「重启」的broker列表：通过判断Epoch值是否与controller缓存值相等，从而得出broker是否发生过重启
    val bouncedBrokerIds = (curBrokerIds & liveOrShuttingDownBrokerIds)
      .filter(brokerId => curBrokerIdAndEpochs(brokerId) > controllerContext.liveBrokerIdAndEpochs(brokerId))

    // #5 准备数据
    val newBrokerAndEpochs = curBrokerAndEpochs.filter { case (broker, _) => newBrokerIds.contains(broker.id) }
    val bouncedBrokerAndEpochs = curBrokerAndEpochs.filter { case (broker, _) => bouncedBrokerIds.contains(broker.id) }
    val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
    val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
    val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
    val bouncedBrokerIdsSorted = bouncedBrokerIds.toSeq.sorted
    info(s"Newly added brokers: ${newBrokerIdsSorted.mkString(",")}, " +
      s"deleted brokers: ${deadBrokerIdsSorted.mkString(",")}, " +
      s"bounced brokers: ${bouncedBrokerIdsSorted.mkString(",")}, " +
      s"all live brokers: ${liveBrokerIdsSorted.mkString(",")}")

    // #6 为新增的broker准备网络相关的组件，这些是由「ControllerChannelManager」管理的
    newBrokerAndEpochs.keySet.foreach(controllerChannelManager.addBroker)

    // #7 broker发生重启，需要先移除controller缓存数据，再新增该broker
    // 因为重启过的Broker可能修改了配置等信息，所以需要重新加入缓存
    bouncedBrokerIds.foreach(controllerChannelManager.removeBroker)
    bouncedBrokerAndEpochs.keySet.foreach(controllerChannelManager.addBroker)

    // #8 移除「离线」broker 相关缓存
    deadBrokerIds.foreach(controllerChannelManager.removeBroker)

    // #9 处理新增的Broker
    if (newBrokerIds.nonEmpty) {
      // #9-1 区分兼容的和不兼容的brokers
      val (newCompatibleBrokerAndEpochs, newIncompatibleBrokerAndEpochs) =
        partitionOnFeatureCompatibility(newBrokerAndEpochs)
      if (!newIncompatibleBrokerAndEpochs.isEmpty) {
        warn("Ignoring registration of new brokers due to incompatibilities with finalized features: " +
          newIncompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
      }

      // #9-2 只有新的可兼容的broker才能被添加进缓存中
      controllerContext.addLiveBrokers(newCompatibleBrokerAndEpochs)

      // #9-3 为新的broker启动相关组件
      onBrokerStartup(newBrokerIdsSorted)
    }

    // #10 处理重启的Brokers
    if (bouncedBrokerIds.nonEmpty) {
      // #10-1 移除缓存
      controllerContext.removeLiveBrokers(bouncedBrokerIds)
      onBrokerFailure(bouncedBrokerIdsSorted)
      val (bouncedCompatibleBrokerAndEpochs, bouncedIncompatibleBrokerAndEpochs) =
        partitionOnFeatureCompatibility(bouncedBrokerAndEpochs)
      if (!bouncedIncompatibleBrokerAndEpochs.isEmpty) {
        warn("Ignoring registration of bounced brokers due to incompatibilities with finalized features: " +
          bouncedIncompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
      }
      // #10-2 重新加入
      controllerContext.addLiveBrokers(bouncedCompatibleBrokerAndEpochs)
      onBrokerStartup(bouncedBrokerIdsSorted)
    }

    // #11 处理「离线」的brokers
    if (deadBrokerIds.nonEmpty) {
      // 移除缓存、调用onBrokerFailure
      controllerContext.removeLiveBrokers(deadBrokerIds)
      onBrokerFailure(deadBrokerIdsSorted)
    }

    if (newBrokerIds.nonEmpty || deadBrokerIds.nonEmpty || bouncedBrokerIds.nonEmpty) {
      info(s"Updated broker epochs cache: ${controllerContext.liveBrokerIdAndEpochs}")
    }
  }

  /**
   * 处理「/brokers/ids/[broker id]」数据变更事件
   *
   * @param brokerId
   */
  private def processBrokerModification(brokerId: Int): Unit = {
    if (!isActive) return
    // #1 获取「/brokers/ids/[broker id]」节点数据，包含监听器（IP+PORT）、安全协议等
    val newMetadataOpt = zkClient.getBroker(brokerId)

    // #2 获取缓存数据
    val oldMetadataOpt = controllerContext.liveOrShuttingDownBroker(brokerId)
    if (newMetadataOpt.nonEmpty && oldMetadataOpt.nonEmpty) {
      val oldMetadata = oldMetadataOpt.get
      val newMetadata = newMetadataOpt.get
      // #3 如果两者不相等，说明Broker数据发生变更，那么就需要更新缓存中的数据，
      //    以及执行「onBrokerUpdate」方法处理Broker更新逻辑
      if (newMetadata.endPoints != oldMetadata.endPoints
        || !oldMetadata.features.equals(newMetadata.features)) {
        info(s"Updated broker metadata: $oldMetadata -> $newMetadata")
        controllerContext.updateBrokerMetadata(oldMetadata, newMetadata)
        // 广播元数据更新请求
        onBrokerUpdate(brokerId)
      }
    }
  }

  /**
   * 处理主题创建/变更事件，一旦向ZK节点「/brokers/topics」新增主题或修改主题元数据信息，
   * 该监听器就会被触发，然后经过 {@link KafkaController.process( )} 方法路由到这里处理此事件
   */
  private def processTopicChange(): Unit = {
    // #1 如果当前Controller已经被关闭，直接返回
    if (!isActive) return

    // #2 获取「/brokers/topics」节点所有主题元数据
    val topics = zkClient.getAllTopicsInCluster(true)

    // #3 与缓存数据比较，找出新增主题列表和已删除主题列表
    val newTopics = topics -- controllerContext.allTopics
    val deletedTopics = controllerContext.allTopics.diff(topics)

    // #4 更新Controller缓存数据
    controllerContext.setAllTopics(topics)

    // #5 为新增的主题注册分区变更监听器，这样当前Controller就可以感知分区变更了
    //    每个主题路径都会对应一个PartitionModificationsHandler处理器
    registerPartitionModificationsHandlers(newTopics.toSeq)

    // #6 从ZK中获取新增主题的副本分区情况
    val addedPartitionReplicaAssignment =
      zkClient.getReplicaAssignmentAndTopicIdForTopics(newTopics)

    // #7 为「已删除」主题清除元数据缓存
    deletedTopics.foreach(controllerContext.removeTopic)

    // #8 为新增主题更新元数据缓存中的副本
    processTopicIds(addedPartitionReplicaAssignment)

    addedPartitionReplicaAssignment.foreach { case TopicIdReplicaAssignment(_, _, newAssignments) =>
      newAssignments.foreach { case (topicAndPartition, newReplicaAssignment) =>
        controllerContext.updatePartitionFullReplicaAssignment(topicAndPartition, newReplicaAssignment)
      }
    }
    info(s"New topics: [$newTopics], deleted topics: [$deletedTopics], new partition replica assignment " +
      s"[$addedPartitionReplicaAssignment]")

    // #9 调整新增主题所有分区以及所属所有副本的运行状态为「上线」状态
    if (addedPartitionReplicaAssignment.nonEmpty) {
      val partitionAssignments = addedPartitionReplicaAssignment
        .map { case TopicIdReplicaAssignment(_, _, partitionsReplicas) => partitionsReplicas.keySet }
        .reduce((s1, s2) => s1.union(s2))
      onNewPartitionCreation(partitionAssignments)
    }
  }

  /**
   * 如果znode中topic_id字段为空，则使用UUID生成一个值并写入znode
   *
   * @param topicIdAssignments
   */
  private def processTopicIds(topicIdAssignments: Set[TopicIdReplicaAssignment]): Unit = {
    // kafka2.8版本支持设置topicid
    if (config.usesTopicId) {
      val updated = zkClient.setTopicIds(topicIdAssignments.filter(_.topicId.isEmpty), controllerContext.epochZkVersion)
      val allTopicIdAssignments = updated ++ topicIdAssignments.filter(_.topicId.isDefined)
      allTopicIdAssignments.foreach(topicIdAssignment => controllerContext.addTopicId(topicIdAssignment.topic, topicIdAssignment.topicId.get))
    }
  }

  private def processLogDirEventNotification(): Unit = {
    if (!isActive) return
    // 获取 /log_dir_event_notification 节点数据
    val sequenceNumbers = zkClient.getAllLogDirEventNotifications
    try {
      // 获取日志路径对应的BrokerID
      val brokerIds = zkClient.getBrokerIdsFromLogDirEvents(sequenceNumbers)
      //
      onBrokerLogDirFailure(brokerIds)
    } finally {
      // delete processed children
      zkClient.deleteLogDirEventNotifications(sequenceNumbers, controllerContext.epochZkVersion)
    }
  }

  private def processPartitionModifications(topic: String): Unit = {
    def restorePartitionReplicaAssignment(
                                           topic: String,
                                           newPartitionReplicaAssignment: Map[TopicPartition, ReplicaAssignment]
                                         ): Unit = {
      info("Restoring the partition replica assignment for topic %s".format(topic))

      val existingPartitions = zkClient.getChildren(TopicPartitionsZNode.path(topic))
      val existingPartitionReplicaAssignment = newPartitionReplicaAssignment
        .filter(p => existingPartitions.contains(p._1.partition.toString))
        .map { case (tp, _) =>
          tp -> controllerContext.partitionFullReplicaAssignment(tp)
        }.toMap

      zkClient.setTopicAssignment(topic,
        controllerContext.topicIds.get(topic),
        existingPartitionReplicaAssignment,
        controllerContext.epochZkVersion)
    }

    if (!isActive) return
    val partitionReplicaAssignment = zkClient.getFullReplicaAssignmentForTopics(immutable.Set(topic))
    val partitionsToBeAdded = partitionReplicaAssignment.filter { case (topicPartition, _) =>
      controllerContext.partitionReplicaAssignment(topicPartition).isEmpty
    }

    if (topicDeletionManager.isTopicQueuedUpForDeletion(topic)) {
      if (partitionsToBeAdded.nonEmpty) {
        warn("Skipping adding partitions %s for topic %s since it is currently being deleted"
          .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))

        restorePartitionReplicaAssignment(topic, partitionReplicaAssignment)
      } else {
        // This can happen if existing partition replica assignment are restored to prevent increasing partition count during topic deletion
        info("Ignoring partition change during topic deletion as no new partitions are added")
      }
    } else if (partitionsToBeAdded.nonEmpty) {
      info(s"New partitions to be added $partitionsToBeAdded")
      partitionsToBeAdded.forKeyValue { (topicPartition, assignedReplicas) =>
        controllerContext.updatePartitionFullReplicaAssignment(topicPartition, assignedReplicas)
      }
      onNewPartitionCreation(partitionsToBeAdded.keySet)
    }
  }

  /**
   * 处理主题删除事件，监听「/admin/delete_topics」节点，如果数据变更，
   * 意味着有主题需要被删除，controller会回调此方法完成删除逻辑
   */
  private def processTopicDeletion(): Unit = {
    if (!isActive) return
    // #1 获取「/admin/delete_topics」节点的所有数据，得到待删除主题的列表
    var topicsToBeDeleted = zkClient.getTopicDeletions.toSet
    debug(s"Delete topics listener fired for topics ${topicsToBeDeleted.mkString(",")} to be deleted")

    // #2 找出缓存中不存在的主题列表，这些主题不需要从controller缓存中移除，只需要将节点从ZK中移除即可
    val nonExistentTopics = topicsToBeDeleted -- controllerContext.allTopics
    if (nonExistentTopics.nonEmpty) {
      warn(s"Ignoring request to delete non-existing topics ${nonExistentTopics.mkString(",")}")
      zkClient.deleteTopicDeletions(nonExistentTopics.toSeq, controllerContext.epochZkVersion)
    }

    // #3 缓存中存在，那么相对复杂一点
    topicsToBeDeleted --= nonExistentTopics
    if (config.deleteTopicEnable) {
      // #3-1 如果「delete.topic.enable」参数为true，则说明启用主题删除功能
      if (topicsToBeDeleted.nonEmpty) {
        info(s"Starting topic deletion for topics ${topicsToBeDeleted.mkString(",")}")
        // mark topic ineligible for deletion if other state changes are in progress

        // #3-2 更新相关的缓存数据
        topicsToBeDeleted.foreach { topic =>
          // #3-3 找出正在迁移中的主题
          val partitionReassignmentInProgress =
            controllerContext.partitionsBeingReassigned.map(_.topic).contains(topic)

          // #3-4 判断是否正在执行分区迁移的主题
          if (partitionReassignmentInProgress) {
            // 如果存在，则将待删除主题交给「TopicDeletionManager」处理
            topicDeletionManager.markTopicIneligibleForDeletion(Set(topic),
              reason = "topic reassignment in progress")
          }
        }
        // #3-5 将此刻可以删除的主题交给「TopicDeletionManager」，它执行真正的删除逻辑
        topicDeletionManager.enqueueTopicsForDeletion(topicsToBeDeleted)
      }
    } else {
      // #4 不允许删除主题，则移除「/admin/delete_topics」节点
      info(s"Removing $topicsToBeDeleted since delete topic is disabled")
      zkClient.deleteTopicDeletions(topicsToBeDeleted.toSeq, controllerContext.epochZkVersion)
    }
  }

  /**
   * 如果我们想对某个分区手动更改副本分配情况，可以按以下步骤进行操作
   * 1. 编写分区重分区JSON文件，这个文件是目标分区副本分配方案，如
   * {
   * "version":1,
   *   "partitions":[
   *     {"topic":"product", "partition":0, "replicas":[4,5,6]},
   *     {"topic":"product", "partition":1, "replicas":[1,2,3]},
   *     {"topic":"product", "partition":4, "replicas":[4,5,6]}
   *  ]
   * }
   * 2. 执行分区重分配命令 kafka-reassign-partition.sh --reassignment-json-file xxx.json --execute
   * 3. 其实这个命令本质是将分配方案写入ZK的/admin/reassign_partitions节点中，Controller会感知到该节点的数据变化，
   *    然后对该分区进行分区重分配操作
   *
   * @return
   */
  private def processZkPartitionReassignment(): Set[TopicPartition] = {
    // #1 注册监听器并判断节点/admin/reassign_partitions是否存在，如果不存在，则返回空集合
    if (isActive && zkClient.registerZNodeChangeHandlerAndCheckExistence(partitionReassignmentHandler)) {
      // 节点存在数据，说明有等待（pending）进行分区重分配的分区集合
      // 分区重分配结果<分区,错误码>
      val reassignmentResults = mutable.Map.empty[TopicPartition, ApiError]
      // 要重新分配的分区<分区, 副本分区详情>
      val partitionsToReassign = mutable.Map.empty[TopicPartition, ReplicaAssignment]

      // #2 对目标分配方案进行分类
      // 从Zookeeper获取/admin/reassign_partitions数据，节点保存的是目标分配方案
      zkClient.getPartitionReassignment.forKeyValue { (tp, targetReplicas) =>
        // 是否构建
        maybeBuildReassignment(tp, Some(targetReplicas)) match {
          case Some(context) => partitionsToReassign.put(tp, context)
          case None => reassignmentResults.put(tp, new ApiError(Errors.NO_REASSIGNMENT_IN_PROGRESS))
        }
      }

      reassignmentResults ++= maybeTriggerPartitionReassignment(partitionsToReassign)
      val (partitionsReassigned, partitionsFailed) = reassignmentResults.partition(_._2.error == Errors.NONE)
      if (partitionsFailed.nonEmpty) {
        warn(s"Failed reassignment through zk with the following errors: $partitionsFailed")
        maybeRemoveFromZkReassignment((tp, _) => partitionsFailed.contains(tp))
      }
      partitionsReassigned.keySet
    } else {
      Set.empty
    }
  }

  /**
   * Process a partition reassignment from the AlterPartitionReassignment API. If there is an
   * existing reassignment through zookeeper for any of the requested partitions, they will be
   * cancelled prior to beginning the new reassignment. Any zk-based reassignment for partitions
   * which are NOT included in this call will not be affected.
   *
   * @param reassignments Map of reassignments passed through the AlterReassignments API. A null value
   *                      means that we should cancel an in-progress reassignment.
   * @param callback      Callback to send AlterReassignments response
   */
  private def processApiPartitionReassignment(reassignments: Map[TopicPartition, Option[Seq[Int]]],
                                              callback: AlterReassignmentsCallback): Unit = {
    if (!isActive) {
      callback(Right(new ApiError(Errors.NOT_CONTROLLER)))
    } else {
      val reassignmentResults = mutable.Map.empty[TopicPartition, ApiError]
      val partitionsToReassign = mutable.Map.empty[TopicPartition, ReplicaAssignment]

      reassignments.forKeyValue { (tp, targetReplicas) =>
        val maybeApiError = targetReplicas.flatMap(validateReplicas(tp, _))
        maybeApiError match {
          case None =>
            maybeBuildReassignment(tp, targetReplicas) match {
              case Some(context) => partitionsToReassign.put(tp, context)
              case None => reassignmentResults.put(tp, new ApiError(Errors.NO_REASSIGNMENT_IN_PROGRESS))
            }
          case Some(err) =>
            reassignmentResults.put(tp, err)
        }
      }

      // The latest reassignment (whether by API or through zk) always takes precedence,
      // so remove from active zk reassignment (if one exists)
      maybeRemoveFromZkReassignment((tp, _) => partitionsToReassign.contains(tp))

      reassignmentResults ++= maybeTriggerPartitionReassignment(partitionsToReassign)
      callback(Left(reassignmentResults))
    }
  }

  private def validateReplicas(topicPartition: TopicPartition, replicas: Seq[Int]): Option[ApiError] = {
    val replicaSet = replicas.toSet
    if (replicas.isEmpty)
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
        s"Empty replica list specified in partition reassignment."))
    else if (replicas.size != replicaSet.size) {
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
        s"Duplicate replica ids in partition reassignment replica list: $replicas"))
    } else if (replicas.exists(_ < 0))
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
        s"Invalid broker id in replica list: $replicas"))
    else {
      // Ensure that any new replicas are among the live brokers
      val currentAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
      val newAssignment = currentAssignment.reassignTo(replicas)
      val areNewReplicasAlive = newAssignment.addingReplicas.toSet.subsetOf(controllerContext.liveBrokerIds)
      if (!areNewReplicasAlive)
        Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
          s"Replica assignment has brokers that are not alive. Replica list: " +
            s"${newAssignment.addingReplicas}, live broker list: ${controllerContext.liveBrokerIds}"))
      else None
    }
  }

  /**
   *
   * @param topicPartition      分区详情
   * @param targetReplicasOpt   目标分配方案（可选）
   * @return
   */
  private def maybeBuildReassignment(topicPartition: TopicPartition,
                                     targetReplicasOpt: Option[Seq[Int]]): Option[ReplicaAssignment] = {
    // 从缓存中获取分区的所有副本（当前, old）
    val replicaAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)

    // 判断当前副本是否正处于重分配过程
    if (replicaAssignment.isBeingReassigned) {
      // 是，那么
      val targetReplicas = targetReplicasOpt.getOrElse(replicaAssignment.originReplicas)
      Some(replicaAssignment.reassignTo(targetReplicas))
    } else {
      // 不处于重分配过程，
      targetReplicasOpt.map { targetReplicas =>
        replicaAssignment.reassignTo(targetReplicas)
      }
    }
  }

  private def processPartitionReassignmentIsrChange(topicPartition: TopicPartition): Unit = {
    if (!isActive) return

    if (controllerContext.partitionsBeingReassigned.contains(topicPartition)) {
      maybeCompleteReassignment(topicPartition)
    }
  }

  private def maybeCompleteReassignment(topicPartition: TopicPartition): Unit = {
    val reassignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
    if (isReassignmentComplete(topicPartition, reassignment)) {
      // resume the partition reassignment process
      info(s"Target replicas ${reassignment.targetReplicas} have all caught up with the leader for " +
        s"reassigning partition $topicPartition")
      onPartitionReassignment(topicPartition, reassignment)
    }
  }

  private def processListPartitionReassignments(partitionsOpt: Option[Set[TopicPartition]], callback: ListReassignmentsCallback): Unit = {
    if (!isActive) {
      callback(Right(new ApiError(Errors.NOT_CONTROLLER)))
    } else {
      val results: mutable.Map[TopicPartition, ReplicaAssignment] = mutable.Map.empty
      val partitionsToList = partitionsOpt match {
        case Some(partitions) => partitions
        case None => controllerContext.partitionsBeingReassigned
      }

      partitionsToList.foreach { tp =>
        val assignment = controllerContext.partitionFullReplicaAssignment(tp)
        if (assignment.isBeingReassigned) {
          results += tp -> assignment
        }
      }

      callback(Left(results))
    }
  }

  /**
   * Returns the new FinalizedVersionRange for the feature, if there are no feature
   * incompatibilities seen with all known brokers for the provided feature update.
   * Otherwise returns an ApiError object containing Errors.INVALID_REQUEST.
   *
   * @param update the feature update to be processed (this can not be meant to delete the feature)
   * @return the new FinalizedVersionRange or error, as described above.
   */
  private def newFinalizedVersionRangeOrIncompatibilityError(update: UpdateFeaturesRequestData.FeatureUpdateKey): Either[FinalizedVersionRange, ApiError] = {
    if (UpdateFeaturesRequest.isDeleteRequest(update)) {
      throw new IllegalArgumentException(s"Provided feature update can not be meant to delete the feature: $update")
    }

    val supportedVersionRange = brokerFeatures.supportedFeatures.get(update.feature)
    if (supportedVersionRange == null) {
      Right(new ApiError(Errors.INVALID_REQUEST,
        "Could not apply finalized feature update because the provided feature" +
          " is not supported."))
    } else {
      var newVersionRange: FinalizedVersionRange = null
      try {
        newVersionRange = new FinalizedVersionRange(supportedVersionRange.min, update.maxVersionLevel)
      } catch {
        case _: IllegalArgumentException => {
          // This exception means the provided maxVersionLevel is invalid. It is handled below
          // outside of this catch clause.
        }
      }
      if (newVersionRange == null) {
        Right(new ApiError(Errors.INVALID_REQUEST,
          "Could not apply finalized feature update because the provided" +
            s" maxVersionLevel:${update.maxVersionLevel} is lower than the" +
            s" supported minVersion:${supportedVersionRange.min}."))
      } else {
        val newFinalizedFeature =
          Features.finalizedFeatures(Utils.mkMap(Utils.mkEntry(update.feature, newVersionRange)))
        val numIncompatibleBrokers = controllerContext.liveOrShuttingDownBrokers.count(broker => {
          BrokerFeatures.hasIncompatibleFeatures(broker.features, newFinalizedFeature)
        })
        if (numIncompatibleBrokers == 0) {
          Left(newVersionRange)
        } else {
          Right(new ApiError(Errors.INVALID_REQUEST,
            "Could not apply finalized feature update because" +
              " brokers were found to have incompatible versions for the feature."))
        }
      }
    }
  }

  /**
   * Validates a feature update on an existing FinalizedVersionRange.
   * If the validation succeeds, then, the return value contains:
   * 1. the new FinalizedVersionRange for the feature, if the feature update was not meant to delete the feature.
   * 2. Option.empty, if the feature update was meant to delete the feature.
   *
   * If the validation fails, then returned value contains a suitable ApiError.
   *
   * @param update               the feature update to be processed.
   * @param existingVersionRange the existing FinalizedVersionRange which can be empty when no
   *                             FinalizedVersionRange exists for the associated feature
   * @return the new FinalizedVersionRange to be updated into ZK or error
   *         as described above.
   */
  private def validateFeatureUpdate(update: UpdateFeaturesRequestData.FeatureUpdateKey,
                                    existingVersionRange: Option[FinalizedVersionRange]): Either[Option[FinalizedVersionRange], ApiError] = {
    def newVersionRangeOrError(update: UpdateFeaturesRequestData.FeatureUpdateKey): Either[Option[FinalizedVersionRange], ApiError] = {
      newFinalizedVersionRangeOrIncompatibilityError(update)
        .fold(versionRange => Left(Some(versionRange)), error => Right(error))
    }

    if (update.feature.isEmpty) {
      // Check that the feature name is not empty.
      Right(new ApiError(Errors.INVALID_REQUEST, "Feature name can not be empty."))
    } else {
      // We handle deletion requests separately from non-deletion requests.
      if (UpdateFeaturesRequest.isDeleteRequest(update)) {
        if (existingVersionRange.isEmpty) {
          // Disallow deletion of a non-existing finalized feature.
          Right(new ApiError(Errors.INVALID_REQUEST,
            "Can not delete non-existing finalized feature."))
        } else {
          Left(Option.empty)
        }
      } else if (update.maxVersionLevel() < 1) {
        // Disallow deletion of a finalized feature without allowDowngrade flag set.
        Right(new ApiError(Errors.INVALID_REQUEST,
          s"Can not provide maxVersionLevel: ${update.maxVersionLevel} less" +
            s" than 1 without setting the allowDowngrade flag to true in the request."))
      } else {
        existingVersionRange.map(existing =>
          if (update.maxVersionLevel == existing.max) {
            // Disallow a case where target maxVersionLevel matches existing maxVersionLevel.
            Right(new ApiError(Errors.INVALID_REQUEST,
              s"Can not ${if (update.allowDowngrade) "downgrade" else "upgrade"}" +
                s" a finalized feature from existing maxVersionLevel:${existing.max}" +
                " to the same value."))
          } else if (update.maxVersionLevel < existing.max && !update.allowDowngrade) {
            // Disallow downgrade of a finalized feature without the allowDowngrade flag set.
            Right(new ApiError(Errors.INVALID_REQUEST,
              s"Can not downgrade finalized feature from existing" +
                s" maxVersionLevel:${existing.max} to provided" +
                s" maxVersionLevel:${update.maxVersionLevel} without setting the" +
                " allowDowngrade flag in the request."))
          } else if (update.allowDowngrade && update.maxVersionLevel > existing.max) {
            // Disallow a request that sets allowDowngrade flag without specifying a
            // maxVersionLevel that's lower than the existing maxVersionLevel.
            Right(new ApiError(Errors.INVALID_REQUEST,
              s"When the allowDowngrade flag set in the request, the provided" +
                s" maxVersionLevel:${update.maxVersionLevel} can not be greater than" +
                s" existing maxVersionLevel:${existing.max}."))
          } else if (update.maxVersionLevel < existing.min) {
            // Disallow downgrade of a finalized feature below the existing finalized
            // minVersionLevel.
            Right(new ApiError(Errors.INVALID_REQUEST,
              s"Can not downgrade finalized feature to maxVersionLevel:${update.maxVersionLevel}" +
                s" because it's lower than the existing minVersionLevel:${existing.min}."))
          } else {
            newVersionRangeOrError(update)
          }
        ).getOrElse(newVersionRangeOrError(update))
      }
    }
  }

  private def processFeatureUpdates(request: UpdateFeaturesRequest,
                                    callback: UpdateFeaturesCallback): Unit = {
    if (isActive) {
      processFeatureUpdatesWithActiveController(request, callback)
    } else {
      callback(Left(new ApiError(Errors.NOT_CONTROLLER)))
    }
  }

  private def processFeatureUpdatesWithActiveController(request: UpdateFeaturesRequest,
                                                        callback: UpdateFeaturesCallback): Unit = {
    val updates = request.data.featureUpdates
    val existingFeatures = featureCache.get
      .map(featuresAndEpoch => featuresAndEpoch.features.features().asScala)
      .getOrElse(Map[String, FinalizedVersionRange]())
    // A map with key being feature name and value being FinalizedVersionRange.
    // This contains the target features to be eventually written to FeatureZNode.
    val targetFeatures = scala.collection.mutable.Map[String, FinalizedVersionRange]() ++ existingFeatures
    // A map with key being feature name and value being error encountered when the FeatureUpdate
    // was applied.
    val errors = scala.collection.mutable.Map[String, ApiError]()

    // Below we process each FeatureUpdate using the following logic:
    //  - If a FeatureUpdate is found to be valid, then:
    //    - The corresponding entry in errors map would be updated to contain Errors.NONE.
    //    - If the FeatureUpdate is an add or update request, then the targetFeatures map is updated
    //      to contain the new FinalizedVersionRange for the feature.
    //    - Otherwise if the FeatureUpdate is a delete request, then the feature is removed from the
    //      targetFeatures map.
    //  - Otherwise if a FeatureUpdate is found to be invalid, then:
    //    - The corresponding entry in errors map would be updated with the appropriate ApiError.
    //    - The entry in targetFeatures map is left untouched.
    updates.asScala.iterator.foreach { update =>
      validateFeatureUpdate(update, existingFeatures.get(update.feature())) match {
        case Left(newVersionRangeOrNone) =>
          newVersionRangeOrNone match {
            case Some(newVersionRange) => targetFeatures += (update.feature() -> newVersionRange)
            case None => targetFeatures -= update.feature()
          }
          errors += (update.feature() -> new ApiError(Errors.NONE))
        case Right(featureUpdateFailureReason) =>
          errors += (update.feature() -> featureUpdateFailureReason)
      }
    }

    // If the existing and target features are the same, then, we skip the update to the
    // FeatureZNode as no changes to the node are required. Otherwise, we replace the contents
    // of the FeatureZNode with the new features. This may result in partial or full modification
    // of the existing finalized features in ZK.
    try {
      if (!existingFeatures.equals(targetFeatures)) {
        val newNode = new FeatureZNode(FeatureZNodeStatus.Enabled, Features.finalizedFeatures(targetFeatures.asJava))
        val newVersion = updateFeatureZNode(newNode)
        featureCache.waitUntilEpochOrThrow(newVersion, request.data().timeoutMs())
      }
    } catch {
      // For all features that correspond to valid FeatureUpdate (i.e. error is Errors.NONE),
      // we set the error as Errors.FEATURE_UPDATE_FAILED since the FeatureZNode update has failed
      // for these. For the rest, the existing error is left untouched.
      case e: Exception =>
        warn(s"Processing of feature updates: $request failed due to error: $e")
        errors.foreach { case (feature, apiError) =>
          if (apiError.error() == Errors.NONE) {
            errors(feature) = new ApiError(Errors.FEATURE_UPDATE_FAILED)
          }
        }
    } finally {
      callback(Right(errors))
    }
  }

  private def processIsrChangeNotification(): Unit = {
    def processUpdateNotifications(partitions: Seq[TopicPartition]): Unit = {
      val liveBrokers: Seq[Int] = controllerContext.liveOrShuttingDownBrokerIds.toSeq
      debug(s"Sending MetadataRequest to Brokers: $liveBrokers for TopicPartitions: $partitions")
      sendUpdateMetadataRequest(liveBrokers, partitions.toSet)
    }

    if (!isActive) return
    val sequenceNumbers = zkClient.getAllIsrChangeNotifications
    try {
      val partitions = zkClient.getPartitionsFromIsrChangeNotifications(sequenceNumbers)
      if (partitions.nonEmpty) {
        updateLeaderAndIsrCache(partitions)
        processUpdateNotifications(partitions)

        // During a partial upgrade, the controller may be on an IBP which assumes
        // ISR changes through the `AlterIsr` API while some brokers are on an older
        // IBP which assumes notification through Zookeeper. In this case, since the
        // controller will not have registered watches for reassigning partitions, we
        // can still rely on the batch ISR change notification path in order to
        // complete the reassignment.
        partitions.filter(controllerContext.partitionsBeingReassigned.contains).foreach { topicPartition =>
          maybeCompleteReassignment(topicPartition)
        }
      }
    } finally {
      // delete the notifications
      zkClient.deleteIsrChangeNotifications(sequenceNumbers, controllerContext.epochZkVersion)
    }
  }

  def electLeaders(
                    partitions: Set[TopicPartition],
                    electionType: ElectionType,
                    callback: ElectLeadersCallback
                  ): Unit = {
    eventManager.put(ReplicaLeaderElection(Some(partitions), electionType, AdminClientTriggered, callback))
  }

  def listPartitionReassignments(partitions: Option[Set[TopicPartition]],
                                 callback: ListReassignmentsCallback): Unit = {
    eventManager.put(ListPartitionReassignments(partitions, callback))
  }

  def updateFeatures(request: UpdateFeaturesRequest,
                     callback: UpdateFeaturesCallback): Unit = {
    eventManager.put(UpdateFeatures(request, callback))
  }

  def alterPartitionReassignments(partitions: Map[TopicPartition, Option[Seq[Int]]],
                                  callback: AlterReassignmentsCallback): Unit = {
    eventManager.put(ApiPartitionReassignment(partitions, callback))
  }

  // 处理Leader副本选举
  private def processReplicaLeaderElection(
                                            partitionsFromAdminClientOpt: Option[Set[TopicPartition]],
                                            electionType: ElectionType,
                                            electionTrigger: ElectionTrigger,
                                            callback: ElectLeadersCallback
                                          ): Unit = {
    if (!isActive) {
      callback(partitionsFromAdminClientOpt.fold(Map.empty[TopicPartition, Either[ApiError, Int]]) { partitions =>
        partitions.iterator.map(partition => partition -> Left(new ApiError(Errors.NOT_CONTROLLER, null))).toMap
      })
    } else {
      // We need to register the watcher if the path doesn't exist in order to detect future preferred replica
      // leader elections and we get the `path exists` check for free
      if (electionTrigger == AdminClientTriggered || zkClient.registerZNodeChangeHandlerAndCheckExistence(preferredReplicaElectionHandler)) {
        val partitions = partitionsFromAdminClientOpt match {
          case Some(partitions) => partitions
          case None => zkClient.getPreferredReplicaElection
        }

        val allPartitions = controllerContext.allPartitions
        val (knownPartitions, unknownPartitions) = partitions.partition(tp => allPartitions.contains(tp))
        unknownPartitions.foreach { p =>
          info(s"Skipping replica leader election ($electionType) for partition $p by $electionTrigger since it doesn't exist.")
        }

        val (partitionsBeingDeleted, livePartitions) = knownPartitions.partition(partition =>
          topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
        if (partitionsBeingDeleted.nonEmpty) {
          warn(s"Skipping replica leader election ($electionType) for partitions $partitionsBeingDeleted " +
            s"by $electionTrigger since the respective topics are being deleted")
        }

        // partition those that have a valid leader
        val (electablePartitions, alreadyValidLeader) = livePartitions.partition { partition =>
          electionType match {
            case ElectionType.PREFERRED =>
              val assignedReplicas = controllerContext.partitionReplicaAssignment(partition)
              val preferredReplica = assignedReplicas.head
              val currentLeader = controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader
              currentLeader != preferredReplica

            case ElectionType.UNCLEAN =>
              val currentLeader = controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader
              currentLeader == LeaderAndIsr.NoLeader || !controllerContext.liveBrokerIds.contains(currentLeader)
          }
        }

        val results = onReplicaElection(electablePartitions, electionType, electionTrigger).map {
          case (k, Left(ex)) =>
            if (ex.isInstanceOf[StateChangeFailedException]) {
              val error = if (electionType == ElectionType.PREFERRED) {
                Errors.PREFERRED_LEADER_NOT_AVAILABLE
              } else {
                Errors.ELIGIBLE_LEADERS_NOT_AVAILABLE
              }
              k -> Left(new ApiError(error, ex.getMessage))
            } else {
              k -> Left(ApiError.fromThrowable(ex))
            }
          case (k, Right(leaderAndIsr)) => k -> Right(leaderAndIsr.leader)
        } ++
          alreadyValidLeader.map(_ -> Left(new ApiError(Errors.ELECTION_NOT_NEEDED))) ++
          partitionsBeingDeleted.map(
            _ -> Left(new ApiError(Errors.INVALID_TOPIC_EXCEPTION, "The topic is being deleted"))
          ) ++
          unknownPartitions.map(
            _ -> Left(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist."))
          )

        debug(s"Waiting for any successful result for election type ($electionType) by $electionTrigger for partitions: $results")
        callback(results)
      }
    }
  }

  def alterIsrs(alterIsrRequest: AlterIsrRequestData, callback: AlterIsrResponseData => Unit): Unit = {
    val isrsToAlter = mutable.Map[TopicPartition, LeaderAndIsr]()

    alterIsrRequest.topics.forEach { topicReq =>
      topicReq.partitions.forEach { partitionReq =>
        val tp = new TopicPartition(topicReq.name, partitionReq.partitionIndex)
        val newIsr = partitionReq.newIsr().asScala.toList.map(_.toInt)
        isrsToAlter.put(tp, new LeaderAndIsr(alterIsrRequest.brokerId, partitionReq.leaderEpoch, newIsr, partitionReq.currentIsrVersion))
      }
    }

    def responseCallback(results: Either[Map[TopicPartition, Either[Errors, LeaderAndIsr]], Errors]): Unit = {
      val resp = new AlterIsrResponseData()
      results match {
        case Right(error) =>
          resp.setErrorCode(error.code)
        case Left(partitionResults) =>
          resp.setTopics(new util.ArrayList())
          partitionResults
            .groupBy { case (tp, _) => tp.topic } // Group by topic
            .foreach { case (topic, partitions) =>
              // Add each topic part to the response
              val topicResp = new AlterIsrResponseData.TopicData()
                .setName(topic)
                .setPartitions(new util.ArrayList())
              resp.topics.add(topicResp)
              partitions.foreach { case (tp, errorOrIsr) =>
                // Add each partition part to the response (new ISR or error)
                errorOrIsr match {
                  case Left(error) => topicResp.partitions.add(
                    new AlterIsrResponseData.PartitionData()
                      .setPartitionIndex(tp.partition)
                      .setErrorCode(error.code))
                  case Right(leaderAndIsr) => topicResp.partitions.add(
                    new AlterIsrResponseData.PartitionData()
                      .setPartitionIndex(tp.partition)
                      .setLeaderId(leaderAndIsr.leader)
                      .setLeaderEpoch(leaderAndIsr.leaderEpoch)
                      .setIsr(leaderAndIsr.isr.map(Integer.valueOf).asJava)
                      .setCurrentIsrVersion(leaderAndIsr.zkVersion))
                }
              }
            }
      }
      callback.apply(resp)
    }

    eventManager.put(AlterIsrReceived(alterIsrRequest.brokerId, alterIsrRequest.brokerEpoch, isrsToAlter, responseCallback))
  }

  private def processAlterIsr(brokerId: Int, brokerEpoch: Long,
                              isrsToAlter: Map[TopicPartition, LeaderAndIsr],
                              callback: AlterIsrCallback): Unit = {

    // Handle a few short-circuits
    if (!isActive) {
      callback.apply(Right(Errors.NOT_CONTROLLER))
      return
    }

    val brokerEpochOpt = controllerContext.liveBrokerIdAndEpochs.get(brokerId)
    if (brokerEpochOpt.isEmpty) {
      info(s"Ignoring AlterIsr due to unknown broker $brokerId")
      callback.apply(Right(Errors.STALE_BROKER_EPOCH))
      return
    }

    if (!brokerEpochOpt.contains(brokerEpoch)) {
      info(s"Ignoring AlterIsr due to stale broker epoch $brokerEpoch for broker $brokerId")
      callback.apply(Right(Errors.STALE_BROKER_EPOCH))
      return
    }

    val response = try {
      val partitionResponses = mutable.HashMap[TopicPartition, Either[Errors, LeaderAndIsr]]()

      // Determine which partitions we will accept the new ISR for
      val adjustedIsrs: Map[TopicPartition, LeaderAndIsr] = isrsToAlter.flatMap {
        case (tp: TopicPartition, newLeaderAndIsr: LeaderAndIsr) =>
          controllerContext.partitionLeadershipInfo(tp) match {
            case Some(leaderIsrAndControllerEpoch) =>
              val currentLeaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
              if (newLeaderAndIsr.leaderEpoch < currentLeaderAndIsr.leaderEpoch) {
                partitionResponses(tp) = Left(Errors.FENCED_LEADER_EPOCH)
                None
              } else if (newLeaderAndIsr.equalsIgnoreZk(currentLeaderAndIsr)) {
                // If a partition is already in the desired state, just return it
                partitionResponses(tp) = Right(currentLeaderAndIsr)
                None
              } else {
                Some(tp -> newLeaderAndIsr)
              }
            case None =>
              partitionResponses(tp) = Left(Errors.UNKNOWN_TOPIC_OR_PARTITION)
              None
          }
      }

      // Do the updates in ZK
      debug(s"Updating ISRs for partitions: ${adjustedIsrs.keySet}.")
      val UpdateLeaderAndIsrResult(finishedUpdates, badVersionUpdates) = zkClient.updateLeaderAndIsr(
        adjustedIsrs, controllerContext.epoch, controllerContext.epochZkVersion)

      val successfulUpdates: Map[TopicPartition, LeaderAndIsr] = finishedUpdates.flatMap {
        case (partition: TopicPartition, isrOrError: Either[Throwable, LeaderAndIsr]) =>
          isrOrError match {
            case Right(updatedIsr) =>
              debug(s"ISR for partition $partition updated to [${updatedIsr.isr.mkString(",")}] and zkVersion updated to [${updatedIsr.zkVersion}]")
              partitionResponses(partition) = Right(updatedIsr)
              Some(partition -> updatedIsr)
            case Left(error) =>
              warn(s"Failed to update ISR for partition $partition", error)
              partitionResponses(partition) = Left(Errors.forException(error))
              None
          }
      }

      badVersionUpdates.foreach(partition => {
        debug(s"Failed to update ISR for partition $partition, bad ZK version")
        partitionResponses(partition) = Left(Errors.INVALID_UPDATE_VERSION)
      })

      def processUpdateNotifications(partitions: Seq[TopicPartition]): Unit = {
        val liveBrokers: Seq[Int] = controllerContext.liveOrShuttingDownBrokerIds.toSeq
        sendUpdateMetadataRequest(liveBrokers, partitions.toSet)
      }

      // Update our cache and send out metadata updates
      updateLeaderAndIsrCache(successfulUpdates.keys.toSeq)
      processUpdateNotifications(isrsToAlter.keys.toSeq)

      Left(partitionResponses)
    } catch {
      case e: Throwable =>
        error(s"Error when processing AlterIsr for partitions: ${isrsToAlter.keys.toSeq}", e)
        Right(Errors.UNKNOWN_SERVER_ERROR)
    }

    callback.apply(response)

    // After we have returned the result of the `AlterIsr` request, we should check whether
    // there are any reassignments which can be completed by a successful ISR expansion.
    response.left.foreach { alterIsrResponses =>
      alterIsrResponses.forKeyValue { (topicPartition, partitionResponse) =>
        if (controllerContext.partitionsBeingReassigned.contains(topicPartition)) {
          val isSuccessfulUpdate = partitionResponse.isRight
          if (isSuccessfulUpdate) {
            maybeCompleteReassignment(topicPartition)
          }
        }
      }
    }
  }

  private def processControllerChange(): Unit = {
    maybeResign()
  }

  private def processReelect(): Unit = {
    maybeResign()
    elect()
  }

  private def processRegisterBrokerAndReelect(): Unit = {
    // #1 创建「/brokers/ids/[broker id]」临时节点，并返回broker epoch值
    _brokerEpoch = zkClient.registerBroker(brokerInfo)

    // #2 ①controller退位;②重新选举
    processReelect()
  }

  /**
   * 处理controller和zookeeper会话过期事件：controller退位
   */
  private def processExpire(): Unit = {
    // #1 将缓存的controller id 置为-1，意味着当前broker没有确认controller
    activeControllerId = -1

    // #2 controlelr退位清理操作
    onControllerResignation()
  }


  /**
   * Controller事件核心处理方法，根据不同的Controller事件类型路由到对应的方法并处理
   *
   * @param event Controller事件
   */
  override def process(event: ControllerEvent): Unit = {
    try {
      event match {
        case event: MockEvent =>
          // Used only in test cases
          event.process()
        case ShutdownEventThread =>
          error("Received a ShutdownEventThread event. This type of event is supposed to be handle by ControllerEventThread")
        case AutoPreferredReplicaLeaderElection =>
          processAutoPreferredReplicaLeaderElection()
        case ReplicaLeaderElection(partitions, electionType, electionTrigger, callback) =>
          processReplicaLeaderElection(partitions, electionType, electionTrigger, callback)
        case UncleanLeaderElectionEnable =>
          processUncleanLeaderElectionEnable()
        case TopicUncleanLeaderElectionEnable(topic) =>
          processTopicUncleanLeaderElectionEnable(topic)
        case ControlledShutdown(id, brokerEpoch, callback) =>
          processControlledShutdown(id, brokerEpoch, callback)
        case LeaderAndIsrResponseReceived(response, brokerId) =>
          processLeaderAndIsrResponseReceived(response, brokerId)
        case UpdateMetadataResponseReceived(response, brokerId) =>
          processUpdateMetadataResponseReceived(response, brokerId)
        case TopicDeletionStopReplicaResponseReceived(replicaId, requestError, partitionErrors) =>
          processTopicDeletionStopReplicaResponseReceived(replicaId, requestError, partitionErrors)
        case BrokerChange =>
          // 处理brokers数量变更
          processBrokerChange()
        case BrokerModifications(brokerId) =>
          processBrokerModification(brokerId)
        case ControllerChange =>
          // 节点「/Controller」数据发生变更
          processControllerChange()
        case Reelect =>
          processReelect()
        case RegisterBrokerAndReelect =>
          // controller session会话过期，controller退位，整个集群重新竞选controller
          processRegisterBrokerAndReelect()
        case Expire =>
          // 处理controller会话过期事件
          processExpire()
        case TopicChange =>
          // 处理主题创建/变更事件
          processTopicChange()
        case LogDirEventNotification =>
          // 日志目录变更事件
          processLogDirEventNotification()
        case PartitionModifications(topic) =>
          processPartitionModifications(topic)
        case TopicDeletion =>
          // 主题删除处理器
          processTopicDeletion()
        case ApiPartitionReassignment(reassignments, callback) =>
          processApiPartitionReassignment(reassignments, callback)
        case ZkPartitionReassignment =>
          processZkPartitionReassignment()
        case ListPartitionReassignments(partitions, callback) =>
          processListPartitionReassignments(partitions, callback)
        case UpdateFeatures(request, callback) =>
          processFeatureUpdates(request, callback)
        case PartitionReassignmentIsrChange(partition) =>
          processPartitionReassignmentIsrChange(partition)
        case IsrChangeNotification =>
          processIsrChangeNotification()
        case AlterIsrReceived(brokerId, brokerEpoch, isrsToAlter, callback) =>
          processAlterIsr(brokerId, brokerEpoch, isrsToAlter, callback)
        case Startup =>
          // 当Broker启动时，生成Startup事件并交给ControllerEventManager处理
          processStartup()
      }
    } catch {
      case e: ControllerMovedException =>
        info(s"Controller moved to another broker when processing $event.", e)
        maybeResign()
      case e: Throwable =>
        error(s"Error processing event $event", e)
    } finally {
      updateMetrics()
    }
  }

  override def preempt(event: ControllerEvent): Unit = {
    event.preempt()
  }
}

/**
 * 关注ZK节点「/brokers/ids」子节点的变更情况
 *
 * @param eventManager
 */
class BrokerChangeHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = BrokerIdsZNode.path

  /**
   * 如果子节点发生变化，将「BrokerChange」事件放入事件管理器中被处理
   */
  override def handleChildChange(): Unit = {
    eventManager.put(BrokerChange)
  }
}

/**
 * 关注「/brokers/ids/[broker id]」节点的数据变化
 *
 * @param eventManager
 * @param brokerId
 */
class BrokerModificationsHandler(eventManager: ControllerEventManager, brokerId: Int) extends ZNodeChangeHandler {
  override val path: String = BrokerIdZNode.path(brokerId)

  /**
   * 如果数据发生变化，将「BrokerModifications」事件放入事件管理器中被处理
   */
  override def handleDataChange(): Unit = {
    eventManager.put(BrokerModifications(brokerId))
  }
}

/**
 * 关注「/brokers/topics」子节点，
 *
 * @param eventManager
 */
class TopicChangeHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = TopicsZNode.path

  /**
   * 处理子节点数据变更情况
   */
  override def handleChildChange(): Unit = eventManager.put(TopicChange)
}

/**
 *
 * @param eventManager
 */
class LogDirEventNotificationHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  // log_dir_event_notification
  override val path: String = LogDirEventNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(LogDirEventNotification)
}

object LogDirEventNotificationHandler {
  val Version: Long = 1L
}

class PartitionModificationsHandler(eventManager: ControllerEventManager, topic: String) extends ZNodeChangeHandler {
  override val path: String = TopicZNode.path(topic)

  override def handleDataChange(): Unit = eventManager.put(PartitionModifications(topic))
}

/**
 * 主题删除处理器：添加 {@link TopicDeletion}事件
 *
 * @param eventManager
 */
class TopicDeletionHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = DeleteTopicsZNode.path

  override def handleChildChange(): Unit = eventManager.put(TopicDeletion)
}

class PartitionReassignmentHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = ReassignPartitionsZNode.path

  // Note that the event is also enqueued when the znode is deleted, but we do it explicitly instead of relying on
  // handleDeletion(). This approach is more robust as it doesn't depend on the watcher being re-registered after
  // it's consumed during data changes (we ensure re-registration when the znode is deleted).
  override def handleCreation(): Unit = eventManager.put(ZkPartitionReassignment)
}

class PartitionReassignmentIsrChangeHandler(eventManager: ControllerEventManager, partition: TopicPartition) extends ZNodeChangeHandler {
  override val path: String = TopicPartitionStateZNode.path(partition)

  override def handleDataChange(): Unit = eventManager.put(PartitionReassignmentIsrChange(partition))
}

class IsrChangeNotificationHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = IsrChangeNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(IsrChangeNotification)
}

object IsrChangeNotificationHandler {
  val Version: Long = 1L
}

class PreferredReplicaElectionHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = PreferredReplicaElectionZNode.path

  override def handleCreation(): Unit = eventManager.put(ReplicaLeaderElection(None, ElectionType.PREFERRED, ZkTriggered))
}

/**
 * 这个处理器监听Zookeeper节点「/controller」的变化：创建、删除、修改都会被监听到，
 * 并配合具体的变化向 {@link ControllerEventManager} 事件管理器中放入对应的Controller事件。
 * 事件驱动模型
 *
 * @param eventManager
 */
class ControllerChangeHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  /**
   * Controller所关闭的Zookeeper节点：/controller
   */
  override val path: String = ControllerZNode.path

  /**
   * 当节点创建时，将 {@link ControllerChange} 事件提交给
   * 事件管理器 {@link ControllerEventManager} 处理
   */
  override def handleCreation(): Unit = eventManager.put(ControllerChange)

  /**
   * 当节点被删除时，将 {@link Reelect} 事件提交给事件管理器处理。
   * 节点被删除意味着接下来需要进行Controller选举
   */
  override def handleDeletion(): Unit = eventManager.put(Reelect)

  /**
   * 当节点被修改时，将 {@link ControllerChange} 事件交给事件管理器处理
   */
  override def handleDataChange(): Unit = eventManager.put(ControllerChange)
}

/**
 * 表示一个分区副本对象，它应该由①主题+分区（TopicPartition）②副本ID（replica）组成，
 * 这样就能确保某个分区下的副本了
 *
 * @param topicPartition 分区详情（主题名称+分区号）
 * @param replica        副本ID
 */
case class PartitionAndReplica(topicPartition: TopicPartition, replica: Int) {
  def topic: String = topicPartition.topic

  def partition: Int = topicPartition.partition

  override def toString: String = {
    s"[Topic=$topic,Partition=$partition,Replica=$replica]"
  }
}

case class LeaderIsrAndControllerEpoch(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString: String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

/**
 * 定义了两大类统计指标：UncleanLeaderElectionsPerSec和所有Controller 事件状态的执行速率与时间。
 */
private[controller] class ControllerStats extends KafkaMetricsGroup {
  val uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)

  val rateAndTimeMetrics: Map[ControllerState, KafkaTimer] = ControllerState.values.flatMap { state =>
    state.rateAndTimeMetricName.map { metricName =>
      state -> new KafkaTimer(newTimer(metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
    }
  }.toMap

}

/**
 * Controller事件顶层接口
 * 说明：事件（ControllerEvent）和状态（ControllerState）并非是一一对应的。
 * 比如{@link ControllerChange}状态就对应的事件有：
 * {@link ControllerChange}、{@link Reelect}、{@link RegisterBrokerAndReelect}、{@link Expire}、{@link Startup}
 *
 * 当发生了Controller事件后，需要变更Controller的状态为对应的值（事件驱动模型）
 */
sealed trait ControllerEvent {
  /**
   * 每个Controller事件都定义了一个状态序号，从0开始
   * 当Controller处理具体事件时，会对该值做变更（状态流转）
   */
  def state: ControllerState

  // preempt() is not executed by `ControllerEventThread` but by the main thread.
  def preempt(): Unit
}

case object ControllerChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange

  override def preempt(): Unit = {}
}

case object Reelect extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange

  override def preempt(): Unit = {}
}

case object RegisterBrokerAndReelect extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange

  override def preempt(): Unit = {}
}

/**
 * Zookeeper会话过期事件
 */
case object Expire extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange

  override def preempt(): Unit = {}
}

case object ShutdownEventThread extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerShutdown

  override def preempt(): Unit = {}
}

case object AutoPreferredReplicaLeaderElection extends ControllerEvent {
  override def state: ControllerState = ControllerState.AutoLeaderBalance

  override def preempt(): Unit = {}
}

case object UncleanLeaderElectionEnable extends ControllerEvent {
  override def state: ControllerState = ControllerState.UncleanLeaderElectionEnable

  override def preempt(): Unit = {}
}

case class TopicUncleanLeaderElectionEnable(topic: String) extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicUncleanLeaderElectionEnable

  override def preempt(): Unit = {}
}

case class ControlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit) extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControlledShutdown

  override def preempt(): Unit = controlledShutdownCallback(Failure(new ControllerMovedException("Controller moved to another broker")))
}

case class LeaderAndIsrResponseReceived(leaderAndIsrResponse: LeaderAndIsrResponse, brokerId: Int) extends ControllerEvent {
  override def state: ControllerState = ControllerState.LeaderAndIsrResponseReceived

  override def preempt(): Unit = {}
}

case class UpdateMetadataResponseReceived(updateMetadataResponse: UpdateMetadataResponse, brokerId: Int) extends ControllerEvent {
  override def state: ControllerState = ControllerState.UpdateMetadataResponseReceived

  override def preempt(): Unit = {}
}

case class TopicDeletionStopReplicaResponseReceived(replicaId: Int,
                                                    requestError: Errors,
                                                    partitionErrors: Map[TopicPartition, Errors]) extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicDeletion

  override def preempt(): Unit = {}
}

case object Startup extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange

  override def preempt(): Unit = {}
}

case object BrokerChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.BrokerChange

  override def preempt(): Unit = {}
}

case class BrokerModifications(brokerId: Int) extends ControllerEvent {
  override def state: ControllerState = ControllerState.BrokerChange

  override def preempt(): Unit = {}
}

case object TopicChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicChange

  override def preempt(): Unit = {}
}

case object LogDirEventNotification extends ControllerEvent {
  override def state: ControllerState = ControllerState.LogDirChange

  override def preempt(): Unit = {}
}

case class PartitionModifications(topic: String) extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicChange

  override def preempt(): Unit = {}
}

case object TopicDeletion extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicDeletion

  override def preempt(): Unit = {}
}

case object ZkPartitionReassignment extends ControllerEvent {
  override def state: ControllerState = ControllerState.AlterPartitionReassignment

  override def preempt(): Unit = {}
}

case class ApiPartitionReassignment(reassignments: Map[TopicPartition, Option[Seq[Int]]],
                                    callback: AlterReassignmentsCallback) extends ControllerEvent {
  override def state: ControllerState = ControllerState.AlterPartitionReassignment

  override def preempt(): Unit = callback(Right(new ApiError(Errors.NOT_CONTROLLER)))
}

case class PartitionReassignmentIsrChange(partition: TopicPartition) extends ControllerEvent {
  override def state: ControllerState = ControllerState.AlterPartitionReassignment

  override def preempt(): Unit = {}
}

case object IsrChangeNotification extends ControllerEvent {
  override def state: ControllerState = ControllerState.IsrChange

  override def preempt(): Unit = {}
}

case class AlterIsrReceived(brokerId: Int, brokerEpoch: Long, isrsToAlter: Map[TopicPartition, LeaderAndIsr],
                            callback: AlterIsrCallback) extends ControllerEvent {
  override def state: ControllerState = ControllerState.IsrChange

  override def preempt(): Unit = {}
}

case class ReplicaLeaderElection(
                                  partitionsFromAdminClientOpt: Option[Set[TopicPartition]],
                                  electionType: ElectionType,
                                  electionTrigger: ElectionTrigger,
                                  callback: ElectLeadersCallback = _ => {}
                                ) extends ControllerEvent {
  override def state: ControllerState = ControllerState.ManualLeaderBalance

  override def preempt(): Unit = callback(
    partitionsFromAdminClientOpt.fold(Map.empty[TopicPartition, Either[ApiError, Int]]) { partitions =>
      partitions.iterator.map(partition => partition -> Left(new ApiError(Errors.NOT_CONTROLLER, null))).toMap
    }
  )
}

/**
 * @param partitionsOpt - an Optional set of partitions. If not present, all reassigning partitions are to be listed
 */
case class ListPartitionReassignments(partitionsOpt: Option[Set[TopicPartition]],
                                      callback: ListReassignmentsCallback) extends ControllerEvent {
  override def state: ControllerState = ControllerState.ListPartitionReassignment

  override def preempt(): Unit = callback(Right(new ApiError(Errors.NOT_CONTROLLER, null)))
}

case class UpdateFeatures(request: UpdateFeaturesRequest,
                          callback: UpdateFeaturesCallback) extends ControllerEvent {
  override def state: ControllerState = ControllerState.UpdateFeatures

  override def preempt(): Unit = {}
}


// Used only in test cases
abstract class MockEvent(val state: ControllerState) extends ControllerEvent {
  def process(): Unit

  def preempt(): Unit
}
