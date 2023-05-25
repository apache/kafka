/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package kafka.controller

import com.yammer.metrics.core.{Gauge, Timer}
import kafka.api._
import kafka.cluster.Broker
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils._
import org.apache.kafka.clients._
import org.apache.kafka.common._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.ShutdownableThread

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.collection.{Seq, Set, mutable}
import scala.jdk.CollectionConverters._

object ControllerChannelManager {
  val QueueSizeMetricName = "QueueSize"
  val RequestRateAndQueueTimeMetricName = "RequestRateAndQueueTimeMs"
}

/**
 * 这个类和 RequestSendThread 是合作共赢的关系，它有两大类任务：
 * 1、管理 Controller 与集群 Broker 之间的连接，并为每个 Broker 创建 RequestSendThread 线程实例；
 * 2、将要发送的请求放入到指定 Broker 的阻塞队列中，等待该 Broker 专属的 RequestSendThread 线程进行处理。
 * @param controllerEpoch
 * @param config
 * @param time
 * @param metrics
 * @param stateChangeLogger
 * @param threadNamePrefix
 */
class ControllerChannelManager(controllerEpoch: () => Int,
                               config: KafkaConfig,
                               time: Time,
                               metrics: Metrics,
                               stateChangeLogger: StateChangeLogger,
                               threadNamePrefix: Option[String] = None) extends Logging {
  import ControllerChannelManager._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  //ControllerChannelManager 类最重要的数据结构是 brokerStateInfo
  //这是一个 HashMap 类型，Key 是 Integer 类型，其实就是集群中 Broker 的 ID 信息，而 Value 是一个 ControllerBrokerStateInfo。
  protected val brokerStateInfo = new mutable.HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  metricsGroup.newGauge("TotalQueueSize",
    () => brokerLock synchronized {
      brokerStateInfo.values.iterator.map(_.messageQueue.size).sum
    }
  )

  /**
   * Controller 组件在启动时，会调用 ControllerChannelManager 的 startup 方法。
   * 该方法会从元数据信息中找到集群的 Broker 列表，然后依次为它们调用 addBroker 方法，
   * 把它们加到 brokerStateInfo 变量中，最后再依次启动 brokerStateInfo 中的 RequestSendThread 线程。
   */
  def startup(initialBrokers: Set[Broker]):Unit = {
    initialBrokers.foreach(addNewBroker)

    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  /**
   * 关闭所有 RequestSendThread 线程，并清空必要的资源。
   */
  def shutdown():Unit = {
    brokerLock synchronized {
      brokerStateInfo.values.toList.foreach(removeExistingBroker)
    }
  }

  /**
   * 就是发送请求，实际上就是把请求对象提交到请求队列。
   */
  def sendRequest(brokerId: Int, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(request.apiKey, request, callback, time.milliseconds()))
        case None =>
          warn(s"Not sending request $request to broker $brokerId, since it is offline.")
      }
    }
  }

  /**
   * 添加目标 Broker 到 brokerStateInfo 数据结构中，并创建必要的配套资源，如请求队列、RequestSendThread 线程对象等。
   * 最后，RequestSendThread 启动线程。
   * 每当集群中扩容了新的 Broker 时，Controller 就会调用这个方法为新 Broker 增加新的 RequestSendThread 线程。
   *
   * 这段代码的逻辑是，判断目标 Broker 的序号，是否已经保存在 brokerStateInfo 中。
   * 如果是，就说明这个 Broker 之前已经添加过了，就没必要再次添加了；
   * 否则，addBroker 方法会对目前的 Broker 执行两个操作：
   * 1、把该 Broker 节点添加到 brokerStateInfo 中；
   * 2、启动与该 Broker 对应的 RequestSendThread 线程。
   */
  def addBroker(broker: Broker): Unit = {
    // be careful here. Maybe the startup() API has already started the request send thread
    //brokerStateInfo 仅仅是一个 HashMap 对象，因为不是线程安全的，所以任何访问该变量的地方，都需要锁的保护。
    brokerLock synchronized {
      // 如果该Broker是新Broker的话
      if (!brokerStateInfo.contains(broker.id)) {
        // 将新Broker加入到Controller管理，并创建对应的RequestSendThread线程
        addNewBroker(broker)
        // 启动RequestSendThread线程
        startRequestSendThread(broker.id)
      }
    }
  }

  /**
   * 从 brokerStateInfo 移除目标 Broker 的相关数据。
   */
  def removeBroker(brokerId: Int): Unit = {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  /**
   * addNewBroker 的关键在于，要为目标 Broker 创建一系列的配套资源，
   * 比如，NetworkClient 用于网络 I/O 操作、messageQueue 用于阻塞队列、requestThread 用于发送请求，等等。
   */
  private def addNewBroker(broker: Broker): Unit = {
    // 为该Broker构造请求阻塞队列
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug(s"Controller ${config.brokerId} trying to connect to broker ${broker.id}")
    val controllerToBrokerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    val controllerToBrokerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)
    // 获取待连接Broker节点对象信息
    val brokerNode = broker.node(controllerToBrokerListenerName)
    val logContext = new LogContext(s"[Controller id=${config.brokerId}, targetBrokerId=${brokerNode.idString}] ")
    val (networkClient, reconfigurableChannelBuilder) = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        controllerToBrokerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        controllerToBrokerListenerName,
        config.saslMechanismInterBrokerProtocol,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )
      val reconfigurableChannelBuilder = channelBuilder match {
        case reconfigurable: Reconfigurable =>
          config.addReconfigurable(reconfigurable)
          Some(reconfigurable)
        case _ => None
      }
      // 创建NIO Selector实例用于网络数据传输
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> brokerNode.idString).asJava,
        false,
        channelBuilder,
        logContext
      )
      // 创建NetworkClient实例
      // NetworkClient类是Kafka clients工程封装的顶层网络客户端API
      // 提供了丰富的方法实现网络层IO数据传输
      val networkClient = new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        config.connectionSetupTimeoutMs,
        config.connectionSetupTimeoutMaxMs,
        time,
        false,
        new ApiVersions,
        logContext
      )
      (networkClient, reconfigurableChannelBuilder)
    }
    // 为这个RequestSendThread线程设置线程名称
    val threadName = threadNamePrefix match {
      case None => s"Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
      case Some(name) => s"$name:Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
    }

    // 构造请求处理速率监控指标
    val requestRateAndQueueTimeMetrics = metricsGroup.newTimer(
      RequestRateAndQueueTimeMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS, brokerMetricTags(broker.id)
    )

    // 创建RequestSendThread实例
    val requestThread = new RequestSendThread(config.brokerId, controllerEpoch, messageQueue, networkClient,
      brokerNode, config, time, requestRateAndQueueTimeMetrics, stateChangeLogger, threadName)
    requestThread.setDaemon(false)

    val queueSizeGauge = metricsGroup.newGauge(QueueSizeMetricName, () => messageQueue.size, brokerMetricTags(broker.id))

    // 创建该Broker专属的ControllerBrokerStateInfo实例
    // 并将其加入到brokerStateInfo统一管理
    brokerStateInfo.put(broker.id, ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue,
      requestThread, queueSizeGauge, requestRateAndQueueTimeMetrics, reconfigurableChannelBuilder))
  }

  private def brokerMetricTags(brokerId: Int) = Map("broker-id" -> brokerId.toString).asJava

  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo): Unit = {
    try {
      // Shutdown the RequestSendThread before closing the NetworkClient to avoid the concurrent use of the
      // non-threadsafe classes as described in KAFKA-4959.
      // The call to shutdownLatch.await() in ShutdownableThread.shutdown() serves as a synchronization barrier that
      // hands off the NetworkClient from the RequestSendThread to the ZkEventThread.
      brokerState.reconfigurableChannelBuilder.foreach(config.removeReconfigurable)
      brokerState.requestSendThread.shutdown()
      brokerState.networkClient.close()
      brokerState.messageQueue.clear()
      metricsGroup.removeMetric(QueueSizeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      metricsGroup.removeMetric(RequestRateAndQueueTimeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  /**
   * 它首先根据给定的 Broker 序号信息，从 brokerStateInfo 中找出对应的 ControllerBrokerStateInfo 对象。
   * 有了这个对象，也就有了为该目标 Broker 服务的所有配套资源。
   * 下一步就是从 ControllerBrokerStateInfo 中拿出 RequestSendThread 对象，再启动它就好了。
   */
  protected def startRequestSendThread(brokerId: Int): Unit = {
    // 获取指定Broker的专属RequestSendThread实例
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if (requestThread.getState == Thread.State.NEW)
    // 启动线程
      requestThread.start()
  }
}

/**
 * 每个 QueueItem 的核心字段都是 AbstractControlRequest.Builder 对象。
 * 基本上可以认为，它就是阻塞队列上 AbstractControlRequest 类型。
 * @param apiKey
 * @param request 这里的“<:”符号，它在 Scala 中表示上边界的意思，即字段 request 必须是 AbstractControlRequest 的子类，
 *                也就是那三类请求：LeaderAndIsrRequest，StopReplicaRequest，UpdateMetadataRequest。
 * @param callback
 * @param enqueueTimeMs
 */
case class QueueItem(apiKey: ApiKeys, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                     callback: AbstractResponse => Unit, enqueueTimeMs: Long)

/**
 * Controller 会为集群中的每个 Broker 都创建一个对应的 RequestSendThread 线程。
 * Broker 上的这个线程，持续地从阻塞队列中获取待发送的请求。
 *
 * 它依然是一个线程安全的阻塞队列，Controller 事件处理线程 负责向这个队列写入待发送的请求，
 * 而一个名为 RequestSendThread 的线程负责执行真正的请求发送。
 * @param controllerId
 * @param controllerEpoch
 * @param queue 每个 QueueItem 实际保存的都是那三类请求中的其中一类。如果使用一个 BlockingQueue 对象来保存这些 QueueItem，那么，代码就实现了一个请求阻塞队列。
 * @param networkClient
 * @param brokerNode
 * @param config
 * @param time
 * @param requestRateAndQueueTimeMetrics
 * @param stateChangeLogger
 * @param name
 */
class RequestSendThread(val controllerId: Int, // Controller所在Broker的Id
                        controllerEpoch: () => Int,
                        val queue: BlockingQueue[QueueItem],//线程安全的请求阻塞队列
                        val networkClient: NetworkClient, // 用于执行发送的网络I/O类
                        val brokerNode: Node, // 目标Broker节点
                        val config: KafkaConfig, // Kafka配置信息
                        val time: Time,
                        val requestRateAndQueueTimeMetrics: Timer,
                        val stateChangeLogger: StateChangeLogger,
                        name: String)
  extends ShutdownableThread(name, true, s"[RequestSendThread controllerId=$controllerId] ")
    with Logging {

  logIdent = logPrefix

  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  /**
   * doWork 的逻辑很直观。它的主要作用是从阻塞队列中取出待发送的请求，然后把它发送出去，之后等待 Response 的返回。
   * 在等待 Response 的过程中，线程将一直处于阻塞状态。当接收到 Response 之后，调用 callback 执行请求处理完成后的回调逻辑。
   */
  override def doWork(): Unit = {

    def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)

    val QueueItem(apiKey, requestBuilder, callback, enqueueTimeMs) = queue.take()
    requestRateAndQueueTimeMetrics.update(time.milliseconds() - enqueueTimeMs, TimeUnit.MILLISECONDS)

    var clientResponse: ClientResponse = null
    try {
      var isSendSuccessful = false
      while (isRunning && !isSendSuccessful) {
        // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
        // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
        try {
          // 如果没有创建与目标Broker的TCP连接，或连接暂时不可用
          if (!brokerReady()) {
            isSendSuccessful = false
            backoff()
          }
          else {
            val clientRequest = networkClient.newClientRequest(brokerNode.idString, requestBuilder,
              time.milliseconds(), true)
            // 发送请求，等待接收Response;调用sendAndReceive后，会原地进入阻塞状态，等待 Response 返回。
            clientResponse = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
            isSendSuccessful = true
          }
        } catch {
          case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
            warn(s"Controller $controllerId epoch ${controllerEpoch()} fails to send request " +
              s"$requestBuilder " +
              s"to broker $brokerNode. Reconnecting to broker.", e)
            // 如果出现异常，关闭与对应Broker的连接
            networkClient.close(brokerNode.idString)
            isSendSuccessful = false
            backoff()
        }
      }
      // 如果接收到了Response
      if (clientResponse != null) {
        val requestHeader = clientResponse.requestHeader
        val api = requestHeader.apiKey
        // 此Response的请求类型必须是LeaderAndIsrRequest、StopReplicaRequest或UpdateMetadataRequest中的一种
        if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA)
          throw new KafkaException(s"Unexpected apiKey received: $apiKey")

        val response = clientResponse.responseBody

        stateChangeLogger.withControllerEpoch(controllerEpoch()).trace(s"Received response " +
          s"$response for request $api with correlation id " +
          s"${requestHeader.correlationId} sent to broker $brokerNode")

        if (callback != null) {
          callback(response)// 处理回调
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Controller $controllerId fails to send a request to broker $brokerNode", e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
        networkClient.close(brokerNode.idString)
    }
  }

  private def brokerReady(): Boolean = {
    try {
      if (!NetworkClientUtils.isReady(networkClient, brokerNode, time.milliseconds())) {
        if (!NetworkClientUtils.awaitReady(networkClient, brokerNode, time, socketTimeoutMs))
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info(s"Controller $controllerId connected to $brokerNode for sending state change requests")
      }

      true
    } catch {
      case e: Throwable =>
        warn(s"Controller $controllerId's connection to broker $brokerNode was unsuccessful", e)
        networkClient.close(brokerNode.idString)
        false
    }
  }

  override def initiateShutdown(): Boolean = {
    if (super.initiateShutdown()) {
      networkClient.initiateClose()
      true
    } else
      false
  }
}

class ControllerBrokerRequestBatch(
  config: KafkaConfig,
  controllerChannelManager: ControllerChannelManager,
  controllerEventManager: ControllerEventManager,
  controllerContext: ControllerContext,
  stateChangeLogger: StateChangeLogger
) extends AbstractControllerBrokerRequestBatch(
  config,
  () => controllerContext,
  () => config.interBrokerProtocolVersion,
  stateChangeLogger
) {

  def sendEvent(event: ControllerEvent): Unit = {
    controllerEventManager.put(event)
  }
  def sendRequest(brokerId: Int,
                  request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    controllerChannelManager.sendRequest(brokerId, request, callback)
  }

  override def handleLeaderAndIsrResponse(response: LeaderAndIsrResponse, broker: Int): Unit = {
    sendEvent(LeaderAndIsrResponseReceived(response, broker))
  }

  override def handleUpdateMetadataResponse(response: UpdateMetadataResponse, broker: Int): Unit = {
    sendEvent(UpdateMetadataResponseReceived(response, broker))
  }

  override def handleStopReplicaResponse(stopReplicaResponse: StopReplicaResponse, brokerId: Int,
                                         partitionErrorsForDeletingTopics: Map[TopicPartition, Errors]): Unit = {
    if (partitionErrorsForDeletingTopics.nonEmpty)
      sendEvent(TopicDeletionStopReplicaResponseReceived(brokerId, stopReplicaResponse.error, partitionErrorsForDeletingTopics))
  }
}

/**
 * Structure to send RPCs from controller to broker to inform about the metadata and leadership
 * changes in the system.
 * @param config Kafka config present in the controller.
 * @param metadataProvider Provider to provide the relevant metadata to build the state needed to
 *                         send RPCs
 * @param metadataVersionProvider Provider to provide the metadata version used by the controller.
 * @param stateChangeLogger logger to log the various events while sending requests and receving
 *                          responses from the brokers
 * @param kraftController whether the controller is KRaft controller
 */
abstract class AbstractControllerBrokerRequestBatch(config: KafkaConfig,
                                                    metadataProvider: () => ControllerChannelContext,
                                                    metadataVersionProvider: () => MetadataVersion,
                                                    stateChangeLogger: StateChangeLogger,
                                                    kraftController: Boolean = false) extends Logging {
  val controllerId: Int = config.brokerId
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, LeaderAndIsrPartitionState]]
  val stopReplicaRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, StopReplicaPartitionState]]
  val updateMetadataRequestBrokerSet = mutable.Set.empty[Int]
  val updateMetadataRequestPartitionInfoMap = mutable.Map.empty[TopicPartition, UpdateMetadataPartitionState]
  private var metadataInstance: ControllerChannelContext = _

  def sendRequest(brokerId: Int,
                  request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit

  def newBatch(): Unit = {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        s"a new one. Some LeaderAndIsr state changes $leaderAndIsrRequestMap might be lost ")
    if (stopReplicaRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some StopReplica state changes $stopReplicaRequestMap might be lost ")
    if (updateMetadataRequestBrokerSet.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some UpdateMetadata state changes to brokers $updateMetadataRequestBrokerSet with partition info " +
        s"$updateMetadataRequestPartitionInfoMap might be lost ")
    metadataInstance = metadataProvider()
  }

  def clear(): Unit = {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
    metadataInstance = null
  }

  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int],
                                       topicPartition: TopicPartition,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicaAssignment: ReplicaAssignment,
                                       isNew: Boolean): Unit = {

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyNew = result.get(topicPartition).exists(_.isNew)
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      val partitionState = new LeaderAndIsrPartitionState()
        .setTopicName(topicPartition.topic)
        .setPartitionIndex(topicPartition.partition)
        .setControllerEpoch(leaderIsrAndControllerEpoch.controllerEpoch)
        .setLeader(leaderAndIsr.leader)
        .setLeaderEpoch(leaderAndIsr.leaderEpoch)
        .setIsr(leaderAndIsr.isr.map(Integer.valueOf).asJava)
        .setPartitionEpoch(leaderAndIsr.partitionEpoch)
        .setReplicas(replicaAssignment.replicas.map(Integer.valueOf).asJava)
        .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
        .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
        .setIsNew(isNew || alreadyNew)

      if (metadataVersionProvider.apply().isAtLeast(IBP_3_2_IV0)) {
        partitionState.setLeaderRecoveryState(leaderAndIsr.leaderRecoveryState.value)
      }

      result.put(topicPartition, partitionState)
    }

    addUpdateMetadataRequestForBrokers(metadataInstance.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
  }

  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int],
                                      topicPartition: TopicPartition,
                                      deletePartition: Boolean): Unit = {
    // A sentinel (-2) is used as an epoch if the topic is queued for deletion. It overrides
    // any existing epoch.
    val leaderEpoch = metadataInstance.leaderEpoch(topicPartition)

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = stopReplicaRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyDelete = result.get(topicPartition).exists(_.deletePartition)
      result.put(topicPartition, new StopReplicaPartitionState()
          .setPartitionIndex(topicPartition.partition())
          .setLeaderEpoch(leaderEpoch)
          .setDeletePartition(alreadyDelete || deletePartition))
    }
  }

  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicPartition]): Unit = {
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
    partitions.foreach { partition =>
      val beingDeleted = metadataInstance.isTopicQueuedUpForDeletion(partition.topic())
      metadataInstance.partitionLeadershipInfo(partition) match {
        case Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)) =>
          val replicas = metadataInstance.partitionReplicaAssignment(partition)
          val offlineReplicas = replicas.filter(!metadataInstance.isReplicaOnline(_, partition))
          val updatedLeaderAndIsr =
            if (beingDeleted) LeaderAndIsr.duringDelete(leaderAndIsr.isr)
            else leaderAndIsr
          addUpdateMetadataRequestForBrokers(brokerIds, controllerEpoch, partition,
            updatedLeaderAndIsr.leader, updatedLeaderAndIsr.leaderEpoch, updatedLeaderAndIsr.partitionEpoch,
            updatedLeaderAndIsr.isr, replicas, offlineReplicas)
        case None =>
          info(s"Leader not yet assigned for partition $partition. Skip sending UpdateMetadataRequest.")
      }
    }
  }

  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int]): Unit = {
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
  }

  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         controllerEpoch: Int,
                                         partition: TopicPartition,
                                         leader: Int,
                                         leaderEpoch: Int,
                                         partitionEpoch: Int,
                                         isrs: List[Int],
                                         replicas: Seq[Int],
                                         offlineReplicas: Seq[Int]): Unit = {
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
    val partitionStateInfo = new UpdateMetadataPartitionState()
      .setTopicName(partition.topic)
      .setPartitionIndex(partition.partition)
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isrs.map(Integer.valueOf).asJava)
      .setZkVersion(partitionEpoch)
      .setReplicas(replicas.map(Integer.valueOf).asJava)
      .setOfflineReplicas(offlineReplicas.map(Integer.valueOf).asJava)
    updateMetadataRequestPartitionInfoMap.put(partition, partitionStateInfo)
  }

  private def sendLeaderAndIsrRequest(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    val metadataVersion = metadataVersionProvider.apply()
    val leaderAndIsrRequestVersion: Short =
      if (metadataVersion.isAtLeast(IBP_3_4_IV0)) 7
      else if (metadataVersion.isAtLeast(IBP_3_2_IV0)) 6
      else if (metadataVersion.isAtLeast(IBP_2_8_IV1)) 5
      else if (metadataVersion.isAtLeast(IBP_2_4_IV1)) 4
      else if (metadataVersion.isAtLeast(IBP_2_4_IV0)) 3
      else if (metadataVersion.isAtLeast(IBP_2_2_IV0)) 2
      else if (metadataVersion.isAtLeast(IBP_1_0_IV0)) 1
      else 0

    leaderAndIsrRequestMap.forKeyValue { (broker, leaderAndIsrPartitionStates) =>
      if (metadataInstance.liveOrShuttingDownBrokerIds.contains(broker)) {
        val leaderIds = mutable.Set.empty[Int]
        var numBecomeLeaders = 0
        leaderAndIsrPartitionStates.forKeyValue { (topicPartition, state) =>
          leaderIds += state.leader
          val typeOfRequest = if (broker == state.leader) {
            numBecomeLeaders += 1
            "become-leader"
          } else {
            "become-follower"
          }
          if (stateChangeLog.isTraceEnabled)
            stateChangeLog.trace(s"Sending $typeOfRequest LeaderAndIsr request $state to broker $broker for partition $topicPartition")
        }
        stateChangeLog.info(s"Sending LeaderAndIsr request to broker $broker with $numBecomeLeaders become-leader " +
          s"and ${leaderAndIsrPartitionStates.size - numBecomeLeaders} become-follower partitions")
        val leaders = metadataInstance.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.node(config.interBrokerListenerName)
        }
        val brokerEpoch = metadataInstance.liveBrokerIdAndEpochs(broker)
        val topicIds = leaderAndIsrPartitionStates.keys
          .map(_.topic)
          .toSet[String]
          .map(topic => (topic, metadataInstance.topicIds.getOrElse(topic, Uuid.ZERO_UUID)))
          .toMap
        val leaderAndIsrRequestBuilder = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId,
          controllerEpoch, brokerEpoch, leaderAndIsrPartitionStates.values.toBuffer.asJava, topicIds.asJava, leaders.asJava, kraftController)
        sendRequest(broker, leaderAndIsrRequestBuilder, (r: AbstractResponse) => {
          val leaderAndIsrResponse = r.asInstanceOf[LeaderAndIsrResponse]
          handleLeaderAndIsrResponse(leaderAndIsrResponse, broker)
        })
      }
    }
    leaderAndIsrRequestMap.clear()
  }

  def handleLeaderAndIsrResponse(response: LeaderAndIsrResponse, broker: Int): Unit

  private def sendUpdateMetadataRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    stateChangeLog.info(s"Sending UpdateMetadata request to brokers $updateMetadataRequestBrokerSet " +
      s"for ${updateMetadataRequestPartitionInfoMap.size} partitions")

    val partitionStates = updateMetadataRequestPartitionInfoMap.values.toBuffer
    val metadataVersion = metadataVersionProvider.apply()
    val updateMetadataRequestVersion: Short =
      if (metadataVersion.isAtLeast(IBP_3_4_IV0)) 8
      else if (metadataVersion.isAtLeast(IBP_2_8_IV1)) 7
      else if (metadataVersion.isAtLeast(IBP_2_4_IV1)) 6
      else if (metadataVersion.isAtLeast(IBP_2_2_IV0)) 5
      else if (metadataVersion.isAtLeast(IBP_1_0_IV0)) 4
      else if (metadataVersion.isAtLeast(IBP_0_10_2_IV0)) 3
      else if (metadataVersion.isAtLeast(IBP_0_10_0_IV1)) 2
      else if (metadataVersion.isAtLeast(IBP_0_9_0)) 1
      else 0

    val liveBrokers = metadataInstance.liveOrShuttingDownBrokers.iterator.map { broker =>
      val endpoints = if (updateMetadataRequestVersion == 0) {
        // Version 0 of UpdateMetadataRequest only supports PLAINTEXT
        val securityProtocol = SecurityProtocol.PLAINTEXT
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val node = broker.node(listenerName)
        Seq(new UpdateMetadataEndpoint()
          .setHost(node.host)
          .setPort(node.port)
          .setSecurityProtocol(securityProtocol.id)
          .setListener(listenerName.value))
      } else {
        broker.endPoints.map { endpoint =>
          new UpdateMetadataEndpoint()
            .setHost(endpoint.host)
            .setPort(endpoint.port)
            .setSecurityProtocol(endpoint.securityProtocol.id)
            .setListener(endpoint.listenerName.value)
        }
      }
      new UpdateMetadataBroker()
        .setId(broker.id)
        .setEndpoints(endpoints.asJava)
        .setRack(broker.rack.orNull)
    }.toBuffer

    updateMetadataRequestBrokerSet.intersect(metadataInstance.liveOrShuttingDownBrokerIds).foreach { broker =>
      val brokerEpoch = metadataInstance.liveBrokerIdAndEpochs(broker)
      val topicIds = partitionStates.map(_.topicName())
        .distinct
        .filter(metadataInstance.topicIds.contains)
        .map(topic => (topic, metadataInstance.topicIds(topic))).toMap
      val updateMetadataRequestBuilder = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion,
        controllerId, controllerEpoch, brokerEpoch, partitionStates.asJava, liveBrokers.asJava, topicIds.asJava, kraftController)
      sendRequest(broker, updateMetadataRequestBuilder, (r: AbstractResponse) => {
        val updateMetadataResponse = r.asInstanceOf[UpdateMetadataResponse]
        handleUpdateMetadataResponse(updateMetadataResponse, broker)
      })

    }
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }

  def handleUpdateMetadataResponse(response: UpdateMetadataResponse, broker: Int): Unit

  private def sendStopReplicaRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    val traceEnabled = stateChangeLog.isTraceEnabled
    val metadataVersion = metadataVersionProvider.apply()
    val stopReplicaRequestVersion: Short =
      if (metadataVersion.isAtLeast(IBP_3_4_IV0)) 4
      else if (metadataVersion.isAtLeast(IBP_2_6_IV0)) 3
      else if (metadataVersion.isAtLeast(IBP_2_4_IV1)) 2
      else if (metadataVersion.isAtLeast(IBP_2_2_IV0)) 1
      else 0

    def responseCallback(brokerId: Int, isPartitionDeleted: TopicPartition => Boolean)
                        (response: AbstractResponse): Unit = {
      val stopReplicaResponse = response.asInstanceOf[StopReplicaResponse]
      val partitionErrorsForDeletingTopics = mutable.Map.empty[TopicPartition, Errors]
      stopReplicaResponse.partitionErrors.forEach { pe =>
        val tp = new TopicPartition(pe.topicName, pe.partitionIndex)
        if (metadataInstance.isTopicDeletionInProgress(pe.topicName) &&
            isPartitionDeleted(tp)) {
          partitionErrorsForDeletingTopics += tp -> Errors.forCode(pe.errorCode)
        }
      }
      if (partitionErrorsForDeletingTopics.nonEmpty)
        handleStopReplicaResponse(stopReplicaResponse, brokerId, partitionErrorsForDeletingTopics.toMap)
    }

    stopReplicaRequestMap.forKeyValue { (brokerId, partitionStates) =>
      if (metadataInstance.liveOrShuttingDownBrokerIds.contains(brokerId)) {
        if (traceEnabled)
          partitionStates.forKeyValue { (topicPartition, partitionState) =>
            stateChangeLog.trace(s"Sending StopReplica request $partitionState to " +
              s"broker $brokerId for partition $topicPartition")
          }

        val brokerEpoch = metadataInstance.liveBrokerIdAndEpochs(brokerId)
        if (stopReplicaRequestVersion >= 3) {
          val stopReplicaTopicState = mutable.Map.empty[String, StopReplicaTopicState]
          partitionStates.forKeyValue { (topicPartition, partitionState) =>
            val topicState = stopReplicaTopicState.getOrElseUpdate(topicPartition.topic,
              new StopReplicaTopicState().setTopicName(topicPartition.topic))
            topicState.partitionStates().add(partitionState)
          }

          stateChangeLog.info(s"Sending StopReplica request for ${partitionStates.size} " +
            s"replicas to broker $brokerId")
          val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
            stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
            false, stopReplicaTopicState.values.toBuffer.asJava, kraftController)
          sendRequest(brokerId, stopReplicaRequestBuilder,
            responseCallback(brokerId, tp => partitionStates.get(tp).exists(_.deletePartition)))
        } else {
          var numPartitionStateWithDelete = 0
          var numPartitionStateWithoutDelete = 0
          val topicStatesWithDelete = mutable.Map.empty[String, StopReplicaTopicState]
          val topicStatesWithoutDelete = mutable.Map.empty[String, StopReplicaTopicState]

          partitionStates.forKeyValue { (topicPartition, partitionState) =>
            val topicStates = if (partitionState.deletePartition()) {
              numPartitionStateWithDelete += 1
              topicStatesWithDelete
            } else {
              numPartitionStateWithoutDelete += 1
              topicStatesWithoutDelete
            }
            val topicState = topicStates.getOrElseUpdate(topicPartition.topic,
              new StopReplicaTopicState().setTopicName(topicPartition.topic))
            topicState.partitionStates().add(partitionState)
          }

          if (topicStatesWithDelete.nonEmpty) {
            stateChangeLog.info(s"Sending StopReplica request (delete = true) for " +
              s"$numPartitionStateWithDelete replicas to broker $brokerId")
            val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
              stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
              true, topicStatesWithDelete.values.toBuffer.asJava, kraftController)
            sendRequest(brokerId, stopReplicaRequestBuilder, responseCallback(brokerId, _ => true))
          }

          if (topicStatesWithoutDelete.nonEmpty) {
            stateChangeLog.info(s"Sending StopReplica request (delete = false) for " +
              s"$numPartitionStateWithoutDelete replicas to broker $brokerId")
            val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
              stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
              false, topicStatesWithoutDelete.values.toBuffer.asJava, kraftController)
            sendRequest(brokerId, stopReplicaRequestBuilder)
          }
        }
      }
    }

    stopReplicaRequestMap.clear()
  }

  def handleStopReplicaResponse(stopReplicaResponse: StopReplicaResponse, brokerId: Int,
                                partitionErrorsForDeletingTopics: Map[TopicPartition, Errors]): Unit

  def sendRequestsToBrokers(controllerEpoch: Int): Unit = {
    try {
      val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerEpoch)
      sendLeaderAndIsrRequest(controllerEpoch, stateChangeLog)
      sendUpdateMetadataRequests(controllerEpoch, stateChangeLog)
      sendStopReplicaRequests(controllerEpoch, stateChangeLog)
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
}

/**
 * 如果 Controller 要给 Broker 发送请求，肯定需要解决三个问题：发给谁？发什么？怎么发？
 * “发给谁”就是由 brokerNode 决定的；
 * messageQueue 里面保存了要发送的请求，因而解决了“发什么”的问题；
 * 最后的“怎么发”就是依赖 requestSendThread 变量实现的。
 */
case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node, //brokerNode：目标 Broker 节点对象，里面封装了目标 Broker 的连接信息，比如主机名、端口号等。
                                     messageQueue: BlockingQueue[QueueItem], //请求消息阻塞队列。你可以发现，Controller 为每个目标 Broker 都创建了一个消息队列。
                                     requestSendThread: RequestSendThread, //Controller 使用这个线程给目标 Broker 发送请求。
                                     queueSizeGauge: Gauge[Int],
                                     requestRateAndTimeMetrics: Timer,
                                     reconfigurableChannelBuilder: Option[Reconfigurable])

