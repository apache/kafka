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

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.yammer.metrics.core.{Gauge, Timer}
import kafka.api._
import kafka.cluster.Broker
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.clients._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{KafkaException, Node, Reconfigurable, TopicPartition, Uuid}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.HashMap
import scala.collection.{Seq, Set, mutable}

object ControllerChannelManager {
  val QueueSizeMetricName = "QueueSize"
  val RequestRateAndQueueTimeMetricName = "RequestRateAndQueueTimeMs"
}

/**
 * Controller管理器组件：和其它Broker沟通的桥梁
 * 控制面三种类型的请求：
 * ①LeaderAndIsr。告知Broker相关主题各个分区的Leader副本所在的Broker节点、ISR副本集合以及这些副本所在的Broker节点。
 * 简单来说，就是告诉其它Broker关于某些主题详情（Leader是谁、ISR集合有哪些、它们这些分区所在的Broker节点信息）
 * ②StopReplica。告知特定的Broker停止它所管理的副本对象。使用的场景是：①分区副本迁移; ②删除主题
 * ③UpdateMetadata。广播请求。通知所有的Broker更新自己的元数据缓存。
 *
 * 消费模型：
 * ① Controller会主动的和所有Broker创建TCP连接，
 * 每个TCP连接都对应一个 {@link RequestSendThread} 线程，这个线程负责发送和接收响应数据（阻塞式）。
 * ② ZK是集群最开始感知Broker节点/主题/分区发生变化，然后会通过Watch机制让Controller感知到，
 * Controller收到对应的ZK事件，然后转换为Controller事件并放入到事件队列中，等待消费者获取并处理。
 *
 * 总结：
 * ControllerChannelManager相对整个Controller组件来说是偏向底层的，这个对象用来管理和Broker的连接通道，
 * 调用 {@link ControllerChannelManager.sendRequest} 发送请求，如果指定回调方法，那么当收到响应的时候由
 * 任务线程触发执行回调方法
 *
 * @param controllerContext Controller上下文
 * @param config            当前Broker所有配置项
 * @param time              时间工具类
 * @param metrics           监控指标
 * @param stateChangeLogger 日志
 * @param threadNamePrefix  线程名称前缀
 */
class ControllerChannelManager(controllerContext: ControllerContext,
                               config: KafkaConfig,
                               time: Time,
                               metrics: Metrics,
                               stateChangeLogger: StateChangeLogger,
                               threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {

  import ControllerChannelManager._

  /**
   * Controller会和每个Broker建立TCP连接，这个字段就是维护当前Controller和其实Broker的连接信息。
   * 每个ControllerBrokerStateInfo对象中都会有一个任务队列，ControllerChannelManager对象是一个生产者，
   * 它产生的请求体会被放入指定的Broker的任务队列中，而每个Broker又会有独立的{@link RequestSendThread}线程，
   * 这个线程就属于消费者，因此，它们构成一个简单的消费者-生产者模型。
   * ControllerChannelManager->产生Request->通过「brokerStateInfo」找到对应的ControllerBrokerStateInfo
   * ->把请求放入任务队列中
   */
  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  newGauge("TotalQueueSize",
    () => brokerLock synchronized {
      brokerStateInfo.values.iterator.map(_.messageQueue.size).sum
    }
  )

  /**
   * 启动Controller管理器，Controller组件在启动时，会调用此方法，它会做以下事情：
   * ① 从元数据信息中得到集群的Broker列表
   * ② 依次为这些Broker调用 {@link ControllerChannelManager.addNewBroker( )} 方法，
   * 这个方法会做很多事件：最终会把它们添加到 {@link ControllerChannelManager.brokerStateInfo} 变量中
   * ③ 依次启动 {@link ControllerChannelManager.brokerStateInfo} 中的 {@link RequestSendThread} 线程
   * 完成这些步骤，就表示Controller启动完成
   */
  def startup() = {
    // #1 从元数据信息中得到集群的Broker列表
    // #2 把它们添加到 {@link ControllerChannelManager.brokerStateInfo} 变量中
    controllerContext.liveOrShuttingDownBrokers.foreach(addNewBroker)

    brokerLock synchronized {
      // 依次启动 {@link ControllerChannelManager.brokerStateInfo} 中的 {@link RequestSendThread} 线程
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  /**
   * 关闭Controller管理器
   */
  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.values.toList.foreach(removeExistingBroker)
    }
  }

  /**
   * 将待发送的请求放入指定的Broker任务队列，等待相应的 {@link RequestSendThread} 线程消费
   *
   * @param brokerId 请求发往的目的地
   * @param request  待发送的请求
   * @param callback 发送成功后的回调方法
   */
  def sendRequest(brokerId: Int, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    brokerLock synchronized {
      // #1 根据Broker ID获取「ControllerBrokerStateInfo」对象
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          // #2 将请求和回调等数据使用QueueItem对象封装，并加入到任务队列中等待发送到Broker端
          stateInfo.messageQueue.put(QueueItem(request.apiKey, request, callback, time.milliseconds()))
        case None =>
          // 找不到，目标Broker离线了
          warn(s"Not sending request $request to broker $brokerId, since it is offline.")
      }
    }
  }

  /**
   * 添加新的Broker对象
   *
   * @param broker
   */
  def addBroker(broker: Broker): Unit = {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if (!brokerStateInfo.contains(broker.id)) {
        //
        addNewBroker(broker)
        startRequestSendThread(broker.id)
      }
    }
  }

  def removeBroker(brokerId: Int): Unit = {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  /**
   * 添加新的Broker对象
   *
   * @param broker
   */
  private def addNewBroker(broker: Broker): Unit = {
    // #1 创建并发的阻塞任务队列，存放待发送给Broker的请求体
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug(s"Controller ${config.brokerId} trying to connect to broker ${broker.id}")

    // #2 获取「inter.broker.listener.name」所配置的监听器，并构建Broker对象
    val controllerToBrokerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    val controllerToBrokerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)
    val brokerNode = broker.node(controllerToBrokerListenerName)

    val logContext = new LogContext(s"[Controller id=${config.brokerId}, targetBrokerId=${brokerNode.idString}] ")

    // #3 创建并配置网络层相关组件，我们只需要知道可以通过networkClinet向底层Sokcet发送数据就可以了
    val (networkClient, reconfigurableChannelBuilder) = {
      // #3-1 channelBuilder可以构建SocketChannel
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
      // #3-2 可动态修改配置类
      val reconfigurableChannelBuilder = channelBuilder match {
        case reconfigurable: Reconfigurable =>
          config.addReconfigurable(reconfigurable)
          Some(reconfigurable)
        case _ => None
      }

      // #3-3 创建轮询器
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

      // #3-4 网络层IO
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
        ClientDnsLookup.USE_ALL_DNS_IPS,
        time,
        false,
        new ApiVersions,
        logContext
      )
      (networkClient, reconfigurableChannelBuilder)
    }

    // #4 线程名
    val threadName = threadNamePrefix match {
      case None => s"Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
      case Some(name) => s"$name:Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
    }

    // #5 监控指标类
    val requestRateAndQueueTimeMetrics = newTimer(
      RequestRateAndQueueTimeMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS, brokerMetricTags(broker.id)
    )

    // #6 创建消费者，即RequestSendThread线程，它会不断从「messageQueue」队列中获取待发送的请求，随后通过networkClient执行底层网络I/O操作
    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, requestRateAndQueueTimeMetrics, stateChangeLogger, threadName)
    requestThread.setDaemon(false)

    // #7 监控指标
    val queueSizeGauge = newGauge(QueueSizeMetricName, () => messageQueue.size, brokerMetricTags(broker.id))

    // #8 将变量使用ControllerBrokerStateInfo POJO类封装起来，然后放入brokerStateInfo中
    brokerStateInfo.put(broker.id, ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue,
      requestThread, queueSizeGauge, requestRateAndQueueTimeMetrics, reconfigurableChannelBuilder))
  }

  private def brokerMetricTags(brokerId: Int) = Map("broker-id" -> brokerId.toString)

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
      removeMetric(QueueSizeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      removeMetric(RequestRateAndQueueTimeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  protected def startRequestSendThread(brokerId: Int): Unit = {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if (requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

/**
 * <: 表示泛型上边界，即request必须是AbstractControlRequest的子类
 *
 * @param apiKey        RPC请求类型
 * @param request       待发送的请求
 * @param callback      响应回调
 * @param enqueueTimeMs 入队时间
 */
case class QueueItem(apiKey: ApiKeys,
                     request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                     callback: AbstractResponse => Unit,
                     enqueueTimeMs: Long)

/**
 * Control 请求发送线程
 *
 * @param controllerId                   Controller所在的Broker节点ID
 * @param controllerContext              Controller上下文，内部保存Controller元数据信息
 * @param queue                          请求阻塞队列
 * @param networkClient                  执行网络I/O的类
 * @param brokerNode                     目标节点ID，发送线程和目标节点建立一条TCP连接
 * @param config                         Kafka配置信息
 * @param time                           时间工具类
 * @param requestRateAndQueueTimeMetrics 相关监控指标
 * @param stateChangeLogger
 * @param name
 */
class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val queue: BlockingQueue[QueueItem],
                        val networkClient: NetworkClient,
                        val brokerNode: Node,
                        val config: KafkaConfig,
                        val time: Time,
                        val requestRateAndQueueTimeMetrics: Timer,
                        val stateChangeLogger: StateChangeLogger,
                        name: String)
  extends ShutdownableThread(name = name) {

  logIdent = s"[RequestSendThread controllerId=$controllerId] "

  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  /**
   * Controller请求发送线程的核心方法，
   * 我们说过，Controller属于消费者-生产者模型，而{@link RequestSendThread}属于消费者，
   * 它会从{@param queue}队列中获取任务（实际就是待发送的请求体），然后通过 {@param networkClient}网络层
   * 向{@link brokerNode}发送请求，并阻塞等待响应成功接收，最后再触发回调方法的执行，
   * 以上就是线程 {@link RequestSendThread} 和其它组件配合使用
   * 小结：①从任务队列中获取待发送的请求②发送并阻塞等待响应返回③执行回调方法
   */
  override def doWork(): Unit = {
    // 重试退避时间
    def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)

    // #1 从缓冲队列中获取待发送的请求
    val QueueItem(apiKey, requestBuilder, callback, enqueueTimeMs) = queue.take()

    // 更新
    requestRateAndQueueTimeMetrics.update(time.milliseconds() - enqueueTimeMs, TimeUnit.MILLISECONDS)

    var clientResponse: ClientResponse = null
    try {
      // 判断是否发送成功
      var isSendSuccessful = false
      while (isRunning && !isSendSuccessful) {
        // 如果一个Broker长时间处于宕机，那么在某个时间点Zookeeper的监听顺会触发一个「removeBroker」事件，这个事件将会调用这个线程的
        // shutdown()方法，这样我们就会停止重试
        try {
          // #2 判断TCP连接处于「可发送数据」状态
          if (!brokerReady()) {
            // #2-1 TCP连接不可用，阻塞线程直到超出重试退避时间（会不断尝试重新建立TCP连接）
            isSendSuccessful = false
            backoff()
          }
          else {
            // #2-2 TCP处于正常状态，可以发送数据，那么就构建新的请求体
            val clientRequest = networkClient.newClientRequest(brokerNode.idString, requestBuilder, time.milliseconds(), true)
            // #2-3 发送请求，等待接收Response（假阻塞，因为是使用while不断轮询）
            clientResponse = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
            isSendSuccessful = true
          }
        } catch {
          case e: Throwable =>
            // 如果发送失败，重连Broker并且重新发送数据
            warn(s"Controller $controllerId epoch ${controllerContext.epoch} fails to send request $requestBuilder " +
              s"to broker $brokerNode. Reconnecting to broker.", e)
            networkClient.close(brokerNode.idString)
            isSendSuccessful = false
            backoff()
        }
      }

      // #3 处理响应体
      if (clientResponse != null) {
        val requestHeader = clientResponse.requestHeader
        // #3-1 获取RPC请求类型
        val api = requestHeader.apiKey
        if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA) {
          // 如果RCP请求类型不属于LEADER_AND_ISR、STOP_REPLICA、UPDATE_METADATA，则抛出异常
          throw new KafkaException(s"Unexpected apiKey received: $apiKey")
        }

        // 获取响应体
        val response = clientResponse.responseBody

        // 日志记录
        stateChangeLogger.withControllerEpoch(controllerContext.epoch).trace(s"Received response " +
          s"$response for request $api with correlation id " +
          s"${requestHeader.correlationId} sent to broker $brokerNode")

        // 触发方法回调
        if (callback != null) {
          callback(response)
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

/**
 *
 * @param config            Broker配置信息
 * @param controllerChannelManager
 * @param controllerEventManager
 * @param controllerContext 集群元数据
 * @param stateChangeLogger 日志
 */
class ControllerBrokerRequestBatch(config: KafkaConfig,
                                   controllerChannelManager: ControllerChannelManager,
                                   controllerEventManager: ControllerEventManager,
                                   controllerContext: ControllerContext,
                                   stateChangeLogger: StateChangeLogger)
  extends AbstractControllerBrokerRequestBatch(config, controllerContext, stateChangeLogger) {

  def sendEvent(event: ControllerEvent): Unit = {
    controllerEventManager.put(event)
  }

  /**
   * 将待发送的请求放入到缓冲队列中，由 {@link RequestSendThread} 线程拉取并发送数据
   * @param brokerId 目标Broker ID
   * @param request  请求体
   * @param callback 收到响应后触发的回调方法
   */
  def sendRequest(brokerId: Int,
                  request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    controllerChannelManager.sendRequest(brokerId, request, callback)
  }

}

/**
 *
 * @param config            Broker配置信息
 * @param controllerContext 集群元数据
 * @param stateChangeLogger 日志
 */
abstract class AbstractControllerBrokerRequestBatch(config: KafkaConfig,
                                                    controllerContext: ControllerContext,
                                                    stateChangeLogger: StateChangeLogger) extends Logging {
  // Controller ID
  val controllerId: Int = config.brokerId

  /**
   * LeaderAndIsr 类型请求放在这个Map集合中
   * key：Broker ID，value：分区所有的元数据信息，包括Leader副本、ISR集合等
   * 发往某个broker有多个分区
   */
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, LeaderAndIsrPartitionState]]

  /**
   * StopReplica 类型请求放在这个Map集合中
   */
  val stopReplicaRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, StopReplicaPartitionState]]

  /**
   * UpdateMetadata 类型请求放在这个Map集合中
   */
  val updateMetadataRequestPartitionInfoMap = mutable.Map.empty[TopicPartition, UpdateMetadataPartitionState]

  /**
   * LeaderAndIsr 类型请求放在这个Map集合中
   */
  val updateMetadataRequestBrokerSet = mutable.Set.empty[Int]

  /**
   * 向 {@link ControllerContext} 注册 Controller 事件
   * 抽象方法，由子类实现
   *
   * @param event
   */
  def sendEvent(event: ControllerEvent): Unit

  /**
   * 通过 {@link ControllerChannelManager} 向Broker发送请求
   *
   * @param brokerId 目标Broker ID
   * @param request  请求体
   * @param callback 收到响应后触发的回调方法
   */
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
  }

  def clear(): Unit = {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }

  /**
   *
   * @param brokerIds
   * @param topicPartition
   * @param leaderIsrAndControllerEpoch
   * @param replicaAssignment
   * @param isNew
   */
  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int],
                                       topicPartition: TopicPartition,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicaAssignment: ReplicaAssignment,
                                       isNew: Boolean): Unit = {

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyNew = result.get(topicPartition).exists(_.isNew)
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      result.put(topicPartition, new LeaderAndIsrPartitionState()
        .setTopicName(topicPartition.topic) // 主题名称
        .setPartitionIndex(topicPartition.partition) // 分区号
        .setControllerEpoch(leaderIsrAndControllerEpoch.controllerEpoch) // controller版本号
        .setLeader(leaderAndIsr.leader) // 副本leader
        .setLeaderEpoch(leaderAndIsr.leaderEpoch) // 副本leader版本号
        .setIsr(leaderAndIsr.isr.map(Integer.valueOf).asJava) // ISR集合
        .setZkVersion(leaderAndIsr.zkVersion) // zk版本号
        .setReplicas(replicaAssignment.replicas.map(Integer.valueOf).asJava) // 分区副本集合
        .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava) // 正在加入的副本集合
        .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava) // 正在删除的副本集合
        .setIsNew(isNew || alreadyNew)) // 该副本是否应该存在broker上
    }

    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
  }

  /**
   *
   * @param brokerIds
   * @param topicPartition
   * @param deletePartition
   */
  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int],
                                      topicPartition: TopicPartition,
                                      deletePartition: Boolean): Unit = {
    // A sentinel (-2) is used as an epoch if the topic is queued for deletion. It overrides
    // any existing epoch.
    val leaderEpoch = if (controllerContext.isTopicQueuedUpForDeletion(topicPartition.topic)) {
      LeaderAndIsr.EpochDuringDelete
    } else {
      controllerContext.partitionLeadershipInfo(topicPartition)
        .map(_.leaderAndIsr.leaderEpoch)
        .getOrElse(LeaderAndIsr.NoEpoch)
    }

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = stopReplicaRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyDelete = result.get(topicPartition).exists(_.deletePartition)
      result.put(topicPartition, new StopReplicaPartitionState()
        .setPartitionIndex(topicPartition.partition())
        .setLeaderEpoch(leaderEpoch)
        .setDeletePartition(alreadyDelete || deletePartition))
    }
  }

  /**
   * 向给定的Broker更新给定的分区的元数据
   *
   * @param brokerIds  目标节点ID
   * @param partitions 期望Broker更新的主题分区
   */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicPartition]): Unit = {

    /**
     * 生成请求体，并将它放入到 {@link AbstractControllerBrokerRequestBatch.updateMetadataRequestBrokerSet} 集合中
     *
     * @param partition    分区详情
     * @param beingDeleted 是否需要删除，如果「controllerContext.topicsToBeDeleted」包含则为true
     */
    def updateMetadataRequestPartitionInfo(partition: TopicPartition, beingDeleted: Boolean): Unit = {
      // 从controller缓存中获取该分区详情，包括Leader副本、ISR集合等元数据信息
      controllerContext.partitionLeadershipInfo(partition) match {
        // 如果controller缓存找到，说明该分区已经选举出Leader副本，才能执行下面逻辑
        case Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)) =>
          // 获取分区的副本ID集合，副本ID是副本所在的Broker ID
          val replicas = controllerContext.partitionReplicaAssignment(partition)

          // 过滤出Broker离线的副本集合
          val offlineReplicas = replicas.filter(!controllerContext.isReplicaOnline(_, partition))
          val updatedLeaderAndIsr =
            if (beingDeleted) LeaderAndIsr.duringDelete(leaderAndIsr.isr)
            else leaderAndIsr

          // 构建「UpdateMetadataRequest」请求对象
          val partitionStateInfo = new UpdateMetadataPartitionState()
            .setTopicName(partition.topic) // 主题名称
            .setPartitionIndex(partition.partition) // 分区索引号
            .setControllerEpoch(controllerEpoch) // controller版本号
            .setLeader(updatedLeaderAndIsr.leader) // 副本Leader ID
            .setLeaderEpoch(updatedLeaderAndIsr.leaderEpoch) // 副本Leader 版本号
            .setIsr(updatedLeaderAndIsr.isr.map(Integer.valueOf).asJava) // ISR列表
            .setZkVersion(updatedLeaderAndIsr.zkVersion) // ZK版本号
            .setReplicas(replicas.map(Integer.valueOf).asJava) // 副本列表
            .setOfflineReplicas(offlineReplicas.map(Integer.valueOf).asJava) // 离线副本列表

          // 将对象放入到Map集合中，待调用
          updateMetadataRequestPartitionInfoMap.put(partition, partitionStateInfo)

        // 本地缓存中找不到分区信息，说明还没有选举Leader副本，那么就跳过发送「UpdateMetadataRequest」
        case None =>
          info(s"Leader not yet assigned for partition $partition. Skip sending UpdateMetadataRequest.")
      }
    }

    // 本轮需要发送「updateMetadataRequest」的Broker有哪些
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)

    // 遍历分区，构造请求体，然后将它放入到updateMetadataRequestPartitionInfoMap集合中
    partitions.foreach(partition => updateMetadataRequestPartitionInfo(partition,
      beingDeleted = controllerContext.topicsToBeDeleted.contains(partition.topic)))
  }

  /**
   * 将在集合 {@link AbstractControllerBrokerRequestBatch.updateMetadataRequestBrokerSet}
   * 的请求真正发送给目标Broker节点
   *
   * @param controllerEpoch controller版本号
   * @param stateChangeLog
   */
  private def sendLeaderAndIsrRequest(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    // 版本号
    val leaderAndIsrRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_8_IV1) 5
      else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 4
      else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV0) 3
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_1_0_IV0) 1
      else 0

    //
    leaderAndIsrRequestMap.forKeyValue { (broker, leaderAndIsrPartitionStates) =>
      // 确认broker是否在线，只有要线的Broker才可以向该节点发送请求
      if (controllerContext.liveOrShuttingDownBrokerIds.contains(broker)) {
        // 记录Leader ID，目的是构建LeaderAndIsrRequest时，只对分区进行一次迭代操作
        val leaderIds = mutable.Set.empty[Int]
        var numBecomeLeaders = 0

        // 遍历待发送的分区的状态
        leaderAndIsrPartitionStates.forKeyValue { (topicPartition, state) =>
          leaderIds += state.leader
          // 判断请求类型，到底当前副本是成为Leader副本还是Follower副本
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

        // 获取Leader所在的Broker信息
        val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.node(config.interBrokerListenerName)
        }

        // broker版本号
        val brokerEpoch = controllerContext.liveBrokerIdAndEpochs(broker)

        // <topicName, TopicId>
        val topicIds = leaderAndIsrPartitionStates.keys
          .map(_.topic)
          .toSet[String]
          .map(topic => (topic, controllerContext.topicIds.getOrElse(topic, Uuid.ZERO_UUID)))
          .toMap

        // LeaderAndIsr请求构造器，
        val leaderAndIsrRequestBuilder = new LeaderAndIsrRequest.Builder(
          leaderAndIsrRequestVersion,
          controllerId,
          controllerEpoch,
          brokerEpoch,
          leaderAndIsrPartitionStates.values.toBuffer.asJava,
          topicIds.asJava,
          leaders.asJava)
        // 发送请求
        sendRequest(broker, leaderAndIsrRequestBuilder, (r: AbstractResponse) => {
          // 回调方法
          val leaderAndIsrResponse = r.asInstanceOf[LeaderAndIsrResponse]
          // 向ControllerEventManager注册「LeaderAndIsrResponseReceived」事件，这个事件会处理收到的响应（异步处理）
          sendEvent(LeaderAndIsrResponseReceived(leaderAndIsrResponse, broker))
        })
      }
    }
    leaderAndIsrRequestMap.clear()
  }

  /**
   *
   * @param controllerEpoch
   * @param stateChangeLog
   */
  private def sendUpdateMetadataRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    stateChangeLog.info(s"Sending UpdateMetadata request to brokers $updateMetadataRequestBrokerSet " +
      s"for ${updateMetadataRequestPartitionInfoMap.size} partitions")

    val partitionStates = updateMetadataRequestPartitionInfoMap.values.toBuffer
    val updateMetadataRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_8_IV1) 7
      else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 6
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 5
      else if (config.interBrokerProtocolVersion >= KAFKA_1_0_IV0) 4
      else if (config.interBrokerProtocolVersion >= KAFKA_0_10_2_IV0) 3
      else if (config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
      else 0

    val liveBrokers = controllerContext.liveOrShuttingDownBrokers.iterator.map { broker =>
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

    updateMetadataRequestBrokerSet.intersect(controllerContext.liveOrShuttingDownBrokerIds).foreach { broker =>
      val brokerEpoch = controllerContext.liveBrokerIdAndEpochs(broker)
      val topicIds = partitionStates.map(_.topicName())
        .distinct
        .filter(controllerContext.topicIds.contains)
        .map(topic => (topic, controllerContext.topicIds(topic))).toMap
      val updateMetadataRequestBuilder = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion,
        controllerId, controllerEpoch, brokerEpoch, partitionStates.asJava, liveBrokers.asJava, topicIds.asJava)
      sendRequest(broker, updateMetadataRequestBuilder, (r: AbstractResponse) => {
        val updateMetadataResponse = r.asInstanceOf[UpdateMetadataResponse]
        sendEvent(UpdateMetadataResponseReceived(updateMetadataResponse, broker))
      })

    }
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }

  /**
   *
   * @param controllerEpoch
   * @param stateChangeLog
   */
  private def sendStopReplicaRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    val traceEnabled = stateChangeLog.isTraceEnabled
    val stopReplicaRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_6_IV0) 3
      else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 1
      else 0

    def responseCallback(brokerId: Int, isPartitionDeleted: TopicPartition => Boolean)
                        (response: AbstractResponse): Unit = {
      val stopReplicaResponse = response.asInstanceOf[StopReplicaResponse]
      val partitionErrorsForDeletingTopics = mutable.Map.empty[TopicPartition, Errors]
      stopReplicaResponse.partitionErrors.forEach { pe =>
        val tp = new TopicPartition(pe.topicName, pe.partitionIndex)
        if (controllerContext.isTopicDeletionInProgress(pe.topicName) &&
          isPartitionDeleted(tp)) {
          partitionErrorsForDeletingTopics += tp -> Errors.forCode(pe.errorCode)
        }
      }
      if (partitionErrorsForDeletingTopics.nonEmpty)
        sendEvent(TopicDeletionStopReplicaResponseReceived(brokerId, stopReplicaResponse.error,
          partitionErrorsForDeletingTopics))
    }

    stopReplicaRequestMap.forKeyValue { (brokerId, partitionStates) =>
      if (controllerContext.liveOrShuttingDownBrokerIds.contains(brokerId)) {
        if (traceEnabled)
          partitionStates.forKeyValue { (topicPartition, partitionState) =>
            stateChangeLog.trace(s"Sending StopReplica request $partitionState to " +
              s"broker $brokerId for partition $topicPartition")
          }

        val brokerEpoch = controllerContext.liveBrokerIdAndEpochs(brokerId)
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
            false, stopReplicaTopicState.values.toBuffer.asJava)
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
              true, topicStatesWithDelete.values.toBuffer.asJava)
            sendRequest(brokerId, stopReplicaRequestBuilder, responseCallback(brokerId, _ => true))
          }

          if (topicStatesWithoutDelete.nonEmpty) {
            stateChangeLog.info(s"Sending StopReplica request (delete = false) for " +
              s"$numPartitionStateWithoutDelete replicas to broker $brokerId")
            val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
              stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
              false, topicStatesWithoutDelete.values.toBuffer.asJava)
            sendRequest(brokerId, stopReplicaRequestBuilder)
          }
        }
      }
    }

    stopReplicaRequestMap.clear()
  }

  /**
   * 发送请求
   *
   * @param controllerEpoch controller版本号
   */
  def sendRequestsToBrokers(controllerEpoch: Int): Unit = {
    try {
      val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerEpoch)

      // #1 发送「LeaderAndIsr」请求
      sendLeaderAndIsrRequest(controllerEpoch, stateChangeLog)

      // #2 发送「UpdateMetadata」请求
      sendUpdateMetadataRequests(controllerEpoch, stateChangeLog)

      // #3 发送「StopReplica」请求
      sendStopReplicaRequests(controllerEpoch, stateChangeLog)
    } catch {
      // 出现异常，输出日志，抛出异常
      case e: Throwable =>
        if (leaderAndIsrRequestMap.nonEmpty) {
          error("Haven't been able to send leader and isr requests, current state of " + s"the map is $leaderAndIsrRequestMap. Exception message: $e")
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
 * POJO类
 *
 * @param networkClient                网络层I/O操作对象，提供方便的API用于发送请求/接收响应
 * @param brokerNode                   Broker节点信息
 * @param messageQueue                 阻塞的消息队列，存储待发送给Broker的请求对象
 * @param requestSendThread            发送线程。Controller与每个Broker都建立一条TCP连接，这个
 * @param queueSizeGauge               监控相关
 * @param requestRateAndTimeMetrics    监控相关
 * @param reconfigurableChannelBuilder 支持动态更改配置的类
 */
case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node,
                                     messageQueue: BlockingQueue[QueueItem],
                                     requestSendThread: RequestSendThread,
                                     queueSizeGauge: Gauge[Int],
                                     requestRateAndTimeMetrics: Timer,
                                     reconfigurableChannelBuilder: Option[Reconfigurable])

