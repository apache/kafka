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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.{Selector => NSelector, _}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.network.ConnectionQuotas._
import kafka.network.Processor._
import kafka.network.RequestChannel.{CloseConnectionResponse, EndThrottlingResponse, NoOpResponse, SendResponse, StartThrottlingResponse}
import kafka.network.SocketServer._
import kafka.security.CredentialProvider
import kafka.server.{ApiVersionManager, BrokerReconfigurable, KafkaConfig}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import kafka.utils._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.{Avg, CumulativeSum, Meter, Rate}
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteEvent
import org.apache.kafka.common.network.{ChannelBuilder, ChannelBuilders, ClientInformation, KafkaChannel, ListenerName, ListenerReconfigurable, NetworkSend, Selectable, Send, Selector => KSelector}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiVersionsRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time, Utils}
import org.apache.kafka.common.{Endpoint, KafkaException, MetricName, Reconfigurable}
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.FutureUtils
import org.slf4j.event.Level

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.ControlThrowable

/**
 * Handles new connections, requests and responses to and from broker.
 * Kafka supports two types of request planes :
 *  - data-plane :
 *    - Handles requests from clients and other brokers in the cluster.
 *    - The threading model is
 *      1 Acceptor thread per listener, that handles new connections.
 *      It is possible to configure multiple data-planes by specifying multiple "," separated endpoints for "listeners" in KafkaConfig.
 *      Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *      M Handler threads that handle requests and produce responses back to the processor threads for writing.
 *  - control-plane :
 *    - Handles requests from controller. This is optional and can be configured by specifying "control.plane.listener.name".
 *      If not configured, the controller requests are handled by the data-plane.
 *    - The threading model is
 *      1 Acceptor thread that handles new connections
 *      Acceptor has 1 Processor thread that has its own selector and read requests from the socket.
 *      1 Handler thread that handles requests and produces responses back to the processor thread for writing.
 *
 *
 *      SocketServer 组件是核心，主要实现了 Reactor 模式，用于处理外部多个 Clients（这里的 Clients 指的是广义的 Clients，
 *      可能包含 Producer、Consumer 或其他 Broker）的并发请求，并负责将处理结果封装进 Response 中，返还给 Clients。
 *      这个组件是 Kafka 网络通信层中最重要的子模块。它下辖的 Acceptor 线程、Processor 线程和 RequestChannel 等对象，都是实施网络通信的重要组成部分
 *
 *
 */
class SocketServer(val config: KafkaConfig,
                   val metrics: Metrics,
                   val time: Time,
                   val credentialProvider: CredentialProvider,
                   val apiVersionManager: ApiVersionManager)
  extends Logging with BrokerReconfigurable {

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  // SocketServer实现BrokerReconfigurable trait表明SocketServer的一些参数配置是允许动态修改的
  // 即在Broker不停机的情况下修改它们
  // SocketServer的请求队列长度，由Broker端参数queued.max.requests值而定，默认值是500
  private val maxQueuedRequests = config.queuedMaxRequests

  protected val nodeId = config.brokerId

  private val logContext = new LogContext(s"[SocketServer listenerType=${apiVersionManager.listenerType}, nodeId=$nodeId] ")

  this.logIdent = logContext.logPrefix

  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", MetricsGroup)
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", MetricsGroup)
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))
  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE

  // Processor 线程池：网络线程池，负责将请求高速地放入到请求队列中。
  // Acceptor 线程池：保存了 SocketServer 为每个监听器定义的 Acceptor 线程，此线程负责分发该监听器上的入站连接建立请求。
  // RequestChannel：承载请求队列的请求处理通道。
  // data-plane
  // 对于 Data plane 来说，线程池的说法是没有问题的，因为 Processor 线程确实有很多个，而 Acceptor 也可能有多个，因为 SocketServer 会为每个 EndPoint（即每套监听器）创建一个对应的 Acceptor 线程。
  // 处理数据类请求的Acceptor线程池，每套监听器对应一个Acceptor线程
  private[network] val dataPlaneAcceptors = new ConcurrentHashMap[EndPoint, DataPlaneAcceptor]()// 处理数据类请求的Processor线程池

  // 处理数据类请求专属的RequestChannel对象
  val dataPlaneRequestChannel = new RequestChannel(maxQueuedRequests, DataPlaneAcceptor.MetricPrefix, time, apiVersionManager.newRequestMetrics)

  // control-plane
  // 用于处理控制类请求的Processor线程
  // 注意：目前定义了专属的Processor线程而非线程池处理控制类请求
  // Control plane 那组属性变量都是以 Opt 结尾的，即它们都是 Option 类型。
  // 这说明了一个重要的事实：你完全可以不使用 Control plane 套装，即你可以让 Kafka 不区分请求类型，就像 2.2.0 之前设计的那样。
  // 但是，一旦你开启了 Control plane 设置，其 Processor 线程就只有 1 个，Acceptor 线程也是 1 个。
  // 另外，你要注意，它对应的 RequestChannel 里面的请求队列长度被硬编码成了 20，而不是一个可配置的值。
  // 这揭示了社区在这里所做的一个假设：即控制类请求的数量应该远远小于数据类请求，因而不需要为它创建线程池和较深的请求队列。
  private[network] var controlPlaneAcceptorOpt: Option[ControlPlaneAcceptor] = None
  // 处理控制类请求专属的RequestChannel对象
  val controlPlaneRequestChannelOpt: Option[RequestChannel] = config.controlPlaneListenerName.map(_ =>
    new RequestChannel(20, ControlPlaneAcceptor.MetricPrefix, time, apiVersionManager.newRequestMetrics))

  private[this] val nextProcessorId: AtomicInteger = new AtomicInteger(0)
  val connectionQuotas = new ConnectionQuotas(config, time, metrics)

  /**
   * A future which is completed once all the authorizer futures are complete.
   */
  private val allAuthorizerFuturesComplete = new CompletableFuture[Void]

  /**
   * True if the SocketServer is stopped. Must be accessed under the SocketServer lock.
   */
  private var stopped = false

  // Socket server metrics
  metricsGroup.newGauge(s"${DataPlaneAcceptor.MetricPrefix}NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
    val dataPlaneProcessors = dataPlaneAcceptors.asScala.values.flatMap(a => a.processors)
    val ioWaitRatioMetricNames = dataPlaneProcessors.map { p =>
      metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags)
    }
    if (dataPlaneProcessors.isEmpty) {
      1.0
    } else {
      ioWaitRatioMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }.sum / dataPlaneProcessors.size
    }
  })
  if (config.requiresZookeeper) {
    metricsGroup.newGauge(s"${ControlPlaneAcceptor.MetricPrefix}NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
      val controlPlaneProcessorOpt = controlPlaneAcceptorOpt.map(a => a.processors(0))
      val ioWaitRatioMetricName = controlPlaneProcessorOpt.map { p =>
        metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags)
      }
      ioWaitRatioMetricName.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }.getOrElse(Double.NaN)
    })
  }
  metricsGroup.newGauge("MemoryPoolAvailable", () => memoryPool.availableMemory)
  metricsGroup.newGauge("MemoryPoolUsed", () => memoryPool.size() - memoryPool.availableMemory)
  metricsGroup.newGauge(s"${DataPlaneAcceptor.MetricPrefix}ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
    val dataPlaneProcessors = dataPlaneAcceptors.asScala.values.flatMap(a => a.processors)
    val expiredConnectionsKilledCountMetricNames = dataPlaneProcessors.map { p =>
      metrics.metricName("expired-connections-killed-count", MetricsGroup, p.metricTags)
    }
    expiredConnectionsKilledCountMetricNames.map { metricName =>
      Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
    }.sum
  })
  if (config.requiresZookeeper) {
    metricsGroup.newGauge(s"${ControlPlaneAcceptor.MetricPrefix}ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
      val controlPlaneProcessorOpt = controlPlaneAcceptorOpt.map(a => a.processors(0))
      val expiredConnectionsKilledCountMetricNames = controlPlaneProcessorOpt.map { p =>
        metrics.metricName("expired-connections-killed-count", MetricsGroup, p.metricTags)
      }
      expiredConnectionsKilledCountMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
      }.getOrElse(0.0)
    })
  }

  // Create acceptors and processors for the statically configured endpoints when the
  // SocketServer is constructed. Note that this just opens the ports and creates the data
  // structures. It does not start the acceptors and processors or their associated JVM
  // threads.
  if (apiVersionManager.listenerType.equals(ListenerType.CONTROLLER)) {
    config.controllerListeners.foreach(createDataPlaneAcceptorAndProcessors)
  } else {
    config.controlPlaneListener.foreach(createControlPlaneAcceptorAndProcessor)
    config.dataPlaneListeners.foreach(createDataPlaneAcceptorAndProcessors)
  }

  // Processors are now created by each Acceptor. However to preserve compatibility, we need to number the processors
  // globally, so we keep the nextProcessorId counter in SocketServer
  def nextProcessorId(): Int = {
    nextProcessorId.getAndIncrement()
  }

  /**
   * This method enables request processing for all endpoints managed by this SocketServer. Each
   * endpoint will be brought up asynchronously as soon as its associated future is completed.
   * Therefore, we do not know that any particular request processor will be running by the end of
   * this function -- just that it might be running.
   *
   * @param authorizerFutures     Future per [[EndPoint]] used to wait before starting the
   *                              processor corresponding to the [[EndPoint]]. Any endpoint
   *                              that does not appear in this map will be started once all
   *                              authorizerFutures are complete.
   *
   * @return                      A future which is completed when all of the acceptor threads have
   *                              successfully started. If any of them do not start, the future will
   *                              be completed with an exception.
   */
  def enableRequestProcessing(
    authorizerFutures: Map[Endpoint, CompletableFuture[Void]]
  ): CompletableFuture[Void] = this.synchronized {
    if (stopped) {
      throw new RuntimeException("Can't enable request processing: SocketServer is stopped.")
    }

    def chainAcceptorFuture(acceptor: Acceptor): Unit = {
      // Because of ephemeral ports, we need to match acceptors to futures by looking at
      // the listener name, rather than the endpoint object.
      val authorizerFuture = authorizerFutures.find {
        case (endpoint, _) => acceptor.endPoint.listenerName.value().equals(endpoint.listenerName().get())
      } match {
        case None => allAuthorizerFuturesComplete
        case Some((_, future)) => future
      }
      authorizerFuture.whenComplete((_, e) => {
        if (e != null) {
          // If the authorizer failed to start, fail the acceptor's startedFuture.
          acceptor.startedFuture.completeExceptionally(e)
        } else {
          // Once the authorizer has started, attempt to start the associated acceptor. The Acceptor.start()
          // function will complete the acceptor started future (either successfully or not)
          acceptor.start()
        }
      })
    }

    info("Enabling request processing.")
    controlPlaneAcceptorOpt.foreach(chainAcceptorFuture)
    dataPlaneAcceptors.values().forEach(chainAcceptorFuture)
    FutureUtils.chainFuture(CompletableFuture.allOf(authorizerFutures.values.toArray: _*),
        allAuthorizerFuturesComplete)

    // Construct a future that will be completed when all Acceptors have been successfully started.
    // Alternately, if any of them fail to start, this future will be completed exceptionally.
    val allAcceptors = dataPlaneAcceptors.values().asScala.toSeq ++ controlPlaneAcceptorOpt
    val enableFuture = new CompletableFuture[Void]
    FutureUtils.chainFuture(CompletableFuture.allOf(allAcceptors.map(_.startedFuture).toArray: _*), enableFuture)
    enableFuture
  }

  /**
   * 方法会遍历你配置的所有监听器，然后为每个监听器执行下面的逻辑。
   * 1、初始化该监听器对应的最大连接数计数器。后续这些计数器将被用来确保没有配额超限的情形发生。
   * 2、为该监听器创建 Acceptor 线程，也就是调用 Acceptor 类的构造函数，生成对应的 Acceptor 线程实例。
   * 3、创建 Processor 线程池。对于 Data plane 而言，线程池的数量由 Broker 端参数 num.network.threads 决定。
   * 4、将 < 监听器，Acceptor 线程 > 对加入到 Acceptor 线程池统一管理。
   * 切记，源码会为每套用于 Data plane 的监听器执行以上这 4 步。
   * @param endpoint
   */
  def createDataPlaneAcceptorAndProcessors(endpoint: EndPoint): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("Can't create new data plane acceptor and processors: SocketServer is stopped.")
    }
    val parsedConfigs = config.valuesFromThisConfigWithPrefixOverride(endpoint.listenerName.configPrefix)
    connectionQuotas.addListener(config, endpoint.listenerName)
    val isPrivilegedListener = controlPlaneRequestChannelOpt.isEmpty &&
      config.interBrokerListenerName == endpoint.listenerName
    val dataPlaneAcceptor = createDataPlaneAcceptor(endpoint, isPrivilegedListener, dataPlaneRequestChannel)
    config.addReconfigurable(dataPlaneAcceptor)
    dataPlaneAcceptor.configure(parsedConfigs)
    dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
    info(s"Created data-plane acceptor and processors for endpoint : ${endpoint.listenerName}")
  }

  /**
   * 基于控制类请求的负载远远小于数据类请求负载的假设，
   * Control plane 的配套资源只有 1 个 Acceptor 线程 + 1 个 Processor 线程 + 1 个深度是 20 的请求队列而已。
   * @param endpoint
   */
  private def createControlPlaneAcceptorAndProcessor(endpoint: EndPoint): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("Can't create new control plane acceptor and processor: SocketServer is stopped.")
    }
    // 如果为Control plane配置了监听器，将监听器纳入到连接配额管理之下
    connectionQuotas.addListener(config, endpoint.listenerName)
    // 为监听器创建对应的Acceptor线程
    val controlPlaneAcceptor = createControlPlaneAcceptor(endpoint, controlPlaneRequestChannelOpt.get)
    // 将Processor线程添加到控制类请求专属RequestChannel中
    // 即添加到RequestChannel实例保存的Processor线程池中
    // 把Processor对象也添加到Acceptor线程管理的Processor线程池中
    controlPlaneAcceptor.addProcessors(1)
    controlPlaneAcceptorOpt = Some(controlPlaneAcceptor)
    info(s"Created control-plane acceptor and processor for endpoint : ${endpoint.listenerName}")
  }

  private def endpoints = config.listeners.map(l => l.listenerName -> l).toMap

  protected def createDataPlaneAcceptor(endPoint: EndPoint, isPrivilegedListener: Boolean, requestChannel: RequestChannel): DataPlaneAcceptor = {
    new DataPlaneAcceptor(this, endPoint, config, nodeId, connectionQuotas, time, isPrivilegedListener, requestChannel, metrics, credentialProvider, logContext, memoryPool, apiVersionManager)
  }

  private def createControlPlaneAcceptor(endPoint: EndPoint, requestChannel: RequestChannel): ControlPlaneAcceptor = {
    new ControlPlaneAcceptor(this, endPoint, config, nodeId, connectionQuotas, time, requestChannel, metrics, credentialProvider, logContext, memoryPool, apiVersionManager)
  }

  /**
   * Stop processing requests and new connections.
   */
  def stopProcessingRequests(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      info("Stopping socket server request processors")
      dataPlaneAcceptors.asScala.values.foreach(_.beginShutdown())
      controlPlaneAcceptorOpt.foreach(_.beginShutdown())
      dataPlaneAcceptors.asScala.values.foreach(_.close())
      controlPlaneAcceptorOpt.foreach(_.close())
      dataPlaneRequestChannel.clear()
      controlPlaneRequestChannelOpt.foreach(_.clear())
      info("Stopped socket server request processors")
    }
  }

  /**
   * Shutdown the socket server. If still processing requests, shutdown
   * acceptors and processors first.
   */
  def shutdown(): Unit = {
    info("Shutting down socket server")
    allAuthorizerFuturesComplete.completeExceptionally(new TimeoutException("The socket " +
      "server was shut down before the Authorizer could be completely initialized."))
    this.synchronized {
      stopProcessingRequests()
      dataPlaneRequestChannel.shutdown()
      controlPlaneRequestChannelOpt.foreach(_.shutdown())
      connectionQuotas.close()
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      val acceptor = dataPlaneAcceptors.get(endpoints(listenerName))
      if (acceptor != null) {
        acceptor.localPort
      } else {
        controlPlaneAcceptorOpt.map(_.localPort).getOrElse(throw new KafkaException("Could not find listenerName : " + listenerName + " in data-plane or control-plane"))
      }
    } catch {
      case e: Exception =>
        throw new KafkaException("Tried to check for port of non-existing protocol", e)
    }
  }

  /**
   * This method is called to dynamically add listeners.
   */
  def addListeners(listenersAdded: Seq[EndPoint]): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("can't add new listeners: SocketServer is stopped.")
    }
    info(s"Adding data-plane listeners for endpoints $listenersAdded")
    listenersAdded.foreach { endpoint =>
      createDataPlaneAcceptorAndProcessors(endpoint)
      val acceptor = dataPlaneAcceptors.get(endpoint)
      // There is no authorizer future for this new listener endpoint. So start the
      // listener once all authorizer futures are complete.
      allAuthorizerFuturesComplete.whenComplete((_, e) => {
        if (e != null) {
          acceptor.startedFuture.completeExceptionally(e)
        } else {
          acceptor.start()
        }
      })
    }
  }

  def removeListeners(listenersRemoved: Seq[EndPoint]): Unit = synchronized {
    info(s"Removing data-plane listeners for endpoints $listenersRemoved")
    listenersRemoved.foreach { endpoint =>
      connectionQuotas.removeListener(config, endpoint.listenerName)
      dataPlaneAcceptors.asScala.remove(endpoint).foreach { acceptor =>
        acceptor.beginShutdown()
        acceptor.close()
        config.removeReconfigurable(acceptor)
      }
    }
  }

  override def reconfigurableConfigs: Set[String] = SocketServer.ReconfigurableConfigs

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {

  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val maxConnectionsPerIp = newConfig.maxConnectionsPerIp
    if (maxConnectionsPerIp != oldConfig.maxConnectionsPerIp) {
      info(s"Updating maxConnectionsPerIp: $maxConnectionsPerIp")
      connectionQuotas.updateMaxConnectionsPerIp(maxConnectionsPerIp)
    }
    val maxConnectionsPerIpOverrides = newConfig.maxConnectionsPerIpOverrides
    if (maxConnectionsPerIpOverrides != oldConfig.maxConnectionsPerIpOverrides) {
      info(s"Updating maxConnectionsPerIpOverrides: ${maxConnectionsPerIpOverrides.map { case (k, v) => s"$k=$v" }.mkString(",")}")
      connectionQuotas.updateMaxConnectionsPerIpOverride(maxConnectionsPerIpOverrides)
    }
    val maxConnections = newConfig.maxConnections
    if (maxConnections != oldConfig.maxConnections) {
      info(s"Updating broker-wide maxConnections: $maxConnections")
      connectionQuotas.updateBrokerMaxConnections(maxConnections)
    }
    val maxConnectionRate = newConfig.maxConnectionCreationRate
    if (maxConnectionRate != oldConfig.maxConnectionCreationRate) {
      info(s"Updating broker-wide maxConnectionCreationRate: $maxConnectionRate")
      connectionQuotas.updateBrokerMaxConnectionRate(maxConnectionRate)
    }
  }

  // For test usage
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  // For test usage
  def dataPlaneAcceptor(listenerName: String): Option[DataPlaneAcceptor] = {
    dataPlaneAcceptors.asScala.foreach { case (endPoint, acceptor) =>
      if (endPoint.listenerName.value() == listenerName)
        return Some(acceptor)
    }
    None
  }
}

object SocketServer {
  val MetricsGroup = "socket-server-metrics"

  //Broker 端参数 max.connections.per.ip、max.connections.per.ip.overrides 和 max.connections 是可以动态修改的。
  val ReconfigurableConfigs = Set(
    KafkaConfig.MaxConnectionsPerIpProp,
    KafkaConfig.MaxConnectionsPerIpOverridesProp,
    KafkaConfig.MaxConnectionsProp,
    KafkaConfig.MaxConnectionCreationRateProp)

  val ListenerReconfigurableConfigs = Set(KafkaConfig.MaxConnectionsProp, KafkaConfig.MaxConnectionCreationRateProp)

  def closeSocket(
    channel: SocketChannel,
    logging: Logging
  ): Unit = {
    CoreUtils.swallow(channel.socket().close(), logging, Level.ERROR)
    CoreUtils.swallow(channel.close(), logging, Level.ERROR)
  }
}

object DataPlaneAcceptor {
  val ThreadPrefix = "data-plane"
  val MetricPrefix = ""
  val ListenerReconfigurableConfigs = Set(KafkaConfig.NumNetworkThreadsProp)
}

class DataPlaneAcceptor(socketServer: SocketServer,
                        endPoint: EndPoint,
                        config: KafkaConfig,
                        nodeId: Int,
                        connectionQuotas: ConnectionQuotas,
                        time: Time,
                        isPrivilegedListener: Boolean,
                        requestChannel: RequestChannel,
                        metrics: Metrics,
                        credentialProvider: CredentialProvider,
                        logContext: LogContext,
                        memoryPool: MemoryPool,
                        apiVersionManager: ApiVersionManager)
  extends Acceptor(socketServer,
                   endPoint,
                   config,
                   nodeId,
                   connectionQuotas,
                   time,
                   isPrivilegedListener,
                   requestChannel,
                   metrics,
                   credentialProvider,
                   logContext,
                   memoryPool,
                   apiVersionManager) with ListenerReconfigurable {

  override def metricPrefix(): String = DataPlaneAcceptor.MetricPrefix
  override def threadPrefix(): String = DataPlaneAcceptor.ThreadPrefix

  /**
   * Returns the listener name associated with this reconfigurable. Listener-specific
   * configs corresponding to this listener name are provided for reconfiguration.
   */
  override def listenerName(): ListenerName = endPoint.listenerName

  /**
   * Returns the names of configs that may be reconfigured.
   */
  override def reconfigurableConfigs(): util.Set[String] = DataPlaneAcceptor.ListenerReconfigurableConfigs.asJava


  /**
   * Validates the provided configuration. The provided map contains
   * all configs including any reconfigurable configs that may be different
   * from the initial configuration. Reconfiguration will be not performed
   * if this method throws any exception.
   *
   * @throws ConfigException if the provided configs are not valid. The exception
   *                         message from ConfigException will be returned to the client in
   *                         the AlterConfigs response.
   */
  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    configs.forEach { (k, v) =>
      if (reconfigurableConfigs.contains(k)) {
        val newValue = v.asInstanceOf[Int]
        val oldValue = processors.length
        if (newValue != oldValue) {
          val errorMsg = s"Dynamic thread count update validation failed for $k=$v"
          if (newValue <= 0)
            throw new ConfigException(s"$errorMsg, value should be at least 1")
          if (newValue < oldValue / 2)
            throw new ConfigException(s"$errorMsg, value should be at least half the current value $oldValue")
          if (newValue > oldValue * 2)
            throw new ConfigException(s"$errorMsg, value should not be greater than double the current value $oldValue")
        }
      }
    }
  }

  /**
   * Reconfigures this instance with the given key-value pairs. The provided
   * map contains all configs including any reconfigurable configs that
   * may have changed since the object was initially configured using
   * {@link Configurable# configure ( Map )}. This method will only be invoked if
   * the configs have passed validation using {@link #validateReconfiguration ( Map )}.
   */
  override def reconfigure(configs: util.Map[String, _]): Unit = {
    val newNumNetworkThreads = configs.get(KafkaConfig.NumNetworkThreadsProp).asInstanceOf[Int]

    if (newNumNetworkThreads != processors.length) {
      info(s"Resizing network thread pool size for ${endPoint.listenerName} listener from ${processors.length} to $newNumNetworkThreads")
      if (newNumNetworkThreads > processors.length) {
        addProcessors(newNumNetworkThreads - processors.length)
      } else if (newNumNetworkThreads < processors.length) {
        removeProcessors(processors.length - newNumNetworkThreads)
      }
    }
  }

  /**
   * Configure this class with the given key-value pairs
   */
  override def configure(configs: util.Map[String, _]): Unit = {
    addProcessors(configs.get(KafkaConfig.NumNetworkThreadsProp).asInstanceOf[Int])
  }
}

//Controller 与 Broker 交互的请求类型有 3 种：LeaderAndIsrRequest、StopReplicaRequest 和 UpdateMetadataRequest。
// 这 3 类请求属于控制类请求，通常应该被赋予高优先级。
object ControlPlaneAcceptor {
  val ThreadPrefix = "control-plane"
  val MetricPrefix = "ControlPlane"
}

class ControlPlaneAcceptor(socketServer: SocketServer,
                           endPoint: EndPoint,
                           config: KafkaConfig,
                           nodeId: Int,
                           connectionQuotas: ConnectionQuotas,
                           time: Time,
                           requestChannel: RequestChannel,
                           metrics: Metrics,
                           credentialProvider: CredentialProvider,
                           logContext: LogContext,
                           memoryPool: MemoryPool,
                           apiVersionManager: ApiVersionManager)
  extends Acceptor(socketServer,
                   endPoint,
                   config,
                   nodeId,
                   connectionQuotas,
                   time,
                   true,
                   requestChannel,
                   metrics,
                   credentialProvider,
                   logContext,
                   memoryPool,
                   apiVersionManager) {

  override def metricPrefix(): String = ControlPlaneAcceptor.MetricPrefix
  override def threadPrefix(): String = ControlPlaneAcceptor.ThreadPrefix

  def processorOpt(): Option[Processor] = {
    if (processors.isEmpty)
      None
    else
      Some(processors.apply(0))
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 * endPoint。它就是你定义的 Kafka Broker 连接信息，比如 PLAINTEXT://localhost:9092。Acceptor 需要用到 endPoint 包含的主机名和端口信息创建 Server Socket。
 * sendBufferSize。它设置的是 SocketOptions 的 SO_SNDBUF，即用于设置出站（Outbound）网络 I/O 的底层缓冲区大小。该值默认是 Broker 端参数 socket.send.buffer.bytes 的值，即 100KB。
 * recvBufferSize。它设置的是 SocketOptions 的 SO_RCVBUF，即用于设置入站（Inbound）网络 I/O 的底层缓冲区大小。该值默认是 Broker 端参数 socket.receive.buffer.bytes 的值，即 100KB。
 */
private[kafka] abstract class Acceptor(val socketServer: SocketServer,
                                       val endPoint: EndPoint,//定义的 Kafka Broker 连接信息
                                       var config: KafkaConfig,
                                       nodeId: Int,
                                       val connectionQuotas: ConnectionQuotas,
                                       time: Time,
                                       isPrivilegedListener: Boolean,
                                       requestChannel: RequestChannel,
                                       metrics: Metrics,
                                       credentialProvider: CredentialProvider,
                                       logContext: LogContext,
                                       memoryPool: MemoryPool,
                                       apiVersionManager: ApiVersionManager)
  extends Runnable with Logging {

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val shouldRun = new AtomicBoolean(true)

  def metricPrefix(): String
  def threadPrefix(): String

  //如果在你的生产环境中，Clients 与 Broker 的通信网络延迟很大（比如 RTT>10ms），建议你调大控制缓冲区大小的两个参数，
  // 也就是 sendBufferSize 和 recvBufferSize。通常来说，默认值 100KB 太小了。
  private val sendBufferSize = config.socketSendBufferBytes //它设置的是 SocketOptions 的 SO_SNDBUF，即用于设置出站（Outbound）网络 I/O 的底层缓冲区大小
  private val recvBufferSize = config.socketReceiveBufferBytes //它设置的是 SocketOptions 的 SO_RCVBUF，即用于设置入站（Inbound）网络 I/O 的底层缓冲区大小。
  private val listenBacklogSize = config.socketListenBacklogSize

  // 创建底层的NIO Selector对象
  // Selector对象负责执行底层实际I/O操作，如监听连接创建请求、读写请求等
  private val nioSelector = NSelector.open()

  // If the port is configured as 0, we are using a wildcard port, so we need to open the socket
  // before we can find out what port we have. If it is set to a nonzero value, defer opening
  // the socket until we start the Acceptor. The reason for deferring the socket opening is so
  // that systems which assume that the socket being open indicates readiness are not confused.
  // Broker端创建对应的ServerSocketChannel实例
  // 后续把该Channel向上一步的Selector对象注册
  private[network] var serverChannel: ServerSocketChannel  = _
  private[network] val localPort: Int  = if (endPoint.port != 0) {
    endPoint.port
  } else {
    serverChannel = openServerSocket(endPoint.host, endPoint.port, listenBacklogSize)
    val newPort = serverChannel.socket().getLocalPort()
    info(s"Opened wildcard endpoint ${endPoint.host}:${newPort}")
    newPort
  }

  // 创建Processor线程池，实际上是Processor线程数组
  private[network] val processors = new ArrayBuffer[Processor]()
  // Build the metric name explicitly in order to keep the existing name for compatibility
  private val blockedPercentMeterMetricName = KafkaMetricsGroup.explicitMetricName(
    "kafka.network",
    "Acceptor",
    s"${metricPrefix()}AcceptorBlockedPercent",
    Map(ListenerMetricTag -> endPoint.listenerName.value).asJava)
  private val blockedPercentMeter = metricsGroup.newMeter(blockedPercentMeterMetricName,"blocked time", TimeUnit.NANOSECONDS)
  private var currentProcessorIndex = 0
  private[network] val throttledSockets = new mutable.PriorityQueue[DelayedCloseSocket]()
  private var started = false
  private[network] val startedFuture = new CompletableFuture[Void]()

  // 线程命名规范：processor线程前缀-kafka-network-thread-broker序号-监听器名称-安全协议-Processor序号
  // 假设为序号为0的Broker设置PLAINTEXT://localhost:9092作为连接信息，那么3个Processor线程名称分别为：
  // data-plane-kafka-network-thread-0-ListenerName(PLAINTEXT)-PLAINTEXT-0
  // data-plane-kafka-network-thread-0-ListenerName(PLAINTEXT)-PLAINTEXT-1
  // data-plane-kafka-network-thread-0-ListenerName(PLAINTEXT)-PLAINTEXT-2
  val thread = KafkaThread.nonDaemon(
    s"${threadPrefix()}-kafka-socket-acceptor-${endPoint.listenerName}-${endPoint.securityProtocol}-${endPoint.port}",
    this)

  def start(): Unit = synchronized {
    try {
      if (!shouldRun.get()) {
        throw new ClosedChannelException()
      }
      if (serverChannel == null) {
        serverChannel = openServerSocket(endPoint.host, endPoint.port, listenBacklogSize)
        debug(s"Opened endpoint ${endPoint.host}:${endPoint.port}")
      }
      debug(s"Starting processors for listener ${endPoint.listenerName}")
      processors.foreach(_.start())
      debug(s"Starting acceptor thread for listener ${endPoint.listenerName}")
      thread.start()
      startedFuture.complete(null)
      started = true
    } catch {
      case e: ClosedChannelException =>
        debug(s"Refusing to start acceptor for ${endPoint.listenerName} since the acceptor has already been shut down.")
        startedFuture.completeExceptionally(e)
      case t: Throwable =>
        error(s"Unable to start acceptor for ${endPoint.listenerName}", t)
        startedFuture.completeExceptionally(new RuntimeException(s"Unable to start acceptor for ${endPoint.listenerName}", t))
    }
  }

  private[network] case class DelayedCloseSocket(socket: SocketChannel, endThrottleTimeMs: Long) extends Ordered[DelayedCloseSocket] {
    override def compare(that: DelayedCloseSocket): Int = endThrottleTimeMs compare that.endThrottleTimeMs
  }

  private[network] def removeProcessors(removeCount: Int): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    // 获取Processor线程池中最后removeCount个线程
    val toRemove = processors.takeRight(removeCount)
    // 移除最后removeCount个线程
    processors.remove(processors.size - removeCount, removeCount)
    // 关闭最后removeCount个线程
    toRemove.foreach(_.close())
    // 在RequestChannel中移除这些Processor
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  def beginShutdown(): Unit = {
    if (shouldRun.getAndSet(false)) {
      wakeup()
      synchronized {
        processors.foreach(_.beginShutdown())
      }
    }
  }

  def close(): Unit = {
    beginShutdown()
    thread.join()
    synchronized {
      processors.foreach(_.close())
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   * 这里的关键点在于，Acceptor 线程会先为每个入站请求确定要处理它的 Processor 线程，
   * 然后调用 assignNewConnection 方法令 Processor 线程创建与发送方的连接。
   * 基本上，Acceptor 线程使用 Java NIO 的 Selector + SocketChannel 的方式循环地轮询准备就绪的 I/O 事件。
   * 这里的 I/O 事件，主要是指网络连接创建事件，即代码中的 SelectionKey.OP_ACCEPT。
   * 一旦接收到外部连接请求，Acceptor 就会指定一个 Processor 线程，并将该请求交由它，让它创建真正的网络连接。
   * 总的来说，Acceptor 线程就做这么点事。
   */
  override def run(): Unit = {
    //注册OP_ACCEPT事件
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    try {
      while (shouldRun.get()) {
        try {
          acceptNewConnections()
          closeThrottledConnections()
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {// 执行各种资源关闭逻辑
      debug("Closing server socket, selector, and any throttled sockets.")
      CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
      CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)
      throttledSockets.foreach(throttledSocket => closeSocket(throttledSocket.socket, this))
      throttledSockets.clear()
    }
  }

  /**
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int, listenBacklogSize: Int): ServerSocketChannel = {
    val socketAddress =
      if (Utils.isBlank(host))
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      serverChannel.socket.bind(socketAddress, listenBacklogSize)
      info(s"Awaiting socket connections on ${socketAddress.getHostString}:${serverChannel.socket.getLocalPort}.")
    } catch {
      case e: SocketException =>
        throw new KafkaException(s"Socket server failed to bind to ${socketAddress.getHostString}:$port: ${e.getMessage}.", e)
    }
    serverChannel
  }

  /**
   * Listen for new connections and assign accepted connections to processors using round-robin.
   */
  private def acceptNewConnections(): Unit = {
    // 每500毫秒获取一次就绪I/O事件
    val ready = nioSelector.select(500)
    if (ready > 0) {// 如果有I/O事件准备就绪
      val keys = nioSelector.selectedKeys()
      val iter = keys.iterator()
      while (iter.hasNext && shouldRun.get()) {
        try {
          val key = iter.next
          iter.remove()

          if (key.isAcceptable) {
            // 调用accept方法创建Socket连接
            accept(key).foreach { socketChannel =>
              // Assign the channel to the next processor (using round-robin) to which the
              // channel can be added without blocking. If newConnections queue is full on
              // all processors, block until the last one is able to accept a connection.
              var retriesLeft = synchronized(processors.length)
              var processor: Processor = null
              do {
                retriesLeft -= 1
                // 指定由哪个Processor线程进行处理
                processor = synchronized {
                  // adjust the index (if necessary) and retrieve the processor atomically for
                  // correct behaviour in case the number of processors is reduced dynamically
                  currentProcessorIndex = currentProcessorIndex % processors.length
                  processors(currentProcessorIndex)
                }
                // 更新Processor线程序号
                currentProcessorIndex += 1
              } while (!assignNewConnection(socketChannel, processor, retriesLeft == 0))
            }
          } else
            throw new IllegalStateException("Unrecognized key state for acceptor thread.")
        } catch {
          case e: Throwable => error("Error while accepting connection", e)
        }
      }
    }
  }

  /**
   * Accept a new connection
   */
  private def accept(key: SelectionKey): Option[SocketChannel] = {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      connectionQuotas.inc(endPoint.listenerName, socketChannel.socket.getInetAddress, blockedPercentMeter)
      configureAcceptedSocketChannel(socketChannel)
      Some(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info(s"Rejected connection from ${e.ip}, address already has the configured maximum of ${e.count} connections.")
        connectionQuotas.closeChannel(this, endPoint.listenerName, socketChannel)
        None
      case e: ConnectionThrottledException =>
        val ip = socketChannel.socket.getInetAddress
        debug(s"Delaying closing of connection from $ip for ${e.throttleTimeMs} ms")
        val endThrottleTimeMs = e.startThrottleTimeMs + e.throttleTimeMs
        throttledSockets += DelayedCloseSocket(socketChannel, endThrottleTimeMs)
        None
      case e: IOException =>
        error(s"Encountered an error while configuring the connection, closing it.", e)
        connectionQuotas.closeChannel(this, endPoint.listenerName, socketChannel)
        None
    }
  }

  protected def configureAcceptedSocketChannel(socketChannel: SocketChannel): Unit = {
    socketChannel.configureBlocking(false)
    socketChannel.socket().setTcpNoDelay(true)
    socketChannel.socket().setKeepAlive(true)
    if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      socketChannel.socket().setSendBufferSize(sendBufferSize)
  }

  /**
   * Close sockets for any connections that have been throttled.
   */
  private def closeThrottledConnections(): Unit = {
    val timeMs = time.milliseconds
    while (throttledSockets.headOption.exists(_.endThrottleTimeMs < timeMs)) {
      val closingSocket = throttledSockets.dequeue()
      debug(s"Closing socket from ip ${closingSocket.socket.getRemoteAddress}")
      closeSocket(closingSocket.socket, this)
    }
  }

  private def assignNewConnection(socketChannel: SocketChannel, processor: Processor, mayBlock: Boolean): Boolean = {
    if (processor.accept(socketChannel, mayBlock, blockedPercentMeter)) {
      debug(s"Accepted connection from ${socketChannel.socket.getRemoteSocketAddress} on" +
        s" ${socketChannel.socket.getLocalSocketAddress} and assigned it to processor ${processor.id}," +
        s" sendBufferSize [actual|requested]: [${socketChannel.socket.getSendBufferSize}|$sendBufferSize]" +
        s" recvBufferSize [actual|requested]: [${socketChannel.socket.getReceiveBufferSize}|$recvBufferSize]")
      true
    } else
      false
  }

  /**
   * Wakeup the thread for selection.
   */
  def wakeup(): Unit = nioSelector.wakeup()

  def addProcessors(toCreate: Int): Unit = synchronized {
    val listenerName = endPoint.listenerName
    val securityProtocol = endPoint.securityProtocol
    val listenerProcessors = new ArrayBuffer[Processor]()

    for (_ <- 0 until toCreate) {
      val processor = newProcessor(socketServer.nextProcessorId(), listenerName, securityProtocol)
      listenerProcessors += processor
      requestChannel.addProcessor(processor) // 添加一组新的Processor线程

      if (started) {// 如果Processor线程池已经启动
        processor.start()// 启动新的Processor线程
      }
    }
    processors ++= listenerProcessors
  }

  def newProcessor(id: Int, listenerName: ListenerName, securityProtocol: SecurityProtocol): Processor = {
    val name = s"${threadPrefix()}-kafka-network-thread-$nodeId-${endPoint.listenerName}-${endPoint.securityProtocol}-${id}"
    new Processor(id,
                  time,
                  config.socketRequestMaxBytes,
                  requestChannel,
                  connectionQuotas,
                  config.connectionsMaxIdleMs,
                  config.failedAuthenticationDelayMs,
                  listenerName,
                  securityProtocol,
                  config,
                  metrics,
                  credentialProvider,
                  memoryPool,
                  logContext,
                  Processor.ConnectionQueueSize,
                  isPrivilegedListener,
                  apiVersionManager,
                  name)
  }
}

private[kafka] object Processor {
  val IdlePercentMetricName = "IdlePercent"
  val NetworkProcessorMetricTag = "networkProcessor"
  val ListenerMetricTag = "listener"
  val ConnectionQueueSize = 20
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 *
 * @param isPrivilegedListener The privileged listener flag is used as one factor to determine whether
 *                             a certain request is forwarded or not. When the control plane is defined,
 *                             the control plane processor would be fellow broker's choice for sending
 *                             forwarding requests; if the control plane is not defined, the processor
 *                             relying on the inter broker listener would be acting as the privileged listener.
 *
 * 如果说 Acceptor 是做入站连接处理的，那么，Processor 代码则是真正创建连接以及分发请求的地方。
 */
private[kafka] class Processor(
  val id: Int,
  time: Time,
  maxRequestSize: Int,
  requestChannel: RequestChannel,
  connectionQuotas: ConnectionQuotas,
  connectionsMaxIdleMs: Long,
  failedAuthenticationDelayMs: Int,
  listenerName: ListenerName,
  securityProtocol: SecurityProtocol,
  config: KafkaConfig,
  metrics: Metrics,
  credentialProvider: CredentialProvider,
  memoryPool: MemoryPool,
  logContext: LogContext,
  connectionQueueSize: Int,
  isPrivilegedListener: Boolean,
  apiVersionManager: ApiVersionManager,
  threadName: String
) extends Runnable with Logging {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val shouldRun = new AtomicBoolean(true)

  val thread = KafkaThread.nonDaemon(threadName, this)

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote, index) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort, Integer.parseInt(index))
        }
      }
      case _ => None
    }
  }

  private[network] case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int, index: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort-$index"
  }

  /**
   * 它保存的是要创建的新连接信息，具体来说，就是 SocketChannel 对象。这是一个默认上限是 20 的队列，而且，目前代码中硬编码了队列的长度，因此，你无法变更这个队列的长度。
   * 每当 Processor 线程接收新的连接请求时，都会将对应的 SocketChannel 放入这个队列。后面在创建连接时（也就是调用 configureNewConnections 时），
   * 就从该队列中取出 SocketChannel，然后注册新的连接。
   */
  private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)

  /**
   * 严格来说，这是一个临时 Response 队列。当 Processor 线程将 Response 返还给 Request 发送方之后，还要将 Response 放入这个临时队列。
   * 为什么需要这个临时队列呢？这是因为，有些 Response 回调逻辑要在 Response 被发送回发送方之后，才能执行，因此需要暂存在一个临时队列里面。
   * 这就是 inflightResponses 存在的意义。
   */
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()

  /**
  * 看名字我们就可以知道，这是 Response 队列，而不是 Request 队列。
   * 这告诉了我们一个事实：每个 Processor 线程都会维护自己的 Response 队列，而不是像网上的某些文章说的，Response 队列是线程共享的或是保存在 RequestChannel 中的。
   * Response 队列里面保存着需要被返还给发送方的所有 Response 对象。
  */
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()

  private[kafka] val metricTags = mutable.LinkedHashMap(
    ListenerMetricTag -> listenerName.value,
    NetworkProcessorMetricTag -> id.toString
  ).asJava

  metricsGroup.newGauge(IdlePercentMetricName, () => {
    Option(metrics.metric(metrics.metricName("io-wait-ratio", MetricsGroup, metricTags))).fold(0.0)(m =>
      Math.min(m.metricValue.asInstanceOf[Double], 1.0))
  },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map(NetworkProcessorMetricTag -> id.toString).asJava
  )

  val expiredConnectionsKilledCount = new CumulativeSum()
  private val expiredConnectionsKilledCountMetricName = metrics.metricName("expired-connections-killed-count", MetricsGroup, metricTags)
  metrics.addMetric(expiredConnectionsKilledCountMetricName, expiredConnectionsKilledCount)

  private[network] val selector = createSelector(
    ChannelBuilders.serverChannelBuilder(
      listenerName,
      listenerName == config.interBrokerListenerName,
      securityProtocol,
      config,
      credentialProvider.credentialCache,
      credentialProvider.tokenCache,
      time,
      logContext,
      () => apiVersionManager.apiVersionResponse(throttleTimeMs = 0)
    )
  )

  // Visible to override for testing
  protected[network] def createSelector(channelBuilder: ChannelBuilder): KSelector = {
    channelBuilder match {
      case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
      case _ =>
    }
    new KSelector(
      maxRequestSize,
      connectionsMaxIdleMs,
      failedAuthenticationDelayMs,
      metrics,
      time,
      "socket-server",
      metricTags,
      false,
      true,
      channelBuilder,
      memoryPool,
      logContext)
  }

  // Connection ids have the format `localAddr:localPort-remoteAddr:remotePort-index`. The index is a
  // non-negative incrementing value that ensures that even if remotePort is reused after a connection is
  // closed, connection ids are not reused while requests from the closed connection are being processed.
  private var nextConnectionIndex = 0

  // 等待Processor线程启动完成
  override def run(): Unit = {
    try {
      while (shouldRun.get()) {
        try {
          // setup any new connections that have been queued up
          configureNewConnections() // 创建新连接
          // register any new responses for writing
          processNewResponses()// 发送Response，并将Response放入到inflightResponses临时队列
          poll()// 执行NIO poll，获取对应SocketChannel上准备就绪的I/O操作
          processCompletedReceives()// 将接收到的Request放入Request队列
          processCompletedSends()// 为临时Response队列中的Response执行回调逻辑
          processDisconnected()// 处理因发送失败而导致的连接断开
          closeExcessConnections()// 关闭超过配额限制部分的连接
        } catch {
          // We catch all the throwables here to prevent the processor thread from exiting. We do this because
          // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
          // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
          // be either associated with a specific socket channel or a bad request. These exceptions are caught and
          // processed by the individual methods above which close the failing channel and continue processing other
          // channels. So this catch block should only ever see ControlThrowables.
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {// 关闭底层资源
      debug(s"Closing selector - processor $id")
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
    }
  }

  private[network] def processException(errorMessage: String, throwable: Throwable): Unit = {
    throwable match {
      case e: ControlThrowable => throw e
      case e => error(errorMessage, e)
    }
  }

  private def processChannelException(channelId: String, errorMessage: String, throwable: Throwable): Unit = {
    if (openOrClosingChannel(channelId).isDefined) {
      error(s"Closing socket for $channelId because of error", throwable)
      close(channelId)
    }
    processException(errorMessage, throwable)
  }

  // 它负责发送 Response 给 Request 发送方，并且将 Response 放入临时 Response 队列。
  private def processNewResponses(): Unit = {
    var currentResponse: RequestChannel.Response = null
    while ({currentResponse = dequeueResponse(); currentResponse != null}) {// Response队列中存在待处理Response
      val channelId = currentResponse.request.context.connectionId// 获取连接通道ID
      try {
        currentResponse match {
          case response: NoOpResponse => // 无需发送Response
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            updateRequestMetrics(response)
            trace(s"Socket server received empty response to send, registering for read: $response")
            // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
            // it will be unmuted immediately. If the channel has been throttled, it will be unmuted only if the
            // throttling delay has already passed by now.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
            tryUnmuteChannel(channelId)

          case response: SendResponse => // 发送Response并将Response放入inflightResponses
            //这里的关键是 SendResponse 分支上的 sendResponse 方法
            sendResponse(response, response.responseSend)
          case response: CloseConnectionResponse => // 关闭对应的连接
            updateRequestMetrics(response)
            trace("Closing socket connection actively according to the response code.")
            close(channelId)
          case _: StartThrottlingResponse =>
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED)
          case _: EndThrottlingResponse =>
            // Try unmuting the channel. The channel will be unmuted only if the response has already been sent out to
            // the client.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_ENDED)
            tryUnmuteChannel(channelId)
          case _ =>
            throw new IllegalArgumentException(s"Unknown response type: ${currentResponse.getClass}")
        }
      } catch {
        case e: Throwable =>
          processChannelException(channelId, s"Exception while processing response for $channelId", e)
      }
    }
  }

  // `protected` for test usage
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send): Unit = {
    val connectionId = response.request.context.connectionId
    trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
    // `channel` can be None if the connection was closed remotely or if selector closed it for being idle for too long
    if (channel(connectionId).isEmpty) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $connectionId")
      response.request.updateRequestMetrics(0L, response)
    }
    // Invoke send for closingChannel as well so that the send is failed and the channel closed properly and
    // removed from the Selector after discarding any pending staged receives.
    // `openOrClosingChannel` can be None if the selector closed the connection because it was idle for too long
    if (openOrClosingChannel(connectionId).isDefined) {// 如果该连接处于可连接状态
      selector.send(new NetworkSend(connectionId, responseSend))// 发送Response
      inflightResponses += (connectionId -> response)// 将Response加入到inflightResponses队列
    }
  }

  private def poll(): Unit = {
    val pollTimeout = if (newConnections.isEmpty) 300 else 0
    try selector.poll(pollTimeout)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        // The exception is not re-thrown and any completed sends/receives/connections/disconnections
        // from this poll will be processed.
        error(s"Processor $id poll failed", e)
    }
  }

  protected def parseRequestHeader(buffer: ByteBuffer): RequestHeader = {
    val header = RequestHeader.parse(buffer)
    if (apiVersionManager.isApiEnabled(header.apiKey, header.apiVersion)) {
      header
    } else {
      throw new InvalidRequestException(s"Received request api key ${header.apiKey} with version ${header.apiVersion} which is not enabled")
    }
  }

  private def processCompletedReceives(): Unit = {
    selector.completedReceives.forEach { receive =>
      try {
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            val header = parseRequestHeader(receive.payload)
            if (header.apiKey == ApiKeys.SASL_HANDSHAKE && channel.maybeBeginServerReauthentication(receive,
              () => time.nanoseconds()))
              trace(s"Begin re-authentication: $channel")
            else {
              val nowNanos = time.nanoseconds()
              if (channel.serverAuthenticationSessionExpired(nowNanos)) {
                // be sure to decrease connection count and drop any in-flight responses
                debug(s"Disconnecting expired channel: $channel : $header")
                close(channel.id)
                expiredConnectionsKilledCount.record(null, 1, 0)
              } else {
                val connectionId = receive.source
                val context = new RequestContext(header, connectionId, channel.socketAddress,
                  channel.principal, listenerName, securityProtocol,
                  channel.channelMetadataRegistry.clientInformation, isPrivilegedListener, channel.principalSerde)

                val req = new RequestChannel.Request(processor = id, context = context,
                  startTimeNanos = nowNanos, memoryPool, receive.payload, requestChannel.metrics, None)

                // KIP-511: ApiVersionsRequest is intercepted here to catch the client software name
                // and version. It is done here to avoid wiring things up to the api layer.
                if (header.apiKey == ApiKeys.API_VERSIONS) {
                  val apiVersionsRequest = req.body[ApiVersionsRequest]
                  if (apiVersionsRequest.isValid) {
                    channel.channelMetadataRegistry.registerClientInformation(new ClientInformation(
                      apiVersionsRequest.data.clientSoftwareName,
                      apiVersionsRequest.data.clientSoftwareVersion))
                  }
                }
                requestChannel.sendRequest(req)
                selector.mute(connectionId)
                handleChannelMuteEvent(connectionId, ChannelMuteEvent.REQUEST_RECEIVED)
              }
            }
          case None =>
            // This should never happen since completed receives are processed immediately after `poll()`
            throw new IllegalStateException(s"Channel ${receive.source} removed from selector before processing completed receive")
        }
      } catch {
        // note that even though we got an exception, we can assume that receive.source is valid.
        // Issues with constructing a valid receive object were handled earlier
        case e: Throwable =>
          processChannelException(receive.source, s"Exception while processing request from ${receive.source}", e)
      }
    }
    selector.clearCompletedReceives()
  }

  private def processCompletedSends(): Unit = {
    selector.completedSends.forEach { send =>
      try {
        val response = inflightResponses.remove(send.destinationId).getOrElse {
          throw new IllegalStateException(s"Send for ${send.destinationId} completed, but not in `inflightResponses`")
        }
        
        // Invoke send completion callback, and then update request metrics since there might be some
        // request metrics got updated during callback
        response.onComplete.foreach(onComplete => onComplete(send))
        updateRequestMetrics(response)

        // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
        // it will be unmuted immediately. If the channel has been throttled, it will unmuted only if the throttling
        // delay has already passed by now.
        handleChannelMuteEvent(send.destinationId, ChannelMuteEvent.RESPONSE_SENT)
        tryUnmuteChannel(send.destinationId)
      } catch {
        case e: Throwable => processChannelException(send.destinationId,
          s"Exception while processing completed send to ${send.destinationId}", e)
      }
    }
    selector.clearCompletedSends()
  }

  private def updateRequestMetrics(response: RequestChannel.Response): Unit = {
    val request = response.request
    val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
    request.updateRequestMetrics(networkThreadTimeNanos, response)
  }

  private def processDisconnected(): Unit = {
    selector.disconnected.keySet.forEach { connectionId =>
      try {
        val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
          throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
        }.remoteHost
        inflightResponses.remove(connectionId).foreach(updateRequestMetrics)
        // the channel has been closed by the selector but the quotas still need to be updated
        connectionQuotas.dec(listenerName, InetAddress.getByName(remoteHost))
      } catch {
        case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
      }
    }
  }

  private def closeExcessConnections(): Unit = {
    if (connectionQuotas.maxConnectionsExceeded(listenerName)) {
      val channel = selector.lowestPriorityChannel()
      if (channel != null)
        close(channel.id)
    }
  }

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   * The channel will be immediately removed from the selector's `channels` or `closingChannels`
   * and no further disconnect notifications will be sent for this channel by the selector.
   * If responses are pending for the channel, they are dropped and metrics is updated.
   * If the channel has already been removed from selector, no action is taken.
   */
  private def close(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach { channel =>
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(listenerName, address)
      selector.close(connectionId)

      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel,
             mayBlock: Boolean,
             acceptorIdlePercentMeter: com.yammer.metrics.core.Meter): Boolean = {
    val accepted = {
      if (newConnections.offer(socketChannel))
        true
      else if (mayBlock) {
        val startNs = time.nanoseconds
        newConnections.put(socketChannel)
        acceptorIdlePercentMeter.mark(time.nanoseconds() - startNs)
        true
      } else
        false
    }
    if (accepted)
      wakeup()
    accepted
  }

  /**
   * Register any new connections that have been queued up. The number of connections processed
   * in each iteration is limited to ensure that traffic and connection close notifications of
   * existing channels are handled promptly.
   * 该方法最重要的逻辑是调用 selector 的 register 来注册 SocketChannel。
   * 每个 Processor 线程都维护了一个 Selector 类实例。
   * Selector 类是社区提供的一个基于 Java NIO Selector 的接口，用于执行非阻塞多通道的网络 I/O 操作。
   * 在核心功能上，Kafka 提供的 Selector 和 Java 提供的是一致的。
   */
  private def configureNewConnections(): Unit = {
    var connectionsProcessed = 0// 当前已配置的连接数计数器
    while (connectionsProcessed < connectionQueueSize && !newConnections.isEmpty) {
      val channel = newConnections.poll()// 从连接队列中取出SocketChannel
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        // 用给定Selector注册该Channel
        // 底层就是调用Java NIO的SocketChannel.register(selector, SelectionKey.OP_READ)
        selector.register(connectionId(channel.socket), channel)
        connectionsProcessed += 1// 更新计数器
      } catch {
        // We explicitly catch all exceptions and close the socket to avoid a socket leak.
        case e: Throwable =>
          val remoteAddress = channel.socket.getRemoteSocketAddress
          // need to close the channel here to avoid a socket leak.
          connectionQuotas.closeChannel(this, listenerName, channel)
          processException(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll(): Unit = {
    while (!newConnections.isEmpty) {
      newConnections.poll().close()
    }
    selector.channels.forEach { channel =>
      close(channel.id)
    }
    selector.close()
    metricsGroup.removeMetric(IdlePercentMetricName, Map(NetworkProcessorMetricTag -> id.toString).asJava)
  }

  // 'protected` to allow override for testing
  protected[network] def connectionId(socket: Socket): String = {
    val localHost = socket.getLocalAddress.getHostAddress
    val localPort = socket.getLocalPort
    val remoteHost = socket.getInetAddress.getHostAddress
    val remotePort = socket.getPort
    val connId = ConnectionId(localHost, localPort, remoteHost, remotePort, nextConnectionIndex).toString
    nextConnectionIndex = if (nextConnectionIndex == Int.MaxValue) 0 else nextConnectionIndex + 1
    connId
  }

  private[network] def enqueueResponse(response: RequestChannel.Response): Unit = {
    responseQueue.put(response)
    wakeup()
  }

  private def dequeueResponse(): RequestChannel.Response = {
    val response = responseQueue.poll()
    if (response != null)
      response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
    response
  }

  private[network] def responseQueueSize = responseQueue.size

  // Only for testing
  private[network] def inflightResponseCount: Int = inflightResponses.size

  // Visible for testing
  // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
  private[network] def openOrClosingChannel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId)).orElse(Option(selector.closingChannel(connectionId)))

  // Indicate the specified channel that the specified channel mute-related event has happened so that it can change its
  // mute state.
  private def handleChannelMuteEvent(connectionId: String, event: ChannelMuteEvent): Unit = {
    openOrClosingChannel(connectionId).foreach(c => c.handleChannelMuteEvent(event))
  }

  private def tryUnmuteChannel(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach(c => selector.unmute(c.id))
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  def start(): Unit = thread.start()

  /**
   * Wakeup the thread for selection.
   */
  def wakeup(): Unit = selector.wakeup()

  def beginShutdown(): Unit = {
    if (shouldRun.getAndSet(false)) {
      wakeup()
    }
  }

  def close(): Unit = {
    try {
      beginShutdown()
      thread.join()
    } finally {
      metricsGroup.removeMetric("IdlePercent", Map("networkProcessor" -> id.toString).asJava)
      metrics.removeMetric(expiredConnectionsKilledCountMetricName)
    }
  }
}

/**
 * Interface for connection quota configuration. Connection quotas can be configured at the
 * broker, listener or IP level.
 */
sealed trait ConnectionQuotaEntity {
  def sensorName: String
  def metricName: String
  def sensorExpiration: Long
  def metricTags: Map[String, String]
}

object ConnectionQuotas {
  private val InactiveSensorExpirationTimeSeconds = TimeUnit.HOURS.toSeconds(1)
  private val ConnectionRateSensorName = "Connection-Accept-Rate"
  private val ConnectionRateMetricName = "connection-accept-rate"
  private val IpMetricTag = "ip"
  private val ListenerThrottlePrefix = ""
  private val IpThrottlePrefix = "ip-"

  private case class ListenerQuotaEntity(listenerName: String) extends ConnectionQuotaEntity {
    override def sensorName: String = s"$ConnectionRateSensorName-$listenerName"
    override def sensorExpiration: Long = Long.MaxValue
    override def metricName: String = ConnectionRateMetricName
    override def metricTags: Map[String, String] = Map(ListenerMetricTag -> listenerName)
  }

  private case object BrokerQuotaEntity extends ConnectionQuotaEntity {
    override def sensorName: String = ConnectionRateSensorName
    override def sensorExpiration: Long = Long.MaxValue
    override def metricName: String = s"broker-$ConnectionRateMetricName"
    override def metricTags: Map[String, String] = Map.empty
  }

  private case class IpQuotaEntity(ip: InetAddress) extends ConnectionQuotaEntity {
    override def sensorName: String = s"$ConnectionRateSensorName-${ip.getHostAddress}"
    override def sensorExpiration: Long = InactiveSensorExpirationTimeSeconds
    override def metricName: String = ConnectionRateMetricName
    override def metricTags: Map[String, String] = Map(IpMetricTag -> ip.getHostAddress)
  }
}

class ConnectionQuotas(config: KafkaConfig, time: Time, metrics: Metrics) extends Logging with AutoCloseable {

  @volatile private var defaultMaxConnectionsPerIp: Int = config.maxConnectionsPerIp
  @volatile private var maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides.map { case (host, count) => (InetAddress.getByName(host), count) }
  @volatile private var brokerMaxConnections = config.maxConnections
  private val interBrokerListenerName = config.interBrokerListenerName
  private val counts = mutable.Map[InetAddress, Int]()

  // Listener counts and configs are synchronized on `counts`
  private val listenerCounts = mutable.Map[ListenerName, Int]()
  private[network] val maxConnectionsPerListener = mutable.Map[ListenerName, ListenerConnectionQuota]()
  @volatile private var totalCount = 0
  // updates to defaultConnectionRatePerIp or connectionRatePerIp must be synchronized on `counts`
  @volatile private var defaultConnectionRatePerIp = QuotaConfigs.IP_CONNECTION_RATE_DEFAULT.intValue()
  private val connectionRatePerIp = new ConcurrentHashMap[InetAddress, Int]()
  // sensor that tracks broker-wide connection creation rate and limit (quota)
  private val brokerConnectionRateSensor = getOrCreateConnectionRateQuotaSensor(config.maxConnectionCreationRate, BrokerQuotaEntity)
  private val maxThrottleTimeMs = TimeUnit.SECONDS.toMillis(config.quotaWindowSizeSeconds.toLong)

  def inc(listenerName: ListenerName, address: InetAddress, acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter): Unit = {
    counts.synchronized {
      waitForConnectionSlot(listenerName, acceptorBlockedPercentMeter)

      recordIpConnectionMaybeThrottle(listenerName, address)
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      totalCount += 1
      if (listenerCounts.contains(listenerName)) {
        listenerCounts.put(listenerName, listenerCounts(listenerName) + 1)
      }
      val max = maxConnectionsPerIpOverrides.getOrElse(address, defaultMaxConnectionsPerIp)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  private[network] def updateMaxConnectionsPerIp(maxConnectionsPerIp: Int): Unit = {
    defaultMaxConnectionsPerIp = maxConnectionsPerIp
  }

  private[network] def updateMaxConnectionsPerIpOverride(overrideQuotas: Map[String, Int]): Unit = {
    maxConnectionsPerIpOverrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  }

  private[network] def updateBrokerMaxConnections(maxConnections: Int): Unit = {
    counts.synchronized {
      brokerMaxConnections = maxConnections
      counts.notifyAll()
    }
  }

  private[network] def updateBrokerMaxConnectionRate(maxConnectionRate: Int): Unit = {
    // if there is a connection waiting on the rate throttle delay, we will let it wait the original delay even if
    // the rate limit increases, because it is just one connection per listener and the code is simpler that way
    updateConnectionRateQuota(maxConnectionRate, BrokerQuotaEntity)
  }

  /**
   * Update the connection rate quota for a given IP and updates quota configs for updated IPs.
   * If an IP is given, metric config will be updated only for the given IP, otherwise
   * all metric configs will be checked and updated if required.
   *
   * @param ip ip to update or default if None
   * @param maxConnectionRate new connection rate, or resets entity to default if None
   */
  def updateIpConnectionRateQuota(ip: Option[InetAddress], maxConnectionRate: Option[Int]): Unit = synchronized {
    def isIpConnectionRateMetric(metricName: MetricName) = {
      metricName.name == ConnectionRateMetricName &&
      metricName.group == MetricsGroup &&
      metricName.tags.containsKey(IpMetricTag)
    }

    def shouldUpdateQuota(metric: KafkaMetric, quotaLimit: Int) = {
      quotaLimit != metric.config.quota.bound
    }

    ip match {
      case Some(address) =>
        // synchronize on counts to ensure reading an IP connection rate quota and creating a quota config is atomic
        counts.synchronized {
          maxConnectionRate match {
            case Some(rate) =>
              info(s"Updating max connection rate override for $address to $rate")
              connectionRatePerIp.put(address, rate)
            case None =>
              info(s"Removing max connection rate override for $address")
              connectionRatePerIp.remove(address)
          }
        }
        updateConnectionRateQuota(connectionRateForIp(address), IpQuotaEntity(address))
      case None =>
        // synchronize on counts to ensure reading an IP connection rate quota and creating a quota config is atomic
        counts.synchronized {
          defaultConnectionRatePerIp = maxConnectionRate.getOrElse(QuotaConfigs.IP_CONNECTION_RATE_DEFAULT.intValue())
        }
        info(s"Updated default max IP connection rate to $defaultConnectionRatePerIp")
        metrics.metrics.forEach { (metricName, metric) =>
          if (isIpConnectionRateMetric(metricName)) {
            val quota = connectionRateForIp(InetAddress.getByName(metricName.tags.get(IpMetricTag)))
            if (shouldUpdateQuota(metric, quota)) {
              debug(s"Updating existing connection rate quota config for ${metricName.tags} to $quota")
              metric.config(rateQuotaMetricConfig(quota))
            }
          }
        }
    }
  }

  // Visible for testing
  def connectionRateForIp(ip: InetAddress): Int = {
    connectionRatePerIp.getOrDefault(ip, defaultConnectionRatePerIp)
  }

  private[network] def addListener(config: KafkaConfig, listenerName: ListenerName): Unit = {
    counts.synchronized {
      if (!maxConnectionsPerListener.contains(listenerName)) {
        val newListenerQuota = new ListenerConnectionQuota(counts, listenerName)
        maxConnectionsPerListener.put(listenerName, newListenerQuota)
        listenerCounts.put(listenerName, 0)
        config.addReconfigurable(newListenerQuota)
        newListenerQuota.configure(config.valuesWithPrefixOverride(listenerName.configPrefix))
      }
      counts.notifyAll()
    }
  }

  private[network] def removeListener(config: KafkaConfig, listenerName: ListenerName): Unit = {
    counts.synchronized {
      maxConnectionsPerListener.remove(listenerName).foreach { listenerQuota =>
        listenerCounts.remove(listenerName)
        // once listener is removed from maxConnectionsPerListener, no metrics will be recorded into listener's sensor
        // so it is safe to remove sensor here
        listenerQuota.close()
        counts.notifyAll() // wake up any waiting acceptors to close cleanly
        config.removeReconfigurable(listenerQuota)
      }
    }
  }

  def dec(listenerName: ListenerName, address: InetAddress): Unit = {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)

      if (totalCount <= 0)
        error(s"Attempted to decrease total connection count for broker with no connections")
      totalCount -= 1

      if (maxConnectionsPerListener.contains(listenerName)) {
        val listenerCount = listenerCounts(listenerName)
        if (listenerCount == 0)
          error(s"Attempted to decrease connection count for listener $listenerName with no connections")
        else
          listenerCounts.put(listenerName, listenerCount - 1)
      }
      counts.notifyAll() // wake up any acceptors waiting to process a new connection since listener connection limit was reached
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

  private def waitForConnectionSlot(listenerName: ListenerName,
                                    acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter): Unit = {
    counts.synchronized {
      val startThrottleTimeMs = time.milliseconds
      val throttleTimeMs = math.max(recordConnectionAndGetThrottleTimeMs(listenerName, startThrottleTimeMs), 0)

      if (throttleTimeMs > 0 || !connectionSlotAvailable(listenerName)) {
        val startNs = time.nanoseconds
        val endThrottleTimeMs = startThrottleTimeMs + throttleTimeMs
        var remainingThrottleTimeMs = throttleTimeMs
        do {
          counts.wait(remainingThrottleTimeMs)
          remainingThrottleTimeMs = math.max(endThrottleTimeMs - time.milliseconds, 0)
        } while (remainingThrottleTimeMs > 0 || !connectionSlotAvailable(listenerName))
        acceptorBlockedPercentMeter.mark(time.nanoseconds - startNs)
      }
    }
  }

  // This is invoked in every poll iteration and we close one LRU connection in an iteration
  // if necessary
  def maxConnectionsExceeded(listenerName: ListenerName): Boolean = {
    totalCount > brokerMaxConnections && !protectedListener(listenerName)
  }

  private def connectionSlotAvailable(listenerName: ListenerName): Boolean = {
    if (listenerCounts(listenerName) >= maxListenerConnections(listenerName))
      false
    else if (protectedListener(listenerName))
      true
    else
      totalCount < brokerMaxConnections
  }

  private def protectedListener(listenerName: ListenerName): Boolean =
    interBrokerListenerName == listenerName && listenerCounts.size > 1

  private def maxListenerConnections(listenerName: ListenerName): Int =
    maxConnectionsPerListener.get(listenerName).map(_.maxConnections).getOrElse(Int.MaxValue)

  /**
   * Calculates the delay needed to bring the observed connection creation rate to listener-level limit or to broker-wide
   * limit, whichever the longest. The delay is capped to the quota window size defined by QuotaWindowSizeSecondsProp
   *
   * @param listenerName listener for which calculate the delay
   * @param timeMs current time in milliseconds
   * @return delay in milliseconds
   */
  private def recordConnectionAndGetThrottleTimeMs(listenerName: ListenerName, timeMs: Long): Long = {
    def recordAndGetListenerThrottleTime(minThrottleTimeMs: Int): Int = {
      maxConnectionsPerListener
        .get(listenerName)
        .map { listenerQuota =>
          val listenerThrottleTimeMs = recordAndGetThrottleTimeMs(listenerQuota.connectionRateSensor, timeMs)
          val throttleTimeMs = math.max(minThrottleTimeMs, listenerThrottleTimeMs)
          // record throttle time due to hitting connection rate quota
          if (throttleTimeMs > 0) {
            listenerQuota.listenerConnectionRateThrottleSensor.record(throttleTimeMs.toDouble, timeMs)
          }
          throttleTimeMs
        }
        .getOrElse(0)
    }

    if (protectedListener(listenerName)) {
      recordAndGetListenerThrottleTime(0)
    } else {
      val brokerThrottleTimeMs = recordAndGetThrottleTimeMs(brokerConnectionRateSensor, timeMs)
      recordAndGetListenerThrottleTime(brokerThrottleTimeMs)
    }
  }

  /**
   * Record IP throttle time on the corresponding listener. To avoid over-recording listener/broker connection rate, we
   * also un-record the listener and broker connection if the IP gets throttled.
   *
   * @param listenerName listener to un-record connection
   * @param throttleMs IP throttle time to record for listener
   * @param timeMs current time in milliseconds
   */
  private def updateListenerMetrics(listenerName: ListenerName, throttleMs: Long, timeMs: Long): Unit = {
    if (!protectedListener(listenerName)) {
      brokerConnectionRateSensor.record(-1.0, timeMs, false)
    }
    maxConnectionsPerListener
      .get(listenerName)
      .foreach { listenerQuota =>
        listenerQuota.ipConnectionRateThrottleSensor.record(throttleMs.toDouble, timeMs)
        listenerQuota.connectionRateSensor.record(-1.0, timeMs, false)
      }
  }

  /**
   * Calculates the delay needed to bring the observed connection creation rate to the IP limit.
   * If the connection would cause an IP quota violation, un-record the connection for both IP,
   * listener, and broker connection rate and throw a ConnectionThrottledException. Calls to
   * this function must be performed with the counts lock to ensure that reading the IP
   * connection rate quota and creating the sensor's metric config is atomic.
   *
   * @param listenerName listener to unrecord connection if throttled
   * @param address ip address to record connection
   */
  private def recordIpConnectionMaybeThrottle(listenerName: ListenerName, address: InetAddress): Unit = {
    val connectionRateQuota = connectionRateForIp(address)
    val quotaEnabled = connectionRateQuota != QuotaConfigs.IP_CONNECTION_RATE_DEFAULT
    if (quotaEnabled) {
      val sensor = getOrCreateConnectionRateQuotaSensor(connectionRateQuota, IpQuotaEntity(address))
      val timeMs = time.milliseconds
      val throttleMs = recordAndGetThrottleTimeMs(sensor, timeMs)
      if (throttleMs > 0) {
        trace(s"Throttling $address for $throttleMs ms")
        // unrecord the connection since we won't accept the connection
        sensor.record(-1.0, timeMs, false)
        updateListenerMetrics(listenerName, throttleMs, timeMs)
        throw new ConnectionThrottledException(address, timeMs, throttleMs)
      }
    }
  }

  /**
   * Records a new connection into a given connection acceptance rate sensor 'sensor' and returns throttle time
   * in milliseconds if quota got violated
   * @param sensor sensor to record connection
   * @param timeMs current time in milliseconds
   * @return throttle time in milliseconds if quota got violated, otherwise 0
   */
  private def recordAndGetThrottleTimeMs(sensor: Sensor, timeMs: Long): Int = {
    try {
      sensor.record(1.0, timeMs)
      0
    } catch {
      case e: QuotaViolationException =>
        val throttleTimeMs = QuotaUtils.boundedThrottleTime(e, maxThrottleTimeMs, timeMs).toInt
        debug(s"Quota violated for sensor (${sensor.name}). Delay time: $throttleTimeMs ms")
        throttleTimeMs
    }
  }

  /**
   * Creates sensor for tracking the connection creation rate and corresponding connection rate quota for a given
   * listener or broker-wide, if listener is not provided.
   * @param quotaLimit connection creation rate quota
   * @param connectionQuotaEntity entity to create the sensor for
   */
  private def getOrCreateConnectionRateQuotaSensor(quotaLimit: Int, connectionQuotaEntity: ConnectionQuotaEntity): Sensor = {
    Option(metrics.getSensor(connectionQuotaEntity.sensorName)).getOrElse {
      val sensor = metrics.sensor(
        connectionQuotaEntity.sensorName,
        rateQuotaMetricConfig(quotaLimit),
        connectionQuotaEntity.sensorExpiration
      )
      sensor.add(connectionRateMetricName(connectionQuotaEntity), new Rate, null)
      sensor
    }
  }

  /**
   * Updates quota configuration for a given connection quota entity
   */
  private def updateConnectionRateQuota(quotaLimit: Int, connectionQuotaEntity: ConnectionQuotaEntity): Unit = {
    Option(metrics.metric(connectionRateMetricName(connectionQuotaEntity))).foreach { metric =>
      metric.config(rateQuotaMetricConfig(quotaLimit))
      info(s"Updated ${connectionQuotaEntity.metricName} max connection creation rate to $quotaLimit")
    }
  }

  private def connectionRateMetricName(connectionQuotaEntity: ConnectionQuotaEntity): MetricName = {
    metrics.metricName(
      connectionQuotaEntity.metricName,
      MetricsGroup,
      s"Tracking rate of accepting new connections (per second)",
      connectionQuotaEntity.metricTags.asJava)
  }

  private def rateQuotaMetricConfig(quotaLimit: Int): MetricConfig = {
    new MetricConfig()
      .timeWindow(config.quotaWindowSizeSeconds.toLong, TimeUnit.SECONDS)
      .samples(config.numQuotaSamples)
      .quota(new Quota(quotaLimit, true))
  }

  def close(): Unit = {
    metrics.removeSensor(brokerConnectionRateSensor.name)
    maxConnectionsPerListener.values.foreach(_.close())
  }

  class ListenerConnectionQuota(lock: Object, listener: ListenerName) extends ListenerReconfigurable with AutoCloseable {
    @volatile private var _maxConnections = Int.MaxValue
    private[network] val connectionRateSensor = getOrCreateConnectionRateQuotaSensor(Int.MaxValue, ListenerQuotaEntity(listener.value))
    private[network] val listenerConnectionRateThrottleSensor = createConnectionRateThrottleSensor(ListenerThrottlePrefix)
    private[network] val ipConnectionRateThrottleSensor = createConnectionRateThrottleSensor(IpThrottlePrefix)

    def maxConnections: Int = _maxConnections

    override def listenerName(): ListenerName = listener

    override def configure(configs: util.Map[String, _]): Unit = {
      _maxConnections = maxConnections(configs)
      updateConnectionRateQuota(maxConnectionCreationRate(configs), ListenerQuotaEntity(listener.value))
    }

    override def reconfigurableConfigs(): util.Set[String] = {
      SocketServer.ListenerReconfigurableConfigs.asJava
    }

    override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
      val value = maxConnections(configs)
      if (value <= 0)
        throw new ConfigException(s"Invalid ${KafkaConfig.MaxConnectionsProp} $value")

      val rate = maxConnectionCreationRate(configs)
      if (rate <= 0)
        throw new ConfigException(s"Invalid ${KafkaConfig.MaxConnectionCreationRateProp} $rate")
    }

    override def reconfigure(configs: util.Map[String, _]): Unit = {
      lock.synchronized {
        _maxConnections = maxConnections(configs)
        updateConnectionRateQuota(maxConnectionCreationRate(configs), ListenerQuotaEntity(listener.value))
        lock.notifyAll()
      }
    }

    def close(): Unit = {
      metrics.removeSensor(connectionRateSensor.name)
      metrics.removeSensor(listenerConnectionRateThrottleSensor.name)
      metrics.removeSensor(ipConnectionRateThrottleSensor.name)
    }

    private def maxConnections(configs: util.Map[String, _]): Int = {
      Option(configs.get(KafkaConfig.MaxConnectionsProp)).map(_.toString.toInt).getOrElse(Int.MaxValue)
    }

    private def maxConnectionCreationRate(configs: util.Map[String, _]): Int = {
      Option(configs.get(KafkaConfig.MaxConnectionCreationRateProp)).map(_.toString.toInt).getOrElse(Int.MaxValue)
    }

    /**
     * Creates sensor for tracking the average throttle time on this listener due to hitting broker/listener connection
     * rate or IP connection rate quota. The average is out of all throttle times > 0, which is consistent with the
     * bandwidth and request quota throttle time metrics.
     */
    private def createConnectionRateThrottleSensor(throttlePrefix: String): Sensor = {
      val sensor = metrics.sensor(s"${throttlePrefix}ConnectionRateThrottleTime-${listener.value}")
      val metricName = metrics.metricName(s"${throttlePrefix}connection-accept-throttle-time",
        MetricsGroup,
        "Tracking average throttle-time, out of non-zero throttle times, per listener",
        Map(ListenerMetricTag -> listener.value).asJava)
      sensor.add(metricName, new Avg)
      sensor
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def closeChannel(log: Logging, listenerName: ListenerName, channel: SocketChannel): Unit = {
    if (channel != null) {
      log.debug(s"Closing connection from ${channel.socket.getRemoteSocketAddress}")
      dec(listenerName, channel.socket.getInetAddress)
      closeSocket(channel, log)
    }
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException(s"Too many connections from $ip (maximum = $count)")

class ConnectionThrottledException(val ip: InetAddress, val startThrottleTimeMs: Long, val throttleTimeMs: Long)
  extends KafkaException(s"$ip throttled for $throttleTimeMs")
