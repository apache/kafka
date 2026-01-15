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
import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.atomic._
import kafka.network.connectionQuotas
import kafka.network.Processor._
import kafka.network.RequestChannel.{CloseConnectionResponse, EndThrottlingResponse, NoOpResponse, SendResponse, StartThrottlingResponse}
import kafka.network.SocketServer._
import kafka.server.{BrokerReconfigurable, KafkaConfig}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import kafka.utils._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.{InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.{Avg, CumulativeSum, Meter, Rate}
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteEvent
import org.apache.kafka.common.network.{ChannelBuilder, ChannelBuilders, ClientInformation, KafkaChannel, ListenerName, ListenerReconfigurable, NetworkSend, Selectable, Send, ServerConnectionId, Selector => KSelector}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiVersionsRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time, Utils}
import org.apache.kafka.common.{Endpoint, KafkaException, MetricName, Reconfigurable}
import org.apache.kafka.network.{ConnectionQuotaEntity, ConnectionThrottledException, SocketServerConfigs, TooManyConnectionsException}
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.{ApiVersionManager, ServerSocketFactory}
import org.apache.kafka.server.config.QuotaConfig
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.network.ConnectionDisconnectListener
import org.apache.kafka.server.quota.QuotaUtils
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
 */
class SocketServer(
  val config: KafkaConfig,
  val metrics: Metrics,
  val time: Time,
  val credentialProvider: CredentialProvider,
  val apiVersionManager: ApiVersionManager,
  val socketFactory: ServerSocketFactory = ServerSocketFactory.INSTANCE,
  val connectionDisconnectListeners: Seq[ConnectionDisconnectListener] = Seq.empty
) extends Logging with BrokerReconfigurable {
  // Changing the package or class name may cause incompatibility with existing code and metrics configuration
  private val metricsPackage = "kafka.network"
  private val metricsClassName = "SocketServer"
  private val metricsGroup = new KafkaMetricsGroup(metricsPackage, metricsClassName)

  private val maxQueuedRequests = config.queuedMaxRequests

  protected val nodeId: Int = config.brokerId

  private val logContext = new LogContext(s"[SocketServer listenerType=${apiVersionManager.listenerType}, nodeId=$nodeId] ")

  this.logIdent = logContext.logPrefix

  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", MetricsGroup)
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", MetricsGroup)
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))
  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE
  // data-plane
  private[network] val dataPlaneAcceptors = new ConcurrentHashMap[Endpoint, DataPlaneAcceptor]()
  val dataPlaneRequestChannel = new RequestChannel(maxQueuedRequests, time, apiVersionManager.newRequestMetrics)

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
  metricsGroup.newGauge(s"NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
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

  metricsGroup.newGauge("MemoryPoolAvailable", () => memoryPool.availableMemory)
  metricsGroup.newGauge("MemoryPoolUsed", () => memoryPool.size() - memoryPool.availableMemory)
  metricsGroup.newGauge(s"ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
    val dataPlaneProcessors = dataPlaneAcceptors.asScala.values.flatMap(a => a.processors)
    val expiredConnectionsKilledCountMetricNames = dataPlaneProcessors.map { p =>
      metrics.metricName("expired-connections-killed-count", MetricsGroup, p.metricTags)
    }
    expiredConnectionsKilledCountMetricNames.map { metricName =>
      Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
    }.sum
  })

  // Create acceptors and processors for the statically configured endpoints when the
  // SocketServer is constructed. Note that this just opens the ports and creates the data
  // structures. It does not start the acceptors and processors or their associated JVM
  // threads.
  if (apiVersionManager.listenerType.equals(ListenerType.CONTROLLER)) {
    config.controllerListeners.foreach(createDataPlaneAcceptorAndProcessors)
  } else {
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
   * @param authorizerFutures     Future per [[Endpoint]] used to wait before starting the
   *                              processor corresponding to the [[Endpoint]]. Any endpoint
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
        case (endpoint, _) => acceptor.endPoint.listener.equals(endpoint.listener())
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
    dataPlaneAcceptors.values().forEach(chainAcceptorFuture)
    FutureUtils.chainFuture(CompletableFuture.allOf(authorizerFutures.values.toArray: _*),
        allAuthorizerFuturesComplete)

    // Construct a future that will be completed when all Acceptors have been successfully started.
    // Alternately, if any of them fail to start, this future will be completed exceptionally.
    val enableFuture = new CompletableFuture[Void]
    FutureUtils.chainFuture(CompletableFuture.allOf(dataPlaneAcceptors.values().asScala.toArray.map(_.startedFuture): _*), enableFuture)
    enableFuture
  }

  private def createDataPlaneAcceptorAndProcessors(endpoint: Endpoint): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("Can't create new data plane acceptor and processors: SocketServer is stopped.")
    }
    val listenerName =  ListenerName.normalised(endpoint.listener)
    val parsedConfigs = config.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
    connectionQuotas.addListener(config, listenerName)
    val isPrivilegedListener = config.interBrokerListenerName == listenerName
    val dataPlaneAcceptor = createDataPlaneAcceptor(endpoint, isPrivilegedListener, dataPlaneRequestChannel)
    config.addReconfigurable(dataPlaneAcceptor)
    dataPlaneAcceptor.configure(parsedConfigs)
    dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
    info(s"Created data-plane acceptor and processors for endpoint : ${listenerName}")
  }

  private def endpoints = config.listeners.map(l => ListenerName.normalised(l.listener) -> l).toMap

  protected def createDataPlaneAcceptor(endPoint: Endpoint, isPrivilegedListener: Boolean, requestChannel: RequestChannel): DataPlaneAcceptor = {
    new DataPlaneAcceptor(this, endPoint, config, nodeId, connectionQuotas, time, isPrivilegedListener, requestChannel, metrics, credentialProvider, logContext, memoryPool, apiVersionManager)
  }

  /**
   * Stop processing requests and new connections.
   */
  def stopProcessingRequests(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      info("Stopping socket server request processors")
      dataPlaneAcceptors.asScala.values.foreach(_.beginShutdown())
      dataPlaneAcceptors.asScala.values.foreach(_.close())
      dataPlaneRequestChannel.clear()
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
        throw new KafkaException("Could not find listenerName : " + listenerName + " in data-plane.")
      }
    } catch {
      case e: Exception =>
        throw new KafkaException("Tried to check for port of non-existing protocol", e)
    }
  }

  /**
   * This method is called to dynamically add listeners.
   */
  def addListeners(listenersAdded: Seq[Endpoint]): Unit = synchronized {
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

  def removeListeners(listenersRemoved: Seq[Endpoint]): Unit = synchronized {
    info(s"Removing data-plane listeners for endpoints $listenersRemoved")
    listenersRemoved.foreach { endpoint =>
      connectionQuotas.removeListener(config, ListenerName.normalised(endpoint.listener))
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
      if (endPoint.listener == listenerName)
        return Some(acceptor)
    }
    None
  }
}

object SocketServer {
  val MetricsGroup = "socket-server-metrics"

  val ReconfigurableConfigs: Set[String] = Set(
    SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG,
    SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG,
    SocketServerConfigs.MAX_CONNECTIONS_CONFIG,
    SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG)

  val ListenerReconfigurableConfigs: Set[String] = Set(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG)

  def closeSocket(channel: SocketChannel): Unit = {
    Utils.closeQuietly(channel.socket, "channel socket")
    Utils.closeQuietly(channel, "channel")
  }
}

object DataPlaneAcceptor {
  val ListenerReconfigurableConfigs: Set[String] = Set(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG)
}

class DataPlaneAcceptor(socketServer: SocketServer,
                        endPoint: Endpoint,
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

  /**
   * Returns the listener name associated with this reconfigurable. Listener-specific
   * configs corresponding to this listener name are provided for reconfiguration.
   */
  override def listenerName(): ListenerName = ListenerName.normalised(endPoint.listener)

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
      if (reconfigurableConfigs().contains(k)) {
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
   * [[org.apache.kafka.common.Configurable#configure( Map )]]. This method will only be invoked if
   * the configs have passed validation using [[validateReconfiguration( Map )]].
   */
  override def reconfigure(configs: util.Map[String, _]): Unit = {
    val newNumNetworkThreads = configs.get(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG).asInstanceOf[Int]

    if (newNumNetworkThreads != processors.length) {
      info(s"Resizing network thread pool size for ${endPoint.listener} listener from ${processors.length} to $newNumNetworkThreads")
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
    addProcessors(configs.get(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG).asInstanceOf[Int])
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] abstract class Acceptor(val socketServer: SocketServer,
                                       val endPoint: Endpoint,
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
  val shouldRun = new AtomicBoolean(true)

  private val sendBufferSize = config.socketSendBufferBytes
  private val recvBufferSize = config.socketReceiveBufferBytes
  private val listenBacklogSize = config.socketListenBacklogSize

  private val nioSelector = NSelector.open()

  // If the port is configured as 0, we are using a wildcard port, so we need to open the socket
  // before we can find out what port we have. If it is set to a nonzero value, defer opening
  // the socket until we start the Acceptor. The reason for deferring the socket opening is so
  // that systems which assume that the socket being open indicates readiness are not confused.
  private[network] var serverChannel: ServerSocketChannel  = _
  private[network] val localPort: Int  = if (endPoint.port != 0) {
    endPoint.port
  } else {
    serverChannel = openServerSocket(endPoint.host, endPoint.port, listenBacklogSize)
    val newPort = serverChannel.socket().getLocalPort
    info(s"Opened wildcard endpoint ${endPoint.host}:$newPort")
    newPort
  }

  private[network] val processors = new ArrayBuffer[Processor]()
  // Build the metric name explicitly in order to keep the existing name for compatibility
  private val backwardCompatibilityMetricGroup = new KafkaMetricsGroup("kafka.network", "Acceptor")
  private val blockedPercentMeterMetricName = backwardCompatibilityMetricGroup.metricName(
    "AcceptorBlockedPercent",
    Map(ListenerMetricTag -> endPoint.listener).asJava)
  private val blockedPercentMeter = backwardCompatibilityMetricGroup.newMeter(blockedPercentMeterMetricName,"blocked time", TimeUnit.NANOSECONDS)
  private var currentProcessorIndex = 0
  private[network] val throttledSockets = new mutable.PriorityQueue[DelayedCloseSocket]()
  private val started = new AtomicBoolean()
  private[network] val startedFuture = new CompletableFuture[Void]()

  val thread: KafkaThread = KafkaThread.nonDaemon(
    s"data-plane-kafka-socket-acceptor-${endPoint.listener}-${endPoint.securityProtocol}-${endPoint.port}",
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
      debug(s"Starting processors for listener ${endPoint.listener}")
      processors.foreach(_.start())
      debug(s"Starting acceptor thread for listener ${endPoint.listener}")
      thread.start()
      startedFuture.complete(null)
      started.set(true)
    } catch {
      case e: ClosedChannelException =>
        debug(s"Refusing to start acceptor for ${endPoint.listener} since the acceptor has already been shut down.")
        startedFuture.completeExceptionally(e)
      case t: Throwable =>
        error(s"Unable to start acceptor for ${endPoint.listener}", t)
        startedFuture.completeExceptionally(new RuntimeException(s"Unable to start acceptor for ${endPoint.listener}", t))
    }
  }

  private[network] case class DelayedCloseSocket(socket: SocketChannel, endThrottleTimeMs: Long) extends Ordered[DelayedCloseSocket] {
    override def compare(that: DelayedCloseSocket): Int = endThrottleTimeMs compare that.endThrottleTimeMs
  }

  private[network] def removeProcessors(removeCount: Int): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)
    toRemove.foreach(_.close())
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
    if (!started.get) {
      closeAll()
    }
    synchronized {
      processors.foreach(_.close())
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   */
  override def run(): Unit = {
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
    } finally {
      closeAll()
    }
  }

  private def closeAll(): Unit = {
    debug("Closing server socket, selector, and any throttled sockets.")
    // The serverChannel will be null if Acceptor's thread is not started
    Utils.closeQuietly(serverChannel, "Acceptor serverChannel")
    Utils.closeQuietly(nioSelector, "Acceptor nioSelector")
    throttledSockets.foreach(throttledSocket => closeSocket(throttledSocket.socket))
    throttledSockets.clear()
  }

  /**
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int, listenBacklogSize: Int): ServerSocketChannel = {
    val socketAddress = if (Utils.isBlank(host)) {
      new InetSocketAddress(port)
    } else {
      new InetSocketAddress(host, port)
    }
    val serverChannel = socketServer.socketFactory.openServerSocket(
      endPoint.listener,
      socketAddress,
      listenBacklogSize,
      recvBufferSize)
    info(s"Awaiting socket connections on ${socketAddress.getHostString}:${serverChannel.socket.getLocalPort}.")
    serverChannel
  }

  /**
   * Listen for new connections and assign accepted connections to processors using round-robin.
   */
  private def acceptNewConnections(): Unit = {
    val ready = nioSelector.select(500)
    if (ready > 0) {
      val keys = nioSelector.selectedKeys()
      val iter = keys.iterator()
      while (iter.hasNext && shouldRun.get()) {
        try {
          val key = iter.next
          iter.remove()

          if (key.isAcceptable) {
            accept(key).foreach { socketChannel =>
              // Assign the channel to the next processor (using round-robin) to which the
              // channel can be added without blocking. If newConnections queue is full on
              // all processors, block until the last one is able to accept a connection.
              var retriesLeft = synchronized(processors.length)
              var processor: Processor = null
              do {
                retriesLeft -= 1
                processor = synchronized {
                  // adjust the index (if necessary) and retrieve the processor atomically for
                  // correct behaviour in case the number of processors is reduced dynamically
                  currentProcessorIndex = currentProcessorIndex % processors.length
                  processors(currentProcessorIndex)
                }
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
    val listenerName = ListenerName.normalised(endPoint.listener)
    try {
      connectionQuotas.inc(listenerName, socketChannel.socket.getInetAddress, blockedPercentMeter)
      configureAcceptedSocketChannel(socketChannel)
      Some(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info(s"Rejected connection from ${e.ip}, address already has the configured maximum of ${e.count} connections.")
        connectionQuotas.closeChannel(this, listenerName, socketChannel)
        None
      case e: ConnectionThrottledException =>
        val ip = socketChannel.socket.getInetAddress
        debug(s"Delaying closing of connection from $ip for ${e.throttleTimeMs} ms")
        val endThrottleTimeMs = e.startThrottleTimeMs + e.throttleTimeMs
        throttledSockets += DelayedCloseSocket(socketChannel, endThrottleTimeMs)
        None
      case e: IOException =>
        error(s"Encountered an error while configuring the connection, closing it.", e)
        connectionQuotas.closeChannel(this, listenerName, socketChannel)
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
      closeSocket(closingSocket.socket)
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
    val listenerName = ListenerName.normalised(endPoint.listener)
    val securityProtocol = endPoint.securityProtocol
    val listenerProcessors = new ArrayBuffer[Processor]()

    for (_ <- 0 until toCreate) {
      val processor = newProcessor(socketServer.nextProcessorId(), listenerName, securityProtocol, socketServer.connectionDisconnectListeners)
      listenerProcessors += processor
      requestChannel.addProcessor(processor)

      if (started.get) {
        processor.start()
      }
    }
    processors ++= listenerProcessors
  }

  def newProcessor(id: Int,
                   listenerName: ListenerName,
                   securityProtocol: SecurityProtocol,
                   connectionDisconnectListeners: Seq[ConnectionDisconnectListener]): Processor = {
    val name = s"data-plane-kafka-network-thread-$nodeId-${endPoint.listener}-${endPoint.securityProtocol}-$id"
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
                  name,
                  connectionDisconnectListeners)
  }
}

private[kafka] object Processor {
  private val IdlePercentMetricName = "IdlePercent"
  val NetworkProcessorMetricTag = "networkProcessor"
  val ListenerMetricTag = "listener"
  val ConnectionQueueSize = 20

  private[network] def parseRequestHeader(apiVersionManager: ApiVersionManager, buffer: ByteBuffer): RequestHeader = {
    val header = RequestHeader.parse(buffer)
    if (apiVersionManager.isApiEnabled(header.apiKey, header.apiVersion)) {
      header
    } else if (header.isApiVersionSupported()) {
      throw new InvalidRequestException(s"Received request for disabled api with key ${header.apiKey.id} (${header.apiKey().name}) and version ${header.apiVersion}")
    } else {
      throw new UnsupportedVersionException(s"Received request for api with key ${header.apiKey.id} (${header.apiKey().name}) and unsupported version ${header.apiVersion}")
    }
  }
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
  threadName: String,
  connectionDisconnectListeners: Seq[ConnectionDisconnectListener]
) extends Runnable with Logging {
  private val metricsPackage = "kafka.network"
  private val metricsClassName = "Processor"
  private val metricsGroup = new KafkaMetricsGroup(metricsPackage, metricsClassName)

  val shouldRun: AtomicBoolean = new AtomicBoolean(true)
  private val started: AtomicBoolean = new AtomicBoolean()

  val thread: KafkaThread = KafkaThread.nonDaemon(threadName, this)

  private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
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

  private val expiredConnectionsKilledCount = new CumulativeSum()
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
      version => apiVersionManager.apiVersionResponse(0, version < 4)
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

  override def run(): Unit = {
    try {
      while (shouldRun.get()) {
        try {
          // setup any new connections that have been queued up
          configureNewConnections()
          // register any new responses for writing
          processNewResponses()
          poll()
          processCompletedReceives()
          processCompletedSends()
          processDisconnected()
          closeExcessConnections()
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
    } finally {
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

  private def processNewResponses(): Unit = {
    var currentResponse: RequestChannel.Response = null
    while ({currentResponse = dequeueResponse(); currentResponse != null}) {
      val channelId = currentResponse.request.context.connectionId
      try {
        currentResponse match {
          case response: NoOpResponse =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            updateRequestMetrics(response)
            trace(s"Socket server received empty response to send, registering for read: $response")
            // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
            // it will be unmuted immediately. If the channel has been throttled, it will be unmuted only if the
            // throttling delay has already passed by now.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
            tryUnmuteChannel(channelId)

          case response: SendResponse =>
            sendResponse(response, response.responseSend)
          case response: CloseConnectionResponse =>
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
    if (openOrClosingChannel(connectionId).isDefined) {
      selector.send(new NetworkSend(connectionId, responseSend))
      inflightResponses += (connectionId -> response)
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

  private def processCompletedReceives(): Unit = {
    selector.completedReceives.forEach { receive =>
      try {
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            val header = parseRequestHeader(apiVersionManager, receive.payload)
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
                val context = new RequestContext(header, connectionId, channel.socketAddress, Optional.of(channel.socketPort()),
                  channel.principal, listenerName, securityProtocol, channel.channelMetadataRegistry.clientInformation,
                  isPrivilegedListener, channel.principalSerde)

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
        val remoteHost = ServerConnectionId.fromString(connectionId).orElseThrow { () =>
          throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
        }.remoteHost
        inflightResponses.remove(connectionId).foreach(updateRequestMetrics)
        // the channel has been closed by the selector but the quotas still need to be updated
        connectionQuotas.dec(listenerName, InetAddress.getByName(remoteHost))
        // Call listeners to notify for closed connection.
        connectionDisconnectListeners.foreach(listener => CoreUtils.swallow(() -> listener.onDisconnect(connectionId), this, Level.ERROR))
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
      // Call listeners to notify for closed connection.
      connectionDisconnectListeners.foreach(listener => CoreUtils.swallow(() -> listener.onDisconnect(connectionId), this, Level.ERROR))

      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel,
             mayBlock: Boolean,
             acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter): Boolean = {
    val accepted = {
      if (newConnections.offer(socketChannel))
        true
      else if (mayBlock) {
        val startNs = time.nanoseconds
        newConnections.put(socketChannel)
        acceptorBlockedPercentMeter.mark(time.nanoseconds() - startNs)
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
   */
  private def configureNewConnections(): Unit = {
    var connectionsProcessed = 0
    while (connectionsProcessed < connectionQueueSize && !newConnections.isEmpty) {
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        selector.register(connectionId(channel.socket), channel)
        connectionsProcessed += 1
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
    val connId = ServerConnectionId.generateConnectionId(socket, id, nextConnectionIndex)
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

  def start(): Unit = {
    if (!started.getAndSet(true)) {
      thread.start()
    }
  }

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
      if (!started.get) {
        CoreUtils.swallow(closeAll(), this, Level.ERROR)
      }
    } finally {
      metricsGroup.removeMetric("IdlePercent", Map("networkProcessor" -> id.toString).asJava)
      metrics.removeMetric(expiredConnectionsKilledCountMetricName)
    }
  }
}