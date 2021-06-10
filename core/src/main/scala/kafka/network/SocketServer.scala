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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.{Selector => NSelector, _}
import java.util
import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.atomic._

import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.metrics.KafkaMetricsGroup
import kafka.network.ConnectionQuotas._
import kafka.network.Processor._
import kafka.network.RequestChannel.{CloseConnectionResponse, EndThrottlingResponse, NoOpResponse, SendResponse, StartThrottlingResponse}
import kafka.network.SocketServer._
import kafka.security.CredentialProvider
import kafka.server.{ApiVersionManager, BrokerReconfigurable, KafkaConfig}
import kafka.utils.Implicits._
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
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}
import org.apache.kafka.common.{Endpoint, KafkaException, MetricName, Reconfigurable}
import org.slf4j.event.Level

import scala.collection._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.jdk.CollectionConverters._
import scala.util.control.ControlThrowable

/**
 * 处理新的TCP连接、接收客户端的响应、向客户端发送响应结果。
 * Kafka支持两种两种类型的请求面：
 * ① 数据面（data-plane）：
 * <1> 处理来自客户端或同一个集群的其它Broker的请求
 * <2> 线程模型：每一个监听器（listener）都有一个Acceptor线程与之对应，用来接收新的TCP连接。
 * 通过在配置文件（server.properties）的配置项（listeners）配置监听器，如果有多个，使用逗号（,）分隔。
 * Acceptor管理N个Processor处理器，这些处理器主要职责是负责底层网络I/O操作，比如获取二进制数据、解码、发送Response等。
 * 另外，还有M个I/O处理线程，这个I/O是指文本I/O操作，比如向本地日志写入数据等操作，可以理解为业务处理线程。I/O线程从Processor处理器队列中
 * 获取Request对象，根据请求内容做相应的I/O操作（不一定每次都需要执行I/O，只是笼统称作I/O线程），然后生成Response对象并交给Processor发送。
 * ② 控制面（control-plane）：
 * <1> 这是一个可选项，可以在通过配置「control.plane.listener.name」开启控制面。如果没有开启，则么控制类的请求还是交给data-plane处理。
 * <2> 线程模型：仅会有一个Acceptor线程和一个Processor线程处理控制类请求。因为控制类请求数量远小于数据类请求。
 *
 * 监听器（Listener）
 *
 * @param config
 * @param metrics
 * @param time
 * @param credentialProvider
 * @param apiVersionManager
 */
class SocketServer(val config: KafkaConfig,
                   val metrics: Metrics,
                   val time: Time,
                   val credentialProvider: CredentialProvider,
                   val apiVersionManager: ApiVersionManager)
  extends Logging with KafkaMetricsGroup with BrokerReconfigurable {

  // 阻塞队列所能容纳最大的请求长度，由Broker端参数「queued.max.request」指定，默认值：500
  private val maxQueuedRequests = config.queuedMaxRequests
  // broker ID
  private val nodeId = config.brokerId

  private val logContext = new LogContext(s"[SocketServer listenerType=${apiVersionManager.listenerType}, nodeId=$nodeId] ")

  this.logIdent = logContext.logPrefix

  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", MetricsGroup)
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", MetricsGroup)
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))
  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE

  // 数据面
  /**
   * 保存全局所有的Processor处理线程
   */
  private val dataPlaneProcessors = new ConcurrentHashMap[Int, Processor]()
  /**
   * 保留全局的接口器对象，key：Acceptor，value：对应的Processor处理线程
   */
  private[network] val dataPlaneAcceptors = new ConcurrentHashMap[EndPoint, Acceptor]()
  /**
   * 数据类请求队列，所有EndPoint共享一个数据类请求队列：由Processor生产出Request请求类，然后放入此队列中
   * 等待I/O线程拉取并消费
   */
  val dataPlaneRequestChannel =
    new RequestChannel(maxQueuedRequests, DataPlaneMetricPrefix, time, apiVersionManager.newRequestMetrics)

  // 控制面
  private var controlPlaneProcessorOpt: Option[Processor] = None
  private[network] var controlPlaneAcceptorOpt: Option[Acceptor] = None
  // 控制面队列大小固定为20
  val controlPlaneRequestChannelOpt: Option[RequestChannel] = config.controlPlaneListenerName.map(_ =>
    new RequestChannel(20, ControlPlaneMetricPrefix, time, apiVersionManager.newRequestMetrics))
  // 下个Processor ID，全局唯一
  private var nextProcessorId = 0
  val connectionQuotas = new ConnectionQuotas(config, time, metrics)

  /**
   * 是否启动Processor线程
   */
  private var startedProcessingRequests = false

  /**
   * 是否停止Processor线程
   */
  private var stoppedProcessingRequests = false

  /**
   * 启动Broker Server。
   * ① 创建所有的Acceptor和Processor（基于配置文件得到的EndPoint集合对象所决定）。
   * 默认一个EndPoint创建一个Accepotor和3个Processor。
   * ② Acceptor在此刻开始监听，其中原由是当这个方法执行完成后我们就可以已知绑定的端口了，即便临时端口也适用。
   * ③ 如果「startProcessingRequests」为true则会启动Acceptors和Processors。
   * 如果为false，则启动步骤会延迟到 [[kafka.network.SocketServer#startProcessingRequests()]] 完成。
   * 延迟启动适用于延迟处理客户端连接，直接服务端初始化完成。比如确保在服务端进行身份验证之前加载所有证书之类的信息。
   * 当处理器启动并调用[[org.apache.kafka.common.network.Selector#poll]]时就会处理向服务端发送的请求。
   *
   * @param startProcessingRequests true：启动processor线程，默认值：false
   * @param controlPlaneListener    控制面监听器（可选）
   * @param dataPlaneListeners      数据面监听器（必有）
   */
  def startup(startProcessingRequests: Boolean = true,
              controlPlaneListener: Option[EndPoint] = config.controlPlaneListener,
              dataPlaneListeners: Seq[EndPoint] = config.dataPlaneListeners): Unit = {
    this.synchronized {
      // #1 创建管制面Acceptor和Processor
      createControlPlaneAcceptorAndProcessor(controlPlaneListener)

      // #2 创建数据面Acceptors和Processors
      createDataPlaneAcceptorsAndProcessors(config.numNetworkThreads, dataPlaneListeners)
      if (startProcessingRequests) {
        // #3 启动Processor线程
        this.startProcessingRequests()
      }
    }

    newGauge(s"${DataPlaneMetricPrefix}NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
      val ioWaitRatioMetricNames = dataPlaneProcessors.values.asScala.iterator.map { p =>
        metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags)
      }
      ioWaitRatioMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }.sum / dataPlaneProcessors.size
    })
    newGauge(s"${ControlPlaneMetricPrefix}NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
      val ioWaitRatioMetricName = controlPlaneProcessorOpt.map { p =>
        metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags)
      }
      ioWaitRatioMetricName.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }.getOrElse(Double.NaN)
    })
    newGauge("MemoryPoolAvailable", () => memoryPool.availableMemory)
    newGauge("MemoryPoolUsed", () => memoryPool.size() - memoryPool.availableMemory)
    newGauge(s"${DataPlaneMetricPrefix}ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
      val expiredConnectionsKilledCountMetricNames = dataPlaneProcessors.values.asScala.iterator.map { p =>
        metrics.metricName("expired-connections-killed-count", MetricsGroup, p.metricTags)
      }
      expiredConnectionsKilledCountMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
      }.sum
    })
    newGauge(s"${ControlPlaneMetricPrefix}ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
      val expiredConnectionsKilledCountMetricNames = controlPlaneProcessorOpt.map { p =>
        metrics.metricName("expired-connections-killed-count", MetricsGroup, p.metricTags)
      }
      expiredConnectionsKilledCountMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
      }.getOrElse(0.0)
    })
  }

  /**
   * 开启处理新的连接和新的请求。
   * 如果Processor线程延迟启用的话（startProcessingRequests=false），那么就会在[[kafka.network.SocketServer#startup]]触发执行。
   * 在为每个Endpoint启动Processor之前，我们确保授权者（authorizer）拥有所有的元数据，通过等待提供的future来给该端点的请求进行授权。
   * 我们在其它监听器之前启动inter-broker监听器，这允许其它监听器的授权元数据被存入在这个集群的Kafka主题中。
   *
   * @param authorizerFutures Future per [[EndPoint]] used to wait before starting the processor
   *                          corresponding to the [[EndPoint]]
   */
  def startProcessingRequests(authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = Map.empty): Unit = {
    info("Starting socket server acceptors and processors")
    this.synchronized {
      // #1 判断是否已经启动Processors线程
      if (!startedProcessingRequests) {

        // #2 没有的话，先启动控制类Processor和Acceptor
        startControlPlaneProcessorAndAcceptor(authorizerFutures)

        // #3 再启动数据类Processors和Acceptor
        startDataPlaneProcessorsAndAcceptors(authorizerFutures)
        startedProcessingRequests = true
      } else {
        info("Socket server acceptors and processors already started")
      }
    }
    info("Started socket server acceptors and processors")
  }

  /**
   * Starts processors of the provided acceptor and the acceptor itself.
   *
   * Before starting them, we ensure that authorizer has all the metadata to authorize
   * requests on that endpoint by waiting on the provided future.
   */

  /**
   * 启动所提供的Acceptor和Processor，
   * 别记了，Acceptor管理Processor的，所以可以通过Acceptor启动Processor。
   *
   * @param threadPrefix      线程名称前缀
   * @param endpoint          终端详情
   * @param acceptor          该终端对应的Accepotor
   * @param authorizerFutures 认证Future，如果有，
   *                          我们需要等待认证完成才进行下一步启动操作
   */
  private def startAcceptorAndProcessors(threadPrefix: String,
                                         endpoint: EndPoint,
                                         acceptor: Acceptor,
                                         authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = Map.empty): Unit = {
    // #1 等待授权者完成对监听器的启动工作
    debug(s"Wait for authorizer to complete start up on listener ${endpoint.listenerName}")
    waitForAuthorizerFuture(acceptor, authorizerFutures)
    debug(s"Start processors on listener ${endpoint.listenerName}")

    // #2 启动Processor处理器
    acceptor.startProcessors(threadPrefix)

    // #3 启动Acceptor
    if (!acceptor.isStarted()) {
      KafkaThread.nonDaemon(
        s"${threadPrefix}-kafka-socket-acceptor-${endpoint.listenerName}-${endpoint.securityProtocol}-${endpoint.port}",
        acceptor
      ).start()
      // #4 线程阻塞等待，直接Acceptor启动成功
      acceptor.awaitStartup()
    }
    info(s"Started $threadPrefix acceptor and processor(s) for endpoint : ${endpoint.listenerName}")
  }

  /**
   * 启动数据面所有的Acceptors和Processors线程
   * 我们会先启动「inter-broker」监听器，随后启动其它监听器。
   * 目的是允许其他监听器的授权元数据被存储在这个集群的Kafka主题中。
   *
   * @param authorizerFutures 认证结果凭证
   */
  private def startDataPlaneProcessorsAndAcceptors(authorizerFutures: Map[Endpoint, CompletableFuture[Void]]): Unit = {
    // #1 获取「inter.broker.listener.name」配置的内部监听器
    val interBrokerListener = dataPlaneAcceptors.asScala.keySet
      .find(_.listenerName == config.interBrokerListenerName)

    // #2 获取年老的Acceptors
    val orderedAcceptors = interBrokerListener match {
      case Some(interBrokerListener) => List(dataPlaneAcceptors.get(interBrokerListener)) ++
        dataPlaneAcceptors.asScala.filter { case (k, _) => k != interBrokerListener }.values
      case None => dataPlaneAcceptors.asScala.values
    }

    // #3 遍历挨个启动Accepotr和Processors
    orderedAcceptors.foreach { acceptor =>
      val endpoint = acceptor.endPoint
      startAcceptorAndProcessors(DataPlaneThreadPrefix, endpoint, acceptor, authorizerFutures)
    }
  }

  /**
   * 启动控制面的Acceptor和Processor线程
   * 控制面只有一个Acceptor和一个Processor线程
   * 如果配置选项「control.plane.listener.name」不为null，那么就意味着启动Control plane。
   * 如果得到端点呢?
   * ① 读取配置项「control.plane.listener.name」得到字符串a
   * ② 读取配置项「listener.security.protocol.map」得到一个Map<协议名称（自定义名称）, 安全协议>，<a,b><c,d>
   * ③ 读取配置项「listeners」   得到一个MAP<安全协议, 主机名称:端口号>
   * ④ 根据字节串从Map<协议名称（自定义名称）, 安全协议>找到安全协议，再根据安全协议从MAP<安全协议, 主机名称:端口号>得到主机名称和端口号
   * {@link KafkaConfig.getControlPlaneListenerNameAndSecurityProtocol}
   *
   * @param authorizerFutures
   */
  private def startControlPlaneProcessorAndAcceptor(authorizerFutures: Map[Endpoint, CompletableFuture[Void]]): Unit = {
    controlPlaneAcceptorOpt.foreach { controlPlaneAcceptor =>
      // #1 获取控制面终端详情
      val endpoint = config.controlPlaneListener.get
      // #2 启动
      startAcceptorAndProcessors(ControlPlaneThreadPrefix, endpoint, controlPlaneAcceptor, authorizerFutures)
    }
  }

  private def endpoints = config.listeners.map(l => l.listenerName -> l).toMap

  /**
   * 创建数据面的Acceptor和Processor，
   * 每个EndPoint都拥有独立的一套Acceptor和Processor
   *
   * @param dataProcessorsPerListener
   * @param endpoints
   */
  private def createDataPlaneAcceptorsAndProcessors(dataProcessorsPerListener: Int,
                                                    endpoints: Seq[EndPoint]): Unit = {
    // #1 遍历监听器集合
    endpoints.foreach { endpoint =>
      // #2 将监听器纳入到连接配额管理之下
      connectionQuotas.addListener(config, endpoint.listenerName)

      // #3 创建一个Acceptor线程
      val dataPlaneAcceptor = createAcceptor(endpoint, DataPlaneMetricPrefix)

      // #4 创建N个Processor线程，并将其交给Acceptor管理
      addDataPlaneProcessors(dataPlaneAcceptor, endpoint, dataProcessorsPerListener)

      // #5 添加到SocketServer全局共享变量中
      dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
      info(s"Created data-plane acceptor and processors for endpoint : ${endpoint.listenerName}")
    }
  }

  /**
   * 创建控制面Acceptor和Processor
   *
   * @param endpointOpt 控制面监听器详情
   */
  private def createControlPlaneAcceptorAndProcessor(endpointOpt: Option[EndPoint]): Unit = {
    // #1 遍历监听器集合
    endpointOpt.foreach { endpoint =>
      // #2 将监听器纳入到连接配额管理之下
      connectionQuotas.addListener(config, endpoint.listenerName)

      // #3 创建一个Acceptor线程
      val controlPlaneAcceptor = createAcceptor(endpoint, ControlPlaneMetricPrefix)

      // #4 创建一个Processor线程
      val controlPlaneProcessor = newProcessor(nextProcessorId, controlPlaneRequestChannelOpt.get,
        connectionQuotas, endpoint.listenerName, endpoint.securityProtocol, memoryPool, isPrivilegedListener = true)

      // #5 添加到全局变量中
      controlPlaneAcceptorOpt = Some(controlPlaneAcceptor)
      controlPlaneProcessorOpt = Some(controlPlaneProcessor)

      val listenerProcessors = new ArrayBuffer[Processor]()
      listenerProcessors += controlPlaneProcessor
      controlPlaneRequestChannelOpt.foreach(_.addProcessor(controlPlaneProcessor))
      nextProcessorId += 1

      // #6 将Processor交给Acceptor管理
      controlPlaneAcceptor.addProcessors(listenerProcessors, ControlPlaneThreadPrefix)
      info(s"Created control-plane acceptor and processor for endpoint : ${endpoint.listenerName}")
    }
  }

  private def createAcceptor(endPoint: EndPoint, metricPrefix: String): Acceptor = {
    val sendBufferSize = config.socketSendBufferBytes
    val recvBufferSize = config.socketReceiveBufferBytes
    new Acceptor(endPoint, sendBufferSize, recvBufferSize, nodeId, connectionQuotas, metricPrefix, time)
  }

  /**
   *
   * @param acceptor
   * @param endpoint
   * @param newProcessorsPerListener Processor处理器数量，默认值：3。如果发现请求处理不过来，可适当增大此参数
   */
  private def addDataPlaneProcessors(acceptor: Acceptor,
                                     endpoint: EndPoint,
                                     newProcessorsPerListener: Int): Unit = {
    // #1 获取监听器名称
    val listenerName = endpoint.listenerName

    // #2 获取监听器对应的安全协议
    val securityProtocol = endpoint.securityProtocol

    // #3 创建处理线程
    val listenerProcessors = new ArrayBuffer[Processor]()

    // #4 判断是否为自定义监听器，默认值：true
    val isPrivilegedListener =
      controlPlaneRequestChannelOpt.isEmpty && config.interBrokerListenerName == listenerName

    // #5 创建「newProcessorsPerListener」指定数量的Processor
    for (_ <- 0 until newProcessorsPerListener) {
      val processor = newProcessor(nextProcessorId, dataPlaneRequestChannel, connectionQuotas,
        listenerName, securityProtocol, memoryPool, isPrivilegedListener)
      listenerProcessors += processor
      dataPlaneRequestChannel.addProcessor(processor)
      nextProcessorId += 1
    }
    // #6
    listenerProcessors.foreach(p => dataPlaneProcessors.put(p.id, p))
    acceptor.addProcessors(listenerProcessors, DataPlaneThreadPrefix)
  }

  /**
   * Stop processing requests and new connections.
   */
  def stopProcessingRequests(): Unit = {
    info("Stopping socket server request processors")
    this.synchronized {
      dataPlaneAcceptors.asScala.values.foreach(_.initiateShutdown())
      dataPlaneAcceptors.asScala.values.foreach(_.awaitShutdown())
      controlPlaneAcceptorOpt.foreach(_.initiateShutdown())
      controlPlaneAcceptorOpt.foreach(_.awaitShutdown())
      dataPlaneRequestChannel.clear()
      controlPlaneRequestChannelOpt.foreach(_.clear())
      stoppedProcessingRequests = true
    }
    info("Stopped socket server request processors")
  }

  def resizeThreadPool(oldNumNetworkThreads: Int, newNumNetworkThreads: Int): Unit = synchronized {
    info(s"Resizing network thread pool size for each data-plane listener from $oldNumNetworkThreads to $newNumNetworkThreads")
    if (newNumNetworkThreads > oldNumNetworkThreads) {
      dataPlaneAcceptors.forEach { (endpoint, acceptor) =>
        addDataPlaneProcessors(acceptor, endpoint, newNumNetworkThreads - oldNumNetworkThreads)
      }
    } else if (newNumNetworkThreads < oldNumNetworkThreads)
      dataPlaneAcceptors.asScala.values.foreach(_.removeProcessors(oldNumNetworkThreads - newNumNetworkThreads, dataPlaneRequestChannel))
  }

  /**
   * Shutdown the socket server. If still processing requests, shutdown
   * acceptors and processors first.
   */
  def shutdown(): Unit = {
    info("Shutting down socket server")
    this.synchronized {
      if (!stoppedProcessingRequests)
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
        acceptor.serverChannel.socket.getLocalPort
      } else {
        controlPlaneAcceptorOpt.map(_.serverChannel.socket().getLocalPort).getOrElse(throw new KafkaException("Could not find listenerName : " + listenerName + " in data-plane or control-plane"))
      }
    } catch {
      case e: Exception =>
        throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  def addListeners(listenersAdded: Seq[EndPoint]): Unit = synchronized {
    info(s"Adding data-plane listeners for endpoints $listenersAdded")
    createDataPlaneAcceptorsAndProcessors(config.numNetworkThreads, listenersAdded)
    listenersAdded.foreach { endpoint =>
      val acceptor = dataPlaneAcceptors.get(endpoint)
      startAcceptorAndProcessors(DataPlaneThreadPrefix, endpoint, acceptor)
    }
  }

  def removeListeners(listenersRemoved: Seq[EndPoint]): Unit = synchronized {
    info(s"Removing data-plane listeners for endpoints $listenersRemoved")
    listenersRemoved.foreach { endpoint =>
      connectionQuotas.removeListener(config, endpoint.listenerName)
      dataPlaneAcceptors.asScala.remove(endpoint).foreach { acceptor =>
        acceptor.initiateShutdown()
        acceptor.awaitShutdown()
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

  private def waitForAuthorizerFuture(acceptor: Acceptor,
                                      authorizerFutures: Map[Endpoint, CompletableFuture[Void]]): Unit = {
    //we can't rely on authorizerFutures.get() due to ephemeral ports. Get the future using listener name
    authorizerFutures.forKeyValue { (endpoint, future) =>
      if (endpoint.listenerName == Optional.of(acceptor.endPoint.listenerName.value))
        future.join()
    }
  }

  /**
   * 创建新的Processor
   *
   * @param id                   processor唯一ID，从0开始，全局唯一，由SocketServer分配
   * @param requestChannel       存放待处理的Request的缓冲队列，是构建微小版的生产者/消费者的关键组件
   * @param connectionQuotas     连接配额器，管理TCP连接：限流
   * @param listenerName         监听器名称
   * @param securityProtocol     监听器对应的安全协议
   * @param memoryPool           内存池
   * @param isPrivilegedListener 监听器是否享有特权
   * @return
   */
  protected[network] def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                      securityProtocol: SecurityProtocol, memoryPool: MemoryPool, isPrivilegedListener: Boolean): Processor = {
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
      apiVersionManager
    )
  }

  // For test usage
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  // For test usage
  private[network] def dataPlaneProcessor(index: Int): Processor = dataPlaneProcessors.get(index)

}

object SocketServer {
  val MetricsGroup = "socket-server-metrics"
  val DataPlaneThreadPrefix = "data-plane"
  val ControlPlaneThreadPrefix = "control-plane"
  val DataPlaneMetricPrefix = ""
  val ControlPlaneMetricPrefix = "ControlPlane"

  /**
   * 定义Broker端可动态配置的配置项
   */
  val ReconfigurableConfigs = Set(
    KafkaConfig.MaxConnectionsPerIpProp,
    KafkaConfig.MaxConnectionsPerIpOverridesProp,
    KafkaConfig.MaxConnectionsProp,
    KafkaConfig.MaxConnectionCreationRateProp)

  val ListenerReconfigurableConfigs = Set(KafkaConfig.MaxConnectionsProp, KafkaConfig.MaxConnectionCreationRateProp)
}

/**
 * 是Acceptor和Processor的抽象基类，定义和线程相关的公共方法，比如唤醒、关闭线程等操。
 * 关注CountDownLatch的使用，用来实现优雅的线程启动、线程关闭等操作。
 *
 * @param connectionQuotas
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  /**
   * 作用：拿一个线程等待其它线程各自执行完毕后再执行
   * 线程A、线程B（持有CountDownLatch(1)变量）
   * 线程A等待线程B->countDownLatch.countDown()->线程A唤醒，继续执行下面逻辑
   */
  private val startupLatch = new CountDownLatch(1)

  /**
   * 如果在构造器中抛出异常（比如地址被占用），那么方法shutdown()在startupComplete()和shutdownComplete()之前被调用。
   * We want shutdown() to proceed in such cases, so we first assign an open
   * latch and then replace it in startupComplete()
   */
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop
   */
  def initiateShutdown(): Unit = {
    if (alive.getAndSet(false))
      wakeup()
  }

  /**
   * Wait for the thread to completely shutdown
   */
  def awaitShutdown(): Unit = shutdownLatch.await

  /**
   * 判断线程是否已经完全启动
   *
   * @return
   */
  def isStarted(): Boolean = startupLatch.getCount == 0

  /**
   * 等待线程完全启动
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * 记录线程已经完全启动
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * 记录线程已经完全关闭
   */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * 判断线程是否仍在运行
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(listenerName: ListenerName, channel: SocketChannel): Unit = {
    if (channel != null) {
      debug(s"Closing connection from ${channel.socket.getRemoteSocketAddress}")
      connectionQuotas.dec(listenerName, channel.socket.getInetAddress)
      closeSocket(channel)
    }
  }

  protected def closeSocket(channel: SocketChannel): Unit = {
    CoreUtils.swallow(channel.socket().close(), this, Level.ERROR)
    CoreUtils.swallow(channel.close(), this, Level.ERROR)
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */

/**
 * 主要用于接收新的TCP连接，
 *
 * @param endPoint         这个就是配置文件定义的端点（比如PLAINTEXT://localhost:9092），
 *                         Acceptor需要使用端点创建ServerSocketChannel
 * @param sendBufferSize   默认值：102400
 * @param recvBufferSize   默认值：102400
 * @param nodeId           Broker ID
 * @param connectionQuotas 限流
 * @param metricPrefix
 * @param time
 * @param logPrefix
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              nodeId: Int,
                              connectionQuotas: ConnectionQuotas,
                              metricPrefix: String,
                              time: Time,
                              logPrefix: String = "") extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  this.logIdent = logPrefix
  // #1 获取JDK底层的Selector轮询器
  private val nioSelector = NSelector.open()

  // #2 为端点创建ServerSocketChannel
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  // #3 Processor线程是在Acceptor线程管理和维护的
  private val processors = new ArrayBuffer[Processor]()

  // 判断Processor线程池是否已经启动
  private val processorsStarted = new AtomicBoolean
  private val blockedPercentMeter = newMeter(s"${metricPrefix}AcceptorBlockedPercent",
    "blocked time", TimeUnit.NANOSECONDS, Map(ListenerMetricTag -> endPoint.listenerName.value))

  //
  private var currentProcessorIndex = 0
  private[network] val throttledSockets = new mutable.PriorityQueue[DelayedCloseSocket]()

  private[network] case class DelayedCloseSocket(socket: SocketChannel, endThrottleTimeMs: Long) extends Ordered[DelayedCloseSocket] {
    override def compare(that: DelayedCloseSocket): Int = endThrottleTimeMs compare that.endThrottleTimeMs
  }

  /**
   * 添加Processor线程
   *
   * @param newProcessors         新的Processor处理线程
   * @param processorThreadPrefix 线程名前缀
   */
  private[network] def addProcessors(newProcessors: Buffer[Processor], processorThreadPrefix: String): Unit = synchronized {
    processors ++= newProcessors
    if (processorsStarted.get) {
      // 如果已经启动Processor线程，则么新加入的Processor也需要立即启动
      startProcessors(newProcessors, processorThreadPrefix)
    }
  }

  /**
   * 启动所有已添加的Procesor线程
   *
   * @param processorThreadPrefix 线程名前缀
   */
  private[network] def startProcessors(processorThreadPrefix: String): Unit = synchronized {
    if (!processorsStarted.getAndSet(true)) {
      startProcessors(processors, processorThreadPrefix)
    }
  }

  private def startProcessors(processors: Seq[Processor], processorThreadPrefix: String): Unit = synchronized {
    processors.foreach { processor =>
      KafkaThread.nonDaemon(
        s"${processorThreadPrefix}-kafka-network-thread-$nodeId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor
      ).start()
    }
  }

  private[network] def removeProcessors(removeCount: Int, requestChannel: RequestChannel): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)
    toRemove.foreach(_.initiateShutdown())
    toRemove.foreach(_.awaitShutdown())
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  override def initiateShutdown(): Unit = {
    super.initiateShutdown()
    synchronized {
      processors.foreach(_.initiateShutdown())
    }
  }

  /**
   * 等待所有的Processor关闭
   */
  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    synchronized {
      processors.foreach(_.awaitShutdown())
    }
  }

  /**
   * 核心逻辑，不断轮询
   */
  def run(): Unit = {
    // #1 将ServerSocketChannel注册到轮询器Selector
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)

    // #2 表示Acceptor启动完成，意味着其它线程等待Accepotr完全启动的线程可以被唤醒
    startupComplete()
    try {
      // #3 不断轮询
      while (isRunning) {
        try {
          // #4 接收新的连接并以轮询算法分配给不同的processor处理器处理
          acceptNewConnections()

          // #5 关闭已被限流的连接
          closeThrottledConnections()
        }
        catch {
          // #6 捕获所有的异常以防止Accepotr退出
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket, selector, and any throttled sockets.")
      CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
      CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)
      throttledSockets.foreach(throttledSocket => closeSocket(throttledSocket.socket))
      throttledSockets.clear()
      shutdownComplete()
    }
  }

  /**
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if (host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      serverChannel.socket.bind(socketAddress)
      info(s"Awaiting socket connections on ${socketAddress.getHostString}:${serverChannel.socket.getLocalPort}.")
    } catch {
      case e: SocketException =>
        throw new KafkaException(s"Socket server failed to bind to ${socketAddress.getHostString}:$port: ${e.getMessage}.", e)
    }
    serverChannel
  }

  /**
   * 接收新的连接并以轮询算法（round-robin）分配给processor
   * ① 创建SocketChannel
   * ②
   */
  private def acceptNewConnections(): Unit = {
    // #1 第500毫秒获取一次就绪I/O事件
    val ready = nioSelector.select(500)
    if (ready > 0) {
      // #2 本轮有准备就绪的事件
      val keys = nioSelector.selectedKeys()
      val iter = keys.iterator()
      while (iter.hasNext && isRunning) {
        try {
          val key = iter.next
          iter.remove()

          if (key.isAcceptable) {
            accept(key).foreach { socketChannel =>
              // #3 使用轮询将SocketChannel分配给Processor
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

                // 尝试将SocketChannel添加到Processor队列中，如果没有任何Processor可容纳，则线程会被阻塞
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
   * 创建一个新的SocketChannel
   *
   * @param key
   * @return
   */
  private def accept(key: SelectionKey): Option[SocketChannel] = {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      connectionQuotas.inc(endPoint.listenerName, socketChannel.socket.getInetAddress, blockedPercentMeter)
      // 配置通道为非阻塞模式
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)
      Some(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info(s"Rejected connection from ${e.ip}, address already has the configured maximum of ${e.count} connections.")
        close(endPoint.listenerName, socketChannel)
        None
      case e: ConnectionThrottledException =>
        val ip = socketChannel.socket.getInetAddress
        debug(s"Delaying closing of connection from $ip for ${e.throttleTimeMs} ms")
        val endThrottleTimeMs = e.startThrottleTimeMs + e.throttleTimeMs
        throttledSockets += DelayedCloseSocket(socketChannel, endThrottleTimeMs)
        None
    }
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

  /**
   * 主要逻辑是调用 {@link Processor.accept( )} 方法
   * 将 SocketChannel 添加到Processor，等待Processor处理
   *
   * @param socketChannel 待添加的SocketChannel
   * @param processor     处理器
   * @param mayBlock      是否阻塞线程直到有可用的Processor处理器
   * @return
   */
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
  @Override
  def wakeup(): Unit = nioSelector.wakeup()

}

private[kafka] object Processor {
  val IdlePercentMetricName = "IdlePercent"
  val NetworkProcessorMetricTag = "networkProcessor"
  val ListenerMetricTag = "listener"
  val ConnectionQueueSize = 20
}

/**
 * Thread that processes all requests from a single connection.
 *
 * There are N of these running in parallel
 * each of which has its own selector
 *
 * @param isPrivilegedListener The privileged listener flag is used as one factor to determine whether
 *                             a certain request is forwarded or not. When the control plane is defined,
 *                             the control plane processor would be fellow broker's choice for sending
 *                             forwarding requests; if the control plane is not defined, the processor
 *                             relying on the inter broker listener would be acting as the privileged listener.
 */

/**
 * 单条TCP连接只会绑定一个Process处理器线程，
 * TCP上的所有请求/响应都由该Processor接收和发送。
 * 当然，每个Processor都会有独立的Selector用来管理SocketChannel。
 *
 * @param id
 * @param time
 * @param maxRequestSize
 * @param requestChannel
 * @param connectionQuotas
 * @param connectionsMaxIdleMs
 * @param failedAuthenticationDelayMs
 * @param listenerName
 * @param securityProtocol
 * @param config
 * @param metrics
 * @param credentialProvider
 * @param memoryPool
 * @param logContext
 * @param connectionQueueSize
 * @param isPrivilegedListener
 * @param apiVersionManager
 */
private[kafka] class Processor(val id: Int,
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
                               apiVersionManager: ApiVersionManager) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

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
   * 保存新创建的SocketChannel对象，这些对象等待Processor消费：将它们注册到自己的Selector轮询器中（调用 {@link configureNewConnections}方法完成）
   */
  private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)

  /**
   * 临时存放Response的队列，主要是为了执行Response的回调方法。
   * 因为这是异步调用而非同步，所以需要使用一个集合记录哪些Reponse对象正在发送，
   * 以便下次轮询可以判断是否发送完成，一旦发送完成就可以触发回调方法。
   */
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()

  /**
   * 这个队列用于存放由 {@link kafka.server.KafkaRequestHandlerPool} I/O处理线程生成的Response，
   * 每个Processor都会有自己的并发队列存储Response。
   */
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()

  private[kafka] val metricTags = mutable.LinkedHashMap(
    ListenerMetricTag -> listenerName.value,
    NetworkProcessorMetricTag -> id.toString
  ).asJava

  newGauge(IdlePercentMetricName, () => {
    Option(metrics.metric(metrics.metricName("io-wait-ratio", MetricsGroup, metricTags))).fold(0.0)(m =>
      Math.min(m.metricValue.asInstanceOf[Double], 1.0))
  },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map(NetworkProcessorMetricTag -> id.toString)
  )

  val expiredConnectionsKilledCount = new CumulativeSum()
  private val expiredConnectionsKilledCountMetricName = metrics.metricName("expired-connections-killed-count", MetricsGroup, metricTags)
  metrics.addMetric(expiredConnectionsKilledCountMetricName, expiredConnectionsKilledCount)

  private val selector = createSelector(
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

  /**
   * Processor核心处理逻辑
   */
  override def run(): Unit = {
    // #1 标记Processor线程启动完成
    startupComplete()
    try {
      // #2 不断轮询处理I/O的Request
      while (isRunning) {
        try {
          // #3 将新的SocketChannel注册到Selector
          configureNewConnections()

          // #4 消费「responseQueue」队列，从中获取Response并发送给"客户端（也可能是其它的Broker对象）"
          //    并且将Response放入「inflightResponses」队列中
          processNewResponses()

          // #5 执行I/O操作
          poll()

          // #6 处理已接收的Request
          processCompletedReceives()

          // #7 处理已完成发送的Respnse（其实就是触发Response相关回调）
          processCompletedSends()

          // #8 每次轮询可能都会出现连接断开的情况，这些已断开的TCP需要快速释放资源
          processDisconnected()

          // #9 关闭超限连接，使用LRU算法移除TCP连接
          closeExcessConnections()
        } catch {
          // 捕获所有的异常以防止Processor线程意外退出。一般来讲，普通的异常上述方法都会捕获并处理。
          // 所以这个方法应该仅会捕获到ControlThrowables异常
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {
      debug(s"Closing selector - processor $id")
      // #10 Processor线程退出，释放资源：关闭Selector、关闭SocketChannel连接
      CoreUtils.swallow(closeAll(), this, Level.ERROR)

      // #11 标志Processor已完全关闭
      shutdownComplete()
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

  /**
   * 处理新的响应，根据类型判断是否需要通过网络发送到对端
   */
  private def processNewResponses(): Unit = {
    var currentResponse: RequestChannel.Response = null
    while ( {
      // #1 从「responseQueue」获取待发送的Response对象
      currentResponse = dequeueResponse();
      currentResponse != null
    }) {
      val channelId = currentResponse.request.context.connectionId
      try {
        // #2 不同的响应类型有不同处理
        currentResponse match {
          case response: NoOpResponse =>
            // #2-1 这里没有要发送给客户端的响应，我们需要从底层的Socket Buffer读取更多的Reqeust
            // 更新请求指标
            updateRequestMetrics(response)
            trace(s"Socket server received empty response to send, registering for read: $response")

            // 尝试移除当前通道静默标识。
            handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
            tryUnmuteChannel(channelId)
          case response: SendResponse =>
            // #2-2 需要向客户端发送Response响应
            sendResponse(response, response.responseSend)
          case response: CloseConnectionResponse =>
            // #2-3 关闭连接
            updateRequestMetrics(response)
            trace("Closing socket connection actively according to the response code.")
            close(channelId)
          case _: StartThrottlingResponse =>
            // #2-4 通道限流
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED)
          case _: EndThrottlingResponse =>
            // #2-5 结束通道限流：①尝试移除当前通道静默标识，只有当响应已经发送给客户端后才会被取消静默
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

  /**
   * 发送Response响应
   *
   * @param response
   * @param responseSend
   */
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send): Unit = {
    val connectionId = response.request.context.connectionId
    trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
    // #1 判断TCP连接是否被关闭，关闭原因可能有：① 远端主动关闭Socket连接;② 由于空闲时间太长导致Socket连接被主动关闭
    if (channel(connectionId).isEmpty) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $connectionId")
      response.request.updateRequestMetrics(0L, response)
    }

    // #2 再次判断TCP连接是否处于关闭状态（重复?）
    if (openOrClosingChannel(connectionId).isDefined) {
      // #3 TCP连接正常，那么就可以发送数据了
      selector.send(new NetworkSend(connectionId, responseSend))
      // #4 加入「inflightResponses」
      inflightResponses += (connectionId -> response)
    }
  }

  /**
   * 执行I/O操作：接收Reqeust/发送Response
   */
  private def poll(): Unit = {
    // #1 如果不需要处理新的连接，则超时时间设置为300毫秒
    val pollTimeout = if (newConnections.isEmpty) 300 else 0

    // #2 执行I/O操作
    try selector.poll(pollTimeout)
    catch {
      case e@(_: IllegalStateException | _: IOException) =>
        // The exception is not re-thrown and any completed sends/receives/connections/disconnections
        // from this poll will be processed.
        error(s"Processor $id poll failed", e)
    }
  }

  protected def parseRequestHeader(buffer: ByteBuffer): RequestHeader = {
    val header = RequestHeader.parse(buffer)
    if (apiVersionManager.isApiEnabled(header.apiKey)) {
      header
    } else {
      throw new InvalidRequestException(s"Received request api key ${header.apiKey} which is not enabled")
    }
  }

  /**
   * 处理已接收的Request
   * ① 对二进制数据进行解码操作，仅得到请求头部分
   * ② 根据请求头做一些校验性判断，比如认证
   * ③ 创建请求上下文对象 RequestContext
   * ④ 创建Request对象，并关联 RequestContest
   * ⑤ 将创建好的Request放入阻塞队列（requestQueue），等待I/O线程拉取并处理
   * ⑥ 静默当前TCP连接，使其无法接收/发送数据
   * ⑦ 清理completedReceives集合，等待下次poll()轮询时填充数据
   */
  private def processCompletedReceives(): Unit = {
    selector.completedReceives.forEach { receive =>
      try {
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            // #1 解析接收到的二进制数据，其实就是Request对象
            val header = parseRequestHeader(receive.payload)

            // #2 判断是否需要重新进行身份认证
            if (header.apiKey == ApiKeys.SASL_HANDSHAKE && channel.maybeBeginServerReauthentication(receive, () => time.nanoseconds())) {
              trace(s"Begin re-authentication: $channel")
            } else {
              val nowNanos = time.nanoseconds()
              // #3 认证会话已过期，则关闭连接
              if (channel.serverAuthenticationSessionExpired(nowNanos)) {
                // be sure to decrease connection count and drop any in-flight responses
                debug(s"Disconnecting expired channel: $channel : $header")
                close(channel.id)
                expiredConnectionsKilledCount.record(null, 1, 0)
              } else {
                // #4 身份正常
                val connectionId = receive.source

                // #5 创建请求上下文
                val context = new RequestContext(header, // 请求头
                  connectionId, // TCP连接标识ID，类似：172.16.210.80:9092-172.16.210.80:52659-0
                  channel.socketAddress, // Broker端IP+PORT
                  channel.principal, //
                  listenerName, // PLAINTEXT
                  securityProtocol, // 安全协议，比如PLAINTEXT
                  channel.channelMetadataRegistry.clientInformation, //
                  isPrivilegedListener, // 是否是私有的监听器，默认为true
                  channel.principalSerde) //

                // #6 创建Request对象，内部持有RequestContext的引用
                val req = new RequestChannel.Request(
                  processor = id, // 处理器ID
                  context = context, // 请求上下文
                  startTimeNanos = nowNanos, // 请求对象创建时间
                  memoryPool, // 内存池对象，可无阻塞分配ByteBuffer对象
                  receive.payload, // Request二进制数据
                  requestChannel.metrics, // 请求通道指标监控类
                  None
                )
                // #7 在这里拦截「ApiVersionReqeust」请求，目的是获取客户端软件名称（software name）和版本号（version）。
                //    在这里做是为了避免将这件事情放在API层去做（KIP-511）
                if (header.apiKey == ApiKeys.API_VERSIONS) {
                  val apiVersionsRequest = req.body[ApiVersionsRequest]
                  if (apiVersionsRequest.isValid) {
                    channel.channelMetadataRegistry.registerClientInformation(new ClientInformation(
                      apiVersionsRequest.data.clientSoftwareName,
                      apiVersionsRequest.data.clientSoftwareVersion))
                  }
                }

                // #8 将已组装好的Request放入到缓存队列中，等待I/O线程拉取并处理
                requestChannel.sendRequest(req)

                // #9 静默当前TCP连接（①更新Channel状态;②移除OP_READ事件）
                selector.mute(connectionId)

                // #10 处理Channel的静默事件
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
    // #11 清理completedReceives集合，等待下次poll()轮询时填充数据
    selector.clearCompletedReceives()
  }

  /**
   * 处理已完全发送的Response
   */
  private def processCompletedSends(): Unit = {
    // #1 遍历底层Selector已完全发送的Response
    selector.completedSends.forEach { send =>
      try {
        // #2 从「in-flight」队列中移除
        val response = inflightResponses.remove(send.destinationId).getOrElse {
          throw new IllegalStateException(s"Send for ${send.destinationId} completed, but not in `inflightResponses`")
        }

        // #3 更新统计指标
        updateRequestMetrics(response)

        // #4 触发Response回调方法，Response的回调方法是由Processor线程执行的
        response.onComplete.foreach(onComplete => onComplete(send))

        // #5 尝试对当前通道取消静默。如果当前通道没有违反配额限定（限流），通道会立刻被取消静默。
        //    如果通道已经被限流，那么当限流结束后才会取消静默。
        handleChannelMuteEvent(send.destinationId, ChannelMuteEvent.RESPONSE_SENT)
        tryUnmuteChannel(send.destinationId)
      } catch {
        case e: Throwable => processChannelException(send.destinationId,
          s"Exception while processing completed send to ${send.destinationId}", e)
      }
    }
    // #6 清除「completedSends」集合数据
    selector.clearCompletedSends()
  }

  private def updateRequestMetrics(response: RequestChannel.Response): Unit = {
    val request = response.request
    val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
    request.updateRequestMetrics(networkThreadTimeNanos, response)
  }

  /**
   * 处理已断开的TCP连接
   */
  private def processDisconnected(): Unit = {
    // #1 遍历本轮poll()收集到的已断连的通道
    selector.disconnected.keySet.forEach { connectionId =>
      try {
        // #2 获取断开连接的远端主机名信息
        val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
          throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
        }.remoteHost

        // #3 根据「connectionId」找到对应的Response并移除，且更新相关监控指标
        inflightResponses.remove(connectionId).foreach(updateRequestMetrics)

        // #4 虽然通道已经被关闭了，但是也还需要更新配额数据
        connectionQuotas.dec(listenerName, InetAddress.getByName(remoteHost))
      } catch {
        case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
      }
    }
  }

  /**
   * 关闭超限的TCP连接：找到最近未被使用的TCP连接并关闭。
   * 如果定义未被使用：在最近一段时间内，没有任何数据流经该TCP连接。
   * 类似LRU淘汰算法
   */
  private def closeExcessConnections(): Unit = {
    // #1 判断监听器是否超出配额限制
    if (connectionQuotas.maxConnectionsExceeded(listenerName)) {
      // #2 超出配额限制，让Selector找出最近最少使用的Channel
      val channel = selector.lowestPriorityChannel()
      if (channel != null) {
        // #3 关闭此通道
        close(channel.id)
      }
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
      // 关闭连接
      selector.close(connectionId)

      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
    }
  }

  /**
   * 将新创建的SocketChannel放入阻塞队列（ArrayBlockingQueue）
   *
   * @param socketChannel            待加入的SocketChannel，
   *                                 后续Processor从此通道中获取二进制数据
   * @param mayBlock                 是否需要阻塞
   * @param acceptorIdlePercentMeter 指标监控
   * @return
   */
  def accept(socketChannel: SocketChannel,
             mayBlock: Boolean,
             acceptorIdlePercentMeter: com.yammer.metrics.core.Meter): Boolean = {
    val accepted = {
      // #1 非阻塞将SocketChannel对象插入阻塞队列
      if (newConnections.offer(socketChannel)) {
        // 插入成功，直接返回true
        true
      } else if (mayBlock) {
        // #2 遍历一圈，没有找到合适的Processor（因为并发请求过多，导致没有空闲空间）
        //    尝试一定次数后，如果还找不到，就会阻塞直到有空闲空间为止
        val startNs = time.nanoseconds
        newConnections.put(socketChannel)
        // 指标监控
        acceptorIdlePercentMeter.mark(time.nanoseconds() - startNs)
        true
      } else
        false
    }
    // #3 插入成功，则唤醒轮询器Selector
    if (accepted)
      wakeup()

    // #4 return
    accepted
  }

  /**
   * 从缓存队列中获取SocketChannel并注册到Selector
   */
  private def configureNewConnections(): Unit = {
    var connectionsProcessed = 0
    // #1 每次迭代处理数量不得超过「connectionQueueSize」，
    //    以确保及时处理现有通道的数据以及关闭通知。
    while (connectionsProcessed < connectionQueueSize && !newConnections.isEmpty) {
      // #2 从缓存缓存队列中获取刚创建的SocketChannel
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        // #3 将SocketChannel注册到自己的Selector，并注册OP_READ事件
        selector.register(connectionId(channel.socket), channel)
        connectionsProcessed += 1
      } catch {
        // 捕获所有的异常避免Socket资源泄漏
        case e: Throwable =>
          val remoteAddress = channel.socket.getRemoteSocketAddress
          // 一旦出现异常，关闭Socket
          close(listenerName, channel)
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
    removeMetric(IdlePercentMetricName, Map(NetworkProcessorMetricTag -> id.toString))
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

  /**
   * 指示指定的通道已经发生了指定的通道静音相关的事件，以便它可以改变其静音状态。
   *
   * @param connectionId
   * @param event
   */
  private def handleChannelMuteEvent(connectionId: String, event: ChannelMuteEvent): Unit = {
    //var conn = openOrClosingChannel(connectionId)
    openOrClosingChannel(connectionId).foreach(c => c.handleChannelMuteEvent(event))
  }

  private def tryUnmuteChannel(connectionId: String) = {
    openOrClosingChannel(connectionId).foreach(c => selector.unmute(c.id))
  }

  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * 唤醒轮询器
   */
  override def wakeup() = selector.wakeup()

  override def initiateShutdown(): Unit = {
    super.initiateShutdown()
    removeMetric("IdlePercent", Map("networkProcessor" -> id.toString))
    metrics.removeMetric(expiredConnectionsKilledCountMetricName)
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

/**
 *
 * @param config
 * @param time
 * @param metrics
 */
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
   * @param ip                ip to update or default if None
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
   * @param timeMs       current time in milliseconds
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
   * @param throttleMs   IP throttle time to record for listener
   * @param timeMs       current time in milliseconds
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
   * @param address      ip address to record connection
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
   *
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
   *
   * @param quotaLimit            connection creation rate quota
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
}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException(s"Too many connections from $ip (maximum = $count)")

class ConnectionThrottledException(val ip: InetAddress, val startThrottleTimeMs: Long, val throttleTimeMs: Long)
  extends KafkaException(s"$ip throttled for $throttleTimeMs")
