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

package kafka.server

import java.io.{File, IOException}
import java.net.SocketTimeoutException
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.yammer.metrics.core.Gauge
import kafka.admin.AdminUtils
import kafka.api.KAFKA_0_9_0
import kafka.cluster.Broker
import kafka.common.{GenerateBrokerIdException, InconsistentBrokerIdException}
import kafka.controller.{ControllerStats, KafkaController}
import kafka.coordinator.GroupCoordinator
import kafka.log.{CleanerConfig, LogConfig, LogManager}
import kafka.metrics.{KafkaMetricsGroup, KafkaMetricsReporter}
import kafka.network.{BlockingChannel, SocketServer}
import kafka.security.CredentialProvider
import kafka.security.auth.Authorizer
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.{ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, _}
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ControlledShutdownRequest, ControlledShutdownResponse}
import org.apache.kafka.common.security.{JaasContext, JaasUtils}
import org.apache.kafka.common.utils.{AppInfoParser, Time}
import org.apache.kafka.common.{ClusterResource, Node}

import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}

object KafkaServer {
  // Copy the subset of properties that are relevant to Logs
  // I'm listing out individual properties here since the names are slightly different in each Config class...
  private[kafka] def copyKafkaConfigToLog(kafkaConfig: KafkaConfig): java.util.Map[String, Object] = {
    val logProps = new util.HashMap[String, Object]()
    logProps.put(LogConfig.SegmentBytesProp, kafkaConfig.logSegmentBytes)
    logProps.put(LogConfig.SegmentMsProp, kafkaConfig.logRollTimeMillis)
    logProps.put(LogConfig.SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis)
    logProps.put(LogConfig.SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes)
    logProps.put(LogConfig.FlushMessagesProp, kafkaConfig.logFlushIntervalMessages)
    logProps.put(LogConfig.FlushMsProp, kafkaConfig.logFlushIntervalMs)
    logProps.put(LogConfig.RetentionBytesProp, kafkaConfig.logRetentionBytes)
    logProps.put(LogConfig.RetentionMsProp, kafkaConfig.logRetentionTimeMillis: java.lang.Long)
    logProps.put(LogConfig.MaxMessageBytesProp, kafkaConfig.messageMaxBytes)
    logProps.put(LogConfig.IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes)
    logProps.put(LogConfig.DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs)
    logProps.put(LogConfig.MinCompactionLagMsProp, kafkaConfig.logCleanerMinCompactionLagMs)
    logProps.put(LogConfig.FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs)
    logProps.put(LogConfig.MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio)
    logProps.put(LogConfig.CleanupPolicyProp, kafkaConfig.logCleanupPolicy)
    logProps.put(LogConfig.MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas)
    logProps.put(LogConfig.CompressionTypeProp, kafkaConfig.compressionType)
    logProps.put(LogConfig.UncleanLeaderElectionEnableProp, kafkaConfig.uncleanLeaderElectionEnable)
    logProps.put(LogConfig.PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable)
    logProps.put(LogConfig.MessageFormatVersionProp, kafkaConfig.logMessageFormatVersion.version)
    logProps.put(LogConfig.MessageTimestampTypeProp, kafkaConfig.logMessageTimestampType.name)
    logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs)
    logProps
  }

  private[server] def metricConfig(kafkaConfig: KafkaConfig): MetricConfig = {
    new MetricConfig()
      .samples(kafkaConfig.metricNumSamples)
      .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel))
      .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS)
  }

}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, time: Time = Time.SYSTEM, threadNamePrefix: Option[String] = None, kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List()) extends Logging with KafkaMetricsGroup {
  private val startupComplete = new AtomicBoolean(false)
  private val isShuttingDown = new AtomicBoolean(false)
  private val isStartingUp = new AtomicBoolean(false)

  private var shutdownLatch = new CountDownLatch(1)

  private val jmxPrefix: String = "kafka.server"
  private val reporters: java.util.List[MetricsReporter] = config.metricReporterClasses
  reporters.add(new JmxReporter(jmxPrefix))

  var metrics: Metrics = null

  private val metricConfig: MetricConfig = KafkaServer.metricConfig(config)

  val brokerState: BrokerState = new BrokerState

  var apis: KafkaApis = null
  var authorizer: Option[Authorizer] = None
  var socketServer: SocketServer = null
  var requestHandlerPool: KafkaRequestHandlerPool = null

  var logManager: LogManager = null

  var replicaManager: ReplicaManager = null
  var adminManager: AdminManager = null

  var dynamicConfigHandlers: Map[String, ConfigHandler] = null
  var dynamicConfigManager: DynamicConfigManager = null
  var credentialProvider: CredentialProvider = null

  var groupCoordinator: GroupCoordinator = null

  var kafkaController: KafkaController = null

  val kafkaScheduler = new KafkaScheduler(config.backgroundThreads)

  var kafkaHealthcheck: KafkaHealthcheck = null
  var metadataCache: MetadataCache = null
  var quotaManagers: QuotaFactory.QuotaManagers = null

  var zkUtils: ZkUtils = null
  val correlationId: AtomicInteger = new AtomicInteger(0)
  val brokerMetaPropsFile = "meta.properties"
  val brokerMetadataCheckpoints = config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator +brokerMetaPropsFile)))).toMap

  private var _clusterId: String = null

  def clusterId: String = _clusterId

  newGauge(
    "BrokerState",
    new Gauge[Int] {
      def value = brokerState.currentState
    }
  )

  newGauge(
    "ClusterId",
    new Gauge[String] {
      def value = clusterId
    }
  )

  newGauge(
    "yammer-metrics-count",
    new Gauge[Int] {
      def value = {
        com.yammer.metrics.Metrics.defaultRegistry().allMetrics().size()
      }
    }
  )

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup() {
    try {
      info("starting")

      if(isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

      if(startupComplete.get)
        return

      val canStartup = isStartingUp.compareAndSet(false, true)
      if (canStartup) {
        metrics = new Metrics(metricConfig, reporters, time, true)
        quotaManagers = QuotaFactory.instantiate(config, metrics, time)

        brokerState.newState(Starting)

        /* start scheduler */
        kafkaScheduler.startup()

        /* setup zookeeper */
        zkUtils = initZk()

        /* Get or create cluster_id */
        _clusterId = getOrGenerateClusterId(zkUtils)
        info(s"Cluster ID = $clusterId")

        notifyClusterListeners(kafkaMetricsReporters ++ reporters.asScala)

        /* start log manager */
        logManager = createLogManager(zkUtils.zkClient, brokerState)
        logManager.startup()

        /* generate brokerId */
        config.brokerId =  getBrokerId
        this.logIdent = "[Kafka Server " + config.brokerId + "], "

        metadataCache = new MetadataCache(config.brokerId)
        credentialProvider = new CredentialProvider(config.saslEnabledMechanisms)

        socketServer = new SocketServer(config, metrics, time, credentialProvider)
        socketServer.startup()

        /* start replica manager */
        replicaManager = createReplicaManager(isShuttingDown)
        replicaManager.startup()

        /* start kafka controller */
        kafkaController = new KafkaController(config, zkUtils, brokerState, time, metrics, threadNamePrefix)
        kafkaController.startup()

        adminManager = new AdminManager(config, metrics, metadataCache, zkUtils)

        /* start group coordinator */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        groupCoordinator = GroupCoordinator(config, zkUtils, replicaManager, Time.SYSTEM)
        groupCoordinator.startup()

        /* Get the authorizer and initialize it if one is specified.*/
        authorizer = Option(config.authorizerClassName).filter(_.nonEmpty).map { authorizerClassName =>
          val authZ = CoreUtils.createObject[Authorizer](authorizerClassName)
          authZ.configure(config.originals())
          authZ
        }

        /* start processing requests */
        apis = new KafkaApis(socketServer.requestChannel, replicaManager, adminManager, groupCoordinator,
          kafkaController, zkUtils, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
          clusterId, time)

        requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, time,
          config.numIoThreads)

        Mx4jLoader.maybeLoad()

        /* start dynamic config manager */
        dynamicConfigHandlers = Map[String, ConfigHandler](ConfigType.Topic -> new TopicConfigHandler(logManager, config, quotaManagers),
                                                           ConfigType.Client -> new ClientIdConfigHandler(quotaManagers),
                                                           ConfigType.User -> new UserConfigHandler(quotaManagers, credentialProvider),
                                                           ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers))

        // Create the config manager. start listening to notifications
        dynamicConfigManager = new DynamicConfigManager(zkUtils, dynamicConfigHandlers)
        dynamicConfigManager.startup()

        /* tell everyone we are alive */
        val listeners = config.advertisedListeners.map { endpoint =>
          if (endpoint.port == 0)
            endpoint.copy(port = socketServer.boundPort(endpoint.listenerName))
          else
            endpoint
        }
        kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, listeners, zkUtils, config.rack,
          config.interBrokerProtocolVersion)
        kafkaHealthcheck.startup()

        // Now that the broker id is successfully registered via KafkaHealthcheck, checkpoint it
        checkpointBrokerId(config.brokerId)

        /* register broker metrics */
        registerStats()

        brokerState.newState(RunningAsBroker)
        shutdownLatch = new CountDownLatch(1)
        startupComplete.set(true)
        isStartingUp.set(false)
        AppInfoParser.registerAppInfo(jmxPrefix, config.brokerId.toString)
        info("started")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        isStartingUp.set(false)
        shutdown()
        throw e
    }
  }

  private def notifyClusterListeners(clusterListeners: Seq[AnyRef]): Unit = {
    val clusterResourceListeners = new ClusterResourceListeners
    clusterResourceListeners.maybeAddAll(clusterListeners.asJava)
    clusterResourceListeners.onUpdate(new ClusterResource(clusterId))
  }

  protected def createReplicaManager(isShuttingDown: AtomicBoolean): ReplicaManager =
    new ReplicaManager(config, metrics, time, zkUtils, kafkaScheduler, logManager, isShuttingDown, quotaManagers.follower)

  private def initZk(): ZkUtils = {
    info(s"Connecting to zookeeper on ${config.zkConnect}")

    val chrootIndex = config.zkConnect.indexOf("/")
    val chrootOption = {
      if (chrootIndex > 0) Some(config.zkConnect.substring(chrootIndex))
      else None
    }

    val secureAclsEnabled = config.zkEnableSecureAcls
    val isZkSecurityEnabled = JaasUtils.isZkSecurityEnabled()

    if (secureAclsEnabled && !isZkSecurityEnabled)
      throw new java.lang.SecurityException(s"${KafkaConfig.ZkEnableSecureAclsProp} is true, but the verification of the JAAS login file failed.")

    chrootOption.foreach { chroot =>
      val zkConnForChrootCreation = config.zkConnect.substring(0, chrootIndex)
      val zkClientForChrootCreation = ZkUtils(zkConnForChrootCreation,
                                              sessionTimeout = config.zkSessionTimeoutMs,
                                              connectionTimeout = config.zkConnectionTimeoutMs,
                                              secureAclsEnabled)
      zkClientForChrootCreation.makeSurePersistentPathExists(chroot)
      info(s"Created zookeeper path $chroot")
      zkClientForChrootCreation.zkClient.close()
    }

    val zkUtils = ZkUtils(config.zkConnect,
                          sessionTimeout = config.zkSessionTimeoutMs,
                          connectionTimeout = config.zkConnectionTimeoutMs,
                          secureAclsEnabled)
    zkUtils.setupCommonPaths()
    zkUtils
  }

  def getOrGenerateClusterId(zkUtils: ZkUtils): String = {
    zkUtils.getClusterId.getOrElse(zkUtils.createOrGetClusterId(CoreUtils.generateUuidAsBase64))
  }

  /**
   *  Forces some dynamic jmx beans to be registered on server startup.
   */
  private def registerStats() {
    BrokerTopicStats.getBrokerAllTopicsStats()
    ControllerStats.uncleanLeaderElectionRate
    ControllerStats.leaderElectionTimer
  }

  /**
   *  Performs controlled shutdown
   */
  private def controlledShutdown() {

    def node(broker: Broker): Node = {
      val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerListenerName)
      new Node(brokerEndPoint.id, brokerEndPoint.host, brokerEndPoint.port)
    }

    val socketTimeoutMs = config.controllerSocketTimeoutMs

    def networkClientControlledShutdown(retries: Int): Boolean = {
      val metadataUpdater = new ManualMetadataUpdater()
      val networkClient = {
        val channelBuilder = ChannelBuilders.clientChannelBuilder(
          config.interBrokerSecurityProtocol,
          JaasContext.Type.SERVER,
          config,
          config.interBrokerListenerName,
          config.saslMechanismInterBrokerProtocol,
          config.saslInterBrokerHandshakeRequestEnable)
        val selector = new Selector(
          NetworkReceive.UNLIMITED,
          config.connectionsMaxIdleMs,
          metrics,
          time,
          "kafka-server-controlled-shutdown",
          Map.empty.asJava,
          false,
          channelBuilder
        )
        new NetworkClient(
          selector,
          metadataUpdater,
          config.brokerId.toString,
          1,
          0,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          config.requestTimeoutMs,
          time,
          false)
      }

      var shutdownSucceeded: Boolean = false

      try {

        var remainingRetries = retries
        var prevController: Broker = null
        var ioException = false

        while (!shutdownSucceeded && remainingRetries > 0) {
          remainingRetries = remainingRetries - 1

          import NetworkClientBlockingOps._

          // 1. Find the controller and establish a connection to it.

          // Get the current controller info. This is to ensure we use the most recent info to issue the
          // controlled shutdown request
          val controllerId = zkUtils.getController()
          zkUtils.getBrokerInfo(controllerId) match {
            case Some(broker) =>
              // if this is the first attempt, if the controller has changed or if an exception was thrown in a previous
              // attempt, connect to the most recent controller
              if (ioException || broker != prevController) {

                ioException = false

                if (prevController != null)
                  networkClient.close(node(prevController).idString)

                prevController = broker
                metadataUpdater.setNodes(Seq(node(prevController)).asJava)
              }
            case None => //ignore and try again
          }

          // 2. issue a controlled shutdown to the controller
          if (prevController != null) {
            try {

              if (!networkClient.blockingReady(node(prevController), socketTimeoutMs)(time))
                throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

              // send the controlled shutdown request
              val controlledShutdownRequest = new ControlledShutdownRequest.Builder(config.brokerId)
              val request = networkClient.newClientRequest(node(prevController).idString, controlledShutdownRequest,
                time.milliseconds(), true)
              val clientResponse = networkClient.blockingSendAndReceive(request)(time)

              val shutdownResponse = clientResponse.responseBody.asInstanceOf[ControlledShutdownResponse]
              if (shutdownResponse.errorCode == Errors.NONE.code && shutdownResponse.partitionsRemaining.isEmpty) {
                shutdownSucceeded = true
                info("Controlled shutdown succeeded")
              }
              else {
                info("Remaining partitions to move: %s".format(shutdownResponse.partitionsRemaining.asScala.mkString(",")))
                info("Error code from controller: %d".format(shutdownResponse.errorCode))
              }
            }
            catch {
              case ioe: IOException =>
                ioException = true
                warn("Error during controlled shutdown, possibly because leader movement took longer than the configured socket.timeout.ms: %s".format(ioe.getMessage))
                // ignore and try again
            }
          }
          if (!shutdownSucceeded) {
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            warn("Retrying controlled shutdown after the previous attempt failed...")
          }
        }
      }
      finally
        networkClient.close()

      shutdownSucceeded
    }

    def blockingChannelControlledShutdown(retries: Int): Boolean = {
      var remainingRetries = retries
      var channel: BlockingChannel = null
      var prevController: Broker = null
      var shutdownSucceeded: Boolean = false
      try {
        while (!shutdownSucceeded && remainingRetries > 0) {
          remainingRetries = remainingRetries - 1

          // 1. Find the controller and establish a connection to it.

          // Get the current controller info. This is to ensure we use the most recent info to issue the
          // controlled shutdown request
          val controllerId = zkUtils.getController()
          zkUtils.getBrokerInfo(controllerId) match {
            case Some(broker) =>
              if (channel == null || prevController == null || !prevController.equals(broker)) {
                // if this is the first attempt or if the controller has changed, create a channel to the most recent
                // controller
                if (channel != null)
                  channel.disconnect()

                val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerListenerName)
                channel = new BlockingChannel(brokerEndPoint.host,
                  brokerEndPoint.port,
                  BlockingChannel.UseDefaultBufferSize,
                  BlockingChannel.UseDefaultBufferSize,
                  config.controllerSocketTimeoutMs)
                channel.connect()
                prevController = broker
              }
            case None => //ignore and try again
          }

          // 2. issue a controlled shutdown to the controller
          if (channel != null) {
            var response: NetworkReceive = null
            try {
              // send the controlled shutdown request
              val request = new kafka.api.ControlledShutdownRequest(0, correlationId.getAndIncrement, None, config.brokerId)
              channel.send(request)

              response = channel.receive()
              val shutdownResponse = kafka.api.ControlledShutdownResponse.readFrom(response.payload())
              if (shutdownResponse.errorCode == Errors.NONE.code && shutdownResponse.partitionsRemaining != null &&
                shutdownResponse.partitionsRemaining.isEmpty) {
                shutdownSucceeded = true
                info ("Controlled shutdown succeeded")
              }
              else {
                info("Remaining partitions to move: %s".format(shutdownResponse.partitionsRemaining.mkString(",")))
                info("Error code from controller: %d".format(shutdownResponse.errorCode))
              }
            }
            catch {
              case ioe: java.io.IOException =>
                channel.disconnect()
                channel = null
                warn("Error during controlled shutdown, possibly because leader movement took longer than the configured socket.timeout.ms: %s".format(ioe.getMessage))
                // ignore and try again
            }
          }
          if (!shutdownSucceeded) {
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            warn("Retrying controlled shutdown after the previous attempt failed...")
          }
        }
      }
      finally {
        if (channel != null) {
          channel.disconnect()
          channel = null
        }
      }
      shutdownSucceeded
    }

    if (startupComplete.get() && config.controlledShutdownEnable) {
      // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
      // of time and try again for a configured number of retries. If all the attempt fails, we simply force
      // the shutdown.
      info("Starting controlled shutdown")

      brokerState.newState(PendingControlledShutdown)

      val shutdownSucceeded =
        // Before 0.9.0.0, `ControlledShutdownRequest` did not contain `client_id` and it's a mandatory field in
        // `RequestHeader`, which is used by `NetworkClient`
        if (config.interBrokerProtocolVersion >= KAFKA_0_9_0)
          networkClientControlledShutdown(config.controlledShutdownMaxRetries.intValue)
        else blockingChannelControlledShutdown(config.controlledShutdownMaxRetries.intValue)

      if (!shutdownSucceeded)
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")

    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    try {
      info("shutting down")

      if (isStartingUp.get)
        throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

      val canShutdown = isShuttingDown.compareAndSet(false, true)
      if (canShutdown && shutdownLatch.getCount > 0) {
        CoreUtils.swallow(controlledShutdown())
        brokerState.newState(BrokerShuttingDown)
        if(socketServer != null)
          CoreUtils.swallow(socketServer.shutdown())
        if(requestHandlerPool != null)
          CoreUtils.swallow(requestHandlerPool.shutdown())
        CoreUtils.swallow(kafkaScheduler.shutdown())
        if(apis != null)
          CoreUtils.swallow(apis.close())
        CoreUtils.swallow(authorizer.foreach(_.close()))
        if(replicaManager != null)
          CoreUtils.swallow(replicaManager.shutdown())
        if (adminManager != null)
          CoreUtils.swallow(adminManager.shutdown())
        if(groupCoordinator != null)
          CoreUtils.swallow(groupCoordinator.shutdown())
        if(logManager != null)
          CoreUtils.swallow(logManager.shutdown())
        if(kafkaController != null)
          CoreUtils.swallow(kafkaController.shutdown())
        if(zkUtils != null)
          CoreUtils.swallow(zkUtils.close())
        if (metrics != null)
          CoreUtils.swallow(metrics.close())

        brokerState.newState(NotRunning)

        startupComplete.set(false)
        isShuttingDown.set(false)
        AppInfoParser.unregisterAppInfo(jmxPrefix, config.brokerId.toString)
        shutdownLatch.countDown()
        info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer shutdown.", e)
        isShuttingDown.set(false)
        throw e
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager(): LogManager = logManager

  def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

  private def createLogManager(zkClient: ZkClient, brokerState: BrokerState): LogManager = {
    val defaultProps = KafkaServer.copyKafkaConfigToLog(config)
    val defaultLogConfig = LogConfig(defaultProps)

    val configs = AdminUtils.fetchAllTopicConfigs(zkUtils).map { case (topic, configs) =>
      topic -> LogConfig.fromProps(defaultProps, configs)
    }
    // read the log configurations from zookeeper
    val cleanerConfig = CleanerConfig(numThreads = config.logCleanerThreads,
                                      dedupeBufferSize = config.logCleanerDedupeBufferSize,
                                      dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
                                      ioBufferSize = config.logCleanerIoBufferSize,
                                      maxMessageSize = config.messageMaxBytes,
                                      maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
                                      backOffMs = config.logCleanerBackoffMs,
                                      enableCleaner = config.logCleanerEnable)
    new LogManager(logDirs = config.logDirs.map(new File(_)).toArray,
                   topicConfigs = configs,
                   defaultConfig = defaultLogConfig,
                   cleanerConfig = cleanerConfig,
                   ioThreads = config.numRecoveryThreadsPerDataDir,
                   flushCheckMs = config.logFlushSchedulerIntervalMs,
                   flushCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
                   retentionCheckMs = config.logCleanupIntervalMs,
                   scheduler = kafkaScheduler,
                   brokerState = brokerState,
                   time = time)
  }

  /**
    * Generates new brokerId if enabled or reads from meta.properties based on following conditions
    * <ol>
    * <li> config has no broker.id provided and broker id generation is enabled, generates a broker.id based on Zookeeper's sequence
    * <li> stored broker.id in meta.properties doesn't match in all the log.dirs throws InconsistentBrokerIdException
    * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
    * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
    * <ol>
    *
    * @return A brokerId.
    */
  private def getBrokerId: Int =  {
    var brokerId = config.brokerId
    val brokerIdSet = mutable.HashSet[Int]()

    for (logDir <- config.logDirs) {
      val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
      brokerMetadataOpt.foreach { brokerMetadata =>
        brokerIdSet.add(brokerMetadata.brokerId)
      }
    }

    if(brokerIdSet.size > 1)
      throw new InconsistentBrokerIdException(
        s"Failed to match broker.id across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
        s"or partial data was manually copied from another broker. Found $brokerIdSet")
    else if(brokerId >= 0 && brokerIdSet.size == 1 && brokerIdSet.last != brokerId)
      throw new InconsistentBrokerIdException(
        s"Configured broker.id $brokerId doesn't match stored broker.id ${brokerIdSet.last} in meta.properties. " +
        s"If you moved your data, make sure your configured broker.id matches. " +
        s"If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    else if(brokerIdSet.isEmpty && brokerId < 0 && config.brokerIdGenerationEnable)  // generate a new brokerId from Zookeeper
      brokerId = generateBrokerId
    else if(brokerIdSet.size == 1) // pick broker.id from meta.properties
      brokerId = brokerIdSet.last

    brokerId
  }

  private def checkpointBrokerId(brokerId: Int) {
    var logDirsWithoutMetaProps: List[String] = List()

    for (logDir <- config.logDirs) {
      val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
      if(brokerMetadataOpt.isEmpty)
          logDirsWithoutMetaProps ++= List(logDir)
    }

    for(logDir <- logDirsWithoutMetaProps) {
      val checkpoint = brokerMetadataCheckpoints(logDir)
      checkpoint.write(BrokerMetadata(brokerId))
    }
  }

  private def generateBrokerId: Int = {
    try {
      zkUtils.getBrokerSequenceId(config.maxReservedBrokerId)
    } catch {
      case e: Exception =>
        error("Failed to generate broker.id due to ", e)
        throw new GenerateBrokerIdException("Failed to generate broker.id", e)
    }
  }
}
