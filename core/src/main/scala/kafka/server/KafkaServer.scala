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
import java.net.{InetAddress, SocketTimeoutException}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import kafka.api.{KAFKA_0_9_0, KAFKA_2_2_IV0, KAFKA_2_4_IV1}
import kafka.cluster.{Broker, EndPoint}
import kafka.common.{GenerateBrokerIdException, InconsistentBrokerIdException, InconsistentClusterIdException}
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.{ProducerIdManager, TransactionCoordinator}
import kafka.log.LogManager
import kafka.metrics.{KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.network.{ControlPlaneAcceptor, DataPlaneAcceptor, RequestChannel, SocketServer}
import kafka.security.CredentialProvider
import kafka.server.metadata.{ZkConfigRepository, ZkMetadataCache}
import kafka.utils._
import kafka.zk.{AdminZkClient, BrokerInfo, KafkaZkClient}
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, NetworkClient, NetworkClientUtils}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.ControlledShutdownRequestData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ControlledShutdownRequest, ControlledShutdownResponse}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.{JaasContext, JaasUtils}
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time, Utils}
import org.apache.kafka.common.{Endpoint, Node}
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.zookeeper.client.ZKClientConfig

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

object KafkaServer {

  def zkClientConfigFromKafkaConfig(config: KafkaConfig, forceZkSslClientEnable: Boolean = false): ZKClientConfig = {
    val clientConfig = new ZKClientConfig
    if (config.zkSslClientEnable || forceZkSslClientEnable) {
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslClientEnableProp, "true")
      config.zkClientCnxnSocketClassName.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkClientCnxnSocketProp, _))
      config.zkSslKeyStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreLocationProp, _))
      config.zkSslKeyStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStorePasswordProp, x.value))
      config.zkSslKeyStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreTypeProp, _))
      config.zkSslTrustStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreLocationProp, _))
      config.zkSslTrustStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStorePasswordProp, x.value))
      config.zkSslTrustStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreTypeProp, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslProtocolProp, config.ZkSslProtocol)
      config.ZkSslEnabledProtocols.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEnabledProtocolsProp, _))
      config.ZkSslCipherSuites.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCipherSuitesProp, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp, config.ZkSslEndpointIdentificationAlgorithm)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCrlEnableProp, config.ZkSslCrlEnable.toString)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslOcspEnableProp, config.ZkSslOcspEnable.toString)
    }
    // The zk sasl is enabled by default so it can produce false error when broker does not intend to use SASL.
    if (!JaasUtils.isZkSaslEnabled) clientConfig.setProperty(JaasUtils.ZK_SASL_CLIENT, "false")
    clientConfig
  }

  val MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS: Long = 120000
}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(
  val config: KafkaConfig,
  time: Time = Time.SYSTEM,
  threadNamePrefix: Option[String] = None,
  enableForwarding: Boolean = false
) extends KafkaBroker with Server {

  private val startupComplete = new AtomicBoolean(false)
  private val isShuttingDown = new AtomicBoolean(false)
  private val isStartingUp = new AtomicBoolean(false)

  @volatile private var _brokerState: BrokerState = BrokerState.NOT_RUNNING
  private var shutdownLatch = new CountDownLatch(1)
  private var logContext: LogContext = null

  private val kafkaMetricsReporters: Seq[KafkaMetricsReporter] =
    KafkaMetricsReporter.startReporters(VerifiableProperties(config.originals))
  var kafkaYammerMetrics: KafkaYammerMetrics = null
  var metrics: Metrics = null

  @volatile var dataPlaneRequestProcessor: KafkaApis = null
  var controlPlaneRequestProcessor: KafkaApis = null

  var authorizer: Option[Authorizer] = None
  @volatile var socketServer: SocketServer = null
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = null
  var controlPlaneRequestHandlerPool: KafkaRequestHandlerPool = null

  var logDirFailureChannel: LogDirFailureChannel = null
  @volatile private var _logManager: LogManager = null

  @volatile private var _replicaManager: ReplicaManager = null
  var adminManager: ZkAdminManager = null
  var tokenManager: DelegationTokenManager = null

  var dynamicConfigHandlers: Map[String, ConfigHandler] = null
  var dynamicConfigManager: ZkConfigManager = null
  var credentialProvider: CredentialProvider = null
  var tokenCache: DelegationTokenCache = null

  @volatile var groupCoordinator: GroupCoordinator = null

  var transactionCoordinator: TransactionCoordinator = null

  @volatile private var _kafkaController: KafkaController = null

  var forwardingManager: Option[ForwardingManager] = None

  var autoTopicCreationManager: AutoTopicCreationManager = null

  var clientToControllerChannelManager: BrokerToControllerChannelManager = null

  var alterIsrManager: AlterIsrManager = null

  var kafkaScheduler: KafkaScheduler = null

  @volatile var metadataCache: ZkMetadataCache = null
  var quotaManagers: QuotaFactory.QuotaManagers = null

  val zkClientConfig: ZKClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(config)
  private var _zkClient: KafkaZkClient = null
  private var configRepository: ZkConfigRepository = null

  val correlationId: AtomicInteger = new AtomicInteger(0)
  val brokerMetaPropsFile = "meta.properties"
  val brokerMetadataCheckpoints = config.logDirs.map { logDir =>
    (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator + brokerMetaPropsFile)))
  }.toMap

  private var _clusterId: String = null
  @volatile var _brokerTopicStats: BrokerTopicStats = null

  private var _featureChangeListener: FinalizedFeatureChangeListener = null

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault()
  val featureCache: FinalizedFeatureCache = new FinalizedFeatureCache(brokerFeatures)

  override def brokerState: BrokerState = _brokerState

  def clusterId: String = _clusterId

  // Visible for testing
  private[kafka] def zkClient = _zkClient

  override def brokerTopicStats = _brokerTopicStats

  private[kafka] def featureChangeListener = _featureChangeListener

  override def replicaManager: ReplicaManager = _replicaManager

  override def logManager: LogManager = _logManager

  def kafkaController: KafkaController = _kafkaController

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  override def startup(): Unit = {
    try {
      info("starting")

      if (isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

      if (startupComplete.get)
        return

      val canStartup = isStartingUp.compareAndSet(false, true)
      if (canStartup) {
        _brokerState = BrokerState.STARTING

        /* setup zookeeper */
        initZkClient(time)
        configRepository = new ZkConfigRepository(new AdminZkClient(zkClient))

        /* initialize features */
        _featureChangeListener = new FinalizedFeatureChangeListener(featureCache, _zkClient)
        if (config.isFeatureVersioningSupported) {
          _featureChangeListener.initOrThrow(config.zkConnectionTimeoutMs)
        }

        /* Get or create cluster_id */
        _clusterId = getOrGenerateClusterId(zkClient)
        info(s"Cluster ID = ${clusterId}")

        /* load metadata */
        val (preloadedBrokerMetadataCheckpoint, initialOfflineDirs) =
          BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(config.logDirs, ignoreMissing = true)

        if (preloadedBrokerMetadataCheckpoint.version != 0) {
          throw new RuntimeException(s"Found unexpected version in loaded `meta.properties`: " +
            s"$preloadedBrokerMetadataCheckpoint. Zk-based brokers only support version 0 " +
            "(which is implicit when the `version` field is missing).")
        }

        /* check cluster id */
        if (preloadedBrokerMetadataCheckpoint.clusterId.isDefined && preloadedBrokerMetadataCheckpoint.clusterId.get != clusterId)
          throw new InconsistentClusterIdException(
            s"The Cluster ID ${clusterId} doesn't match stored clusterId ${preloadedBrokerMetadataCheckpoint.clusterId} in meta.properties. " +
            s"The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong.")

        /* generate brokerId */
        config.brokerId = getOrGenerateBrokerId(preloadedBrokerMetadataCheckpoint)
        logContext = new LogContext(s"[KafkaServer id=${config.brokerId}] ")
        this.logIdent = logContext.logPrefix

        // initialize dynamic broker configs from ZooKeeper. Any updates made after this will be
        // applied after ZkConfigManager starts.
        config.dynamicConfig.initialize(Some(zkClient))

        /* start scheduler */
        kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
        kafkaScheduler.startup()

        /* create and configure metrics */
        kafkaYammerMetrics = KafkaYammerMetrics.INSTANCE
        kafkaYammerMetrics.configure(config.originals)
        metrics = Server.initializeMetrics(config, time, clusterId)

        /* register broker metrics */
        _brokerTopicStats = new BrokerTopicStats

        quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))
        KafkaBroker.notifyClusterListeners(clusterId, kafkaMetricsReporters ++ metrics.reporters.asScala)

        logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

        /* start log manager */
        _logManager = LogManager(
          config,
          initialOfflineDirs,
          configRepository,
          kafkaScheduler,
          time,
          brokerTopicStats,
          logDirFailureChannel,
          config.usesTopicId)
        _brokerState = BrokerState.RECOVERY
        logManager.startup(zkClient.getAllTopicsInCluster())

        metadataCache = MetadataCache.zkMetadataCache(config.brokerId)
        // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
        // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
        tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
        credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

        clientToControllerChannelManager = BrokerToControllerChannelManager(
          controllerNodeProvider = MetadataCacheControllerNodeProvider(config, metadataCache),
          time = time,
          metrics = metrics,
          config = config,
          channelName = "forwarding",
          threadNamePrefix = threadNamePrefix,
          retryTimeoutMs = config.requestTimeoutMs.longValue)
        clientToControllerChannelManager.start()

        /* start forwarding manager */
        var autoTopicCreationChannel = Option.empty[BrokerToControllerChannelManager]
        if (enableForwarding) {
          this.forwardingManager = Some(ForwardingManager(clientToControllerChannelManager))
          autoTopicCreationChannel = Some(clientToControllerChannelManager)
        }

        val apiVersionManager = ApiVersionManager(
          ListenerType.ZK_BROKER,
          config,
          forwardingManager,
          brokerFeatures,
          featureCache
        )

        // Create and start the socket server acceptor threads so that the bound port is known.
        // Delay starting processors until the end of the initialization sequence to ensure
        // that credentials have been loaded before processing authentications.
        //
        // Note that we allow the use of KRaft mode controller APIs when forwarding is enabled
        // so that the Envelope request is exposed. This is only used in testing currently.
        socketServer = new SocketServer(config, metrics, time, credentialProvider, apiVersionManager)
        socketServer.startup(startProcessingRequests = false)

        // Start alter partition manager based on the IBP version
        alterIsrManager = if (config.interBrokerProtocolVersion.isAlterIsrSupported) {
          AlterIsrManager(
            config = config,
            metadataCache = metadataCache,
            scheduler = kafkaScheduler,
            time = time,
            metrics = metrics,
            threadNamePrefix = threadNamePrefix,
            brokerEpochSupplier = () => kafkaController.brokerEpoch
          )
        } else {
          AlterIsrManager(kafkaScheduler, time, zkClient)
        }
        alterIsrManager.start()

        /* start replica manager */
        _replicaManager = createReplicaManager(isShuttingDown)
        replicaManager.startup()

        val brokerInfo = createBrokerInfo
        val brokerEpoch = zkClient.registerBroker(brokerInfo)

        // Now that the broker is successfully registered, checkpoint its metadata
        checkpointBrokerMetadata(ZkMetaProperties(clusterId, config.brokerId))

        /* start token manager */
        tokenManager = new DelegationTokenManager(config, tokenCache, time , zkClient)
        tokenManager.startup()

        /* start kafka controller */
        _kafkaController = new KafkaController(config, zkClient, time, metrics, brokerInfo, brokerEpoch, tokenManager, brokerFeatures, featureCache, threadNamePrefix)
        kafkaController.startup()

        adminManager = new ZkAdminManager(config, metrics, metadataCache, zkClient)

        /* start group coordinator */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        groupCoordinator = GroupCoordinator(config, replicaManager, Time.SYSTEM, metrics)
        groupCoordinator.startup(() => zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicPartitions))

        /* create producer ids manager */
        val producerIdManager = if (config.interBrokerProtocolVersion.isAllocateProducerIdsSupported) {
          ProducerIdManager.rpc(
            config.brokerId,
            brokerEpochSupplier = () => kafkaController.brokerEpoch,
            clientToControllerChannelManager,
            config.requestTimeoutMs
          )
        } else {
          ProducerIdManager.zk(config.brokerId, zkClient)
        }
        /* start transaction coordinator, with a separate background thread scheduler for transaction expiration and log loading */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        transactionCoordinator = TransactionCoordinator(config, replicaManager, new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-"),
          () => producerIdManager, metrics, metadataCache, Time.SYSTEM)
        transactionCoordinator.startup(
          () => zkClient.getTopicPartitionCount(Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionTopicPartitions))

        /* start auto topic creation manager */
        this.autoTopicCreationManager = AutoTopicCreationManager(
          config,
          metadataCache,
          threadNamePrefix,
          autoTopicCreationChannel,
          Some(adminManager),
          Some(kafkaController),
          groupCoordinator,
          transactionCoordinator
        )

        /* Get the authorizer and initialize it if one is specified.*/
        authorizer = config.authorizer
        authorizer.foreach(_.configure(config.originals))
        val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
          case Some(authZ) =>
            authZ.start(brokerInfo.broker.toServerInfo(clusterId, config)).asScala.map { case (ep, cs) =>
              ep -> cs.toCompletableFuture
            }
          case None =>
            brokerInfo.broker.endPoints.map { ep =>
              ep.toJava -> CompletableFuture.completedFuture[Void](null)
            }.toMap
        }

        val fetchManager = new FetchManager(Time.SYSTEM,
          new FetchSessionCache(config.maxIncrementalFetchSessionCacheSlots,
            KafkaServer.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS))

        /* start processing requests */
        val zkSupport = ZkSupport(adminManager, kafkaController, zkClient, forwardingManager, metadataCache)

        def createKafkaApis(requestChannel: RequestChannel): KafkaApis = new KafkaApis(
          requestChannel = requestChannel,
          metadataSupport = zkSupport,
          replicaManager = replicaManager,
          groupCoordinator = groupCoordinator,
          txnCoordinator = transactionCoordinator,
          autoTopicCreationManager = autoTopicCreationManager,
          brokerId = config.brokerId,
          config = config,
          configRepository = configRepository,
          metadataCache = metadataCache,
          metrics = metrics,
          authorizer = authorizer,
          quotas = quotaManagers,
          fetchManager = fetchManager,
          brokerTopicStats = brokerTopicStats,
          clusterId = clusterId,
          time = time,
          tokenManager = tokenManager,
          apiVersionManager = apiVersionManager)

        dataPlaneRequestProcessor = createKafkaApis(socketServer.dataPlaneRequestChannel)

        dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
          config.numIoThreads, s"${DataPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent", DataPlaneAcceptor.ThreadPrefix)

        socketServer.controlPlaneRequestChannelOpt.foreach { controlPlaneRequestChannel =>
          controlPlaneRequestProcessor = createKafkaApis(controlPlaneRequestChannel)
          controlPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.controlPlaneRequestChannelOpt.get, controlPlaneRequestProcessor, time,
            1, s"${ControlPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent", ControlPlaneAcceptor.ThreadPrefix)
        }

        Mx4jLoader.maybeLoad()

        /* Add all reconfigurables for config change notification before starting config handlers */
        config.dynamicConfig.addReconfigurables(this)
        Option(logManager.cleaner).foreach(config.dynamicConfig.addBrokerReconfigurable)

        /* start dynamic config manager */
        dynamicConfigHandlers = Map[String, ConfigHandler](ConfigType.Topic -> new TopicConfigHandler(logManager, config, quotaManagers, Some(kafkaController)),
                                                           ConfigType.Client -> new ClientIdConfigHandler(quotaManagers),
                                                           ConfigType.User -> new UserConfigHandler(quotaManagers, credentialProvider),
                                                           ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers),
                                                           ConfigType.Ip -> new IpConfigHandler(socketServer.connectionQuotas))

        // Create the config manager. start listening to notifications
        dynamicConfigManager = new ZkConfigManager(zkClient, dynamicConfigHandlers)
        dynamicConfigManager.startup()

        socketServer.startProcessingRequests(authorizerFutures)

        _brokerState = BrokerState.RUNNING
        shutdownLatch = new CountDownLatch(1)
        startupComplete.set(true)
        isStartingUp.set(false)
        AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
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

  protected def createReplicaManager(isShuttingDown: AtomicBoolean): ReplicaManager = {
    new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = kafkaScheduler,
      logManager = logManager,
      quotaManagers = quotaManagers,
      metadataCache = metadataCache,
      logDirFailureChannel = logDirFailureChannel,
      alterIsrManager = alterIsrManager,
      brokerTopicStats = brokerTopicStats,
      isShuttingDown = isShuttingDown,
      zkClient = Some(zkClient),
      threadNamePrefix = threadNamePrefix)
  }

  private def initZkClient(time: Time): Unit = {
    info(s"Connecting to zookeeper on ${config.zkConnect}")

    val secureAclsEnabled = config.zkEnableSecureAcls
    val isZkSecurityEnabled = JaasUtils.isZkSaslEnabled() || KafkaConfig.zkTlsClientAuthEnabled(zkClientConfig)

    if (secureAclsEnabled && !isZkSecurityEnabled)
      throw new java.lang.SecurityException(s"${KafkaConfig.ZkEnableSecureAclsProp} is true, but ZooKeeper client TLS configuration identifying at least $KafkaConfig.ZkSslClientEnableProp, $KafkaConfig.ZkClientCnxnSocketProp, and $KafkaConfig.ZkSslKeyStoreLocationProp was not present and the " +
        s"verification of the JAAS login file failed ${JaasUtils.zkSecuritySysConfigString}")

    _zkClient = KafkaZkClient(config.zkConnect, secureAclsEnabled, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
      config.zkMaxInFlightRequests, time, name = "Kafka server", zkClientConfig = zkClientConfig,
      createChrootIfNecessary = true)
    _zkClient.createTopLevelPaths()
  }

  private def getOrGenerateClusterId(zkClient: KafkaZkClient): String = {
    zkClient.getClusterId.getOrElse(zkClient.createOrGetClusterId(CoreUtils.generateUuidAsBase64()))
  }

  def createBrokerInfo: BrokerInfo = {
    val endPoints = config.effectiveAdvertisedListeners.map(e => s"${e.host}:${e.port}")
    zkClient.getAllBrokersInCluster.filter(_.id != config.brokerId).foreach { broker =>
      val commonEndPoints = broker.endPoints.map(e => s"${e.host}:${e.port}").intersect(endPoints)
      require(commonEndPoints.isEmpty, s"Configured end points ${commonEndPoints.mkString(",")} in" +
        s" advertised listeners are already registered by broker ${broker.id}")
    }

    val listeners = config.effectiveAdvertisedListeners.map { endpoint =>
      if (endpoint.port == 0)
        endpoint.copy(port = socketServer.boundPort(endpoint.listenerName))
      else
        endpoint
    }

    val updatedEndpoints = listeners.map(endpoint =>
      if (Utils.isBlank(endpoint.host))
        endpoint.copy(host = InetAddress.getLocalHost.getCanonicalHostName)
      else
        endpoint
    )

    val jmxPort = System.getProperty("com.sun.management.jmxremote.port", "-1").toInt
    BrokerInfo(
      Broker(config.brokerId, updatedEndpoints, config.rack, brokerFeatures.supportedFeatures),
      config.interBrokerProtocolVersion,
      jmxPort)
  }

  /**
   * Performs controlled shutdown
   */
  private def controlledShutdown(): Unit = {
    val socketTimeoutMs = config.controllerSocketTimeoutMs

    def doControlledShutdown(retries: Int): Boolean = {
      val metadataUpdater = new ManualMetadataUpdater()
      val networkClient = {
        val channelBuilder = ChannelBuilders.clientChannelBuilder(
          config.interBrokerSecurityProtocol,
          JaasContext.Type.SERVER,
          config,
          config.interBrokerListenerName,
          config.saslMechanismInterBrokerProtocol,
          time,
          config.saslInterBrokerHandshakeRequestEnable,
          logContext)
        val selector = new Selector(
          NetworkReceive.UNLIMITED,
          config.connectionsMaxIdleMs,
          metrics,
          time,
          "kafka-server-controlled-shutdown",
          Map.empty.asJava,
          false,
          channelBuilder,
          logContext
        )
        new NetworkClient(
          selector,
          metadataUpdater,
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
          logContext)
      }

      var shutdownSucceeded: Boolean = false

      try {

        var remainingRetries = retries
        var prevController: Node = null
        var ioException = false

        while (!shutdownSucceeded && remainingRetries > 0) {
          remainingRetries = remainingRetries - 1

          // 1. Find the controller and establish a connection to it.
          // If the controller id or the broker registration are missing, we sleep and retry (if there are remaining retries)
          metadataCache.getControllerId match {
            case Some(controllerId) =>
              metadataCache.getAliveBrokerNode(controllerId, config.interBrokerListenerName) match {
                case Some(broker) =>
                  // if this is the first attempt, if the controller has changed or if an exception was thrown in a previous
                  // attempt, connect to the most recent controller
                  if (ioException || broker != prevController) {

                    ioException = false

                    if (prevController != null)
                      networkClient.close(prevController.idString)

                    prevController = broker
                    metadataUpdater.setNodes(Seq(prevController).asJava)
                  }
                case None =>
                  info(s"Broker registration for controller $controllerId is not available in the metadata cache")
              }
            case None =>
              info("No controller present in the metadata cache")
          }

          // 2. issue a controlled shutdown to the controller
          if (prevController != null) {
            try {

              if (!NetworkClientUtils.awaitReady(networkClient, prevController, time, socketTimeoutMs))
                throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

              // send the controlled shutdown request
              val controlledShutdownApiVersion: Short =
                if (config.interBrokerProtocolVersion < KAFKA_0_9_0) 0
                else if (config.interBrokerProtocolVersion < KAFKA_2_2_IV0) 1
                else if (config.interBrokerProtocolVersion < KAFKA_2_4_IV1) 2
                else 3

              val controlledShutdownRequest = new ControlledShutdownRequest.Builder(
                  new ControlledShutdownRequestData()
                    .setBrokerId(config.brokerId)
                    .setBrokerEpoch(kafkaController.brokerEpoch),
                    controlledShutdownApiVersion)
              val request = networkClient.newClientRequest(prevController.idString, controlledShutdownRequest,
                time.milliseconds(), true)
              val clientResponse = NetworkClientUtils.sendAndReceive(networkClient, request, time)

              val shutdownResponse = clientResponse.responseBody.asInstanceOf[ControlledShutdownResponse]
              if (shutdownResponse.error != Errors.NONE) {
                info(s"Controlled shutdown request returned after ${clientResponse.requestLatencyMs}ms " +
                  s"with error ${shutdownResponse.error}")
              } else if (shutdownResponse.data.remainingPartitions.isEmpty) {
                shutdownSucceeded = true
                info("Controlled shutdown request returned successfully " +
                  s"after ${clientResponse.requestLatencyMs}ms")
              } else {
                info(s"Controlled shutdown request returned after ${clientResponse.requestLatencyMs}ms " +
                  s"with ${shutdownResponse.data.remainingPartitions.size} partitions remaining to move")

                if (isDebugEnabled) {
                  debug("Remaining partitions to move during controlled shutdown: " +
                    s"${shutdownResponse.data.remainingPartitions}")
                }
              }
            }
            catch {
              case ioe: IOException =>
                ioException = true
                warn("Error during controlled shutdown, possibly because leader movement took longer than the " +
                  s"configured controller.socket.timeout.ms and/or request.timeout.ms: ${ioe.getMessage}")
                // ignore and try again
            }
          }
          if (!shutdownSucceeded && remainingRetries > 0) {
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            info(s"Retrying controlled shutdown ($remainingRetries retries remaining)")
          }
        }
      }
      finally
        networkClient.close()

      shutdownSucceeded
    }

    if (startupComplete.get() && config.controlledShutdownEnable) {
      // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
      // of time and try again for a configured number of retries. If all the attempt fails, we simply force
      // the shutdown.
      info("Starting controlled shutdown")

      _brokerState = BrokerState.PENDING_CONTROLLED_SHUTDOWN

      val shutdownSucceeded = doControlledShutdown(config.controlledShutdownMaxRetries.intValue)

      if (!shutdownSucceeded)
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  override def shutdown(): Unit = {
    try {
      info("shutting down")

      if (isStartingUp.get)
        throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

      // To ensure correct behavior under concurrent calls, we need to check `shutdownLatch` first since it gets updated
      // last in the `if` block. If the order is reversed, we could shutdown twice or leave `isShuttingDown` set to
      // `true` at the end of this method.
      if (shutdownLatch.getCount > 0 && isShuttingDown.compareAndSet(false, true)) {
        CoreUtils.swallow(controlledShutdown(), this)
        _brokerState = BrokerState.SHUTTING_DOWN

        if (dynamicConfigManager != null)
          CoreUtils.swallow(dynamicConfigManager.shutdown(), this)

        // Stop socket server to stop accepting any more connections and requests.
        // Socket server will be shutdown towards the end of the sequence.
        if (socketServer != null)
          CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
        if (dataPlaneRequestHandlerPool != null)
          CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
        if (controlPlaneRequestHandlerPool != null)
          CoreUtils.swallow(controlPlaneRequestHandlerPool.shutdown(), this)

        /**
         * We must shutdown the scheduler early because otherwise, the scheduler could touch other
         * resources that might have been shutdown and cause exceptions.
         * For example, if we didn't shutdown the scheduler first, when LogManager was closing
         * partitions one by one, the scheduler might concurrently delete old segments due to
         * retention. However, the old segments could have been closed by the LogManager, which would
         * cause an IOException and subsequently mark logdir as offline. As a result, the broker would
         * not flush the remaining partitions or write the clean shutdown marker. Ultimately, the
         * broker would have to take hours to recover the log during restart.
         */
        if (kafkaScheduler != null)
          CoreUtils.swallow(kafkaScheduler.shutdown(), this)

        if (dataPlaneRequestProcessor != null)
          CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
        if (controlPlaneRequestProcessor != null)
          CoreUtils.swallow(controlPlaneRequestProcessor.close(), this)
        CoreUtils.swallow(authorizer.foreach(_.close()), this)
        if (adminManager != null)
          CoreUtils.swallow(adminManager.shutdown(), this)

        if (transactionCoordinator != null)
          CoreUtils.swallow(transactionCoordinator.shutdown(), this)
        if (groupCoordinator != null)
          CoreUtils.swallow(groupCoordinator.shutdown(), this)

        if (tokenManager != null)
          CoreUtils.swallow(tokenManager.shutdown(), this)

        if (replicaManager != null)
          CoreUtils.swallow(replicaManager.shutdown(), this)

        if (alterIsrManager != null)
          CoreUtils.swallow(alterIsrManager.shutdown(), this)

        if (clientToControllerChannelManager != null)
          CoreUtils.swallow(clientToControllerChannelManager.shutdown(), this)

        if (logManager != null)
          CoreUtils.swallow(logManager.shutdown(), this)

        if (kafkaController != null)
          CoreUtils.swallow(kafkaController.shutdown(), this)

        if (featureChangeListener != null)
          CoreUtils.swallow(featureChangeListener.close(), this)

        if (zkClient != null)
          CoreUtils.swallow(zkClient.close(), this)

        if (quotaManagers != null)
          CoreUtils.swallow(quotaManagers.shutdown(), this)

        // Even though socket server is stopped much earlier, controller can generate
        // response for controlled shutdown request. Shutdown server at the end to
        // avoid any failures (e.g. when metrics are recorded)
        if (socketServer != null)
          CoreUtils.swallow(socketServer.shutdown(), this)
        if (metrics != null)
          CoreUtils.swallow(metrics.close(), this)
        if (brokerTopicStats != null)
          CoreUtils.swallow(brokerTopicStats.close(), this)

        // Clear all reconfigurable instances stored in DynamicBrokerConfig
        config.dynamicConfig.clear()

        _brokerState = BrokerState.NOT_RUNNING

        startupComplete.set(false)
        isShuttingDown.set(false)
        CoreUtils.swallow(AppInfoParser.unregisterAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics), this)
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
  override def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager: LogManager = logManager

  override def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

  /** Return advertised listeners with the bound port (this may differ from the configured port if the latter is `0`). */
  def advertisedListeners: Seq[EndPoint] = {
    config.effectiveAdvertisedListeners.map { endPoint =>
      endPoint.copy(port = boundPort(endPoint.listenerName))
    }
  }

  /**
   * Checkpoint the BrokerMetadata to all the online log.dirs
   *
   * @param brokerMetadata
   */
  private def checkpointBrokerMetadata(brokerMetadata: ZkMetaProperties) = {
    for (logDir <- config.logDirs if logManager.isLogDirOnline(new File(logDir).getAbsolutePath)) {
      val checkpoint = brokerMetadataCheckpoints(logDir)
      checkpoint.write(brokerMetadata.toProperties)
    }
  }

  /**
   * Generates new brokerId if enabled or reads from meta.properties based on following conditions
   * <ol>
   * <li> config has no broker.id provided and broker id generation is enabled, generates a broker.id based on Zookeeper's sequence
   * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
   * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
   * <ol>
   *
   * @return The brokerId.
   */
  private def getOrGenerateBrokerId(brokerMetadata: RawMetaProperties): Int = {
    val brokerId = config.brokerId

    if (brokerId >= 0 && brokerMetadata.brokerId.exists(_ != brokerId))
      throw new InconsistentBrokerIdException(
        s"Configured broker.id $brokerId doesn't match stored broker.id ${brokerMetadata.brokerId} in meta.properties. " +
          s"If you moved your data, make sure your configured broker.id matches. " +
          s"If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    else if (brokerMetadata.brokerId.isDefined)
      brokerMetadata.brokerId.get
    else if (brokerId < 0 && config.brokerIdGenerationEnable) // generate a new brokerId from Zookeeper
      generateBrokerId()
    else
      brokerId
  }

  /**
    * Return a sequence id generated by updating the broker sequence id path in ZK.
    * Users can provide brokerId in the config. To avoid conflicts between ZK generated
    * sequence id and configured brokerId, we increment the generated sequence id by KafkaConfig.MaxReservedBrokerId.
    */
  private def generateBrokerId(): Int = {
    try {
      zkClient.generateBrokerSequenceId() + config.maxReservedBrokerId
    } catch {
      case e: Exception =>
        error("Failed to generate broker.id due to ", e)
        throw new GenerateBrokerIdException("Failed to generate broker.id", e)
    }
  }
}
