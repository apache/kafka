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

import kafka.cluster.{Broker, EndPoint}
import kafka.common.GenerateBrokerIdException
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinatorAdapter
import kafka.coordinator.transaction.{TransactionCoordinator, ZkProducerIdManager}
import kafka.log.LogManager
import kafka.log.remote.RemoteLogManager
import kafka.metrics.KafkaMetricsReporter
import kafka.network.{ControlPlaneAcceptor, DataPlaneAcceptor, RequestChannel, SocketServer}
import kafka.raft.KafkaRaftManager
import kafka.server.metadata.{OffsetTrackingListener, ZkConfigRepository, ZkMetadataCache}
import kafka.utils._
import kafka.zk.{AdminZkClient, BrokerInfo, KafkaZkClient}
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, MetadataRecoveryStrategy, NetworkClient, NetworkClientUtils}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{Listener, ListenerCollection}
import org.apache.kafka.common.message.ControlledShutdownRequestData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ControlledShutdownRequest, ControlledShutdownResponse}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.{JaasContext, JaasUtils}
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time, Utils}
import org.apache.kafka.common.{Endpoint, Node, TopicPartition}
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.coordinator.transaction.ProducerIdManager
import org.apache.kafka.image.loader.metrics.MetadataLoaderMetrics
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag.REQUIRE_V0
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble}
import org.apache.kafka.metadata.{BrokerState, MetadataRecordSerde, VersionRange}
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.raft.Endpoints
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.BrokerFeatures
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.MetadataVersion._
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion, NodeToControllerChannelManager}
import org.apache.kafka.server.config.{ConfigType, ZkConfigs}
import org.apache.kafka.server.fault.LoggingFaultHandler
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.util.KafkaScheduler
import org.apache.kafka.storage.internals.log.LogDirFailureChannel
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.apache.zookeeper.client.ZKClientConfig

import java.io.{File, IOException}
import java.net.{InetAddress, SocketTimeoutException}
import java.nio.file.{Files, Paths}
import java.time.Duration
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.{Optional, OptionalInt, OptionalLong}
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption

object KafkaServer {
  def zkClientConfigFromKafkaConfig(config: KafkaConfig, forceZkSslClientEnable: Boolean = false): ZKClientConfig = {
    val clientConfig = new ZKClientConfig
    if (config.zkSslClientEnable || forceZkSslClientEnable) {
      KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG, "true")
      config.zkClientCnxnSocketClassName.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_CLIENT_CNXN_SOCKET_CONFIG, _))
      config.zkSslKeyStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_KEY_STORE_LOCATION_CONFIG, _))
      config.zkSslKeyStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_KEY_STORE_PASSWORD_CONFIG, x.value))
      config.zkSslKeyStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_KEY_STORE_TYPE_CONFIG, _))
      config.zkSslTrustStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_TRUST_STORE_LOCATION_CONFIG, _))
      config.zkSslTrustStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_TRUST_STORE_PASSWORD_CONFIG, x.value))
      config.zkSslTrustStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_TRUST_STORE_TYPE_CONFIG, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_PROTOCOL_CONFIG, config.ZkSslProtocol)
      config.ZkSslEnabledProtocols.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_CONFIG, _))
      config.ZkSslCipherSuites.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_CIPHER_SUITES_CONFIG, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, config.ZkSslEndpointIdentificationAlgorithm)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_CRL_ENABLE_CONFIG, config.ZkSslCrlEnable.toString)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, ZkConfigs.ZK_SSL_OCSP_ENABLE_CONFIG, config.ZkSslOcspEnable.toString)
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
  private var logContext: LogContext = _

  private val kafkaMetricsReporters: Seq[KafkaMetricsReporter] =
    KafkaMetricsReporter.startReporters(VerifiableProperties(config.originals))
  var kafkaYammerMetrics: KafkaYammerMetrics = _
  var metrics: Metrics = _

  @volatile var dataPlaneRequestProcessor: KafkaApis = _
  private var controlPlaneRequestProcessor: KafkaApis = _

  var authorizer: Option[Authorizer] = None
  @volatile var socketServer: SocketServer = _
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = _
  private var controlPlaneRequestHandlerPool: KafkaRequestHandlerPool = _

  var logDirFailureChannel: LogDirFailureChannel = _
  @volatile private var _logManager: LogManager = _
  var remoteLogManagerOpt: Option[RemoteLogManager] = None

  @volatile private var _replicaManager: ReplicaManager = _
  var adminManager: ZkAdminManager = _
  var tokenManager: DelegationTokenManager = _

  var dynamicConfigHandlers: Map[String, ConfigHandler] = _
  private var dynamicConfigManager: ZkConfigManager = _
  var credentialProvider: CredentialProvider = _
  var tokenCache: DelegationTokenCache = _

  @volatile var groupCoordinator: GroupCoordinator = _

  var transactionCoordinator: TransactionCoordinator = _

  @volatile private var _kafkaController: KafkaController = _

  var forwardingManager: Option[ForwardingManager] = None

  var autoTopicCreationManager: AutoTopicCreationManager = _

  var clientToControllerChannelManager: NodeToControllerChannelManager = _

  var alterPartitionManager: AlterPartitionManager = _

  var kafkaScheduler: KafkaScheduler = _

  @volatile var metadataCache: ZkMetadataCache = _

  @volatile var quorumControllerNodeProvider: RaftControllerNodeProvider = _

  var quotaManagers: QuotaFactory.QuotaManagers = _

  val zkClientConfig: ZKClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(config)
  private var _zkClient: KafkaZkClient = _
  private var configRepository: ZkConfigRepository = _

  val correlationId: AtomicInteger = new AtomicInteger(0)

  private var _clusterId: String = _
  @volatile private var _brokerTopicStats: BrokerTopicStats = _

  private var _featureChangeListener: FinalizedFeatureChangeListener = _

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createEmpty()

  override def brokerState: BrokerState = _brokerState

  def clusterId: String = _clusterId

  // Visible for testing
  private[kafka] def zkClient = _zkClient

  override def brokerTopicStats: BrokerTopicStats = _brokerTopicStats

  private[kafka] def featureChangeListener = _featureChangeListener

  override def replicaManager: ReplicaManager = _replicaManager

  override def logManager: LogManager = _logManager

  @volatile def kafkaController: KafkaController = _kafkaController

  var lifecycleManager: BrokerLifecycleManager = _
  private var raftManager: KafkaRaftManager[ApiMessageAndVersion] = _

  @volatile var brokerEpochManager: ZkBrokerEpochManager = _

  def brokerEpochSupplier(): Long = Option(brokerEpochManager).map(_.get()).getOrElse(-1)

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

        /* Get or create cluster_id */
        _clusterId = getOrGenerateClusterId(zkClient)
        info(s"Cluster ID = $clusterId")

        /* load metadata */
        val initialMetaPropsEnsemble = {
          val loader = new MetaPropertiesEnsemble.Loader()
          loader.addLogDirs(config.logDirs.asJava)
          if (config.migrationEnabled) {
            loader.addMetadataLogDir(config.metadataLogDir)
          }
          loader.load()
        }

        val verificationId = if (config.brokerId < 0) {
          OptionalInt.empty()
        } else {
          OptionalInt.of(config.brokerId)
        }
        val verificationFlags = if (config.migrationEnabled) {
          util.EnumSet.noneOf(classOf[VerificationFlag])
        } else {
          util.EnumSet.of(REQUIRE_V0)
        }
        initialMetaPropsEnsemble.verify(Optional.of(_clusterId), verificationId, verificationFlags)

        /* generate brokerId */
        config.brokerId = getOrGenerateBrokerId(initialMetaPropsEnsemble)
        logContext = new LogContext(s"[KafkaServer id=${config.brokerId}] ")
        this.logIdent = logContext.logPrefix

        // initialize dynamic broker configs from ZooKeeper. Any updates made after this will be
        // applied after ZkConfigManager starts.
        config.dynamicConfig.initialize(Some(zkClient), clientMetricsReceiverPluginOpt = None)

        /* start scheduler */
        kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
        kafkaScheduler.startup()

        /* create and configure metrics */
        kafkaYammerMetrics = KafkaYammerMetrics.INSTANCE
        kafkaYammerMetrics.configure(config.originals)
        metrics = Server.initializeMetrics(config, time, clusterId)
        createCurrentControllerIdMetric()

        /* register broker metrics */
        _brokerTopicStats = new BrokerTopicStats(config.remoteLogManagerConfig.isRemoteStorageSystemEnabled())

        quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))
        KafkaBroker.notifyClusterListeners(clusterId, kafkaMetricsReporters ++ metrics.reporters.asScala)

        logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

        // Make sure all storage directories have meta.properties files.
        val metaPropsEnsemble = {
          val copier = new MetaPropertiesEnsemble.Copier(initialMetaPropsEnsemble)
          initialMetaPropsEnsemble.nonFailedDirectoryProps().forEachRemaining(e => {
            val logDir = e.getKey
            val builder = new MetaProperties.Builder(e.getValue).
              setClusterId(_clusterId).
              setNodeId(config.brokerId)
            if (!builder.directoryId().isPresent) {
              if (config.migrationEnabled) {
                builder.setDirectoryId(copier.generateValidDirectoryId())
              }
            }
            copier.setLogDirProps(logDir, builder.build())
          })
          copier.emptyLogDirs().clear()
          copier.setPreWriteHandler((logDir, _, _) => {
            info(s"Rewriting $logDir${File.separator}meta.properties")
            Files.createDirectories(Paths.get(logDir))
          })
          copier.setWriteErrorHandler((logDir, e) => {
            logDirFailureChannel.maybeAddOfflineLogDir(logDir, s"Error while writing meta.properties to $logDir", e)
          })
          copier.writeLogDirChanges()
          copier.copy()
        }
        metaPropsEnsemble.verify(Optional.of(_clusterId), OptionalInt.of(config.brokerId), verificationFlags)

        /* start log manager */
        _logManager = LogManager(
          config,
          metaPropsEnsemble.errorLogDirs().asScala.toSeq,
          configRepository,
          kafkaScheduler,
          time,
          brokerTopicStats,
          logDirFailureChannel,
          config.usesTopicId)
        _brokerState = BrokerState.RECOVERY
        logManager.startup(zkClient.getAllTopicsInCluster())

        remoteLogManagerOpt = createRemoteLogManager()

        metadataCache = MetadataCache.zkMetadataCache(
          config.brokerId,
          config.interBrokerProtocolVersion,
          brokerFeatures,
          config.migrationEnabled)
        val controllerNodeProvider = new MetadataCacheControllerNodeProvider(metadataCache, config,
          () => Option(quorumControllerNodeProvider).map(_.getControllerInfo()))

        /* initialize feature change listener */
        _featureChangeListener = new FinalizedFeatureChangeListener(metadataCache, _zkClient)
        if (config.isFeatureVersioningSupported) {
          _featureChangeListener.initOrThrow(config.zkConnectionTimeoutMs)
        }

        // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
        // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
        tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
        credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

        clientToControllerChannelManager = new NodeToControllerChannelManagerImpl(
          controllerNodeProvider = controllerNodeProvider,
          time = time,
          metrics = metrics,
          config = config,
          channelName = "forwarding",
          s"zk-broker-${config.nodeId}-",
          retryTimeoutMs = config.requestTimeoutMs.longValue
        )
        clientToControllerChannelManager.start()

        /* start forwarding manager */
        var autoTopicCreationChannel = Option.empty[NodeToControllerChannelManager]
        if (enableForwarding) {
          this.forwardingManager = Some(ForwardingManager(clientToControllerChannelManager, metrics))
          autoTopicCreationChannel = Some(clientToControllerChannelManager)
        }

        val apiVersionManager = ApiVersionManager(
          ListenerType.ZK_BROKER,
          config,
          forwardingManager,
          brokerFeatures,
          metadataCache,
          None
        )

        // Create and start the socket server acceptor threads so that the bound port is known.
        // Delay starting processors until the end of the initialization sequence to ensure
        // that credentials have been loaded before processing authentications.
        //
        // Note that we allow the use of KRaft mode controller APIs when forwarding is enabled
        // so that the Envelope request is exposed. This is only used in testing currently.
        socketServer = new SocketServer(config, metrics, time, credentialProvider, apiVersionManager)

        // Start alter partition manager based on the IBP version
        alterPartitionManager = if (config.interBrokerProtocolVersion.isAlterPartitionSupported) {
          AlterPartitionManager(
            config = config,
            metadataCache = metadataCache,
            scheduler = kafkaScheduler,
            controllerNodeProvider,
            time = time,
            metrics = metrics,
            s"zk-broker-${config.nodeId}-",
            brokerEpochSupplier = brokerEpochSupplier
          )
        } else {
          AlterPartitionManager(kafkaScheduler, time, zkClient)
        }
        alterPartitionManager.start()

        // Start replica manager
        _replicaManager = createReplicaManager(isShuttingDown)
        replicaManager.startup()

        val brokerInfo = createBrokerInfo
        val brokerEpoch = zkClient.registerBroker(brokerInfo)

        /* start token manager */
        tokenManager = new DelegationTokenManagerZk(config, tokenCache, time , zkClient)
        tokenManager.startup()

        /* start kafka controller */
        _kafkaController = new KafkaController(config, zkClient, time, metrics, brokerInfo, brokerEpoch, tokenManager, brokerFeatures, metadataCache, threadNamePrefix)
        kafkaController.startup()

        if (config.migrationEnabled) {
          logger.info("Starting up additional components for ZooKeeper migration")
          lifecycleManager = new BrokerLifecycleManager(config,
            time,
            s"zk-broker-${config.nodeId}-",
            isZkBroker = true,
            logManager.directoryIdsSet)

          // For ZK brokers in migration mode, always delete the metadata partition on startup.
          logger.info(s"Deleting local metadata log from ${config.metadataLogDir} since this is a ZK broker in migration mode.")
          KafkaRaftManager.maybeDeleteMetadataLogDir(config)
          logger.info("Successfully deleted local metadata log. It will be re-created.")

          // If the ZK broker is in migration mode, start up a RaftManager to learn about the new KRaft controller
          val quorumVoters = QuorumConfig.parseVoterConnections(config.quorumConfig.voters)
          raftManager = new KafkaRaftManager[ApiMessageAndVersion](
            metaPropsEnsemble.clusterId().get(),
            config,
            // metadata log dir and directory.id must exist because migration is enabled
            metaPropsEnsemble.logDirProps.get(metaPropsEnsemble.metadataLogDir.get).directoryId.get,
            new MetadataRecordSerde,
            KafkaRaftServer.MetadataPartition,
            KafkaRaftServer.MetadataTopicId,
            time,
            metrics,
            threadNamePrefix,
            CompletableFuture.completedFuture(quorumVoters),
            QuorumConfig.parseBootstrapServers(config.quorumConfig.bootstrapServers),
            // Endpoint information is only needed for KRaft controllers (voters). ZK brokers
            // (observers) can never be KRaft controllers
            Endpoints.empty(),
            fatalFaultHandler = new LoggingFaultHandler("raftManager", () => shutdown())
          )
          quorumControllerNodeProvider = RaftControllerNodeProvider(raftManager, config)
          val brokerToQuorumChannelManager = new NodeToControllerChannelManagerImpl(
            controllerNodeProvider = quorumControllerNodeProvider,
            time = time,
            metrics = metrics,
            config = config,
            channelName = "quorum",
            s"zk-broker-${config.nodeId}-",
            retryTimeoutMs = config.requestTimeoutMs.longValue
          )

          val listener = new OffsetTrackingListener()
          raftManager.register(listener)
          raftManager.startup()

          val networkListeners = new ListenerCollection()
          config.effectiveAdvertisedBrokerListeners.foreach { ep =>
            networkListeners.add(new Listener().
              setHost(if (Utils.isBlank(ep.host)) InetAddress.getLocalHost.getCanonicalHostName else ep.host).
              setName(ep.listenerName.value()).
              setPort(if (ep.port == 0) socketServer.boundPort(ep.listenerName) else ep.port).
              setSecurityProtocol(ep.securityProtocol.id))
          }

          val features = BrokerFeatures.createDefaultFeatureMap(BrokerFeatures.createDefault(config.unstableFeatureVersionsEnabled)).asScala

          // Even though ZK brokers don't use "metadata.version" feature, we need to overwrite it with our IBP as part of registration
          // so the KRaft controller can verify that all brokers are on the same IBP before starting the migration.
          val featuresRemapped = features + (MetadataVersion.FEATURE_NAME ->
            VersionRange.of(config.interBrokerProtocolVersion.featureLevel(), config.interBrokerProtocolVersion.featureLevel()))

          lifecycleManager.start(
            () => listener.highestOffset,
            brokerToQuorumChannelManager,
            clusterId,
            networkListeners,
            featuresRemapped.asJava,
            OptionalLong.empty()
          )
          logger.debug("Start RaftManager")
        }

        // Used by ZK brokers during a KRaft migration. When talking to a KRaft controller, we need to use the epoch
        // from BrokerLifecycleManager rather than ZK (via KafkaController)
        brokerEpochManager = new ZkBrokerEpochManager(metadataCache, kafkaController, Option(lifecycleManager))

        adminManager = new ZkAdminManager(config, metrics, metadataCache, zkClient)

        /* start group coordinator */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        groupCoordinator = GroupCoordinatorAdapter(
          config,
          replicaManager,
          Time.SYSTEM,
          metrics
        )
        groupCoordinator.startup(() => zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.groupCoordinatorConfig.offsetsTopicPartitions))

        /* create producer ids manager */
        val producerIdManager = if (config.interBrokerProtocolVersion.isAllocateProducerIdsSupported) {
          ProducerIdManager.rpc(
            config.brokerId,
            time,
            () => brokerEpochSupplier(),
            clientToControllerChannelManager
          )
        } else {
          new ZkProducerIdManager(config.brokerId, zkClient)
        }
        /* start transaction coordinator, with a separate background thread scheduler for transaction expiration and log loading */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        transactionCoordinator = TransactionCoordinator(config, replicaManager, new KafkaScheduler(1, true, "transaction-log-manager-"),
          () => producerIdManager, metrics, metadataCache, Time.SYSTEM)
        transactionCoordinator.startup(
          () => zkClient.getTopicPartitionCount(Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionLogConfig.transactionTopicPartitions))

        /* start auto topic creation manager */
        this.autoTopicCreationManager = AutoTopicCreationManager(
          config,
          autoTopicCreationChannel,
          Some(adminManager),
          Some(kafkaController),
          groupCoordinator,
          transactionCoordinator,
          None
        )

        /* Get the authorizer and initialize it if one is specified.*/
        authorizer = config.createNewAuthorizer()
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

        // The FetchSessionCache is divided into config.numIoThreads shards, each responsible
        // for Math.max(1, shardNum * sessionIdRange) <= sessionId < (shardNum + 1) * sessionIdRange
        val sessionIdRange = Int.MaxValue / NumFetchSessionCacheShards
        val fetchSessionCacheShards = (0 until NumFetchSessionCacheShards)
          .map(shardNum => new FetchSessionCacheShard(
            config.maxIncrementalFetchSessionCacheSlots / NumFetchSessionCacheShards,
            KafkaServer.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS,
            sessionIdRange,
            shardNum
          ))
        val fetchManager = new FetchManager(Time.SYSTEM, new FetchSessionCache(fetchSessionCacheShards))

        // Start RemoteLogManager before broker start serving the requests.
        remoteLogManagerOpt.foreach { rlm =>
          val listenerName = config.remoteLogManagerConfig.remoteLogMetadataManagerListenerName()
          if (listenerName != null) {
            brokerInfo.broker.endPoints
              .find(e => e.listenerName.equals(ListenerName.normalised(listenerName)))
              .orElse(throw new ConfigException(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP,
                listenerName, "Should be set as a listener name within valid broker listener name list: "
                  + brokerInfo.broker.endPoints.map(_.listenerName).mkString(",")))
              .foreach(e => rlm.onEndPointCreated(e.toJava))
          }
          rlm.startup()
        }

        /* start processing requests */
        val zkSupport = ZkSupport(adminManager, kafkaController, zkClient, forwardingManager, metadataCache, brokerEpochManager)

        def createKafkaApis(requestChannel: RequestChannel): KafkaApis = new KafkaApis(
          requestChannel = requestChannel,
          metadataSupport = zkSupport,
          replicaManager = replicaManager,
          groupCoordinator = groupCoordinator,
          txnCoordinator = transactionCoordinator,
          shareCoordinator = None,  //share coord only supported in kraft mode
          autoTopicCreationManager = autoTopicCreationManager,
          brokerId = config.brokerId,
          config = config,
          configRepository = configRepository,
          metadataCache = metadataCache,
          metrics = metrics,
          authorizer = authorizer,
          quotas = quotaManagers,
          fetchManager = fetchManager,
          sharePartitionManager = None,
          brokerTopicStats = brokerTopicStats,
          clusterId = clusterId,
          time = time,
          tokenManager = tokenManager,
          apiVersionManager = apiVersionManager,
          clientMetricsManager = None)

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
        dynamicConfigHandlers = Map[String, ConfigHandler](ConfigType.TOPIC -> new TopicConfigHandler(replicaManager, config, quotaManagers, Some(kafkaController)),
                                                           ConfigType.CLIENT -> new ClientIdConfigHandler(quotaManagers),
                                                           ConfigType.USER -> new UserConfigHandler(quotaManagers, credentialProvider),
                                                           ConfigType.BROKER -> new BrokerConfigHandler(config, quotaManagers),
                                                           ConfigType.IP -> new IpConfigHandler(socketServer.connectionQuotas))

        // Create the config manager. start listening to notifications
        dynamicConfigManager = new ZkConfigManager(zkClient, dynamicConfigHandlers)
        dynamicConfigManager.startup()

        if (config.migrationEnabled && lifecycleManager != null) {
          lifecycleManager.initialCatchUpFuture.whenComplete { case (_, t) =>
            if (t != null) {
              fatal("Encountered an exception when waiting to catch up with KRaft metadata log", t)
              shutdown()
            } else {
              info("Finished catching up on KRaft metadata log, requesting that the KRaft controller unfence this broker")
              lifecycleManager.setReadyToUnfence()
            }
          }
        }

        val enableRequestProcessingFuture = socketServer.enableRequestProcessing(authorizerFutures)
        // Block here until all the authorizer futures are complete
        try {
          info("Start processing authorizer futures")
          CompletableFuture.allOf(authorizerFutures.values.toSeq: _*).join()
          info("End processing authorizer futures")
        } catch {
          case t: Throwable => throw new RuntimeException("Received a fatal error while " +
            "waiting for all of the authorizer futures to be completed.", t)
        }
        // Wait for all the SocketServer ports to be open, and the Acceptors to be started.
        try {
          info("Start processing enable request processing future")
          enableRequestProcessingFuture.join()
          info("End processing enable request processing future")
        } catch {
          case t: Throwable => throw new RuntimeException("Received a fatal error while " +
            "waiting for the SocketServer Acceptors to be started.", t)
        }

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

  private def createCurrentControllerIdMetric(): Unit = {
    KafkaYammerMetrics.defaultRegistry().newGauge(MetadataLoaderMetrics.CURRENT_CONTROLLER_ID,
      () => getCurrentControllerIdFromOldController())
  }

  /**
   * Get the current controller ID from the old controller code.
   * This is the most up-to-date controller ID we can get when in ZK mode.
   */
  def getCurrentControllerIdFromOldController(): Int = {
    Option(_kafkaController) match {
      case None => -1
      case Some(controller) => controller.activeControllerId
    }
  }

  private def unregisterCurrentControllerIdMetric(): Unit = {
    KafkaYammerMetrics.defaultRegistry().removeMetric(MetadataLoaderMetrics.CURRENT_CONTROLLER_ID)
  }

  protected def createRemoteLogManager(): Option[RemoteLogManager] = {
    if (config.remoteLogManagerConfig.isRemoteStorageSystemEnabled()) {
      Some(new RemoteLogManager(config.remoteLogManagerConfig, config.brokerId, config.logDirs.head, clusterId, time,
        (tp: TopicPartition) => logManager.getLog(tp).toJava,
        (tp: TopicPartition, remoteLogStartOffset: java.lang.Long) => {
          logManager.getLog(tp).foreach { log =>
            log.updateLogStartOffsetFromRemoteTier(remoteLogStartOffset)
          }
      },
        brokerTopicStats, metrics))
    } else {
      None
    }
  }

  protected def createReplicaManager(isShuttingDown: AtomicBoolean): ReplicaManager = {
    val addPartitionsLogContext = new LogContext(s"[AddPartitionsToTxnManager broker=${config.brokerId}]")
    val addPartitionsToTxnNetworkClient = NetworkUtils.buildNetworkClient("AddPartitionsManager", config, metrics, time, addPartitionsLogContext)
    val addPartitionsToTxnManager = new AddPartitionsToTxnManager(
      config,
      addPartitionsToTxnNetworkClient,
      metadataCache,
      // The transaction coordinator is not created at this point so we must
      // use a lambda here.
      transactionalId => transactionCoordinator.partitionFor(transactionalId),
      time
    )

    new ReplicaManager(
      metrics = metrics,
      config = config,
      time = time,
      scheduler = kafkaScheduler,
      logManager = logManager,
      remoteLogManager = remoteLogManagerOpt,
      quotaManagers = quotaManagers,
      metadataCache = metadataCache,
      logDirFailureChannel = logDirFailureChannel,
      alterPartitionManager = alterPartitionManager,
      brokerTopicStats = brokerTopicStats,
      isShuttingDown = isShuttingDown,
      zkClient = Some(zkClient),
      delayedRemoteFetchPurgatoryParam = None,
      threadNamePrefix = threadNamePrefix,
      brokerEpochSupplier = brokerEpochSupplier,
      addPartitionsToTxnManager = Some(addPartitionsToTxnManager))
  }

  private def initZkClient(time: Time): Unit = {
    info(s"Connecting to zookeeper on ${config.zkConnect}")
    _zkClient = KafkaZkClient.createZkClient("Kafka server", time, config, zkClientConfig)
    _zkClient.createTopLevelPaths()
  }

  private def getOrGenerateClusterId(zkClient: KafkaZkClient): String = {
    zkClient.getClusterId.getOrElse(zkClient.createOrGetClusterId(CoreUtils.generateUuidAsBase64()))
  }

  def createBrokerInfo: BrokerInfo = {
    val endPoints = config.effectiveAdvertisedBrokerListeners.map(e => s"${e.host}:${e.port}")
    zkClient.getAllBrokersInCluster.filter(_.id != config.brokerId).foreach { broker =>
      val commonEndPoints = broker.endPoints.map(e => s"${e.host}:${e.port}").intersect(endPoints)
      require(commonEndPoints.isEmpty, s"Configured end points ${commonEndPoints.mkString(",")} in" +
        s" advertised listeners are already registered by broker ${broker.id}")
    }

    val listeners = config.effectiveAdvertisedBrokerListeners.map { endpoint =>
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
      if (config.requiresZookeeper &&
        metadataCache.getControllerId.exists(_.isInstanceOf[KRaftCachedControllerId])) {
        info("ZkBroker currently has a KRaft controller. Controlled shutdown will be handled " +
          "through broker lifecycle manager")
        return true
      }
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
          logContext,
          MetadataRecoveryStrategy.NONE)
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
            case Some(controllerId: ZkCachedControllerId)  =>
              metadataCache.getAliveBrokerNode(controllerId.id, config.interBrokerListenerName) match {
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
            case Some(_: KRaftCachedControllerId) | None =>
              info("No zk controller present in the metadata cache")
          }

          // 2. issue a controlled shutdown to the controller
          if (prevController != null) {
            try {

              if (!NetworkClientUtils.awaitReady(networkClient, prevController, time, socketTimeoutMs))
                throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

              // send the controlled shutdown request
              val controlledShutdownApiVersion: Short =
                if (config.interBrokerProtocolVersion.isLessThan(IBP_0_9_0)) 0
                else if (config.interBrokerProtocolVersion.isLessThan(IBP_2_2_IV0)) 1
                else if (config.interBrokerProtocolVersion.isLessThan(IBP_2_4_IV1)) 2
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

      if (config.migrationEnabled && lifecycleManager != null && metadataCache.getControllerId.exists(_.isInstanceOf[KRaftCachedControllerId])) {
        // For now we'll send the heartbeat with WantShutDown set so the KRaft controller can see a broker
        // shutting down without waiting for the heartbeat to time out.
        info("Notifying KRaft of controlled shutdown")
        lifecycleManager.beginControlledShutdown()
        try {
          lifecycleManager.controlledShutdownFuture.get(5L, TimeUnit.MINUTES)
        } catch {
          case _: TimeoutException =>
            error("Timed out waiting for the controller to approve controlled shutdown")
          case e: Throwable =>
            error("Got unexpected exception waiting for controlled shutdown future", e)
        }
      }

      val shutdownSucceeded = doControlledShutdown(config.controlledShutdownMaxRetries.intValue)

      if (!shutdownSucceeded)
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  override def shutdown(timeout: Duration): Unit = {
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
        authorizer.foreach(Utils.closeQuietly(_, "authorizer"))
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

        if (alterPartitionManager != null)
          CoreUtils.swallow(alterPartitionManager.shutdown(), this)

        if (forwardingManager.isDefined)
          CoreUtils.swallow(forwardingManager.get.close(), this)

        if (clientToControllerChannelManager != null)
          CoreUtils.swallow(clientToControllerChannelManager.shutdown(), this)

        if (logManager != null)
          CoreUtils.swallow(logManager.shutdown(), this)

        if (kafkaController != null)
          CoreUtils.swallow(kafkaController.shutdown(), this)

        // Close remote log manager before stopping processing requests, to give a chance to any
        // of its underlying clients (especially in RemoteStorageManager and RemoteLogMetadataManager)
        // to close gracefully.
        remoteLogManagerOpt.foreach(Utils.closeQuietly(_, "remote log manager"))

        if (featureChangeListener != null)
          CoreUtils.swallow(featureChangeListener.close(), this)

        Utils.closeQuietly(zkClient, "zk client")

        if (quotaManagers != null)
          CoreUtils.swallow(quotaManagers.shutdown(), this)

        // Even though socket server is stopped much earlier, controller can generate
        // response for controlled shutdown request. Shutdown server at the end to
        // avoid any failures (e.g. when metrics are recorded)
        if (socketServer != null)
          CoreUtils.swallow(socketServer.shutdown(), this)
        unregisterCurrentControllerIdMetric()
        Utils.closeQuietly(metrics, "metrics")
        Utils.closeQuietly(brokerTopicStats, "broker topic stats")

        // Clear all reconfigurable instances stored in DynamicBrokerConfig
        config.dynamicConfig.clear()

        if (raftManager != null)
          CoreUtils.swallow(raftManager.shutdown(), this)

        if (lifecycleManager != null) {
          lifecycleManager.close()
        }
        _brokerState = BrokerState.NOT_RUNNING

        quorumControllerNodeProvider = null

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

  override def isShutdown(): Boolean = {
    BrokerState.fromValue(brokerState.value()) match {
      case BrokerState.SHUTTING_DOWN | BrokerState.NOT_RUNNING => true
      case _ => false
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
    config.effectiveAdvertisedBrokerListeners.map { endPoint =>
      endPoint.copy(port = boundPort(endPoint.listenerName))
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
  private def getOrGenerateBrokerId(metaPropsEnsemble: MetaPropertiesEnsemble): Int = {
    if (config.brokerId >= 0) {
      config.brokerId
    } else if (metaPropsEnsemble.nodeId().isPresent) {
      metaPropsEnsemble.nodeId().getAsInt
    } else if (config.brokerIdGenerationEnable) {
      generateBrokerId()
    } else
      throw new RuntimeException(s"No broker ID found, and ${config.brokerIdGenerationEnable} is disabled.")
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
