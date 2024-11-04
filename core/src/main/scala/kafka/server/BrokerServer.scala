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

import kafka.coordinator.group.{CoordinatorLoaderImpl, CoordinatorPartitionWriter, GroupCoordinatorAdapter}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.LogManager
import kafka.log.remote.RemoteLogManager
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.raft.KafkaRaftManager
import kafka.server.metadata._
import kafka.server.share.SharePartitionManager
import kafka.utils.CoreUtils
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.common.{ClusterResource, TopicPartition, Uuid}
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord
import org.apache.kafka.coordinator.group.metrics.{GroupCoordinatorMetrics, GroupCoordinatorRuntimeMetrics}
import org.apache.kafka.coordinator.group.{GroupConfigManager, GroupCoordinator, GroupCoordinatorRecordSerde, GroupCoordinatorService}
import org.apache.kafka.coordinator.share.metrics.{ShareCoordinatorMetrics, ShareCoordinatorRuntimeMetrics}
import org.apache.kafka.coordinator.share.{ShareCoordinator, ShareCoordinatorRecordSerde, ShareCoordinatorService}
import org.apache.kafka.coordinator.transaction.ProducerIdManager
import org.apache.kafka.image.publisher.{BrokerRegistrationTracker, MetadataPublisher}
import org.apache.kafka.metadata.{BrokerState, ListenerInfo}
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{ApiMessageAndVersion, DirectoryEventHandler, NodeToControllerChannelManager, TopicIdPartition}
import org.apache.kafka.server.config.ConfigType
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.{ClientMetricsReceiverPlugin, KafkaYammerMetrics}
import org.apache.kafka.server.network.{EndpointReadyFutures, KafkaAuthorizerServerInfo}
import org.apache.kafka.server.share.persister.{DefaultStatePersister, NoOpShareStatePersister, Persister, PersisterStateManager}
import org.apache.kafka.server.share.session.ShareSessionCache
import org.apache.kafka.server.util.timer.{SystemTimer, SystemTimerReaper}
import org.apache.kafka.server.util.{Deadline, FutureUtils, KafkaScheduler}
import org.apache.kafka.server.{AssignmentsManager, BrokerFeatures, ClientMetricsManager, DelayedActionQueue}
import org.apache.kafka.storage.internals.log.LogDirFailureChannel
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.time.Duration
import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{Condition, ReentrantLock}
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit, TimeoutException}
import scala.collection.Map
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption


/**
 * A Kafka broker that runs in KRaft (Kafka Raft) mode.
 */
class BrokerServer(
  val sharedServer: SharedServer
) extends KafkaBroker {
  val config: KafkaConfig = sharedServer.brokerConfig
  val time: Time = sharedServer.time
  def metrics: Metrics = sharedServer.metrics

  // Get raftManager from SharedServer. It will be initialized during startup.
  def raftManager: KafkaRaftManager[ApiMessageAndVersion] = sharedServer.raftManager

  override def brokerState: BrokerState = Option(lifecycleManager).
    flatMap(m => Some(m.state)).getOrElse(BrokerState.NOT_RUNNING)

  import kafka.server.Server._

  private val logContext: LogContext = new LogContext(s"[BrokerServer id=${config.nodeId}] ")

  this.logIdent = logContext.logPrefix

  @volatile var lifecycleManager: BrokerLifecycleManager = _

  private var assignmentsManager: AssignmentsManager = _

  private val isShuttingDown = new AtomicBoolean(false)

  val lock: ReentrantLock = new ReentrantLock()
  val awaitShutdownCond: Condition = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  @volatile var dataPlaneRequestProcessor: KafkaApis = _

  var authorizer: Option[Authorizer] = None
  @volatile var socketServer: SocketServer = _
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = _

  var logDirFailureChannel: LogDirFailureChannel = _
  var logManager: LogManager = _
  var remoteLogManagerOpt: Option[RemoteLogManager] = None

  var tokenManager: DelegationTokenManager = _

  var dynamicConfigHandlers: Map[String, ConfigHandler] = _

  @volatile private[this] var _replicaManager: ReplicaManager = _

  var credentialProvider: CredentialProvider = _
  var tokenCache: DelegationTokenCache = _

  @volatile var groupCoordinator: GroupCoordinator = _

  var groupConfigManager: GroupConfigManager = _

  var transactionCoordinator: TransactionCoordinator = _

  var shareCoordinator: Option[ShareCoordinator] = None

  var clientToControllerChannelManager: NodeToControllerChannelManager = _

  var forwardingManager: ForwardingManager = _

  var alterPartitionManager: AlterPartitionManager = _

  var autoTopicCreationManager: AutoTopicCreationManager = _

  var kafkaScheduler: KafkaScheduler = _

  @volatile var metadataCache: KRaftMetadataCache = _

  var quotaManagers: QuotaFactory.QuotaManagers = _

  var clientQuotaMetadataManager: ClientQuotaMetadataManager = _

  @volatile var brokerTopicStats: BrokerTopicStats = _

  val clusterId: String = sharedServer.metaPropsEnsemble.clusterId().get()

  var brokerMetadataPublisher: BrokerMetadataPublisher = _

  var brokerRegistrationTracker: BrokerRegistrationTracker = _

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault(config.unstableFeatureVersionsEnabled)

  def kafkaYammerMetrics: KafkaYammerMetrics = KafkaYammerMetrics.INSTANCE

  val metadataPublishers: util.List[MetadataPublisher] = new util.ArrayList[MetadataPublisher]()

  var clientMetricsManager: ClientMetricsManager = _

  var sharePartitionManager: SharePartitionManager = _

  var persister: Persister = _

  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      info(s"Transition from $status to $to")

      status = to
      if (to == SHUTTING_DOWN) {
        isShuttingDown.set(true)
      } else if (to == SHUTDOWN) {
        isShuttingDown.set(false)
        awaitShutdownCond.signalAll()
      }
    } finally {
      lock.unlock()
    }
    true
  }

  def replicaManager: ReplicaManager = _replicaManager

  override def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    val startupDeadline = Deadline.fromDelay(time, config.serverMaxStartupTimeMs, TimeUnit.MILLISECONDS)
    try {
      sharedServer.startForBroker()

      info("Starting broker")

      val clientMetricsReceiverPlugin = new ClientMetricsReceiverPlugin()
      config.dynamicConfig.initialize(zkClientOpt = None, Some(clientMetricsReceiverPlugin))

      /* start scheduler */
      kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
      kafkaScheduler.startup()

      /* register broker metrics */
      brokerTopicStats = new BrokerTopicStats(config.remoteLogManagerConfig.isRemoteStorageSystemEnabled())

      quotaManagers = QuotaFactory.instantiate(config, metrics, time, s"broker-${config.nodeId}-")

      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

      metadataCache = MetadataCache.kRaftMetadataCache(config.nodeId, () => raftManager.client.kraftVersion())

      // Create log manager, but don't start it because we need to delay any potential unclean shutdown log recovery
      // until we catch up on the metadata log and have up-to-date topic and broker configs.
      logManager = LogManager(config,
        sharedServer.metaPropsEnsemble.errorLogDirs().asScala.toSeq,
        metadataCache,
        kafkaScheduler,
        time,
        brokerTopicStats,
        logDirFailureChannel,
        keepPartitionMetadataFile = true)

      remoteLogManagerOpt = createRemoteLogManager()

      lifecycleManager = new BrokerLifecycleManager(config,
        time,
        s"broker-${config.nodeId}-",
        isZkBroker = false,
        logDirs = logManager.directoryIdsSet,
        () => new Thread(() => shutdown(), "kafka-shutdown-thread").start())

      // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
      // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "controller quorum voters future",
        sharedServer.controllerQuorumVotersFuture,
        startupDeadline, time)
      val controllerNodeProvider = RaftControllerNodeProvider(raftManager, config)

      clientToControllerChannelManager = new NodeToControllerChannelManagerImpl(
        controllerNodeProvider,
        time,
        metrics,
        config,
        channelName = "forwarding",
        s"broker-${config.nodeId}-",
        retryTimeoutMs = 60000
      )
      clientToControllerChannelManager.start()
      forwardingManager = new ForwardingManagerImpl(clientToControllerChannelManager, metrics)
      clientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, config.clientTelemetryMaxBytes, time, metrics)

      val apiVersionManager = ApiVersionManager(
        ListenerType.BROKER,
        config,
        Some(forwardingManager),
        brokerFeatures,
        metadataCache,
        Some(clientMetricsManager)
      )

      val connectionDisconnectListeners = Seq(clientMetricsManager.connectionDisconnectListener())
      // Create and start the socket server acceptor threads so that the bound port is known.
      // Delay starting processors until the end of the initialization sequence to ensure
      // that credentials have been loaded before processing authentications.
      socketServer = new SocketServer(config,
        metrics,
        time,
        credentialProvider,
        apiVersionManager,
        sharedServer.socketFactory,
        connectionDisconnectListeners)

      clientQuotaMetadataManager = new ClientQuotaMetadataManager(quotaManagers, socketServer.connectionQuotas)

      val listenerInfo = ListenerInfo.create(Optional.of(config.interBrokerListenerName.value()),
          config.effectiveAdvertisedBrokerListeners.map(_.toJava).asJava).
            withWildcardHostnamesResolved().
            withEphemeralPortsCorrected(name => socketServer.boundPort(new ListenerName(name)))

      alterPartitionManager = AlterPartitionManager(
        config,
        metadataCache,
        scheduler = kafkaScheduler,
        controllerNodeProvider,
        time = time,
        metrics,
        s"broker-${config.nodeId}-",
        brokerEpochSupplier = () => lifecycleManager.brokerEpoch
      )
      alterPartitionManager.start()

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

      val assignmentsChannelManager = new NodeToControllerChannelManagerImpl(
        controllerNodeProvider,
        time,
        metrics,
        config,
        "directory-assignments",
        s"broker-${config.nodeId}-",
        retryTimeoutMs = 60000
      )
      assignmentsManager = new AssignmentsManager(
        time,
        assignmentsChannelManager,
        config.brokerId,
        () => metadataCache.getImage(),
        (directoryId: Uuid) => logManager.directoryPath(directoryId).
          getOrElse("[unknown directory path]")
      )
      val directoryEventHandler = new DirectoryEventHandler {
        override def handleAssignment(partition: TopicIdPartition, directoryId: Uuid, reason: String, callback: Runnable): Unit =
          assignmentsManager.onAssignment(partition, directoryId, reason, callback)

        override def handleFailure(directoryId: Uuid): Unit =
          lifecycleManager.propagateDirectoryFailure(directoryId, config.logDirFailureTimeoutMs)
      }

      /**
       * TODO: move this action queue to handle thread so we can simplify concurrency handling
       */
      val defaultActionQueue = new DelayedActionQueue

      this._replicaManager = new ReplicaManager(
        config = config,
        metrics = metrics,
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
        zkClient = None,
        threadNamePrefix = None, // The ReplicaManager only runs on the broker, and already includes the ID in thread names.
        delayedRemoteFetchPurgatoryParam = None,
        brokerEpochSupplier = () => lifecycleManager.brokerEpoch,
        addPartitionsToTxnManager = Some(addPartitionsToTxnManager),
        directoryEventHandler = directoryEventHandler,
        defaultActionQueue = defaultActionQueue
      )

      /* start token manager */
      tokenManager = new DelegationTokenManager(config, tokenCache, time)
      tokenManager.startup()

      /* initializing the groupConfigManager */
      groupConfigManager = new GroupConfigManager(config.groupCoordinatorConfig.extractGroupConfigMap(config.shareGroupConfig))

      /* create share coordinator */
      shareCoordinator = createShareCoordinator()

      /* create persister */
      persister = createShareStatePersister()

      groupCoordinator = createGroupCoordinator()

      val producerIdManagerSupplier = () => ProducerIdManager.rpc(
        config.brokerId,
        time,
        () => lifecycleManager.brokerEpoch,
        clientToControllerChannelManager
      )

      // Create transaction coordinator, but don't start it until we've started replica manager.
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      transactionCoordinator = TransactionCoordinator(config, replicaManager,
        new KafkaScheduler(1, true, "transaction-log-manager-"),
        producerIdManagerSupplier, metrics, metadataCache, Time.SYSTEM)

      autoTopicCreationManager = new DefaultAutoTopicCreationManager(
        config, Some(clientToControllerChannelManager), None, None,
        groupCoordinator, transactionCoordinator, shareCoordinator)

      dynamicConfigHandlers = Map[String, ConfigHandler](
        ConfigType.TOPIC -> new TopicConfigHandler(replicaManager, config, quotaManagers, None),
        ConfigType.BROKER -> new BrokerConfigHandler(config, quotaManagers),
        ConfigType.CLIENT_METRICS -> new ClientMetricsConfigHandler(clientMetricsManager),
        ConfigType.GROUP -> new GroupConfigHandler(groupCoordinator))

      val featuresRemapped = BrokerFeatures.createDefaultFeatureMap(brokerFeatures)

      val brokerLifecycleChannelManager = new NodeToControllerChannelManagerImpl(
        controllerNodeProvider,
        time,
        metrics,
        config,
        "heartbeat",
        s"broker-${config.nodeId}-",
        config.brokerSessionTimeoutMs / 2 // KAFKA-14392
      )
      lifecycleManager.start(
        () => sharedServer.loader.lastAppliedOffset(),
        brokerLifecycleChannelManager,
        clusterId,
        listenerInfo.toBrokerRegistrationRequest,
        featuresRemapped,
        logManager.readBrokerEpochFromCleanShutdownFiles()
      )

      // Create and initialize an authorizer if one is configured.
      authorizer = config.createNewAuthorizer()
      authorizer.foreach(_.configure(config.originals))

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

      val shareFetchSessionCache : ShareSessionCache = new ShareSessionCache(
        config.shareGroupConfig.shareGroupMaxGroups * config.groupCoordinatorConfig.shareGroupMaxSize,
        KafkaServer.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS)

      sharePartitionManager = new SharePartitionManager(
        replicaManager,
        time,
        shareFetchSessionCache,
        config.shareGroupConfig.shareGroupRecordLockDurationMs,
        config.shareGroupConfig.shareGroupDeliveryCountLimit,
        config.shareGroupConfig.shareGroupPartitionMaxRecordLocks,
        config.shareGroupConfig.shareFetchMaxFetchRecords,
        persister,
        groupConfigManager,
        metrics
      )

      // Create the request processor objects.
      val raftSupport = RaftSupport(forwardingManager, metadataCache)
      dataPlaneRequestProcessor = new KafkaApis(
        requestChannel = socketServer.dataPlaneRequestChannel,
        metadataSupport = raftSupport,
        replicaManager = replicaManager,
        groupCoordinator = groupCoordinator,
        txnCoordinator = transactionCoordinator,
        shareCoordinator = shareCoordinator,
        autoTopicCreationManager = autoTopicCreationManager,
        brokerId = config.nodeId,
        config = config,
        configRepository = metadataCache,
        metadataCache = metadataCache,
        metrics = metrics,
        authorizer = authorizer,
        quotas = quotaManagers,
        fetchManager = fetchManager,
        sharePartitionManager = Some(sharePartitionManager),
        brokerTopicStats = brokerTopicStats,
        clusterId = clusterId,
        time = time,
        tokenManager = tokenManager,
        apiVersionManager = apiVersionManager,
        clientMetricsManager = Some(clientMetricsManager))

      dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.nodeId,
        socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
        config.numIoThreads, s"${DataPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent",
        DataPlaneAcceptor.ThreadPrefix)

      // Start RemoteLogManager before initializing broker metadata publishers.
      remoteLogManagerOpt.foreach { rlm =>
        val listenerName = config.remoteLogManagerConfig.remoteLogMetadataManagerListenerName()
        if (listenerName != null) {
          val endpoint = listenerInfo.listeners().values().stream
            .filter(e =>
              e.listenerName().isPresent &&
                ListenerName.normalised(e.listenerName().get()).equals(ListenerName.normalised(listenerName))
            )
            .findFirst()
            .orElseThrow(() => new ConfigException(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP,
              listenerName, "Should be set as a listener name within valid broker listener name list: " + listenerInfo.listeners().values()))
          rlm.onEndPointCreated(endpoint)
        }
        rlm.startup()
      }

      metadataPublishers.add(new MetadataVersionConfigValidator(config, sharedServer.metadataPublishingFaultHandler))
      brokerMetadataPublisher = new BrokerMetadataPublisher(config,
        metadataCache,
        logManager,
        replicaManager,
        groupCoordinator,
        transactionCoordinator,
        shareCoordinator,
        new DynamicConfigPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          dynamicConfigHandlers.toMap,
        "broker"),
        new DynamicClientQuotaPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          clientQuotaMetadataManager),
        new ScramPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          credentialProvider),
        new DelegationTokenPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          tokenManager),
        new AclPublisher(
          config.nodeId,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          authorizer
        ),
        sharedServer.initialBrokerMetadataLoadFaultHandler,
        sharedServer.metadataPublishingFaultHandler
      )
      // If the BrokerLifecycleManager's initial catch-up future fails, it means we timed out
      // or are shutting down before we could catch up. Therefore, also fail the firstPublishFuture.
      lifecycleManager.initialCatchUpFuture.whenComplete((_, e) => {
        if (e != null) brokerMetadataPublisher.firstPublishFuture.completeExceptionally(e)
      })
      metadataPublishers.add(brokerMetadataPublisher)
      brokerRegistrationTracker = new BrokerRegistrationTracker(config.brokerId,
        () => lifecycleManager.resendBrokerRegistrationUnlessZkMode())
      metadataPublishers.add(brokerRegistrationTracker)


      // Register parts of the broker that can be reconfigured via dynamic configs.  This needs to
      // be done before we publish the dynamic configs, so that we don't miss anything.
      config.dynamicConfig.addReconfigurables(this)

      // Install all the metadata publishers.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the broker metadata publishers to be installed",
        sharedServer.loader.installPublishers(metadataPublishers), startupDeadline, time)

      // Wait for this broker to contact the quorum, and for the active controller to acknowledge
      // us as caught up. It will do this by returning a heartbeat response with isCaughtUp set to
      // true. The BrokerLifecycleManager tracks this.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the controller to acknowledge that we are caught up",
        lifecycleManager.initialCatchUpFuture, startupDeadline, time)

      // Wait for the first metadata update to be published. Metadata updates are not published
      // until we read at least up to the high water mark of the cluster metadata partition.
      // Usually, we publish the initial metadata before lifecycleManager.initialCatchUpFuture
      // is completed, so this check is not necessary. But this is a simple check to make
      // completely sure.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the initial broker metadata update to be published",
        brokerMetadataPublisher.firstPublishFuture , startupDeadline, time)

      // Now that we have loaded some metadata, we can log a reasonably up-to-date broker
      // configuration.  Keep in mind that KafkaConfig.originals is a mutable field that gets set
      // by the dynamic configuration publisher. Ironically, KafkaConfig.originals does not
      // contain the original configuration values.
      new KafkaConfig(config.originals(), true)

      // We're now ready to unfence the broker. This also allows this broker to transition
      // from RECOVERY state to RUNNING state, once the controller unfences the broker.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the broker to be unfenced",
        lifecycleManager.setReadyToUnfence(), startupDeadline, time)

      // Enable inbound TCP connections. Each endpoint will be started only once its matching
      // authorizer future is completed.
      val endpointReadyFutures = {
        val builder = new EndpointReadyFutures.Builder()
        builder.build(authorizer.toJava,
          new KafkaAuthorizerServerInfo(
            new ClusterResource(clusterId),
            config.nodeId,
            listenerInfo.listeners().values(),
            listenerInfo.firstListener(),
            config.earlyStartListeners.map(_.value()).asJava))
      }
      val authorizerFutures = endpointReadyFutures.futures().asScala.toMap
      val enableRequestProcessingFuture = socketServer.enableRequestProcessing(authorizerFutures)

      // Block here until all the authorizer futures are complete.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the authorizer futures to be completed",
        CompletableFuture.allOf(authorizerFutures.values.toSeq: _*), startupDeadline, time)

      // Wait for all the SocketServer ports to be open, and the Acceptors to be started.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the SocketServer Acceptors to be started",
        enableRequestProcessingFuture, startupDeadline, time)

      maybeChangeStatus(STARTING, STARTED)
    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during broker startup. Prepare to shutdown", e)
        shutdown()
        throw if (e.isInstanceOf[ExecutionException]) e.getCause else e
    }
  }

  private def createGroupCoordinator(): GroupCoordinator = {
    // Create group coordinator, but don't start it until we've started replica manager.
    // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good
    // to fix the underlying issue.
    if (config.isNewGroupCoordinatorEnabled) {
      val time = Time.SYSTEM
      val serde = new GroupCoordinatorRecordSerde
      val timer = new SystemTimerReaper(
        "group-coordinator-reaper",
        new SystemTimer("group-coordinator")
      )
      val loader = new CoordinatorLoaderImpl[CoordinatorRecord](
        time,
        replicaManager,
        serde,
        config.groupCoordinatorConfig.offsetsLoadBufferSize
      )
      val writer = new CoordinatorPartitionWriter(
        replicaManager
      )
      new GroupCoordinatorService.Builder(config.brokerId, config.groupCoordinatorConfig)
        .withTime(time)
        .withTimer(timer)
        .withLoader(loader)
        .withWriter(writer)
        .withCoordinatorRuntimeMetrics(new GroupCoordinatorRuntimeMetrics(metrics))
        .withGroupCoordinatorMetrics(new GroupCoordinatorMetrics(KafkaYammerMetrics.defaultRegistry, metrics))
        .withGroupConfigManager(groupConfigManager)
        .build()
    } else {
      GroupCoordinatorAdapter(
        config,
        replicaManager,
        Time.SYSTEM,
        metrics
      )
    }
  }

  private def createShareCoordinator(): Option[ShareCoordinator] = {
    if (config.shareGroupConfig.isShareGroupEnabled &&
      config.shareGroupConfig.shareGroupPersisterClassName().nonEmpty) {
      val time = Time.SYSTEM
      val timer = new SystemTimerReaper(
        "share-coordinator-reaper",
        new SystemTimer("share-coordinator")
      )

      val serde = new ShareCoordinatorRecordSerde
      val loader = new CoordinatorLoaderImpl[CoordinatorRecord](
        time,
        replicaManager,
        serde,
        config.shareCoordinatorConfig.shareCoordinatorLoadBufferSize()
      )
      val writer = new CoordinatorPartitionWriter(
        replicaManager
      )
      Some(new ShareCoordinatorService.Builder(config.brokerId, config.shareCoordinatorConfig)
        .withTimer(timer)
        .withTime(time)
        .withLoader(loader)
        .withWriter(writer)
        .withCoordinatorRuntimeMetrics(new ShareCoordinatorRuntimeMetrics(metrics))
        .withCoordinatorMetrics(new ShareCoordinatorMetrics(metrics))
        .build())
    } else {
      None
    }
  }

  private def createShareStatePersister(): Persister = {
    if (config.shareGroupConfig.isShareGroupEnabled &&
      config.shareGroupConfig.shareGroupPersisterClassName.nonEmpty) {
      val klass = Utils.loadClass(config.shareGroupConfig.shareGroupPersisterClassName, classOf[Object]).asInstanceOf[Class[Persister]]

      if (klass.getName.equals(classOf[DefaultStatePersister].getName)) {
        klass.getConstructor(classOf[PersisterStateManager])
          .newInstance(
            new PersisterStateManager(
              NetworkUtils.buildNetworkClient("Persister", config, metrics, Time.SYSTEM, new LogContext(s"[Persister broker=${config.brokerId}]")),
              new ShareCoordinatorMetadataCacheHelperImpl(metadataCache, key => shareCoordinator.get.partitionFor(key), config.interBrokerListenerName),
              Time.SYSTEM,
              new SystemTimerReaper(
                "persister-state-manager-reaper",
                new SystemTimer("persister")
              )
            )
          )
      } else if (klass.getName.equals(classOf[NoOpShareStatePersister].getName)) {
        info("Using no op persister")
        new NoOpShareStatePersister()
      } else {
        error("Unknown persister specified. Persister is only factory pluggable!")
        throw new IllegalArgumentException("Unknown persiser specified " + config.shareGroupConfig.shareGroupPersisterClassName)
      }
    } else {
      // in case share coordinator not enabled or
      // persister class name deliberately empty (key=)
      info("Using no op persister")
      new NoOpShareStatePersister()
    }
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

  override def shutdown(timeout: Duration): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      val deadline = time.milliseconds() + timeout.toMillis
      info("shutting down")

      if (config.controlledShutdownEnable) {
        if (replicaManager != null)
          replicaManager.beginControlledShutdown()

        lifecycleManager.beginControlledShutdown()
        try {
          val controlledShutdownTimeoutMs = deadline - time.milliseconds()
          lifecycleManager.controlledShutdownFuture.get(controlledShutdownTimeoutMs, TimeUnit.MILLISECONDS)
        } catch {
          case _: TimeoutException =>
            error("Timed out waiting for the controller to approve controlled shutdown")
          case e: Throwable =>
            error("Got unexpected exception waiting for controlled shutdown future", e)
        }
      }
      lifecycleManager.beginShutdown()
      // Stop socket server to stop accepting any more connections and requests.
      // Socket server will be shutdown towards the end of the sequence.
      if (socketServer != null) {
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      }
      metadataPublishers.forEach(p => sharedServer.loader.removeAndClosePublisher(p).get())
      metadataPublishers.clear()
      if (dataPlaneRequestHandlerPool != null)
        CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
      if (dataPlaneRequestProcessor != null)
        CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
      authorizer.foreach(Utils.closeQuietly(_, "authorizer"))

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

      if (transactionCoordinator != null)
        CoreUtils.swallow(transactionCoordinator.shutdown(), this)

      if (groupConfigManager != null)
        CoreUtils.swallow(groupConfigManager.close(), this)
      if (groupCoordinator != null)
        CoreUtils.swallow(groupCoordinator.shutdown(), this)
      if (shareCoordinator.isDefined)
        CoreUtils.swallow(shareCoordinator.get.shutdown(), this)

      if (tokenManager != null)
        CoreUtils.swallow(tokenManager.shutdown(), this)

      if (assignmentsManager != null)
        CoreUtils.swallow(assignmentsManager.close(), this)

      if (replicaManager != null)
        CoreUtils.swallow(replicaManager.shutdown(), this)

      if (alterPartitionManager != null)
        CoreUtils.swallow(alterPartitionManager.shutdown(), this)

      if (forwardingManager != null)
        CoreUtils.swallow(forwardingManager.close(), this)

      if (clientToControllerChannelManager != null)
        CoreUtils.swallow(clientToControllerChannelManager.shutdown(), this)

      if (logManager != null)
        CoreUtils.swallow(logManager.shutdown(lifecycleManager.brokerEpoch), this)

      // Close remote log manager to give a chance to any of its underlying clients
      // (especially in RemoteStorageManager and RemoteLogMetadataManager) to close gracefully.
      remoteLogManagerOpt.foreach(Utils.closeQuietly(_, "remote log manager"))

      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)

      if (socketServer != null)
        CoreUtils.swallow(socketServer.shutdown(), this)

      Utils.closeQuietly(brokerTopicStats, "broker topic stats")
      Utils.closeQuietly(sharePartitionManager, "share partition manager")

      if (persister != null)
        CoreUtils.swallow(persister.stop(), this)

      isShuttingDown.set(false)

      CoreUtils.swallow(lifecycleManager.close(), this)
      CoreUtils.swallow(config.dynamicConfig.clear(), this)
      Utils.closeQuietly(clientMetricsManager, "client metrics manager")
      sharedServer.stopForBroker()
      info("shut down completed")
    } catch {
      case e: Throwable =>
        fatal("Fatal error during broker shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  override def isShutdown(): Boolean = {
    status == SHUTDOWN || status == SHUTTING_DOWN
  }

  override def awaitShutdown(): Unit = {
    lock.lock()
    try {
      while (true) {
        if (status == SHUTDOWN) return
        awaitShutdownCond.awaitUninterruptibly()
      }
    } finally {
      lock.unlock()
    }
  }

  override def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

}
