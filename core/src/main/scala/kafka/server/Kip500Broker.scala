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

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.{Broker, EndPoint}
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.{ProducerIdGenerator, TransactionCoordinator}
import kafka.log.LogManager
import kafka.metrics.KafkaYammerMetrics
import kafka.network.SocketServer
import kafka.security.CredentialProvider
import kafka.server.KafkaBroker.metricsPrefix
import kafka.server.metadata.BrokerMetadataListener
import kafka.utils.{CoreUtils, KafkaScheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{FeatureCollection, Listener, ListenerCollection}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time}
import org.apache.kafka.common.{Endpoint, KafkaException}
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.metalog.MetaLogManager
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

/**
 * A KIP-500 Kafka broker.
 */
class Kip500Broker(
  val config: KafkaConfig,
  val metaProps: MetaProperties,
  val metaLogManager: MetaLogManager,
  val time: Time,
  val metrics: Metrics,
  val threadNamePrefix: Option[String],
  offlineDirs: Seq[String]
) extends KafkaBroker {

  import kafka.server.KafkaServer._

  val lifecycleManager: BrokerLifecycleManager =
      new BrokerLifecycleManager(config, time, threadNamePrefix)

  private val isShuttingDown = new AtomicBoolean(false)

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  private var logContext: LogContext = null

  var dataPlaneRequestProcessor: KafkaApis = null
  var controlPlaneRequestProcessor: KafkaApis = null

  var authorizer: Option[Authorizer] = None
  var socketServer: SocketServer = null
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = null
  var controlPlaneRequestHandlerPool: KafkaRequestHandlerPool = null

  var logDirFailureChannel: LogDirFailureChannel = null
  var logManager: LogManager = null

  var tokenManager: DelegationTokenManager = null

  var replicaManager: ReplicaManager = null

  var credentialProvider: CredentialProvider = null
  var tokenCache: DelegationTokenCache = null

  var groupCoordinator: GroupCoordinator = null

  var transactionCoordinator: TransactionCoordinator = null

  var forwardingChannelManager: BrokerToControllerChannelManager = null

  var alterIsrChannelManager: BrokerToControllerChannelManager = null

  var kafkaScheduler: KafkaScheduler = null

  var metadataCache: MetadataCache = null
  var quotaManagers: QuotaFactory.QuotaManagers = null

  private var _brokerTopicStats: BrokerTopicStats = null

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault()

  val featureCache: FinalizedFeatureCache = new FinalizedFeatureCache(brokerFeatures)

  val clusterId: String = metaProps.clusterId.toString

  var brokerMetadataListener: BrokerMetadataListener = null
  val _brokerMetadataListenerFuture: CompletableFuture[BrokerMetadataListener] = new CompletableFuture[BrokerMetadataListener]()

  def kafkaYammerMetrics: kafka.metrics.KafkaYammerMetrics = KafkaYammerMetrics.INSTANCE

  private[kafka] def brokerTopicStats = _brokerTopicStats

  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
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

  override def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    try {
      info("Starting broker")

      logContext = new LogContext(s"[KIP-500 KafkaServer id=${config.brokerId}] ")
      this.logIdent = logContext.logPrefix

      // initialize dynamic broker configs from static config. Any updates will be
      // applied as we process the metadata log.
      config.dynamicConfig.initialize() // Currently we don't wait for catch-up on the metadata log.  TODO?

      /* start scheduler */
      kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
      kafkaScheduler.startup()

      val controllerNodeProvider = new RaftControllerNodeProvider(metaLogManager, config.controllerConnectNodes)

      /* register broker metrics */
      _brokerTopicStats = new BrokerTopicStats

      quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))

      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

      // Create log manager, but don't start it because we need to delay any potential unclean shutdown log recovery
      // until we catch up on the metadata log and have up-to-date topic and broker configs.
      logManager = LogManager(config, offlineDirs, kafkaScheduler, time, brokerTopicStats, logDirFailureChannel)

      metadataCache = new MetadataCache(config.brokerId)
      // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
      // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

      // Create and start the socket server acceptor threads so that the bound port is known.
      // Delay starting processors until the end of the initialization sequence to ensure
      // that credentials have been loaded before processing authentications.
      socketServer = new SocketServer(config, metrics, time, credentialProvider)
      socketServer.startup(startProcessingRequests = false)

      // Create replica manager, but don't start it until we've started log manager.
      replicaManager = createReplicaManager(isShuttingDown)

      /* start broker-to-controller channel managers */
      alterIsrChannelManager = new BrokerToControllerChannelManager(controllerNodeProvider,
        time, metrics, config, "alterisr", threadNamePrefix)
      alterIsrChannelManager.start()
      forwardingChannelManager = new BrokerToControllerChannelManager(controllerNodeProvider,
        time, metrics, config, "forwarding", threadNamePrefix)
      forwardingChannelManager.start()
      val forwardingManager = new ForwardingManager(forwardingChannelManager, time, config.requestTimeoutMs.longValue)

      /* start token manager */
      if (config.tokenAuthEnabled) {
        throw new UnsupportedOperationException("Delegation tokens are not supported")
      }
      tokenManager = new DelegationTokenManager(config, tokenCache, time , null)
      tokenManager.startup() // does nothing, we just need a token manager in order to compile right now...

      // Create group coordinator, but don't start it until we've started replica manager.
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      groupCoordinator = GroupCoordinator(config, () => getGroupMetadataTopicPartitionCount(), replicaManager, Time.SYSTEM, metrics)

      // Create transaction coordinator, but don't start it until we've started replica manager.
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      transactionCoordinator = TransactionCoordinator(config, replicaManager, new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-"),
        createTemporaryProducerIdManager(), getTransactionTopicPartitionCount,
        metrics, metadataCache, Time.SYSTEM)

      /* Add all reconfigurables for config change notification before starting the metadata listener */
      config.dynamicConfig.addReconfigurables(this)

      brokerMetadataListener = new BrokerMetadataListener(
        config, metadataCache, time,
        BrokerMetadataListener.defaultProcessors(
          config, clusterId, metadataCache, groupCoordinator, quotaManagers, replicaManager, transactionCoordinator,
          logManager))
      brokerMetadataListener.start()

      lifecycleManager.start(() => brokerMetadataListener.currentMetadataOffset(),
        new BrokerToControllerChannelManager(controllerNodeProvider,
          time, metrics, config, "heartbeat", threadNamePrefix), metaProps.clusterId)

      // Register a listener with the Raft layer to receive metadata event notifications
      metaLogManager.register(brokerMetadataListener)

      val listeners = new ListenerCollection()
      config.advertisedListeners.foreach { ep =>
        listeners.add(new Listener().setHost(ep.host).setName(ep.listenerName.value()).setPort(ep.port.shortValue())
          .setSecurityProtocol(ep.securityProtocol.id))
      }

      val features = new FeatureCollection()

      val endPoints = new ArrayBuffer[EndPoint](listeners.size())
      listeners.iterator().forEachRemaining(listener => {
        endPoints += new EndPoint(listener.host(), listener.port(), new ListenerName(listener.name()),
          SecurityProtocol.forId(listener.securityProtocol()))
      })
      val supportedFeaturesMap = mutable.Map[String, SupportedVersionRange]()
      features.iterator().forEachRemaining(feature => {
        supportedFeaturesMap(feature.name()) = new SupportedVersionRange(feature.minSupportedVersion(), feature.maxSupportedVersion())
      })

      val broker = Broker(config.brokerId, endPoints, config.rack, Features.supportedFeatures(supportedFeaturesMap.asJava))

      /* Get the authorizer and initialize it if one is specified.*/
      authorizer = config.authorizer
      authorizer.foreach(_.configure(config.originals))
      val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
        case Some(authZ) =>
          authZ.start(broker.toServerInfo(clusterId, config)).asScala.map { case (ep, cs) =>
            ep -> cs.toCompletableFuture
          }
        case None =>
          broker.endPoints.map { ep =>
            ep.toJava -> CompletableFuture.completedFuture[Void](null)
          }.toMap
      }

      val fetchManager = new FetchManager(Time.SYSTEM,
        new FetchSessionCache(config.maxIncrementalFetchSessionCacheSlots,
          KafkaBroker.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS))

      // Start processing requests once we've caught up on the metadata log, recovered logs if necessary,
      // and started all services that we previously delayed starting.
      val adminManager: LegacyAdminManager = null
      val zkClient: KafkaZkClient = null
      val kafkaController: KafkaController = null
      dataPlaneRequestProcessor = new KafkaApis(socketServer.dataPlaneRequestChannel,
        replicaManager, adminManager, groupCoordinator, transactionCoordinator,
        kafkaController, forwardingManager, zkClient, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
        fetchManager, brokerTopicStats, clusterId, time, tokenManager, brokerFeatures, featureCache, brokerMetadataListener)

      dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
        config.numIoThreads, s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.DataPlaneThreadPrefix)

      socketServer.controlPlaneRequestChannelOpt.foreach { controlPlaneRequestChannel =>
        controlPlaneRequestProcessor = new KafkaApis(controlPlaneRequestChannel,
          replicaManager, adminManager, groupCoordinator, transactionCoordinator,
          kafkaController, forwardingManager, zkClient, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
          fetchManager, brokerTopicStats, clusterId, time, tokenManager, brokerFeatures, featureCache, brokerMetadataListener)

        controlPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.controlPlaneRequestChannelOpt.get, controlPlaneRequestProcessor, time,
          1, s"${SocketServer.ControlPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.ControlPlaneThreadPrefix)
      }

      // Block until we've caught up on the metadata log
      lifecycleManager.initialCatchUpFuture.get()
      _brokerMetadataListenerFuture.complete(brokerMetadataListener)
      // Start log manager, which will perform (potentially lengthy) recovery-from-unclean-shutdown if required.
      logManager.startup()
      // Start other services that we've delayed starting, in the appropriate order.
      replicaManager.startup()
      groupCoordinator.startup()
      transactionCoordinator.startup()

      socketServer.startProcessingRequests(authorizerFutures)

      // We're now ready to unfence the broker.
      lifecycleManager.setReadyToUnfence()

      info("started")
      maybeChangeStatus(STARTING, STARTED)
    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during broker startup. Prepare to shutdown", e)
        shutdown()
        throw e
    }
  }

  class TemporaryProducerIdManager() extends ProducerIdGenerator {
    val maxProducerIdsPerBrokerEpoch = 1000000
    var currentOffset = -1
    override def generateProducerId(): Long = {
      currentOffset = currentOffset + 1
      if (currentOffset >= maxProducerIdsPerBrokerEpoch) {
        fatal(s"Exhausted all demo/temporary producerIds as the next one will has extend past the block size of $maxProducerIdsPerBrokerEpoch")
        throw new KafkaException("Have exhausted all demo/temporary producerIds.")
      }
      lifecycleManager.initialCatchUpFuture.get()
      lifecycleManager.brokerEpoch() * maxProducerIdsPerBrokerEpoch + currentOffset
    }
  }

  def createTemporaryProducerIdManager(): ProducerIdGenerator = {
    new TemporaryProducerIdManager()
  }

  protected def createReplicaManager(isShuttingDown: AtomicBoolean): ReplicaManager = {
    val alterIsrManager = new AlterIsrManagerImpl(alterIsrChannelManager, kafkaScheduler,
      time, config.brokerId, () => lifecycleManager.brokerEpoch())
    // explicitly declare to eliminate spotbugs error in Scala 2.12
    val zkClient: Option[KafkaZkClient] = None
    new ReplicaManager(config, metrics, time, zkClient, kafkaScheduler, logManager, isShuttingDown, quotaManagers,
      brokerTopicStats, metadataCache, logDirFailureChannel, alterIsrManager, None, Some(_brokerMetadataListenerFuture))
  }

  /**
   * Gets the partition count of the group metadata topic from the metadata log.
   * If the topic does not exist, the configured partition count is returned.
   */
  def getGroupMetadataTopicPartitionCount(): Int = {
    // wait until we are caught up on the metadata log if necessary
    lifecycleManager.initialCatchUpFuture.get()
    // now return the number of partitions if the topic exists, otherwise the default
    metadataCache.numPartitions(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicPartitions)
  }

  /**
   * Gets the partition count of the transaction log topic from the metadata log.
   * If the topic does not exist, the default partition count is returned.
   */
  def getTransactionTopicPartitionCount(): Int = {
    // wait until we are caught up on the metadata log if necessary
    lifecycleManager.initialCatchUpFuture.get()
    // now return the number of partitions if the topic exists, otherwise the default
    metadataCache.numPartitions(Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionTopicPartitions)
  }

  /**
   * Performs controlled shutdown
   */
  private def controlledShutdown(): Unit = {

    if (config.controlledShutdownEnable) {
      // We request the heartbeat to initiate a controlled shutdown.
      info("Controlled shutdown requested")

      // TODO: request controlled shutdown from broker lifecycle manager
      // TODO: wait for controlled shutdown to complete
    }
    lifecycleManager.beginShutdown()
  }

  def shutdown(): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      info("shutting down")

      CoreUtils.swallow(controlledShutdown(), this)

      // Stop socket server to stop accepting any more connections and requests.
      // Socket server will be shutdown towards the end of the sequence.
      if (socketServer != null) {
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      }
      if (dataPlaneRequestHandlerPool != null)
        CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
      if (controlPlaneRequestHandlerPool != null)
        CoreUtils.swallow(controlPlaneRequestHandlerPool.shutdown(), this)
      if (kafkaScheduler != null)
        CoreUtils.swallow(kafkaScheduler.shutdown(), this)

      if (dataPlaneRequestProcessor != null)
        CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
      if (controlPlaneRequestProcessor != null)
        CoreUtils.swallow(controlPlaneRequestProcessor.close(), this)
      CoreUtils.swallow(authorizer.foreach(_.close()), this)

      if (brokerMetadataListener !=  null) {
        CoreUtils.swallow(brokerMetadataListener.close(), this)
      }
      _brokerMetadataListenerFuture.cancel(true)

      if (transactionCoordinator != null)
        CoreUtils.swallow(transactionCoordinator.shutdown(), this)
      if (groupCoordinator != null)
        CoreUtils.swallow(groupCoordinator.shutdown(), this)

      if (tokenManager != null)
        CoreUtils.swallow(tokenManager.shutdown(), this)

      if (replicaManager != null)
        CoreUtils.swallow(replicaManager.shutdown(), this)

      if (alterIsrChannelManager != null)
        CoreUtils.swallow(alterIsrChannelManager.shutdown(), this)

      if (forwardingChannelManager != null)
        CoreUtils.swallow(forwardingChannelManager.shutdown(), this)

      if (logManager != null)
        CoreUtils.swallow(logManager.shutdown(), this)

      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)

      if (socketServer != null)
        CoreUtils.swallow(socketServer.shutdown(), this)
      if (metrics != null)
        CoreUtils.swallow(metrics.close(), this)
      if (brokerTopicStats != null)
        CoreUtils.swallow(brokerTopicStats.close(), this)

      // Clear all reconfigurable instances stored in DynamicBrokerConfig
      config.dynamicConfig.clear()

      isShuttingDown.set(false)

      CoreUtils.swallow(lifecycleManager.close(), this)

      CoreUtils.swallow(AppInfoParser.unregisterAppInfo(metricsPrefix, config.brokerId.toString, metrics), this)
      info("shut down completed")
    } catch {
      case e: Throwable =>
        fatal("Fatal error during broker shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
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

  override def currentState(): BrokerState = lifecycleManager.state()

}
