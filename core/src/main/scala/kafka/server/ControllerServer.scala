/*
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

import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.raft.KafkaRaftManager
import kafka.server.QuotaFactory.QuotaManagers

import scala.collection.immutable
import kafka.server.metadata.{AclPublisher, ClientQuotaMetadataManager, DelegationTokenPublisher, DynamicClientQuotaPublisher, DynamicConfigPublisher, KRaftMetadataCache, KRaftMetadataCachePublisher, ScramPublisher}
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Utils}
import org.apache.kafka.common.{ClusterResource, Endpoint, Uuid}
import org.apache.kafka.controller.metrics.{ControllerMetadataMetricsPublisher, QuorumControllerMetrics}
import org.apache.kafka.controller.{Controller, QuorumController, QuorumFeatures}
import org.apache.kafka.image.publisher.{ControllerRegistrationsPublisher, MetadataPublisher}
import org.apache.kafka.metadata.{KafkaConfigSchema, ListenerInfo}
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata
import org.apache.kafka.metadata.publisher.FeaturesPublisher
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.config.ServerLogConfigs.{ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG, CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG}
import org.apache.kafka.server.common.{ApiMessageAndVersion, NodeToControllerChannelManager}
import org.apache.kafka.server.config.ConfigType
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics, LinuxIoMetricsCollector}
import org.apache.kafka.server.network.{EndpointReadyFutures, KafkaAuthorizerServerInfo}
import org.apache.kafka.server.policy.{AlterConfigPolicy, CreateTopicPolicy}
import org.apache.kafka.server.util.{Deadline, FutureUtils}

import java.util
import java.util.{Optional, OptionalLong}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption


/**
 * A Kafka controller that runs in KRaft (Kafka Raft) mode.
 */
class ControllerServer(
  val sharedServer: SharedServer,
  val configSchema: KafkaConfigSchema,
  val bootstrapMetadata: BootstrapMetadata
) extends Logging {

  import kafka.server.Server._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val config = sharedServer.controllerConfig
  val logContext = new LogContext(s"[ControllerServer id=${config.nodeId}] ")
  val time = sharedServer.time
  def metrics = sharedServer.metrics
  def raftManager: KafkaRaftManager[ApiMessageAndVersion] = sharedServer.raftManager

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  var linuxIoMetricsCollector: LinuxIoMetricsCollector = _
  @volatile var authorizer: Option[Authorizer] = None
  var tokenCache: DelegationTokenCache = _
  var credentialProvider: CredentialProvider = _
  var socketServer: SocketServer = _
  val socketServerFirstBoundPortFuture = new CompletableFuture[Integer]()
  var createTopicPolicy: Option[CreateTopicPolicy] = None
  var alterConfigPolicy: Option[AlterConfigPolicy] = None
  @volatile var quorumControllerMetrics: QuorumControllerMetrics = _
  var controller: Controller = _
  var quotaManagers: QuotaManagers = _
  var clientQuotaMetadataManager: ClientQuotaMetadataManager = _
  var controllerApis: ControllerApis = _
  var controllerApisHandlerPool: KafkaRequestHandlerPool = _
  def kafkaYammerMetrics: KafkaYammerMetrics = KafkaYammerMetrics.INSTANCE
  val metadataPublishers: util.List[MetadataPublisher] = new util.ArrayList[MetadataPublisher]()
  @volatile var metadataCache : KRaftMetadataCache = _
  @volatile var metadataCachePublisher: KRaftMetadataCachePublisher = _
  @volatile var featuresPublisher: FeaturesPublisher = _
  @volatile var registrationsPublisher: ControllerRegistrationsPublisher = _
  @volatile var incarnationId: Uuid = _
  @volatile var registrationManager: ControllerRegistrationManager = _
  @volatile var registrationChannelManager: NodeToControllerChannelManager = _

  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      status = to
      if (to == SHUTDOWN) awaitShutdownCond.signalAll()
    } finally {
      lock.unlock()
    }
    true
  }

  def clusterId: String = sharedServer.clusterId

  def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    val startupDeadline = Deadline.fromDelay(time, config.serverMaxStartupTimeMs, TimeUnit.MILLISECONDS)
    try {
      this.logIdent = logContext.logPrefix()
      info("Starting controller")
      config.dynamicConfig.initialize(zkClientOpt = None, clientMetricsReceiverPluginOpt = None)

      maybeChangeStatus(STARTING, STARTED)

      metricsGroup.newGauge("ClusterId", () => clusterId)
      metricsGroup.newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

      linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", time)
      if (linuxIoMetricsCollector.usable()) {
        metricsGroup.newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
        metricsGroup.newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
      }

      authorizer = config.createNewAuthorizer()
      authorizer.foreach(_.configure(config.originals))

      metadataCache = MetadataCache.kRaftMetadataCache(config.nodeId, () => raftManager.client.kraftVersion())

      metadataCachePublisher = new KRaftMetadataCachePublisher(metadataCache)

      featuresPublisher = new FeaturesPublisher(logContext)

      registrationsPublisher = new ControllerRegistrationsPublisher()

      incarnationId = Uuid.randomUuid()

      val apiVersionManager = new SimpleApiVersionManager(
        ListenerType.CONTROLLER,
        config.unstableApiVersionsEnabled,
        () => featuresPublisher.features()
      )

      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)
      socketServer = new SocketServer(config,
        metrics,
        time,
        credentialProvider,
        apiVersionManager,
        sharedServer.socketFactory)

      val listenerInfo = ListenerInfo
        .create(config.effectiveAdvertisedControllerListeners.map(_.toJava).asJava)
        .withWildcardHostnamesResolved()
        .withEphemeralPortsCorrected(name => socketServer.boundPort(new ListenerName(name)))
      socketServerFirstBoundPortFuture.complete(listenerInfo.firstListener().port())

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

      sharedServer.startForController(listenerInfo)

      createTopicPolicy = Option(config.
        getConfiguredInstance(CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG, classOf[CreateTopicPolicy]))
      alterConfigPolicy = Option(config.
        getConfiguredInstance(ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG, classOf[AlterConfigPolicy]))

      val voterConnections = FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "controller quorum voters future",
        sharedServer.controllerQuorumVotersFuture,
        startupDeadline, time)
      val controllerNodes = QuorumConfig.voterConnectionsToNodes(voterConnections)
      val quorumFeatures = new QuorumFeatures(config.nodeId,
        QuorumFeatures.defaultFeatureMap(config.unstableFeatureVersionsEnabled),
        controllerNodes.asScala.map(node => Integer.valueOf(node.id())).asJava)

      val delegationTokenKeyString = {
        if (config.tokenAuthEnabled) {
          config.delegationTokenSecretKey.value
        } else {
          null
        }
      }

      val controllerBuilder = {
        val leaderImbalanceCheckIntervalNs = if (config.autoLeaderRebalanceEnable) {
          OptionalLong.of(TimeUnit.NANOSECONDS.convert(config.leaderImbalanceCheckIntervalSeconds, TimeUnit.SECONDS))
        } else {
          OptionalLong.empty()
        }

        val maxIdleIntervalNs = config.metadataMaxIdleIntervalNs.fold(OptionalLong.empty)(OptionalLong.of)

        quorumControllerMetrics = new QuorumControllerMetrics(Optional.of(KafkaYammerMetrics.defaultRegistry), time)

        new QuorumController.Builder(config.nodeId, sharedServer.clusterId).
          setTime(time).
          setThreadNamePrefix(s"quorum-controller-${config.nodeId}-").
          setConfigSchema(configSchema).
          setRaftClient(raftManager.client).
          setQuorumFeatures(quorumFeatures).
          setDefaultReplicationFactor(config.defaultReplicationFactor.toShort).
          setDefaultNumPartitions(config.numPartitions.intValue()).
          setSessionTimeoutNs(TimeUnit.NANOSECONDS.convert(config.brokerSessionTimeoutMs.longValue(),
            TimeUnit.MILLISECONDS)).
          setLeaderImbalanceCheckIntervalNs(leaderImbalanceCheckIntervalNs).
          setMaxIdleIntervalNs(maxIdleIntervalNs).
          setMetrics(quorumControllerMetrics).
          setCreateTopicPolicy(createTopicPolicy.toJava).
          setAlterConfigPolicy(alterConfigPolicy.toJava).
          setConfigurationValidator(new ControllerConfigurationValidator(sharedServer.brokerConfig)).
          setStaticConfig(config.originals).
          setBootstrapMetadata(bootstrapMetadata).
          setFatalFaultHandler(sharedServer.fatalQuorumControllerFaultHandler).
          setNonFatalFaultHandler(sharedServer.nonFatalQuorumControllerFaultHandler).
          setDelegationTokenCache(tokenCache).
          setDelegationTokenSecretKey(delegationTokenKeyString).
          setDelegationTokenMaxLifeMs(config.delegationTokenMaxLifeMs).
          setDelegationTokenExpiryTimeMs(config.delegationTokenExpiryTimeMs).
          setDelegationTokenExpiryCheckIntervalMs(config.delegationTokenExpiryCheckIntervalMs).
          setUncleanLeaderElectionCheckIntervalMs(config.uncleanLeaderElectionCheckIntervalMs).
          setInterBrokerListenerName(config.interBrokerListenerName.value())
      }
      controller = controllerBuilder.build()

      // If we are using a ClusterMetadataAuthorizer, requests to add or remove ACLs must go
      // through the controller.
      authorizer match {
        case Some(a: ClusterMetadataAuthorizer) => a.setAclMutator(controller)
        case _ =>
      }

      quotaManagers = QuotaFactory.instantiate(config,
        metrics,
        time,
        s"controller-${config.nodeId}-")
      clientQuotaMetadataManager = new ClientQuotaMetadataManager(quotaManagers, socketServer.connectionQuotas)
      controllerApis = new ControllerApis(socketServer.dataPlaneRequestChannel,
        authorizer,
        quotaManagers,
        time,
        controller,
        raftManager,
        config,
        clusterId,
        registrationsPublisher,
        apiVersionManager,
        metadataCache)
      controllerApisHandlerPool = new KafkaRequestHandlerPool(config.nodeId,
        socketServer.dataPlaneRequestChannel,
        controllerApis,
        time,
        config.numIoThreads,
        s"${DataPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent",
        DataPlaneAcceptor.ThreadPrefix,
        "controller")

      // Set up the metadata cache publisher.
      metadataPublishers.add(metadataCachePublisher)

      // Set up the metadata features publisher.
      metadataPublishers.add(featuresPublisher)

      // Set up the controller registrations publisher.
      metadataPublishers.add(registrationsPublisher)

      // Create the registration manager, which handles sending KIP-919 controller registrations.
      registrationManager = new ControllerRegistrationManager(config.nodeId,
        clusterId,
        time,
        s"controller-${config.nodeId}-",
        QuorumFeatures.defaultFeatureMap(config.unstableFeatureVersionsEnabled),
        incarnationId,
        listenerInfo)

      // Add the registration manager to the list of metadata publishers, so that it receives
      // callbacks when the cluster registrations change.
      metadataPublishers.add(registrationManager)

      // Set up the dynamic config publisher. This runs even in combined mode, since the broker
      // has its own separate dynamic configuration object.
      metadataPublishers.add(new DynamicConfigPublisher(
        config,
        sharedServer.metadataPublishingFaultHandler,
        immutable.Map[String, ConfigHandler](
          // controllers don't host topics, so no need to do anything with dynamic topic config changes here
          ConfigType.BROKER -> new BrokerConfigHandler(config, quotaManagers)
        ),
        "controller"))

      // Register this instance for dynamic config changes to the KafkaConfig. This must be called
      // after the authorizer and quotaManagers are initialized, since it references those objects.
      // It must be called before DynamicClientQuotaPublisher is installed, since otherwise we may
      // miss the initial update which establishes the dynamic configurations that are in effect on
      // startup.
      config.dynamicConfig.addReconfigurables(this)

      // Set up the client quotas publisher. This will enable controller mutation quotas and any
      // other quotas which are applicable.
      metadataPublishers.add(new DynamicClientQuotaPublisher(
        config,
        sharedServer.metadataPublishingFaultHandler,
        "controller",
        clientQuotaMetadataManager))

      // Set up the SCRAM publisher.
      metadataPublishers.add(new ScramPublisher(
        config,
        sharedServer.metadataPublishingFaultHandler,
        "controller",
        credentialProvider
      ))

      // Set up the DelegationToken publisher.
      // We need a tokenManager for the Publisher
      // The tokenCache in the tokenManager is the same used in DelegationTokenControlManager
      metadataPublishers.add(new DelegationTokenPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          "controller",
          new DelegationTokenManager(config, tokenCache, time)
      ))

      // Set up the metrics publisher.
      metadataPublishers.add(new ControllerMetadataMetricsPublisher(
        sharedServer.controllerServerMetrics,
        sharedServer.metadataPublishingFaultHandler
      ))

      // Set up the ACL publisher.
      metadataPublishers.add(new AclPublisher(
        config.nodeId,
        sharedServer.metadataPublishingFaultHandler,
        "controller",
        authorizer
      ))

      // Install all metadata publishers.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the controller metadata publishers to be installed",
        sharedServer.loader.installPublishers(metadataPublishers), startupDeadline, time)

      val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = endpointReadyFutures.futures().asScala.toMap

      /**
       * Enable the controller endpoint(s). If we are using an authorizer which stores
       * ACLs in the metadata log, such as StandardAuthorizer, we will be able to start
       * accepting requests from principals included super.users right after this point,
       * but we will not be able to process requests from non-superusers until AclPublisher
       * publishes metadata from the QuorumController. MetadataPublishers do not publish
       * metadata until the controller has caught up to the high watermark.
       */
      val socketServerFuture = socketServer.enableRequestProcessing(authorizerFutures)

      /**
       * Start the KIP-919 controller registration manager.
       */
      val controllerNodeProvider = RaftControllerNodeProvider(raftManager, config)
      registrationChannelManager = new NodeToControllerChannelManagerImpl(
        controllerNodeProvider,
        time,
        metrics,
        config,
        "registration",
        s"controller-${config.nodeId}-",
        5000)
      registrationChannelManager.start()
      registrationManager.start(registrationChannelManager)

      // Block here until all the authorizer futures are complete
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the authorizer futures to be completed",
        CompletableFuture.allOf(authorizerFutures.values.toSeq: _*), startupDeadline, time)

      // Wait for all the SocketServer ports to be open, and the Acceptors to be started.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the SocketServer Acceptors to be started",
        socketServerFuture, startupDeadline, time)
    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        sharedServer.controllerStartupFaultHandler.handleFault("caught exception", e)
        shutdown()
        throw e
    }
  }

  def shutdown(): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      info("shutting down")
      // Ensure that we're not the Raft leader prior to shutting down our socket server, for a
      // smoother transition.
      sharedServer.ensureNotRaftLeader()
      incarnationId = null
      Utils.closeQuietly(registrationManager, "registration manager")
      registrationManager = null
      if (registrationChannelManager != null) {
        CoreUtils.swallow(registrationChannelManager.shutdown(), this)
        registrationChannelManager = null
      }
      metadataPublishers.forEach(p => sharedServer.loader.removeAndClosePublisher(p).get())
      metadataPublishers.clear()
      if (metadataCache != null) {
        metadataCache = null
      }
      Utils.closeQuietly(metadataCachePublisher, "metadata cache publisher")
      metadataCachePublisher = null
      Utils.closeQuietly(featuresPublisher, "features publisher")
      featuresPublisher = null
      Utils.closeQuietly(registrationsPublisher, "registrations publisher")
      registrationsPublisher = null
      if (socketServer != null)
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      if (controller != null)
        controller.beginShutdown()
      if (socketServer != null)
        CoreUtils.swallow(socketServer.shutdown(), this)
      if (controllerApisHandlerPool != null)
        CoreUtils.swallow(controllerApisHandlerPool.shutdown(), this)
      if (controllerApis != null)
        CoreUtils.swallow(controllerApis.close(), this)
      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)
      Utils.closeQuietly(controller, "controller")
      Utils.closeQuietly(quorumControllerMetrics, "quorum controller metrics")
      authorizer.foreach(Utils.closeQuietly(_, "authorizer"))
      createTopicPolicy.foreach(policy => Utils.closeQuietly(policy, "create topic policy"))
      alterConfigPolicy.foreach(policy => Utils.closeQuietly(policy, "alter config policy"))
      socketServerFirstBoundPortFuture.completeExceptionally(new RuntimeException("shutting down"))
      CoreUtils.swallow(config.dynamicConfig.clear(), this)
      sharedServer.stopForController()
    } catch {
      case e: Throwable =>
        fatal("Fatal error during controller shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  def awaitShutdown(): Unit = {
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
}
