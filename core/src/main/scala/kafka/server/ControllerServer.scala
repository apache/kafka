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

import kafka.cluster.Broker.ServerInfo
import kafka.metrics.LinuxIoMetricsCollector
import kafka.migration.MigrationPropagator
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.raft.KafkaRaftManager
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig.{AlterConfigPolicyClassNameProp, CreateTopicPolicyClassNameProp}
import kafka.server.QuotaFactory.QuotaManagers

import scala.collection.immutable
import kafka.server.metadata.{ClientQuotaMetadataManager, DynamicClientQuotaPublisher, DynamicConfigPublisher, ScramPublisher}
import kafka.utils.{CoreUtils, Logging, PasswordEncoder}
import kafka.zk.{KafkaZkClient, ZkMigrationClient}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.{ClusterResource, Endpoint}
import org.apache.kafka.controller.metrics.{ControllerMetadataMetricsPublisher, QuorumControllerMetrics}
import org.apache.kafka.controller.{Controller, QuorumController, QuorumFeatures}
import org.apache.kafka.image.publisher.MetadataPublisher
import org.apache.kafka.metadata.KafkaConfigSchema
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata
import org.apache.kafka.metadata.migration.{KRaftMigrationDriver, LegacyPropagator}
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics}
import org.apache.kafka.server.policy.{AlterConfigPolicy, CreateTopicPolicy}
import org.apache.kafka.server.util.{Deadline, FutureUtils}

import java.util
import java.util.{Optional, OptionalLong}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._


case class ControllerMigrationSupport(
  zkClient: KafkaZkClient,
  migrationDriver: KRaftMigrationDriver,
  brokersRpcClient: LegacyPropagator
) {
  def shutdown(logging: Logging): Unit = {
    if (zkClient != null) {
      CoreUtils.swallow(zkClient.close(), logging)
    }
    if (brokersRpcClient != null) {
      CoreUtils.swallow(brokersRpcClient.shutdown(), logging)
    }
    if (migrationDriver != null) {
      CoreUtils.swallow(migrationDriver.close(), logging)
    }
  }
}

/**
 * A Kafka controller that runs in KRaft (Kafka Raft) mode.
 */
class ControllerServer(
  val sharedServer: SharedServer,
  val configSchema: KafkaConfigSchema,
  val bootstrapMetadata: BootstrapMetadata,
) extends Logging {

  import kafka.server.Server._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val config = sharedServer.controllerConfig
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
  var migrationSupport: Option[ControllerMigrationSupport] = None
  def kafkaYammerMetrics: KafkaYammerMetrics = KafkaYammerMetrics.INSTANCE
  val metadataPublishers: util.List[MetadataPublisher] = new util.ArrayList[MetadataPublisher]()

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

  def clusterId: String = sharedServer.metaProps.clusterId

  def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    val startupDeadline = Deadline.fromDelay(time, config.serverMaxStartupTimeMs, TimeUnit.MILLISECONDS)
    try {
      this.logIdent = new LogContext(s"[ControllerServer id=${config.nodeId}] ").logPrefix()
      info("Starting controller")
      config.dynamicConfig.initialize(zkClientOpt = None)

      maybeChangeStatus(STARTING, STARTED)

      metricsGroup.newGauge("ClusterId", () => clusterId)
      metricsGroup.newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

      linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", time, logger.underlying)
      if (linuxIoMetricsCollector.usable()) {
        metricsGroup.newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
        metricsGroup.newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
      }

      val javaListeners = config.controllerListeners.map(_.toJava).asJava
      authorizer = config.createNewAuthorizer()
      authorizer.foreach(_.configure(config.originals))

      val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
        case Some(authZ) =>
          // It would be nice to remove some of the broker-specific assumptions from
          // AuthorizerServerInfo, such as the assumption that there is an inter-broker
          // listener, or that ID is named brokerId.
          val controllerAuthorizerInfo = ServerInfo(
            new ClusterResource(clusterId),
            config.nodeId,
            javaListeners,
            javaListeners.get(0),
            config.earlyStartListeners.map(_.value()).asJava)
          authZ.start(controllerAuthorizerInfo).asScala.map { case (ep, cs) =>
            ep -> cs.toCompletableFuture
          }.toMap
        case None =>
          javaListeners.asScala.map {
            ep => ep -> CompletableFuture.completedFuture[Void](null)
          }.toMap
      }

      val apiVersionManager = new SimpleApiVersionManager(
        ListenerType.CONTROLLER,
        config.unstableApiVersionsEnabled,
        config.migrationEnabled
      )

      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)
      socketServer = new SocketServer(config,
        metrics,
        time,
        credentialProvider,
        apiVersionManager)

      if (config.controllerListeners.nonEmpty) {
        socketServerFirstBoundPortFuture.complete(socketServer.boundPort(
          config.controllerListeners.head.listenerName))
      } else {
        throw new ConfigException("No controller.listener.names defined for controller")
      }

      sharedServer.startForController()

      createTopicPolicy = Option(config.
        getConfiguredInstance(CreateTopicPolicyClassNameProp, classOf[CreateTopicPolicy]))
      alterConfigPolicy = Option(config.
        getConfiguredInstance(AlterConfigPolicyClassNameProp, classOf[AlterConfigPolicy]))

      val voterConnections = FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "controller quorum voters future",
        sharedServer.controllerQuorumVotersFuture,
        startupDeadline, time)
      val controllerNodes = RaftConfig.voterConnectionsToNodes(voterConnections)
      val quorumFeatures = QuorumFeatures.create(config.nodeId,
        sharedServer.raftManager.apiVersions,
        QuorumFeatures.defaultFeatureMap(),
        controllerNodes)

      val controllerBuilder = {
        val leaderImbalanceCheckIntervalNs = if (config.autoLeaderRebalanceEnable) {
          OptionalLong.of(TimeUnit.NANOSECONDS.convert(config.leaderImbalanceCheckIntervalSeconds, TimeUnit.SECONDS))
        } else {
          OptionalLong.empty()
        }

        val maxIdleIntervalNs = config.metadataMaxIdleIntervalNs.fold(OptionalLong.empty)(OptionalLong.of)

        quorumControllerMetrics = new QuorumControllerMetrics(Optional.of(KafkaYammerMetrics.defaultRegistry), time)

        new QuorumController.Builder(config.nodeId, sharedServer.metaProps.clusterId).
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
          setCreateTopicPolicy(createTopicPolicy.asJava).
          setAlterConfigPolicy(alterConfigPolicy.asJava).
          setConfigurationValidator(new ControllerConfigurationValidator()).
          setStaticConfig(config.originals).
          setBootstrapMetadata(bootstrapMetadata).
          setFatalFaultHandler(sharedServer.quorumControllerFaultHandler).
          setZkMigrationEnabled(config.migrationEnabled)
      }
      authorizer match {
        case Some(a: ClusterMetadataAuthorizer) => controllerBuilder.setAuthorizer(a)
        case _ => // nothing to do
      }
      controller = controllerBuilder.build()

      if (config.migrationEnabled) {
        val zkClient = KafkaZkClient.createZkClient("KRaft Migration", time, config, KafkaServer.zkClientConfigFromKafkaConfig(config))
        val zkConfigEncoder = config.passwordEncoderSecret match {
          case Some(secret) => PasswordEncoder.encrypting(secret,
            config.passwordEncoderKeyFactoryAlgorithm,
            config.passwordEncoderCipherAlgorithm,
            config.passwordEncoderKeyLength,
            config.passwordEncoderIterations)
          case None => PasswordEncoder.noop()
        }
        val migrationClient = ZkMigrationClient(zkClient, zkConfigEncoder)
        val propagator: LegacyPropagator = new MigrationPropagator(config.nodeId, config)
        val migrationDriver = new KRaftMigrationDriver(
          config.nodeId,
          controller.asInstanceOf[QuorumController].zkRecordConsumer(),
          migrationClient,
          propagator,
          publisher => sharedServer.loader.installPublishers(java.util.Collections.singletonList(publisher)),
          sharedServer.faultHandlerFactory.build(
            "zk migration",
            fatal = false,
            () => {}
          ),
          quorumFeatures
        )
        migrationDriver.start()
        migrationSupport = Some(ControllerMigrationSupport(zkClient, migrationDriver, propagator))
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
        sharedServer.metaProps,
        controllerNodes.asScala.toSeq,
        apiVersionManager)
      controllerApisHandlerPool = new KafkaRequestHandlerPool(config.nodeId,
        socketServer.dataPlaneRequestChannel,
        controllerApis,
        time,
        config.numIoThreads,
        s"${DataPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent",
        DataPlaneAcceptor.ThreadPrefix)

      /**
       * Enable the controller endpoint(s). If we are using an authorizer which stores
       * ACLs in the metadata log, such as StandardAuthorizer, we will be able to start
       * accepting requests from principals included super.users right after this point,
       * but we will not be able to process requests from non-superusers until the
       * QuorumController declares that we have caught up to the high water mark of the
       * metadata log. See @link{QuorumController#maybeCompleteAuthorizerInitialLoad}
       * and KIP-801 for details.
       */
      val socketServerFuture = socketServer.enableRequestProcessing(authorizerFutures)

      // Block here until all the authorizer futures are complete
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the authorizer futures to be completed",
        CompletableFuture.allOf(authorizerFutures.values.toSeq: _*), startupDeadline, time)

      // Wait for all the SocketServer ports to be open, and the Acceptors to be started.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the SocketServer Acceptors to be started",
        socketServerFuture, startupDeadline, time)

      // register this instance for dynamic config changes to the KafkaConfig
      config.dynamicConfig.addReconfigurables(this)

      // Set up the dynamic config publisher. This runs even in combined mode, since the broker
      // has its own separate dynamic configuration object.
      metadataPublishers.add(new DynamicConfigPublisher(
        config,
        sharedServer.metadataPublishingFaultHandler,
        immutable.Map[String, ConfigHandler](
          // controllers don't host topics, so no need to do anything with dynamic topic config changes here
          ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers)
        ),
        "controller"))

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

      // Set up the metrics publisher.
      metadataPublishers.add(new ControllerMetadataMetricsPublisher(
        sharedServer.controllerServerMetrics,
        sharedServer.metadataPublishingFaultHandler
      ))

      // Install all metadata publishers.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the controller metadata publishers to be installed",
        sharedServer.loader.installPublishers(metadataPublishers), startupDeadline, time)
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
      metadataPublishers.forEach(p => sharedServer.loader.removeAndClosePublisher(p).get())
      metadataPublishers.clear()
      if (socketServer != null)
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      migrationSupport.foreach(_.shutdown(this))
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
      if (controller != null)
        controller.close()
      if (quorumControllerMetrics != null)
        CoreUtils.swallow(quorumControllerMetrics.close(), this)
      CoreUtils.swallow(authorizer.foreach(_.close()), this)
      createTopicPolicy.foreach(policy => CoreUtils.swallow(policy.close(), this))
      alterConfigPolicy.foreach(policy => CoreUtils.swallow(policy.close(), this))
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
