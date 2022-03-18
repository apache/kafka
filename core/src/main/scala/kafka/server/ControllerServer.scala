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

import java.util
import java.util.OptionalLong
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CompletableFuture, TimeUnit}

import kafka.api.KAFKA_3_2_IV0
import kafka.cluster.Broker.ServerInfo
import kafka.metrics.{KafkaMetricsGroup, KafkaYammerMetrics, LinuxIoMetricsCollector}
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.raft.RaftManager
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig.{AlterConfigPolicyClassNameProp, CreateTopicPolicyClassNameProp}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{ClusterResource, Endpoint}
import org.apache.kafka.controller.{Controller, QuorumController, QuorumControllerMetrics}
import org.apache.kafka.metadata.{KafkaConfigSchema, VersionRange}
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.raft.RaftConfig.AddressSpec
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer
import org.apache.kafka.server.policy.{AlterConfigPolicy, CreateTopicPolicy}

import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._

/**
 * A Kafka controller that runs in KRaft (Kafka Raft) mode.
 */
class ControllerServer(
  val metaProperties: MetaProperties,
  val config: KafkaConfig,
  val raftManager: RaftManager[ApiMessageAndVersion],
  val time: Time,
  val metrics: Metrics,
  val threadNamePrefix: Option[String],
  val controllerQuorumVotersFuture: CompletableFuture[util.Map[Integer, AddressSpec]],
  val configSchema: KafkaConfigSchema,
) extends Logging with KafkaMetricsGroup {
  import kafka.server.Server._

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  var linuxIoMetricsCollector: LinuxIoMetricsCollector = null
  @volatile var authorizer: Option[Authorizer] = null
  var tokenCache: DelegationTokenCache = null
  var credentialProvider: CredentialProvider = null
  var socketServer: SocketServer = null
  val socketServerFirstBoundPortFuture = new CompletableFuture[Integer]()
  var createTopicPolicy: Option[CreateTopicPolicy] = None
  var alterConfigPolicy: Option[AlterConfigPolicy] = None
  var controller: Controller = null
  val supportedFeatures: Map[String, VersionRange] = Map()
  var quotaManagers: QuotaManagers = null
  var controllerApis: ControllerApis = null
  var controllerApisHandlerPool: KafkaRequestHandlerPool = null

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

  def clusterId: String = metaProperties.clusterId

  def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    try {
      info("Starting controller")

      maybeChangeStatus(STARTING, STARTED)
      // TODO: initialize the log dir(s)
      this.logIdent = new LogContext(s"[ControllerServer id=${config.nodeId}] ").logPrefix()

      newGauge("ClusterId", () => clusterId)
      newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

      linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", time, logger.underlying)
      if (linuxIoMetricsCollector.usable()) {
        newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
        newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
      }

      val javaListeners = config.controllerListeners.map(_.toJava).asJava
      authorizer = config.authorizer
      authorizer.foreach(_.configure(config.originals))

      val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
        case Some(authZ) =>
          // It would be nice to remove some of the broker-specific assumptions from
          // AuthorizerServerInfo, such as the assumption that there is an inter-broker
          // listener, or that ID is named brokerId.
          val controllerAuthorizerInfo = ServerInfo(
            new ClusterResource(clusterId), config.nodeId, javaListeners, javaListeners.get(0))
          authZ.start(controllerAuthorizerInfo).asScala.map { case (ep, cs) =>
            ep -> cs.toCompletableFuture
          }.toMap
        case None =>
          javaListeners.asScala.map {
            ep => ep -> CompletableFuture.completedFuture[Void](null)
          }.toMap
      }

      val apiVersionManager = new SimpleApiVersionManager(ListenerType.CONTROLLER)

      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)
      socketServer = new SocketServer(config,
        metrics,
        time,
        credentialProvider,
        apiVersionManager)
      socketServer.startup(startProcessingRequests = false, controlPlaneListener = None, config.controllerListeners)

      if (config.controllerListeners.nonEmpty) {
        socketServerFirstBoundPortFuture.complete(socketServer.boundPort(
          config.controllerListeners.head.listenerName))
      } else {
        throw new ConfigException("No controller.listener.names defined for controller");
      }

      val threadNamePrefixAsString = threadNamePrefix.getOrElse("")

      createTopicPolicy = Option(config.
        getConfiguredInstance(CreateTopicPolicyClassNameProp, classOf[CreateTopicPolicy]))
      alterConfigPolicy = Option(config.
        getConfiguredInstance(AlterConfigPolicyClassNameProp, classOf[AlterConfigPolicy]))

      val controllerBuilder = {
        val leaderImbalanceCheckIntervalNs = if (config.autoLeaderRebalanceEnable) {
          OptionalLong.of(TimeUnit.NANOSECONDS.convert(config.leaderImbalanceCheckIntervalSeconds, TimeUnit.SECONDS))
        } else {
          OptionalLong.empty()
        }

        new QuorumController.Builder(config.nodeId, metaProperties.clusterId).
          setTime(time).
          setThreadNamePrefix(threadNamePrefixAsString).
          setConfigSchema(configSchema).
          setRaftClient(raftManager.client).
          setDefaultReplicationFactor(config.defaultReplicationFactor.toShort).
          setDefaultNumPartitions(config.numPartitions.intValue()).
          setIsLeaderRecoverySupported(config.interBrokerProtocolVersion >= KAFKA_3_2_IV0).
          setSessionTimeoutNs(TimeUnit.NANOSECONDS.convert(config.brokerSessionTimeoutMs.longValue(),
            TimeUnit.MILLISECONDS)).
          setSnapshotMaxNewRecordBytes(config.metadataSnapshotMaxNewRecordBytes).
          setLeaderImbalanceCheckIntervalNs(leaderImbalanceCheckIntervalNs).
          setMetrics(new QuorumControllerMetrics(KafkaYammerMetrics.defaultRegistry())).
          setCreateTopicPolicy(createTopicPolicy.asJava).
          setAlterConfigPolicy(alterConfigPolicy.asJava).
          setConfigurationValidator(new ControllerConfigurationValidator())
      }
      authorizer match {
        case Some(a: ClusterMetadataAuthorizer) => controllerBuilder.setAuthorizer(a)
        case _ => // nothing to do
      }
      controller = controllerBuilder.build()

      quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))
      val controllerNodes = RaftConfig.voterConnectionsToNodes(controllerQuorumVotersFuture.get()).asScala
      controllerApis = new ControllerApis(socketServer.dataPlaneRequestChannel,
        authorizer,
        quotaManagers,
        time,
        supportedFeatures,
        controller,
        raftManager,
        config,
        metaProperties,
        controllerNodes.toSeq,
        apiVersionManager)
      controllerApisHandlerPool = new KafkaRequestHandlerPool(config.nodeId,
        socketServer.dataPlaneRequestChannel,
        controllerApis,
        time,
        config.numIoThreads,
        s"${DataPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent",
        DataPlaneAcceptor.ThreadPrefix)
      socketServer.startProcessingRequests(authorizerFutures)
    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during controller startup. Prepare to shutdown", e)
        shutdown()
        throw e
    }
  }

  def shutdown(): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      info("shutting down")
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
      if (controller != null)
        controller.close()
      createTopicPolicy.foreach(policy => CoreUtils.swallow(policy.close(), this))
      alterConfigPolicy.foreach(policy => CoreUtils.swallow(policy.close(), this))
      socketServerFirstBoundPortFuture.completeExceptionally(new RuntimeException("shutting down"))
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
