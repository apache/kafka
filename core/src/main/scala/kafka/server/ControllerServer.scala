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

import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util
import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.Broker.ServerInfo
import kafka.log.LogConfig
import kafka.metrics.{KafkaMetricsGroup, KafkaYammerMetrics, LinuxIoMetricsCollector}
import kafka.network.SocketServer
import kafka.raft.RaftManager
import kafka.security.CredentialProvider
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{ClusterResource, Endpoint}
import org.apache.kafka.controller.{Controller, QuorumController, QuorumControllerMetrics}
import org.apache.kafka.metadata.{ApiMessageAndVersion, VersionRange}
import org.apache.kafka.metalog.MetaLogManager
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.raft.RaftConfig.AddressSpec
import org.apache.kafka.server.authorizer.Authorizer

import scala.jdk.CollectionConverters._

/**
 * A Kafka controller that runs in KRaft (Kafka Raft) mode.
 */
class ControllerServer(
                        val metaProperties: MetaProperties,
                        val config: KafkaConfig,
                        val metaLogManager: MetaLogManager,
                        val raftManager: RaftManager[ApiMessageAndVersion],
                        val time: Time,
                        val metrics: Metrics,
                        val threadNamePrefix: Option[String],
                        val controllerQuorumVotersFuture: CompletableFuture[util.Map[Integer, AddressSpec]]
                      ) extends Logging with KafkaMetricsGroup {
  import kafka.server.Server._

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  var linuxIoMetricsCollector: LinuxIoMetricsCollector = null
  var authorizer: Option[Authorizer] = null
  var tokenCache: DelegationTokenCache = null
  var credentialProvider: CredentialProvider = null
  var socketServer: SocketServer = null
  val socketServerFirstBoundPortFuture = new CompletableFuture[Integer]()
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

  def clusterId: String = metaProperties.clusterId.toString

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
      socketServerFirstBoundPortFuture.complete(socketServer.boundPort(
        config.controllerListeners.head.listenerName))

      val configDefs = Map(ConfigResource.Type.BROKER -> KafkaConfig.configDef,
        ConfigResource.Type.TOPIC -> LogConfig.configDefCopy).asJava
      val threadNamePrefixAsString = threadNamePrefix.getOrElse("")
      controller = new QuorumController.Builder(config.nodeId).
        setTime(time).
        setThreadNamePrefix(threadNamePrefixAsString).
        setConfigDefs(configDefs).
        setLogManager(metaLogManager).
        setDefaultReplicationFactor(config.defaultReplicationFactor.toShort).
        setDefaultNumPartitions(config.numPartitions.intValue()).
        setSessionTimeoutNs(TimeUnit.NANOSECONDS.convert(config.brokerSessionTimeoutMs.longValue(),
          TimeUnit.MILLISECONDS)).
        setMetrics(new QuorumControllerMetrics(KafkaYammerMetrics.defaultRegistry())).
        build()


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
        s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent",
        SocketServer.DataPlaneThreadPrefix)
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
      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)
      if (controller != null)
        controller.close()
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
