/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.raft

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Paths
import java.util.OptionalInt
import java.util.concurrent.CompletableFuture
import java.util.{Map => JMap}
import java.util.{Collection => JCollection}
import kafka.log.LogManager
import kafka.log.UnifiedLog
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils
import kafka.utils.Logging
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, MetadataRecoveryStrategy, NetworkClient}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, ListenerName, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.requests.RequestContext
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.raft.{Endpoints, FileQuorumStateStore, KafkaNetworkChannel, KafkaRaftClient, KafkaRaftClientDriver, LeaderAndEpoch, QuorumConfig, RaftClient, ReplicatedLog}
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.common.Feature
import org.apache.kafka.server.common.serialization.RecordSerde
import org.apache.kafka.server.util.{FileLock, KafkaScheduler}
import org.apache.kafka.server.fault.FaultHandler
import org.apache.kafka.server.util.timer.SystemTimer

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

object KafkaRaftManager {
  private def createLogDirectory(logDir: File, logDirName: String): File = {
    val logDirPath = logDir.getAbsolutePath
    val dir = new File(logDirPath, logDirName)
    Files.createDirectories(dir.toPath)
    dir
  }

  private def lockDataDir(dataDir: File): FileLock = {
    val lock = new FileLock(new File(dataDir, LogManager.LockFileName))

    if (!lock.tryLock()) {
      throw new KafkaException(
        s"Failed to acquire lock on file .lock in ${lock.file.getParent}. A Kafka instance in another process or " +
        "thread is using this directory."
      )
    }

    lock
  }

  /**
   * Test if the configured metadata log dir is one of the data log dirs.
   */
  private def hasDifferentLogDir(config: KafkaConfig): Boolean = {
    !config
      .logDirs
      .map(Paths.get(_).toAbsolutePath)
      .contains(Paths.get(config.metadataLogDir).toAbsolutePath)
  }

  /**
   * Obtain the file lock and delete the metadata log directory completely.
   *
   * This is only used by ZK brokers that are in pre-migration or hybrid mode of the ZK to KRaft migration.
   * The rationale for deleting the metadata log in these cases is that it is safe to do on brokers and it
   * makes recovery from a failed migration much easier. See KAFKA-16463.
   *
   * @param config  The broker config
   */
  def maybeDeleteMetadataLogDir(config: KafkaConfig): Unit = {
    // These constraints are enforced in KafkaServer, but repeating them here to guard against future callers
    if (config.processRoles.nonEmpty) {
      throw new RuntimeException("Not deleting metadata log dir since this node is in KRaft mode.")
    } else if (!config.migrationEnabled) {
      throw new RuntimeException("Not deleting metadata log dir since migrations are not enabled.")
    } else {
      val metadataDir = new File(config.metadataLogDir)
      val logDirName = UnifiedLog.logDirName(Topic.CLUSTER_METADATA_TOPIC_PARTITION)
      val metadataPartitionDir = KafkaRaftManager.createLogDirectory(metadataDir, logDirName)
      val deletionLock = if (hasDifferentLogDir(config)) {
        Some(KafkaRaftManager.lockDataDir(metadataDir))
      } else {
        None
      }

      try {
        Utils.delete(metadataPartitionDir)
      } catch {
        case t: Throwable => throw new RuntimeException("Failed to delete metadata log", t)
      } finally {
        deletionLock.foreach(_.destroy())
      }
    }
  }
}

trait RaftManager[T] {
  def handleRequest(
    context: RequestContext,
    header: RequestHeader,
    request: ApiMessage,
    createdTimeMs: Long
  ): CompletableFuture[ApiMessage]

  def register(
    listener: RaftClient.Listener[T]
  ): Unit

  def leaderAndEpoch: LeaderAndEpoch

  def client: RaftClient[T]

  def replicatedLog: ReplicatedLog

  def voterNode(id: Int, listener: ListenerName): Option[Node]
}

class KafkaRaftManager[T](
  clusterId: String,
  config: KafkaConfig,
  metadataLogDirUuid: Uuid,
  recordSerde: RecordSerde[T],
  topicPartition: TopicPartition,
  topicId: Uuid,
  time: Time,
  metrics: Metrics,
  threadNamePrefixOpt: Option[String],
  val controllerQuorumVotersFuture: CompletableFuture[JMap[Integer, InetSocketAddress]],
  bootstrapServers: JCollection[InetSocketAddress],
  localListeners: Endpoints,
  fatalFaultHandler: FaultHandler
) extends RaftManager[T] with Logging {

  val apiVersions = new ApiVersions()
  private val raftConfig = new QuorumConfig(config)
  private val threadNamePrefix = threadNamePrefixOpt.getOrElse("kafka-raft")
  private val logContext = new LogContext(s"[RaftManager id=${config.nodeId}] ")
  this.logIdent = logContext.logPrefix()

  private val scheduler = new KafkaScheduler(1, true, threadNamePrefix + "-scheduler")
  scheduler.startup()

  private val dataDir = createDataDir()

  private val dataDirLock = {
    // Acquire the log dir lock if the metadata log dir is different from the log dirs
    val differentMetadataLogDir = KafkaRaftManager.hasDifferentLogDir(config)

    // Or this node is only a controller
    val isOnlyController = config.processRoles == Set(ProcessRole.ControllerRole)

    if (differentMetadataLogDir || isOnlyController) {
      Some(KafkaRaftManager.lockDataDir(new File(config.metadataLogDir)))
    } else {
      None
    }
  }

  override val replicatedLog: ReplicatedLog = buildMetadataLog()
  private val netChannel = buildNetworkChannel()
  private val expirationTimer = new SystemTimer("raft-expiration-executor")
  private val expirationService = new TimingWheelExpirationService(expirationTimer)
  override val client: KafkaRaftClient[T] = buildRaftClient()
  private val clientDriver = new KafkaRaftClientDriver[T](client, threadNamePrefix, fatalFaultHandler, logContext)

  def startup(): Unit = {
    client.initialize(
      controllerQuorumVotersFuture.get(),
      new FileQuorumStateStore(new File(dataDir, FileQuorumStateStore.DEFAULT_FILE_NAME)),
      metrics
    )
    netChannel.start()
    clientDriver.start()
  }

  def shutdown(): Unit = {
    CoreUtils.swallow(expirationService.shutdown(), this)
    Utils.closeQuietly(expirationTimer, "expiration timer")
    CoreUtils.swallow(clientDriver.shutdown(), this)
    CoreUtils.swallow(scheduler.shutdown(), this)
    Utils.closeQuietly(netChannel, "net channel")
    Utils.closeQuietly(replicatedLog, "replicated log")
    CoreUtils.swallow(dataDirLock.foreach(_.destroy()), this)
  }

  override def register(
    listener: RaftClient.Listener[T]
  ): Unit = {
    client.register(listener)
  }

  override def handleRequest(
    context: RequestContext,
    header: RequestHeader,
    request: ApiMessage,
    createdTimeMs: Long
  ): CompletableFuture[ApiMessage] = {
    clientDriver.handleRequest(context, header, request, createdTimeMs)
  }

  private def buildRaftClient(): KafkaRaftClient[T] = {
    new KafkaRaftClient(
      OptionalInt.of(config.nodeId),
      metadataLogDirUuid,
      recordSerde,
      netChannel,
      replicatedLog,
      time,
      expirationService,
      logContext,
      // Controllers should always flush the log on replication because they may become voters
      config.processRoles.contains(ProcessRole.ControllerRole),
      clusterId,
      bootstrapServers,
      localListeners,
      Feature.KRAFT_VERSION.supportedVersionRange(),
      raftConfig
    )
  }

  private def buildNetworkChannel(): KafkaNetworkChannel = {
    val (listenerName, netClient) = buildNetworkClient()
    new KafkaNetworkChannel(time, listenerName, netClient, config.quorumConfig.requestTimeoutMs, threadNamePrefix)
  }

  private def createDataDir(): File = {
    val logDirName = UnifiedLog.logDirName(topicPartition)
    KafkaRaftManager.createLogDirectory(new File(config.metadataLogDir), logDirName)
  }

  private def buildMetadataLog(): KafkaMetadataLog = {
    KafkaMetadataLog(
      topicPartition,
      topicId,
      dataDir,
      time,
      scheduler,
      config = MetadataLogConfig(config, KafkaRaftClient.MAX_BATCH_SIZE_BYTES, KafkaRaftClient.MAX_FETCH_SIZE_BYTES)
    )
  }

  private def buildNetworkClient(): (ListenerName, NetworkClient) = {
    val controllerListenerName = new ListenerName(config.controllerListenerNames.head)
    val controllerSecurityProtocol = config.effectiveListenerSecurityProtocolMap.getOrElse(
      controllerListenerName,
      SecurityProtocol.forName(controllerListenerName.value())
    )
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      controllerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      controllerListenerName,
      config.saslMechanismControllerProtocol,
      time,
      config.saslInterBrokerHandshakeRequestEnable,
      logContext
    )

    val metricGroupPrefix = "raft-channel"
    val collectPerConnectionMetrics = false

    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      config.connectionsMaxIdleMs,
      metrics,
      time,
      metricGroupPrefix,
      Map.empty[String, String].asJava,
      collectPerConnectionMetrics,
      channelBuilder,
      logContext
    )

    val clientId = s"raft-client-${config.nodeId}"
    val maxInflightRequestsPerConnection = 1
    val reconnectBackoffMs = 50
    val reconnectBackoffMsMs = 500
    val discoverBrokerVersions = true

    val networkClient = new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      maxInflightRequestsPerConnection,
      reconnectBackoffMs,
      reconnectBackoffMsMs,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      config.quorumConfig.requestTimeoutMs,
      config.connectionSetupTimeoutMs,
      config.connectionSetupTimeoutMaxMs,
      time,
      discoverBrokerVersions,
      apiVersions,
      logContext,
      MetadataRecoveryStrategy.NONE
    )

    (controllerListenerName, networkClient)
  }

  override def leaderAndEpoch: LeaderAndEpoch = {
    client.leaderAndEpoch
  }

  override def voterNode(id: Int, listener: ListenerName): Option[Node] = {
    client.voterNode(id, listener).toScala
  }
}
