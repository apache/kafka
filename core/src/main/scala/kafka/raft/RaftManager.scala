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
import java.nio.file.Files
import java.util
import java.util.OptionalInt
import java.util.concurrent.CompletableFuture

import kafka.log.{Log, LogConfig, LogManager}
import kafka.raft.KafkaRaftManager.RaftIoThread
import kafka.server.{BrokerTopicStats, KafkaConfig, LogDirFailureChannel, MetaProperties}
import kafka.utils.timer.SystemTimer
import kafka.utils.{KafkaScheduler, Logging, ShutdownableThread}
import org.apache.kafka.clients.{ApiVersions, ClientDnsLookup, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.raft.RaftConfig.{AddressSpec, InetAddressSpec, NON_ROUTABLE_ADDRESS, UnknownAddressSpec}
import org.apache.kafka.raft.{FileBasedStateStore, KafkaRaftClient, RaftClient, RaftConfig, RaftRequest, RecordSerde}

import scala.jdk.CollectionConverters._

object KafkaRaftManager {
  class RaftIoThread(
    client: KafkaRaftClient[_],
    threadNamePrefix: String
  ) extends ShutdownableThread(
    name = threadNamePrefix + "-io-thread",
    isInterruptible = false
  ) {
    override def doWork(): Unit = {
      client.poll()
    }

    override def initiateShutdown(): Boolean = {
      if (super.initiateShutdown()) {
        client.shutdown(5000).whenComplete { (_, exception) =>
          if (exception != null) {
            error("Graceful shutdown of RaftClient failed", exception)
          } else {
            info("Completed graceful shutdown of RaftClient")
          }
        }
        true
      } else {
        false
      }
    }

    override def isRunning: Boolean = {
      client.isRunning && !isThreadFailed
    }
  }

  private def createLogDirectory(logDir: File, logDirName: String): File = {
    val logDirPath = logDir.getAbsolutePath
    val dir = new File(logDirPath, logDirName)
    Files.createDirectories(dir.toPath)
    dir
  }
}

trait RaftManager[T] {
  def handleRequest(
    header: RequestHeader,
    request: ApiMessage,
    createdTimeMs: Long
  ): CompletableFuture[ApiMessage]

  def register(
    listener: RaftClient.Listener[T]
  ): Unit

  def scheduleAppend(
    epoch: Int,
    records: Seq[T]
  ): Option[Long]
}

class KafkaRaftManager[T](
  metaProperties: MetaProperties,
  config: KafkaConfig,
  recordSerde: RecordSerde[T],
  topicPartition: TopicPartition,
  time: Time,
  metrics: Metrics,
  threadNamePrefixOpt: Option[String]
) extends RaftManager[T] with Logging {

  private val raftConfig = new RaftConfig(config)
  private val threadNamePrefix = threadNamePrefixOpt.getOrElse("kafka-raft")
  private val logContext = new LogContext(s"[RaftManager nodeId=${config.nodeId}] ")
  this.logIdent = logContext.logPrefix()

  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix + "-scheduler")
  scheduler.startup()

  private val dataDir = createDataDir()
  private val metadataLog = buildMetadataLog()
  private val netChannel = buildNetworkChannel()
  private val raftClient = buildRaftClient()
  private val raftIoThread = new RaftIoThread(raftClient, threadNamePrefix)

  def startup(): Unit = {
    // Update the voter endpoints (if valid) with what's in RaftConfig
    val voterAddresses: util.Map[Integer, AddressSpec] = raftConfig.quorumVoterConnections
    for (voterAddressEntry <- voterAddresses.entrySet.asScala) {
      voterAddressEntry.getValue match {
        case spec: InetAddressSpec =>
          netChannel.updateEndpoint(voterAddressEntry.getKey, spec)
        case _: UnknownAddressSpec =>
          logger.info(s"Skipping channel update for destination ID: ${voterAddressEntry.getKey} " +
            s"because of non-routable endpoint: ${NON_ROUTABLE_ADDRESS.toString}")
        case invalid: AddressSpec =>
          logger.warn(s"Unexpected address spec (type: ${invalid.getClass}) for channel update for " +
            s"destination ID: ${voterAddressEntry.getKey}")
      }
    }
    netChannel.start()
    raftIoThread.start()
  }

  def shutdown(): Unit = {
    raftIoThread.shutdown()
    raftClient.close()
    scheduler.shutdown()
    netChannel.close()
    metadataLog.close()
  }

  override def register(
    listener: RaftClient.Listener[T]
  ): Unit = {
    raftClient.register(listener)
  }

  override def scheduleAppend(
    epoch: Int,
    records: Seq[T]
  ): Option[Long] = {
    val offset: java.lang.Long = raftClient.scheduleAppend(epoch, records.asJava)
    if (offset == null) {
      None
    } else {
      Some(Long.unbox(offset))
    }
  }

  override def handleRequest(
    header: RequestHeader,
    request: ApiMessage,
    createdTimeMs: Long
  ): CompletableFuture[ApiMessage] = {
    val inboundRequest = new RaftRequest.Inbound(
      header.correlationId,
      request,
      createdTimeMs
    )

    raftClient.handle(inboundRequest)

    inboundRequest.completion.thenApply { response =>
      response.data
    }
  }

  private def buildRaftClient(): KafkaRaftClient[T] = {
    val expirationTimer = new SystemTimer("raft-expiration-executor")
    val expirationService = new TimingWheelExpirationService(expirationTimer)
    val quorumStateStore = new FileBasedStateStore(new File(dataDir, "quorum-state"))

    val client = new KafkaRaftClient(
      recordSerde,
      netChannel,
      metadataLog,
      quorumStateStore,
      time,
      metrics,
      expirationService,
      logContext,
      OptionalInt.of(config.nodeId),
      raftConfig
    )
    client.initialize()
    client
  }

  private def buildNetworkChannel(): KafkaNetworkChannel = {
    val netClient = buildNetworkClient()
    new KafkaNetworkChannel(time, netClient, config.quorumRequestTimeoutMs, threadNamePrefix)
  }

  private def createDataDir(): File = {
    val logDirName = Log.logDirName(topicPartition)
    KafkaRaftManager.createLogDirectory(new File(config.metadataLogDir), logDirName)
  }

  private def buildMetadataLog(): KafkaMetadataLog = {
    val defaultProps = LogConfig.extractLogConfigMap(config)
    LogConfig.validateValues(defaultProps)
    val defaultLogConfig = LogConfig(defaultProps)

    val log = Log(
      dir = dataDir,
      config = defaultLogConfig,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = scheduler,
      brokerTopicStats = new BrokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = config.transactionalIdExpirationMs,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(5)
    )

    KafkaMetadataLog(log, topicPartition)
  }

  private def buildNetworkClient(): NetworkClient = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      config.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      config.interBrokerListenerName,
      config.saslMechanismInterBrokerProtocol,
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
    val discoverBrokerVersions = false

    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      maxInflightRequestsPerConnection,
      reconnectBackoffMs,
      reconnectBackoffMsMs,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      config.quorumRequestTimeoutMs,
      config.connectionSetupTimeoutMs,
      config.connectionSetupTimeoutMaxMs,
      ClientDnsLookup.USE_ALL_DNS_IPS,
      time,
      discoverBrokerVersions,
      new ApiVersions,
      logContext
    )
  }

}
