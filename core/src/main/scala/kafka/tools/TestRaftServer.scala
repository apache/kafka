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

package kafka.tools

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.{Properties, Random}

import joptsimple.OptionParser
import kafka.log.{Log, LogConfig, LogManager}
import kafka.network.{ConnectionQuotas, Processor, RequestChannel, SocketServer}
import kafka.raft.{KafkaFuturePurgatory, KafkaMetadataLog, KafkaNetworkChannel}
import kafka.security.CredentialProvider
import kafka.server.{BrokerTopicStats, KafkaConfig, KafkaRequestHandlerPool, KafkaServer, LogDirFailureChannel}
import kafka.utils.timer.SystemTimer
import kafka.utils.{CommandLineUtils, CoreUtils, Exit, KafkaScheduler, Logging, ShutdownableThread}
import org.apache.kafka.clients.{ApiVersions, ClientDnsLookup, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, ListenerName, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.raft.internals.LogOffset
import org.apache.kafka.raft.{FileBasedStateStore, KafkaRaftClient, QuorumState, RaftConfig}

import scala.jdk.CollectionConverters._

/**
 * This is an experimental Raft server which is intended for testing purposes only.
 * It can really only be used for performance testing using the producer performance
 * tool with a hard-coded `__cluster_metadata` topic.
 */
class TestRaftServer(val config: KafkaConfig) extends Logging {

  private val partition = new TopicPartition("__cluster_metadata", 0)
  private val time = Time.SYSTEM
  private val shutdownLatch = new CountDownLatch(1)

  var socketServer: SocketServer = _
  var credentialProvider: CredentialProvider = _
  var tokenCache: DelegationTokenCache = _
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = _
  var scheduler: KafkaScheduler = _
  var metrics: Metrics = _
  var raftIoThread: RaftIoThread = _
  var networkChannel: KafkaNetworkChannel = _
  var metadataLog: KafkaMetadataLog = _

  def startup(): Unit = {
    val logContext = new LogContext(s"[Raft id=${config.brokerId}] ")

    metrics = new Metrics()
    scheduler = new KafkaScheduler(threads = 1)

    scheduler.startup()

    tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
    credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

    socketServer = new RaftSocketServer(config, metrics, time, credentialProvider, logContext)
    socketServer.startup(startProcessingRequests = false)

    val logDirName = Log.logDirName(partition)
    val logDir = createLogDirectory(new File(config.logDirs.head), logDirName)

    val raftConfig = new RaftConfig(config.originals)
    val metadataLog = buildMetadataLog(logDir)
    val networkChannel = buildNetworkChannel(raftConfig, logContext)

    val raftClient = buildRaftClient(
      raftConfig,
      metadataLog,
      networkChannel,
      logContext,
      logDir
    )

    raftClient.initialize()

    val requestHandler = new TestRaftRequestHandler(
      networkChannel,
      socketServer.dataPlaneRequestChannel,
      time,
      raftClient,
      partition
    )

    dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(
      config.brokerId,
      socketServer.dataPlaneRequestChannel,
      requestHandler,
      time,
      config.numIoThreads,
      s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent",
      SocketServer.DataPlaneThreadPrefix
    )

    socketServer.startProcessingRequests(Map.empty)
    raftIoThread = new RaftIoThread(raftClient)
    raftIoThread.start()

  }

  def shutdown(): Unit = {
    if (raftIoThread != null)
      CoreUtils.swallow(raftIoThread.shutdown(), this)
    if (dataPlaneRequestHandlerPool != null)
      CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
    if (socketServer != null)
      CoreUtils.swallow(socketServer.shutdown(), this)
    if (networkChannel != null)
      CoreUtils.swallow(networkChannel.close(), this)
    if (scheduler != null)
      CoreUtils.swallow(scheduler.shutdown(), this)
    if (metrics != null)
      CoreUtils.swallow(metrics.close(), this)
    if (metadataLog != null)
      CoreUtils.swallow(metadataLog.close(), this)

    shutdownLatch.countDown()
  }

  def awaitShutdown(): Unit = {
    shutdownLatch.await()
  }

  private def buildNetworkChannel(raftConfig: RaftConfig,
                                  logContext: LogContext): KafkaNetworkChannel = {
    val netClient = buildNetworkClient(raftConfig, logContext)
    val clientId = s"Raft-${config.brokerId}"
    new KafkaNetworkChannel(time, netClient, clientId,
      raftConfig.retryBackoffMs, raftConfig.requestTimeoutMs)
  }

  private def buildMetadataLog(logDir: File): KafkaMetadataLog = {
    if (config.logDirs.size != 1) {
      throw new ConfigException("There must be exactly one configured log dir")
    }

    val defaultProps = KafkaServer.copyKafkaConfigToLog(config)
    LogConfig.validateValues(defaultProps)
    val defaultLogConfig = LogConfig(defaultProps)

    val log = Log(
      dir = logDir,
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
    new KafkaMetadataLog(log, partition)
  }

  private def createLogDirectory(logDir: File, logDirName: String): File = {
    val logDirPath = logDir.getAbsolutePath
    val dir = new File(logDirPath, logDirName)
    Files.createDirectories(dir.toPath)
    dir
  }

  private def buildRaftClient(raftConfig: RaftConfig,
                              metadataLog: KafkaMetadataLog,
                              networkChannel: KafkaNetworkChannel,
                              logContext: LogContext,
                              logDir: File): KafkaRaftClient = {
    val quorumState = new QuorumState(
      config.brokerId,
      raftConfig.quorumVoterIds,
      raftConfig.electionTimeoutMs,
      raftConfig.fetchTimeoutMs,
      new FileBasedStateStore(new File(logDir, "quorum-state")),
      time,
      logContext,
      new Random()
    )

    val fetchPurgatory = new KafkaFuturePurgatory[LogOffset](
      config.brokerId,
      new SystemTimer("raft-fetch-purgatory-reaper"))

    val appendPurgatory = new KafkaFuturePurgatory[LogOffset](
      config.brokerId,
      new SystemTimer("raft-append-purgatory-reaper"))

    new KafkaRaftClient(
      raftConfig,
      networkChannel,
      metadataLog,
      quorumState,
      time,
      fetchPurgatory,
      appendPurgatory,
      logContext
    )
  }

  private def buildNetworkClient(raftConfig: RaftConfig,
                                 logContext: LogContext): NetworkClient = {
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

    val clientId = s"broker-${config.brokerId}-raft-client"
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
      raftConfig.requestTimeoutMs,
      config.connectionSetupTimeoutMs,
      config.connectionSetupTimeoutMaxMs,
      ClientDnsLookup.USE_ALL_DNS_IPS,
      time,
      discoverBrokerVersions,
      new ApiVersions,
      logContext
    )
  }

  class RaftIoThread(client: KafkaRaftClient) extends ShutdownableThread("raft-io-thread") {
    override def doWork(): Unit = {
      client.poll()
    }

    override def initiateShutdown(): Boolean = {
      if (super.initiateShutdown()) {
        client.shutdown(5000).whenComplete { (_, exception) =>
          if (exception != null) {
            logger.error("Shutdown of RaftClient failed", exception)
          } else {
            logger.info("Completed shutdown of RaftClient")
          }
        }
        true
      } else {
        false
      }
    }

    override def isRunning: Boolean = {
      client.isRunning
    }
  }

}

class RaftSocketServer(
  config: KafkaConfig,
  metrics: Metrics,
  time: Time,
  credentialProvider: CredentialProvider,
  logContext: LogContext
) extends SocketServer(config, metrics, time, credentialProvider) {
  override def newProcessor(
    id: Int,
    requestChannel: RequestChannel,
    connectionQuotas: ConnectionQuotas,
    listenerName: ListenerName,
    securityProtocol: SecurityProtocol,
    memoryPool: MemoryPool,
    isPrivilegedListener: Boolean
  ): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      config.failedAuthenticationDelayMs,
      listenerName,
      securityProtocol,
      config,
      metrics,
      credentialProvider,
      memoryPool,
      logContext,
      isPrivilegedListener = isPrivilegedListener
    ) {
      // We extend this API to skip the check for only enabled APIs. This
      // gets us access to Vote, BeginQuorumEpoch, etc. which are not usable
      // from the Kafka broker yet.
      override def parseRequestHeader(buffer: ByteBuffer): RequestHeader = {
        RequestHeader.parse(buffer)
      }
    }
  }

}


object TestRaftServer extends Logging {
  import kafka.utils.Implicits._

  def getPropsFromArgs(args: Array[String]): Properties = {
    val optionParser = new OptionParser(false)
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])
    // This is just to make the parameter show up in the help output, we are not actually using this due the
    // fact that this class ignores the first parameter which is interpreted as positional and mandatory
    // but would not be mandatory if --version is specified
    // This is a bit of an ugly crutch till we get a chance to rework the entire command line parsing
    optionParser.accepts("version", "Print version information and exit.")

    if (args.length == 0 || args.contains("--help")) {
      CommandLineUtils.printUsageAndDie(optionParser, "USAGE: java [options] %s server.properties [--override property=value]*".format(classOf[TestRaftServer].getSimpleName()))
    }

    if (args.contains("--version")) {
      CommandLineUtils.printVersionAndDie()
    }

    val props = Utils.loadProps(args(0))

    if (args.length > 1) {
      val options = optionParser.parse(args.slice(1, args.length): _*)

      if (options.nonOptionArguments().size() > 0) {
        CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }

      props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala)
    }
    props
  }

  def main(args: Array[String]): Unit = {
    try {
      val serverProps = getPropsFromArgs(args)
      val config = KafkaConfig.fromProps(serverProps, false)
      val server = new TestRaftServer(config)

      Exit.addShutdownHook("raft-shutdown-hook", server.shutdown())

      server.startup()
      server.awaitShutdown()
    }
    catch {
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception", e)
        Exit.exit(1)
    }
    Exit.exit(0)
  }
}
