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
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{CountDownLatch, LinkedBlockingDeque, TimeUnit}
import java.util.{Collections, Random}

import joptsimple.OptionException
import kafka.log.{Log, LogConfig, LogManager}
import kafka.network.SocketServer
import kafka.raft.{KafkaMetadataLog, KafkaNetworkChannel, TimingWheelExpirationService}
import kafka.security.CredentialProvider
import kafka.server.{BrokerTopicStats, KafkaConfig, KafkaRequestHandlerPool, KafkaServer, LogDirFailureChannel}
import kafka.utils.timer.SystemTimer
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, CoreUtils, Exit, KafkaScheduler, Logging, ShutdownableThread}
import org.apache.kafka.clients.{ApiVersions, ClientDnsLookup, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing
import org.apache.kafka.common.metrics.stats.{Meter, Percentile, Percentiles}
import org.apache.kafka.common.network.{ChannelBuilders, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.Writable
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.common.{TopicPartition, protocol}
import org.apache.kafka.raft.BatchReader.Batch
import org.apache.kafka.raft.{BatchReader, FileBasedStateStore, KafkaRaftClient, QuorumState, RaftClient, RaftConfig, RecordSerde}

import scala.jdk.CollectionConverters._

/**
 * This is an experimental server which is intended for testing the performance
 * of the Raft implementation. It uses a hard-coded `__cluster_metadata` topic.
 */
class TestRaftServer(
  val config: KafkaConfig,
  val throughput: Int,
  val recordSize: Int
) extends Logging {
  import kafka.tools.TestRaftServer._

  private val partition = new TopicPartition("__cluster_metadata", 0)
  private val time = Time.SYSTEM
  private val shutdownLatch = new CountDownLatch(1)

  var socketServer: SocketServer = _
  var credentialProvider: CredentialProvider = _
  var tokenCache: DelegationTokenCache = _
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = _
  var scheduler: KafkaScheduler = _
  var metrics: Metrics = _
  var ioThread: RaftIoThread = _
  var networkChannel: KafkaNetworkChannel = _
  var metadataLog: KafkaMetadataLog = _
  var workloadGenerator: RaftWorkloadGenerator = _

  def startup(): Unit = {
    val logContext = new LogContext(s"[Raft id=${config.brokerId}] ")

    metrics = new Metrics()
    scheduler = new KafkaScheduler(threads = 1)

    scheduler.startup()

    tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
    credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

    socketServer = new SocketServer(config, metrics, time, credentialProvider, allowDisabledApis = true)
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

    workloadGenerator = new RaftWorkloadGenerator(
      raftClient,
      time,
      recordsPerSec = 20000,
      recordSize = 256
    )

    raftClient.register(workloadGenerator)
    raftClient.initialize()

    val requestHandler = new TestRaftRequestHandler(
      networkChannel,
      socketServer.dataPlaneRequestChannel,
      time
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
    ioThread = new RaftIoThread(raftClient)
    ioThread.start()
    workloadGenerator.start()
  }

  def shutdown(): Unit = {
    if (workloadGenerator != null)
      CoreUtils.swallow(workloadGenerator.shutdown(), this)
    if (ioThread != null)
      CoreUtils.swallow(ioThread.shutdown(), this)
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
                              logDir: File): KafkaRaftClient[Array[Byte]] = {
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

    val expirationTimer = new SystemTimer("raft-expiration-executor")
    val expirationService = new TimingWheelExpirationService(expirationTimer)
    val serde = new ByteArraySerde

    new KafkaRaftClient(
      raftConfig,
      serde,
      networkChannel,
      metadataLog,
      quorumState,
      time,
      expirationService,
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

  class RaftWorkloadGenerator(
    client: KafkaRaftClient[Array[Byte]],
    time: Time,
    recordsPerSec: Int,
    recordSize: Int
  ) extends ShutdownableThread(name = "raft-workload-generator")
    with RaftClient.Listener[Array[Byte]] {

    sealed trait RaftEvent
    case class HandleClaim(epoch: Int) extends RaftEvent
    case object HandleResign extends RaftEvent
    case class HandleCommit(reader: BatchReader[Array[Byte]]) extends RaftEvent
    case object Shutdown extends RaftEvent

    private val eventQueue = new LinkedBlockingDeque[RaftEvent]()
    private val stats = new WriteStats(metrics, time, printIntervalMs = 5000)
    private val payload = new Array[Byte](recordSize)
    private val pendingAppends = new LinkedBlockingDeque[PendingAppend]()
    private val recordCount = new AtomicInteger(0)
    private val throttler = new ThroughputThrottler(time, recordsPerSec)

    private var claimedEpoch: Option[Int] = None

    override def handleClaim(epoch: Int): Unit = {
      eventQueue.offer(HandleClaim(epoch))
    }

    override def handleResign(): Unit = {
      eventQueue.offer(HandleResign)
    }

    override def handleCommit(reader: BatchReader[Array[Byte]]): Unit = {
      eventQueue.offer(HandleCommit(reader))
    }

    override def initiateShutdown(): Boolean = {
      val initiated = super.initiateShutdown()
      eventQueue.offer(Shutdown)
      initiated
    }

    private def sendNow(
      leaderEpoch: Int,
      currentTimeMs: Long
    ): Unit = {
      recordCount.incrementAndGet()

      val offset = client.scheduleAppend(leaderEpoch, Collections.singletonList(payload))
      if (offset == null) {
        time.sleep(10)
      } else {
        pendingAppends.offer(PendingAppend(offset, currentTimeMs))
      }
    }

    override def doWork(): Unit = {
      val startTimeMs = time.milliseconds()
      val eventTimeoutMs = claimedEpoch.map { leaderEpoch =>
        val throttleTimeMs = throttler.maybeThrottle(recordCount.get() + 1, startTimeMs)
        if (throttleTimeMs == 0) {
          sendNow(leaderEpoch, startTimeMs)
        }
        throttleTimeMs
      }.getOrElse(Long.MaxValue)

      eventQueue.poll(eventTimeoutMs, TimeUnit.MILLISECONDS) match {
        case HandleClaim(epoch) =>
          claimedEpoch = Some(epoch)
          throttler.reset()
          pendingAppends.clear()
          recordCount.set(0)

        case HandleResign =>
          claimedEpoch = None
          pendingAppends.clear()

        case HandleCommit(reader) =>
          try {
            while (reader.hasNext) {
              val batch = reader.next()
              claimedEpoch.foreach { leaderEpoch =>
                handleLeaderCommit(leaderEpoch, batch)
              }
            }
          } finally {
            reader.close()
          }

        case _ =>
      }
    }

    private def handleLeaderCommit(
      leaderEpoch: Int,
      batch: Batch[Array[Byte]]
    ): Unit = {
      val batchEpoch = batch.epoch()
      var offset = batch.baseOffset
      val currentTimeMs = time.milliseconds()

      // We are only interested in batches written during the current leader's
      // epoch since this allows us to rely on the local clock
      if (batchEpoch != leaderEpoch) {
        return
      }

      for (record <- batch.records.asScala) {
        val pendingAppend = pendingAppends.peek()

        if (pendingAppend == null || pendingAppend.offset != offset) {
          throw new IllegalStateException(s"Unexpected append at offset $offset. The " +
            s"next offset we expected was ${pendingAppend.offset}")
        }

        pendingAppends.poll()
        val latencyMs = math.max(0, currentTimeMs - pendingAppend.appendTimeMs).toInt
        stats.record(latencyMs, record.length, currentTimeMs)
        offset += 1
      }
    }

  }

  class RaftIoThread(
    client: KafkaRaftClient[Array[Byte]]
  ) extends ShutdownableThread(
    name = "raft-io-thread",
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
      client.isRunning
    }
  }

}

object TestRaftServer extends Logging {

  case class PendingAppend(
    offset: Long,
    appendTimeMs: Long
  ) {
    override def toString: String = {
      s"PendingAppend(offset=$offset, appendTimeMs=$appendTimeMs)"
    }
  }

  private class ByteArraySerde extends RecordSerde[Array[Byte]] {
    override def recordSize(data: Array[Byte], context: Any): Int = {
      data.length
    }

    override def write(data: Array[Byte], context: Any, out: Writable): Unit = {
      out.writeByteArray(data)
    }

    override def read(input: protocol.Readable, size: Int): Array[Byte] = {
      val data = new Array[Byte](size)
      input.readArray(data)
      data
    }
  }

  private class LatencyHistogram(
    metrics: Metrics,
    name: String,
    group: String
  ) {
    private val sensor = metrics.sensor(name)
    private val latencyP75Name = metrics.metricName(s"$name.p75", group)
    private val latencyP99Name = metrics.metricName(s"$name.p99", group)
    private val latencyP999Name = metrics.metricName(s"$name.p999", group)

    sensor.add(new Percentiles(
      1000,
      250.0,
      BucketSizing.CONSTANT,
      new Percentile(latencyP75Name, 75),
      new Percentile(latencyP99Name, 99),
      new Percentile(latencyP999Name, 99.9)
    ))

    private val p75 = metrics.metric(latencyP75Name)
    private val p99 = metrics.metric(latencyP99Name)
    private val p999 = metrics.metric(latencyP999Name)

    def record(latencyMs: Int): Unit = sensor.record(latencyMs)
    def currentP75: Double = p75.metricValue.asInstanceOf[Double]
    def currentP99: Double = p99.metricValue.asInstanceOf[Double]
    def currentP999: Double = p999.metricValue.asInstanceOf[Double]
  }

  private class ThroughputMeter(
    metrics: Metrics,
    name: String,
    group: String
  ) {
    private val sensor = metrics.sensor(name)
    private val throughputRateName = metrics.metricName(s"$name.rate", group)
    private val throughputTotalName = metrics.metricName(s"$name.total", group)

    sensor.add(new Meter(throughputRateName, throughputTotalName))

    private val rate = metrics.metric(throughputRateName)

    def record(bytes: Int): Unit = sensor.record(bytes)
    def currentRate: Double = rate.metricValue.asInstanceOf[Double]
  }

  private class ThroughputThrottler(
    time: Time,
    targetRecordsPerSec: Int
  ) {
    private val startTimeMs = new AtomicLong(time.milliseconds())

    require(targetRecordsPerSec > 0)

    def reset(): Unit = {
      this.startTimeMs.set(time.milliseconds())
    }

    def maybeThrottle(
      currentCount: Int,
      currentTimeMs: Long
    ): Long = {
      val targetDurationMs = math.round(currentCount / targetRecordsPerSec.toDouble * 1000)
      if (targetDurationMs > 0) {
        val targetDeadlineMs = startTimeMs.get() + targetDurationMs
        if (targetDeadlineMs > currentTimeMs) {
          val throttleDurationMs = targetDeadlineMs - currentTimeMs
          return throttleDurationMs
        }
      }
      0
    }
  }

  private class WriteStats(
    metrics: Metrics,
    time: Time,
    printIntervalMs: Long
  ) {
    private var lastReportTimeMs = time.milliseconds()
    private val latency = new LatencyHistogram(metrics, name = "commit.latency", group = "kafka.raft")
    private val throughput = new ThroughputMeter(metrics, name = "bytes.committed", group = "kafka.raft")

    def record(
      latencyMs: Int,
      bytes: Int,
      currentTimeMs: Long
    ): Unit = {
      throughput.record(bytes)
      latency.record(latencyMs)

      if (currentTimeMs - lastReportTimeMs >= printIntervalMs) {
        printSummary()
        this.lastReportTimeMs = currentTimeMs
      }
    }

    private def printSummary(): Unit = {
      println("Throughput (bytes/second): %.2f, Latency (ms): %.1f p75 %.1f p99 %.1f p999".format(
        throughput.currentRate,
        latency.currentP75,
        latency.currentP99,
        latency.currentP999
      ))
    }
  }

  class TestRaftServerOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val configOpt = parser.accepts("config", "Required configured file")
      .withRequiredArg
      .describedAs("filename")
      .ofType(classOf[String])

    val throughputOpt = parser.accepts("throughput",
      "The number of records per second the leader will write to the metadata topic")
      .withRequiredArg
      .describedAs("records/sec")
      .ofType(classOf[Int])
      .defaultsTo(5000)

    val recordSizeOpt = parser.accepts("record-size", "The size of each record")
      .withRequiredArg
      .describedAs("size in bytes")
      .ofType(classOf[Int])
      .defaultsTo(256)

    options = parser.parse(args : _*)
  }

  def main(args: Array[String]): Unit = {
    val opts = new TestRaftServerOptions(args)
    try {
      CommandLineUtils.printHelpAndExitIfNeeded(opts,
        "Standalone raft server for performance testing")

      val configFile = opts.options.valueOf(opts.configOpt)
      val serverProps = Utils.loadProps(configFile)
      val config = KafkaConfig.fromProps(serverProps, doLog = false)
      val throughput = opts.options.valueOf(opts.throughputOpt)
      val recordSize = opts.options.valueOf(opts.recordSizeOpt)
      val server = new TestRaftServer(config, throughput, recordSize)

      Exit.addShutdownHook("raft-shutdown-hook", server.shutdown())

      server.startup()
      server.awaitShutdown()
      Exit.exit(0)
    } catch {
      case e: OptionException =>
        CommandLineUtils.printUsageAndDie(opts.parser, e.getMessage)
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception", e)
        Exit.exit(1)
    }
  }

}
