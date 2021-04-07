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

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{CompletableFuture, CountDownLatch, LinkedBlockingDeque, TimeUnit}
import joptsimple.OptionException
import kafka.network.SocketServer
import kafka.raft.{KafkaRaftManager, RaftManager}
import kafka.security.CredentialProvider
import kafka.server.{KafkaConfig, KafkaRequestHandlerPool, MetaProperties, SimpleApiVersionManager}
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, CoreUtils, Exit, Logging, ShutdownableThread}
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing
import org.apache.kafka.common.metrics.stats.{Meter, Percentile, Percentiles}
import org.apache.kafka.common.protocol.{ObjectSerializationCache, Writable}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{TopicPartition, Uuid, protocol}
import org.apache.kafka.raft.BatchReader.Batch
import org.apache.kafka.raft.{BatchReader, RaftClient, RaftConfig, RecordSerde}

import scala.jdk.CollectionConverters._

/**
 * This is an experimental server which is intended for testing the performance
 * of the Raft implementation. It uses a hard-coded `__raft_performance_test` topic.
 */
class TestRaftServer(
  val config: KafkaConfig,
  val throughput: Int,
  val recordSize: Int
) extends Logging {
  import kafka.tools.TestRaftServer._

  private val partition = new TopicPartition("__raft_performance_test", 0)
  // The topic ID must be constant. This value was chosen as to not conflict with the topic ID used for @metadata.
  private val topicId = new Uuid(0L, 2L)
  private val time = Time.SYSTEM
  private val metrics = new Metrics(time)
  private val shutdownLatch = new CountDownLatch(1)
  private val threadNamePrefix = "test-raft"

  var socketServer: SocketServer = _
  var credentialProvider: CredentialProvider = _
  var tokenCache: DelegationTokenCache = _
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = _
  var workloadGenerator: RaftWorkloadGenerator = _
  var raftManager: KafkaRaftManager[Array[Byte]] = _

  def startup(): Unit = {
    tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
    credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

    val apiVersionManager = new SimpleApiVersionManager(ListenerType.CONTROLLER)
    socketServer = new SocketServer(config, metrics, time, credentialProvider, apiVersionManager)
    socketServer.startup(startProcessingRequests = false)

    val metaProperties = MetaProperties(
      clusterId = Uuid.ZERO_UUID.toString,
      nodeId = config.nodeId
    )

    raftManager = new KafkaRaftManager[Array[Byte]](
      metaProperties,
      config,
      new ByteArraySerde,
      partition,
      topicId,
      time,
      metrics,
      Some(threadNamePrefix),
      CompletableFuture.completedFuture(RaftConfig.parseVoterConnections(config.quorumVoters))
    )

    workloadGenerator = new RaftWorkloadGenerator(
      raftManager,
      time,
      recordsPerSec = 20000,
      recordSize = 256
    )

    val requestHandler = new TestRaftRequestHandler(
      raftManager,
      socketServer.dataPlaneRequestChannel,
      time,
      apiVersionManager
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

    workloadGenerator.start()
    raftManager.startup()
    socketServer.startProcessingRequests(Map.empty)
  }

  def shutdown(): Unit = {
    if (raftManager != null)
      CoreUtils.swallow(raftManager.shutdown(), this)
    if (workloadGenerator != null)
      CoreUtils.swallow(workloadGenerator.shutdown(), this)
    if (dataPlaneRequestHandlerPool != null)
      CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
    if (socketServer != null)
      CoreUtils.swallow(socketServer.shutdown(), this)
    if (metrics != null)
      CoreUtils.swallow(metrics.close(), this)
    shutdownLatch.countDown()
  }

  def awaitShutdown(): Unit = {
    shutdownLatch.await()
  }

  class RaftWorkloadGenerator(
    raftManager: RaftManager[Array[Byte]],
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

    raftManager.register(this)

    override def handleClaim(epoch: Int): Unit = {
      eventQueue.offer(HandleClaim(epoch))
    }

    override def handleResign(epoch: Int): Unit = {
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

      raftManager.scheduleAppend(leaderEpoch, Seq(payload)) match {
        case Some(offset) => pendingAppends.offer(PendingAppend(offset, currentTimeMs))
        case None => time.sleep(10)
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
    override def recordSize(data: Array[Byte], serializationCache: ObjectSerializationCache): Int = {
      data.length
    }

    override def write(data: Array[Byte], serializationCache: ObjectSerializationCache, out: Writable): Unit = {
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
      if (configFile == null) {
        throw new InvalidConfigurationException("Missing configuration file. Should specify with '--config'")
      }
      val serverProps = Utils.loadProps(configFile)

      // KafkaConfig requires either `process.roles` or `zookeeper.connect`. Neither are
      // actually used by the test server, so we fill in `process.roles` with an arbitrary value.
      serverProps.put(KafkaConfig.ProcessRolesProp, "controller")

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
        fatal("Exiting raft server due to fatal exception", e)
        Exit.exit(1)
    }
  }

}
