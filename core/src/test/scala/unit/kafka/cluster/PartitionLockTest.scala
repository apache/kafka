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

package kafka.cluster

import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.{ArrayBlockingQueue, Executors, Future, TimeUnit}

import kafka.api.{ApiVersion, LeaderAndIsr}
import kafka.log._
import kafka.server._
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.utils._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.{MetricName, TopicPartition}
import org.apache.kafka.common.metrics.stats.{Avg, Max}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{After, Before, Test}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Verifies that slow appends to log don't block request threads processing replica fetch requests.
 *
 * Test simulates:
 *   1) Produce request handling by performing append to log as leader.
 *   2) Replica fetch request handling by processing update of ISR and HW based on log read result.
 *   3) Some tests also simulate a scheduler thread that checks or updates ISRs when replica falls out of ISR
 */
class PartitionLockTest extends Logging {

  val numReplicaFetchers = 2
  val numProducers = 3
  val numRecordsPerProducer = 5
  // Use artificially large delays to enable automated testing of locking delays
  val appendDelayMs = 300
  val isrCheckDelayMs = 10
  val isrUpdateDelayMs = 50

  val mockTime = new MockTime()
  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val executorService = Executors.newFixedThreadPool(numReplicaFetchers + numProducers + 1)

  val writeLockCount = new AtomicInteger()
  val readLockCount = new AtomicInteger()
  val metrics = new Metrics
  val appendSensor = metrics.sensor("append-latency")
  val appendLatencyAvg = metrics.metricName("append-latency-avg", "partition-lock-test")
  val appendLatencyMax = metrics.metricName("append-latency-max", "partition-lock-test")
  val updateFollowerFetchStateSensor = metrics.sensor("replica-updateFollowerFetchState-latency")
  val updateFollowerFetchStateLatencyAvg = metrics.metricName("replica-updateFollowerFetchState-latency-avg", "partition-lock-test")
  val updateFollowerFetchStateLatencyMax = metrics.metricName("replica-updateFollowerFetchState-latency-max", "partition-lock-test")

  var logManager: LogManager = _
  var partition: Partition = _


  @Before
  def setUp(): Unit = {
    appendSensor.add(appendLatencyAvg, new Avg())
    appendSensor.add(appendLatencyMax, new Max())
    updateFollowerFetchStateSensor.add(updateFollowerFetchStateLatencyAvg, new Avg())
    updateFollowerFetchStateSensor.add(updateFollowerFetchStateLatencyMax, new Max())

    val logConfig = new LogConfig(new Properties)
    logManager = TestUtils.createLogManager(Seq(logDir), logConfig, CleanerConfig(enableCleaner = false), mockTime)
    partition = setupPartitionWithMocks(logManager, logConfig)
  }

  @After
  def tearDown(): Unit = {
    executorService.shutdownNow()
    metrics.close()
    logManager.liveLogDirs.foreach(Utils.delete)
    Utils.delete(tmpDir)
  }

  /**
   * Verifies that delays in appending to leader while processing produce requests has no impact on timing
   * of update of log read result when processing replica fetch request if no ISR update is required.
   */
  @Test
  def testNoLockContentionWithoutIsrUpdate(): Unit = {
    val startMs = System.currentTimeMillis

    concurrentProduceFetch(numProducers, numReplicaFetchers, numRecordsPerProducer, appendDelayMs)
    val timeMs = System.currentTimeMillis - startMs

    verifyMetrics(expectLockContention = false)
    assertTrue(s"Test took too long to run:$timeMs", timeMs < numRecordsPerProducer * appendDelayMs + 1000)
    assertEquals(0, writeLockCount.get())
    assertTrue(readLockCount.get() > 0)
  }

  /**
   * Verifies that delays in appending to leader while processing produce requests has no impact on timing
   * of update of log read result when processing replica fetch request even if a scheduler thread is checking
   * for ISR shrink conditions if no ISR update is required.
   */
  @Test
  def testAppendReplicaFetchWithSchedulerCheckForShrinkIsr(): Unit = {
    val active = new AtomicBoolean(true)

    val future = scheduleShrinkIsr(active, mockTimeSleepMs = 0)
    concurrentProduceFetch(numProducers, numReplicaFetchers, numRecordsPerProducer, appendDelayMs)
    active.set(false)
    future.get(30, TimeUnit.SECONDS)

    verifyMetrics(expectLockContention = false)
    assertEquals(0, writeLockCount.get())
    assertTrue(readLockCount.get() > 0)
  }

  /**
   * Verifies concurrent produce and replica fetch log read result update with ISR updates. This
   * can result in delays in processing produce and replica fetch requets since write lock is obtained,
   * but it should complete without any failures.
   */
  @Test
  def testAppendReplicaFetchWithUpdateIsr(): Unit = {
    val active = new AtomicBoolean(true)

    val future = scheduleShrinkIsr(active, mockTimeSleepMs = 10000)
    concurrentProduceFetch(numProducers, numReplicaFetchers, numRecordsPerProducer, appendDelayMs)
    active.set(false)
    future.get(30, TimeUnit.SECONDS)

    verifyMetrics(expectLockContention = true)
    assertTrue(writeLockCount.get() > 0)
    assertTrue(readLockCount.get() > 0)
  }

  private def verifyMetrics(expectLockContention : Boolean): Unit = {
    def metricValue(metricName: MetricName): Double = {
      val nanos = metrics.metric(metricName).metricValue.asInstanceOf[Double]
      nanos / TimeUnit.MILLISECONDS.toNanos(1)
    }

    val appendAvg = metricValue(appendLatencyAvg)
    val appendMax = metricValue(appendLatencyMax)
    val updateFollowerFetchStateAvg = metricValue(updateFollowerFetchStateLatencyAvg)
    val updateFollowerFetchStateMax = metricValue(updateFollowerFetchStateLatencyMax)
    val metricsValues = s"appendAvg=$appendAvg appendMax=$appendMax updateFollowerFetchStateAvg=$updateFollowerFetchStateAvg updateFollowerFetchStateMax=$updateFollowerFetchStateMax"
    info(s"Metrics: $metricsValues")
    val errorMessage = s"Unexpected metrics: $metricsValues"

    val toleranceMs = if (expectLockContention) (appendDelayMs + isrUpdateDelayMs) * 2 else 200
    assertTrue(errorMessage, appendAvg < appendDelayMs + toleranceMs)
    assertTrue(errorMessage, appendMax < appendDelayMs + toleranceMs + 500)
    assertTrue(errorMessage, updateFollowerFetchStateAvg <= isrCheckDelayMs + toleranceMs)
    assertTrue(errorMessage, updateFollowerFetchStateMax <= isrCheckDelayMs + toleranceMs + 500)
  }

  private def concurrentProduceFetch(numProducers: Int, numReplicaFetchers: Int, numRecords: Int, appendDelayMs: Long): Unit = {
    val followerQueues = (0 until numReplicaFetchers).map(_ => new ArrayBlockingQueue[MemoryRecords](2))

    val totalRecords = numRecordsPerProducer * numProducers
    val futures = new ArrayBuffer[Future[_]]()

    (0 until numProducers).foreach(_ => {
      futures += executorService.submit(new Runnable {
        override def run(): Unit = {
          try {
            append(partition, numRecordsPerProducer, followerQueues)
          } catch {
            case e: Throwable =>
              PartitionLockTest.this.error("Exception during append", e)
              throw e
          }
        }
      })
    })
    (1 to numReplicaFetchers).foreach(i => {
      futures += executorService.submit(new Runnable {
        override def run(): Unit = {
          try {
            updateFollowerFetchState(partition, i, totalRecords, followerQueues(i -1))
          } catch {
            case e: Throwable =>
              PartitionLockTest.this.error("Exception during updateFollowerFetchState", e)
              throw e
          }
        }
      })
    })
    futures.foreach(_.get())
  }

  private def scheduleShrinkIsr(activeFlag: AtomicBoolean, mockTimeSleepMs: Long): Future[_] = {
    executorService.submit(new Runnable {
      override def run(): Unit = {
        while (activeFlag.get) {
          if (mockTimeSleepMs > 0)
            mockTime.sleep(mockTimeSleepMs)
          partition.maybeShrinkIsr()
          Thread.sleep(1) // just to avoid tight loop
        }
      }
    })
  }

  private def setupPartitionWithMocks(logManager: LogManager, logConfig: LogConfig): Partition = {
    val leaderEpoch = 1
    val brokerId = 0
    val topicPartition = new TopicPartition("test-topic", 0)
    val stateStore: PartitionStateStore = mock(classOf[PartitionStateStore])
    val delayedOperations: DelayedOperations = mock(classOf[DelayedOperations])
    val metadataCache: MetadataCache = mock(classOf[MetadataCache])
    val offsetCheckpoints: OffsetCheckpoints = mock(classOf[OffsetCheckpoints])

    logManager.startup()
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = kafka.server.Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      mockTime,
      stateStore,
      delayedOperations,
      metadataCache,
      logManager) {

      override def needsExpandIsr(followerReplica: Replica): Boolean = {
        if (isrCheckDelayMs > 0)
          Thread.sleep(isrCheckDelayMs)
        super.needsExpandIsr(followerReplica)
      }

      override def needsShrinkIsr(): Boolean = {
        if (isrCheckDelayMs > 0)
          Thread.sleep(isrCheckDelayMs)
        super.needsShrinkIsr()
      }

      override def getOutOfSyncReplicas(maxLagMs: Long): Set[Int] = {
        if (isrCheckDelayMs > 0)
          Thread.sleep(isrCheckDelayMs)
        super.getOutOfSyncReplicas(maxLagMs)
      }

      override def maybeUpdateIsrAndVersion(isr: Set[Int], zkVersionOpt: Option[Int]): Unit = {
        if (isrUpdateDelayMs > 0)
          Thread.sleep(isrUpdateDelayMs)
        super.maybeUpdateIsrAndVersion(isr, zkVersionOpt)
      }

      override def createLog(replicaId: Int, isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints): Log = {
        val log = super.createLog(replicaId, isNew, isFutureReplica, offsetCheckpoints)
        new SlowLog(log, mockTime, appendDelayMs)
      }

      override def inReadLock[T](lock: ReadWriteLock)(fun: => T): T = {
        readLockCount.incrementAndGet()
        super.inReadLock(lock)(fun)
      }

      override def inWriteLock[T](lock: ReadWriteLock)(fun: => T): T = {
        writeLockCount.incrementAndGet()
        super.inWriteLock(lock)(fun)
      }
    }
    when(stateStore.fetchTopicConfig()).thenReturn(createLogProperties(Map.empty))
    when(offsetCheckpoints.fetch(ArgumentMatchers.anyString, ArgumentMatchers.eq(topicPartition)))
      .thenReturn(None)
    when(stateStore.shrinkIsr(ArgumentMatchers.anyInt, ArgumentMatchers.any[LeaderAndIsr]))
      .thenReturn(Some(2))

    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)

    val controllerId = 0
    val controllerEpoch = 0
    val replicas = (0 to numReplicaFetchers).map(i => Integer.valueOf(brokerId + i)).toList.asJava
    val isr = replicas

    assertTrue("Expected become leader transition to succeed", partition.makeLeader(controllerId, new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(true), 0, offsetCheckpoints))

    readLockCount.set(0)
    writeLockCount.set(0)
    partition
  }

  private def createLogProperties(overrides: Map[String, String]): Properties = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 512: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    overrides.foreach { case (k, v) => logProps.put(k, v) }
    logProps
  }

  private def append(partition: Partition, numRecords: Int, followerQueues: Seq[ArrayBlockingQueue[MemoryRecords]]): Unit = {
    (0 until numRecords).foreach(_ => {
      val batch = TestUtils.records(records = List(new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)))
      val startNs = System.nanoTime
      partition.appendRecordsToLeader(batch, origin = AppendOrigin.Client, requiredAcks = 0)
      appendSensor.record(System.nanoTime - startNs)
      followerQueues.foreach(_.put(batch))
    })
  }

  private def updateFollowerFetchState(partition: Partition, followerId: Int, numRecords: Int, followerQueue: ArrayBlockingQueue[MemoryRecords]): Unit = {
    (1 to numRecords).foreach { i =>
      val batch = followerQueue.poll(5, TimeUnit.SECONDS)
      if (batch == null)
        throw new RuntimeException(s"Timed out waiting for next batch $i")
      Thread.sleep(5) // Leave a delay between leader append and ISR update to allow expand/shrink ISR in tests
      val startNs = System.nanoTime
      partition.updateFollowerFetchState(
        followerId,
        followerFetchOffsetMetadata = LogOffsetMetadata(i),
        followerStartOffset = 0L,
        followerFetchTimeMs = mockTime.milliseconds(),
        leaderEndOffset = partition.localLogOrException.logEndOffset,
        lastSentHighwatermark = partition.localLogOrException.highWatermark)
      updateFollowerFetchStateSensor.record(System.nanoTime - startNs)
    }
  }

  private class SlowLog(log: Log, mockTime: MockTime, appendDelayMs: Long) extends Log(
    log.dir,
    log.config,
    log.logStartOffset,
    log.recoveryPoint,
    mockTime.scheduler,
    new BrokerTopicStats,
    log.time,
    log.maxProducerIdExpirationMs,
    log.producerIdExpirationCheckIntervalMs,
    log.topicPartition,
    log.producerStateManager,
    new LogDirFailureChannel(1)) {

    override def appendAsLeader(records: MemoryRecords, leaderEpoch: Int, origin: AppendOrigin, interBrokerProtocolVersion: ApiVersion): LogAppendInfo = {
      if (appendDelayMs > 0)
        Thread.sleep(appendDelayMs)
      super.appendAsLeader(records, leaderEpoch, origin, interBrokerProtocolVersion)
    }
  }
}