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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent._

import kafka.api.ApiVersion
import kafka.log._
import kafka.server._
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.utils._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

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

  val mockTime = new MockTime() // Used for check to shrink ISR
  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val executorService = Executors.newFixedThreadPool(numReplicaFetchers + numProducers + 1)
  val appendSemaphore = new Semaphore(0)
  val shrinkIsrSemaphore = new Semaphore(0)
  val followerQueues = (0 until numReplicaFetchers).map(_ => new ArrayBlockingQueue[MemoryRecords](2))

  var logManager: LogManager = _
  var partition: Partition = _

  @BeforeEach
  def setUp(): Unit = {
    val logConfig = new LogConfig(new Properties)
    logManager = TestUtils.createLogManager(Seq(logDir), logConfig, CleanerConfig(enableCleaner = false), mockTime)
    partition = setupPartitionWithMocks(logManager, logConfig)
  }

  @AfterEach
  def tearDown(): Unit = {
    executorService.shutdownNow()
    logManager.liveLogDirs.foreach(Utils.delete)
    Utils.delete(tmpDir)
  }

  /**
   * Verifies that delays in appending to leader while processing produce requests has no impact on timing
   * of update of log read result when processing replica fetch request if no ISR update is required.
   */
  @Test
  def testNoLockContentionWithoutIsrUpdate(): Unit = {
    concurrentProduceFetchWithReadLockOnly()
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
    concurrentProduceFetchWithReadLockOnly()
    active.set(false)
    future.get(15, TimeUnit.SECONDS)
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
    TestUtils.waitUntilTrue(() => shrinkIsrSemaphore.hasQueuedThreads, "shrinkIsr not invoked")
    concurrentProduceFetchWithWriteLock()
    active.set(false)
    future.get(15, TimeUnit.SECONDS)
  }

  /**
   * Concurrently calling updateAssignmentAndIsr should always ensure that non-lock access
   * to the inner remoteReplicaMap (accessed by getReplica) cannot see an intermediate state
   * where replicas present both in the old and new assignment are missing
   */
  @Test
  def testGetReplicaWithUpdateAssignmentAndIsr(): Unit = {
    val active = new AtomicBoolean(true)
    val replicaToCheck = 3
    val firstReplicaSet = Seq[Integer](3, 4, 5).asJava
    val secondReplicaSet = Seq[Integer](1, 2, 3).asJava
    def partitionState(replicas: java.util.List[Integer]) = new LeaderAndIsrPartitionState()
      .setControllerEpoch(1)
      .setLeader(replicas.get(0))
      .setLeaderEpoch(1)
      .setIsr(replicas)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(true)
    val offsetCheckpoints: OffsetCheckpoints = mock(classOf[OffsetCheckpoints])
    // Update replica set synchronously first to avoid race conditions
    partition.makeLeader(partitionState(secondReplicaSet), offsetCheckpoints)
    assertTrue(partition.getReplica(replicaToCheck).isDefined, s"Expected replica $replicaToCheck to be defined")

    val future = executorService.submit((() => {
      var i = 0
      // Flip assignment between two replica sets
      while (active.get) {
        val replicas = if (i % 2 == 0) {
          firstReplicaSet
        } else {
          secondReplicaSet
        }

        partition.makeLeader(partitionState(replicas), offsetCheckpoints)

        i += 1
        Thread.sleep(1) // just to avoid tight loop
      }
    }): Runnable)

    val deadline = 1.seconds.fromNow
    while (deadline.hasTimeLeft()) {
      assertTrue(partition.getReplica(replicaToCheck).isDefined, s"Expected replica $replicaToCheck to be defined")
    }
    active.set(false)
    future.get(5, TimeUnit.SECONDS)
    assertTrue(partition.getReplica(replicaToCheck).isDefined, s"Expected replica $replicaToCheck to be defined")
  }

  /**
   * Perform concurrent appends and replica fetch requests that don't require write lock to
   * update follower state. Release sufficient append permits to complete all except one append.
   * Verify that follower state updates complete even though an append holding read lock is in progress.
   * Then release the permit for the final append and verify that all appends and follower updates complete.
   */
  private def concurrentProduceFetchWithReadLockOnly(): Unit = {
    val appendFutures = scheduleAppends()
    val stateUpdateFutures = scheduleUpdateFollowers(numProducers * numRecordsPerProducer - 1)

    appendSemaphore.release(numProducers * numRecordsPerProducer - 1)
    stateUpdateFutures.foreach(_.get(15, TimeUnit.SECONDS))

    appendSemaphore.release(1)
    scheduleUpdateFollowers(1).foreach(_.get(15, TimeUnit.SECONDS)) // just to make sure follower state update still works
    appendFutures.foreach(_.get(15, TimeUnit.SECONDS))
  }

  /**
   * Perform concurrent appends and replica fetch requests that may require write lock to update
   * follower state. Threads waiting for write lock to update follower state while append thread is
   * holding read lock will prevent other threads acquiring the read or write lock. So release sufficient
   * permits for all appends to complete before verifying state updates.
   */
  private def concurrentProduceFetchWithWriteLock(): Unit = {

    val appendFutures = scheduleAppends()
    val stateUpdateFutures = scheduleUpdateFollowers(numProducers * numRecordsPerProducer)

    assertFalse(stateUpdateFutures.exists(_.isDone))
    appendSemaphore.release(numProducers * numRecordsPerProducer)
    assertFalse(appendFutures.exists(_.isDone))

    shrinkIsrSemaphore.release()
    stateUpdateFutures.foreach(_.get(15, TimeUnit.SECONDS))
    appendFutures.foreach(_.get(15, TimeUnit.SECONDS))
  }

  private def scheduleAppends(): Seq[Future[_]] = {
    (0 until numProducers).map { _ =>
      executorService.submit((() => {
        try {
          append(partition, numRecordsPerProducer, followerQueues)
        } catch {
          case e: Throwable =>
            error("Exception during append", e)
            throw e
        }
      }): Runnable)
    }
  }

  private def scheduleUpdateFollowers(numRecords: Int): Seq[Future[_]] = {
    (1 to numReplicaFetchers).map { index =>
      executorService.submit((() => {
        try {
          updateFollowerFetchState(partition, index, numRecords, followerQueues(index - 1))
        } catch {
          case e: Throwable =>
            error("Exception during updateFollowerFetchState", e)
            throw e
        }
      }): Runnable)
    }
  }

  private def scheduleShrinkIsr(activeFlag: AtomicBoolean, mockTimeSleepMs: Long): Future[_] = {
    executorService.submit((() => {
      while (activeFlag.get) {
        if (mockTimeSleepMs > 0)
          mockTime.sleep(mockTimeSleepMs)
        partition.maybeShrinkIsr()
        Thread.sleep(1) // just to avoid tight loop
      }
    }): Runnable)
  }

  private def setupPartitionWithMocks(logManager: LogManager, logConfig: LogConfig): Partition = {
    val leaderEpoch = 1
    val brokerId = 0
    val topicPartition = new TopicPartition("test-topic", 0)
    val topicConfigProvider = TestUtils.createTopicConfigProvider(createLogProperties(Map.empty))
    val isrChangeListener: IsrChangeListener = mock(classOf[IsrChangeListener])
    val delayedOperations: DelayedOperations = mock(classOf[DelayedOperations])
    val metadataCache: MetadataCache = mock(classOf[MetadataCache])
    val offsetCheckpoints: OffsetCheckpoints = mock(classOf[OffsetCheckpoints])
    val alterIsrManager: AlterIsrManager = mock(classOf[AlterIsrManager])

    logManager.startup()
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = kafka.server.Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      mockTime,
      topicConfigProvider,
      isrChangeListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterIsrManager) {

      override def shrinkIsr(newIsr: Set[Int]): Unit = {
        shrinkIsrSemaphore.acquire()
        try {
          super.shrinkIsr(newIsr)
        } finally {
          shrinkIsrSemaphore.release()
        }
      }

      override def createLog(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints): Log = {
        val log = super.createLog(isNew, isFutureReplica, offsetCheckpoints)
        new SlowLog(log, mockTime, appendSemaphore)
      }
    }
    when(offsetCheckpoints.fetch(ArgumentMatchers.anyString, ArgumentMatchers.eq(topicPartition)))
      .thenReturn(None)
    when(alterIsrManager.submit(ArgumentMatchers.any[AlterIsrItem]))
      .thenReturn(true)

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)

    val controllerEpoch = 0
    val replicas = (0 to numReplicaFetchers).map(i => Integer.valueOf(brokerId + i)).toList.asJava
    val isr = replicas

    assertTrue(partition.makeLeader(new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(true), offsetCheckpoints), "Expected become leader transition to succeed")

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
    (0 until numRecords).foreach { _ =>
      val batch = TestUtils.records(records = List(new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)))
      partition.appendRecordsToLeader(batch, origin = AppendOrigin.Client, requiredAcks = 0)
      followerQueues.foreach(_.put(batch))
    }
  }

  private def updateFollowerFetchState(partition: Partition, followerId: Int, numRecords: Int, followerQueue: ArrayBlockingQueue[MemoryRecords]): Unit = {
    (1 to numRecords).foreach { i =>
      val batch = followerQueue.poll(15, TimeUnit.SECONDS)
      if (batch == null)
        throw new RuntimeException(s"Timed out waiting for next batch $i")
      val batches = batch.batches.iterator.asScala.toList
      assertEquals(1, batches.size)
      val recordBatch = batches.head
      partition.updateFollowerFetchState(
        followerId,
        followerFetchOffsetMetadata = LogOffsetMetadata(recordBatch.lastOffset + 1),
        followerStartOffset = 0L,
        followerFetchTimeMs = mockTime.milliseconds(),
        leaderEndOffset = partition.localLogOrException.logEndOffset)
    }
  }

  private class SlowLog(log: Log, mockTime: MockTime, appendSemaphore: Semaphore) extends Log(
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
      val appendInfo = super.appendAsLeader(records, leaderEpoch, origin, interBrokerProtocolVersion)
      appendSemaphore.acquire()
      appendInfo
    }
  }
}
