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

import java.util.{Optional, Properties}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import kafka.api.LeaderAndIsr
import kafka.log._
import kafka.server._
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.metadata.MockConfigRepository
import kafka.utils._
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.log.internals.{AppendOrigin, LogDirFailureChannel}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

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

  var logManager: LogManager = _
  var partition: Partition = _

  private val topicPartition = new TopicPartition("test-topic", 0)

  @BeforeEach
  def setUp(): Unit = {
    val logConfig = new LogConfig(new Properties)
    val configRepository = MockConfigRepository.forTopic(topicPartition.topic, createLogProperties(Map.empty))
    logManager = TestUtils.createLogManager(Seq(logDir), logConfig, configRepository,
      CleanerConfig(enableCleaner = false), mockTime)
    partition = setupPartitionWithMocks(logManager)
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
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setIsNew(true)
    val offsetCheckpoints: OffsetCheckpoints = mock(classOf[OffsetCheckpoints])
    // Update replica set synchronously first to avoid race conditions
    partition.makeLeader(partitionState(secondReplicaSet), offsetCheckpoints, None)
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

        partition.makeLeader(partitionState(replicas), offsetCheckpoints, None)

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
    val leaderEpoch = partition.getLeaderEpoch

    val appendFutures = scheduleAppends()
    val stateUpdateFutures = scheduleFollowerFetches(leaderEpoch, numRecords = numProducers * numRecordsPerProducer - 1)

    appendSemaphore.release(numProducers * numRecordsPerProducer - 1)
    stateUpdateFutures.foreach(_.get(15, TimeUnit.SECONDS))

    appendSemaphore.release(1)
    scheduleFollowerFetches(leaderEpoch, numRecords = 1).foreach(_.get(15, TimeUnit.SECONDS)) // just to make sure follower state update still works
    appendFutures.foreach(_.get(15, TimeUnit.SECONDS))
  }

  /**
   * Perform concurrent appends and replica fetch requests that may require write lock to update
   * follower state. Threads waiting for write lock to update follower state while append thread is
   * holding read lock will prevent other threads acquiring the read or write lock. So release sufficient
   * permits for all appends to complete before verifying state updates.
   */
  private def concurrentProduceFetchWithWriteLock(): Unit = {
    val leaderEpoch = partition.getLeaderEpoch

    val appendFutures = scheduleAppends()
    val stateUpdateFutures = scheduleFollowerFetches(leaderEpoch, numRecords = numProducers * numRecordsPerProducer)

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
          append(partition, numRecordsPerProducer)
        } catch {
          case e: Throwable =>
            error("Exception during append", e)
            throw e
        }
      }): Runnable)
    }
  }

  private def scheduleFollowerFetches(leaderEpoch: Int, numRecords: Int): Seq[Future[_]] = {
    (1 to numReplicaFetchers).map { index =>
      executorService.submit((() => {
        try {
          fetchFollower(partition, index, leaderEpoch, numRecords)
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

  private def setupPartitionWithMocks(logManager: LogManager): Partition = {
    val leaderEpoch = 1
    val brokerId = 0
    val isrChangeListener: AlterPartitionListener = mock(classOf[AlterPartitionListener])
    val delayedOperations: DelayedOperations = mock(classOf[DelayedOperations])
    val metadataCache: MetadataCache = mock(classOf[MetadataCache])
    val offsetCheckpoints: OffsetCheckpoints = mock(classOf[OffsetCheckpoints])
    val alterIsrManager: AlterPartitionManager = mock(classOf[AlterPartitionManager])

    logManager.startup(Set.empty)
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = kafka.server.Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = MetadataVersion.latest,
      localBrokerId = brokerId,
      mockTime,
      isrChangeListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterIsrManager) {

      override def prepareIsrShrink(
        currentState: CommittedPartitionState,
        outOfSyncReplicaIds: Set[Int]
      ): PendingShrinkIsr = {
        shrinkIsrSemaphore.acquire()
        try {
          super.prepareIsrShrink(currentState, outOfSyncReplicaIds)
        } finally {
          shrinkIsrSemaphore.release()
        }
      }

      override def createLog(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints, topicId: Option[Uuid]): UnifiedLog = {
        val log = super.createLog(isNew, isFutureReplica, offsetCheckpoints, None)
        val logDirFailureChannel = new LogDirFailureChannel(1)
        val segments = new LogSegments(log.topicPartition)
        val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(log.dir, log.topicPartition, logDirFailureChannel, log.config.recordVersion, "")
        val maxTransactionTimeout = 5 * 60 * 1000
        val producerStateManagerConfig = new ProducerStateManagerConfig(kafka.server.Defaults.ProducerIdExpirationMs)
        val producerStateManager = new ProducerStateManager(
          log.topicPartition,
          log.dir,
          maxTransactionTimeout,
          producerStateManagerConfig,
          mockTime
        )
        val offsets = new LogLoader(
          log.dir,
          log.topicPartition,
          log.config,
          mockTime.scheduler,
          mockTime,
          logDirFailureChannel,
          hadCleanShutdown = true,
          segments,
          0L,
          0L,
          leaderEpochCache,
          producerStateManager
        ).load()
        val localLog = new LocalLog(log.dir, log.config, segments, offsets.recoveryPoint,
          offsets.nextOffsetMetadata, mockTime.scheduler, mockTime, log.topicPartition,
          logDirFailureChannel)
        new SlowLog(log, offsets.logStartOffset, localLog, leaderEpochCache, producerStateManager, appendSemaphore)
      }
    }

    val topicIdPartition = new TopicIdPartition(partition.topicId.getOrElse(Uuid.ZERO_UUID), topicPartition)
    when(offsetCheckpoints.fetch(
      ArgumentMatchers.anyString,
      ArgumentMatchers.eq(topicPartition)
    )).thenReturn(None)
    when(alterIsrManager.submit(
      ArgumentMatchers.eq(topicIdPartition),
      ArgumentMatchers.any[LeaderAndIsr],
      ArgumentMatchers.anyInt()
    )).thenReturn(new CompletableFuture[LeaderAndIsr]())

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val controllerEpoch = 0
    val replicas = (0 to numReplicaFetchers).map(i => Integer.valueOf(brokerId + i)).toList.asJava
    val isr = replicas

    assertTrue(partition.makeLeader(new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setPartitionEpoch(1)
      .setReplicas(replicas)
      .setIsNew(true), offsetCheckpoints, None), "Expected become leader transition to succeed")

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

  private def append(
    partition: Partition,
    numRecords: Int
  ): Unit = {
    val requestLocal = RequestLocal.withThreadConfinedCaching
    (0 until numRecords).foreach { _ =>
      val batch = TestUtils.records(records = List(new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes)))
      partition.appendRecordsToLeader(batch, origin = AppendOrigin.CLIENT, requiredAcks = 0, requestLocal)
    }
  }

  private def fetchFollower(
    partition: Partition,
    followerId: Int,
    leaderEpoch: Int,
    numRecords: Int
  ): Unit = {
    val logStartOffset = 0L
    var fetchOffset = 0L
    var lastFetchedEpoch = Optional.empty[Integer]
    val maxBytes = 1

    while (fetchOffset < numRecords) {
      val fetchParams = FetchParams(
        requestVersion = ApiKeys.FETCH.latestVersion,
        replicaId = followerId,
        maxWaitMs = 0,
        minBytes = 1,
        maxBytes = maxBytes,
        isolation = FetchLogEnd,
        clientMetadata = None
      )

      val fetchPartitionData = new FetchRequest.PartitionData(
        Uuid.ZERO_UUID,
        fetchOffset,
        logStartOffset,
        maxBytes,
        Optional.of(Int.box(leaderEpoch)),
        lastFetchedEpoch
      )

      val logReadInfo = partition.fetchRecords(
        fetchParams,
        fetchPartitionData,
        mockTime.milliseconds(),
        maxBytes,
        minOneMessage = true,
        updateFetchState = true
      )

      assertTrue(logReadInfo.divergingEpoch.isEmpty)

      val batches = logReadInfo.fetchedData.records.batches.asScala
      if (batches.nonEmpty) {
        assertEquals(1, batches.size)

        val batch = batches.head
        lastFetchedEpoch = Optional.of(Int.box(batch.partitionLeaderEpoch))
        fetchOffset = batch.lastOffset + 1
      }
    }
  }

  private class SlowLog(
    log: UnifiedLog,
    logStartOffset: Long,
    localLog: LocalLog,
    leaderEpochCache: Option[LeaderEpochFileCache],
    producerStateManager: ProducerStateManager,
    appendSemaphore: Semaphore
  ) extends UnifiedLog(
    logStartOffset,
    localLog,
    new BrokerTopicStats,
    log.producerIdExpirationCheckIntervalMs,
    leaderEpochCache,
    producerStateManager,
    _topicId = None,
    keepPartitionMetadataFile = true) {

    override def appendAsLeader(records: MemoryRecords, leaderEpoch: Int, origin: AppendOrigin,
                                interBrokerProtocolVersion: MetadataVersion, requestLocal: RequestLocal): LogAppendInfo = {
      val appendInfo = super.appendAsLeader(records, leaderEpoch, origin, interBrokerProtocolVersion, requestLocal)
      appendSemaphore.acquire()
      appendInfo
    }
 }
}
