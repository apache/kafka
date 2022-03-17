/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.cluster

import com.yammer.metrics.core.Metric
import kafka.api.{ApiVersion, KAFKA_2_6_IV0}
import kafka.common.UnexpectedAppendOffsetException
import kafka.log.{Defaults => _, _}
import kafka.metrics.KafkaYammerMetrics
import kafka.server._
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.errors.{ApiException, InconsistentTopicIdException, NotLeaderOrFollowerException, OffsetNotAvailableException, OffsetOutOfRangeException}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.common.{IsolationLevel, TopicPartition, Uuid}
import org.apache.kafka.metadata.LeaderRecoveryState
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock

import java.nio.ByteBuffer
import java.util.Optional
import java.util.concurrent.{CountDownLatch, Semaphore}
import kafka.server.epoch.LeaderEpochFileCache

import scala.jdk.CollectionConverters._

class PartitionTest extends AbstractPartitionTest {

  @Test
  def testLastFetchedOffsetValidation(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    def append(leaderEpoch: Int, count: Int): Unit = {
      val recordArray = (1 to count).map { i =>
        new SimpleRecord(s"$i".getBytes)
      }
      val records = MemoryRecords.withRecords(0L, CompressionType.NONE, leaderEpoch,
        recordArray: _*)
      log.appendAsLeader(records, leaderEpoch = leaderEpoch)
    }

    append(leaderEpoch = 0, count = 2) // 0
    append(leaderEpoch = 3, count = 3) // 2
    append(leaderEpoch = 3, count = 3) // 5
    append(leaderEpoch = 4, count = 5) // 8
    append(leaderEpoch = 7, count = 1) // 13
    append(leaderEpoch = 9, count = 3) // 14
    assertEquals(17L, log.logEndOffset)

    val leaderEpoch = 10
    val partition = setupPartitionWithMocks(leaderEpoch = leaderEpoch, isLeader = true)

    def epochEndOffset(epoch: Int, endOffset: Long): FetchResponseData.EpochEndOffset = {
      new FetchResponseData.EpochEndOffset()
        .setEpoch(epoch)
        .setEndOffset(endOffset)
    }

    def read(lastFetchedEpoch: Int, fetchOffset: Long): LogReadInfo = {
      partition.readRecords(
        Optional.of(lastFetchedEpoch),
        fetchOffset,
        currentLeaderEpoch = Optional.of(leaderEpoch),
        maxBytes = Int.MaxValue,
        fetchIsolation = FetchLogEnd,
        fetchOnlyFromLeader = true,
        minOneMessage = true
      )
    }

    def assertDivergence(
      divergingEpoch: FetchResponseData.EpochEndOffset,
      readInfo: LogReadInfo
    ): Unit = {
      assertEquals(Some(divergingEpoch), readInfo.divergingEpoch)
      assertEquals(0, readInfo.fetchedData.records.sizeInBytes)
    }

    def assertNoDivergence(readInfo: LogReadInfo): Unit = {
      assertEquals(None, readInfo.divergingEpoch)
    }

    assertDivergence(epochEndOffset(epoch = 0, endOffset = 2), read(lastFetchedEpoch = 2, fetchOffset = 5))
    assertDivergence(epochEndOffset(epoch = 0, endOffset= 2), read(lastFetchedEpoch = 0, fetchOffset = 4))
    assertDivergence(epochEndOffset(epoch = 4, endOffset = 13), read(lastFetchedEpoch = 6, fetchOffset = 6))
    assertDivergence(epochEndOffset(epoch = 4, endOffset = 13), read(lastFetchedEpoch = 5, fetchOffset = 9))
    assertDivergence(epochEndOffset(epoch = 10, endOffset = 17), read(lastFetchedEpoch = 10, fetchOffset = 18))
    assertNoDivergence(read(lastFetchedEpoch = 0, fetchOffset = 2))
    assertNoDivergence(read(lastFetchedEpoch = 7, fetchOffset = 14))
    assertNoDivergence(read(lastFetchedEpoch = 9, fetchOffset = 17))
    assertNoDivergence(read(lastFetchedEpoch = 10, fetchOffset = 17))

    // Reads from epochs larger than we know about should cause an out of range error
    assertThrows(classOf[OffsetOutOfRangeException], () => read(lastFetchedEpoch = 11, fetchOffset = 5))

    // Move log start offset to the middle of epoch 3
    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(newLogStartOffset = 5L, ClientRecordDeletion)

    assertDivergence(epochEndOffset(epoch = 2, endOffset = 5), read(lastFetchedEpoch = 2, fetchOffset = 8))
    assertNoDivergence(read(lastFetchedEpoch = 0, fetchOffset = 5))
    assertNoDivergence(read(lastFetchedEpoch = 3, fetchOffset = 5))

    assertThrows(classOf[OffsetOutOfRangeException], () => read(lastFetchedEpoch = 0, fetchOffset = 0))

    // Fetch offset lower than start offset should throw OffsetOutOfRangeException
    log.maybeIncrementLogStartOffset(newLogStartOffset = 10, ClientRecordDeletion)
    assertThrows(classOf[OffsetOutOfRangeException], () => read(lastFetchedEpoch = 5, fetchOffset = 6)) // diverging
    assertThrows(classOf[OffsetOutOfRangeException], () => read(lastFetchedEpoch = 3, fetchOffset = 6)) // not diverging
  }

  @Test
  def testMakeLeaderUpdatesEpochCache(): Unit = {
    val leaderEpoch = 8

    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    log.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes)
    ), leaderEpoch = 0)
    log.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 5,
      new SimpleRecord("k3".getBytes, "v3".getBytes),
      new SimpleRecord("k4".getBytes, "v4".getBytes)
    ), leaderEpoch = 5)
    assertEquals(4, log.logEndOffset)

    val partition = setupPartitionWithMocks(leaderEpoch = leaderEpoch, isLeader = true)
    assertEquals(Some(4), partition.leaderLogIfLocal.map(_.logEndOffset))

    val epochEndOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpoch = Optional.of[Integer](leaderEpoch),
      leaderEpoch = leaderEpoch, fetchOnlyFromLeader = true)
    assertEquals(4, epochEndOffset.endOffset)
    assertEquals(leaderEpoch, epochEndOffset.leaderEpoch)
  }

  // Verify that partition.removeFutureLocalReplica() and partition.maybeReplaceCurrentWithFutureReplica() can run concurrently
  @Test
  def testMaybeReplaceCurrentWithFutureReplica(): Unit = {
    val latch = new CountDownLatch(1)

    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir1.getAbsolutePath)
    partition.createLogIfNotExists(isNew = true, isFutureReplica = false, offsetCheckpoints, None)
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir2.getAbsolutePath)
    partition.maybeCreateFutureReplica(logDir2.getAbsolutePath, offsetCheckpoints)

    val thread1 = new Thread {
      override def run(): Unit = {
        latch.await()
        partition.removeFutureLocalReplica()
      }
    }

    val thread2 = new Thread {
      override def run(): Unit = {
        latch.await()
        partition.maybeReplaceCurrentWithFutureReplica()
      }
    }

    thread1.start()
    thread2.start()

    latch.countDown()
    thread1.join()
    thread2.join()
    assertEquals(None, partition.futureLog)
  }

  // Verify that partition.makeFollower() and partition.appendRecordsToFollowerOrFutureReplica() can run concurrently
  @Test
  def testMakeFollowerWithWithFollowerAppendRecords(): Unit = {
    val appendSemaphore = new Semaphore(0)
    val mockTime = new MockTime()

    partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      isrChangeListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterIsrManager) {

      override def createLog(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints, topicId: Option[Uuid]): UnifiedLog = {
        val log = super.createLog(isNew, isFutureReplica, offsetCheckpoints, None)
        val logDirFailureChannel = new LogDirFailureChannel(1)
        val segments = new LogSegments(log.topicPartition)
        val leaderEpochCache = UnifiedLog.maybeCreateLeaderEpochCache(log.dir, log.topicPartition, logDirFailureChannel, log.config.recordVersion, "")
        val maxTransactionTimeoutMs = 5 * 60 * 1000
        val maxProducerIdExpirationMs = 60 * 60 * 1000
        val producerStateManager = new ProducerStateManager(
          log.topicPartition,
          log.dir,
          maxTransactionTimeoutMs,
          maxProducerIdExpirationMs,
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
          segments = segments,
          logStartOffsetCheckpoint = 0L,
          recoveryPointCheckpoint = 0L,
          leaderEpochCache,
          producerStateManager
        ).load()
        val localLog = new LocalLog(log.dir, log.config, segments, offsets.recoveryPoint,
          offsets.nextOffsetMetadata, mockTime.scheduler, mockTime, log.topicPartition,
          logDirFailureChannel)
        new SlowLog(log, offsets.logStartOffset, localLog, leaderEpochCache, producerStateManager, appendSemaphore)
      }
    }

    partition.createLogIfNotExists(isNew = true, isFutureReplica = false, offsetCheckpoints, None)

    val appendThread = new Thread {
      override def run(): Unit = {
        val records = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
          new SimpleRecord("k2".getBytes, "v2".getBytes)),
          baseOffset = 0)
        partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)
      }
    }
    appendThread.start()
    TestUtils.waitUntilTrue(() => appendSemaphore.hasQueuedThreads, "follower log append is not called.")

    val partitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(0)
      .setLeader(2)
      .setLeaderEpoch(1)
      .setIsr(List[Integer](0, 1, 2, brokerId).asJava)
      .setZkVersion(1)
      .setReplicas(List[Integer](0, 1, 2, brokerId).asJava)
      .setIsNew(false)
    assertTrue(partition.makeFollower(partitionState, offsetCheckpoints, None))

    appendSemaphore.release()
    appendThread.join()

    assertEquals(2L, partition.localLogOrException.logEndOffset)
    assertEquals(2L, partition.leaderReplicaIdOpt.get)
  }

  @Test
  // Verify that replacement works when the replicas have the same log end offset but different base offsets in the
  // active segment
  def testMaybeReplaceCurrentWithFutureReplicaDifferentBaseOffsets(): Unit = {
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir1.getAbsolutePath)
    partition.createLogIfNotExists(isNew = true, isFutureReplica = false, offsetCheckpoints, None)
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir2.getAbsolutePath)
    partition.maybeCreateFutureReplica(logDir2.getAbsolutePath, offsetCheckpoints)

    // Write records with duplicate keys to current replica and roll at offset 6
    val currentLog = partition.log.get
    currentLog.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k1".getBytes, "v2".getBytes),
      new SimpleRecord("k1".getBytes, "v3".getBytes),
      new SimpleRecord("k2".getBytes, "v4".getBytes),
      new SimpleRecord("k2".getBytes, "v5".getBytes),
      new SimpleRecord("k2".getBytes, "v6".getBytes)
    ), leaderEpoch = 0)
    currentLog.roll()
    currentLog.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k3".getBytes, "v7".getBytes),
      new SimpleRecord("k4".getBytes, "v8".getBytes)
    ), leaderEpoch = 0)

    // Write to the future replica as if the log had been compacted, and do not roll the segment

    val buffer = ByteBuffer.allocate(1024)
    val builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
      TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, 0)
    builder.appendWithOffset(2L, new SimpleRecord("k1".getBytes, "v3".getBytes))
    builder.appendWithOffset(5L, new SimpleRecord("k2".getBytes, "v6".getBytes))
    builder.appendWithOffset(6L, new SimpleRecord("k3".getBytes, "v7".getBytes))
    builder.appendWithOffset(7L, new SimpleRecord("k4".getBytes, "v8".getBytes))

    val futureLog = partition.futureLocalLogOrException
    futureLog.appendAsFollower(builder.build())

    assertTrue(partition.maybeReplaceCurrentWithFutureReplica())
  }

  @Test
  def testFetchOffsetSnapshotEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertSnapshotError(expectedError: Errors, currentLeaderEpoch: Optional[Integer]): Unit = {
      try {
        partition.fetchOffsetSnapshot(currentLeaderEpoch, fetchOnlyFromLeader = true)
        assertEquals(Errors.NONE, expectedError)
      } catch {
        case error: ApiException => assertEquals(expectedError, Errors.forException(error))
      }
    }

    assertSnapshotError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertSnapshotError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
    assertSnapshotError(Errors.NONE, Optional.of(leaderEpoch))
    assertSnapshotError(Errors.NONE, Optional.empty())
  }

  @Test
  def testFetchOffsetSnapshotEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertSnapshotError(expectedError: Errors,
                            currentLeaderEpoch: Optional[Integer],
                            fetchOnlyLeader: Boolean): Unit = {
      try {
        partition.fetchOffsetSnapshot(currentLeaderEpoch, fetchOnlyFromLeader = fetchOnlyLeader)
        assertEquals(Errors.NONE, expectedError)
      } catch {
        case error: ApiException => assertEquals(expectedError, Errors.forException(error))
      }
    }

    assertSnapshotError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertSnapshotError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertSnapshotError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertSnapshotError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertSnapshotError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertSnapshotError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.empty(), fetchOnlyLeader = true)
    assertSnapshotError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertSnapshotError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testOffsetForLeaderEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertLastOffsetForLeaderError(error: Errors, currentLeaderEpochOpt: Optional[Integer]): Unit = {
      val endOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpochOpt, 0,
        fetchOnlyFromLeader = true)
      assertEquals(error.code, endOffset.errorCode)
    }

    assertLastOffsetForLeaderError(Errors.NONE, Optional.empty())
    assertLastOffsetForLeaderError(Errors.NONE, Optional.of(leaderEpoch))
    assertLastOffsetForLeaderError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertLastOffsetForLeaderError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
  }

  @Test
  def testOffsetForLeaderEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertLastOffsetForLeaderError(error: Errors,
                                       currentLeaderEpochOpt: Optional[Integer],
                                       fetchOnlyLeader: Boolean): Unit = {
      val endOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpochOpt, 0,
        fetchOnlyFromLeader = fetchOnlyLeader)
      assertEquals(error.code, endOffset.errorCode)
    }

    assertLastOffsetForLeaderError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertLastOffsetForLeaderError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.empty(), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testReadRecordEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertReadRecordsError(error: Errors,
                               currentLeaderEpochOpt: Optional[Integer]): Unit = {
      try {
        partition.readRecords(
          lastFetchedEpoch = Optional.empty(),
          fetchOffset = 0L,
          currentLeaderEpoch = currentLeaderEpochOpt,
          maxBytes = 1024,
          fetchIsolation = FetchLogEnd,
          fetchOnlyFromLeader = true,
          minOneMessage = false)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertReadRecordsError(Errors.NONE, Optional.empty())
    assertReadRecordsError(Errors.NONE, Optional.of(leaderEpoch))
    assertReadRecordsError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertReadRecordsError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
  }

  @Test
  def testReadRecordEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertReadRecordsError(error: Errors,
                               currentLeaderEpochOpt: Optional[Integer],
                               fetchOnlyLeader: Boolean): Unit = {
      try {
        partition.readRecords(
          lastFetchedEpoch = Optional.empty(),
          fetchOffset = 0L,
          currentLeaderEpoch = currentLeaderEpochOpt,
          maxBytes = 1024,
          fetchIsolation = FetchLogEnd,
          fetchOnlyFromLeader = fetchOnlyLeader,
          minOneMessage = false)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertReadRecordsError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertReadRecordsError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertReadRecordsError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertReadRecordsError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertReadRecordsError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.empty(), fetchOnlyLeader = true)
    assertReadRecordsError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertReadRecordsError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertReadRecordsError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testFetchOffsetForTimestampEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertFetchOffsetError(error: Errors,
                               currentLeaderEpochOpt: Optional[Integer]): Unit = {
      try {
        partition.fetchOffsetForTimestamp(0L,
          isolationLevel = None,
          currentLeaderEpoch = currentLeaderEpochOpt,
          fetchOnlyFromLeader = true)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertFetchOffsetError(Errors.NONE, Optional.empty())
    assertFetchOffsetError(Errors.NONE, Optional.of(leaderEpoch))
    assertFetchOffsetError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertFetchOffsetError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
  }

  @Test
  def testFetchOffsetForTimestampEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertFetchOffsetError(error: Errors,
                               currentLeaderEpochOpt: Optional[Integer],
                               fetchOnlyLeader: Boolean): Unit = {
      try {
        partition.fetchOffsetForTimestamp(0L,
          isolationLevel = None,
          currentLeaderEpoch = currentLeaderEpochOpt,
          fetchOnlyFromLeader = fetchOnlyLeader)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertFetchOffsetError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertFetchOffsetError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertFetchOffsetError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertFetchOffsetError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertFetchOffsetError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.empty(), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.NOT_LEADER_OR_FOLLOWER, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testFetchLatestOffsetIncludesLeaderEpoch(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    val timestampAndOffsetOpt = partition.fetchOffsetForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP,
      isolationLevel = None,
      currentLeaderEpoch = Optional.empty(),
      fetchOnlyFromLeader = true)

    assertTrue(timestampAndOffsetOpt.isDefined)

    val timestampAndOffset = timestampAndOffsetOpt.get
    assertEquals(leaderEpoch, timestampAndOffset.leaderEpoch.get)
  }

  /**
    * This test checks that after a new leader election, we don't answer any ListOffsetsRequest until
    * the HW of the new leader has caught up to its startLogOffset for this epoch. From a client
    * perspective this helps guarantee monotonic offsets
    *
    * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-207%3A+Offsets+returned+by+ListOffsetsResponse+should+be+monotonically+increasing+even+during+a+partition+leader+change">KIP-207</a>
    */
  @Test
  def testMonotonicOffsetsAfterLeaderChange(): Unit = {
    val controllerEpoch = 3
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val replicas = List(leader, follower1, follower2)
    val isr = List[Integer](leader, follower2).asJava
    val leaderEpoch = 8
    val batch1 = TestUtils.records(records = List(
      new SimpleRecord(10, "k1".getBytes, "v1".getBytes),
      new SimpleRecord(11,"k2".getBytes, "v2".getBytes)))
    val batch2 = TestUtils.records(records = List(new SimpleRecord("k3".getBytes, "v1".getBytes),
      new SimpleRecord(20,"k4".getBytes, "v2".getBytes),
      new SimpleRecord(21,"k5".getBytes, "v3".getBytes)))

    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas.map(Int.box).asJava)
      .setIsNew(true)

    assertTrue(partition.makeLeader(leaderState, offsetCheckpoints, None), "Expected first makeLeader() to return 'leader changed'")
    assertEquals(leaderEpoch, partition.getLeaderEpoch, "Current leader epoch")
    assertEquals(Set[Integer](leader, follower2), partition.partitionState.isr, "ISR")

    val requestLocal = RequestLocal.withThreadConfinedCaching
    // after makeLeader(() call, partition should know about all the replicas
    // append records with initial leader epoch
    partition.appendRecordsToLeader(batch1, origin = AppendOrigin.Client, requiredAcks = 0, requestLocal)
    partition.appendRecordsToLeader(batch2, origin = AppendOrigin.Client, requiredAcks = 0, requestLocal)
    assertEquals(partition.localLogOrException.logStartOffset, partition.localLogOrException.highWatermark,
      "Expected leader's HW not move")

    // let the follower in ISR move leader's HW to move further but below LEO
    def updateFollowerFetchState(followerId: Int, fetchOffsetMetadata: LogOffsetMetadata): Unit = {
      partition.updateFollowerFetchState(
        followerId,
        followerFetchOffsetMetadata = fetchOffsetMetadata,
        followerStartOffset = 0L,
        followerFetchTimeMs = time.milliseconds(),
        leaderEndOffset = partition.localLogOrException.logEndOffset)
    }

    def fetchOffsetsForTimestamp(timestamp: Long, isolation: Option[IsolationLevel]): Either[ApiException, Option[TimestampAndOffset]] = {
      try {
        Right(partition.fetchOffsetForTimestamp(
          timestamp = timestamp,
          isolationLevel = isolation,
          currentLeaderEpoch = Optional.of(partition.getLeaderEpoch),
          fetchOnlyFromLeader = true
        ))
      } catch {
        case e: ApiException => Left(e)
      }
    }

    updateFollowerFetchState(follower1, LogOffsetMetadata(0))
    updateFollowerFetchState(follower1, LogOffsetMetadata(2))

    updateFollowerFetchState(follower2, LogOffsetMetadata(0))
    updateFollowerFetchState(follower2, LogOffsetMetadata(2))

    // Simulate successful ISR update
    alterIsrManager.completeIsrUpdate(2)

    // At this point, the leader has gotten 5 writes, but followers have only fetched two
    assertEquals(2, partition.localLogOrException.highWatermark)

    // Get the LEO
    fetchOffsetsForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, None) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(5, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e) => fail("Should not have seen an error")
    }

    // Get the HW
    fetchOffsetsForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(2, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e) => fail("Should not have seen an error")
    }

    // Get a offset beyond the HW by timestamp, get a None
    assertEquals(Right(None), fetchOffsetsForTimestamp(30, Some(IsolationLevel.READ_UNCOMMITTED)))

    // Make into a follower
    val followerState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(follower2)
      .setLeaderEpoch(leaderEpoch + 1)
      .setIsr(isr)
      .setZkVersion(4)
      .setReplicas(replicas.map(Int.box).asJava)
      .setIsNew(false)

    assertTrue(partition.makeFollower(followerState, offsetCheckpoints, None))

    // Back to leader, this resets the startLogOffset for this epoch (to 2), we're now in the fault condition
    val newLeaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch + 2)
      .setIsr(isr)
      .setZkVersion(5)
      .setReplicas(replicas.map(Int.box).asJava)
      .setIsNew(false)

    assertTrue(partition.makeLeader(newLeaderState, offsetCheckpoints, None))

    // Try to get offsets as a client
    fetchOffsetsForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => fail("Should have failed with OffsetNotAvailable")
      case Right(None) => fail("Should have seen an error")
      case Left(e: OffsetNotAvailableException) => // ok
      case Left(e: ApiException) => fail(s"Expected OffsetNotAvailableException, got $e")
    }

    // If request is not from a client, we skip the check
    fetchOffsetsForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, None) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(5, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // If we request the earliest timestamp, we skip the check
    fetchOffsetsForTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(0, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // If we request an offset by timestamp earlier than the HW, we are ok
    fetchOffsetsForTimestamp(11, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) =>
        assertEquals(1, offsetAndTimestamp.offset)
        assertEquals(11, offsetAndTimestamp.timestamp)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // Request an offset by timestamp beyond the HW, get an error now since we're in a bad state
    fetchOffsetsForTimestamp(100, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => fail("Should have failed")
      case Right(None) => fail("Should have failed")
      case Left(e: OffsetNotAvailableException) => // ok
      case Left(e: ApiException) => fail(s"Should have seen OffsetNotAvailableException, saw $e")
    }

    // Next fetch from replicas, HW is moved up to 5 (ahead of the LEO)
    updateFollowerFetchState(follower1, LogOffsetMetadata(5))
    updateFollowerFetchState(follower2, LogOffsetMetadata(5))

    // Simulate successful ISR update
    alterIsrManager.completeIsrUpdate(6)

    // Error goes away
    fetchOffsetsForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertEquals(5, offsetAndTimestamp.offset)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // Now we see None instead of an error for out of range timestamp
    assertEquals(Right(None), fetchOffsetsForTimestamp(100, Some(IsolationLevel.READ_UNCOMMITTED)))
  }

  @Test
  def testAppendRecordsAsFollowerBelowLogStartOffset(): Unit = {
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    val log = partition.localLogOrException

    val initialLogStartOffset = 5L
    partition.truncateFullyAndStartAt(initialLogStartOffset, isFuture = false)
    assertEquals(initialLogStartOffset, log.logEndOffset,
      s"Log end offset after truncate fully and start at $initialLogStartOffset:")
    assertEquals(initialLogStartOffset, log.logStartOffset,
      s"Log start offset after truncate fully and start at $initialLogStartOffset:")

    // verify that we cannot append records that do not contain log start offset even if the log is empty
    assertThrows(classOf[UnexpectedAppendOffsetException], () =>
      // append one record with offset = 3
      partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 3L), isFuture = false)
    )
    assertEquals(initialLogStartOffset, log.logEndOffset,
      s"Log end offset should not change after failure to append")

    // verify that we can append records that contain log start offset, even when first
    // offset < log start offset if the log is empty
    val newLogStartOffset = 4L
    val records = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                     new SimpleRecord("k2".getBytes, "v2".getBytes),
                                     new SimpleRecord("k3".getBytes, "v3".getBytes)),
                                baseOffset = newLogStartOffset)
    partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)
    assertEquals(7L, log.logEndOffset, s"Log end offset after append of 3 records with base offset $newLogStartOffset:")
    assertEquals(newLogStartOffset, log.logStartOffset, s"Log start offset after append of 3 records with base offset $newLogStartOffset:")

    // and we can append more records after that
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 7L), isFuture = false)
    assertEquals(8L, log.logEndOffset, s"Log end offset after append of 1 record at offset 7:")
    assertEquals(newLogStartOffset, log.logStartOffset, s"Log start offset not expected to change:")

    // but we cannot append to offset < log start if the log is not empty
    val records2 = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes)),
      baseOffset = 3L)
    assertThrows(classOf[UnexpectedAppendOffsetException], () => partition.appendRecordsToFollowerOrFutureReplica(records2, isFuture = false))
    assertEquals(8L, log.logEndOffset, s"Log end offset should not change after failure to append")

    // we still can append to next offset
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 8L), isFuture = false)
    assertEquals(9L, log.logEndOffset, s"Log end offset after append of 1 record at offset 8:")
    assertEquals(newLogStartOffset, log.logStartOffset, s"Log start offset not expected to change:")
  }

  @Test
  def testListOffsetIsolationLevels(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicas

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    assertTrue(partition.makeLeader(new LeaderAndIsrPartitionState()
        .setControllerEpoch(controllerEpoch)
        .setLeader(brokerId)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setZkVersion(1)
        .setReplicas(replicas)
        .setIsNew(true), offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(leaderEpoch, partition.getLeaderEpoch)

    val records = createTransactionalRecords(List(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes)),
      baseOffset = 0L)
    partition.appendRecordsToLeader(records, origin = AppendOrigin.Client, requiredAcks = 0, RequestLocal.withThreadConfinedCaching)

    def fetchLatestOffset(isolationLevel: Option[IsolationLevel]): TimestampAndOffset = {
      val res = partition.fetchOffsetForTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP,
        isolationLevel = isolationLevel,
        currentLeaderEpoch = Optional.empty(),
        fetchOnlyFromLeader = true)
      assertTrue(res.isDefined)
      res.get
    }

    def fetchEarliestOffset(isolationLevel: Option[IsolationLevel]): TimestampAndOffset = {
      val res = partition.fetchOffsetForTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP,
        isolationLevel = isolationLevel,
        currentLeaderEpoch = Optional.empty(),
        fetchOnlyFromLeader = true)
      assertTrue(res.isDefined)
      res.get
    }

    assertEquals(3L, fetchLatestOffset(isolationLevel = None).offset)
    assertEquals(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)).offset)
    assertEquals(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)).offset)

    partition.log.get.updateHighWatermark(1L)

    assertEquals(3L, fetchLatestOffset(isolationLevel = None).offset)
    assertEquals(1L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)).offset)
    assertEquals(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)).offset)

    assertEquals(0L, fetchEarliestOffset(isolationLevel = None).offset)
    assertEquals(0L, fetchEarliestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)).offset)
    assertEquals(0L, fetchEarliestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)).offset)
  }

  @Test
  def testGetReplica(): Unit = {
    assertEquals(None, partition.log)
    assertThrows(classOf[NotLeaderOrFollowerException], () =>
      partition.localLogOrException
    )
  }

  @Test
  def testAppendRecordsToFollowerWithNoReplicaThrowsException(): Unit = {
    assertThrows(classOf[NotLeaderOrFollowerException], () =>
      partition.appendRecordsToFollowerOrFutureReplica(
           createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 0L), isFuture = false)
    )
  }

  @Test
  def testMakeFollowerWithNoLeaderIdChange(): Unit = {
    // Start off as follower
    var partitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(0)
      .setLeader(1)
      .setLeaderEpoch(1)
      .setIsr(List[Integer](0, 1, 2, brokerId).asJava)
      .setZkVersion(1)
      .setReplicas(List[Integer](0, 1, 2, brokerId).asJava)
      .setIsNew(false)
    partition.makeFollower(partitionState, offsetCheckpoints, None)

    // Request with same leader and epoch increases by only 1, do become-follower steps
    partitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(0)
      .setLeader(1)
      .setLeaderEpoch(4)
      .setIsr(List[Integer](0, 1, 2, brokerId).asJava)
      .setZkVersion(1)
      .setReplicas(List[Integer](0, 1, 2, brokerId).asJava)
      .setIsNew(false)
    assertTrue(partition.makeFollower(partitionState, offsetCheckpoints, None))

    // Request with same leader and same epoch, skip become-follower steps
    partitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(0)
      .setLeader(1)
      .setLeaderEpoch(4)
      .setIsr(List[Integer](0, 1, 2, brokerId).asJava)
      .setZkVersion(1)
      .setReplicas(List[Integer](0, 1, 2, brokerId).asJava)
    assertFalse(partition.makeFollower(partitionState, offsetCheckpoints, None))
  }

  @Test
  def testFollowerDoesNotJoinISRUntilCaughtUpToOffsetWithinCurrentLeaderEpoch(): Unit = {
    val controllerEpoch = 3
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val replicas = List[Integer](leader, follower1, follower2).asJava
    val isr = List[Integer](leader, follower2).asJava
    val leaderEpoch = 8
    val batch1 = TestUtils.records(records = List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k2".getBytes, "v2".getBytes)))
    val batch2 = TestUtils.records(records = List(new SimpleRecord("k3".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k4".getBytes, "v2".getBytes),
                                                  new SimpleRecord("k5".getBytes, "v3".getBytes)))
    val batch3 = TestUtils.records(records = List(new SimpleRecord("k6".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k7".getBytes, "v2".getBytes)))

    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(true)
    assertTrue(partition.makeLeader(leaderState, offsetCheckpoints, None), "Expected first makeLeader() to return 'leader changed'")
    assertEquals(leaderEpoch, partition.getLeaderEpoch, "Current leader epoch")
    assertEquals(Set[Integer](leader, follower2), partition.partitionState.isr, "ISR")

    val requestLocal = RequestLocal.withThreadConfinedCaching

    // after makeLeader(() call, partition should know about all the replicas
    // append records with initial leader epoch
    val lastOffsetOfFirstBatch = partition.appendRecordsToLeader(batch1, origin = AppendOrigin.Client,
      requiredAcks = 0, requestLocal).lastOffset
    partition.appendRecordsToLeader(batch2, origin = AppendOrigin.Client, requiredAcks = 0, requestLocal)
    assertEquals(partition.localLogOrException.logStartOffset, partition.log.get.highWatermark, "Expected leader's HW not move")

    // let the follower in ISR move leader's HW to move further but below LEO
    def updateFollowerFetchState(followerId: Int, fetchOffsetMetadata: LogOffsetMetadata): Unit = {
      partition.updateFollowerFetchState(
        followerId,
        followerFetchOffsetMetadata = fetchOffsetMetadata,
        followerStartOffset = 0L,
        followerFetchTimeMs = time.milliseconds(),
        leaderEndOffset = partition.localLogOrException.logEndOffset)
    }

    updateFollowerFetchState(follower2, LogOffsetMetadata(0))
    updateFollowerFetchState(follower2, LogOffsetMetadata(lastOffsetOfFirstBatch))
    assertEquals(lastOffsetOfFirstBatch, partition.log.get.highWatermark, "Expected leader's HW")

    // current leader becomes follower and then leader again (without any new records appended)
    val followerState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(follower2)
      .setLeaderEpoch(leaderEpoch + 1)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(false)
    partition.makeFollower(followerState, offsetCheckpoints, None)

    val newLeaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch + 2)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(false)
    assertTrue(partition.makeLeader(newLeaderState, offsetCheckpoints, None),
      "Expected makeLeader() to return 'leader changed' after makeFollower()")
    val currentLeaderEpochStartOffset = partition.localLogOrException.logEndOffset

    // append records with the latest leader epoch
    partition.appendRecordsToLeader(batch3, origin = AppendOrigin.Client, requiredAcks = 0, requestLocal)

    // fetch from follower not in ISR from log start offset should not add this follower to ISR
    updateFollowerFetchState(follower1, LogOffsetMetadata(0))
    updateFollowerFetchState(follower1, LogOffsetMetadata(lastOffsetOfFirstBatch))
    assertEquals(Set[Integer](leader, follower2), partition.partitionState.isr, "ISR")

    // fetch from the follower not in ISR from start offset of the current leader epoch should
    // add this follower to ISR
    updateFollowerFetchState(follower1, LogOffsetMetadata(currentLeaderEpochStartOffset))

    // Expansion does not affect the ISR
    assertEquals(Set[Integer](leader, follower2), partition.partitionState.isr, "ISR")
    assertEquals(Set[Integer](leader, follower1, follower2), partition.partitionState.maximalIsr, "ISR")
    assertEquals(alterIsrManager.isrUpdates.head.leaderAndIsr.isr.toSet,
      Set(leader, follower1, follower2), "AlterIsr")
  }

  def createRecords(records: Iterable[SimpleRecord], baseOffset: Long, partitionLeaderEpoch: Int = 0): MemoryRecords = {
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(
      buf, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.LOG_APPEND_TIME,
      baseOffset, time.milliseconds, partitionLeaderEpoch)
    records.foreach(builder.append)
    builder.build()
  }

  def createTransactionalRecords(records: Iterable[SimpleRecord],
                                 baseOffset: Long): MemoryRecords = {
    val producerId = 1L
    val producerEpoch = 0.toShort
    val baseSequence = 0
    val isTransactional = true
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(buf, CompressionType.NONE, baseOffset, producerId,
      producerEpoch, baseSequence, isTransactional)
    records.foreach(builder.append)
    builder.build()
  }

  /**
    * Test for AtMinIsr partition state. We set the partition replica set size as 3, but only set one replica as an ISR.
    * As the default minIsr configuration is 1, then the partition should be at min ISR (isAtMinIsr = true).
    */
  @Test
  def testAtMinIsr(): Unit = {
    val controllerEpoch = 3
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val replicas = List[Integer](leader, follower1, follower2).asJava
    val isr = List[Integer](leader).asJava
    val leaderEpoch = 8

    assertFalse(partition.isAtMinIsr)
    // Make isr set to only have leader to trigger AtMinIsr (default min isr config is 1)
    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(true)
    partition.makeLeader(leaderState, offsetCheckpoints, None)
    assertTrue(partition.isAtMinIsr)
  }

  @Test
  def testUpdateFollowerFetchState(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 6, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List[Integer](brokerId, remoteBrokerId).asJava
    val isr = replicas

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)

    val initializeTimeMs = time.milliseconds()
    assertTrue(partition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas)
          .setIsNew(true),
        offsetCheckpoints, None), "Expected become leader transition to succeed")

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(UnifiedLog.UnknownOffset, remoteReplica.logStartOffset)

    time.sleep(500)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(3),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(3L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    time.sleep(500)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(6L),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(time.milliseconds(), remoteReplica.lastCaughtUpTimeMs)
    assertEquals(6L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

  }

  @Test
  def testInvalidAlterPartitionRequestsAreNotRetried(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List[Integer](brokerId, remoteBrokerId).asJava
    val isr = List[Integer](brokerId).asJava

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    assertTrue(partition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas)
          .setIsNew(true),
        offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(Set(brokerId), partition.partitionState.isr)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(UnifiedLog.UnknownOffset, remoteReplica.logStartOffset)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 10L)

    // Check that the isr didn't change and alter update is scheduled
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(1, alterIsrManager.isrUpdates.size)
    assertEquals(Set(brokerId, remoteBrokerId), alterIsrManager.isrUpdates.head.leaderAndIsr.isr.toSet)

    // Simulate invalid request failure
    alterIsrManager.failIsrUpdate(Errors.INVALID_REQUEST)

    // Still no ISR change and no retry
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(0, alterIsrManager.isrUpdates.size)

    assertEquals(0, isrChangeListener.expands.get)
    assertEquals(0, isrChangeListener.shrinks.get)
    assertEquals(1, isrChangeListener.failures.get)
  }

  @Test
  def testIsrExpansion(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId).asJava

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    assertTrue(partition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas.map(Int.box).asJava)
          .setIsNew(true),
        offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(Set(brokerId), partition.partitionState.isr)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(UnifiedLog.UnknownOffset, remoteReplica.logStartOffset)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(3),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(Set(brokerId), partition.partitionState.isr)
    assertEquals(3L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(alterIsrManager.isrUpdates.size, 1)
    val isrItem = alterIsrManager.isrUpdates.head
    assertEquals(isrItem.leaderAndIsr.isr, List(brokerId, remoteBrokerId))
    assertEquals(Set(brokerId), partition.partitionState.isr)
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // Complete the ISR expansion
    alterIsrManager.completeIsrUpdate(2)
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.isr)

    assertEquals(isrChangeListener.expands.get, 1)
    assertEquals(isrChangeListener.shrinks.get, 0)
    assertEquals(isrChangeListener.failures.get, 0)
  }

  @Test
  def testIsrNotExpandedIfUpdateFails(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List[Integer](brokerId, remoteBrokerId).asJava
    val isr = List[Integer](brokerId).asJava

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints, None)
    assertTrue(partition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas)
          .setIsNew(true),
        offsetCheckpoints, None), "Expected become leader transition to succeed")
    assertEquals(Set(brokerId), partition.partitionState.isr)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(UnifiedLog.UnknownOffset, remoteReplica.logStartOffset)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 10L)

    // Follower state is updated, but the ISR has not expanded
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(alterIsrManager.isrUpdates.size, 1)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // Simulate failure callback
    alterIsrManager.failIsrUpdate(Errors.INVALID_UPDATE_VERSION)

    // Still no ISR change and it doesn't retry
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(alterIsrManager.isrUpdates.size, 0)
    assertEquals(isrChangeListener.expands.get, 0)
    assertEquals(isrChangeListener.shrinks.get, 0)
    assertEquals(isrChangeListener.failures.get, 1)
  }

  @Test
  def testRetryShrinkIsr(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Seq(brokerId, remoteBrokerId)
    val isr = Seq(brokerId, remoteBrokerId)
    val topicId = Uuid.randomUuid()

    assertTrue(makeLeader(
      topicId = Some(topicId),
      controllerEpoch = controllerEpoch,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      zkVersion = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // Sleep enough time to shrink the ISR
    time.sleep(partition.replicaLagTimeMaxMs + 1)

    // Try to shrink the ISR
    partition.maybeShrinkIsr()
    assertEquals(alterIsrManager.isrUpdates.size, 1)
    assertEquals(alterIsrManager.isrUpdates.head.leaderAndIsr.isr, List(brokerId))
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.isr)
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)

    // The shrink fails and we retry
    alterIsrManager.failIsrUpdate(Errors.NETWORK_EXCEPTION)
    assertEquals(0, isrChangeListener.shrinks.get)
    assertEquals(1, isrChangeListener.failures.get)
    assertEquals(1, partition.getZkVersion)
    assertEquals(alterIsrManager.isrUpdates.size, 1)
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.isr)
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // The shrink succeeds after retrying
    alterIsrManager.completeIsrUpdate(newZkVersion = 2)
    assertEquals(1, isrChangeListener.shrinks.get)
    assertEquals(2, partition.getZkVersion)
    assertEquals(alterIsrManager.isrUpdates.size, 0)
    assertEquals(Set(brokerId), partition.partitionState.isr)
    assertEquals(Set(brokerId), partition.partitionState.maximalIsr)
    assertEquals(log.logEndOffset, partition.localLogOrException.highWatermark)
  }

  @Test
  def testMaybeShrinkIsr(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Seq(brokerId, remoteBrokerId)
    val isr = Seq(brokerId, remoteBrokerId)
    val initializeTimeMs = time.milliseconds()

    assertTrue(makeLeader(
      topicId = None,
      controllerEpoch = controllerEpoch,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      zkVersion = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(UnifiedLog.UnknownOffset, remoteReplica.logStartOffset)

    // On initialization, the replica is considered caught up and should not be removed
    partition.maybeShrinkIsr()
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.isr)

    // If enough time passes without a fetch update, the ISR should shrink
    time.sleep(partition.replicaLagTimeMaxMs + 1)

    // Shrink the ISR
    partition.maybeShrinkIsr()
    assertEquals(0, isrChangeListener.shrinks.get)
    assertEquals(alterIsrManager.isrUpdates.size, 1)
    assertEquals(alterIsrManager.isrUpdates.head.leaderAndIsr.isr, List(brokerId))
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.isr)
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // After the ISR shrink completes, the ISR state should be updated and the
    // high watermark should be advanced
    alterIsrManager.completeIsrUpdate(newZkVersion = 2)
    assertEquals(1, isrChangeListener.shrinks.get)
    assertEquals(2, partition.getZkVersion)
    assertEquals(alterIsrManager.isrUpdates.size, 0)
    assertEquals(Set(brokerId), partition.partitionState.isr)
    assertEquals(Set(brokerId), partition.partitionState.maximalIsr)
    assertEquals(log.logEndOffset, partition.localLogOrException.highWatermark)
  }

  @Test
  def testAlterIsrLeaderAndIsrRace(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Seq(brokerId, remoteBrokerId)
    val isr = Seq(brokerId, remoteBrokerId)
    val initializeTimeMs = time.milliseconds()

    assertTrue(makeLeader(
      topicId = None,
      controllerEpoch = controllerEpoch,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      zkVersion = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(UnifiedLog.UnknownOffset, remoteReplica.logStartOffset)

    // Shrink the ISR
    time.sleep(partition.replicaLagTimeMaxMs + 1)
    partition.maybeShrinkIsr()
    assertTrue(partition.partitionState.isInflight)

    // Become leader again, reset the ISR state
    assertFalse(makeLeader(
      topicId = None,
      controllerEpoch = controllerEpoch,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      zkVersion = 2,
      isNew = false
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)
    assertFalse(partition.partitionState.isInflight, "ISR should be committed and not inflight")

    // Try the shrink again, should not submit until AlterIsr response arrives
    time.sleep(partition.replicaLagTimeMaxMs + 1)
    partition.maybeShrinkIsr()
    assertFalse(partition.partitionState.isInflight, "ISR should still be committed and not inflight")

    // Complete the AlterIsr update and now we can make modifications again
    alterIsrManager.completeIsrUpdate(10)
    partition.maybeShrinkIsr()
    assertTrue(partition.partitionState.isInflight, "ISR should be pending a shrink")
  }

  @Test
  def testShouldNotShrinkIsrIfPreviousFetchIsCaughtUp(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Seq(brokerId, remoteBrokerId)
    val isr = Seq(brokerId, remoteBrokerId)
    val initializeTimeMs = time.milliseconds()

    assertTrue(makeLeader(
      topicId = None,
      controllerEpoch = controllerEpoch,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      zkVersion = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(UnifiedLog.UnknownOffset, remoteReplica.logStartOffset)

    // There is a short delay before the first fetch. The follower is not yet caught up to the log end.
    time.sleep(5000)
    val firstFetchTimeMs = time.milliseconds()
    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(5),
      followerStartOffset = 0L,
      followerFetchTimeMs = firstFetchTimeMs,
      leaderEndOffset = 10L)
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(5L, partition.localLogOrException.highWatermark)
    assertEquals(5L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // Some new data is appended, but the follower catches up to the old end offset.
    // The total elapsed time from initialization is larger than the max allowed replica lag.
    time.sleep(5001)
    seedLogData(log, numRecords = 5, leaderEpoch = leaderEpoch)
    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 15L)
    assertEquals(firstFetchTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(10L, partition.localLogOrException.highWatermark)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // The ISR should not be shrunk because the follower has caught up with the leader at the
    // time of the first fetch.
    partition.maybeShrinkIsr()
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.isr)
    assertEquals(alterIsrManager.isrUpdates.size, 0)
  }

  @Test
  def testShouldNotShrinkIsrIfFollowerCaughtUpToLogEnd(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Seq(brokerId, remoteBrokerId)
    val isr = Seq(brokerId, remoteBrokerId)
    val initializeTimeMs = time.milliseconds()

    assertTrue(makeLeader(
      topicId = None,
      controllerEpoch = controllerEpoch,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      zkVersion = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(UnifiedLog.UnknownOffset, remoteReplica.logStartOffset)

    // The follower catches up to the log end immediately.
    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 10L)
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(10L, partition.localLogOrException.highWatermark)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // Sleep longer than the max allowed follower lag
    time.sleep(30001)

    // The ISR should not be shrunk because the follower is caught up to the leader's log end
    partition.maybeShrinkIsr()
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.isr)
    assertEquals(alterIsrManager.isrUpdates.size, 0)
  }

  @Test
  def testIsrNotShrunkIfUpdateFails(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Seq(brokerId, remoteBrokerId)
    val isr = Seq(brokerId, remoteBrokerId)
    val initializeTimeMs = time.milliseconds()

    assertTrue(makeLeader(
      topicId = None,
      controllerEpoch = controllerEpoch,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      zkVersion = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(UnifiedLog.UnknownOffset, remoteReplica.logStartOffset)

    time.sleep(30001)

    // Enqueue and AlterIsr that will fail
    partition.maybeShrinkIsr()
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(alterIsrManager.isrUpdates.size, 1)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // Simulate failure callback
    alterIsrManager.failIsrUpdate(Errors.INVALID_UPDATE_VERSION)

    // Ensure ISR hasn't changed
    assertEquals(partition.partitionState.getClass, classOf[PendingShrinkIsr])
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(alterIsrManager.isrUpdates.size, 0)
    assertEquals(0L, partition.localLogOrException.highWatermark)
  }

  @Test
  def testAlterIsrUnknownTopic(): Unit = {
    handleAlterIsrFailure(Errors.UNKNOWN_TOPIC_OR_PARTITION,
      (brokerId: Int, remoteBrokerId: Int, partition: Partition) => {
        assertEquals(partition.partitionState.isr, Set(brokerId))
        assertEquals(partition.partitionState.maximalIsr, Set(brokerId, remoteBrokerId))
        assertEquals(alterIsrManager.isrUpdates.size, 0)
      })
  }

  @Test
  def testAlterIsrInvalidVersion(): Unit = {
    handleAlterIsrFailure(Errors.INVALID_UPDATE_VERSION,
      (brokerId: Int, remoteBrokerId: Int, partition: Partition) => {
        assertEquals(partition.partitionState.isr, Set(brokerId))
        assertEquals(partition.partitionState.maximalIsr, Set(brokerId, remoteBrokerId))
        assertEquals(alterIsrManager.isrUpdates.size, 0)
      })
  }

  @Test
  def testAlterIsrUnexpectedError(): Unit = {
    handleAlterIsrFailure(Errors.UNKNOWN_SERVER_ERROR,
      (brokerId: Int, remoteBrokerId: Int, partition: Partition) => {
        // We retry these
        assertEquals(partition.partitionState.isr, Set(brokerId))
        assertEquals(partition.partitionState.maximalIsr, Set(brokerId, remoteBrokerId))
        assertEquals(alterIsrManager.isrUpdates.size, 1)
      })
  }

  def handleAlterIsrFailure(error: Errors, callback: (Int, Int, Partition) => Unit): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = Seq(brokerId, remoteBrokerId)
    val isr = Seq(brokerId)

    assertTrue(makeLeader(
      topicId = None,
      controllerEpoch = controllerEpoch,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      zkVersion = 1,
      isNew = true
    ))
    assertEquals(10L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(UnifiedLog.UnknownOffset, remoteReplica.logStartOffset)

    // This will attempt to expand the ISR
    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 10L)

    // Follower state is updated, but the ISR has not expanded
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(Set(brokerId, remoteBrokerId), partition.partitionState.maximalIsr)
    assertEquals(alterIsrManager.isrUpdates.size, 1)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // Failure
    alterIsrManager.failIsrUpdate(error)
    callback(brokerId, remoteBrokerId, partition)
  }

  @Test
  def testSingleInFlightAlterIsr(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val follower3 = brokerId + 3
    val replicas = Seq(brokerId, follower1, follower2, follower3)
    val isr = Seq(brokerId, follower1, follower2)

    doNothing().when(delayedOperations).checkAndCompleteAll()

    assertTrue(makeLeader(
      topicId = None,
      controllerEpoch = controllerEpoch,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      zkVersion = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // Expand ISR
    partition.updateFollowerFetchState(
      followerId = follower3,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 10
    )
    assertEquals(Set(brokerId, follower1, follower2), partition.partitionState.isr)
    assertEquals(Set(brokerId, follower1, follower2, follower3), partition.partitionState.maximalIsr)

    // One AlterIsr request in-flight
    assertEquals(alterIsrManager.isrUpdates.size, 1)

    // Try to modify ISR again, should do nothing
    time.sleep(partition.replicaLagTimeMaxMs + 1)
    partition.maybeShrinkIsr()
    assertEquals(alterIsrManager.isrUpdates.size, 1)
  }

  @Test
  def testZkIsrManagerAsyncCallback(): Unit = {
    // We need a real scheduler here so that the ISR write lock works properly
    val scheduler = new KafkaScheduler(1, "zk-isr-test")
    scheduler.startup()
    val kafkaZkClient = mock(classOf[KafkaZkClient])

    doAnswer(_ => (true, 2))
      .when(kafkaZkClient)
      .conditionalUpdatePath(anyString(), any(), ArgumentMatchers.eq(1), any())

    val zkIsrManager = AlterIsrManager(scheduler, time, kafkaZkClient)
    zkIsrManager.start()

    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = KAFKA_2_6_IV0, // shouldn't matter, but set this to a ZK isr version
      localBrokerId = brokerId,
      time,
      isrChangeListener,
      delayedOperations,
      metadataCache,
      logManager,
      zkIsrManager)

    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val follower3 = brokerId + 3
    val replicas = Seq(brokerId, follower1, follower2, follower3)
    val isr = Seq(brokerId, follower1, follower2)

    doNothing().when(delayedOperations).checkAndCompleteAll()

    assertTrue(makeLeader(
      partition = partition,
      topicId = None,
      controllerEpoch = controllerEpoch,
      leaderEpoch = leaderEpoch,
      isr = isr,
      replicas = replicas,
      zkVersion = 1,
      isNew = true
    ))
    assertEquals(0L, partition.localLogOrException.highWatermark)

    // Expand ISR
    partition.updateFollowerFetchState(
      followerId = follower3,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 10
    )

    // Try avoiding a race
    TestUtils.waitUntilTrue(() => !partition.partitionState.isInflight, "Expected ISR state to be committed", 100)

    partition.partitionState match {
      case CommittedPartitionState(isr, _) => assertEquals(Set(brokerId, follower1, follower2, follower3), isr)
      case _ => fail("Expected a committed ISR following Zk expansion")
    }

    scheduler.shutdown()
  }

  @Test
  def testUseCheckpointToInitializeHighWatermark(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, topicId = None)
    seedLogData(log, numRecords = 6, leaderEpoch = 5)

    when(offsetCheckpoints.fetch(logDir1.getAbsolutePath, topicPartition))
      .thenReturn(Some(4L))

    val controllerEpoch = 3
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(6)
      .setIsr(replicas)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(false)
    partition.makeLeader(leaderState, offsetCheckpoints, None)
    assertEquals(4, partition.localLogOrException.highWatermark)
  }

  @Test
  def testTopicIdAndPartitionMetadataFileForLeader(): Unit = {
    val controllerEpoch = 3
    val leaderEpoch = 5
    val topicId = Uuid.randomUuid()
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(replicas)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(false)
    partition.makeLeader(leaderState, offsetCheckpoints, Some(topicId))

    checkTopicId(topicId, partition)

    // Create new Partition object for same topicPartition
    val partition2 = new Partition(topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      isrChangeListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterIsrManager)

    // partition2 should not yet be associated with the log, but should be able to get ID
    assertTrue(partition2.topicId.isDefined)
    assertEquals(topicId, partition2.topicId.get)
    assertFalse(partition2.log.isDefined)

    // Calling makeLeader with a new topic ID should not overwrite the old topic ID. We should get an InconsistentTopicIdException.
    // This scenario should not occur, since the topic ID check will fail.
    assertThrows(classOf[InconsistentTopicIdException], () => partition2.makeLeader(leaderState, offsetCheckpoints, Some(Uuid.randomUuid())))

    // Calling makeLeader with no topic ID should not overwrite the old topic ID. We should get the original log.
    partition2.makeLeader(leaderState, offsetCheckpoints, None)
    checkTopicId(topicId, partition2)
  }

  @Test
  def testTopicIdAndPartitionMetadataFileForFollower(): Unit = {
    val controllerEpoch = 3
    val leaderEpoch = 5
    val topicId = Uuid.randomUuid()
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(replicas)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(false)
    partition.makeFollower(leaderState, offsetCheckpoints, Some(topicId))

    checkTopicId(topicId, partition)

    // Create new Partition object for same topicPartition
    val partition2 = new Partition(topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      isrChangeListener,
      delayedOperations,
      metadataCache,
      logManager,
      alterIsrManager)

    // partition2 should not yet be associated with the log, but should be able to get ID
    assertTrue(partition2.topicId.isDefined)
    assertEquals(topicId, partition2.topicId.get)
    assertFalse(partition2.log.isDefined)

    // Calling makeFollower with a new topic ID should not overwrite the old topic ID. We should get an InconsistentTopicIdException.
    // This scenario should not occur, since the topic ID check will fail.
    assertThrows(classOf[InconsistentTopicIdException], () => partition2.makeFollower(leaderState, offsetCheckpoints, Some(Uuid.randomUuid())))

    // Calling makeFollower with no topic ID should not overwrite the old topic ID. We should get the original log.
    partition2.makeFollower(leaderState, offsetCheckpoints, None)
    checkTopicId(topicId, partition2)
  }

  def checkTopicId(expectedTopicId: Uuid, partition: Partition): Unit = {
    assertTrue(partition.topicId.isDefined)
    assertEquals(expectedTopicId, partition.topicId.get)
    assertTrue(partition.log.isDefined)
    val log = partition.log.get
    assertEquals(expectedTopicId, log.topicId.get)
    assertTrue(log.partitionMetadataFile.exists())
    assertEquals(expectedTopicId, log.partitionMetadataFile.read().topicId)
  }

  @Test
  def testAddAndRemoveMetrics(): Unit = {
    val metricsToCheck = List(
      "UnderReplicated",
      "UnderMinIsr",
      "InSyncReplicasCount",
      "ReplicasCount",
      "LastStableOffsetLag",
      "AtMinIsr")

    def getMetric(metric: String): Option[Metric] = {
      KafkaYammerMetrics.defaultRegistry().allMetrics().asScala.filter { case (metricName, _) =>
        metricName.getName == metric && metricName.getType == "Partition"
      }.headOption.map(_._2)
    }

    assertTrue(metricsToCheck.forall(getMetric(_).isDefined))

    Partition.removeMetrics(topicPartition)

    assertEquals(Set(), KafkaYammerMetrics.defaultRegistry().allMetrics().asScala.keySet.filter(_.getType == "Partition"))
  }

  @Test
  def testUnderReplicatedPartitionsCorrectSemantics(): Unit = {
    val controllerEpoch = 3
    val replicas = List[Integer](brokerId, brokerId + 1, brokerId + 2).asJava
    val isr = List[Integer](brokerId, brokerId + 1).asJava

    var leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(6)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(false)
    partition.makeLeader(leaderState, offsetCheckpoints, None)
    assertTrue(partition.isUnderReplicated)

    leaderState = leaderState.setIsr(replicas)
    partition.makeLeader(leaderState, offsetCheckpoints, None)
    assertFalse(partition.isUnderReplicated)
  }

  @Test
  def testUpdateAssignmentAndIsr(): Unit = {
    val topicPartition = new TopicPartition("test", 1)
    val partition = new Partition(
      topicPartition, 1000, ApiVersion.latestVersion, 0,
      new SystemTime(), mock(classOf[IsrChangeListener]), mock(classOf[DelayedOperations]),
      mock(classOf[MetadataCache]), mock(classOf[LogManager]), mock(classOf[AlterIsrManager]))

    val replicas = Seq(0, 1, 2, 3)
    val isr = Set(0, 1, 2, 3)
    val adding = Seq(4, 5)
    val removing = Seq(1, 2)

    // Test with ongoing reassignment
    partition.updateAssignmentAndIsr(replicas, isr, adding, removing, LeaderRecoveryState.RECOVERED)

    assertTrue(partition.assignmentState.isInstanceOf[OngoingReassignmentState], "The assignmentState is not OngoingReassignmentState")
    assertEquals(replicas, partition.assignmentState.replicas)
    assertEquals(isr, partition.partitionState.isr)
    assertEquals(adding, partition.assignmentState.asInstanceOf[OngoingReassignmentState].addingReplicas)
    assertEquals(removing, partition.assignmentState.asInstanceOf[OngoingReassignmentState].removingReplicas)
    assertEquals(Seq(1, 2, 3), partition.remoteReplicas.map(_.brokerId))

    // Test with simple assignment
    val replicas2 = Seq(0, 3, 4, 5)
    val isr2 = Set(0, 3, 4, 5)
    partition.updateAssignmentAndIsr(replicas2, isr2, Seq.empty, Seq.empty, LeaderRecoveryState.RECOVERED)

    assertTrue(partition.assignmentState.isInstanceOf[SimpleAssignmentState], "The assignmentState is not SimpleAssignmentState")
    assertEquals(replicas2, partition.assignmentState.replicas)
    assertEquals(isr2, partition.partitionState.isr)
    assertEquals(Seq(3, 4, 5), partition.remoteReplicas.map(_.brokerId))
  }

  /**
   * Test when log is getting initialized, its config remains untouched after initialization is done.
   */
  @Test
  def testLogConfigNotDirty(): Unit = {
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = CleanerConfig(enableCleaner = false), time = time)
    val spyLogManager = spy(logManager)
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      isrChangeListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterIsrManager)

    partition.createLog(isNew = true, isFutureReplica = false, offsetCheckpoints, topicId = None)

    // Validate that initializingLog and finishedInitializingLog was called
    verify(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))
    verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(topicPartition), ArgumentMatchers.any())

    // We should retrieve configs only once
    verify(spyConfigRepository, times(1)).topicConfig(topicPartition.topic())
  }

  /**
   * Test when log is getting initialized, its config remains gets reloaded if Topic config gets changed
   * before initialization is done.
   */
  @Test
  def testLogConfigDirtyAsTopicUpdated(): Unit = {
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = CleanerConfig(enableCleaner = false), time = time)
    val spyLogManager = spy(logManager)
    doAnswer((_: InvocationOnMock) => {
      logManager.initializingLog(topicPartition)
      logManager.topicConfigUpdated(topicPartition.topic())
    }).when(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))

    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      isrChangeListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterIsrManager)

    partition.createLog(isNew = true, isFutureReplica = false, offsetCheckpoints, topicId = None)

    // Validate that initializingLog and finishedInitializingLog was called
    verify(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))
    verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(topicPartition), ArgumentMatchers.any())

    // We should retrieve configs twice, once before log is created, and second time once
    // we find log config is dirty and refresh it.
    verify(spyConfigRepository, times(2)).topicConfig(topicPartition.topic())
  }

  /**
   * Test when log is getting initialized, its config remains gets reloaded if Broker config gets changed
   * before initialization is done.
   */
  @Test
  def testLogConfigDirtyAsBrokerUpdated(): Unit = {
    logManager.shutdown()
    val spyConfigRepository = spy(configRepository)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, configRepository = spyConfigRepository,
      cleanerConfig = CleanerConfig(enableCleaner = false), time = time)
    logManager.startup(Set.empty)

    val spyLogManager = spy(logManager)
    doAnswer((_: InvocationOnMock) => {
      logManager.initializingLog(topicPartition)
      logManager.brokerConfigUpdated()
    }).when(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))

    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      isrChangeListener,
      delayedOperations,
      metadataCache,
      spyLogManager,
      alterIsrManager)

    partition.createLog(isNew = true, isFutureReplica = false, offsetCheckpoints, topicId = None)

    // Validate that initializingLog and finishedInitializingLog was called
    verify(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))
    verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(topicPartition), ArgumentMatchers.any())

    // We should get configs twice, once before log is created, and second time once
    // we find log config is dirty and refresh it.
    verify(spyConfigRepository, times(2)).topicConfig(topicPartition.topic())
  }

  private def makeLeader(
    topicId: Option[Uuid],
    controllerEpoch: Int,
    leaderEpoch: Int,
    isr: Seq[Int],
    replicas: Seq[Int],
    zkVersion: Int,
    isNew: Boolean,
    partition: Partition = partition
  ): Boolean = {
    partition.createLogIfNotExists(
      isNew = isNew,
      isFutureReplica = false,
      offsetCheckpoints,
      topicId
    )
    val newLeader = partition.makeLeader(
      new LeaderAndIsrPartitionState()
        .setControllerEpoch(controllerEpoch)
        .setLeader(brokerId)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr.map(Int.box).asJava)
        .setZkVersion(zkVersion)
        .setReplicas(replicas.map(Int.box).asJava)
        .setIsNew(isNew),
      offsetCheckpoints,
      topicId
    )
    assertTrue(partition.isLeader)
    assertFalse(partition.partitionState.isInflight)
    assertEquals(topicId, partition.topicId)
    assertEquals(leaderEpoch, partition.getLeaderEpoch)
    assertEquals(isr.toSet, partition.partitionState.isr)
    assertEquals(isr.toSet, partition.partitionState.maximalIsr)
    assertEquals(zkVersion, partition.getZkVersion)
    newLeader
  }

  private def seedLogData(log: UnifiedLog, numRecords: Int, leaderEpoch: Int): Unit = {
    for (i <- 0 until numRecords) {
      val records = MemoryRecords.withRecords(0L, CompressionType.NONE, leaderEpoch,
        new SimpleRecord(s"k$i".getBytes, s"v$i".getBytes))
      log.appendAsLeader(records, leaderEpoch)
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

    override def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
      appendSemaphore.acquire()
      val appendInfo = super.appendAsFollower(records)
      appendInfo
    }
  }
}
