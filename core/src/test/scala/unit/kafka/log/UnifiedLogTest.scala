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

package kafka.log

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.{InvalidRecordException, TopicPartition, Uuid}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{ListOffsetsRequest, ListOffsetsResponse}
import org.apache.kafka.common.utils.{BufferSupplier, Time, Utils}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.common.{RequestLocal, TransactionVersion}
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig
import org.apache.kafka.server.log.remote.storage.{NoOpRemoteLogMetadataManager, NoOpRemoteStorageManager, RemoteLogManager, RemoteLogManagerConfig}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.purgatory.{DelayedOperationPurgatory, DelayedRemoteListOffsets}
import org.apache.kafka.server.storage.log.{FetchIsolation, UnexpectedAppendOffsetException}
import org.apache.kafka.server.util.{KafkaScheduler, MockTime, Scheduler}
import org.apache.kafka.storage.internals.checkpoint.{LeaderEpochCheckpointFile, PartitionMetadataFile}
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.{AbortedTxn, AppendOrigin, AsyncOffsetReader, Cleaner, EpochEntry, LogConfig, LogFileUtils, LogOffsetMetadata, LogOffsetSnapshot, LogOffsetsListener, LogSegment, LogSegments, LogStartOffsetIncrementReason, LogToClean, OffsetResultHolder, OffsetsOutOfOrderException, ProducerStateManager, ProducerStateManagerConfig, RecordValidationException, UnifiedLog, VerificationGuard}
import org.apache.kafka.storage.internals.utils.Throttler
import org.apache.kafka.storage.log.metrics.{BrokerTopicMetrics, BrokerTopicStats}
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, _}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ArgumentsSource
import org.junit.jupiter.params.provider.{EnumSource, ValueSource}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{doAnswer, doThrow, spy}

import net.jqwik.api.AfterFailureMode
import net.jqwik.api.ForAll
import net.jqwik.api.Property
import org.apache.kafka.server.config.KRaftConfigs

import java.io._
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util
import java.util.concurrent.{Callable, ConcurrentHashMap, Executors, TimeUnit}
import java.util.{Optional, OptionalLong, Properties}
import scala.collection.immutable.SortedSet
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class UnifiedLogTest {
  var config: KafkaConfig = _
  val brokerTopicStats = new BrokerTopicStats
  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val mockTime = new MockTime()
  var logsToClose: Seq[UnifiedLog] = Seq()
  val producerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false)
  def metricsKeySet = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = -1)
    config = KafkaConfig.fromProps(props)
  }

  @AfterEach
  def tearDown(): Unit = {
    brokerTopicStats.close()
    logsToClose.foreach(l => Utils.closeQuietly(l, "UnifiedLog"))
    Utils.delete(tmpDir)
  }

  def createEmptyLogs(dir: File, offsets: Int*): Unit = {
    for (offset <- offsets) {
      Files.createFile(LogFileUtils.logFile(dir, offset).toPath)
      Files.createFile(LogFileUtils.offsetIndexFile(dir, offset).toPath)
    }
  }

  @Test
  def testHighWatermarkMetadataUpdatedAfterSegmentRoll(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)

    def assertFetchSizeAndOffsets(fetchOffset: Long,
                                  expectedSize: Int,
                                  expectedOffsets: Seq[Long]): Unit = {
      val readInfo = log.read(
        fetchOffset,
        2048,
        FetchIsolation.HIGH_WATERMARK,
        false)
      assertEquals(expectedSize, readInfo.records.sizeInBytes)
      assertEquals(expectedOffsets, readInfo.records.records.asScala.map(_.offset))
    }

    val records = TestUtils.records(List(
      new SimpleRecord(mockTime.milliseconds, "a".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "b".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "c".getBytes, "value".getBytes)
    ))

    log.appendAsLeader(records, 0)
    assertFetchSizeAndOffsets(fetchOffset = 0L, 0, Seq())

    log.maybeIncrementHighWatermark(log.logEndOffsetMetadata)
    assertFetchSizeAndOffsets(fetchOffset = 0L, records.sizeInBytes, Seq(0, 1, 2))

    log.roll()
    assertFetchSizeAndOffsets(fetchOffset = 0L, records.sizeInBytes, Seq(0, 1, 2))

    log.appendAsLeader(records, 0)
    assertFetchSizeAndOffsets(fetchOffset = 3L, 0, Seq())
  }

  @Test
  def testAppendAsLeaderWithRaftLeader(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)
    val leaderEpoch = 0

    def records(offset: Long): MemoryRecords = TestUtils.records(List(
      new SimpleRecord(mockTime.milliseconds, "a".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "b".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "c".getBytes, "value".getBytes)
    ), baseOffset = offset, partitionLeaderEpoch = leaderEpoch)

    log.appendAsLeader(records(0), leaderEpoch, AppendOrigin.RAFT_LEADER)
    assertEquals(0, log.logStartOffset)
    assertEquals(3L, log.logEndOffset)

    // Since raft leader is responsible for assigning offsets, and the LogValidator is bypassed from the performance perspective,
    // so the first offset of the MemoryRecords to be append should equal to the next offset in the log
    assertThrows(classOf[UnexpectedAppendOffsetException], () => log.appendAsLeader(records(1), leaderEpoch, AppendOrigin.RAFT_LEADER))

    // When the first offset of the MemoryRecords to be append equals to the next offset in the log, append will succeed
    log.appendAsLeader(records(3), leaderEpoch, AppendOrigin.RAFT_LEADER)
    assertEquals(6, log.logEndOffset)
  }

  @Test
  def testAppendInfoFirstOffset(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)

    val simpleRecords = List(
      new SimpleRecord(mockTime.milliseconds, "a".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "b".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "c".getBytes, "value".getBytes)
    )

    val records = TestUtils.records(simpleRecords)

    val firstAppendInfo = log.appendAsLeader(records, 0)
    assertEquals(0, firstAppendInfo.firstOffset)

    val secondAppendInfo = log.appendAsLeader(
      TestUtils.records(simpleRecords),
      0
    )
    assertEquals(simpleRecords.size, secondAppendInfo.firstOffset)

    log.roll()
    val afterRollAppendInfo =  log.appendAsLeader(TestUtils.records(simpleRecords), 0)
    assertEquals(simpleRecords.size * 2, afterRollAppendInfo.firstOffset)
  }

  @Test
  def testTruncateBelowFirstUnstableOffset(): Unit = {
    testTruncateBelowFirstUnstableOffset((log, targetOffset) => log.truncateTo(targetOffset))
  }

  @Test
  def testTruncateFullyAndStartBelowFirstUnstableOffset(): Unit = {
    testTruncateBelowFirstUnstableOffset((log, targetOffset) => log.truncateFullyAndStartAt(targetOffset, Optional.empty))
  }

  @Test
  def testTruncateFullyAndStart(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)

    val producerId = 17L
    val producerEpoch: Short = 10
    val sequence = 0

    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("0".getBytes),
      new SimpleRecord("1".getBytes),
      new SimpleRecord("2".getBytes)
    )), 0)

    log.appendAsLeader(MemoryRecords.withTransactionalRecords(
      Compression.NONE,
      producerId,
      producerEpoch,
      sequence,
      new SimpleRecord("3".getBytes),
      new SimpleRecord("4".getBytes)
    ), 0)

    assertEquals(Optional.of(3L), log.firstUnstableOffset)

    // We close and reopen the log to ensure that the first unstable offset segment
    // position will be undefined when we truncate the log.
    log.close()

    val reopened = createLog(logDir, logConfig)
    assertEquals(Optional.of(new LogOffsetMetadata(3L)), reopened.producerStateManager.firstUnstableOffset)

    reopened.truncateFullyAndStartAt(2L, Optional.of(1L))
    assertEquals(Optional.empty, reopened.firstUnstableOffset)
    assertEquals(util.Map.of, reopened.producerStateManager.activeProducers)
    assertEquals(1L, reopened.logStartOffset)
    assertEquals(2L, reopened.logEndOffset)
  }

  private def testTruncateBelowFirstUnstableOffset(truncateFunc: (UnifiedLog, Long) => Unit): Unit = {
    // Verify that truncation below the first unstable offset correctly
    // resets the producer state. Specifically we are testing the case when
    // the segment position of the first unstable offset is unknown.

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)

    val producerId = 17L
    val producerEpoch: Short = 10
    val sequence = 0

    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("0".getBytes),
      new SimpleRecord("1".getBytes),
      new SimpleRecord("2".getBytes)
    )), 0)

    log.appendAsLeader(MemoryRecords.withTransactionalRecords(
      Compression.NONE,
      producerId,
      producerEpoch,
      sequence,
      new SimpleRecord("3".getBytes),
      new SimpleRecord("4".getBytes)
    ), 0)

    assertEquals(Optional.of(3L), log.firstUnstableOffset)

    // We close and reopen the log to ensure that the first unstable offset segment
    // position will be undefined when we truncate the log.
    log.close()

    val reopened = createLog(logDir, logConfig)
    assertEquals(Optional.of(new LogOffsetMetadata(3L)), reopened.producerStateManager.firstUnstableOffset)

    truncateFunc(reopened, 0L)
    assertEquals(Optional.empty, reopened.firstUnstableOffset)
    assertEquals(util.Map.of, reopened.producerStateManager.activeProducers)
  }

  @Test
  def testHighWatermarkMaintenance(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)
    val leaderEpoch = 0

    def records(offset: Long): MemoryRecords = TestUtils.records(List(
      new SimpleRecord(mockTime.milliseconds, "a".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "b".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "c".getBytes, "value".getBytes)
    ), baseOffset = offset, partitionLeaderEpoch= leaderEpoch)

    def assertHighWatermark(offset: Long): Unit = {
      assertEquals(offset, log.highWatermark)
      assertValidLogOffsetMetadata(log, log.fetchOffsetSnapshot.highWatermark)
    }

    // High watermark initialized to 0
    assertHighWatermark(0L)

    // High watermark not changed by append
    log.appendAsLeader(records(0), leaderEpoch)
    assertHighWatermark(0L)

    // Update high watermark as leader
    log.maybeIncrementHighWatermark(new LogOffsetMetadata(1L))
    assertHighWatermark(1L)

    // Cannot update past the log end offset
    log.updateHighWatermark(5L)
    assertHighWatermark(3L)

    // Update high watermark as follower
    log.appendAsFollower(records(3L), leaderEpoch)
    log.updateHighWatermark(6L)
    assertHighWatermark(6L)

    // High watermark should be adjusted by truncation
    log.truncateTo(3L)
    assertHighWatermark(3L)

    log.appendAsLeader(records(0L), 0)
    assertHighWatermark(3L)
    assertEquals(6L, log.logEndOffset)
    assertEquals(0L, log.logStartOffset)

    // Full truncation should also reset high watermark
    log.truncateFullyAndStartAt(4L, Optional.empty)
    assertEquals(4L, log.logEndOffset)
    assertEquals(4L, log.logStartOffset)
    assertHighWatermark(4L)
  }

  private def assertNonEmptyFetch(log: UnifiedLog, offset: Long, isolation: FetchIsolation, batchBaseOffset: Long): Unit = {
    val readInfo = log.read(offset, Int.MaxValue, isolation, true)

    assertFalse(readInfo.firstEntryIncomplete)
    assertTrue(readInfo.records.sizeInBytes > 0)

    val upperBoundOffset = isolation match {
      case FetchIsolation.LOG_END => log.logEndOffset
      case FetchIsolation.HIGH_WATERMARK => log.highWatermark
      case FetchIsolation.TXN_COMMITTED => log.lastStableOffset
    }

    for (record <- readInfo.records.records.asScala)
      assertTrue(record.offset < upperBoundOffset)

    assertEquals(batchBaseOffset, readInfo.fetchOffsetMetadata.messageOffset)
    assertValidLogOffsetMetadata(log, readInfo.fetchOffsetMetadata)
  }

  private def assertEmptyFetch(log: UnifiedLog, offset: Long, isolation: FetchIsolation, batchBaseOffset: Long): Unit = {
    val readInfo = log.read(offset, Int.MaxValue, isolation, true)
    assertFalse(readInfo.firstEntryIncomplete)
    assertEquals(0, readInfo.records.sizeInBytes)
    assertEquals(batchBaseOffset, readInfo.fetchOffsetMetadata.messageOffset)
    assertValidLogOffsetMetadata(log, readInfo.fetchOffsetMetadata)
  }

  @Test
  def testFetchUpToLogEndOffset(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)

    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("0".getBytes),
      new SimpleRecord("1".getBytes),
      new SimpleRecord("2".getBytes)
    )), 0)
    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("3".getBytes),
      new SimpleRecord("4".getBytes)
    )), 0)
    val batchBaseOffsets = SortedSet[Long](0, 3, 5)

    (log.logStartOffset until log.logEndOffset).foreach { offset =>
      val batchBaseOffset = batchBaseOffsets.rangeTo(offset).lastKey
      assertNonEmptyFetch(log, offset, FetchIsolation.LOG_END, batchBaseOffset)
    }
  }

  @Test
  def testFetchUpToHighWatermark(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)

    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("0".getBytes),
      new SimpleRecord("1".getBytes),
      new SimpleRecord("2".getBytes)
    )), 0)
    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("3".getBytes),
      new SimpleRecord("4".getBytes)
    )), 0)
    val batchBaseOffsets = SortedSet[Long](0, 3, 5)

    def assertHighWatermarkBoundedFetches(): Unit = {
      (log.logStartOffset until log.highWatermark).foreach { offset =>
        val batchBaseOffset = batchBaseOffsets.rangeTo(offset).lastKey
        assertNonEmptyFetch(log, offset, FetchIsolation.HIGH_WATERMARK, batchBaseOffset)
      }

      (log.highWatermark to log.logEndOffset).foreach { offset =>
        val batchBaseOffset = batchBaseOffsets.rangeTo(offset).lastKey
        assertEmptyFetch(log, offset, FetchIsolation.HIGH_WATERMARK, batchBaseOffset)
      }
    }

    assertHighWatermarkBoundedFetches()

    log.updateHighWatermark(3L)
    assertHighWatermarkBoundedFetches()

    log.updateHighWatermark(5L)
    assertHighWatermarkBoundedFetches()
  }

  @Test
  def testActiveProducers(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)

    def assertProducerState(
      producerId: Long,
      producerEpoch: Short,
      lastSequence: Int,
      currentTxnStartOffset: Option[Long],
      coordinatorEpoch: Option[Int]
    ): Unit = {
      val producerStateOpt = log.activeProducers.asScala.find(_.producerId == producerId)
      assertTrue(producerStateOpt.isDefined)

      val producerState = producerStateOpt.get
      assertEquals(producerEpoch, producerState.producerEpoch)
      assertEquals(lastSequence, producerState.lastSequence)
      assertEquals(currentTxnStartOffset.getOrElse(-1L), producerState.currentTxnStartOffset)
      assertEquals(coordinatorEpoch.getOrElse(-1), producerState.coordinatorEpoch)
    }

    // Test transactional producer state (open transaction)
    val producer1Epoch = 5.toShort
    val producerId1 = 1L
    LogTestUtils.appendTransactionalAsLeader(log, producerId1, producer1Epoch, mockTime)(5)
    assertProducerState(
      producerId1,
      producer1Epoch,
      lastSequence = 4,
      currentTxnStartOffset = Some(0L),
      coordinatorEpoch = None
    )

    // Test transactional producer state (closed transaction)
    val coordinatorEpoch = 15
    LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId1, producer1Epoch, ControlRecordType.COMMIT,
      mockTime.milliseconds(), coordinatorEpoch, leaderEpoch = 0, transactionVersion = TransactionVersion.TV_0.featureLevel())
    assertProducerState(
      producerId1,
      producer1Epoch,
      lastSequence = 4,
      currentTxnStartOffset = None,
      coordinatorEpoch = Some(coordinatorEpoch)
    )

    // Test idempotent producer state
    val producer2Epoch = 5.toShort
    val producerId2 = 2L
    LogTestUtils.appendIdempotentAsLeader(log, producerId2, producer2Epoch, mockTime)(3)
    assertProducerState(
      producerId2,
      producer2Epoch,
      lastSequence = 2,
      currentTxnStartOffset = None,
      coordinatorEpoch = None
    )
  }

  @Test
  def testFetchUpToLastStableOffset(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)
    val epoch = 0.toShort

    val producerId1 = 1L
    val producerId2 = 2L

    val appendProducer1 = LogTestUtils.appendTransactionalAsLeader(log, producerId1, epoch, mockTime)
    val appendProducer2 = LogTestUtils.appendTransactionalAsLeader(log, producerId2, epoch, mockTime)

    appendProducer1(5)
    LogTestUtils.appendNonTransactionalAsLeader(log, 3)
    appendProducer2(2)
    appendProducer1(4)
    LogTestUtils.appendNonTransactionalAsLeader(log, 2)
    appendProducer1(10)

    val batchBaseOffsets = SortedSet[Long](0, 5, 8, 10, 14, 16, 26, 27, 28)

    def assertLsoBoundedFetches(): Unit = {
      (log.logStartOffset until log.lastStableOffset).foreach { offset =>
        val batchBaseOffset = batchBaseOffsets.rangeTo(offset).lastKey
        assertNonEmptyFetch(log, offset, FetchIsolation.TXN_COMMITTED, batchBaseOffset)
      }

      (log.lastStableOffset to log.logEndOffset).foreach { offset =>
        val batchBaseOffset = batchBaseOffsets.rangeTo(offset).lastKey
        assertEmptyFetch(log, offset, FetchIsolation.TXN_COMMITTED, batchBaseOffset)
      }
    }

    assertLsoBoundedFetches()

    log.updateHighWatermark(log.logEndOffset)
    assertLsoBoundedFetches()

    LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId1, epoch, ControlRecordType.COMMIT, mockTime.milliseconds(),
      transactionVersion = TransactionVersion.TV_0.featureLevel())
    assertEquals(0L, log.lastStableOffset)

    log.updateHighWatermark(log.logEndOffset)
    assertEquals(8L, log.lastStableOffset)
    assertLsoBoundedFetches()

    LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId2, epoch, ControlRecordType.ABORT, mockTime.milliseconds(),
      transactionVersion = TransactionVersion.TV_0.featureLevel())
    assertEquals(8L, log.lastStableOffset)

    log.updateHighWatermark(log.logEndOffset)
    assertEquals(log.logEndOffset, log.lastStableOffset)
    assertLsoBoundedFetches()
  }

  /**
   * Tests for time based log roll. This test appends messages then changes the time
   * using the mock clock to force the log to roll and checks the number of segments.
   */
  @Test
  def testTimeBasedLogRollDuringAppend(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val logConfig = LogTestUtils.createLogConfig(segmentMs = 1 * 60 * 60L)

    // create a log
    val log = createLog(logDir, logConfig, producerStateManagerConfig = new ProducerStateManagerConfig(24 * 60, false))
    assertEquals(1, log.numberOfSegments, "Log begins with a single empty segment.")
    // Test the segment rolling behavior when messages do not have a timestamp.
    mockTime.sleep(log.config.segmentMs + 1)
    log.appendAsLeader(createRecords, 0)
    assertEquals(1, log.numberOfSegments, "Log doesn't roll if doing so creates an empty segment.")

    log.appendAsLeader(createRecords, 0)
    assertEquals(2, log.numberOfSegments, "Log rolls on this append since time has expired.")

    for (numSegments <- 3 until 5) {
      mockTime.sleep(log.config.segmentMs + 1)
      log.appendAsLeader(createRecords, 0)
      assertEquals(numSegments, log.numberOfSegments, "Changing time beyond rollMs and appending should create a new segment.")
    }

    // Append a message with timestamp to a segment whose first message do not have a timestamp.
    val timestamp = mockTime.milliseconds + log.config.segmentMs + 1
    def createRecordsWithTimestamp = TestUtils.singletonRecords(value = "test".getBytes, timestamp = timestamp)
    log.appendAsLeader(createRecordsWithTimestamp, 0)
    assertEquals(4, log.numberOfSegments, "Segment should not have been rolled out because the log rolling should be based on wall clock.")

    // Test the segment rolling behavior when messages have timestamps.
    mockTime.sleep(log.config.segmentMs + 1)
    log.appendAsLeader(createRecordsWithTimestamp, 0)
    assertEquals(5, log.numberOfSegments, "A new segment should have been rolled out")

    // move the wall clock beyond log rolling time
    mockTime.sleep(log.config.segmentMs + 1)
    log.appendAsLeader(createRecordsWithTimestamp, 0)
    assertEquals(5, log.numberOfSegments, "Log should not roll because the roll should depend on timestamp of the first message.")

    val recordWithExpiredTimestamp = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    log.appendAsLeader(recordWithExpiredTimestamp, 0)
    assertEquals(6, log.numberOfSegments, "Log should roll because the timestamp in the message should make the log segment expire.")

    val numSegments = log.numberOfSegments
    mockTime.sleep(log.config.segmentMs + 1)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE), 0)
    assertEquals(numSegments, log.numberOfSegments, "Appending an empty message set should not roll log even if sufficient time has passed.")
  }

  @Test
  def testRollSegmentThatAlreadyExists(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentMs = 1 * 60 * 60L)
    val partitionLeaderEpoch = 0

    // create a log
    val log = createLog(logDir, logConfig)
    assertEquals(1, log.numberOfSegments, "Log begins with a single empty segment.")

    // roll active segment with the same base offset of size zero should recreate the segment
    log.roll(Optional.of(0L))
    assertEquals(1, log.numberOfSegments, "Expect 1 segment after roll() empty segment with base offset.")

    // should be able to append records to active segment
    val records = TestUtils.records(
      List(new SimpleRecord(mockTime.milliseconds, "k1".getBytes, "v1".getBytes)),
      baseOffset = 0L, partitionLeaderEpoch = partitionLeaderEpoch)
    log.appendAsFollower(records, partitionLeaderEpoch)
    assertEquals(1, log.numberOfSegments, "Expect one segment.")
    assertEquals(0L, log.activeSegment.baseOffset)

    // make sure we can append more records
    val records2 = TestUtils.records(
      List(new SimpleRecord(mockTime.milliseconds + 10, "k2".getBytes, "v2".getBytes)),
      baseOffset = 1L, partitionLeaderEpoch = partitionLeaderEpoch)
    log.appendAsFollower(records2, partitionLeaderEpoch)

    assertEquals(2, log.logEndOffset, "Expect two records in the log")
    assertEquals(0, LogTestUtils.readLog(log, 0, 1).records.batches.iterator.next().lastOffset)
    assertEquals(1, LogTestUtils.readLog(log, 1, 1).records.batches.iterator.next().lastOffset)

    // roll so that active segment is empty
    log.roll()
    assertEquals(2L, log.activeSegment.baseOffset, "Expect base offset of active segment to be LEO")
    assertEquals(2, log.numberOfSegments, "Expect two segments.")

    // manually resize offset index to force roll of an empty active segment on next append
    log.activeSegment.offsetIndex.resize(0)
    val records3 = TestUtils.records(
      List(new SimpleRecord(mockTime.milliseconds + 12, "k3".getBytes, "v3".getBytes)),
      baseOffset = 2L, partitionLeaderEpoch = partitionLeaderEpoch)
    log.appendAsFollower(records3, partitionLeaderEpoch)
    assertTrue(log.activeSegment.offsetIndex.maxEntries > 1)
    assertEquals(2, LogTestUtils.readLog(log, 2, 1).records.batches.iterator.next().lastOffset)
    assertEquals(2, log.numberOfSegments, "Expect two segments.")
  }

  @Test
  def testNonSequentialAppend(): Unit = {
    // create a log
    val log = createLog(logDir, new LogConfig(new Properties))
    val pid = 1L
    val epoch: Short = 0

    val records = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)), producerId = pid, producerEpoch = epoch, sequence = 0)
    log.appendAsLeader(records, 0)

    val nextRecords = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)), producerId = pid, producerEpoch = epoch, sequence = 2)
    assertThrows(classOf[OutOfOrderSequenceException], () => log.appendAsLeader(nextRecords, 0))
  }

  @Test
  def testTruncateToEndOffsetClearsEpochCache(): Unit = {
    val log = createLog(logDir, new LogConfig(new Properties))

    // Seed some initial data in the log
    val records = TestUtils.records(List(new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)),
      baseOffset = 27)
    appendAsFollower(log, records, 19)
    assertEquals(Optional.of(new EpochEntry(19, 27)), log.leaderEpochCache.latestEntry)
    assertEquals(29, log.logEndOffset)

    def verifyTruncationClearsEpochCache(epoch: Int, truncationOffset: Long): Unit = {
      // Simulate becoming a leader
      log.assignEpochStartOffset(epoch, log.logEndOffset)
      assertEquals(Optional.of(new EpochEntry(epoch, 29)), log.leaderEpochCache.latestEntry)
      assertEquals(29, log.logEndOffset)

      // Now we become the follower and truncate to an offset greater
      // than or equal to the log end offset. The trivial epoch entry
      // at the end of the log should be gone
      log.truncateTo(truncationOffset)
      assertEquals(Optional.of(new EpochEntry(19, 27)), log.leaderEpochCache.latestEntry)
      assertEquals(29, log.logEndOffset)
    }

    // Truncations greater than or equal to the log end offset should
    // clear the epoch cache
    verifyTruncationClearsEpochCache(epoch = 20, truncationOffset = log.logEndOffset)
    verifyTruncationClearsEpochCache(epoch = 24, truncationOffset = log.logEndOffset + 1)
  }

  /**
   * Test the values returned by the logSegments call
   */
  @Test
  def testLogSegmentsCallCorrect(): Unit = {
    // Create 3 segments and make sure we get the right values from various logSegments calls.
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    def getSegmentOffsets(log :UnifiedLog, from: Long, to: Long) = log.logSegments(from, to).stream().map { _.baseOffset }.toList
    val setSize = createRecords.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * setSize  // each segment will be 10 messages
    // create a log
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = segmentSize)
    val log = createLog(logDir, logConfig)
    assertEquals(1, log.numberOfSegments, "There should be exactly 1 segment.")

    // segments expire in size
    for (_ <- 1 to (2 * msgPerSeg + 2))
      log.appendAsLeader(createRecords, 0)
    assertEquals(3, log.numberOfSegments, "There should be exactly 3 segments.")

    // from == to should always be null
    assertEquals(util.List.of(), getSegmentOffsets(log, 10, 10))
    assertEquals(util.List.of(), getSegmentOffsets(log, 15, 15))

    assertEquals(util.List.of(0L, 10L, 20L), getSegmentOffsets(log, 0, 21))

    assertEquals(util.List.of(0L), getSegmentOffsets(log, 1, 5))
    assertEquals(util.List.of(10L, 20L), getSegmentOffsets(log, 13, 21))
    assertEquals(util.List.of(10L), getSegmentOffsets(log, 13, 17))

    // from < to is bad
    assertThrows(classOf[IllegalArgumentException], () => log.logSegments(10, 0))
  }

  @Test
  def testInitializationOfProducerSnapshotsUpgradePath(): Unit = {
    // simulate the upgrade path by creating a new log with several segments, deleting the
    // snapshot files, and then reloading the log
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 64 * 10)
    var log = createLog(logDir, logConfig)
    assertEquals(OptionalLong.empty(), log.oldestProducerSnapshotOffset)

    for (i <- 0 to 100) {
      val record = new SimpleRecord(mockTime.milliseconds, i.toString.getBytes)
      log.appendAsLeader(TestUtils.records(List(record)), 0)
    }
    assertTrue(log.logSegments.size >= 2)
    val logEndOffset = log.logEndOffset
    log.close()

    LogTestUtils.deleteProducerSnapshotFiles(logDir)

    // Reload after clean shutdown
    log = createLog(logDir, logConfig, recoveryPoint = logEndOffset)
    var expectedSnapshotOffsets = log.logSegments.asScala.map(_.baseOffset).takeRight(2).toVector :+ log.logEndOffset
    assertEquals(expectedSnapshotOffsets, LogTestUtils.listProducerSnapshotOffsets(logDir))
    log.close()

    LogTestUtils.deleteProducerSnapshotFiles(logDir)

    // Reload after unclean shutdown with recoveryPoint set to log end offset
    log = createLog(logDir, logConfig, recoveryPoint = logEndOffset, lastShutdownClean = false)
    assertEquals(expectedSnapshotOffsets, LogTestUtils.listProducerSnapshotOffsets(logDir))
    log.close()

    LogTestUtils.deleteProducerSnapshotFiles(logDir)

    // Reload after unclean shutdown with recoveryPoint set to 0
    log = createLog(logDir, logConfig, recoveryPoint = 0L, lastShutdownClean = false)
    // We progressively create a snapshot for each segment after the recovery point
    expectedSnapshotOffsets = log.logSegments.asScala.map(_.baseOffset).tail.toVector :+ log.logEndOffset
    assertEquals(expectedSnapshotOffsets, LogTestUtils.listProducerSnapshotOffsets(logDir))
    log.close()
  }

  @Test
  def testLogReinitializeAfterManualDelete(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    // simulate a case where log data does not exist but the start offset is non-zero
    val log = createLog(logDir, logConfig, logStartOffset = 500)
    assertEquals(500, log.logStartOffset)
    assertEquals(500, log.logEndOffset)
  }

  /**
   * Test that "PeriodicProducerExpirationCheck" scheduled task gets canceled after log
   * is deleted.
   */
  @Test
  def testProducerExpireCheckAfterDelete(): Unit = {
    val scheduler = new KafkaScheduler(1)
    try {
      scheduler.startup()
      val logConfig = LogTestUtils.createLogConfig()
      val log = createLog(logDir, logConfig, scheduler = scheduler)

      val producerExpireCheck = log.producerExpireCheck
      assertTrue(scheduler.taskRunning(producerExpireCheck), "producerExpireCheck isn't as part of scheduled tasks")

      log.delete()
      assertFalse(scheduler.taskRunning(producerExpireCheck),
        "producerExpireCheck is part of scheduled tasks even after log deletion")
    } finally {
      scheduler.shutdown()
    }
  }

  @Test
  def testProducerIdMapOffsetUpdatedForNonIdempotentData(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val records = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)))
    log.appendAsLeader(records, 0)
    log.takeProducerSnapshot()
    assertEquals(OptionalLong.of(1), log.latestProducerSnapshotOffset)
  }

  @Test
  def testRebuildProducerIdMapWithCompactedData(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid = 1L
    val producerEpoch = 0.toShort
    val partitionLeaderEpoch = 0
    val seq = 0
    val baseOffset = 23L

    // create a batch with a couple gaps to simulate compaction
    val records = TestUtils.records(
      producerId = pid,
      producerEpoch = producerEpoch,
      sequence = seq,
      baseOffset = baseOffset,
      records = List(
        new SimpleRecord(mockTime.milliseconds(), "a".getBytes),
        new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "b".getBytes),
        new SimpleRecord(mockTime.milliseconds(), "c".getBytes),
        new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "d".getBytes)
      )
    )
    records.batches.forEach(_.setPartitionLeaderEpoch(partitionLeaderEpoch))

    val filtered = ByteBuffer.allocate(2048)
    records.filterTo(new RecordFilter(0, 0) {
      override def checkBatchRetention(batch: RecordBatch): RecordFilter.BatchRetentionResult =
        new RecordFilter.BatchRetentionResult(RecordFilter.BatchRetention.DELETE_EMPTY, false)
      override def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = !record.hasKey
    }, filtered, BufferSupplier.NO_CACHING)
    filtered.flip()
    val filteredRecords = MemoryRecords.readableRecords(filtered)

    log.appendAsFollower(filteredRecords, partitionLeaderEpoch)

    // append some more data and then truncate to force rebuilding of the PID map
    val moreRecords = TestUtils.records(
      baseOffset = baseOffset + 4,
      records = List(
        new SimpleRecord(mockTime.milliseconds(), "e".getBytes),
        new SimpleRecord(mockTime.milliseconds(), "f".getBytes)
      )
    )
    moreRecords.batches.forEach(_.setPartitionLeaderEpoch(partitionLeaderEpoch))
    log.appendAsFollower(moreRecords, partitionLeaderEpoch)

    log.truncateTo(baseOffset + 4)

    val activeProducers = log.activeProducersWithLastSequence
    assertTrue(activeProducers.containsKey(pid))

    val lastSeq = activeProducers.get(pid)
    assertEquals(3, lastSeq)
  }

  @Test
  def testRebuildProducerStateWithEmptyCompactedBatch(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid = 1L
    val producerEpoch = 0.toShort
    val partitionLeaderEpoch = 0
    val seq = 0
    val baseOffset = 23L

    // create an empty batch
    val records = TestUtils.records(
      producerId = pid,
      producerEpoch = producerEpoch,
      sequence = seq,
      baseOffset = baseOffset,
      records = List(
        new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "a".getBytes),
        new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "b".getBytes)
      )
    )
    records.batches.forEach(_.setPartitionLeaderEpoch(partitionLeaderEpoch))

    val filtered = ByteBuffer.allocate(2048)
    records.filterTo(new RecordFilter(0, 0) {
      override def checkBatchRetention(batch: RecordBatch): RecordFilter.BatchRetentionResult =
        new RecordFilter.BatchRetentionResult(RecordFilter.BatchRetention.RETAIN_EMPTY, true)
      override def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = false
    }, filtered, BufferSupplier.NO_CACHING)
    filtered.flip()
    val filteredRecords = MemoryRecords.readableRecords(filtered)

    log.appendAsFollower(filteredRecords, partitionLeaderEpoch)

    // append some more data and then truncate to force rebuilding of the PID map
    val moreRecords = TestUtils.records(
      baseOffset = baseOffset + 2,
      records = List(
        new SimpleRecord(mockTime.milliseconds(), "e".getBytes),
        new SimpleRecord(mockTime.milliseconds(), "f".getBytes)
      )
    )
    moreRecords.batches.forEach(_.setPartitionLeaderEpoch(partitionLeaderEpoch))
    log.appendAsFollower(moreRecords, partitionLeaderEpoch)

    log.truncateTo(baseOffset + 2)

    val activeProducers = log.activeProducersWithLastSequence
    assertTrue(activeProducers.containsKey(pid))

    val lastSeq = activeProducers.get(pid)
    assertEquals(1, lastSeq)
  }

  @Test
  def testUpdateProducerIdMapWithCompactedData(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid = 1L
    val producerEpoch = 0.toShort
    val partitionLeaderEpoch = 0
    val seq = 0
    val baseOffset = 23L

    // create a batch with a couple gaps to simulate compaction
    val records = TestUtils.records(
      producerId = pid,
      producerEpoch = producerEpoch,
      sequence = seq,
      baseOffset = baseOffset,
      records = List(
        new SimpleRecord(mockTime.milliseconds(), "a".getBytes),
        new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "b".getBytes),
        new SimpleRecord(mockTime.milliseconds(), "c".getBytes),
        new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "d".getBytes)
      )
    )
    records.batches.forEach(_.setPartitionLeaderEpoch(partitionLeaderEpoch))

    val filtered = ByteBuffer.allocate(2048)
    records.filterTo(new RecordFilter(0, 0) {
      override def checkBatchRetention(batch: RecordBatch): RecordFilter.BatchRetentionResult =
        new RecordFilter.BatchRetentionResult(RecordFilter.BatchRetention.DELETE_EMPTY, false)
      override def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = !record.hasKey
    }, filtered, BufferSupplier.NO_CACHING)
    filtered.flip()
    val filteredRecords = MemoryRecords.readableRecords(filtered)

    log.appendAsFollower(filteredRecords, partitionLeaderEpoch)
    val activeProducers = log.activeProducersWithLastSequence
    assertTrue(activeProducers.containsKey(pid))

    val lastSeq = activeProducers.get(pid)
    assertEquals(3, lastSeq)
  }

  @Test
  def testProducerIdMapTruncateTo(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes))), 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes))), 0)
    log.takeProducerSnapshot()

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes))), 0)
    log.takeProducerSnapshot()

    log.truncateTo(2)
    assertEquals(OptionalLong.of(2), log.latestProducerSnapshotOffset)
    assertEquals(2, log.latestProducerStateEndOffset)

    log.truncateTo(1)
    assertEquals(OptionalLong.of(1), log.latestProducerSnapshotOffset)
    assertEquals(1, log.latestProducerStateEndOffset)

    log.truncateTo(0)
    assertEquals(OptionalLong.empty(), log.latestProducerSnapshotOffset)
    assertEquals(0, log.latestProducerStateEndOffset)
  }

  @Test
  def testProducerIdMapTruncateToWithNoSnapshots(): Unit = {
    // This ensures that the upgrade optimization path cannot be hit after initial loading
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid = 1L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes)), producerId = pid,
      producerEpoch = epoch, sequence = 0), 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)), producerId = pid,
      producerEpoch = epoch, sequence = 1), 0)

    LogTestUtils.deleteProducerSnapshotFiles(logDir)

    log.truncateTo(1L)
    assertEquals(1, log.activeProducersWithLastSequence.size)

    val lastSeq = log.activeProducersWithLastSequence.get(pid)
    assertEquals(0, lastSeq)
  }

  @ParameterizedTest(name = "testRetentionDeletesProducerStateSnapshots with createEmptyActiveSegment: {0}")
  @ValueSource(booleans = Array(true, false))
  def testRetentionDeletesProducerStateSnapshots(createEmptyActiveSegment: Boolean): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5, retentionBytes = 0, retentionMs = 1000 * 60, fileDeleteDelayMs = 0)
    val log = createLog(logDir, logConfig)
    val pid1 = 1L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 0), 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 1), 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 2), 0)
    if (createEmptyActiveSegment) {
      log.roll()
    }

    log.updateHighWatermark(log.logEndOffset)

    val numProducerSnapshots = if (createEmptyActiveSegment) 3 else 2
    assertEquals(numProducerSnapshots, ProducerStateManager.listSnapshotFiles(logDir).size)
    // Sleep to breach the retention period
    mockTime.sleep(1000 * 60 + 1)
    log.deleteOldSegments()
    // Sleep to breach the file delete delay and run scheduled file deletion tasks
    mockTime.sleep(1)
    assertEquals(1, ProducerStateManager.listSnapshotFiles(logDir).size,
      "expect a single producer state snapshot remaining")
    assertEquals(3, log.logStartOffset)
  }

  @Test
  def testRetentionIdempotency(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5, retentionBytes = -1, retentionMs = 900, fileDeleteDelayMs = 0)
    val log = createLog(logDir, logConfig)

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds() + 100, "a".getBytes))), 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "b".getBytes))), 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds() + 100, "c".getBytes))), 0)

    mockTime.sleep(901)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(1L, LogStartOffsetIncrementReason.ClientRecordDeletion)
    assertEquals(2, log.deleteOldSegments(),
      "Expecting two segment deletions as log start offset retention should unblock time based retention")
    assertEquals(0, log.deleteOldSegments())
  }


  @Test
  def testLogStartOffsetMovementDeletesSnapshots(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5, retentionBytes = -1, fileDeleteDelayMs = 0)
    val log = createLog(logDir, logConfig)
    val pid1 = 1L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 0), 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 1), 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 2), 0)
    log.updateHighWatermark(log.logEndOffset)
    assertEquals(2, ProducerStateManager.listSnapshotFiles(logDir).size)

    // Increment the log start offset to exclude the first two segments.
    log.maybeIncrementLogStartOffset(log.logEndOffset - 1, LogStartOffsetIncrementReason.ClientRecordDeletion)
    log.deleteOldSegments()
    // Sleep to breach the file delete delay and run scheduled file deletion tasks
    mockTime.sleep(1)
    assertEquals(1, ProducerStateManager.listSnapshotFiles(logDir).size,
      "expect a single producer state snapshot remaining")
  }

  @Test
  def testCompactionDeletesProducerStateSnapshots(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5, cleanupPolicy = TopicConfig.CLEANUP_POLICY_COMPACT, fileDeleteDelayMs = 0)
    val log = createLog(logDir, logConfig)
    val pid1 = 1L
    val epoch = 0.toShort
    val cleaner = new Cleaner(0,
      new FakeOffsetMap(Int.MaxValue),
      64 * 1024,
      64 * 1024,
      0.75,
      new Throttler(Double.MaxValue, Long.MaxValue, "throttler", "entries", mockTime),
      mockTime,
      tp => {})

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes, "a".getBytes())), producerId = pid1,
      producerEpoch = epoch, sequence = 0), 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes, "b".getBytes())), producerId = pid1,
      producerEpoch = epoch, sequence = 1), 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes, "c".getBytes())), producerId = pid1,
      producerEpoch = epoch, sequence = 2), 0)
    log.updateHighWatermark(log.logEndOffset)
    assertEquals(log.logSegments.asScala.map(_.baseOffset).toSeq.sorted.drop(1), ProducerStateManager.listSnapshotFiles(logDir).asScala.map(_.offset).sorted,
      "expected a snapshot file per segment base offset, except the first segment")
    assertEquals(2, ProducerStateManager.listSnapshotFiles(logDir).size)

    // Clean segments, this should delete everything except the active segment since there only
    // exists the key "a".
    cleaner.clean(new LogToClean(log, 0, log.logEndOffset, false))
    log.deleteOldSegments()
    // Sleep to breach the file delete delay and run scheduled file deletion tasks
    mockTime.sleep(1)
    assertEquals(log.logSegments.asScala.map(_.baseOffset).toSeq.sorted.drop(1), ProducerStateManager.listSnapshotFiles(logDir).asScala.map(_.offset).sorted,
      "expected a snapshot file per segment base offset, excluding the first")
  }

  /**
   * After loading the log, producer state is truncated such that there are no producer state snapshot files which
   * exceed the log end offset. This test verifies that these are removed.
   */
  @Test
  def testLoadingLogDeletesProducerStateSnapshotsPastLogEndOffset(): Unit = {
    val straySnapshotFile = LogFileUtils.producerSnapshotFile(logDir, 42).toPath
    Files.createFile(straySnapshotFile)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5, retentionBytes = -1, fileDeleteDelayMs = 0)
    createLog(logDir, logConfig)
    assertEquals(0, ProducerStateManager.listSnapshotFiles(logDir).size,
      "expected producer state snapshots greater than the log end offset to be cleaned up")
  }

  @Test
  def testProducerIdMapTruncateFullyAndStartAt(): Unit = {
    val records = TestUtils.singletonRecords("foo".getBytes)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = records.sizeInBytes, retentionBytes = records.sizeInBytes * 2)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(records, 0)
    log.takeProducerSnapshot()

    log.appendAsLeader(TestUtils.singletonRecords("bar".getBytes), 0)
    log.appendAsLeader(TestUtils.singletonRecords("baz".getBytes), 0)
    log.takeProducerSnapshot()

    assertEquals(3, log.logSegments.size)
    assertEquals(3, log.latestProducerStateEndOffset)
    assertEquals(OptionalLong.of(3), log.latestProducerSnapshotOffset)

    log.truncateFullyAndStartAt(29, Optional.empty)
    assertEquals(1, log.logSegments.size)
    assertEquals(OptionalLong.empty(), log.latestProducerSnapshotOffset)
    assertEquals(29, log.latestProducerStateEndOffset)
  }

  @Test
  def testProducerIdExpirationOnSegmentDeletion(): Unit = {
    val pid1 = 1L
    val records = TestUtils.records(Seq(new SimpleRecord("foo".getBytes)), producerId = pid1, producerEpoch = 0, sequence = 0)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = records.sizeInBytes, retentionBytes = records.sizeInBytes * 2)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(records, 0)
    log.takeProducerSnapshot()

    val pid2 = 2L
    log.appendAsLeader(TestUtils.records(Seq(new SimpleRecord("bar".getBytes)), producerId = pid2, producerEpoch = 0, sequence = 0),
      0)
    log.appendAsLeader(TestUtils.records(Seq(new SimpleRecord("baz".getBytes)), producerId = pid2, producerEpoch = 0, sequence = 1),
      0)
    log.takeProducerSnapshot()

    assertEquals(3, log.logSegments.size)
    assertEquals(util.Set.of(pid1, pid2), log.activeProducersWithLastSequence.keySet)

    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()

    // Producer state should not be removed when deleting log segment
    assertEquals(2, log.logSegments.size)
    assertEquals(util.Set.of(pid1, pid2), log.activeProducersWithLastSequence.keySet)
  }

  @Test
  def testTakeSnapshotOnRollAndDeleteSnapshotOnRecoveryPointCheckpoint(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(TestUtils.singletonRecords("a".getBytes), 0)
    log.roll(Optional.of(1L))
    assertEquals(OptionalLong.of(1L), log.latestProducerSnapshotOffset)
    assertEquals(OptionalLong.of(1L), log.oldestProducerSnapshotOffset)

    log.appendAsLeader(TestUtils.singletonRecords("b".getBytes), 0)
    log.roll(Optional.of(2L))
    assertEquals(OptionalLong.of(2L), log.latestProducerSnapshotOffset)
    assertEquals(OptionalLong.of(1L), log.oldestProducerSnapshotOffset)

    log.appendAsLeader(TestUtils.singletonRecords("c".getBytes), 0)
    log.roll(Optional.of(3L))
    assertEquals(OptionalLong.of(3L), log.latestProducerSnapshotOffset)

    // roll triggers a flush at the starting offset of the new segment, we should retain all snapshots
    assertEquals(OptionalLong.of(1L), log.oldestProducerSnapshotOffset)

    // even if we flush within the active segment, the snapshot should remain
    log.appendAsLeader(TestUtils.singletonRecords("baz".getBytes), 0)
    log.flushUptoOffsetExclusive(4L)
    assertEquals(OptionalLong.of(3L), log.latestProducerSnapshotOffset)
  }

  @Test
  def testProducerSnapshotAfterSegmentRollOnAppend(): Unit = {
    val producerId = 1L
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024)
    val log = createLog(logDir, logConfig)

    log.appendAsLeader(TestUtils.records(Seq(new SimpleRecord(mockTime.milliseconds(), new Array[Byte](512))),
      producerId = producerId, producerEpoch = 0, sequence = 0),
      0)

    // The next append should overflow the segment and cause it to roll
    log.appendAsLeader(TestUtils.records(Seq(new SimpleRecord(mockTime.milliseconds(), new Array[Byte](512))),
      producerId = producerId, producerEpoch = 0, sequence = 1),
      0)

    assertEquals(2, log.logSegments.size)
    assertEquals(1L, log.activeSegment.baseOffset)
    assertEquals(OptionalLong.of(1L), log.latestProducerSnapshotOffset)

    // Force a reload from the snapshot to check its consistency
    log.truncateTo(1L)

    assertEquals(2, log.logSegments.size)
    assertEquals(1L, log.activeSegment.baseOffset)
    assertTrue(log.activeSegment.log.batches.asScala.isEmpty)
    assertEquals(OptionalLong.of(1L), log.latestProducerSnapshotOffset)

    val lastEntry = log.producerStateManager.lastEntry(producerId)
    assertTrue(lastEntry.isPresent)
    assertEquals(0L, lastEntry.get.firstDataOffset)
    assertEquals(0L, lastEntry.get.lastDataOffset)
  }

  @Test
  def testRebuildTransactionalState(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    val pid = 137L
    val epoch = 5.toShort
    val seq = 0

    // add some transactional records
    val records = MemoryRecords.withTransactionalRecords(Compression.NONE, pid, epoch, seq,
      new SimpleRecord("foo".getBytes),
      new SimpleRecord("bar".getBytes),
      new SimpleRecord("baz".getBytes))
    log.appendAsLeader(records, 0)
    val abortAppendInfo = LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT,
      mockTime.milliseconds(), transactionVersion = TransactionVersion.TV_0.featureLevel())
    log.updateHighWatermark(abortAppendInfo.lastOffset + 1)

    // now there should be no first unstable offset
    assertEquals(Optional.empty, log.firstUnstableOffset)

    log.close()

    val reopenedLog = createLog(logDir, logConfig, lastShutdownClean = false)
    reopenedLog.updateHighWatermark(abortAppendInfo.lastOffset + 1)
    assertEquals(Optional.empty, reopenedLog.firstUnstableOffset)
  }

  @Test
  def testPeriodicProducerIdExpiration(): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(200, false)
    val producerIdExpirationCheckIntervalMs = 100

    val pid = 23L
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig,
      producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs)
    val records = Seq(new SimpleRecord(mockTime.milliseconds(), "foo".getBytes))
    log.appendAsLeader(TestUtils.records(records, producerId = pid, producerEpoch = 0, sequence = 0), 0)

    assertEquals(util.Set.of(pid), log.activeProducersWithLastSequence.keySet)

    mockTime.sleep(producerIdExpirationCheckIntervalMs)
    assertEquals(util.Set.of(pid), log.activeProducersWithLastSequence.keySet)

    mockTime.sleep(producerIdExpirationCheckIntervalMs)
    assertEquals(util.Set.of(), log.activeProducersWithLastSequence.keySet)
  }

  @Test
  def testDuplicateAppends(): Unit = {
    // create a log
    val log = createLog(logDir, new LogConfig(new Properties))
    val pid = 1L
    val epoch: Short = 0

    var seq = 0
    // Pad the beginning of the log.
    for (_ <- 0 to 5) {
      val record = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)),
        producerId = pid, producerEpoch = epoch, sequence = seq)
      log.appendAsLeader(record, 0)
      seq = seq + 1
    }
    // Append an entry with multiple log records.
    def createRecords = TestUtils.records(List(
      new SimpleRecord(mockTime.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes),
      new SimpleRecord(mockTime.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes),
      new SimpleRecord(mockTime.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes)
    ), producerId = pid, producerEpoch = epoch, sequence = seq)
    val multiEntryAppendInfo = log.appendAsLeader(createRecords, 0)
    assertEquals(
      multiEntryAppendInfo.lastOffset - multiEntryAppendInfo.firstOffset + 1,
      3,
      "should have appended 3 entries"
    )

    // Append a Duplicate of the tail, when the entry at the tail has multiple records.
    val dupMultiEntryAppendInfo = log.appendAsLeader(createRecords, 0)
    assertEquals(
      multiEntryAppendInfo.firstOffset,
      dupMultiEntryAppendInfo.firstOffset,
      "Somehow appended a duplicate entry with multiple log records to the tail"
    )
    assertEquals(multiEntryAppendInfo.lastOffset, dupMultiEntryAppendInfo.lastOffset,
      "Somehow appended a duplicate entry with multiple log records to the tail")

    seq = seq + 3

    // Append a partial duplicate of the tail. This is not allowed.
    var records = TestUtils.records(
      List(
        new SimpleRecord(mockTime.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes),
        new SimpleRecord(mockTime.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = seq - 2)
    assertThrows(classOf[OutOfOrderSequenceException], () => log.appendAsLeader(records, 0),
      () => "Should have received an OutOfOrderSequenceException since we attempted to append a duplicate of a records in the middle of the log.")

    // Append a duplicate of the batch which is 4th from the tail. This should succeed without error since we
    // retain the batch metadata of the last 5 batches.
    val duplicateOfFourth = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 2)
    log.appendAsLeader(duplicateOfFourth, 0)

    // Duplicates at older entries are reported as OutOfOrderSequence errors
    records = TestUtils.records(
      List(new SimpleRecord(mockTime.milliseconds, s"key-1".getBytes, s"value-1".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 1)
    assertThrows(classOf[OutOfOrderSequenceException], () => log.appendAsLeader(records, 0),
      () => "Should have received an OutOfOrderSequenceException since we attempted to append a duplicate of a batch which is older than the last 5 appended batches.")

    // Append a duplicate entry with a single records at the tail of the log. This should return the appendInfo of the original entry.
    def createRecordsWithDuplicate = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = seq)
    val origAppendInfo = log.appendAsLeader(createRecordsWithDuplicate, 0)
    val newAppendInfo = log.appendAsLeader(createRecordsWithDuplicate, 0)
    assertEquals(
      origAppendInfo.firstOffset,
      newAppendInfo.firstOffset,
      "Inserted a duplicate records into the log"
    )
    assertEquals(origAppendInfo.lastOffset, newAppendInfo.lastOffset,
      "Inserted a duplicate records into the log")
  }

  @Test
  def testMultipleProducerIdsPerMemoryRecord(): Unit = {
    // create a log
    val log = createLog(logDir, new LogConfig(new Properties))

    val producerEpoch: Short = 0
    val partitionLeaderEpoch = 0
    val buffer = ByteBuffer.allocate(512)

    var builder = MemoryRecords.builder(
      buffer, RecordBatch.MAGIC_VALUE_V2, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 0L, mockTime.milliseconds(), 1L, producerEpoch, 0, false,
      partitionLeaderEpoch
    )
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 1L, mockTime.milliseconds(), 2L, producerEpoch, 0, false,
      partitionLeaderEpoch)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    builder = MemoryRecords.builder(
      buffer, RecordBatch.MAGIC_VALUE_V2, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 2L, mockTime.milliseconds(), 3L, producerEpoch, 0, false,
      partitionLeaderEpoch
    )
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    builder = MemoryRecords.builder(
      buffer, RecordBatch.MAGIC_VALUE_V2, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 3L, mockTime.milliseconds(), 4L, producerEpoch, 0, false,
      partitionLeaderEpoch
    )
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    buffer.flip()
    val memoryRecords = MemoryRecords.readableRecords(buffer)

    log.appendAsFollower(memoryRecords, partitionLeaderEpoch)
    log.flush(false)

    val fetchedData = LogTestUtils.readLog(log, 0, Int.MaxValue)

    val origIterator = memoryRecords.batches.iterator()
    for (batch <- fetchedData.records.batches.asScala) {
      assertTrue(origIterator.hasNext)
      val origEntry = origIterator.next()
      assertEquals(origEntry.producerId, batch.producerId)
      assertEquals(origEntry.baseOffset, batch.baseOffset)
      assertEquals(origEntry.baseSequence, batch.baseSequence)
    }
  }

  @Test
  def testDuplicateAppendToFollower(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)
    val producerEpoch: Short = 0
    val pid = 1L
    val baseSequence = 0
    val partitionLeaderEpoch = 0
    // The point of this test is to ensure that validation isn't performed on the follower.
    // this is a bit contrived. to trigger the duplicate case for a follower append, we have to append
    // a batch with matching sequence numbers, but valid increasing offsets
    assertEquals(0L, log.logEndOffset)
    log.appendAsFollower(
      MemoryRecords.withIdempotentRecords(
        0L,
        Compression.NONE,
        pid,
        producerEpoch,
        baseSequence,
        partitionLeaderEpoch,
        new SimpleRecord("a".getBytes),
        new SimpleRecord("b".getBytes)
      ),
      partitionLeaderEpoch
    )
    log.appendAsFollower(
      MemoryRecords.withIdempotentRecords(
        2L,
        Compression.NONE,
        pid,
        producerEpoch,
        baseSequence,
        partitionLeaderEpoch,
        new SimpleRecord("a".getBytes),
        new SimpleRecord("b".getBytes)
      ),
      partitionLeaderEpoch
    )

    // Ensure that even the duplicate sequences are accepted on the follower.
    assertEquals(4L, log.logEndOffset)
  }

  @Test
  def testMultipleProducersWithDuplicatesInSingleAppend(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    val pid1 = 1L
    val pid2 = 2L
    val producerEpoch: Short = 0

    val buffer = ByteBuffer.allocate(512)

    // pid1 seq = 0
    var builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 0L, mockTime.milliseconds(), pid1, producerEpoch, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // pid2 seq = 0
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 1L, mockTime.milliseconds(), pid2, producerEpoch, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // pid1 seq = 1
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 2L, mockTime.milliseconds(), pid1, producerEpoch, 1)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // pid2 seq = 1
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 3L, mockTime.milliseconds(), pid2, producerEpoch, 1)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // // pid1 seq = 1 (duplicate)
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 4L, mockTime.milliseconds(), pid1, producerEpoch, 1)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    buffer.flip()

    val epoch = 0
    val records = MemoryRecords.readableRecords(buffer)
    records.batches.forEach(_.setPartitionLeaderEpoch(epoch))

    // Ensure that batches with duplicates are accepted on the follower.
    assertEquals(0L, log.logEndOffset)
    log.appendAsFollower(records, epoch)
    assertEquals(5L, log.logEndOffset)
  }

  @Test
  def testOldProducerEpoch(): Unit = {
    // create a log
    val log = createLog(logDir, new LogConfig(new Properties))
    val pid = 1L
    val newEpoch: Short = 1
    val oldEpoch: Short = 0

    val records = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)), producerId = pid, producerEpoch = newEpoch, sequence = 0)
    log.appendAsLeader(records, 0)

    val nextRecords = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)), producerId = pid, producerEpoch = oldEpoch, sequence = 0)
    assertThrows(classOf[InvalidProducerEpochException], () => log.appendAsLeader(nextRecords, 0))
  }

  @Test
  def testDeleteSnapshotsOnIncrementLogStartOffset(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid1 = 1L
    val pid2 = 2L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 0), 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "b".getBytes)), producerId = pid2,
      producerEpoch = epoch, sequence = 0), 0)
    log.roll()

    assertEquals(2, log.activeProducersWithLastSequence.size)
    assertEquals(2, ProducerStateManager.listSnapshotFiles(log.dir).size)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(2L, LogStartOffsetIncrementReason.ClientRecordDeletion)
    log.deleteOldSegments() // force retention to kick in so that the snapshot files are cleaned up.
    mockTime.sleep(logConfig.fileDeleteDelayMs + 1000) // advance the clock so file deletion takes place

    // Deleting records should not remove producer state but should delete snapshots after the file deletion delay.
    assertEquals(2, log.activeProducersWithLastSequence.size)
    assertEquals(1, ProducerStateManager.listSnapshotFiles(log.dir).size)
    val retainedLastSeq = log.activeProducersWithLastSequence.get(pid2)
    assertEquals(0, retainedLastSeq)
  }

  /**
   * Test for jitter s for time based log roll. This test appends messages then changes the time
   * using the mock clock to force the log to roll and checks the number of segments.
   */
  @Test
  def testTimeBasedLogRollJitter(): Unit = {
    var set = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    val maxJitter = 20 * 60L
    // create a log
    val logConfig = LogTestUtils.createLogConfig(segmentMs = 1 * 60 * 60L, segmentJitterMs = maxJitter)
    val log = createLog(logDir, logConfig)
    assertEquals(1, log.numberOfSegments, "Log begins with a single empty segment.")
    log.appendAsLeader(set, 0)

    mockTime.sleep(log.config.segmentMs - maxJitter)
    set = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    log.appendAsLeader(set, 0)
    assertEquals(1, log.numberOfSegments,
      "Log does not roll on this append because it occurs earlier than max jitter")
    mockTime.sleep(maxJitter - log.activeSegment.rollJitterMs + 1)
    set = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    log.appendAsLeader(set, 0)
    assertEquals(2, log.numberOfSegments,
      "Log should roll after segmentMs adjusted by random jitter")
  }

  /**
   * Test that appending more than the maximum segment size rolls the log
   */
  @Test
  def testSizeBasedLogRoll(): Unit = {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    val setSize = createRecords.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * (setSize - 1) // each segment will be 10 messages
    // create a log
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = segmentSize)
    val log = createLog(logDir, logConfig)
    assertEquals(1, log.numberOfSegments, "There should be exactly 1 segment.")

    // segments expire in size
    for (_ <- 1 to (msgPerSeg + 1))
      log.appendAsLeader(createRecords, 0)
    assertEquals(2, log.numberOfSegments,
      "There should be exactly 2 segments.")
  }

  /**
   * Test that we can open and append to an empty log
   */
  @Test
  def testLoadEmptyLog(): Unit = {
    createEmptyLogs(logDir, 0)
    val log = createLog(logDir, new LogConfig(new Properties))
    log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds), 0)
  }

  /**
   * This test case appends a bunch of messages and checks that we can read them all back using sequential offsets.
   */
  @Test
  def testAppendAndReadWithSequentialOffsets(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 71)
    val log = createLog(logDir, logConfig)
    val values = (0 until 100 by 2).map(id => id.toString.getBytes).toArray

    for (value <- values)
      log.appendAsLeader(TestUtils.singletonRecords(value = value), 0)

    for (i <- values.indices) {
      val read = LogTestUtils.readLog(log, i, 1).records.batches.iterator.next()
      assertEquals(i, read.lastOffset, "Offset read should match order appended.")
      val actual = read.iterator.next()
      assertNull(actual.key, "Key should be null")
      assertEquals(ByteBuffer.wrap(values(i)), actual.value, "Values not equal")
    }
    assertEquals(0, LogTestUtils.readLog(log, values.length, 100).records.batches.asScala.size,
      "Reading beyond the last message returns nothing.")
  }

  /**
   * This test appends a bunch of messages with non-sequential offsets and checks that we can an the correct message
   * from any offset less than the logEndOffset including offsets not appended.
   */
  @Test
  def testAppendAndReadWithNonSequentialOffsets(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 72)
    val log = createLog(logDir, logConfig)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val records = messageIds.map(id => new SimpleRecord(id.toString.getBytes))

    // now test the case that we give the offsets and use non-sequential offsets
    for (i <- records.indices) {
      log.appendAsFollower(
        MemoryRecords.withRecords(messageIds(i), Compression.NONE, 0, records(i)),
        Int.MaxValue
      )
    }
    for (i <- 50 until messageIds.max) {
      val idx = messageIds.indexWhere(_ >= i)
      val read = LogTestUtils.readLog(log, i, 100).records.records.iterator.next()
      assertEquals(messageIds(idx), read.offset, "Offset read should match message id.")
      assertEquals(records(idx), new SimpleRecord(read), "Message should match appended.")
    }
  }

  /**
   * This test covers an odd case where we have a gap in the offsets that falls at the end of a log segment.
   * Specifically we create a log where the last message in the first segment has offset 0. If we
   * then read offset 1, we should expect this read to come from the second segment, even though the
   * first segment has the greatest lower bound on the offset.
   */
  @Test
  def testReadAtLogGap(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 300)
    val log = createLog(logDir, logConfig)

    // keep appending until we have two segments with only a single message in the second segment
    while (log.numberOfSegments == 1)
      log.appendAsLeader(TestUtils.singletonRecords(value = "42".getBytes), 0)

    // now manually truncate off all but one message from the first segment to create a gap in the messages
    log.logSegments.asScala.head.truncateTo(1)

    assertEquals(log.logEndOffset - 1, LogTestUtils.readLog(log, 1, 200).records.batches.iterator.next().lastOffset,
      "A read should now return the last message in the log")
  }

  @Test
  def testLogRollAfterLogHandlerClosed(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    val log = createLog(logDir,  logConfig)
    log.closeHandlers()
    assertThrows(classOf[KafkaStorageException], () => log.roll(Optional.of(1L)))
  }

  @Test
  def testReadWithMinMessage(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 72)
    val log = createLog(logDir,  logConfig)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val records = messageIds.map(id => new SimpleRecord(id.toString.getBytes))

    // now test the case that we give the offsets and use non-sequential offsets
    for (i <- records.indices) {
      log.appendAsFollower(
        MemoryRecords.withRecords(messageIds(i), Compression.NONE, 0, records(i)),
        Int.MaxValue
      )
    }

    for (i <- 50 until messageIds.max) {
      val idx = messageIds.indexWhere(_ >= i)
      val reads = Seq(
        LogTestUtils.readLog(log, i, 1),
        LogTestUtils.readLog(log, i, 100000),
        LogTestUtils.readLog(log, i, 100)
      ).map(_.records.records.iterator.next())
      reads.foreach { read =>
        assertEquals(messageIds(idx), read.offset, "Offset read should match message id.")
        assertEquals(records(idx), new SimpleRecord(read), "Message should match appended.")
      }
    }
  }

  @Test
  def testReadWithTooSmallMaxLength(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 72)
    val log = createLog(logDir,  logConfig)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val records = messageIds.map(id => new SimpleRecord(id.toString.getBytes))

    // now test the case that we give the offsets and use non-sequential offsets
    for (i <- records.indices) {
      log.appendAsFollower(
        MemoryRecords.withRecords(messageIds(i), Compression.NONE, 0, records(i)),
        Int.MaxValue
      )
    }

    for (i <- 50 until messageIds.max) {
      assertEquals(MemoryRecords.EMPTY, LogTestUtils.readLog(log, i, maxLength = 0, minOneMessage = false).records)

      // we return an incomplete message instead of an empty one for the case below
      // we use this mechanism to tell consumers of the fetch request version 2 and below that the message size is
      // larger than the fetch size
      // in fetch request version 3, we no longer need this as we return oversized messages from the first non-empty
      // partition
      val fetchInfo = LogTestUtils.readLog(log, i, maxLength = 1, minOneMessage = false)
      assertTrue(fetchInfo.firstEntryIncomplete)
      assertTrue(fetchInfo.records.isInstanceOf[FileRecords])
      assertEquals(1, fetchInfo.records.sizeInBytes)
    }
  }

  /**
   * Test reading at the boundary of the log, specifically
   * - reading from the logEndOffset should give an empty message set
   * - reading from the maxOffset should give an empty message set
   * - reading beyond the log end offset should throw an OffsetOutOfRangeException
   */
  @Test
  def testReadOutOfRange(): Unit = {
    createEmptyLogs(logDir, 1024)
    // set up replica log starting with offset 1024 and with one message (at offset 1024)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(TestUtils.singletonRecords(value = "42".getBytes), 0)

    assertEquals(0, LogTestUtils.readLog(log, 1025, 1000).records.sizeInBytes,
      "Reading at the log end offset should produce 0 byte read.")

    assertThrows(classOf[OffsetOutOfRangeException], () => LogTestUtils.readLog(log, 0, 1000))
    assertThrows(classOf[OffsetOutOfRangeException], () => LogTestUtils.readLog(log, 1026, 1000))
  }

  @Test
  def testFlushingEmptyActiveSegments(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    val log = createLog(logDir, logConfig)
    val message = TestUtils.singletonRecords(value = "Test".getBytes, timestamp = mockTime.milliseconds)
    log.appendAsLeader(message, 0)
    log.roll()
    assertEquals(2, logDir.listFiles(_.getName.endsWith(".log")).length)
    assertEquals(1, logDir.listFiles(_.getName.endsWith(".index")).length)
    assertEquals(0, log.activeSegment.size)
    log.flush(true)
    assertEquals(2, logDir.listFiles(_.getName.endsWith(".log")).length)
    assertEquals(2, logDir.listFiles(_.getName.endsWith(".index")).length)
  }

  /**
   * Test that covers reads and writes on a multisegment log. This test appends a bunch of messages
   * and then reads them all back and checks that the message read and offset matches what was appended.
   */
  @Test
  def testLogRolls(): Unit = {
    /* create a multipart log with 100 messages */
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 100)
    val log = createLog(logDir, logConfig)
    val numMessages = 100
    val messageSets = (0 until numMessages).map(i => TestUtils.singletonRecords(value = i.toString.getBytes,
                                                                                timestamp = mockTime.milliseconds))
    messageSets.foreach(log.appendAsLeader(_, 0))
    log.flush(false)

    /* do successive reads to ensure all our messages are there */
    var offset = 0L
    for (i <- 0 until numMessages) {
      val messages = LogTestUtils.readLog(log, offset, 1024*1024).records.batches
      val head = messages.iterator.next()
      assertEquals(offset, head.lastOffset, "Offsets not equal")

      val expected = messageSets(i).records.iterator.next()
      val actual = head.iterator.next()
      assertEquals(expected.key, actual.key, s"Keys not equal at offset $offset")
      assertEquals(expected.value, actual.value, s"Values not equal at offset $offset")
      assertEquals(expected.timestamp, actual.timestamp, s"Timestamps not equal at offset $offset")
      offset = head.lastOffset + 1
    }
    val lastRead = LogTestUtils.readLog(log, startOffset = numMessages, maxLength = 1024*1024).records
    assertEquals(0, lastRead.records.asScala.size, "Should be no more messages")

    // check that rolling the log forced a flushed, the flush is async so retry in case of failure
    TestUtils.retry(1000L) {
      assertTrue(log.recoveryPoint >= log.activeSegment.baseOffset, "Log role should have forced flush")
    }
  }

  /**
   * Test reads at offsets that fall within compressed message set boundaries.
   */
  @Test
  def testCompressedMessages(): Unit = {
    /* this log should roll after every messageset */
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 110)
    val log = createLog(logDir, logConfig)

    /* append 2 compressed message sets, each with two messages giving offsets 0, 1, 2, 3 */
    log.appendAsLeader(MemoryRecords.withRecords(Compression.gzip().build(), new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes)), 0)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.gzip().build(), new SimpleRecord("alpha".getBytes), new SimpleRecord("beta".getBytes)), 0)

    def read(offset: Int) = LogTestUtils.readLog(log, offset, 4096).records.records

    /* we should always get the first message in the compressed set when reading any offset in the set */
    assertEquals(0, read(0).iterator.next().offset, "Read at offset 0 should produce 0")
    assertEquals(0, read(1).iterator.next().offset, "Read at offset 1 should produce 0")
    assertEquals(2, read(2).iterator.next().offset, "Read at offset 2 should produce 2")
    assertEquals(2, read(3).iterator.next().offset, "Read at offset 3 should produce 2")
  }

  /**
   * Test garbage collecting old segments
   */
  @Test
  def testThatGarbageCollectingSegmentsDoesntChangeOffset(): Unit = {
    for (messagesToAppend <- List(0, 1, 25)) {
      logDir.mkdirs()
      // first test a log segment starting at 0
      val logConfig = LogTestUtils.createLogConfig(segmentBytes = 100, retentionMs = 0)
      val log = createLog(logDir, logConfig)
      for (i <- 0 until messagesToAppend)
        log.appendAsLeader(TestUtils.singletonRecords(value = i.toString.getBytes, timestamp = mockTime.milliseconds - 10), 0)

      val currOffset = log.logEndOffset
      assertEquals(currOffset, messagesToAppend)

      // time goes by; the log file is deleted
      log.updateHighWatermark(currOffset)
      log.deleteOldSegments()

      assertEquals(currOffset, log.logEndOffset, "Deleting segments shouldn't have changed the logEndOffset")
      assertEquals(1, log.numberOfSegments, "We should still have one segment left")
      assertEquals(0, log.deleteOldSegments(), "Further collection shouldn't delete anything")
      assertEquals(currOffset, log.logEndOffset, "Still no change in the logEndOffset")
      assertEquals(
        currOffset,
        log.appendAsLeader(
          TestUtils.singletonRecords(value = "hello".getBytes, timestamp = mockTime.milliseconds),
          0
        ).firstOffset,
        "Should still be able to append and should get the logEndOffset assigned to the new append")

      // cleanup the log
      log.delete()
    }
  }

  /**
   *  MessageSet size shouldn't exceed the config.segmentSize, check that it is properly enforced by
   * appending a message set larger than the config.segmentSize setting and checking that an exception is thrown.
   */
  @Test
  def testMessageSetSizeCheck(): Unit = {
    val messageSet = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("You".getBytes), new SimpleRecord("bethe".getBytes))
    // append messages to log
    val configSegmentSize = messageSet.sizeInBytes - 1
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = configSegmentSize)
    val log = createLog(logDir, logConfig)

    assertThrows(classOf[RecordBatchTooLargeException], () => log.appendAsLeader(messageSet, 0))
  }

  @Test
  def testCompactedTopicConstraints(): Unit = {
    val keyedMessage = new SimpleRecord("and here it is".getBytes, "this message has a key".getBytes)
    val anotherKeyedMessage = new SimpleRecord("another key".getBytes, "this message also has a key".getBytes)
    val unkeyedMessage = new SimpleRecord("this message does not have a key".getBytes)

    val messageSetWithUnkeyedMessage = MemoryRecords.withRecords(Compression.NONE, unkeyedMessage, keyedMessage)
    val messageSetWithOneUnkeyedMessage = MemoryRecords.withRecords(Compression.NONE, unkeyedMessage)
    val messageSetWithCompressedKeyedMessage = MemoryRecords.withRecords(Compression.gzip().build(), keyedMessage)
    val messageSetWithCompressedUnkeyedMessage = MemoryRecords.withRecords(Compression.gzip().build(), keyedMessage, unkeyedMessage)

    val messageSetWithKeyedMessage = MemoryRecords.withRecords(Compression.NONE, keyedMessage)
    val messageSetWithKeyedMessages = MemoryRecords.withRecords(Compression.NONE, keyedMessage, anotherKeyedMessage)

    val logConfig = LogTestUtils.createLogConfig(cleanupPolicy = TopicConfig.CLEANUP_POLICY_COMPACT)
    val log = createLog(logDir, logConfig)

    val errorMsgPrefix = "Compacted topic cannot accept message without key"

    var e = assertThrows(classOf[RecordValidationException],
      () => log.appendAsLeader(messageSetWithUnkeyedMessage, 0))
    assertTrue(e.invalidException.isInstanceOf[InvalidRecordException])
    assertEquals(1, e.recordErrors.size)
    assertEquals(0, e.recordErrors.get(0).batchIndex)
    assertTrue(e.recordErrors.get(0).message.startsWith(errorMsgPrefix))

    e = assertThrows(classOf[RecordValidationException],
      () => log.appendAsLeader(messageSetWithOneUnkeyedMessage, 0))
    assertTrue(e.invalidException.isInstanceOf[InvalidRecordException])
    assertEquals(1, e.recordErrors.size)
    assertEquals(0, e.recordErrors.get(0).batchIndex)
    assertTrue(e.recordErrors.get(0).message.startsWith(errorMsgPrefix))

    e = assertThrows(classOf[RecordValidationException],
      () => log.appendAsLeader(messageSetWithCompressedUnkeyedMessage, 0))
    assertTrue(e.invalidException.isInstanceOf[InvalidRecordException])
    assertEquals(1, e.recordErrors.size)
    assertEquals(1, e.recordErrors.get(0).batchIndex)     // batch index is 1
    assertTrue(e.recordErrors.get(0).message.startsWith(errorMsgPrefix))

    // check if metric for NoKeyCompactedTopicRecordsPerSec is logged
    assertEquals(metricsKeySet.count(_.getMBeanName.endsWith(s"${BrokerTopicMetrics.NO_KEY_COMPACTED_TOPIC_RECORDS_PER_SEC}")), 1)
    assertTrue(TestUtils.meterCount(s"${BrokerTopicMetrics.NO_KEY_COMPACTED_TOPIC_RECORDS_PER_SEC}") > 0)

    // the following should succeed without any InvalidMessageException
    log.appendAsLeader(messageSetWithKeyedMessage, 0)
    log.appendAsLeader(messageSetWithKeyedMessages, 0)
    log.appendAsLeader(messageSetWithCompressedKeyedMessage, 0)
  }

  /**
   * We have a max size limit on message appends, check that it is properly enforced by appending a message larger than the
   * setting and checking that an exception is thrown.
   */
  @Test
  def testMessageSizeCheck(): Unit = {
    val first = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("You".getBytes), new SimpleRecord("bethe".getBytes))
    val second = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord("change (I need more bytes)... blah blah blah.".getBytes),
      new SimpleRecord("More padding boo hoo".getBytes))

    // append messages to log
    val maxMessageSize = second.sizeInBytes - 1
    val logConfig = LogTestUtils.createLogConfig(maxMessageBytes = maxMessageSize)
    val log = createLog(logDir, logConfig)

    // should be able to append the small message
    log.appendAsLeader(first, 0)

    assertThrows(classOf[RecordTooLargeException], () => log.appendAsLeader(second, 0),
      () => "Second message set should throw MessageSizeTooLargeException.")
  }

  @Test
  def testMessageSizeCheckInAppendAsFollower(): Unit = {
    val first = MemoryRecords.withRecords(0, Compression.NONE, 0,
      new SimpleRecord("You".getBytes), new SimpleRecord("bethe".getBytes))
    val second = MemoryRecords.withRecords(5, Compression.NONE, 0,
      new SimpleRecord("change (I need more bytes)... blah blah blah.".getBytes),
      new SimpleRecord("More padding boo hoo".getBytes))

    val log = createLog(logDir, LogTestUtils.createLogConfig(maxMessageBytes = second.sizeInBytes - 1))

    log.appendAsFollower(first, Int.MaxValue)
    // the second record is larger than limit but appendAsFollower does not validate the size.
    log.appendAsFollower(second, Int.MaxValue)
  }

  @ParameterizedTest
  @ArgumentsSource(classOf[InvalidMemoryRecordsProvider])
  def testInvalidMemoryRecords(records: MemoryRecords, expectedException: Optional[Class[Exception]]): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    val log = createLog(logDir, logConfig)
    val previousEndOffset = log.logEndOffsetMetadata.messageOffset

    if (expectedException.isPresent) {
      assertThrows(
        expectedException.get(),
        () => log.appendAsFollower(records, Int.MaxValue)
      )
    } else {
        log.appendAsFollower(records, Int.MaxValue)
    }

    assertEquals(previousEndOffset, log.logEndOffsetMetadata.messageOffset)
  }

  @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
  def testRandomRecords(
    @ForAll(supplier = classOf[ArbitraryMemoryRecords]) records: MemoryRecords
  ): Unit = {
    val tempDir = TestUtils.tempDir()
    val logDir = TestUtils.randomPartitionLogDir(tempDir)
    try {
      val logConfig = LogTestUtils.createLogConfig()
      val log = createLog(logDir, logConfig)
      val previousEndOffset = log.logEndOffsetMetadata.messageOffset

      // Depending on the corruption, unified log sometimes throws and sometimes returns an
      // empty set of batches
      assertThrows(
        classOf[CorruptRecordException],
        () => {
          val info = log.appendAsFollower(records, Int.MaxValue)
          if (info.firstOffset == UnifiedLog.UNKNOWN_OFFSET) {
            throw new CorruptRecordException("Unknown offset is test")
          }
        }
      )

      assertEquals(previousEndOffset, log.logEndOffsetMetadata.messageOffset)
    } finally {
      Utils.delete(tempDir)
    }
  }

  @Test
  def testInvalidLeaderEpoch(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    val log = createLog(logDir, logConfig)
    val previousEndOffset = log.logEndOffsetMetadata.messageOffset
    val epoch = log.latestEpoch.orElse(0) + 1
    val numberOfRecords = 10

    val batchWithValidEpoch = MemoryRecords.withRecords(
      previousEndOffset,
      Compression.NONE,
      epoch,
      (0 until numberOfRecords).map(number => new SimpleRecord(number.toString.getBytes)): _*
    )

    val batchWithInvalidEpoch = MemoryRecords.withRecords(
      previousEndOffset + numberOfRecords,
      Compression.NONE,
      epoch + 1,
      (0 until numberOfRecords).map(number => new SimpleRecord(number.toString.getBytes)): _*
    )

    val buffer = ByteBuffer.allocate(batchWithValidEpoch.sizeInBytes() + batchWithInvalidEpoch.sizeInBytes())
    buffer.put(batchWithValidEpoch.buffer())
    buffer.put(batchWithInvalidEpoch.buffer())
    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)

    log.appendAsFollower(records, epoch)

    // Check that only the first batch was appended
    assertEquals(previousEndOffset + numberOfRecords, log.logEndOffsetMetadata.messageOffset)
    // Check that the last fetched epoch matches the first batch
    assertEquals(epoch, log.latestEpoch.get)
  }

  @Test
  def testLogFlushesPartitionMetadataOnAppend(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    val log = createLog(logDir, logConfig)
    val record = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("simpleValue".getBytes))

    val topicId = Uuid.randomUuid()
    log.partitionMetadataFile.get.record(topicId)

    // Should trigger a synchronous flush
    log.appendAsLeader(record, 0)
    assertTrue(log.partitionMetadataFile.get.exists())
    assertEquals(topicId, log.partitionMetadataFile.get.read().topicId)
  }

  @Test
  def testLogFlushesPartitionMetadataOnClose(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    var log = createLog(logDir, logConfig)

    val topicId = Uuid.randomUuid()
    log.partitionMetadataFile.get.record(topicId)

    // Should trigger a synchronous flush
    log.close()

    // We open the log again, and the partition metadata file should exist with the same ID.
    log = createLog(logDir, logConfig)
    assertTrue(log.partitionMetadataFile.get.exists())
    assertEquals(topicId, log.partitionMetadataFile.get.read().topicId)
  }

  @Test
  def testLogRecoversTopicId(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    var log = createLog(logDir, logConfig)

    val topicId = Uuid.randomUuid()
    log.assignTopicId(topicId)
    log.close()

    // test recovery case
    log = createLog(logDir, logConfig)
    assertTrue(log.topicId.isPresent)
    assertEquals(topicId, log.topicId.get)
    log.close()
  }

  @Test
  def testLogFailsWhenInconsistentTopicIdSet(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    var log = createLog(logDir, logConfig)

    val topicId = Uuid.randomUuid()
    log.assignTopicId(topicId)
    log.close()

    // test creating a log with a new ID
    try {
      log = createLog(logDir, logConfig, topicId = Some(Uuid.randomUuid()))
      log.close()
    } catch {
      case e: Throwable => assertTrue(e.isInstanceOf[InconsistentTopicIdException])
    }
  }

  /**
   * Test building the time index on the follower by setting assignOffsets to false.
   */
  @Test
  def testBuildTimeIndexWhenNotAssigningOffsets(): Unit = {
    val numMessages = 100
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 10000, indexIntervalBytes = 1)
    val log = createLog(logDir, logConfig)

    val messages = (0 until numMessages).map { i =>
      MemoryRecords.withRecords(100 + i, Compression.NONE, 0, new SimpleRecord(mockTime.milliseconds + i, i.toString.getBytes()))
    }
    messages.foreach(message => log.appendAsFollower(message, Int.MaxValue))
    val timeIndexEntries = log.logSegments.asScala.foldLeft(0) { (entries, segment) => entries + segment.timeIndex.entries }
    assertEquals(numMessages - 1, timeIndexEntries, s"There should be ${numMessages - 1} time index entries")
    assertEquals(mockTime.milliseconds + numMessages - 1, log.activeSegment.timeIndex.lastEntry.timestamp,
      s"The last time index entry should have timestamp ${mockTime.milliseconds + numMessages - 1}")
  }

  @Test
  def testFetchOffsetByTimestampIncludesLeaderEpoch(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1)
    val log = createLog(logDir, logConfig)

    assertEquals(new OffsetResultHolder(Optional.empty[FileRecords.TimestampAndOffset]()), log.fetchOffsetByTimestamp(0L, Optional.empty))

    val firstTimestamp = mockTime.milliseconds
    val firstLeaderEpoch = 0
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      firstLeaderEpoch)

    val secondTimestamp = firstTimestamp + 1
    val secondLeaderEpoch = 1
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = secondTimestamp),
      secondLeaderEpoch)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(firstTimestamp, 0L, Optional.of(firstLeaderEpoch))),
      log.fetchOffsetByTimestamp(firstTimestamp, Optional.empty))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(secondTimestamp, 1L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(secondTimestamp, Optional.empty))

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Optional.empty))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Optional.empty))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty))

    // The cache can be updated directly after a leader change.
    // The new latest offset should reflect the updated epoch.
    log.assignEpochStartOffset(2, 2L)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(2))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty))
  }

  @Test
  def testFetchOffsetByTimestampWithMaxTimestampIncludesTimestamp(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1)
    val log = createLog(logDir, logConfig)

    assertEquals(new OffsetResultHolder(Optional.empty[FileRecords.TimestampAndOffset]()), log.fetchOffsetByTimestamp(0L, Optional.empty))

    val firstTimestamp = mockTime.milliseconds
    val leaderEpoch = 0
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      leaderEpoch)

    val secondTimestamp = firstTimestamp + 1
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = secondTimestamp),
      leaderEpoch)

    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      leaderEpoch)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(secondTimestamp, 1L, Optional.of(leaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP, Optional.empty))
  }

  @Test
  def testFetchOffsetByTimestampFromRemoteStorage(): Unit = {
    val config: KafkaConfig = createKafkaConfigWithRLM
    val purgatory = new DelayedOperationPurgatory[DelayedRemoteListOffsets]("RemoteListOffsets", config.brokerId)
    val remoteLogManager = spy(new RemoteLogManager(config.remoteLogManagerConfig,
      0,
      logDir.getAbsolutePath,
      "clusterId",
      mockTime,
      _ => Optional.empty[UnifiedLog](),
      (_, _) => {},
      brokerTopicStats,
      new Metrics(),
      Optional.empty))
    remoteLogManager.setDelayedOperationPurgatory(purgatory)

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1,
      remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true, remoteLogManager = Some(remoteLogManager))
    // Note that the log is empty, so remote offset read won't happen
    assertEquals(new OffsetResultHolder(Optional.empty[FileRecords.TimestampAndOffset]()), log.fetchOffsetByTimestamp(0L, Optional.of(remoteLogManager)))

    val firstTimestamp = mockTime.milliseconds
    val firstLeaderEpoch = 0
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      firstLeaderEpoch)

    val secondTimestamp = firstTimestamp + 1
    val secondLeaderEpoch = 1
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = secondTimestamp),
      secondLeaderEpoch)

    doAnswer(ans => {
      val timestamp = ans.getArgument(1).asInstanceOf[Long]
      Optional.of(timestamp)
        .filter(_ == firstTimestamp)
        .map[TimestampAndOffset](x => new TimestampAndOffset(x, 0L, Optional.of(firstLeaderEpoch)))
    }).when(remoteLogManager).findOffsetByTimestamp(ArgumentMatchers.eq(log.topicPartition),
      anyLong(), anyLong(), ArgumentMatchers.eq(log.leaderEpochCache))
    log.updateLocalLogStartOffset(1)

    def assertFetchOffsetByTimestamp(expected: Option[TimestampAndOffset], timestamp: Long): Unit = {
      val offsetResultHolder = log.fetchOffsetByTimestamp(timestamp, Optional.of(remoteLogManager))
      assertTrue(offsetResultHolder.futureHolderOpt.isPresent)
      offsetResultHolder.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS)
      assertTrue(offsetResultHolder.futureHolderOpt.get.taskFuture.isDone)
      assertTrue(offsetResultHolder.futureHolderOpt.get.taskFuture.get().hasTimestampAndOffset)
      assertEquals(expected.get, offsetResultHolder.futureHolderOpt.get.taskFuture.get().timestampAndOffset().orElse(null))
    }

    // In the assertions below we test that offset 0 (first timestamp) is in remote and offset 1 (second timestamp) is in local storage.
    assertFetchOffsetByTimestamp(Some(new TimestampAndOffset(firstTimestamp, 0L, Optional.of(firstLeaderEpoch))), firstTimestamp)
    assertFetchOffsetByTimestamp(Some(new TimestampAndOffset(secondTimestamp, 1L, Optional.of(secondLeaderEpoch))), secondTimestamp)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Optional.of(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 1L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Optional.of(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.of(remoteLogManager)))

    // The cache can be updated directly after a leader change.
    // The new latest offset should reflect the updated epoch.
    log.assignEpochStartOffset(2, 2L)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(2))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.of(remoteLogManager)))
  }

  @Test
  def testFetchLatestTieredTimestampNoRemoteStorage(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1)
    val log = createLog(logDir, logConfig)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, -1, Optional.of(-1))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP, Optional.empty))

    val firstTimestamp = mockTime.milliseconds
    val leaderEpoch = 0
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      leaderEpoch)

    val secondTimestamp = firstTimestamp + 1
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = secondTimestamp),
      leaderEpoch)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, -1, Optional.of(-1))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP, Optional.empty))
  }

  @Test
  def testFetchLatestTieredTimestampWithRemoteStorage(): Unit = {
    val config: KafkaConfig = createKafkaConfigWithRLM
    val purgatory = new DelayedOperationPurgatory[DelayedRemoteListOffsets]("RemoteListOffsets", config.brokerId)
    val remoteLogManager = spy(new RemoteLogManager(config.remoteLogManagerConfig,
      0,
      logDir.getAbsolutePath,
      "clusterId",
      mockTime,
      _ => Optional.empty[UnifiedLog](),
      (_, _) => {},
      brokerTopicStats,
      new Metrics(),
      Optional.empty))
    remoteLogManager.setDelayedOperationPurgatory(purgatory)

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1,
      remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true, remoteLogManager = Some(remoteLogManager))
    // Note that the log is empty, so remote offset read won't happen
    assertEquals(new OffsetResultHolder(Optional.empty[FileRecords.TimestampAndOffset]()), log.fetchOffsetByTimestamp(0L, Optional.of(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0, Optional.empty())),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Optional.of(remoteLogManager)))

    val firstTimestamp = mockTime.milliseconds
    val firstLeaderEpoch = 0
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      firstLeaderEpoch)

    val secondTimestamp = firstTimestamp + 1
    val secondLeaderEpoch = 1
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = secondTimestamp),
      secondLeaderEpoch)

    doAnswer(ans => {
      val timestamp = ans.getArgument(1).asInstanceOf[Long]
      Optional.of(timestamp)
        .filter(_ == firstTimestamp)
        .map[TimestampAndOffset](x => new TimestampAndOffset(x, 0L, Optional.of(firstLeaderEpoch)))
    }).when(remoteLogManager).findOffsetByTimestamp(ArgumentMatchers.eq(log.topicPartition),
      anyLong(), anyLong(), ArgumentMatchers.eq(log.leaderEpochCache))
    log.updateLocalLogStartOffset(1)
    log.updateHighestOffsetInRemoteStorage(0)

    def assertFetchOffsetByTimestamp(expected: Option[TimestampAndOffset], timestamp: Long): Unit = {
      val offsetResultHolder = log.fetchOffsetByTimestamp(timestamp, Optional.of(remoteLogManager))
      assertTrue(offsetResultHolder.futureHolderOpt.isPresent)
      offsetResultHolder.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS)
      assertTrue(offsetResultHolder.futureHolderOpt.get.taskFuture.isDone)
      assertTrue(offsetResultHolder.futureHolderOpt.get.taskFuture.get().hasTimestampAndOffset)
      assertEquals(expected.get, offsetResultHolder.futureHolderOpt.get.taskFuture.get().timestampAndOffset().orElse(null))
    }

    // In the assertions below we test that offset 0 (first timestamp) is in remote and offset 1 (second timestamp) is in local storage.
    assertFetchOffsetByTimestamp(Some(new TimestampAndOffset(firstTimestamp, 0L, Optional.of(firstLeaderEpoch))), firstTimestamp)
    assertFetchOffsetByTimestamp(Some(new TimestampAndOffset(secondTimestamp, 1L, Optional.of(secondLeaderEpoch))), secondTimestamp)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Optional.of(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP, Optional.of(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 1L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Optional.of(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.of(remoteLogManager)))

    // The cache can be updated directly after a leader change.
    // The new latest offset should reflect the updated epoch.
    log.assignEpochStartOffset(2, 2L)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(2))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.of(remoteLogManager)))
  }

  private def createKafkaConfigWithRLM: KafkaConfig = {
    val props = new Properties()
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "0")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    props.setProperty("controller.quorum.bootstrap.servers", "localhost:9093")
    props.setProperty("listeners", "CONTROLLER://:9093")
    props.setProperty("advertised.listeners", "CONTROLLER://127.0.0.1:9093")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    // set log reader threads number to 2
    props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP, "2")
    KafkaConfig.fromProps(props)
  }

  @Test
  def testFetchEarliestPendingUploadTimestampNoRemoteStorage(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1)
    val log = createLog(logDir, logConfig)

    // Test initial state before any records
    assertFetchOffsetBySpecialTimestamp(log, None, new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, -1, Optional.of(-1)),
      ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP)

    // Append records
    val _ = prepareLogWithSequentialRecords(log, recordCount = 2)

    // Test state after records are appended
    assertFetchOffsetBySpecialTimestamp(log, None, new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, -1, Optional.of(-1)),
      ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP)
  }

  @Test
  def testFetchEarliestPendingUploadTimestampWithRemoteStorage(): Unit = {
    val logStartOffset = 0
    val (remoteLogManager: RemoteLogManager, log: UnifiedLog, timestampAndEpochs: Seq[TimestampAndEpoch]) = prepare(logStartOffset)

    val (firstTimestamp, firstLeaderEpoch) = (timestampAndEpochs.head.timestamp, timestampAndEpochs.head.leaderEpoch)
    val (secondTimestamp, secondLeaderEpoch) = (timestampAndEpochs(1).timestamp, timestampAndEpochs(1).leaderEpoch)
    val (_, thirdLeaderEpoch) = (timestampAndEpochs(2).timestamp, timestampAndEpochs(2).leaderEpoch)

    doAnswer(ans => {
      val timestamp = ans.getArgument(1).asInstanceOf[Long]
      Optional.of(timestamp)
        .filter(_ == timestampAndEpochs.head.timestamp)
        .map[TimestampAndOffset](x => new TimestampAndOffset(x, 0L, Optional.of(timestampAndEpochs.head.leaderEpoch)))
    }).when(remoteLogManager).findOffsetByTimestamp(ArgumentMatchers.eq(log.topicPartition),
      anyLong(), anyLong(), ArgumentMatchers.eq(log.leaderEpochCache))

    // Offset 0 (first timestamp) is in remote storage and deleted locally. Offset 1 (second timestamp) is in local storage.
    log.updateLocalLogStartOffset(1)
    log.updateHighestOffsetInRemoteStorage(0)

    // In the assertions below we test that offset 0 (first timestamp) is only in remote and offset 1 (second timestamp) is in local storage.
    assertFetchOffsetByTimestamp(log, Some(remoteLogManager), Some(new TimestampAndOffset(firstTimestamp, 0L, Optional.of(firstLeaderEpoch))), firstTimestamp)
    assertFetchOffsetByTimestamp(log, Some(remoteLogManager), Some(new TimestampAndOffset(secondTimestamp, 1L, Optional.of(secondLeaderEpoch))), secondTimestamp)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager), new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager), new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch)),
      ListOffsetsRequest.LATEST_TIERED_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager), new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 1L, Optional.of(secondLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager), new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 3L, Optional.of(thirdLeaderEpoch)),
      ListOffsetsRequest.LATEST_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager), new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 1L, Optional.of(secondLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP)
  }

  @Test
  def testFetchEarliestPendingUploadTimestampWithRemoteStorageNoLocalDeletion(): Unit = {
    val logStartOffset = 0
    val (remoteLogManager: RemoteLogManager, log: UnifiedLog, timestampAndEpochs: Seq[TimestampAndEpoch]) = prepare(logStartOffset)

    val (firstTimestamp, firstLeaderEpoch) = (timestampAndEpochs.head.timestamp, timestampAndEpochs.head.leaderEpoch)
    val (secondTimestamp, secondLeaderEpoch) = (timestampAndEpochs(1).timestamp, timestampAndEpochs(1).leaderEpoch)
    val (_, thirdLeaderEpoch) = (timestampAndEpochs(2).timestamp, timestampAndEpochs(2).leaderEpoch)

    // Offsets upto 1 are in remote storage
    doAnswer(ans => {
      val timestamp = ans.getArgument(1).asInstanceOf[Long]
      Optional.of(
        timestamp match {
          case x if x == firstTimestamp => new TimestampAndOffset(x, 0L, Optional.of(firstLeaderEpoch))
          case x if x == secondTimestamp => new TimestampAndOffset(x, 1L, Optional.of(secondLeaderEpoch))
          case _ => null
        }
      )
    }).when(remoteLogManager).findOffsetByTimestamp(ArgumentMatchers.eq(log.topicPartition),
      anyLong(), anyLong(), ArgumentMatchers.eq(log.leaderEpochCache))

    // Offsets 0, 1 (first and second timestamps) are in remote storage and not deleted locally.
    log.updateLocalLogStartOffset(0)
    log.updateHighestOffsetInRemoteStorage(1)

    // In the assertions below we test that offset 0 (first timestamp) and offset 1 (second timestamp) are on both remote and local storage
    assertFetchOffsetByTimestamp(log, Some(remoteLogManager), Some(new TimestampAndOffset(firstTimestamp, 0L, Optional.of(firstLeaderEpoch))), firstTimestamp)
    assertFetchOffsetByTimestamp(log, Some(remoteLogManager), Some(new TimestampAndOffset(secondTimestamp, 1L, Optional.of(secondLeaderEpoch))), secondTimestamp)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager), new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 1L, Optional.of(secondLeaderEpoch)),
      ListOffsetsRequest.LATEST_TIERED_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 3L, Optional.of(thirdLeaderEpoch)),
      ListOffsetsRequest.LATEST_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(thirdLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP)
  }

  @Test
  def testFetchEarliestPendingUploadTimestampNoSegmentsUploaded(): Unit = {
    val logStartOffset = 0
    val (remoteLogManager: RemoteLogManager, log: UnifiedLog, timestampAndEpochs: Seq[TimestampAndEpoch]) = prepare(logStartOffset)

    val (firstTimestamp, firstLeaderEpoch) = (timestampAndEpochs.head.timestamp, timestampAndEpochs.head.leaderEpoch)
    val (secondTimestamp, secondLeaderEpoch) = (timestampAndEpochs(1).timestamp, timestampAndEpochs(1).leaderEpoch)
    val (_, thirdLeaderEpoch) = (timestampAndEpochs(2).timestamp, timestampAndEpochs(2).leaderEpoch)

    // No offsets are in remote storage
    doAnswer(_ => Optional.empty[TimestampAndOffset]())
      .when(remoteLogManager).findOffsetByTimestamp(ArgumentMatchers.eq(log.topicPartition),
        anyLong(), anyLong(), ArgumentMatchers.eq(log.leaderEpochCache))

    // Offsets 0, 1, 2 (first, second and third timestamps) are in local storage only and not uploaded to remote storage.
    log.updateLocalLogStartOffset(0)
    log.updateHighestOffsetInRemoteStorage(-1)

    // In the assertions below we test that offset 0 (first timestamp), offset 1 (second timestamp) and offset 2 (third timestamp) are only on the local storage.
    assertFetchOffsetByTimestamp(log, Some(remoteLogManager), Some(new TimestampAndOffset(firstTimestamp, 0L, Optional.of(firstLeaderEpoch))), firstTimestamp)
    assertFetchOffsetByTimestamp(log, Some(remoteLogManager), Some(new TimestampAndOffset(secondTimestamp, 1L, Optional.of(secondLeaderEpoch))), secondTimestamp)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, -1L, Optional.of(-1)),
      ListOffsetsRequest.LATEST_TIERED_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 3L, Optional.of(thirdLeaderEpoch)),
      ListOffsetsRequest.LATEST_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP)
  }

  @Test
  def testFetchEarliestPendingUploadTimestampStaleHighestOffsetInRemote(): Unit = {
    val logStartOffset = 100
    val (remoteLogManager: RemoteLogManager, log: UnifiedLog, timestampAndEpochs: Seq[TimestampAndEpoch]) = prepare(logStartOffset)

    val (firstTimestamp, firstLeaderEpoch) = (timestampAndEpochs.head.timestamp, timestampAndEpochs.head.leaderEpoch)
    val (secondTimestamp, secondLeaderEpoch) = (timestampAndEpochs(1).timestamp, timestampAndEpochs(1).leaderEpoch)
    val (_, thirdLeaderEpoch) = (timestampAndEpochs(2).timestamp, timestampAndEpochs(2).leaderEpoch)

    // Offsets 100, 101, 102 (first, second and third timestamps) are in local storage and not uploaded to remote storage.
    // Tiered storage copy was disabled and then enabled again, because of which the remote log segments are deleted but
    // the highest offset in remote storage has become stale
    doAnswer(_ => Optional.empty[TimestampAndOffset]())
      .when(remoteLogManager).findOffsetByTimestamp(ArgumentMatchers.eq(log.topicPartition),
        anyLong(), anyLong(), ArgumentMatchers.eq(log.leaderEpochCache))

    log.updateLocalLogStartOffset(100)
    log.updateHighestOffsetInRemoteStorage(50)

    // In the assertions below we test that offset 100 (first timestamp), offset 101 (second timestamp) and offset 102 (third timestamp) are only on the local storage.
    assertFetchOffsetByTimestamp(log, Some(remoteLogManager), Some(new TimestampAndOffset(firstTimestamp, 100L, Optional.of(firstLeaderEpoch))), firstTimestamp)
    assertFetchOffsetByTimestamp(log, Some(remoteLogManager), Some(new TimestampAndOffset(secondTimestamp, 101L, Optional.of(secondLeaderEpoch))), secondTimestamp)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 100L, Optional.of(firstLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 50L, Optional.empty()),
      ListOffsetsRequest.LATEST_TIERED_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 100L, Optional.of(firstLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 103L, Optional.of(thirdLeaderEpoch)),
      ListOffsetsRequest.LATEST_TIMESTAMP)
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager),new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 100L, Optional.of(firstLeaderEpoch)),
      ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP)
  }

  private def prepare(logStartOffset: Int): (RemoteLogManager, UnifiedLog, Seq[TimestampAndEpoch]) = {
    val config: KafkaConfig = createKafkaConfigWithRLM
    val purgatory = new DelayedOperationPurgatory[DelayedRemoteListOffsets]("RemoteListOffsets", config.brokerId)
    val remoteLogManager = spy(new RemoteLogManager(config.remoteLogManagerConfig,
      0,
      logDir.getAbsolutePath,
      "clusterId",
      mockTime,
      _ => Optional.empty[UnifiedLog](),
      (_, _) => {},
      brokerTopicStats,
      new Metrics(),
      Optional.empty))
    remoteLogManager.setDelayedOperationPurgatory(purgatory)

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1, remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, logStartOffset = logStartOffset, remoteStorageSystemEnable = true, remoteLogManager = Some(remoteLogManager))

    // Verify earliest pending upload offset for empty log
    assertFetchOffsetBySpecialTimestamp(log, Some(remoteLogManager), new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, logStartOffset, Optional.empty()),
      ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP)

    val timestampAndEpochs = prepareLogWithSequentialRecords(log, recordCount = 3)
    (remoteLogManager, log, timestampAndEpochs)
  }

  /**
   * Test the Log truncate operations
   */
  @Test
  def testTruncateTo(): Unit = {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    val setSize = createRecords.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * setSize  // each segment will be 10 messages

    // create a log
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = segmentSize)
    val log = createLog(logDir, logConfig)
    assertEquals(1, log.numberOfSegments, "There should be exactly 1 segment.")

    for (_ <- 1 to msgPerSeg)
      log.appendAsLeader(createRecords, 0)

    assertEquals(1, log.numberOfSegments, "There should be exactly 1 segments.")
    assertEquals(msgPerSeg, log.logEndOffset, "Log end offset should be equal to number of messages")

    val lastOffset = log.logEndOffset
    val size = log.size
    log.truncateTo(log.logEndOffset) // keep the entire log
    assertEquals(lastOffset, log.logEndOffset, "Should not change offset")
    assertEquals(size, log.size, "Should not change log size")
    log.truncateTo(log.logEndOffset + 1) // try to truncate beyond lastOffset
    assertEquals(lastOffset, log.logEndOffset, "Should not change offset but should log error")
    assertEquals(size, log.size, "Should not change log size")
    log.truncateTo(msgPerSeg/2) // truncate somewhere in between
    assertEquals(log.logEndOffset, msgPerSeg/2, "Should change offset")
    assertTrue(log.size < size, "Should change log size")
    log.truncateTo(0) // truncate the entire log
    assertEquals(0, log.logEndOffset, "Should change offset")
    assertEquals(0, log.size, "Should change log size")

    for (_ <- 1 to msgPerSeg)
      log.appendAsLeader(createRecords, 0)

    assertEquals(log.logEndOffset, lastOffset, "Should be back to original offset")
    assertEquals(log.size, size, "Should be back to original size")
    log.truncateFullyAndStartAt(log.logEndOffset - (msgPerSeg - 1), Optional.empty)
    assertEquals(log.logEndOffset, lastOffset - (msgPerSeg - 1), "Should change offset")
    assertEquals(log.size, 0, "Should change log size")

    for (_ <- 1 to msgPerSeg)
      log.appendAsLeader(createRecords, 0)

    assertTrue(log.logEndOffset > msgPerSeg, "Should be ahead of to original offset")
    assertEquals(size, log.size, "log size should be same as before")
    log.truncateTo(0) // truncate before first start offset in the log
    assertEquals(0, log.logEndOffset, "Should change offset")
    assertEquals(log.size, 0, "Should change log size")
  }

  /**
   * Verify that when we truncate a log the index of the last segment is resized to the max index size to allow more appends
   */
  @Test
  def testIndexResizingAtTruncation(): Unit = {
    val setSize = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds).sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * setSize  // each segment will be 10 messages
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = segmentSize, indexIntervalBytes = setSize - 1)
    val log = createLog(logDir, logConfig)
    assertEquals(1, log.numberOfSegments, "There should be exactly 1 segment.")

    for (i<- 1 to msgPerSeg)
      log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds + i), 0)
    assertEquals(1, log.numberOfSegments, "There should be exactly 1 segment.")

    mockTime.sleep(msgPerSeg)
    for (i<- 1 to msgPerSeg)
      log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds + i), 0)
    assertEquals(2, log.numberOfSegments, "There should be exactly 2 segment.")
    val expectedEntries = msgPerSeg - 1

    assertEquals(expectedEntries, log.logSegments.asScala.toList.head.offsetIndex.maxEntries,
      s"The index of the first segment should have $expectedEntries entries")
    assertEquals(expectedEntries, log.logSegments.asScala.toList.head.timeIndex.maxEntries,
      s"The time index of the first segment should have $expectedEntries entries")

    log.truncateTo(0)
    assertEquals(1, log.numberOfSegments, "There should be exactly 1 segment.")
    assertEquals(log.config.maxIndexSize/8, log.logSegments.asScala.toList.head.offsetIndex.maxEntries,
      "The index of segment 1 should be resized to maxIndexSize")
    assertEquals(log.config.maxIndexSize/12, log.logSegments.asScala.toList.head.timeIndex.maxEntries,
      "The time index of segment 1 should be resized to maxIndexSize")

    mockTime.sleep(msgPerSeg)
    for (i<- 1 to msgPerSeg)
      log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds + i), 0)
    assertEquals(1, log.numberOfSegments,
      "There should be exactly 1 segment.")
  }

  /**
   * Test that deleted files are deleted after the appropriate time.
   */
  @Test
  def testAsyncDelete(): Unit = {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds - 1000L)
    val asyncDeleteMs = 1000
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, segmentIndexBytes = 1000, indexIntervalBytes = 10000,
                                    retentionMs = 999, fileDeleteDelayMs = asyncDeleteMs)
    val log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, 0)

    // files should be renamed
    val segments = log.logSegments.asScala.toArray
    val oldFiles = segments.map(_.log.file) ++ segments.map(_.offsetIndexFile)

    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()

    assertEquals(1, log.numberOfSegments, "Only one segment should remain.")
    assertTrue(segments.forall(_.log.file.getName.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) &&
      segments.forall(_.offsetIndexFile.getName.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)),
      "All log and index files should end in .deleted")
    assertTrue(segments.forall(_.log.file.exists) && segments.forall(_.offsetIndexFile.exists),
      "The .deleted files should still be there.")
    assertTrue(oldFiles.forall(!_.exists), "The original file should be gone.")

    // when enough time passes the files should be deleted
    val deletedFiles = segments.map(_.log.file) ++ segments.map(_.offsetIndexFile)
    mockTime.sleep(asyncDeleteMs + 1)
    assertTrue(deletedFiles.forall(!_.exists), "Files should all be gone.")
  }

  @Test
  def testAppendMessageWithNullPayload(): Unit = {
    val log = createLog(logDir, new LogConfig(new Properties))
    log.appendAsLeader(TestUtils.singletonRecords(value = null), 0)
    val head = LogTestUtils.readLog(log, 0, 4096).records.records.iterator.next()
    assertEquals(0, head.offset)
    assertFalse(head.hasValue, "Message payload should be null.")
  }

  @Test
  def testAppendWithOutOfOrderOffsetsThrowsException(): Unit = {
    val log = createLog(logDir, new LogConfig(new Properties))

    val epoch = 0
    val appendOffsets = Seq(0L, 1L, 3L, 2L, 4L)
    val buffer = ByteBuffer.allocate(512)
    for (offset <- appendOffsets) {
      val builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, Compression.NONE,
                                          TimestampType.LOG_APPEND_TIME, offset, mockTime.milliseconds(),
                                          1L, 0, 0, false, epoch)
      builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
      builder.close()
    }
    buffer.flip()
    val memoryRecords = MemoryRecords.readableRecords(buffer)

    assertThrows(
      classOf[OffsetsOutOfOrderException],
      () => log.appendAsFollower(memoryRecords, epoch)
    )
  }

  @Test
  def testAppendBelowExpectedOffsetThrowsException(): Unit = {
    val log = createLog(logDir, new LogConfig(new Properties))
    val records = (0 until 2).map(id => new SimpleRecord(id.toString.getBytes)).toArray
    records.foreach(record => log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, record), 0))

    val magicVals = Seq(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)
    val compressionTypes = Seq(CompressionType.NONE, CompressionType.LZ4)
    for (magic <- magicVals; compressionType <- compressionTypes) {
      val compression = Compression.of(compressionType).build()
      val invalidRecord = MemoryRecords.withRecords(magic, compression, new SimpleRecord(1.toString.getBytes))
      assertThrows(
        classOf[UnexpectedAppendOffsetException],
        () => log.appendAsFollower(invalidRecord, Int.MaxValue),
        () => s"Magic=$magic, compressionType=$compressionType"
      )
    }
  }

  @Test
  def testAppendEmptyLogBelowLogStartOffsetThrowsException(): Unit = {
    createEmptyLogs(logDir, 7)
    val log = createLog(logDir, new LogConfig(new Properties), brokerTopicStats = brokerTopicStats)
    assertEquals(7L, log.logStartOffset)
    assertEquals(7L, log.logEndOffset)

    val firstOffset = 4L
    val magicVals = Seq(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)
    val compressionTypes = Seq(CompressionType.NONE, CompressionType.LZ4)
    for (magic <- magicVals; compressionType <- compressionTypes) {
      val batch = TestUtils.records(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                         new SimpleRecord("k2".getBytes, "v2".getBytes),
                                         new SimpleRecord("k3".getBytes, "v3".getBytes)),
                                    magicValue = magic, codec = Compression.of(compressionType).build(),
                                    baseOffset = firstOffset)

      val exception = assertThrows(
        classOf[UnexpectedAppendOffsetException],
        () => log.appendAsFollower(batch, Int.MaxValue)
      )
      assertEquals(firstOffset, exception.firstOffset, s"Magic=$magic, compressionType=$compressionType, UnexpectedAppendOffsetException#firstOffset")
      assertEquals(firstOffset + 2, exception.lastOffset, s"Magic=$magic, compressionType=$compressionType, UnexpectedAppendOffsetException#lastOffset")
    }
  }

  @Test
  def testAppendWithNoTimestamp(): Unit = {
    val log = createLog(logDir, new LogConfig(new Properties))
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(RecordBatch.NO_TIMESTAMP, "key".getBytes, "value".getBytes)), 0)
  }

  @Test
  def testAppendToOrReadFromLogInFailedLogDir(): Unit = {
    val pid = 1L
    val epoch = 0.toShort
    val log = createLog(logDir, new LogConfig(new Properties))
    log.appendAsLeader(TestUtils.singletonRecords(value = null), 0)
    assertEquals(0, LogTestUtils.readLog(log, 0, 4096).records.records.iterator.next().offset)
    val append = LogTestUtils.appendTransactionalAsLeader(log, pid, epoch, mockTime)
    append(10)
    // Kind of a hack, but renaming the index to a directory ensures that the append
    // to the index will fail.
    log.activeSegment.txnIndex.renameTo(log.dir)
    assertThrows(classOf[KafkaStorageException], () => LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT,
      mockTime.milliseconds(), coordinatorEpoch = 1, transactionVersion = TransactionVersion.TV_0.featureLevel()))
    assertThrows(classOf[KafkaStorageException], () => log.appendAsLeader(TestUtils.singletonRecords(value = null), 0))
    assertThrows(classOf[KafkaStorageException], () => LogTestUtils.readLog(log, 0, 4096).records.records.iterator.next().offset)
  }

  @Test
  def testWriteLeaderEpochCheckpointAfterDirectoryRename(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), 5)
    assertEquals(Optional.of(5), log.latestEpoch)

    // Ensure that after a directory rename, the epoch cache is written to the right location
    val tp = UnifiedLog.parseTopicPartitionName(log.dir)
    log.renameDir(UnifiedLog.logDeleteDirName(tp), true)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), 10)
    assertEquals(Optional.of(10), log.latestEpoch)
    assertTrue(LeaderEpochCheckpointFile.newFile(log.dir).exists())
    assertFalse(LeaderEpochCheckpointFile.newFile(this.logDir).exists())
  }

  @Test
  def testTopicIdTransfersAfterDirectoryRename(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)

    // Write a topic ID to the partition metadata file to ensure it is transferred correctly.
    val topicId = Uuid.randomUuid()
    log.assignTopicId(topicId)

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), 5)
    assertEquals(Optional.of(5), log.latestEpoch)

    // Ensure that after a directory rename, the partition metadata file is written to the right location.
    val tp = UnifiedLog.parseTopicPartitionName(log.dir)
    log.renameDir(UnifiedLog.logDeleteDirName(tp), true)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), 10)
    assertEquals(Optional.of(10), log.latestEpoch)
    assertTrue(PartitionMetadataFile.newFile(log.dir).exists())
    assertFalse(PartitionMetadataFile.newFile(this.logDir).exists())

    // Check the topic ID remains in memory and was copied correctly.
    assertTrue(log.topicId.isPresent)
    assertEquals(topicId, log.topicId.get)
    assertEquals(topicId, log.partitionMetadataFile.get.read().topicId)
  }

  @Test
  def testTopicIdFlushesBeforeDirectoryRename(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)

    // Write a topic ID to the partition metadata file to ensure it is transferred correctly.
    val topicId = Uuid.randomUuid()
    log.partitionMetadataFile.get.record(topicId)

    // Ensure that after a directory rename, the partition metadata file is written to the right location.
    val tp = UnifiedLog.parseTopicPartitionName(log.dir)
    log.renameDir(UnifiedLog.logDeleteDirName(tp), true)
    assertTrue(PartitionMetadataFile.newFile(log.dir).exists())
    assertFalse(PartitionMetadataFile.newFile(this.logDir).exists())

    // Check the file holds the correct contents.
    assertTrue(log.partitionMetadataFile.get.exists())
    assertEquals(topicId, log.partitionMetadataFile.get.read().topicId)
  }

  @Test
  def testLeaderEpochCacheClearedAfterDowngradeInAppendedMessages(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), 5)
    assertEquals(Optional.of(5), log.leaderEpochCache.latestEpoch)

    log.appendAsFollower(
      TestUtils.records(
        List(
          new SimpleRecord("foo".getBytes())
        ),
        baseOffset = 1L,
        magicValue = RecordVersion.V1.value
      ),
      5
    )
    assertEquals(Optional.empty, log.leaderEpochCache.latestEpoch)
  }

  @Test
  def testLeaderEpochCacheCreatedAfterMessageFormatUpgrade(): Unit = {
    val logProps = new Properties()
    logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, "1000")
    logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "1")
    logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "65536")
    val logConfig = new LogConfig(logProps)
    val log = createLog(logDir, logConfig)
    log.appendAsLeaderWithRecordVersion(TestUtils.records(List(new SimpleRecord("bar".getBytes())),
      magicValue = RecordVersion.V1.value), 5, RecordVersion.V1)
    assertTrue(log.latestEpoch.isEmpty)

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes())),
      magicValue = RecordVersion.V2.value), 5)
    assertEquals(5, log.latestEpoch.get)
  }

  @Test
  def testSplitOnOffsetOverflow(): Unit = {
    // create a log such that one log segment has offsets that overflow, and call the split API on that segment
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val (log, segmentWithOverflow) = createLogWithOffsetOverflow(logConfig)
    assertTrue(LogTestUtils.hasOffsetOverflow(log), "At least one segment must have offset overflow")

    val allRecordsBeforeSplit = UnifiedLogTest.allRecords(log)

    // split the segment with overflow
    log.splitOverflowedSegment(segmentWithOverflow)

    // assert we were successfully able to split the segment
    assertEquals(4, log.numberOfSegments)
    UnifiedLogTest.verifyRecordsInLog(log, allRecordsBeforeSplit)

    // verify we do not have offset overflow anymore
    assertFalse(LogTestUtils.hasOffsetOverflow(log))
  }

  @Test
  def testDegenerateSegmentSplit(): Unit = {
    // This tests a scenario where all of the batches appended to a segment have overflowed.
    // When we split the overflowed segment, only one new segment will be created.

    val overflowOffset = Int.MaxValue + 1L
    val batch1 = MemoryRecords.withRecords(overflowOffset, Compression.NONE, 0,
      new SimpleRecord("a".getBytes))
    val batch2 = MemoryRecords.withRecords(overflowOffset + 1, Compression.NONE, 0,
      new SimpleRecord("b".getBytes))

    testDegenerateSplitSegmentWithOverflow(segmentBaseOffset = 0L, List(batch1, batch2))
  }

  @Test
  def testDegenerateSegmentSplitWithOutOfRangeBatchLastOffset(): Unit = {
    // Degenerate case where the only batch in the segment overflows. In this scenario,
    // the first offset of the batch is valid, but the last overflows.

    val firstBatchBaseOffset = Int.MaxValue - 1
    val records = MemoryRecords.withRecords(firstBatchBaseOffset, Compression.NONE, 0,
      new SimpleRecord("a".getBytes),
      new SimpleRecord("b".getBytes),
      new SimpleRecord("c".getBytes))

    testDegenerateSplitSegmentWithOverflow(segmentBaseOffset = 0L, List(records))
  }

  private def testDegenerateSplitSegmentWithOverflow(segmentBaseOffset: Long, records: List[MemoryRecords]): Unit = {
    val segment = LogTestUtils.rawSegment(logDir, segmentBaseOffset)
    // Need to create the offset files explicitly to avoid triggering segment recovery to truncate segment.
    Files.createFile(LogFileUtils.offsetIndexFile(logDir, segmentBaseOffset).toPath)
    Files.createFile(LogFileUtils.timeIndexFile(logDir, segmentBaseOffset).toPath)
    records.foreach(segment.append)
    segment.close()

    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, fileDeleteDelayMs = 1000)
    val log = createLog(logDir, logConfig, recoveryPoint = Long.MaxValue)

    val segmentWithOverflow = LogTestUtils.firstOverflowSegment(log).getOrElse {
      throw new AssertionError("Failed to create log with a segment which has overflowed offsets")
    }

    val allRecordsBeforeSplit = UnifiedLogTest.allRecords(log)
    log.splitOverflowedSegment(segmentWithOverflow)

    assertEquals(1, log.numberOfSegments)

    val firstBatchBaseOffset = records.head.batches.asScala.head.baseOffset
    assertEquals(firstBatchBaseOffset, log.activeSegment.baseOffset)
    UnifiedLogTest.verifyRecordsInLog(log, allRecordsBeforeSplit)

    assertFalse(LogTestUtils.hasOffsetOverflow(log))
  }

  @Test
  def testDeleteOldSegments(): Unit = {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds - 1000)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, segmentIndexBytes = 1000, retentionMs = 999)
    val log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, 0)

    log.assignEpochStartOffset(0, 40)
    log.assignEpochStartOffset(1, 90)

    // segments are not eligible for deletion if no high watermark has been set
    val numSegments = log.numberOfSegments
    log.deleteOldSegments()
    assertEquals(numSegments, log.numberOfSegments)
    assertEquals(0L, log.logStartOffset)

    // only segments with offset before the current high watermark are eligible for deletion
    for (hw <- 25 to 30) {
      log.updateHighWatermark(hw)
      log.deleteOldSegments()
      assertTrue(log.logStartOffset <= hw)
      log.logSegments.forEach { segment =>
        val segmentFetchInfo = segment.read(segment.baseOffset, Int.MaxValue)
        val segmentLastOffsetOpt = segmentFetchInfo.records.records.asScala.lastOption.map(_.offset)
        segmentLastOffsetOpt.foreach { lastOffset =>
          assertTrue(lastOffset >= hw)
        }
      }
    }

    // expire all segments
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    assertEquals(1, log.numberOfSegments, "The deleted segments should be gone.")
    assertEquals(1, epochCache(log).epochEntries.size, "Epoch entries should have gone.")
    assertEquals(new EpochEntry(1, 100), epochCache(log).epochEntries.get(0), "Epoch entry should be the latest epoch and the leo.")

    // append some messages to create some segments
    for (_ <- 0 until 100)
      log.appendAsLeader(createRecords, 0)

    log.delete()
    assertEquals(0, log.numberOfSegments, "The number of segments should be 0")
    assertEquals(0, log.deleteOldSegments(), "The number of deleted segments should be zero.")
    assertEquals(0, epochCache(log).epochEntries.size, "Epoch entries should have gone.")
  }

  @Test
  def testLogDeletionAfterClose(): Unit = {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds - 1000)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, segmentIndexBytes = 1000, retentionMs = 999)
    val log = createLog(logDir, logConfig)

    // append some messages to create some segments
    log.appendAsLeader(createRecords, 0)

    assertEquals(1, log.numberOfSegments, "The deleted segments should be gone.")
    assertEquals(1, epochCache(log).epochEntries.size, "Epoch entries should have gone.")

    log.close()
    log.delete()
    assertEquals(0, log.numberOfSegments, "The number of segments should be 0")
    assertEquals(0, epochCache(log).epochEntries.size, "Epoch entries should have gone.")
  }

  @Test
  def testLogDeletionAfterDeleteRecords(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5)
    val log = createLog(logDir, logConfig)

    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, 0)
    assertEquals(3, log.numberOfSegments, "should have 3 segments")
    assertEquals(log.logStartOffset, 0)
    log.updateHighWatermark(log.logEndOffset)

    log.maybeIncrementLogStartOffset(1, LogStartOffsetIncrementReason.ClientRecordDeletion)
    log.deleteOldSegments()
    assertEquals(3, log.numberOfSegments, "should have 3 segments")
    assertEquals(log.logStartOffset, 1)

    log.maybeIncrementLogStartOffset(6, LogStartOffsetIncrementReason.ClientRecordDeletion)
    log.deleteOldSegments()
    assertEquals(2, log.numberOfSegments, "should have 2 segments")
    assertEquals(log.logStartOffset, 6)

    log.maybeIncrementLogStartOffset(15, LogStartOffsetIncrementReason.ClientRecordDeletion)
    log.deleteOldSegments()
    assertEquals(1, log.numberOfSegments, "should have 1 segments")
    assertEquals(log.logStartOffset, 15)
  }

  def epochCache(log: UnifiedLog): LeaderEpochFileCache = log.leaderEpochCache

  @Test
  def shouldDeleteSizeBasedSegments(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionBytes = createRecords.sizeInBytes * 10)
    val log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, 0)

    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    assertEquals(2,log.numberOfSegments, "should have 2 segments")
  }

  @Test
  def shouldNotDeleteSizeBasedSegmentsWhenUnderRetentionSize(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionBytes = createRecords.sizeInBytes * 15)
    val log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, 0)

    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    assertEquals(3,log.numberOfSegments, "should have 3 segments")
  }

  @Test
  def shouldDeleteTimeBasedSegmentsReadyToBeDeleted(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes, timestamp = 10)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionMs = 10000)
    val log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, 0)

    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    assertEquals(1, log.numberOfSegments, "There should be 1 segment remaining")
  }

  @Test
  def shouldNotDeleteTimeBasedSegmentsWhenNoneReadyToBeDeleted(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes, timestamp = mockTime.milliseconds)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionMs = 10000000)
    val log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, 0)

    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    assertEquals(3, log.numberOfSegments, "There should be 3 segments remaining")
  }

  @Test
  def shouldNotDeleteSegmentsWhenPolicyDoesNotIncludeDelete(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes(), timestamp = 10L)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionMs = 10000, cleanupPolicy = "compact")
    val log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, 0)

    // mark the oldest segment as older the retention.ms
    log.logSegments.asScala.head.setLastModified(mockTime.milliseconds - 20000)

    val segments = log.numberOfSegments
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    assertEquals(segments, log.numberOfSegments, "There should be 3 segments remaining")
  }

  @Test
  def shouldDeleteLocalLogSegmentsWhenPolicyIsEmptyWithSizeRetention(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes(), timestamp = 10L)
    val recordSize = createRecords.sizeInBytes
    val logConfig = LogTestUtils.createLogConfig(
      segmentBytes = recordSize * 2,
      localRetentionBytes = recordSize / 2,
      cleanupPolicy = "",
      remoteLogStorageEnable = true
    )
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)

    for (_ <- 0 until 10)
      log.appendAsLeader(createRecords, 0)

    val segmentsBefore = log.numberOfSegments
    log.updateHighWatermark(log.logEndOffset)
    log.updateHighestOffsetInRemoteStorage(log.logEndOffset - 1)
    val deleteOldSegments = log.deleteOldSegments()

    assertTrue(log.numberOfSegments < segmentsBefore, "Some segments should be deleted due to size retention")
    assertTrue(deleteOldSegments > 0, "At least one segment should be deleted")
  }

  @Test
  def shouldDeleteLocalLogSegmentsWhenPolicyIsEmptyWithMsRetention(): Unit = {
    val oldTimestamp = mockTime.milliseconds - 20000
    def oldRecords = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes(), timestamp = oldTimestamp)
    val recordSize = oldRecords.sizeInBytes
    val logConfig = LogTestUtils.createLogConfig(
      segmentBytes = recordSize * 2,
      localRetentionMs = 5000,
      cleanupPolicy = "",
      remoteLogStorageEnable = true
    )
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)

    for (_ <- 0 until 10)
      log.appendAsLeader(oldRecords, 0)

    def newRecords = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes(), timestamp = mockTime.milliseconds)
    for (_ <- 0 until 5)
      log.appendAsLeader(newRecords, 0)

    val segmentsBefore = log.numberOfSegments

    log.updateHighWatermark(log.logEndOffset)
    log.updateHighestOffsetInRemoteStorage(log.logEndOffset - 1)
    val deleteOldSegments = log.deleteOldSegments()

    assertTrue(log.numberOfSegments < segmentsBefore, "Some segments should be deleted due to time retention")
    assertTrue(deleteOldSegments > 0, "At least one segment should be deleted")
  }

  @Test
  def shouldDeleteSegmentsReadyToBeDeletedWhenCleanupPolicyIsCompactAndDelete(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes, timestamp = 10L)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionMs = 10000, cleanupPolicy = "compact,delete")
    val log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, 0)

    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    assertEquals(1, log.numberOfSegments, "There should be 1 segment remaining")
  }

  @Test
  def shouldDeleteStartOffsetBreachedSegmentsWhenPolicyDoesNotIncludeDelete(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes, timestamp = 10L)
    val recordsPerSegment = 5
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * recordsPerSegment, retentionMs = 10000, cleanupPolicy = "compact")
    val log = createLog(logDir, logConfig, brokerTopicStats)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, 0)

    // Three segments should be created
    assertEquals(3, log.logSegments.asScala.count(_ => true))
    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(recordsPerSegment, LogStartOffsetIncrementReason.ClientRecordDeletion)

    // The first segment, which is entirely before the log start offset, should be deleted
    // Of the remaining the segments, the first can overlap the log start offset and the rest must have a base offset
    // greater than the start offset
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    assertEquals(2, log.numberOfSegments, "There should be 2 segments remaining")
    assertTrue(log.logSegments.asScala.head.baseOffset <= log.logStartOffset)
    assertTrue(log.logSegments.asScala.tail.forall(s => s.baseOffset > log.logStartOffset))
  }

  @Test
  def shouldApplyEpochToMessageOnAppendIfLeader(): Unit = {
    val records = (0 until 50).toArray.map(id => new SimpleRecord(id.toString.getBytes))

    //Given this partition is on leader epoch 72
    val epoch = 72
    val log = createLog(logDir, new LogConfig(new Properties))
    log.assignEpochStartOffset(epoch, records.length)

    //When appending messages as a leader (i.e. assignOffsets = true)
    for (record <- records)
      log.appendAsLeader(
        MemoryRecords.withRecords(Compression.NONE, record),
        epoch
      )

    //Then leader epoch should be set on messages
    for (i <- records.indices) {
      val read = LogTestUtils.readLog(log, i, 1).records.batches.iterator.next()
      assertEquals(72, read.partitionLeaderEpoch, "Should have set leader epoch")
    }
  }

  @Test
  def followerShouldSaveEpochInformationFromReplicatedMessagesToTheEpochCache(): Unit = {
    val messageIds = (0 until 50).toArray
    val records = messageIds.map(id => new SimpleRecord(id.toString.getBytes))

    //Given each message has an offset & epoch, as msgs from leader would
    def recordsForEpoch(i: Int): MemoryRecords = {
      val recs = MemoryRecords.withRecords(messageIds(i), Compression.NONE, records(i))
      recs.batches.forEach{record =>
        record.setPartitionLeaderEpoch(42)
        record.setLastOffset(i)
      }
      recs
    }

    val log = createLog(logDir, new LogConfig(new Properties))

    //When appending as follower (assignOffsets = false)
    for (i <- records.indices)
      log.appendAsFollower(recordsForEpoch(i), i)

    assertEquals(Optional.of(42), log.latestEpoch)
  }

  @Test
  def shouldTruncateLeaderEpochsWhenDeletingSegments(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionBytes = createRecords.sizeInBytes * 10)
    val log = createLog(logDir, logConfig)
    val cache = epochCache(log)

    // Given three segments of 5 messages each
    for (_ <- 0 until 15) {
      log.appendAsLeader(createRecords, 0)
    }

    //Given epochs
    cache.assign(0, 0)
    cache.assign(1, 5)
    cache.assign(2, 10)

    //When first segment is removed
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()

    //The oldest epoch entry should have been removed
    assertEquals(util.List.of(new EpochEntry(1, 5), new EpochEntry(2, 10)), cache.epochEntries)
  }

  @Test
  def shouldUpdateOffsetForLeaderEpochsWhenDeletingSegments(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionBytes = createRecords.sizeInBytes * 10)
    val log = createLog(logDir, logConfig)
    val cache = epochCache(log)

    // Given three segments of 5 messages each
    for (_ <- 0 until 15) {
      log.appendAsLeader(createRecords, 0)
    }

    //Given epochs
    cache.assign(0, 0)
    cache.assign(1, 7)
    cache.assign(2, 10)

    //When first segment removed (up to offset 5)
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()

    //The first entry should have gone from (0,0) => (0,5)
    assertEquals(util.List.of(new EpochEntry(0, 5), new EpochEntry(1, 7), new EpochEntry(2, 10)), cache.epochEntries)
  }

  @Test
  def shouldTruncateLeaderEpochCheckpointFileWhenTruncatingLog(): Unit = {
    def createRecords(startOffset: Long, epoch: Int): MemoryRecords = {
      TestUtils.records(Seq(new SimpleRecord("value".getBytes)),
        baseOffset = startOffset, partitionLeaderEpoch = epoch)
    }

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 10 * createRecords(0, 0).sizeInBytes)
    val log = createLog(logDir, logConfig)
    val cache = epochCache(log)

    def append(epoch: Int, startOffset: Long, count: Int): Unit = {
      for (i <- 0 until count)
        log.appendAsFollower(createRecords(startOffset + i, epoch), epoch)
    }

    //Given 2 segments, 10 messages per segment
    append(epoch = 0, startOffset = 0, count = 10)
    append(epoch = 1, startOffset = 10, count = 6)
    append(epoch = 2, startOffset = 16, count = 4)

    assertEquals(2, log.numberOfSegments)
    assertEquals(20, log.logEndOffset)

    //When truncate to LEO (no op)
    log.truncateTo(log.logEndOffset)

    //Then no change
    assertEquals(3, cache.epochEntries.size)

    //When truncate
    log.truncateTo(11)

    //Then no change
    assertEquals(2, cache.epochEntries.size)

    //When truncate
    log.truncateTo(10)

    //Then
    assertEquals(1, cache.epochEntries.size)

    //When truncate all
    log.truncateTo(0)

    //Then
    assertEquals(0, cache.epochEntries.size)
  }

  @Test
  def testFirstUnstableOffsetNoTransactionalData(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    val records = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord("foo".getBytes),
      new SimpleRecord("bar".getBytes),
      new SimpleRecord("baz".getBytes))

    log.appendAsLeader(records, 0)
    assertEquals(Optional.empty, log.firstUnstableOffset)
  }

  @Test
  def testFirstUnstableOffsetWithTransactionalData(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    val pid = 137L
    val epoch = 5.toShort
    var seq = 0

    // add some transactional records
    val records = MemoryRecords.withTransactionalRecords(Compression.NONE, pid, epoch, seq,
      new SimpleRecord("foo".getBytes),
      new SimpleRecord("bar".getBytes),
      new SimpleRecord("baz".getBytes))

    val firstAppendInfo = log.appendAsLeader(records, 0)
    assertEquals(Optional.of(firstAppendInfo.firstOffset), log.firstUnstableOffset)

    // add more transactional records
    seq += 3
    log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.NONE, pid, epoch, seq,
      new SimpleRecord("blah".getBytes)), 0)

    // LSO should not have changed
    assertEquals(Optional.of(firstAppendInfo.firstOffset), log.firstUnstableOffset)

    // now transaction is committed
    val commitAppendInfo = LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.COMMIT,
      mockTime.milliseconds(), transactionVersion = TransactionVersion.TV_0.featureLevel())

    // first unstable offset is not updated until the high watermark is advanced
    assertEquals(Optional.of(firstAppendInfo.firstOffset), log.firstUnstableOffset)
    log.updateHighWatermark(commitAppendInfo.lastOffset + 1)

    // now there should be no first unstable offset
    assertEquals(Optional.empty, log.firstUnstableOffset)
  }

  @Test
  def testReadCommittedWithConcurrentHighWatermarkUpdates(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)
    val lastOffset = 50L

    val producerEpoch = 0.toShort
    val producerId = 15L
    val appendProducer = LogTestUtils.appendTransactionalAsLeader(log, producerId, producerEpoch, mockTime)

    // Thread 1 writes single-record transactions and attempts to read them
    // before they have been aborted, and then aborts them
    val txnWriteAndReadLoop: Callable[Int] = () => {
      var nonEmptyReads = 0
      while (log.logEndOffset < lastOffset) {
        val currentLogEndOffset = log.logEndOffset

        appendProducer(1)

        val readInfo = log.read(currentLogEndOffset, Int.MaxValue, FetchIsolation.TXN_COMMITTED, false)

        if (readInfo.records.sizeInBytes() > 0)
          nonEmptyReads += 1

        LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, producerEpoch, ControlRecordType.ABORT,
          mockTime.milliseconds(), transactionVersion = TransactionVersion.TV_0.featureLevel())
      }
      nonEmptyReads
    }

    // Thread 2 watches the log and updates the high watermark
    val hwUpdateLoop: Runnable = () => {
      while (log.logEndOffset < lastOffset) {
        log.updateHighWatermark(log.logEndOffset)
      }
    }

    val executor = Executors.newFixedThreadPool(2)
    try {
      executor.submit(hwUpdateLoop)

      val future = executor.submit(txnWriteAndReadLoop)
      val nonEmptyReads = future.get()

      assertEquals(0, nonEmptyReads)
    } finally {
      executor.shutdownNow()
    }
  }

  @Test
  def testTransactionIndexUpdated(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)
    val epoch = 0.toShort

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = LogTestUtils.appendTransactionalAsLeader(log, pid1, epoch, mockTime)
    val appendPid2 = LogTestUtils.appendTransactionalAsLeader(log, pid2, epoch, mockTime)
    val appendPid3 = LogTestUtils.appendTransactionalAsLeader(log, pid3, epoch, mockTime)
    val appendPid4 = LogTestUtils.appendTransactionalAsLeader(log, pid4, epoch, mockTime)

    // mix transactional and non-transactional data
    appendPid1(5) // nextOffset: 5
    LogTestUtils.appendNonTransactionalAsLeader(log, 3) // 8
    appendPid2(2) // 10
    appendPid1(4) // 14
    appendPid3(3) // 17
    LogTestUtils.appendNonTransactionalAsLeader(log, 2) // 19
    appendPid1(10) // 29
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT,
      mockTime.milliseconds(), transactionVersion = TransactionVersion.TV_0.featureLevel()) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT,
      mockTime.milliseconds(), transactionVersion = TransactionVersion.TV_0.featureLevel()) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT,
      mockTime.milliseconds(), transactionVersion = TransactionVersion.TV_0.featureLevel()) // 75
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT,
      mockTime.milliseconds(), transactionVersion = TransactionVersion.TV_0.featureLevel()) // 90

    val abortedTransactions = LogTestUtils.allAbortedTransactions(log)
    val expectedTransactions = List(
      new AbortedTxn(pid1, 0L, 29L, 8L),
      new AbortedTxn(pid2, 8L, 74L, 36L)
    )
    assertEquals(expectedTransactions, abortedTransactions)

    // Verify caching of the segment position of the first unstable offset
    log.updateHighWatermark(30L)
    assertCachedFirstUnstableOffset(log, expectedOffset = 8L)

    log.updateHighWatermark(75L)
    assertCachedFirstUnstableOffset(log, expectedOffset = 36L)

    log.updateHighWatermark(log.logEndOffset)
    assertEquals(Optional.empty, log.firstUnstableOffset)
  }

  @Test
  def testTransactionIndexUpdatedThroughReplication(): Unit = {
    val epoch = 0.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)
    val buffer = ByteBuffer.allocate(2048)

    val pid1 = 1L
    val pid2 = 2L
    val pid3 = 3L
    val pid4 = 4L

    val appendPid1 = appendTransactionalToBuffer(buffer, pid1, epoch)
    val appendPid2 = appendTransactionalToBuffer(buffer, pid2, epoch)
    val appendPid3 = appendTransactionalToBuffer(buffer, pid3, epoch)
    val appendPid4 = appendTransactionalToBuffer(buffer, pid4, epoch)

    appendPid1(0L, 5)
    appendNonTransactionalToBuffer(buffer, 5L, 3)
    appendPid2(8L, 2)
    appendPid1(10L, 4)
    appendPid3(14L, 3)
    appendNonTransactionalToBuffer(buffer, 17L, 2)
    appendPid1(19L, 10)
    appendEndTxnMarkerToBuffer(buffer, pid1, epoch, 29L, ControlRecordType.ABORT)
    appendPid2(30L, 6)
    appendPid4(36L, 3)
    appendNonTransactionalToBuffer(buffer, 39L, 10)
    appendPid3(49L, 9)
    appendEndTxnMarkerToBuffer(buffer, pid3, epoch, 58L, ControlRecordType.COMMIT)
    appendPid4(59L, 8)
    appendPid2(67L, 7)
    appendEndTxnMarkerToBuffer(buffer, pid2, epoch, 74L, ControlRecordType.ABORT)
    appendNonTransactionalToBuffer(buffer, 75L, 10)
    appendPid4(85L, 4)
    appendEndTxnMarkerToBuffer(buffer, pid4, epoch, 89L, ControlRecordType.COMMIT)

    buffer.flip()

    appendAsFollower(log, MemoryRecords.readableRecords(buffer), epoch)

    val abortedTransactions = LogTestUtils.allAbortedTransactions(log)
    val expectedTransactions = List(
      new AbortedTxn(pid1, 0L, 29L, 8L),
      new AbortedTxn(pid2, 8L, 74L, 36L)
    )

    assertEquals(expectedTransactions, abortedTransactions)

    // Verify caching of the segment position of the first unstable offset
    log.updateHighWatermark(30L)
    assertCachedFirstUnstableOffset(log, expectedOffset = 8L)

    log.updateHighWatermark(75L)
    assertCachedFirstUnstableOffset(log, expectedOffset = 36L)

    log.updateHighWatermark(log.logEndOffset)
    assertEquals(Optional.empty, log.firstUnstableOffset)
  }

  private def assertCachedFirstUnstableOffset(log: UnifiedLog, expectedOffset: Long): Unit = {
    assertTrue(log.producerStateManager.firstUnstableOffset.isPresent)
    val firstUnstableOffset = log.producerStateManager.firstUnstableOffset.get
    assertEquals(expectedOffset, firstUnstableOffset.messageOffset)
    assertFalse(firstUnstableOffset.messageOffsetOnly)
    assertValidLogOffsetMetadata(log, firstUnstableOffset)
  }

  private def assertValidLogOffsetMetadata(log: UnifiedLog, offsetMetadata: LogOffsetMetadata): Unit = {
    assertFalse(offsetMetadata.messageOffsetOnly)

    val segmentBaseOffset = offsetMetadata.segmentBaseOffset
    val segments = log.logSegments(segmentBaseOffset, segmentBaseOffset + 1)
    assertFalse(segments.isEmpty)

    val segment = segments.iterator().next()
    assertEquals(segmentBaseOffset, segment.baseOffset)
    assertTrue(offsetMetadata.relativePositionInSegment <= segment.size)

    val readInfo = segment.read(offsetMetadata.messageOffset,
      2048,
      Optional.of(segment.size),
      false)

    if (offsetMetadata.relativePositionInSegment < segment.size)
      assertEquals(offsetMetadata, readInfo.fetchOffsetMetadata)
    else
      assertNull(readInfo)
  }

  @Test
  def testZombieCoordinatorFenced(): Unit = {
    val pid = 1L
    val epoch = 0.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    val append = LogTestUtils.appendTransactionalAsLeader(log, pid, epoch, mockTime)

    append(10)
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(),
      coordinatorEpoch = 1, transactionVersion = TransactionVersion.TV_0.featureLevel())

    append(5)
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.COMMIT, mockTime.milliseconds(),
      coordinatorEpoch = 2, transactionVersion = TransactionVersion.TV_0.featureLevel())

    assertThrows(
      classOf[TransactionCoordinatorFencedException],
      () => LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(),
        coordinatorEpoch = 1, transactionVersion = TransactionVersion.TV_0.featureLevel()))
  }

  @Test
  def testZombieCoordinatorFencedEmptyTransaction(): Unit = {
    val pid = 1L
    val epoch = 0.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    val buffer = ByteBuffer.allocate(256)
    val append = appendTransactionalToBuffer(buffer, pid, epoch, 1)
    append(0, 10)
    appendEndTxnMarkerToBuffer(buffer, pid, epoch, 10L, ControlRecordType.COMMIT, 1)

    buffer.flip()
    log.appendAsFollower(MemoryRecords.readableRecords(buffer), epoch)

    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(),
      coordinatorEpoch = 2, leaderEpoch = 1, transactionVersion = TransactionVersion.TV_0.featureLevel())
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(),
      coordinatorEpoch = 2, leaderEpoch = 1, transactionVersion = TransactionVersion.TV_0.featureLevel())
    assertThrows(classOf[TransactionCoordinatorFencedException],
      () => LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(),
        coordinatorEpoch = 1, leaderEpoch = 1, transactionVersion = TransactionVersion.TV_0.featureLevel()))
  }

  @ParameterizedTest(name = "testEndTxnWithFencedProducerEpoch with transactionVersion={0}")
  @ValueSource(shorts = Array(1, 2))
  def testEndTxnWithFencedProducerEpoch(transactionVersion: Short): Unit = {
    val producerId = 1L
    val epoch = 5.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)
    
    // First, write some transactional records to establish the current epoch
    val records = MemoryRecords.withTransactionalRecords(
      Compression.NONE, producerId, epoch, 0,
      new SimpleRecord("key".getBytes, "value".getBytes)
    )
    log.appendAsLeader(records, 0, AppendOrigin.CLIENT, RequestLocal.noCaching(), VerificationGuard.SENTINEL, transactionVersion)
    
    // Test 1: Old epoch (epoch - 1) should be rejected for both TV0/TV1 and TV2
    // TV0/TV1: markerEpoch < currentEpoch is rejected
    // TV2: markerEpoch <= currentEpoch is rejected (requires strict >)
    assertThrows(classOf[InvalidProducerEpochException],
      () => LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, (epoch - 1).toShort, 
        ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 1, 
        leaderEpoch = 0, transactionVersion = transactionVersion))
    
    // Test 2: Same epoch behavior differs between TV0/TV1 and TV2
    // TV0/TV1: same epoch is allowed (markerEpoch >= currentEpoch)
    // TV2: same epoch is rejected (requires strict >, markerEpoch > currentEpoch)
    if (transactionVersion >= 2) {
      // TV2: same epoch should be rejected
      assertThrows(classOf[InvalidProducerEpochException],
        () => LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, epoch, 
          ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 1, 
          leaderEpoch = 0, transactionVersion = transactionVersion))
    } else {
      // TV0/TV1: same epoch should be allowed
      assertDoesNotThrow(() => LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, epoch, 
        ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 1, 
        leaderEpoch = 0, transactionVersion = transactionVersion))
    }
  }

  @Test
  def testTV2MarkerWithBumpedEpochSucceeds(): Unit = {
    // Test that TV2 markers with bumped epochs (epoch + 1) are accepted (positive case)
    // TV2 (KIP-890): Coordinator bumps epoch before writing marker, so markerEpoch = currentEpoch + 1
    val transactionVersion: Short = 2
    val producerId = 1L
    val epoch = 5.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)
    
    // First, write some transactional records to establish the current epoch
    val records = MemoryRecords.withTransactionalRecords(
      Compression.NONE, producerId, epoch, 0,
      new SimpleRecord("key".getBytes, "value".getBytes)
    )
    log.appendAsLeader(records, 0, AppendOrigin.CLIENT, RequestLocal.noCaching(), VerificationGuard.SENTINEL, transactionVersion)
    
    // TV2: Verify that bumped epoch (epoch + 1) is accepted
    val bumpedEpoch = (epoch + 1).toShort
    assertDoesNotThrow(() => LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, bumpedEpoch,
      ControlRecordType.COMMIT, mockTime.milliseconds(), coordinatorEpoch = 1,
      leaderEpoch = 0, transactionVersion = TransactionVersion.TV_2.featureLevel()))
    
    // Verify the marker was successfully appended by checking producer state
    val producerState = log.producerStateManager.activeProducers.get(producerId)
    assertNotNull(producerState)
    // After a commit marker, the producer epoch should be updated to the bumped epoch for TV2
    assertEquals(bumpedEpoch, producerState.producerEpoch)
  }

  @Test
  def testReplicationWithTVUnknownAllowed(): Unit = {
    // Test that TV_UNKNOWN is allowed for replication (REPLICATION origin) and uses TV_0 validation
    // This simulates the scenario where:
    // 1. Leader receives WriteTxnMarkersRequest with transactionVersion=2 and validates with strict TV2 rules
    // 2. Leader writes MemoryRecords to log (transactionVersion is not stored in MemoryRecords)
    // 3. Follower receives MemoryRecords via replication (without transactionVersion metadata)
    // 4. Follower uses TV_UNKNOWN which defaults to TV_0 validation (more permissive, safe because leader already validated)
    
    val producerId = 1L
    val epoch = 5.toShort
    val coordinatorEpoch = 1
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)
    
    // Step 1: Write transactional records as leader to establish current epoch
    val transactionalRecords = MemoryRecords.withTransactionalRecords(
      Compression.NONE, producerId, epoch, 0,
      new SimpleRecord("key".getBytes, "value".getBytes)
    )
    log.appendAsLeader(transactionalRecords, 0, AppendOrigin.CLIENT, RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_2.featureLevel())
    
    // Step 2: Simulate leader writing TV2 marker with bumped epoch (epoch + 1)
    // This is what happens at the leader when WriteTxnMarkersRequest is received
    val bumpedEpoch = (epoch + 1).toShort
    val leaderMarker = MemoryRecords.withEndTransactionMarker(
      mockTime.milliseconds(),
      producerId,
      bumpedEpoch,
      new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch)
    )
    // Leader validates with TV2 (strict: markerEpoch > currentEpoch)
    log.appendAsLeader(leaderMarker, 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_2.featureLevel())
    
    // Verify leader state
    val leaderProducerState = log.producerStateManager.activeProducers.get(producerId)
    assertNotNull(leaderProducerState)
    assertEquals(bumpedEpoch, leaderProducerState.producerEpoch)
    
    // Step 3: Create a new log to simulate a follower
    val followerLogDir = TestUtils.randomPartitionLogDir(tmpDir)
    val followerLog = createLog(followerLogDir, logConfig)
    
    // Step 4: Follower replicates transactional records first
    val followerTransactionalRecords = MemoryRecords.withTransactionalRecords(
      0L,
      Compression.NONE, producerId, epoch, 0,
      0,
      new SimpleRecord("key".getBytes, "value".getBytes)
    )
    followerLog.appendAsFollower(followerTransactionalRecords, 0)
    
    // Step 5: Follower replicates the marker (appendAsFollower uses TV_UNKNOWN internally)
    // This should succeed because TV_UNKNOWN is allowed for REPLICATION origin
    // and defaults to TV_0 validation (markerEpoch >= currentEpoch), which is more permissive
    // The marker should be at offset 1 (after the transactional record at offset 0)
    val followerMarker = MemoryRecords.withEndTransactionMarker(
      1L, // offset after the transactional record
      mockTime.milliseconds(),
      0, // partition leader epoch
      producerId,
      bumpedEpoch,
      new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch)
    )
    
    // This should not throw an exception - TV_UNKNOWN is allowed for replication
    assertDoesNotThrow(() => followerLog.appendAsFollower(followerMarker, 0))
    
    // Verify follower state matches leader state
    val followerProducerState = followerLog.producerStateManager.activeProducers.get(producerId)
    assertNotNull(followerProducerState)
    assertEquals(bumpedEpoch, followerProducerState.producerEpoch)
    assertEquals(coordinatorEpoch, followerProducerState.coordinatorEpoch)
    
    // Verify the marker was written to the follower log
    assertEquals(2L, followerLog.logEndOffset) // 1 transactional record + 1 marker
  }

  @Test
  def testLeaderRejectsTVUnknownForTransactionMarker(): Unit = {
    // Test that TV_UNKNOWN is rejected for COORDINATOR origin (leader writing transaction markers)
    // TV_UNKNOWN is only allowed for REPLICATION origin (followers)
    val producerId = 1L
    val epoch = 5.toShort
    val coordinatorEpoch = 1
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)
    
    // Write transactional records as leader to establish current epoch
    val transactionalRecords = MemoryRecords.withTransactionalRecords(
      Compression.NONE, producerId, epoch, 0,
      new SimpleRecord("key".getBytes, "value".getBytes)
    )
    log.appendAsLeader(transactionalRecords, 0, AppendOrigin.CLIENT, RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_2.featureLevel())
    
    // Attempt to write a transaction marker with TV_UNKNOWN as COORDINATOR (leader)
    // This should throw IllegalArgumentException because TV_UNKNOWN is not allowed for COORDINATOR origin
    val marker = MemoryRecords.withEndTransactionMarker(
      mockTime.milliseconds(),
      producerId,
      (epoch + 1).toShort, // bumped epoch for TV2
      new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch)
    )
    
    val exception = assertThrows(classOf[IllegalArgumentException], () => {
      log.appendAsLeader(marker, 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_UNKNOWN)
    })
    
    assertTrue(exception.getMessage.contains("transactionVersion must be explicitly specified"))
    assertTrue(exception.getMessage.contains("TV_UNKNOWN"))
    assertTrue(exception.getMessage.contains("COORDINATOR"))
  }

  @Test
  def testLastStableOffsetDoesNotExceedLogStartOffsetMidSegment(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)
    val epoch = 0.toShort
    val pid = 1L
    val appendPid = LogTestUtils.appendTransactionalAsLeader(log, pid, epoch, mockTime)

    appendPid(5)
    LogTestUtils.appendNonTransactionalAsLeader(log, 3)
    assertEquals(8L, log.logEndOffset)

    log.roll()
    assertEquals(2, log.logSegments.size)
    appendPid(5)

    assertEquals(Optional.of(0L), log.firstUnstableOffset)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(5L, LogStartOffsetIncrementReason.ClientRecordDeletion)

    // the first unstable offset should be lower bounded by the log start offset
    assertEquals(Optional.of(5L), log.firstUnstableOffset)
  }

  @Test
  def testLastStableOffsetDoesNotExceedLogStartOffsetAfterSegmentDeletion(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)
    val epoch = 0.toShort
    val pid = 1L
    val appendPid = LogTestUtils.appendTransactionalAsLeader(log, pid, epoch, mockTime)

    appendPid(5)
    LogTestUtils.appendNonTransactionalAsLeader(log, 3)
    assertEquals(8L, log.logEndOffset)

    log.roll()
    assertEquals(2, log.logSegments.size)
    appendPid(5)

    assertEquals(Optional.of(0L), log.firstUnstableOffset)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(8L, LogStartOffsetIncrementReason.ClientRecordDeletion)
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    assertEquals(1, log.logSegments.size)

    // the first unstable offset should be lower bounded by the log start offset
    assertEquals(Optional.of(8L), log.firstUnstableOffset)
  }

  @Test
  def testAppendToTransactionIndexFailure(): Unit = {
    val pid = 1L
    val epoch = 0.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    val append = LogTestUtils.appendTransactionalAsLeader(log, pid, epoch, mockTime)
    append(10)

    // Kind of a hack, but renaming the index to a directory ensures that the append
    // to the index will fail.
    log.activeSegment.txnIndex.renameTo(log.dir)

    // The append will be written to the log successfully, but the write to the index will fail
    assertThrows(
      classOf[KafkaStorageException],
      () => LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(),
        coordinatorEpoch = 1, transactionVersion = TransactionVersion.TV_0.featureLevel()))
    assertEquals(11L, log.logEndOffset)
    assertEquals(0L, log.lastStableOffset)

    // Try the append a second time. The appended offset in the log should not increase
    // because the log dir is marked as failed.  Nor will there be a write to the transaction
    // index.
    assertThrows(
      classOf[KafkaStorageException],
      () => LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(),
        coordinatorEpoch = 1, transactionVersion = TransactionVersion.TV_0.featureLevel()))
    assertEquals(11L, log.logEndOffset)
    assertEquals(0L, log.lastStableOffset)

    // Even if the high watermark is updated, the first unstable offset does not move
    log.updateHighWatermark(12L)
    assertEquals(0L, log.lastStableOffset)

    assertThrows(classOf[KafkaStorageException], () => log.close())
    val reopenedLog = createLog(logDir, logConfig, lastShutdownClean = false)
    assertEquals(11L, reopenedLog.logEndOffset)
    assertEquals(1, reopenedLog.activeSegment.txnIndex.allAbortedTxns.size)
    reopenedLog.updateHighWatermark(12L)
    assertEquals(Optional.empty, reopenedLog.firstUnstableOffset)
  }

  @Test
  def testOffsetSnapshot(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    // append a few records
    appendAsFollower(
      log,
      MemoryRecords.withRecords(
        Compression.NONE,
        new SimpleRecord("a".getBytes),
        new SimpleRecord("b".getBytes),
        new SimpleRecord("c".getBytes)
      ),
      5
    )


    log.updateHighWatermark(3L)
    var offsets: LogOffsetSnapshot = log.fetchOffsetSnapshot
    assertEquals(offsets.highWatermark.messageOffset, 3L)
    assertFalse(offsets.highWatermark.messageOffsetOnly)

    offsets = log.fetchOffsetSnapshot
    assertEquals(offsets.highWatermark.messageOffset, 3L)
    assertFalse(offsets.highWatermark.messageOffsetOnly)
  }

  @Test
  def testLastStableOffsetWithMixedProducerData(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    // for convenience, both producers share the same epoch
    val epoch = 5.toShort

    val pid1 = 137L
    val seq1 = 0
    val pid2 = 983L
    val seq2 = 0

    // add some transactional records
    val firstAppendInfo = log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.NONE, pid1, epoch, seq1,
      new SimpleRecord("a".getBytes),
      new SimpleRecord("b".getBytes),
      new SimpleRecord("c".getBytes)), 0)
    assertEquals(Optional.of(firstAppendInfo.firstOffset), log.firstUnstableOffset)

    // mix in some non-transactional data
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord("g".getBytes),
      new SimpleRecord("h".getBytes),
      new SimpleRecord("i".getBytes)), 0)

    // append data from a second transactional producer
    val secondAppendInfo = log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.NONE, pid2, epoch, seq2,
      new SimpleRecord("d".getBytes),
      new SimpleRecord("e".getBytes),
      new SimpleRecord("f".getBytes)), 0)

    // LSO should not have changed
    assertEquals(Optional.of(firstAppendInfo.firstOffset), log.firstUnstableOffset)

    // now first producer's transaction is aborted
    val abortAppendInfo = LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT,
      mockTime.milliseconds(), transactionVersion = TransactionVersion.TV_0.featureLevel())
    log.updateHighWatermark(abortAppendInfo.lastOffset + 1)

    // LSO should now point to one less than the first offset of the second transaction
    assertEquals(Optional.of(secondAppendInfo.firstOffset), log.firstUnstableOffset)

    // commit the second transaction
    val commitAppendInfo = LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.COMMIT,
      mockTime.milliseconds(), transactionVersion = TransactionVersion.TV_0.featureLevel())
    log.updateHighWatermark(commitAppendInfo.lastOffset + 1)

    // now there should be no first unstable offset
    assertEquals(Optional.empty, log.firstUnstableOffset)
  }

  @Test
  def testAbortedTransactionSpanningMultipleSegments(): Unit = {
    val pid = 137L
    val epoch = 5.toShort
    var seq = 0

    val records = MemoryRecords.withTransactionalRecords(Compression.NONE, pid, epoch, seq,
      new SimpleRecord("a".getBytes),
      new SimpleRecord("b".getBytes),
      new SimpleRecord("c".getBytes))

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = records.sizeInBytes)
    val log = createLog(logDir, logConfig)

    val firstAppendInfo = log.appendAsLeader(records, 0)
    assertEquals(Optional.of(firstAppendInfo.firstOffset), log.firstUnstableOffset)

    // this write should spill to the second segment
    seq = 3
    log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.NONE, pid, epoch, seq,
      new SimpleRecord("d".getBytes),
      new SimpleRecord("e".getBytes),
      new SimpleRecord("f".getBytes)), 0)
    assertEquals(Optional.of(firstAppendInfo.firstOffset), log.firstUnstableOffset)
    assertEquals(3L, log.logEndOffsetMetadata.segmentBaseOffset)

    // now abort the transaction
    val abortAppendInfo = LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT,
      mockTime.milliseconds(), transactionVersion = TransactionVersion.TV_0.featureLevel())
    log.updateHighWatermark(abortAppendInfo.lastOffset + 1)
    assertEquals(Optional.empty, log.firstUnstableOffset)

    // now check that a fetch includes the aborted transaction
    val fetchDataInfo = log.read(0L, 2048, FetchIsolation.TXN_COMMITTED, true)

    assertTrue(fetchDataInfo.abortedTransactions.isPresent)
    assertEquals(1, fetchDataInfo.abortedTransactions.get.size)
    assertEquals(new FetchResponseData.AbortedTransaction().setProducerId(pid).setFirstOffset(0), fetchDataInfo.abortedTransactions.get.get(0))
  }

  @Test
  def testLoadPartitionDirWithNoSegmentsShouldNotThrow(): Unit = {
    val dirName = UnifiedLog.logDeleteDirName(new TopicPartition("foo", 3))
    val logDir = new File(tmpDir, dirName)
    logDir.mkdirs()
    val logConfig = LogTestUtils.createLogConfig()
    val log = createLog(logDir, logConfig)
    assertEquals(1, log.numberOfSegments)
  }

  @Test
  def testSegmentDeletionWithHighWatermarkInitialization(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(
      segmentBytes = 512,
      segmentIndexBytes = 1000,
      retentionMs = 999
    )
    val log = createLog(logDir, logConfig)

    val expiredTimestamp = mockTime.milliseconds() - 1000
    for (i <- 0 until 100) {
      val records = TestUtils.singletonRecords(value = s"test$i".getBytes, timestamp = expiredTimestamp)
      log.appendAsLeader(records, 0)
    }

    val initialHighWatermark = log.updateHighWatermark(25L)
    assertEquals(25L, initialHighWatermark)

    val initialNumSegments = log.numberOfSegments
    log.deleteOldSegments()
    assertTrue(log.numberOfSegments < initialNumSegments)
    assertTrue(log.logStartOffset <= initialHighWatermark)
  }

  @Test
  def testCannotDeleteSegmentsAtOrAboveHighWatermark(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(
      segmentBytes = 512,
      segmentIndexBytes = 1000,
      retentionMs = 999
    )
    val log = createLog(logDir, logConfig)

    val expiredTimestamp = mockTime.milliseconds() - 1000
    for (i <- 0 until 100) {
      val records = TestUtils.singletonRecords(value = s"test$i".getBytes, timestamp = expiredTimestamp)
      log.appendAsLeader(records, 0)
    }

    // ensure we have at least a few segments so the test case is not trivial
    assertTrue(log.numberOfSegments > 5)
    assertEquals(0L, log.highWatermark)
    assertEquals(0L, log.logStartOffset)
    assertEquals(100L, log.logEndOffset)

    for (hw <- 0 to 100) {
      log.updateHighWatermark(hw)
      assertEquals(hw, log.highWatermark)
      log.deleteOldSegments()
      assertTrue(log.logStartOffset <= hw)

      // verify that all segments up to the high watermark have been deleted
      log.logSegments.asScala.headOption.foreach { segment =>
        assertTrue(segment.baseOffset <= hw)
        assertTrue(segment.baseOffset >= log.logStartOffset)
      }
      log.logSegments.asScala.tail.foreach { segment =>
        assertTrue(segment.baseOffset > hw)
        assertTrue(segment.baseOffset >= log.logStartOffset)
      }
    }

    assertEquals(100L, log.logStartOffset)
    assertEquals(1, log.numberOfSegments)
    assertEquals(0, log.activeSegment.size)
  }

  @Test
  def testCannotIncrementLogStartOffsetPastHighWatermark(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(
      segmentBytes = 512,
      segmentIndexBytes = 1000,
      retentionMs = 999
    )
    val log = createLog(logDir, logConfig)

    for (i <- 0 until 100) {
      val records = TestUtils.singletonRecords(value = s"test$i".getBytes)
      log.appendAsLeader(records, 0)
    }

    log.updateHighWatermark(25L)
    assertThrows(classOf[OffsetOutOfRangeException], () => log.maybeIncrementLogStartOffset(26L, LogStartOffsetIncrementReason.ClientRecordDeletion))
  }

  @Test
  def testBackgroundDeletionWithIOException(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)
    assertEquals(1, log.numberOfSegments, "The number of segments should be 1")

    // Delete the underlying directory to trigger a KafkaStorageException
    val dir = log.dir
    Utils.delete(dir)
    Files.createFile(dir.toPath)

    assertThrows(classOf[KafkaStorageException], () => {
      log.delete()
    })
    assertTrue(log.logDirFailureChannel.hasOfflineLogDir(tmpDir.toString))
  }

  /**
   * test renaming a log's dir without reinitialization, which is the case during topic deletion
   */
  @Test
  def testRenamingDirWithoutReinitialization(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)
    assertEquals(1, log.numberOfSegments, "The number of segments should be 1")

    val newDir = TestUtils.randomPartitionLogDir(tmpDir)
    assertTrue(newDir.exists())

    log.renameDir(newDir.getName, false)
    assertFalse(log.leaderEpochCache.nonEmpty)
    assertTrue(log.partitionMetadataFile.isEmpty)
    assertEquals(0, log.logEndOffset)

    // verify that the background deletion can succeed
    log.delete()
    assertEquals(0, log.numberOfSegments, "The number of segments should be 0")
    assertFalse(newDir.exists())
  }

  @Test
  def testMaybeUpdateHighWatermarkAsFollower(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    val log = createLog(logDir, logConfig)

    for (i <- 0 until 100) {
      val records = TestUtils.singletonRecords(value = s"test$i".getBytes)
      log.appendAsLeader(records, 0)
    }

    assertEquals(Optional.of(99L), log.maybeUpdateHighWatermark(99L))
    assertEquals(Optional.empty, log.maybeUpdateHighWatermark(99L))

    assertEquals(Optional.of(100L), log.maybeUpdateHighWatermark(100L))
    assertEquals(Optional.empty, log.maybeUpdateHighWatermark(100L))

    // bound by the log end offset
    assertEquals(Optional.empty, log.maybeUpdateHighWatermark(101L))
  }

  @Test
  def testEnableRemoteLogStorageOnCompactedTopics(): Unit = {
      var logConfig = LogTestUtils.createLogConfig()
      var log = createLog(logDir, logConfig)
      assertFalse(log.remoteLogEnabled())

      log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)
      assertFalse(log.remoteLogEnabled())

      logConfig = LogTestUtils.createLogConfig(remoteLogStorageEnable = true)
      log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)
      assertTrue(log.remoteLogEnabled())

      logConfig = LogTestUtils.createLogConfig(cleanupPolicy = TopicConfig.CLEANUP_POLICY_COMPACT, remoteLogStorageEnable = true)
      log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)
      assertFalse(log.remoteLogEnabled())

      logConfig = LogTestUtils.createLogConfig(cleanupPolicy = TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE,
        remoteLogStorageEnable = true)
      log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)
      assertFalse(log.remoteLogEnabled())
    }

    @Test
    def testRemoteLogStorageIsDisabledOnInternalAndRemoteLogMetadataTopic(): Unit = {
      val partitions = Seq(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME,
        Topic.TRANSACTION_STATE_TOPIC_NAME, Topic.TRANSACTION_STATE_TOPIC_NAME)
        .map(topic => new TopicPartition(topic, 0))
      for (partition <- partitions) {
        val logConfig = LogTestUtils.createLogConfig(remoteLogStorageEnable = true)
        val internalLogDir = new File(TestUtils.tempDir(), partition.toString)
        internalLogDir.mkdir()
        val log = createLog(internalLogDir, logConfig, remoteStorageSystemEnable = true)
        assertFalse(log.remoteLogEnabled())
      }
    }

    @Test
    def testNoOpWhenRemoteLogStorageIsDisabled(): Unit = {
      val logConfig = LogTestUtils.createLogConfig()
      val log = createLog(logDir, logConfig)

      for (i <- 0 until 100) {
        val records = TestUtils.singletonRecords(value = s"test$i".getBytes)
        log.appendAsLeader(records, 0)
      }

      log.updateHighWatermark(90L)
      log.maybeIncrementLogStartOffset(20L, LogStartOffsetIncrementReason.SegmentDeletion)
      assertEquals(20, log.logStartOffset)
    }

  @Test
  def testStartOffsetsRemoteLogStorageIsEnabled(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)

    for (i <- 0 until 100) {
      val records = TestUtils.singletonRecords(value = s"test$i".getBytes)
      log.appendAsLeader(records, 0)
    }

    log.updateHighWatermark(80L)
    val newLogStartOffset = 40L
    log.maybeIncrementLogStartOffset(newLogStartOffset, LogStartOffsetIncrementReason.SegmentDeletion)
    assertEquals(newLogStartOffset, log.logStartOffset)
    assertEquals(log.logStartOffset, log.localLogStartOffset())

    // Truncate the local log and verify that the offsets are updated to expected values
    val newLocalLogStartOffset = 60L
    log.truncateFullyAndStartAt(newLocalLogStartOffset, Optional.of(newLogStartOffset))
    assertEquals(newLogStartOffset, log.logStartOffset)
    assertEquals(newLocalLogStartOffset, log.localLogStartOffset())
  }

  private class MockLogOffsetsListener extends LogOffsetsListener {
    private var highWatermark: Long = -1L

    override def onHighWatermarkUpdated(offset: Long): Unit = {
      highWatermark = offset
    }

    private def clear(): Unit = {
      highWatermark = -1L
    }

    /**
     * Verifies the callbacks that have been triggered since the last
     * verification. Values different than `-1` are the ones that have
     * been updated.
     */
    def verify(expectedHighWatermark: Long = -1L): Unit = {
      assertEquals(expectedHighWatermark, highWatermark,
        "Unexpected high watermark")
      clear()
    }
  }

  @Test
  def testLogOffsetsListener(): Unit = {
    def records(offset: Long): MemoryRecords = TestUtils.records(List(
      new SimpleRecord(mockTime.milliseconds, "a".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "b".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "c".getBytes, "value".getBytes)
    ), baseOffset = offset, partitionLeaderEpoch = 0)

    val listener = new MockLogOffsetsListener()
    listener.verify()

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig, logOffsetsListener = listener)

    listener.verify(expectedHighWatermark = 0)

    log.appendAsLeader(records(0), 0)
    log.appendAsLeader(records(0), 0)

    log.maybeIncrementHighWatermark(new LogOffsetMetadata(4))
    listener.verify(expectedHighWatermark = 4)

    log.truncateTo(3)
    listener.verify(expectedHighWatermark = 3)

    log.appendAsLeader(records(0), 0)
    log.truncateFullyAndStartAt(4, Optional.empty)
    listener.verify(expectedHighWatermark = 4)
  }

  @Test
  def testUpdateLogOffsetsListener(): Unit = {
    def records(offset: Long): MemoryRecords = TestUtils.records(List(
      new SimpleRecord(mockTime.milliseconds, "a".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "b".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "c".getBytes, "value".getBytes)
    ), baseOffset = offset, partitionLeaderEpoch = 0)

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)

    log.appendAsLeader(records(0), 0)
    log.maybeIncrementHighWatermark(new LogOffsetMetadata(2))
    log.maybeIncrementLogStartOffset(1, LogStartOffsetIncrementReason.SegmentDeletion)

    val listener = new MockLogOffsetsListener()
    listener.verify()

    log.setLogOffsetsListener(listener)
    listener.verify() // it is still empty because we don't call the listener when it is set.

    log.appendAsLeader(records(0), 0)
    log.maybeIncrementHighWatermark(new LogOffsetMetadata(4))
    listener.verify(expectedHighWatermark = 4)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[AppendOrigin], names = Array("CLIENT", "COORDINATOR"))
  def testTransactionIsOngoingAndVerificationGuardTV2(appendOrigin: AppendOrigin): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, true)

    val producerId = 23L
    val producerEpoch = 1.toShort
    // For TV2, when there's no existing producer state, sequence must be 0 for both CLIENT and COORDINATOR
    var sequence = 0
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig)
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))
    assertFalse(log.verificationGuard(producerId).verify(VerificationGuard.SENTINEL))

    val idempotentRecords = MemoryRecords.withIdempotentRecords(
      Compression.NONE,
      producerId,
      producerEpoch,
      sequence,
      new SimpleRecord("1".getBytes),
      new SimpleRecord("2".getBytes)
    )

    // Only clients have nonzero sequences
    if (appendOrigin == AppendOrigin.CLIENT)
      sequence = sequence + 2

    val transactionalRecords = MemoryRecords.withTransactionalRecords(
      Compression.NONE,
      producerId,
      producerEpoch,
      sequence,
      new SimpleRecord("1".getBytes),
      new SimpleRecord("2".getBytes)
    )

    val verificationGuard = log.maybeStartTransactionVerification(producerId, sequence, producerEpoch, true)
    assertNotEquals(VerificationGuard.SENTINEL, verificationGuard)

    log.appendAsLeader(idempotentRecords, 0, appendOrigin)
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))

    // Since we wrote idempotent records, we keep VerificationGuard.
    assertEquals(verificationGuard, log.verificationGuard(producerId))

    // Now write the transactional records
    assertTrue(log.verificationGuard(producerId).verify(verificationGuard))
    log.appendAsLeader(transactionalRecords, 0, appendOrigin, RequestLocal.noCaching(), verificationGuard,
      TransactionVersion.TV_2.featureLevel())
    assertTrue(log.hasOngoingTransaction(producerId, producerEpoch))
    // VerificationGuard should be cleared now.
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))

    // A subsequent maybeStartTransactionVerification will be empty since we are already verified.
    assertEquals(VerificationGuard.SENTINEL, log.maybeStartTransactionVerification(producerId, sequence, producerEpoch, true))

    // For TV2, the coordinator bumps the epoch before writing the marker (KIP-890)
    val bumpedEpoch = (producerEpoch + 1).toShort
    val endTransactionMarkerRecord = MemoryRecords.withEndTransactionMarker(
      producerId,
      bumpedEpoch,
      new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    )

    log.appendAsLeader(endTransactionMarkerRecord, 0, AppendOrigin.COORDINATOR,
      RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_2.featureLevel())
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))

    if (appendOrigin == AppendOrigin.CLIENT)
      sequence = sequence + 1

    // A new maybeStartTransactionVerification will not be empty, as we need to verify the next transaction.
    // For TV2, after the marker is written with bumped epoch, the producer state now has the bumped epoch
    val newVerificationGuard = log.maybeStartTransactionVerification(producerId, sequence, bumpedEpoch, true)
    assertNotEquals(VerificationGuard.SENTINEL, newVerificationGuard)
    assertNotEquals(verificationGuard, newVerificationGuard)
    assertFalse(verificationGuard.verify(newVerificationGuard))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[AppendOrigin], names = Array("CLIENT", "COORDINATOR"))
  def testTransactionIsOngoingAndVerificationGuardTV1(appendOrigin: AppendOrigin): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, false)

    val producerId = 23L
    val producerEpoch = 1.toShort
    // For TV1, can start with non-zero sequences even with non-zero epoch when no existing producer state
    var sequence = if (appendOrigin == AppendOrigin.CLIENT) 3 else 0
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig)
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))
    assertFalse(log.verificationGuard(producerId).verify(VerificationGuard.SENTINEL))

    val idempotentRecords = MemoryRecords.withIdempotentRecords(
      Compression.NONE,
      producerId,
      producerEpoch,
      sequence,
      new SimpleRecord("1".getBytes),
      new SimpleRecord("2".getBytes)
    )

    // Only clients have nonzero sequences
    if (appendOrigin == AppendOrigin.CLIENT)
      sequence = sequence + 2

    val transactionalRecords = MemoryRecords.withTransactionalRecords(
      Compression.NONE,
      producerId,
      producerEpoch,
      sequence,
      new SimpleRecord("1".getBytes),
      new SimpleRecord("2".getBytes)
    )

    // For TV1, create verification guard with supportsEpochBump=false
    val verificationGuard = log.maybeStartTransactionVerification(producerId, sequence, producerEpoch, false)
    assertNotEquals(VerificationGuard.SENTINEL, verificationGuard)

    log.appendAsLeader(idempotentRecords, 0, appendOrigin)
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))

    // Since we wrote idempotent records, we keep VerificationGuard.
    assertEquals(verificationGuard, log.verificationGuard(producerId))

    // Now write the transactional records
    assertTrue(log.verificationGuard(producerId).verify(verificationGuard))
    log.appendAsLeader(transactionalRecords, 0, appendOrigin, RequestLocal.noCaching(), verificationGuard,
      TransactionVersion.TV_1.featureLevel())
    assertTrue(log.hasOngoingTransaction(producerId, producerEpoch))
    // VerificationGuard should be cleared now.
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))

    // A subsequent maybeStartTransactionVerification will be empty since we are already verified.
    assertEquals(VerificationGuard.SENTINEL, log.maybeStartTransactionVerification(producerId, sequence, producerEpoch, false))

    val endTransactionMarkerRecord = MemoryRecords.withEndTransactionMarker(
      producerId,
      producerEpoch,
      new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    )

    log.appendAsLeader(endTransactionMarkerRecord, 0, AppendOrigin.COORDINATOR,
      RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_1.featureLevel())
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))

    if (appendOrigin == AppendOrigin.CLIENT)
      sequence = sequence + 1

    // A new maybeStartTransactionVerification will not be empty, as we need to verify the next transaction.
    val newVerificationGuard = log.maybeStartTransactionVerification(producerId, sequence, producerEpoch, false)
    assertNotEquals(VerificationGuard.SENTINEL, newVerificationGuard)
    assertNotEquals(verificationGuard, newVerificationGuard)
    assertFalse(verificationGuard.verify(newVerificationGuard))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testEmptyTransactionStillClearsVerificationGuard(supportsEpochBump: Boolean): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, true)

    val producerId = 23L
    val producerEpoch = 1.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig)

    val verificationGuard = log.maybeStartTransactionVerification(producerId, 0, producerEpoch, supportsEpochBump)
    assertNotEquals(VerificationGuard.SENTINEL, verificationGuard)

    val endMarkerProducerEpoch = if (supportsEpochBump) (producerEpoch + 1).toShort else producerEpoch
    val transactionVersion = if (supportsEpochBump) TransactionVersion.TV_2.featureLevel() else TransactionVersion.TV_1.featureLevel()
    val endTransactionMarkerRecord = MemoryRecords.withEndTransactionMarker(
      producerId,
      endMarkerProducerEpoch,
      new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    )

    log.appendAsLeader(endTransactionMarkerRecord, 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
      VerificationGuard.SENTINEL, transactionVersion)
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))
  }

  @Test
  def testNextTransactionVerificationGuardNotCleared(): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, true)

    val producerId = 23L
    val producerEpoch = 1.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig)

    val verificationGuard = log.maybeStartTransactionVerification(producerId, 0, producerEpoch, true)
    assertNotEquals(VerificationGuard.SENTINEL, verificationGuard)

    // If the producer epoch is the same on the EndTxn marker, the verification must be for the next transaction, so we shouldn't clear it.
    val endTransactionMarkerRecord = MemoryRecords.withEndTransactionMarker(
      producerId,
      producerEpoch,
      new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    )

    log.appendAsLeader(endTransactionMarkerRecord, 0, AppendOrigin.COORDINATOR,
      RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel())
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(verificationGuard, log.verificationGuard(producerId))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testDisabledVerificationClearsVerificationGuard(supportsEpochBump: Boolean): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, true)

    val producerId = 23L
    val producerEpoch = 1.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig)

    val verificationGuard = log.maybeStartTransactionVerification(producerId, 0, producerEpoch, supportsEpochBump)
    assertNotEquals(VerificationGuard.SENTINEL, verificationGuard)

    producerStateManagerConfig.setTransactionVerificationEnabled(false)

    val transactionalRecords = MemoryRecords.withTransactionalRecords(
      Compression.NONE,
      producerId,
      producerEpoch,
      0,
      new SimpleRecord("1".getBytes),
      new SimpleRecord("2".getBytes)
    )
    log.appendAsLeader(transactionalRecords, 0)

    assertTrue(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))
  }

  @Test
  def testEnablingVerificationWhenRequestIsAtLogLayer(): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, false)

    val producerId = 23L
    val producerEpoch = 1.toShort
    val sequence = 0
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig)

    producerStateManagerConfig.setTransactionVerificationEnabled(true)

    val transactionalRecords = MemoryRecords.withTransactionalRecords(
      Compression.NONE,
      producerId,
      producerEpoch,
      sequence,
      new SimpleRecord("1".getBytes),
      new SimpleRecord("2".getBytes)
    )
    assertThrows(classOf[InvalidTxnStateException], () => log.appendAsLeader(transactionalRecords, 0))
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))

    val verificationGuard = log.maybeStartTransactionVerification(producerId, sequence, producerEpoch, true)
    assertNotEquals(VerificationGuard.SENTINEL, verificationGuard)

    log.appendAsLeader(transactionalRecords, 0, AppendOrigin.CLIENT, RequestLocal.noCaching,
      verificationGuard, TransactionVersion.TV_2.featureLevel())
    assertTrue(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testNonZeroSequenceOnFirstAppendNonZeroEpoch(transactionVerificationEnabled: Boolean): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, transactionVerificationEnabled)

    val producerId = 23L
    val producerEpoch = 1.toShort
    val sequence = 3
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig)
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))

    val transactionalRecords = MemoryRecords.withTransactionalRecords(
      Compression.NONE,
      producerId,
      producerEpoch,
      sequence,
      new SimpleRecord("1".getBytes),
      new SimpleRecord("2".getBytes)
    )

    if (transactionVerificationEnabled) {
      // TV2 behavior: Create verification state that supports epoch bumps
      val verificationGuard = log.maybeStartTransactionVerification(producerId, sequence, producerEpoch, true)
      // Should reject non-zero sequences when there's no existing producer state
      assertThrows(classOf[OutOfOrderSequenceException], () => 
        log.appendAsLeader(transactionalRecords, 0, AppendOrigin.CLIENT, RequestLocal.noCaching, verificationGuard,
          TransactionVersion.TV_0.featureLevel()))
    } else {
      // TV1 behavior: Create verification state with supportsEpochBump=false
      val verificationGuard = log.maybeStartTransactionVerification(producerId, sequence, producerEpoch, false)
      // Should allow non-zero sequences with non-zero epoch
      log.appendAsLeader(transactionalRecords, 0, AppendOrigin.CLIENT, RequestLocal.noCaching, verificationGuard,
        TransactionVersion.TV_0.featureLevel())
      assertTrue(log.hasOngoingTransaction(producerId, producerEpoch))
    }
  }

  @Test
  def testRecoveryPointNotIncrementedOnProducerStateSnapshotFlushFailure(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    val log = spy(createLog(logDir, logConfig))

    doThrow(new KafkaStorageException("Injected exception")).when(log).flushProducerStateSnapshot(any())

    log.appendAsLeader(TestUtils.singletonRecords("a".getBytes), 0)
    try {
      log.roll(Optional.of(1L))
    } catch {
      case _: KafkaStorageException => // ignore
    }

    // check that the recovery point isn't incremented
    assertEquals(0L, log.recoveryPoint)
  }

  @Test
  def testDeletableSegmentsFilter(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)
    for (_ <- 0 to 8) {
      val records = TestUtils.records(List(
        new SimpleRecord(mockTime.milliseconds, "a".getBytes),
      ))
      log.appendAsLeader(records, 0)
      log.roll()
    }
    log.maybeIncrementHighWatermark(log.logEndOffsetMetadata)

    assertEquals(10, log.logSegments.size())

    {
      val deletable = log.deletableSegments(
        (segment: LogSegment, _: Optional[LogSegment]) => segment.baseOffset <= 5)
      val expected = log.nonActiveLogSegmentsFrom(0L).stream().filter(segment => segment.baseOffset <= 5).toList
      assertEquals(6, expected.size)
      assertEquals(expected, deletable)
    }

    {
      val deletable = log.deletableSegments((_: LogSegment, _: Optional[LogSegment]) => true)
      val expected = log.nonActiveLogSegmentsFrom(0L).stream().toList
      assertEquals(9, expected.size)
      assertEquals(expected, deletable)
    }

    {
      val records = TestUtils.records(List(
        new SimpleRecord(mockTime.milliseconds, "a".getBytes),
      ))
      log.appendAsLeader(records, 0)
      log.maybeIncrementHighWatermark(log.logEndOffsetMetadata)
      val deletable = log.deletableSegments((_: LogSegment, _: Optional[LogSegment]) => true)
      val expected = log.logSegments.stream().toList
      assertEquals(10, expected.size)
      assertEquals(expected, deletable)
    }
  }

  @Test
  def testDeletableSegmentsIteration(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
    val log = createLog(logDir, logConfig)
    for (_ <- 0 to 8) {
      val records = TestUtils.records(List(
        new SimpleRecord(mockTime.milliseconds, "a".getBytes),
      ))
      log.appendAsLeader(records, 0)
      log.roll()
    }
    log.maybeIncrementHighWatermark(log.logEndOffsetMetadata)

    assertEquals(10, log.logSegments.size())

    var offset = 0
    val deletableSegments = log.deletableSegments(
      (segment: LogSegment, nextSegmentOpt: Optional[LogSegment]) => {
        assertEquals(offset, segment.baseOffset)
        val logSegments = new LogSegments(log.topicPartition)
        log.logSegments.forEach(segment => logSegments.add(segment))
        val floorSegmentOpt = logSegments.floorSegment(offset)
        assertTrue(floorSegmentOpt.isPresent)
        assertEquals(floorSegmentOpt.get, segment)
        if (offset == log.logEndOffset) {
          assertFalse(nextSegmentOpt.isPresent)
        } else {
          assertTrue(nextSegmentOpt.isPresent)
          val higherSegmentOpt = logSegments.higherSegment(segment.baseOffset)
          assertTrue(higherSegmentOpt.isPresent)
          assertEquals(segment.baseOffset + 1, higherSegmentOpt.get.baseOffset)
          assertEquals(higherSegmentOpt.get, nextSegmentOpt.get)
        }
        offset += 1
        true
      })
    assertEquals(10L, log.logSegments.size())
    assertEquals(log.nonActiveLogSegmentsFrom(0L).stream.toList, deletableSegments)
  }

  @Test
  def testActiveSegmentDeletionDueToRetentionTimeBreachWithRemoteStorage(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, segmentIndexBytes = 12,
      retentionMs = 3, localRetentionMs = 1, fileDeleteDelayMs = 0, remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)

    // Append 1 message to the active segment
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes))),
      0)
    // Update the highWatermark so that these segments will be eligible for deletion.
    log.updateHighWatermark(log.logEndOffset)
    assertEquals(1, log.logSegments.size)
    assertEquals(0, log.activeSegment.baseOffset())

    mockTime.sleep(2)
    // It should have rolled the active segment as they are eligible for deletion
    log.deleteOldSegments()
    assertEquals(2, log.logSegments.size)
    log.logSegments.asScala.zipWithIndex.foreach {
      case (segment, idx) => assertEquals(idx, segment.baseOffset)
    }

    // Once rolled, the segment should be uploaded to remote storage and eligible for deletion
    log.updateHighestOffsetInRemoteStorage(1)
    log.deleteOldSegments()
    assertEquals(1, log.logSegments.size)
    assertEquals(1, log.logSegments.asScala.head.baseOffset())
    assertEquals(1, log.localLogStartOffset())
    assertEquals(1, log.logEndOffset)
    assertEquals(0, log.logStartOffset)
  }

  @Test
  def testSegmentDeletionEnabledBeforeUploadToRemoteTierWhenLogStartOffsetMovedAhead(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(retentionBytes = 1, fileDeleteDelayMs = 0, remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)
    val pid = 1L
    val epoch = 0.toShort

    assertTrue(log.isEmpty)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 0), 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 1), 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 2), 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("d".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 3), 1)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("e".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 4), 2)
    log.updateHighWatermark(log.logEndOffset)
    assertEquals(2, log.logSegments.size)

    // No segments are uploaded to remote storage, none of the local log segments should be eligible for deletion
    log.updateHighestOffsetInRemoteStorage(-1L)
    log.deleteOldSegments()
    mockTime.sleep(1)
    assertEquals(2, log.logSegments.size)
    assertFalse(log.isEmpty)

    // Update the log-start-offset from 0 to 3, then the base segment should not be eligible for deletion
    log.updateLogStartOffsetFromRemoteTier(3L)
    log.deleteOldSegments()
    mockTime.sleep(1)
    assertEquals(2, log.logSegments.size)
    assertFalse(log.isEmpty)

    // Update the log-start-offset from 3 to 4, then the base segment should be eligible for deletion now even
    // if it is not uploaded to remote storage
    log.updateLogStartOffsetFromRemoteTier(4L)
    log.deleteOldSegments()
    mockTime.sleep(1)
    assertEquals(1, log.logSegments.size)
    assertFalse(log.isEmpty)

    log.updateLogStartOffsetFromRemoteTier(5L)
    log.deleteOldSegments()
    mockTime.sleep(1)
    assertEquals(1, log.logSegments.size)
    assertTrue(log.isEmpty)
  }

  @Test
  def testRetentionOnLocalLogDeletionWhenRemoteLogCopyEnabledAndDefaultLocalRetentionBytes(): Unit = {
    def createRecords = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes)))
    val segmentBytes = createRecords.sizeInBytes()
    val retentionBytesConfig = LogTestUtils.createLogConfig(segmentBytes = segmentBytes, retentionBytes = 1,
      fileDeleteDelayMs = 0, remoteLogStorageEnable = true)
    val log = createLog(logDir, retentionBytesConfig, remoteStorageSystemEnable = true)

    // Given 6 segments of 1 message each
    for (_ <- 0 until 6) {
      log.appendAsLeader(createRecords, 0)
    }
    assertEquals(6, log.logSegments.size)

    log.updateHighWatermark(log.logEndOffset)
    // simulate calls to upload 2 segments to remote storage
    log.updateHighestOffsetInRemoteStorage(1)
    log.deleteOldSegments()
    assertEquals(4, log.logSegments.size())
    assertEquals(0, log.logStartOffset)
    assertEquals(2, log.localLogStartOffset())
  }

  @Test
  def testRetentionOnLocalLogDeletionWhenRemoteLogCopyEnabledAndDefaultLocalRetentionMs(): Unit = {
    def createRecords = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes)))
    val segmentBytes = createRecords.sizeInBytes()
    val retentionBytesConfig = LogTestUtils.createLogConfig(segmentBytes = segmentBytes, retentionMs = 1000,
      fileDeleteDelayMs = 0, remoteLogStorageEnable = true)
    val log = createLog(logDir, retentionBytesConfig, remoteStorageSystemEnable = true)

    // Given 6 segments of 1 message each
    for (_ <- 0 until 6) {
      log.appendAsLeader(createRecords, 0)
    }
    assertEquals(6, log.logSegments.size)

    log.updateHighWatermark(log.logEndOffset)
    // simulate calls to upload 2 segments to remote storage
    log.updateHighestOffsetInRemoteStorage(1)

    mockTime.sleep(1001)
    log.deleteOldSegments()
    assertEquals(4, log.logSegments.size())
    assertEquals(0, log.logStartOffset)
    assertEquals(2, log.localLogStartOffset())
  }

  @Test
  def testRetentionOnLocalLogDeletionWhenRemoteLogCopyDisabled(): Unit = {
    def createRecords = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes)))
    val segmentBytes = createRecords.sizeInBytes()
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = segmentBytes, localRetentionBytes = 1, retentionBytes = segmentBytes * 5,
          fileDeleteDelayMs = 0, remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)

    // Given 6 segments of 1 message each
    for (_ <- 0 until 6) {
      log.appendAsLeader(createRecords, 0)
    }
    assertEquals(6, log.logSegments.size)

    log.updateHighWatermark(log.logEndOffset)

    // Should not delete local log because highest remote storage offset is -1 (default value)
    log.deleteOldSegments()
    assertEquals(6, log.logSegments.size())
    assertEquals(0, log.logStartOffset)
    assertEquals(0, log.localLogStartOffset())

    // simulate calls to upload 2 segments to remote storage
    log.updateHighestOffsetInRemoteStorage(1)

    log.deleteOldSegments()
    assertEquals(4, log.logSegments.size())
    assertEquals(0, log.logStartOffset)
    assertEquals(2, log.localLogStartOffset())

    // add remoteCopyDisabled = true
    val copyDisabledLogConfig = LogTestUtils.createLogConfig(segmentBytes = segmentBytes, localRetentionBytes = 1, retentionBytes = segmentBytes * 5,
      fileDeleteDelayMs = 0, remoteLogStorageEnable = true, remoteLogCopyDisable = true)
    log.updateConfig(copyDisabledLogConfig)

    // No local logs will be deleted even though local retention bytes is 1 because we'll adopt retention.ms/bytes
    // when remote.log.copy.disable = true
    log.deleteOldSegments()
    assertEquals(4, log.logSegments.size())
    assertEquals(0, log.logStartOffset)
    assertEquals(2, log.localLogStartOffset())

    // simulate the remote logs are all deleted due to retention policy
    log.updateLogStartOffsetFromRemoteTier(2)
    assertEquals(4, log.logSegments.size())
    assertEquals(2, log.logStartOffset)
    assertEquals(2, log.localLogStartOffset())

    // produce 3 more segments
    for (_ <- 0 until 3) {
      log.appendAsLeader(createRecords, 0)
    }
    assertEquals(7, log.logSegments.size)
    log.updateHighWatermark(log.logEndOffset)

    // try to delete local logs again, 2 segments will be deleted this time because we'll adopt retention.ms/bytes (retention.bytes = 5)
    // when remote.log.copy.disable = true
    log.deleteOldSegments()
    assertEquals(5, log.logSegments.size())
    assertEquals(4, log.logStartOffset)
    assertEquals(4, log.localLogStartOffset())

    // add localRetentionMs = 1, retentionMs = 1000
    val retentionMsConfig = LogTestUtils.createLogConfig(segmentBytes = segmentBytes, localRetentionMs = 1, retentionMs = 1000,
      fileDeleteDelayMs = 0, remoteLogStorageEnable = true, remoteLogCopyDisable = true)
    log.updateConfig(retentionMsConfig)

    // Should not delete any logs because no local logs expired using retention.ms = 1000
    mockTime.sleep(10)
    log.deleteOldSegments()
    assertEquals(5, log.logSegments.size())
    assertEquals(4, log.logStartOffset)
    assertEquals(4, log.localLogStartOffset())

    // Should delete all logs because all of them are expired based on retentionMs = 1000
    mockTime.sleep(1000)
    log.deleteOldSegments()
    assertEquals(1, log.logSegments.size())
    assertEquals(9, log.logStartOffset)
    assertEquals(9, log.localLogStartOffset())
  }

  @Test
  def testIncrementLocalLogStartOffsetAfterLocalLogDeletion(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(localRetentionBytes = 1, fileDeleteDelayMs = 0, remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)

    var offset = 0L
    for(_ <- 0 until 50) {
      val records = TestUtils.singletonRecords("test".getBytes())
      val info = log.appendAsLeader(records, 0)
      offset = info.lastOffset
      if (offset != 0 && offset % 10 == 0)
        log.roll()
    }
    assertEquals(5, log.logSegments.size)
    log.updateHighWatermark(log.logEndOffset)
    // simulate calls to upload 3 segments to remote storage
    log.updateHighestOffsetInRemoteStorage(30)

    log.deleteOldSegments()
    assertEquals(2, log.logSegments.size())
    assertEquals(0, log.logStartOffset)
    assertEquals(31, log.localLogStartOffset())
  }

  @Test
  def testConvertToOffsetMetadataDoesNotThrowOffsetOutOfRangeError(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(localRetentionBytes = 1, fileDeleteDelayMs = 0, remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)

    var offset = 0L
    for(_ <- 0 until 50) {
      val records = TestUtils.singletonRecords("test".getBytes())
      val info = log.appendAsLeader(records, 0)
      offset = info.lastOffset
      if (offset != 0 && offset % 10 == 0)
        log.roll()
    }
    assertEquals(5, log.logSegments.size)
    log.updateHighWatermark(log.logEndOffset)
    // simulate calls to upload 3 segments to remote storage
    log.updateHighestOffsetInRemoteStorage(30)

    log.deleteOldSegments()
    assertEquals(2, log.logSegments.size())
    assertEquals(0, log.logStartOffset)
    assertEquals(31, log.localLogStartOffset())

    log.updateLogStartOffsetFromRemoteTier(15)
    assertEquals(15, log.logStartOffset)

    // case-1: offset is higher than the local-log-start-offset.
    // log-start-offset < local-log-start-offset < offset-to-be-converted < log-end-offset
    assertEquals(new LogOffsetMetadata(35, 31, 288), log.maybeConvertToOffsetMetadata(35))
    // case-2: offset is less than the local-log-start-offset
    // log-start-offset < offset-to-be-converted < local-log-start-offset < log-end-offset
    assertEquals(new LogOffsetMetadata(29, -1L, -1), log.maybeConvertToOffsetMetadata(29))
    // case-3: offset is higher than the log-end-offset
    // log-start-offset < local-log-start-offset < log-end-offset < offset-to-be-converted
    assertEquals(new LogOffsetMetadata(log.logEndOffset + 1, -1L, -1), log.maybeConvertToOffsetMetadata(log.logEndOffset + 1))
    // case-4: offset is less than the log-start-offset
    // offset-to-be-converted < log-start-offset < local-log-start-offset < log-end-offset
    assertEquals(new LogOffsetMetadata(14, -1L, -1), log.maybeConvertToOffsetMetadata(14))
  }

  @Test
  def testGetFirstBatchTimestampForSegments(): Unit = {
    val log = createLog(logDir, LogTestUtils.createLogConfig())

    val segments: util.List[LogSegment] = new util.ArrayList[LogSegment]()
    val seg1 = LogTestUtils.createSegment(1, logDir, 10, Time.SYSTEM)
    val seg2 = LogTestUtils.createSegment(2, logDir, 10, Time.SYSTEM)
    segments.add(seg1)
    segments.add(seg2)
    assertEquals(Seq(Long.MaxValue, Long.MaxValue), log.getFirstBatchTimestampForSegments(segments).asScala.toSeq)

    seg1.append(1, MemoryRecords.withRecords(1, Compression.NONE, new SimpleRecord(1000L, "one".getBytes)))
    seg2.append(2, MemoryRecords.withRecords(2, Compression.NONE, new SimpleRecord(2000L, "two".getBytes)))
    assertEquals(Seq(1000L, 2000L), log.getFirstBatchTimestampForSegments(segments).asScala.toSeq)

    seg1.close()
    seg2.close()
  }

  @Test
  def testFetchOffsetByTimestampShouldReadOnlyLocalLogWhenLogIsEmpty(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)
    val result = log.fetchOffsetByTimestamp(mockTime.milliseconds(), Optional.empty)
    assertEquals(new OffsetResultHolder(Optional.empty(), Optional.empty()), result)
  }

  private def appendTransactionalToBuffer(buffer: ByteBuffer,
                                          producerId: Long,
                                          producerEpoch: Short,
                                          leaderEpoch: Int = 0): (Long, Int) => Unit = {
    var sequence = 0
    (offset: Long, numRecords: Int) => {
      val builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME,
        offset, mockTime.milliseconds(), producerId, producerEpoch, sequence, true, leaderEpoch)
      for (seq <- sequence until sequence + numRecords) {
        val record = new SimpleRecord(s"$seq".getBytes)
        builder.append(record)
      }

      sequence += numRecords
      builder.close()
    }
  }

  private def appendEndTxnMarkerToBuffer(buffer: ByteBuffer,
                                         producerId: Long,
                                         producerEpoch: Short,
                                         offset: Long,
                                         controlType: ControlRecordType,
                                         coordinatorEpoch: Int = 0,
                                         leaderEpoch: Int = 0): Unit = {
    val marker = new EndTransactionMarker(controlType, coordinatorEpoch)
    MemoryRecords.writeEndTransactionalMarker(buffer, offset, mockTime.milliseconds(), leaderEpoch, producerId, producerEpoch, marker)
  }

  private def appendNonTransactionalToBuffer(buffer: ByteBuffer, offset: Long, numRecords: Int): Unit = {
    val builder = MemoryRecords.builder(buffer, Compression.NONE, TimestampType.CREATE_TIME, offset)
    (0 until numRecords).foreach { seq =>
      builder.append(new SimpleRecord(s"$seq".getBytes))
    }
    builder.close()
  }

  private def appendAsFollower(log: UnifiedLog, records: MemoryRecords, leaderEpoch: Int): Unit = {
    records.batches.forEach(_.setPartitionLeaderEpoch(leaderEpoch))
    log.appendAsFollower(records, leaderEpoch)
  }

  private def createLog(dir: File,
                        config: LogConfig,
                        brokerTopicStats: BrokerTopicStats = brokerTopicStats,
                        logStartOffset: Long = 0L,
                        recoveryPoint: Long = 0L,
                        scheduler: Scheduler = mockTime.scheduler,
                        time: Time = mockTime,
                        maxTransactionTimeoutMs: Int = 60 * 60 * 1000,
                        producerStateManagerConfig: ProducerStateManagerConfig = producerStateManagerConfig,
                        producerIdExpirationCheckIntervalMs: Int = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
                        lastShutdownClean: Boolean = true,
                        topicId: Option[Uuid] = None,
                        remoteStorageSystemEnable: Boolean = false,
                        remoteLogManager: Option[RemoteLogManager] = None,
                        logOffsetsListener: LogOffsetsListener = LogOffsetsListener.NO_OP_OFFSETS_LISTENER): UnifiedLog = {
    val log = LogTestUtils.createLog(dir, config, brokerTopicStats, scheduler, time, logStartOffset, recoveryPoint,
      maxTransactionTimeoutMs, producerStateManagerConfig, producerIdExpirationCheckIntervalMs,
      lastShutdownClean, topicId, new ConcurrentHashMap[String, Integer],
      remoteStorageSystemEnable, remoteLogManager, logOffsetsListener)
    logsToClose = logsToClose :+ log
    log
  }

  private def createLogWithOffsetOverflow(logConfig: LogConfig): (UnifiedLog, LogSegment) = {
    LogTestUtils.initializeLogDirWithOverflowedSegment(logDir)

    val log = createLog(logDir, logConfig, recoveryPoint = Long.MaxValue)
    val segmentWithOverflow = LogTestUtils.firstOverflowSegment(log).getOrElse {
      throw new AssertionError("Failed to create log with a segment which has overflowed offsets")
    }

    (log, segmentWithOverflow)
  }

  private def assertFetchOffsetByTimestamp(log: UnifiedLog, remoteLogManagerOpt: Option[RemoteLogManager], expected: Option[TimestampAndOffset], timestamp: Long): Unit = {
    val remoteOffsetReader = getRemoteOffsetReader(remoteLogManagerOpt)
    val offsetResultHolder = log.fetchOffsetByTimestamp(timestamp, remoteOffsetReader)
    assertTrue(offsetResultHolder.futureHolderOpt.isPresent)
    offsetResultHolder.futureHolderOpt.get.taskFuture.get(1, TimeUnit.SECONDS)
    assertTrue(offsetResultHolder.futureHolderOpt.get.taskFuture.isDone)
    assertTrue(offsetResultHolder.futureHolderOpt.get.taskFuture.get().hasTimestampAndOffset)
    assertEquals(expected.get, offsetResultHolder.futureHolderOpt.get.taskFuture.get().timestampAndOffset().orElse(null))
  }

  private def assertFetchOffsetBySpecialTimestamp(log: UnifiedLog, remoteLogManagerOpt: Option[RemoteLogManager], expected: TimestampAndOffset, timestamp: Long): Unit = {
    val remoteOffsetReader = getRemoteOffsetReader(remoteLogManagerOpt)
    val offsetResultHolder = log.fetchOffsetByTimestamp(timestamp, remoteOffsetReader)
    assertEquals(new OffsetResultHolder(expected), offsetResultHolder)
  }

  private def getRemoteOffsetReader(remoteLogManagerOpt: Option[Any]): Optional[AsyncOffsetReader] = {
    remoteLogManagerOpt match {
      case Some(remoteLogManager) => Optional.of(remoteLogManager.asInstanceOf[AsyncOffsetReader])
      case None => Optional.empty[AsyncOffsetReader]()
    }
  }

  private def prepareLogWithSequentialRecords(log: UnifiedLog, recordCount: Int): Seq[TimestampAndEpoch] = {
    val firstTimestamp = mockTime.milliseconds()

    (0 until recordCount).map { i =>
      val timestampAndEpoch = TimestampAndEpoch(firstTimestamp + i, i)
      log.appendAsLeader(
        TestUtils.singletonRecords(value = TestUtils.randomBytes(10), timestamp = timestampAndEpoch.timestamp),
        timestampAndEpoch.leaderEpoch
      )
      timestampAndEpoch
    }
  }

  case class TimestampAndEpoch(timestamp: Long, leaderEpoch: Int)

  @Test
  def testStaleProducerEpochReturnsRecoverableErrorForTV1Clients(): Unit = {
    // Producer epoch gets incremented (coordinator fail over, completed transaction, etc.)
    // and client has stale cached epoch. Fix prevents fatal InvalidTxnStateException.
    
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, true)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig)
    
    val producerId = 123L
    val oldEpoch = 5.toShort
    val newEpoch = 6.toShort
    
    // Step 1: Simulate a scenario where producer epoch was incremented to fence the producer
    val previousRecords = MemoryRecords.withTransactionalRecords(
      Compression.NONE, producerId, newEpoch, 0,
      new SimpleRecord("previous-key".getBytes, "previous-value".getBytes)
    )
    val previousGuard = log.maybeStartTransactionVerification(producerId, 0, newEpoch, false)  // TV1 = supportsEpochBump = false
    log.appendAsLeader(previousRecords, 0, AppendOrigin.CLIENT, RequestLocal.noCaching, previousGuard,
      TransactionVersion.TV_1.featureLevel())
    
    // Complete the transaction normally (commits do update producer state with current epoch)
    val commitMarker = MemoryRecords.withEndTransactionMarker(
      producerId, newEpoch, new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    )
    log.appendAsLeader(commitMarker, 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching, VerificationGuard.SENTINEL,
      TransactionVersion.TV_1.featureLevel())
    
    // Step 2: TV1 client tries to write with stale cached epoch (before learning about epoch increment)  
    val staleEpochRecords = MemoryRecords.withTransactionalRecords(
      Compression.NONE, producerId, oldEpoch, 0,
      new SimpleRecord("stale-epoch-key".getBytes, "stale-epoch-value".getBytes)
    )
    
    // Step 3: Verify our fix - should get InvalidProducerEpochException (recoverable), not InvalidTxnStateException (fatal)
    val exception = assertThrows(classOf[InvalidProducerEpochException], () => {
      val staleGuard = log.maybeStartTransactionVerification(producerId, 0, oldEpoch, false)  
      log.appendAsLeader(staleEpochRecords, 0, AppendOrigin.CLIENT, RequestLocal.noCaching, staleGuard,
        TransactionVersion.TV_1.featureLevel())
     })
     
     // Verify the error message indicates epoch mismatch  
     assertTrue(exception.getMessage.contains("smaller than the last seen epoch"))
     assertTrue(exception.getMessage.contains(s"$oldEpoch"))
     assertTrue(exception.getMessage.contains(s"$newEpoch"))
  }

  @Test
  def testStaleProducerEpochReturnsRecoverableErrorForTV2Clients(): Unit = {
    // Check producer epoch FIRST - if stale, return recoverable error before verification checks.
    
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, true)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig)
    
    val producerId = 456L
    val originalEpoch = 3.toShort
    val bumpedEpoch = 4.toShort
    
    // Step 1: Start transaction with epoch 3 (before timeout)
    val initialRecords = MemoryRecords.withTransactionalRecords(
      Compression.NONE, producerId, originalEpoch, 0,
      new SimpleRecord("ks-initial-key".getBytes, "ks-initial-value".getBytes)
    )
    val initialGuard = log.maybeStartTransactionVerification(producerId, 0, originalEpoch, true)  // TV2 = supportsEpochBump = true
    log.appendAsLeader(initialRecords, 0, AppendOrigin.CLIENT, RequestLocal.noCaching, initialGuard,
      TransactionVersion.TV_2.featureLevel())
    
    // Step 2: Coordinator times out and aborts transaction
    // TV2 (KIP-890): Coordinator bumps epoch from 3 → 4 and sends abort marker with epoch 4
    val abortMarker = MemoryRecords.withEndTransactionMarker(
      producerId, bumpedEpoch, new EndTransactionMarker(ControlRecordType.ABORT, 0)
    )
    log.appendAsLeader(abortMarker, 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching, VerificationGuard.SENTINEL,
      TransactionVersion.TV_2.featureLevel())
    
    // Step 3: TV2 transactional producer tries to append with stale epoch (timeout recovery scenario)
    val staleEpochRecords = MemoryRecords.withTransactionalRecords(
      Compression.NONE, producerId, originalEpoch, 0,
      new SimpleRecord("ks-resume-key".getBytes, "ks-resume-value".getBytes)
    )
    
    // Step 4: Verify our fix works for TV2 - should get InvalidProducerEpochException (recoverable), not InvalidTxnStateException (fatal)
    val exception = assertThrows(classOf[InvalidProducerEpochException], () => {
      val staleGuard = log.maybeStartTransactionVerification(producerId, 0, originalEpoch, true)  // TV2 = supportsEpochBump = true
      log.appendAsLeader(staleEpochRecords, 0, AppendOrigin.CLIENT, RequestLocal.noCaching, staleGuard,
        TransactionVersion.TV_2.featureLevel())
     })
     
     // Verify the error message indicates epoch mismatch (3 < 4)
     assertTrue(exception.getMessage.contains("smaller than the last seen epoch"))
     assertTrue(exception.getMessage.contains(s"$originalEpoch"))
     assertTrue(exception.getMessage.contains(s"$bumpedEpoch"))
  }
}

object UnifiedLogTest {
  def allRecords(log: UnifiedLog): List[Record] = {
    val recordsFound = ListBuffer[Record]()
    for (logSegment <- log.logSegments.asScala) {
      for (batch <- logSegment.log.batches.asScala) {
        recordsFound ++= batch.iterator().asScala
      }
    }
    recordsFound.toList
  }

  def verifyRecordsInLog(log: UnifiedLog, expectedRecords: List[Record]): Unit = {
    assertEquals(expectedRecords, allRecords(log))
  }
}
