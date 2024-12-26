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

import kafka.log.remote.RemoteLogManager
import kafka.server.{DelayedRemoteListOffsets, KafkaConfig}
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
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig
import org.apache.kafka.server.log.remote.storage.{NoOpRemoteLogMetadataManager, NoOpRemoteStorageManager, RemoteLogManagerConfig}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.purgatory.DelayedOperationPurgatory
import org.apache.kafka.server.storage.log.{FetchIsolation, UnexpectedAppendOffsetException}
import org.apache.kafka.server.util.{KafkaScheduler, MockTime, Scheduler}
import org.apache.kafka.storage.internals.checkpoint.{LeaderEpochCheckpointFile, PartitionMetadataFile}
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.{AbortedTxn, AppendOrigin, EpochEntry, LogConfig, LogFileUtils, LogOffsetMetadata, LogOffsetSnapshot, LogOffsetsListener, LogSegment, LogSegments, LogStartOffsetIncrementReason, OffsetResultHolder, OffsetsOutOfOrderException, ProducerStateManager, ProducerStateManagerConfig, RecordValidationException, VerificationGuard}
import org.apache.kafka.storage.internals.utils.Throttler
import org.apache.kafka.storage.log.metrics.{BrokerTopicMetrics, BrokerTopicStats}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, ValueSource}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{doAnswer, doThrow, spy}

import java.io._
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.{Callable, ConcurrentHashMap, Executors, TimeUnit}
import java.util.{Optional, OptionalLong, Properties}
import scala.collection.immutable.SortedSet
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.{RichOptional, RichOptionalInt}

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
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
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
        startOffset = fetchOffset,
        maxLength = 2048,
        isolation = FetchIsolation.HIGH_WATERMARK,
        minOneMessage = false)
      assertEquals(expectedSize, readInfo.records.sizeInBytes)
      assertEquals(expectedOffsets, readInfo.records.records.asScala.map(_.offset))
    }

    val records = TestUtils.records(List(
      new SimpleRecord(mockTime.milliseconds, "a".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "b".getBytes, "value".getBytes),
      new SimpleRecord(mockTime.milliseconds, "c".getBytes, "value".getBytes)
    ))

    log.appendAsLeader(records, leaderEpoch = 0)
    assertFetchSizeAndOffsets(fetchOffset = 0L, 0, Seq())

    log.maybeIncrementHighWatermark(log.logEndOffsetMetadata)
    assertFetchSizeAndOffsets(fetchOffset = 0L, records.sizeInBytes, Seq(0, 1, 2))

    log.roll()
    assertFetchSizeAndOffsets(fetchOffset = 0L, records.sizeInBytes, Seq(0, 1, 2))

    log.appendAsLeader(records, leaderEpoch = 0)
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

    val firstAppendInfo = log.appendAsLeader(records, leaderEpoch = 0)
    assertEquals(0, firstAppendInfo.firstOffset)

    val secondAppendInfo = log.appendAsLeader(
      TestUtils.records(simpleRecords),
      leaderEpoch = 0
    )
    assertEquals(simpleRecords.size, secondAppendInfo.firstOffset)

    log.roll()
    val afterRollAppendInfo =  log.appendAsLeader(TestUtils.records(simpleRecords), leaderEpoch = 0)
    assertEquals(simpleRecords.size * 2, afterRollAppendInfo.firstOffset)
  }

  @Test
  def testTruncateBelowFirstUnstableOffset(): Unit = {
    testTruncateBelowFirstUnstableOffset((log, targetOffset) => log.truncateTo(targetOffset))
  }

  @Test
  def testTruncateFullyAndStartBelowFirstUnstableOffset(): Unit = {
    testTruncateBelowFirstUnstableOffset((log, targetOffset) => log.truncateFullyAndStartAt(targetOffset))
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
    )), leaderEpoch = 0)

    log.appendAsLeader(MemoryRecords.withTransactionalRecords(
      Compression.NONE,
      producerId,
      producerEpoch,
      sequence,
      new SimpleRecord("3".getBytes),
      new SimpleRecord("4".getBytes)
    ), leaderEpoch = 0)

    assertEquals(Some(3L), log.firstUnstableOffset)

    // We close and reopen the log to ensure that the first unstable offset segment
    // position will be undefined when we truncate the log.
    log.close()

    val reopened = createLog(logDir, logConfig)
    assertEquals(Optional.of(new LogOffsetMetadata(3L)), reopened.producerStateManager.firstUnstableOffset)

    reopened.truncateFullyAndStartAt(2L, Some(1L))
    assertEquals(None, reopened.firstUnstableOffset)
    assertEquals(java.util.Collections.emptyMap(), reopened.producerStateManager.activeProducers)
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
    )), leaderEpoch = 0)

    log.appendAsLeader(MemoryRecords.withTransactionalRecords(
      Compression.NONE,
      producerId,
      producerEpoch,
      sequence,
      new SimpleRecord("3".getBytes),
      new SimpleRecord("4".getBytes)
    ), leaderEpoch = 0)

    assertEquals(Some(3L), log.firstUnstableOffset)

    // We close and reopen the log to ensure that the first unstable offset segment
    // position will be undefined when we truncate the log.
    log.close()

    val reopened = createLog(logDir, logConfig)
    assertEquals(Optional.of(new LogOffsetMetadata(3L)), reopened.producerStateManager.firstUnstableOffset)

    truncateFunc(reopened, 0L)
    assertEquals(None, reopened.firstUnstableOffset)
    assertEquals(java.util.Collections.emptyMap(), reopened.producerStateManager.activeProducers)
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
    log.appendAsFollower(records(3L))
    log.updateHighWatermark(6L)
    assertHighWatermark(6L)

    // High watermark should be adjusted by truncation
    log.truncateTo(3L)
    assertHighWatermark(3L)

    log.appendAsLeader(records(0L), leaderEpoch = 0)
    assertHighWatermark(3L)
    assertEquals(6L, log.logEndOffset)
    assertEquals(0L, log.logStartOffset)

    // Full truncation should also reset high watermark
    log.truncateFullyAndStartAt(4L)
    assertEquals(4L, log.logEndOffset)
    assertEquals(4L, log.logStartOffset)
    assertHighWatermark(4L)
  }

  private def assertNonEmptyFetch(log: UnifiedLog, offset: Long, isolation: FetchIsolation, batchBaseOffset: Long): Unit = {
    val readInfo = log.read(startOffset = offset,
      maxLength = Int.MaxValue,
      isolation = isolation,
      minOneMessage = true)

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
    val readInfo = log.read(startOffset = offset,
      maxLength = Int.MaxValue,
      isolation = isolation,
      minOneMessage = true)
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
    )), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("3".getBytes),
      new SimpleRecord("4".getBytes)
    )), leaderEpoch = 0)
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
    )), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("3".getBytes),
      new SimpleRecord("4".getBytes)
    )), leaderEpoch = 0)
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
      val producerStateOpt = log.activeProducers.find(_.producerId == producerId)
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
    LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId1, producer1Epoch, ControlRecordType.COMMIT, mockTime.milliseconds(), coordinatorEpoch)
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

    LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId1, epoch, ControlRecordType.COMMIT, mockTime.milliseconds())
    assertEquals(0L, log.lastStableOffset)

    log.updateHighWatermark(log.logEndOffset)
    assertEquals(8L, log.lastStableOffset)
    assertLsoBoundedFetches()

    LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId2, epoch, ControlRecordType.ABORT, mockTime.milliseconds())
    assertEquals(8L, log.lastStableOffset)

    log.updateHighWatermark(log.logEndOffset)
    assertEquals(log.logEndOffset, log.lastStableOffset)
    assertLsoBoundedFetches()
  }

  @Test
  def testOffsetFromProducerSnapshotFile(): Unit = {
    val offset = 23423423L
    val snapshotFile = LogFileUtils.producerSnapshotFile(tmpDir, offset)
    assertEquals(offset, UnifiedLog.offsetFromFile(snapshotFile))
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
    log.appendAsLeader(createRecords, leaderEpoch = 0)
    assertEquals(1, log.numberOfSegments, "Log doesn't roll if doing so creates an empty segment.")

    log.appendAsLeader(createRecords, leaderEpoch = 0)
    assertEquals(2, log.numberOfSegments, "Log rolls on this append since time has expired.")

    for (numSegments <- 3 until 5) {
      mockTime.sleep(log.config.segmentMs + 1)
      log.appendAsLeader(createRecords, leaderEpoch = 0)
      assertEquals(numSegments, log.numberOfSegments, "Changing time beyond rollMs and appending should create a new segment.")
    }

    // Append a message with timestamp to a segment whose first message do not have a timestamp.
    val timestamp = mockTime.milliseconds + log.config.segmentMs + 1
    def createRecordsWithTimestamp = TestUtils.singletonRecords(value = "test".getBytes, timestamp = timestamp)
    log.appendAsLeader(createRecordsWithTimestamp, leaderEpoch = 0)
    assertEquals(4, log.numberOfSegments, "Segment should not have been rolled out because the log rolling should be based on wall clock.")

    // Test the segment rolling behavior when messages have timestamps.
    mockTime.sleep(log.config.segmentMs + 1)
    log.appendAsLeader(createRecordsWithTimestamp, leaderEpoch = 0)
    assertEquals(5, log.numberOfSegments, "A new segment should have been rolled out")

    // move the wall clock beyond log rolling time
    mockTime.sleep(log.config.segmentMs + 1)
    log.appendAsLeader(createRecordsWithTimestamp, leaderEpoch = 0)
    assertEquals(5, log.numberOfSegments, "Log should not roll because the roll should depend on timestamp of the first message.")

    val recordWithExpiredTimestamp = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    log.appendAsLeader(recordWithExpiredTimestamp, leaderEpoch = 0)
    assertEquals(6, log.numberOfSegments, "Log should roll because the timestamp in the message should make the log segment expire.")

    val numSegments = log.numberOfSegments
    mockTime.sleep(log.config.segmentMs + 1)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE), leaderEpoch = 0)
    assertEquals(numSegments, log.numberOfSegments, "Appending an empty message set should not roll log even if sufficient time has passed.")
  }

  @Test
  def testRollSegmentThatAlreadyExists(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentMs = 1 * 60 * 60L)

    // create a log
    val log = createLog(logDir, logConfig)
    assertEquals(1, log.numberOfSegments, "Log begins with a single empty segment.")

    // roll active segment with the same base offset of size zero should recreate the segment
    log.roll(Some(0L))
    assertEquals(1, log.numberOfSegments, "Expect 1 segment after roll() empty segment with base offset.")

    // should be able to append records to active segment
    val records = TestUtils.records(
      List(new SimpleRecord(mockTime.milliseconds, "k1".getBytes, "v1".getBytes)),
      baseOffset = 0L, partitionLeaderEpoch = 0)
    log.appendAsFollower(records)
    assertEquals(1, log.numberOfSegments, "Expect one segment.")
    assertEquals(0L, log.activeSegment.baseOffset)

    // make sure we can append more records
    val records2 = TestUtils.records(
      List(new SimpleRecord(mockTime.milliseconds + 10, "k2".getBytes, "v2".getBytes)),
      baseOffset = 1L, partitionLeaderEpoch = 0)
    log.appendAsFollower(records2)

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
      baseOffset = 2L, partitionLeaderEpoch = 0)
    log.appendAsFollower(records3)
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
    log.appendAsLeader(records, leaderEpoch = 0)

    val nextRecords = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)), producerId = pid, producerEpoch = epoch, sequence = 2)
    assertThrows(classOf[OutOfOrderSequenceException], () => log.appendAsLeader(nextRecords, leaderEpoch = 0))
  }

  @Test
  def testTruncateToEndOffsetClearsEpochCache(): Unit = {
    val log = createLog(logDir, new LogConfig(new Properties))

    // Seed some initial data in the log
    val records = TestUtils.records(List(new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)),
      baseOffset = 27)
    appendAsFollower(log, records, leaderEpoch = 19)
    assertEquals(Some(new EpochEntry(19, 27)),
      log.leaderEpochCache.flatMap(_.latestEntry.toScala))
    assertEquals(29, log.logEndOffset)

    def verifyTruncationClearsEpochCache(epoch: Int, truncationOffset: Long): Unit = {
      // Simulate becoming a leader
      log.maybeAssignEpochStartOffset(leaderEpoch = epoch, startOffset = log.logEndOffset)
      assertEquals(Some(new EpochEntry(epoch, 29)),
        log.leaderEpochCache.flatMap(_.latestEntry.toScala))
      assertEquals(29, log.logEndOffset)

      // Now we become the follower and truncate to an offset greater
      // than or equal to the log end offset. The trivial epoch entry
      // at the end of the log should be gone
      log.truncateTo(truncationOffset)
      assertEquals(Some(new EpochEntry(19, 27)),
        log.leaderEpochCache.flatMap(_.latestEntry.toScala))
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
    def getSegmentOffsets(log :UnifiedLog, from: Long, to: Long) = log.logSegments(from, to).map { _.baseOffset }
    val setSize = createRecords.sizeInBytes
    val msgPerSeg = 10
    val segmentSize = msgPerSeg * setSize  // each segment will be 10 messages
    // create a log
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = segmentSize)
    val log = createLog(logDir, logConfig)
    assertEquals(1, log.numberOfSegments, "There should be exactly 1 segment.")

    // segments expire in size
    for (_ <- 1 to (2 * msgPerSeg + 2))
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    assertEquals(3, log.numberOfSegments, "There should be exactly 3 segments.")

    // from == to should always be null
    assertEquals(List.empty[LogSegment], getSegmentOffsets(log, 10, 10))
    assertEquals(List.empty[LogSegment], getSegmentOffsets(log, 15, 15))

    assertEquals(List[Long](0, 10, 20), getSegmentOffsets(log, 0, 21))

    assertEquals(List[Long](0), getSegmentOffsets(log, 1, 5))
    assertEquals(List[Long](10, 20), getSegmentOffsets(log, 13, 21))
    assertEquals(List[Long](10), getSegmentOffsets(log, 13, 17))

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
      log.appendAsLeader(TestUtils.records(List(record)), leaderEpoch = 0)
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
    log.appendAsLeader(records, leaderEpoch = 0)
    log.takeProducerSnapshot()
    assertEquals(OptionalLong.of(1), log.latestProducerSnapshotOffset)
  }

  @Test
  def testRebuildProducerIdMapWithCompactedData(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid = 1L
    val epoch = 0.toShort
    val seq = 0
    val baseOffset = 23L

    // create a batch with a couple gaps to simulate compaction
    val records = TestUtils.records(producerId = pid, producerEpoch = epoch, sequence = seq, baseOffset = baseOffset, records = List(
      new SimpleRecord(mockTime.milliseconds(), "a".getBytes),
      new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "b".getBytes),
      new SimpleRecord(mockTime.milliseconds(), "c".getBytes),
      new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "d".getBytes)))
    records.batches.forEach(_.setPartitionLeaderEpoch(0))

    val filtered = ByteBuffer.allocate(2048)
    records.filterTo(new TopicPartition("foo", 0), new RecordFilter(0, 0) {
      override def checkBatchRetention(batch: RecordBatch): RecordFilter.BatchRetentionResult =
        new RecordFilter.BatchRetentionResult(RecordFilter.BatchRetention.DELETE_EMPTY, false)
      override def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = !record.hasKey
    }, filtered, Int.MaxValue, TimestampType.CREATE_TIME, BufferSupplier.NO_CACHING)
    filtered.flip()
    val filteredRecords = MemoryRecords.readableRecords(filtered)

    log.appendAsFollower(filteredRecords)

    // append some more data and then truncate to force rebuilding of the PID map
    val moreRecords = TestUtils.records(baseOffset = baseOffset + 4, records = List(
      new SimpleRecord(mockTime.milliseconds(), "e".getBytes),
      new SimpleRecord(mockTime.milliseconds(), "f".getBytes)))
    moreRecords.batches.forEach(_.setPartitionLeaderEpoch(0))
    log.appendAsFollower(moreRecords)

    log.truncateTo(baseOffset + 4)

    val activeProducers = log.activeProducersWithLastSequence
    assertTrue(activeProducers.contains(pid))

    val lastSeq = activeProducers(pid)
    assertEquals(3, lastSeq)
  }

  @Test
  def testRebuildProducerStateWithEmptyCompactedBatch(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid = 1L
    val epoch = 0.toShort
    val seq = 0
    val baseOffset = 23L

    // create an empty batch
    val records = TestUtils.records(producerId = pid, producerEpoch = epoch, sequence = seq, baseOffset = baseOffset, records = List(
      new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "a".getBytes),
      new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "b".getBytes)))
    records.batches.forEach(_.setPartitionLeaderEpoch(0))

    val filtered = ByteBuffer.allocate(2048)
    records.filterTo(new TopicPartition("foo", 0), new RecordFilter(0, 0) {
      override def checkBatchRetention(batch: RecordBatch): RecordFilter.BatchRetentionResult =
        new RecordFilter.BatchRetentionResult(RecordFilter.BatchRetention.RETAIN_EMPTY, true)
      override def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = false
    }, filtered, Int.MaxValue, TimestampType.CREATE_TIME, BufferSupplier.NO_CACHING)
    filtered.flip()
    val filteredRecords = MemoryRecords.readableRecords(filtered)

    log.appendAsFollower(filteredRecords)

    // append some more data and then truncate to force rebuilding of the PID map
    val moreRecords = TestUtils.records(baseOffset = baseOffset + 2, records = List(
      new SimpleRecord(mockTime.milliseconds(), "e".getBytes),
      new SimpleRecord(mockTime.milliseconds(), "f".getBytes)))
    moreRecords.batches.forEach(_.setPartitionLeaderEpoch(0))
    log.appendAsFollower(moreRecords)

    log.truncateTo(baseOffset + 2)

    val activeProducers = log.activeProducersWithLastSequence
    assertTrue(activeProducers.contains(pid))

    val lastSeq = activeProducers(pid)
    assertEquals(1, lastSeq)
  }

  @Test
  def testUpdateProducerIdMapWithCompactedData(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid = 1L
    val epoch = 0.toShort
    val seq = 0
    val baseOffset = 23L

    // create a batch with a couple gaps to simulate compaction
    val records = TestUtils.records(producerId = pid, producerEpoch = epoch, sequence = seq, baseOffset = baseOffset, records = List(
      new SimpleRecord(mockTime.milliseconds(), "a".getBytes),
      new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "b".getBytes),
      new SimpleRecord(mockTime.milliseconds(), "c".getBytes),
      new SimpleRecord(mockTime.milliseconds(), "key".getBytes, "d".getBytes)))
    records.batches.forEach(_.setPartitionLeaderEpoch(0))

    val filtered = ByteBuffer.allocate(2048)
    records.filterTo(new TopicPartition("foo", 0), new RecordFilter(0, 0) {
      override def checkBatchRetention(batch: RecordBatch): RecordFilter.BatchRetentionResult =
        new RecordFilter.BatchRetentionResult(RecordFilter.BatchRetention.DELETE_EMPTY, false)
      override def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = !record.hasKey
    }, filtered, Int.MaxValue, TimestampType.CREATE_TIME, BufferSupplier.NO_CACHING)
    filtered.flip()
    val filteredRecords = MemoryRecords.readableRecords(filtered)

    log.appendAsFollower(filteredRecords)
    val activeProducers = log.activeProducersWithLastSequence
    assertTrue(activeProducers.contains(pid))

    val lastSeq = activeProducers(pid)
    assertEquals(3, lastSeq)
  }

  @Test
  def testProducerIdMapTruncateTo(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes))), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes))), leaderEpoch = 0)
    log.takeProducerSnapshot()

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes))), leaderEpoch = 0)
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
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)), producerId = pid,
      producerEpoch = epoch, sequence = 1), leaderEpoch = 0)

    LogTestUtils.deleteProducerSnapshotFiles(logDir)

    log.truncateTo(1L)
    assertEquals(1, log.activeProducersWithLastSequence.size)

    val lastSeqOpt = log.activeProducersWithLastSequence.get(pid)
    assertTrue(lastSeqOpt.isDefined)

    val lastSeq = lastSeqOpt.get
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
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 1), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 2), leaderEpoch = 0)
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

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds() + 100, "a".getBytes))), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "b".getBytes))), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds() + 100, "c".getBytes))), leaderEpoch = 0)

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
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 1), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 2), leaderEpoch = 0)
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
    val cleaner = new Cleaner(id = 0,
      offsetMap = new FakeOffsetMap(Int.MaxValue),
      ioBufferSize = 64 * 1024,
      maxIoBufferSize = 64 * 1024,
      dupBufferLoadFactor = 0.75,
      throttler = new Throttler(Double.MaxValue, Long.MaxValue, "throttler", "entries", mockTime),
      time = mockTime,
      checkDone = _ => {})

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes, "a".getBytes())), producerId = pid1,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes, "b".getBytes())), producerId = pid1,
      producerEpoch = epoch, sequence = 1), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes, "c".getBytes())), producerId = pid1,
      producerEpoch = epoch, sequence = 2), leaderEpoch = 0)
    log.updateHighWatermark(log.logEndOffset)
    assertEquals(log.logSegments.asScala.map(_.baseOffset).toSeq.sorted.drop(1), ProducerStateManager.listSnapshotFiles(logDir).asScala.map(_.offset).sorted,
      "expected a snapshot file per segment base offset, except the first segment")
    assertEquals(2, ProducerStateManager.listSnapshotFiles(logDir).size)

    // Clean segments, this should delete everything except the active segment since there only
    // exists the key "a".
    cleaner.clean(LogToClean(log.topicPartition, log, 0, log.logEndOffset))
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
    log.appendAsLeader(records, leaderEpoch = 0)
    log.takeProducerSnapshot()

    log.appendAsLeader(TestUtils.singletonRecords("bar".getBytes), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.singletonRecords("baz".getBytes), leaderEpoch = 0)
    log.takeProducerSnapshot()

    assertEquals(3, log.logSegments.size)
    assertEquals(3, log.latestProducerStateEndOffset)
    assertEquals(OptionalLong.of(3), log.latestProducerSnapshotOffset)

    log.truncateFullyAndStartAt(29)
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
    log.appendAsLeader(records, leaderEpoch = 0)
    log.takeProducerSnapshot()

    val pid2 = 2L
    log.appendAsLeader(TestUtils.records(Seq(new SimpleRecord("bar".getBytes)), producerId = pid2, producerEpoch = 0, sequence = 0),
      leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(Seq(new SimpleRecord("baz".getBytes)), producerId = pid2, producerEpoch = 0, sequence = 1),
      leaderEpoch = 0)
    log.takeProducerSnapshot()

    assertEquals(3, log.logSegments.size)
    assertEquals(Set(pid1, pid2), log.activeProducersWithLastSequence.keySet)

    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()

    // Producer state should not be removed when deleting log segment
    assertEquals(2, log.logSegments.size)
    assertEquals(Set(pid1, pid2), log.activeProducersWithLastSequence.keySet)
  }

  @Test
  def testTakeSnapshotOnRollAndDeleteSnapshotOnRecoveryPointCheckpoint(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(TestUtils.singletonRecords("a".getBytes), leaderEpoch = 0)
    log.roll(Some(1L))
    assertEquals(OptionalLong.of(1L), log.latestProducerSnapshotOffset)
    assertEquals(OptionalLong.of(1L), log.oldestProducerSnapshotOffset)

    log.appendAsLeader(TestUtils.singletonRecords("b".getBytes), leaderEpoch = 0)
    log.roll(Some(2L))
    assertEquals(OptionalLong.of(2L), log.latestProducerSnapshotOffset)
    assertEquals(OptionalLong.of(1L), log.oldestProducerSnapshotOffset)

    log.appendAsLeader(TestUtils.singletonRecords("c".getBytes), leaderEpoch = 0)
    log.roll(Some(3L))
    assertEquals(OptionalLong.of(3L), log.latestProducerSnapshotOffset)

    // roll triggers a flush at the starting offset of the new segment, we should retain all snapshots
    assertEquals(OptionalLong.of(1L), log.oldestProducerSnapshotOffset)

    // even if we flush within the active segment, the snapshot should remain
    log.appendAsLeader(TestUtils.singletonRecords("baz".getBytes), leaderEpoch = 0)
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
      leaderEpoch = 0)

    // The next append should overflow the segment and cause it to roll
    log.appendAsLeader(TestUtils.records(Seq(new SimpleRecord(mockTime.milliseconds(), new Array[Byte](512))),
      producerId = producerId, producerEpoch = 0, sequence = 1),
      leaderEpoch = 0)

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
    log.appendAsLeader(records, leaderEpoch = 0)
    val abortAppendInfo = LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds())
    log.updateHighWatermark(abortAppendInfo.lastOffset + 1)

    // now there should be no first unstable offset
    assertEquals(None, log.firstUnstableOffset)

    log.close()

    val reopenedLog = createLog(logDir, logConfig, lastShutdownClean = false)
    reopenedLog.updateHighWatermark(abortAppendInfo.lastOffset + 1)
    assertEquals(None, reopenedLog.firstUnstableOffset)
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
    log.appendAsLeader(TestUtils.records(records, producerId = pid, producerEpoch = 0, sequence = 0), leaderEpoch = 0)

    assertEquals(Set(pid), log.activeProducersWithLastSequence.keySet)

    mockTime.sleep(producerIdExpirationCheckIntervalMs)
    assertEquals(Set(pid), log.activeProducersWithLastSequence.keySet)

    mockTime.sleep(producerIdExpirationCheckIntervalMs)
    assertEquals(Set(), log.activeProducersWithLastSequence.keySet)
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
      log.appendAsLeader(record, leaderEpoch = 0)
      seq = seq + 1
    }
    // Append an entry with multiple log records.
    def createRecords = TestUtils.records(List(
      new SimpleRecord(mockTime.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes),
      new SimpleRecord(mockTime.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes),
      new SimpleRecord(mockTime.milliseconds, s"key-$seq".getBytes, s"value-$seq".getBytes)
    ), producerId = pid, producerEpoch = epoch, sequence = seq)
    val multiEntryAppendInfo = log.appendAsLeader(createRecords, leaderEpoch = 0)
    assertEquals(
      multiEntryAppendInfo.lastOffset - multiEntryAppendInfo.firstOffset + 1,
      3,
      "should have appended 3 entries"
    )

    // Append a Duplicate of the tail, when the entry at the tail has multiple records.
    val dupMultiEntryAppendInfo = log.appendAsLeader(createRecords, leaderEpoch = 0)
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
    assertThrows(classOf[OutOfOrderSequenceException], () => log.appendAsLeader(records, leaderEpoch = 0),
      () => "Should have received an OutOfOrderSequenceException since we attempted to append a duplicate of a records in the middle of the log.")

    // Append a duplicate of the batch which is 4th from the tail. This should succeed without error since we
    // retain the batch metadata of the last 5 batches.
    val duplicateOfFourth = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 2)
    log.appendAsLeader(duplicateOfFourth, leaderEpoch = 0)

    // Duplicates at older entries are reported as OutOfOrderSequence errors
    records = TestUtils.records(
      List(new SimpleRecord(mockTime.milliseconds, s"key-1".getBytes, s"value-1".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 1)
    assertThrows(classOf[OutOfOrderSequenceException], () => log.appendAsLeader(records, leaderEpoch = 0),
      () => "Should have received an OutOfOrderSequenceException since we attempted to append a duplicate of a batch which is older than the last 5 appended batches.")

    // Append a duplicate entry with a single records at the tail of the log. This should return the appendInfo of the original entry.
    def createRecordsWithDuplicate = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = seq)
    val origAppendInfo = log.appendAsLeader(createRecordsWithDuplicate, leaderEpoch = 0)
    val newAppendInfo = log.appendAsLeader(createRecordsWithDuplicate, leaderEpoch = 0)
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

    val epoch: Short = 0
    val buffer = ByteBuffer.allocate(512)

    var builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 0L, mockTime.milliseconds(), 1L, epoch, 0, false, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 1L, mockTime.milliseconds(), 2L, epoch, 0, false, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 2L, mockTime.milliseconds(), 3L, epoch, 0, false, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 3L, mockTime.milliseconds(), 4L, epoch, 0, false, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    buffer.flip()
    val memoryRecords = MemoryRecords.readableRecords(buffer)

    log.appendAsFollower(memoryRecords)
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
    val epoch: Short = 0
    val pid = 1L
    val baseSequence = 0
    val partitionLeaderEpoch = 0
    // The point of this test is to ensure that validation isn't performed on the follower.
    // this is a bit contrived. to trigger the duplicate case for a follower append, we have to append
    // a batch with matching sequence numbers, but valid increasing offsets
    assertEquals(0L, log.logEndOffset)
    log.appendAsFollower(MemoryRecords.withIdempotentRecords(0L, Compression.NONE, pid, epoch, baseSequence,
      partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))
    log.appendAsFollower(MemoryRecords.withIdempotentRecords(2L, Compression.NONE, pid, epoch, baseSequence,
      partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

    // Ensure that even the duplicate sequences are accepted on the follower.
    assertEquals(4L, log.logEndOffset)
  }

  @Test
  def testMultipleProducersWithDuplicatesInSingleAppend(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    val pid1 = 1L
    val pid2 = 2L
    val epoch: Short = 0

    val buffer = ByteBuffer.allocate(512)

    // pid1 seq = 0
    var builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 0L, mockTime.milliseconds(), pid1, epoch, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // pid2 seq = 0
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 1L, mockTime.milliseconds(), pid2, epoch, 0)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // pid1 seq = 1
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 2L, mockTime.milliseconds(), pid1, epoch, 1)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // pid2 seq = 1
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 3L, mockTime.milliseconds(), pid2, epoch, 1)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    // // pid1 seq = 1 (duplicate)
    builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
      TimestampType.LOG_APPEND_TIME, 4L, mockTime.milliseconds(), pid1, epoch, 1)
    builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
    builder.close()

    buffer.flip()

    val records = MemoryRecords.readableRecords(buffer)
    records.batches.forEach(_.setPartitionLeaderEpoch(0))

    // Ensure that batches with duplicates are accepted on the follower.
    assertEquals(0L, log.logEndOffset)
    log.appendAsFollower(records)
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
    log.appendAsLeader(records, leaderEpoch = 0)

    val nextRecords = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)), producerId = pid, producerEpoch = oldEpoch, sequence = 0)
    assertThrows(classOf[InvalidProducerEpochException], () => log.appendAsLeader(nextRecords, leaderEpoch = 0))
  }

  @Test
  def testDeleteSnapshotsOnIncrementLogStartOffset(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig)
    val pid1 = 1L
    val pid2 = 2L
    val epoch = 0.toShort

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes)), producerId = pid1,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "b".getBytes)), producerId = pid2,
      producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
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
    val retainedLastSeqOpt = log.activeProducersWithLastSequence.get(pid2)
    assertTrue(retainedLastSeqOpt.isDefined)
    assertEquals(0, retainedLastSeqOpt.get)
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
    log.appendAsLeader(set, leaderEpoch = 0)

    mockTime.sleep(log.config.segmentMs - maxJitter)
    set = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    log.appendAsLeader(set, leaderEpoch = 0)
    assertEquals(1, log.numberOfSegments,
      "Log does not roll on this append because it occurs earlier than max jitter")
    mockTime.sleep(maxJitter - log.activeSegment.rollJitterMs + 1)
    set = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    log.appendAsLeader(set, leaderEpoch = 0)
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
      log.appendAsLeader(createRecords, leaderEpoch = 0)
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
    log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds), leaderEpoch = 0)
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
      log.appendAsLeader(TestUtils.singletonRecords(value = value), leaderEpoch = 0)

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
    for (i <- records.indices)
      log.appendAsFollower(MemoryRecords.withRecords(messageIds(i), Compression.NONE, 0, records(i)))
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
      log.appendAsLeader(TestUtils.singletonRecords(value = "42".getBytes), leaderEpoch = 0)

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
    assertThrows(classOf[KafkaStorageException], () => log.roll(Some(1L)))
  }

  @Test
  def testReadWithMinMessage(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 72)
    val log = createLog(logDir,  logConfig)
    val messageIds = ((0 until 50) ++ (50 until 200 by 7)).toArray
    val records = messageIds.map(id => new SimpleRecord(id.toString.getBytes))

    // now test the case that we give the offsets and use non-sequential offsets
    for (i <- records.indices)
      log.appendAsFollower(MemoryRecords.withRecords(messageIds(i), Compression.NONE, 0, records(i)))

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
    for (i <- records.indices)
      log.appendAsFollower(MemoryRecords.withRecords(messageIds(i), Compression.NONE, 0, records(i)))

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
    log.appendAsLeader(TestUtils.singletonRecords(value = "42".getBytes), leaderEpoch = 0)

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
    log.appendAsLeader(message, leaderEpoch = 0)
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
    messageSets.foreach(log.appendAsLeader(_, leaderEpoch = 0))
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
    log.appendAsLeader(MemoryRecords.withRecords(Compression.gzip().build(), new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes)), leaderEpoch = 0)
    log.appendAsLeader(MemoryRecords.withRecords(Compression.gzip().build(), new SimpleRecord("alpha".getBytes), new SimpleRecord("beta".getBytes)), leaderEpoch = 0)

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
        log.appendAsLeader(TestUtils.singletonRecords(value = i.toString.getBytes, timestamp = mockTime.milliseconds - 10), leaderEpoch = 0)

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
          leaderEpoch = 0
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

    assertThrows(classOf[RecordBatchTooLargeException], () => log.appendAsLeader(messageSet, leaderEpoch = 0))
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
      () => log.appendAsLeader(messageSetWithUnkeyedMessage, leaderEpoch = 0))
    assertTrue(e.invalidException.isInstanceOf[InvalidRecordException])
    assertEquals(1, e.recordErrors.size)
    assertEquals(0, e.recordErrors.get(0).batchIndex)
    assertTrue(e.recordErrors.get(0).message.startsWith(errorMsgPrefix))

    e = assertThrows(classOf[RecordValidationException],
      () => log.appendAsLeader(messageSetWithOneUnkeyedMessage, leaderEpoch = 0))
    assertTrue(e.invalidException.isInstanceOf[InvalidRecordException])
    assertEquals(1, e.recordErrors.size)
    assertEquals(0, e.recordErrors.get(0).batchIndex)
    assertTrue(e.recordErrors.get(0).message.startsWith(errorMsgPrefix))

    e = assertThrows(classOf[RecordValidationException],
      () => log.appendAsLeader(messageSetWithCompressedUnkeyedMessage, leaderEpoch = 0))
    assertTrue(e.invalidException.isInstanceOf[InvalidRecordException])
    assertEquals(1, e.recordErrors.size)
    assertEquals(1, e.recordErrors.get(0).batchIndex)     // batch index is 1
    assertTrue(e.recordErrors.get(0).message.startsWith(errorMsgPrefix))

    // check if metric for NoKeyCompactedTopicRecordsPerSec is logged
    assertEquals(metricsKeySet.count(_.getMBeanName.endsWith(s"${BrokerTopicMetrics.NO_KEY_COMPACTED_TOPIC_RECORDS_PER_SEC}")), 1)
    assertTrue(TestUtils.meterCount(s"${BrokerTopicMetrics.NO_KEY_COMPACTED_TOPIC_RECORDS_PER_SEC}") > 0)

    // the following should succeed without any InvalidMessageException
    log.appendAsLeader(messageSetWithKeyedMessage, leaderEpoch = 0)
    log.appendAsLeader(messageSetWithKeyedMessages, leaderEpoch = 0)
    log.appendAsLeader(messageSetWithCompressedKeyedMessage, leaderEpoch = 0)
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
    log.appendAsLeader(first, leaderEpoch = 0)

    assertThrows(classOf[RecordTooLargeException], () => log.appendAsLeader(second, leaderEpoch = 0),
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

    log.appendAsFollower(first)
    // the second record is larger then limit but appendAsFollower does not validate the size.
    log.appendAsFollower(second)
  }

  @Test
  def testLogFlushesPartitionMetadataOnAppend(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    val log = createLog(logDir, logConfig)
    val record = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("simpleValue".getBytes))

    val topicId = Uuid.randomUuid()
    log.partitionMetadataFile.get.record(topicId)

    // Should trigger a synchronous flush
    log.appendAsLeader(record, leaderEpoch = 0)
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
    assertTrue(log.topicId.isDefined)
    assertTrue(log.topicId.get == topicId)
    log.close()
  }

  @Test
  def testNoOpWhenKeepPartitionMetadataFileIsFalse(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    val log = createLog(logDir, logConfig, keepPartitionMetadataFile = false)

    val topicId = Uuid.randomUuid()
    log.assignTopicId(topicId)
    // We should not write to this file or set the topic ID
    assertFalse(log.partitionMetadataFile.get.exists())
    assertEquals(None, log.topicId)
    log.close()

    val log2 = createLog(logDir, logConfig, topicId = Some(Uuid.randomUuid()),  keepPartitionMetadataFile = false)

    // We should not write to this file or set the topic ID
    assertFalse(log2.partitionMetadataFile.get.exists())
    assertEquals(None, log2.topicId)
    log2.close()
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
    messages.foreach(log.appendAsFollower)
    val timeIndexEntries = log.logSegments.asScala.foldLeft(0) { (entries, segment) => entries + segment.timeIndex.entries }
    assertEquals(numMessages - 1, timeIndexEntries, s"There should be ${numMessages - 1} time index entries")
    assertEquals(mockTime.milliseconds + numMessages - 1, log.activeSegment.timeIndex.lastEntry.timestamp,
      s"The last time index entry should have timestamp ${mockTime.milliseconds + numMessages - 1}")
  }

  @Test
  def testFetchOffsetByTimestampIncludesLeaderEpoch(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1)
    val log = createLog(logDir, logConfig)

    assertEquals(new OffsetResultHolder(Optional.empty[FileRecords.TimestampAndOffset]()), log.fetchOffsetByTimestamp(0L))

    val firstTimestamp = mockTime.milliseconds
    val firstLeaderEpoch = 0
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      leaderEpoch = firstLeaderEpoch)

    val secondTimestamp = firstTimestamp + 1
    val secondLeaderEpoch = 1
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = secondTimestamp),
      leaderEpoch = secondLeaderEpoch)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(firstTimestamp, 0L, Optional.of(firstLeaderEpoch))),
      log.fetchOffsetByTimestamp(firstTimestamp))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(secondTimestamp, 1L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(secondTimestamp))

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP))

    // The cache can be updated directly after a leader change.
    // The new latest offset should reflect the updated epoch.
    log.maybeAssignEpochStartOffset(2, 2L)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(2))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP))
  }

  @Test
  def testFetchOffsetByTimestampWithMaxTimestampIncludesTimestamp(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1)
    val log = createLog(logDir, logConfig)

    assertEquals(new OffsetResultHolder(Optional.empty[FileRecords.TimestampAndOffset]()), log.fetchOffsetByTimestamp(0L))

    val firstTimestamp = mockTime.milliseconds
    val leaderEpoch = 0
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      leaderEpoch = leaderEpoch)

    val secondTimestamp = firstTimestamp + 1
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = secondTimestamp),
      leaderEpoch = leaderEpoch)

    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      leaderEpoch = leaderEpoch)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(secondTimestamp, 1L, Optional.of(leaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP))
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
      new Metrics()))
    remoteLogManager.setDelayedOperationPurgatory(purgatory)

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1,
      remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true, remoteLogManager = Some(remoteLogManager))
    // Note that the log is empty, so remote offset read won't happen
    assertEquals(new OffsetResultHolder(Optional.empty[FileRecords.TimestampAndOffset]()), log.fetchOffsetByTimestamp(0L, Some(remoteLogManager)))

    val firstTimestamp = mockTime.milliseconds
    val firstLeaderEpoch = 0
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      leaderEpoch = firstLeaderEpoch)

    val secondTimestamp = firstTimestamp + 1
    val secondLeaderEpoch = 1
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = secondTimestamp),
      leaderEpoch = secondLeaderEpoch)

    doAnswer(ans => {
      val timestamp = ans.getArgument(1).asInstanceOf[Long]
      Optional.of(timestamp)
        .filter(_ == firstTimestamp)
        .map[TimestampAndOffset](x => new TimestampAndOffset(x, 0L, Optional.of(firstLeaderEpoch)))
    }).when(remoteLogManager).findOffsetByTimestamp(ArgumentMatchers.eq(log.topicPartition),
      anyLong(), anyLong(), ArgumentMatchers.eq(log.leaderEpochCache.get))
    log._localLogStartOffset = 1

    def assertFetchOffsetByTimestamp(expected: Option[TimestampAndOffset], timestamp: Long): Unit = {
      val offsetResultHolder = log.fetchOffsetByTimestamp(timestamp, Some(remoteLogManager))
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
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Some(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 1L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Some(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Some(remoteLogManager)))

    // The cache can be updated directly after a leader change.
    // The new latest offset should reflect the updated epoch.
    log.maybeAssignEpochStartOffset(2, 2L)
    
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(2))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Some(remoteLogManager)))
  }

  @Test
  def testFetchLatestTieredTimestampNoRemoteStorage(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1)
    val log = createLog(logDir, logConfig)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, -1, Optional.of(-1))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP))

    val firstTimestamp = mockTime.milliseconds
    val leaderEpoch = 0
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      leaderEpoch = leaderEpoch)

    val secondTimestamp = firstTimestamp + 1
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = secondTimestamp),
      leaderEpoch = leaderEpoch)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, -1, Optional.of(-1))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP))
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
      new Metrics()))
    remoteLogManager.setDelayedOperationPurgatory(purgatory)

    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 200, indexIntervalBytes = 1,
      remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true, remoteLogManager = Some(remoteLogManager))
    // Note that the log is empty, so remote offset read won't happen
    assertEquals(new OffsetResultHolder(Optional.empty[FileRecords.TimestampAndOffset]()), log.fetchOffsetByTimestamp(0L, Some(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0, Optional.empty())),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Some(remoteLogManager)))

    val firstTimestamp = mockTime.milliseconds
    val firstLeaderEpoch = 0
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = firstTimestamp),
      leaderEpoch = firstLeaderEpoch)

    val secondTimestamp = firstTimestamp + 1
    val secondLeaderEpoch = 1
    log.appendAsLeader(TestUtils.singletonRecords(
      value = TestUtils.randomBytes(10),
      timestamp = secondTimestamp),
      leaderEpoch = secondLeaderEpoch)

    doAnswer(ans => {
      val timestamp = ans.getArgument(1).asInstanceOf[Long]
      Optional.of(timestamp)
        .filter(_ == firstTimestamp)
        .map[TimestampAndOffset](x => new TimestampAndOffset(x, 0L, Optional.of(firstLeaderEpoch)))
    }).when(remoteLogManager).findOffsetByTimestamp(ArgumentMatchers.eq(log.topicPartition),
      anyLong(), anyLong(), ArgumentMatchers.eq(log.leaderEpochCache.get))
    log._localLogStartOffset = 1
    log._highestOffsetInRemoteStorage = 0

    def assertFetchOffsetByTimestamp(expected: Option[TimestampAndOffset], timestamp: Long): Unit = {
      val offsetResultHolder = log.fetchOffsetByTimestamp(timestamp, Some(remoteLogManager))
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
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Some(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP, Some(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 1L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Some(remoteLogManager)))
    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(secondLeaderEpoch))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Some(remoteLogManager)))

    // The cache can be updated directly after a leader change.
    // The new latest offset should reflect the updated epoch.
    log.maybeAssignEpochStartOffset(2, 2L)

    assertEquals(new OffsetResultHolder(new TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(2))),
      log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Some(remoteLogManager)))
  }

  private def createKafkaConfigWithRLM: KafkaConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", "test")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
    // set log reader threads number to 2
    props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP, "2")
    KafkaConfig.fromProps(props)
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
      log.appendAsLeader(createRecords, leaderEpoch = 0)

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
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    assertEquals(log.logEndOffset, lastOffset, "Should be back to original offset")
    assertEquals(log.size, size, "Should be back to original size")
    log.truncateFullyAndStartAt(log.logEndOffset - (msgPerSeg - 1))
    assertEquals(log.logEndOffset, lastOffset - (msgPerSeg - 1), "Should change offset")
    assertEquals(log.size, 0, "Should change log size")

    for (_ <- 1 to msgPerSeg)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

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
      log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds + i), leaderEpoch = 0)
    assertEquals(1, log.numberOfSegments, "There should be exactly 1 segment.")

    mockTime.sleep(msgPerSeg)
    for (i<- 1 to msgPerSeg)
      log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds + i), leaderEpoch = 0)
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
      log.appendAsLeader(TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds + i), leaderEpoch = 0)
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
      log.appendAsLeader(createRecords, leaderEpoch = 0)

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
    log.appendAsLeader(TestUtils.singletonRecords(value = null), leaderEpoch = 0)
    val head = LogTestUtils.readLog(log, 0, 4096).records.records.iterator.next()
    assertEquals(0, head.offset)
    assertFalse(head.hasValue, "Message payload should be null.")
  }

  @Test
  def testAppendWithOutOfOrderOffsetsThrowsException(): Unit = {
    val log = createLog(logDir, new LogConfig(new Properties))

    val appendOffsets = Seq(0L, 1L, 3L, 2L, 4L)
    val buffer = ByteBuffer.allocate(512)
    for (offset <- appendOffsets) {
      val builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, Compression.NONE,
                                          TimestampType.LOG_APPEND_TIME, offset, mockTime.milliseconds(),
                                          1L, 0, 0, false, 0)
      builder.append(new SimpleRecord("key".getBytes, "value".getBytes))
      builder.close()
    }
    buffer.flip()
    val memoryRecords = MemoryRecords.readableRecords(buffer)

    assertThrows(classOf[OffsetsOutOfOrderException], () =>
      log.appendAsFollower(memoryRecords)
    )
  }

  @Test
  def testAppendBelowExpectedOffsetThrowsException(): Unit = {
    val log = createLog(logDir, new LogConfig(new Properties))
    val records = (0 until 2).map(id => new SimpleRecord(id.toString.getBytes)).toArray
    records.foreach(record => log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, record), leaderEpoch = 0))

    val magicVals = Seq(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)
    val compressionTypes = Seq(CompressionType.NONE, CompressionType.LZ4)
    for (magic <- magicVals; compressionType <- compressionTypes) {
      val compression = Compression.of(compressionType).build()
      val invalidRecord = MemoryRecords.withRecords(magic, compression, new SimpleRecord(1.toString.getBytes))
      assertThrows(classOf[UnexpectedAppendOffsetException],
        () => log.appendAsFollower(invalidRecord),
        () => s"Magic=$magic, compressionType=$compressionType")
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

      val exception = assertThrows(classOf[UnexpectedAppendOffsetException], () => log.appendAsFollower(records = batch))
      assertEquals(firstOffset, exception.firstOffset, s"Magic=$magic, compressionType=$compressionType, UnexpectedAppendOffsetException#firstOffset")
      assertEquals(firstOffset + 2, exception.lastOffset, s"Magic=$magic, compressionType=$compressionType, UnexpectedAppendOffsetException#lastOffset")
    }
  }

  @Test
  def testAppendWithNoTimestamp(): Unit = {
    val log = createLog(logDir, new LogConfig(new Properties))
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(RecordBatch.NO_TIMESTAMP, "key".getBytes, "value".getBytes)), leaderEpoch = 0)
  }

  @Test
  def testAppendToOrReadFromLogInFailedLogDir(): Unit = {
    val pid = 1L
    val epoch = 0.toShort
    val log = createLog(logDir, new LogConfig(new Properties))
    log.appendAsLeader(TestUtils.singletonRecords(value = null), leaderEpoch = 0)
    assertEquals(0, LogTestUtils.readLog(log, 0, 4096).records.records.iterator.next().offset)
    val append = LogTestUtils.appendTransactionalAsLeader(log, pid, epoch, mockTime)
    append(10)
    // Kind of a hack, but renaming the index to a directory ensures that the append
    // to the index will fail.
    log.activeSegment.txnIndex.renameTo(log.dir)
    assertThrows(classOf[KafkaStorageException], () => LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 1))
    assertThrows(classOf[KafkaStorageException], () => log.appendAsLeader(TestUtils.singletonRecords(value = null), leaderEpoch = 0))
    assertThrows(classOf[KafkaStorageException], () => LogTestUtils.readLog(log, 0, 4096).records.records.iterator.next().offset)
  }

  @Test
  def testWriteLeaderEpochCheckpointAfterDirectoryRename(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024)
    val log = createLog(logDir, logConfig)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), leaderEpoch = 5)
    assertEquals(Some(5), log.latestEpoch)

    // Ensure that after a directory rename, the epoch cache is written to the right location
    val tp = UnifiedLog.parseTopicPartitionName(log.dir)
    log.renameDir(UnifiedLog.logDeleteDirName(tp), true)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), leaderEpoch = 10)
    assertEquals(Some(10), log.latestEpoch)
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

    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), leaderEpoch = 5)
    assertEquals(Some(5), log.latestEpoch)

    // Ensure that after a directory rename, the partition metadata file is written to the right location.
    val tp = UnifiedLog.parseTopicPartitionName(log.dir)
    log.renameDir(UnifiedLog.logDeleteDirName(tp), true)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), leaderEpoch = 10)
    assertEquals(Some(10), log.latestEpoch)
    assertTrue(PartitionMetadataFile.newFile(log.dir).exists())
    assertFalse(PartitionMetadataFile.newFile(this.logDir).exists())

    // Check the topic ID remains in memory and was copied correctly.
    assertTrue(log.topicId.isDefined)
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
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("foo".getBytes()))), leaderEpoch = 5)
    assertEquals(Some(5), log.leaderEpochCache.flatMap(_.latestEpoch.toScala))

    log.appendAsFollower(TestUtils.records(List(new SimpleRecord("foo".getBytes())),
      baseOffset = 1L,
      magicValue = RecordVersion.V1.value))
    assertEquals(None, log.leaderEpochCache.flatMap(_.latestEpoch.toScala))
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
    records.foreach(segment.append _)
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
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.maybeAssignEpochStartOffset(0, 40)
    log.maybeAssignEpochStartOffset(1, 90)

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
      log.appendAsLeader(createRecords, leaderEpoch = 0)

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
    log.appendAsLeader(createRecords, leaderEpoch = 0)

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
      log.appendAsLeader(createRecords, leaderEpoch = 0)
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

  def epochCache(log: UnifiedLog): LeaderEpochFileCache = {
    log.leaderEpochCache.get
  }

  @Test
  def shouldDeleteSizeBasedSegments(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionBytes = createRecords.sizeInBytes * 10)
    val log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

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
      log.appendAsLeader(createRecords, leaderEpoch = 0)

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
      log.appendAsLeader(createRecords, leaderEpoch = 0)

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
      log.appendAsLeader(createRecords, leaderEpoch = 0)

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
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    // mark oldest segment as older the retention.ms
    log.logSegments.asScala.head.setLastModified(mockTime.milliseconds - 20000)

    val segments = log.numberOfSegments
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    assertEquals(segments, log.numberOfSegments, "There should be 3 segments remaining")
  }

  @Test
  def shouldDeleteSegmentsReadyToBeDeletedWhenCleanupPolicyIsCompactAndDelete(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes, key = "test".getBytes, timestamp = 10L)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionMs = 10000, cleanupPolicy = "compact,delete")
    val log = createLog(logDir, logConfig)

    // append some messages to create some segments
    for (_ <- 0 until 15)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

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
      log.appendAsLeader(createRecords, leaderEpoch = 0)

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
    log.maybeAssignEpochStartOffset(epoch, records.length)

    //When appending messages as a leader (i.e. assignOffsets = true)
    for (record <- records)
      log.appendAsLeader(
        MemoryRecords.withRecords(Compression.NONE, record),
        leaderEpoch = epoch
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
      log.appendAsFollower(recordsForEpoch(i))

    assertEquals(Some(42), log.latestEpoch)
  }

  @Test
  def shouldTruncateLeaderEpochsWhenDeletingSegments(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionBytes = createRecords.sizeInBytes * 10)
    val log = createLog(logDir, logConfig)
    val cache = epochCache(log)

    // Given three segments of 5 messages each
    for (_ <- 0 until 15) {
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    }

    //Given epochs
    cache.assign(0, 0)
    cache.assign(1, 5)
    cache.assign(2, 10)

    //When first segment is removed
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()

    //The oldest epoch entry should have been removed
    assertEquals(java.util.Arrays.asList(new EpochEntry(1, 5), new EpochEntry(2, 10)), cache.epochEntries)
  }

  @Test
  def shouldUpdateOffsetForLeaderEpochsWhenDeletingSegments(): Unit = {
    def createRecords = TestUtils.singletonRecords("test".getBytes)
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, retentionBytes = createRecords.sizeInBytes * 10)
    val log = createLog(logDir, logConfig)
    val cache = epochCache(log)

    // Given three segments of 5 messages each
    for (_ <- 0 until 15) {
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    }

    //Given epochs
    cache.assign(0, 0)
    cache.assign(1, 7)
    cache.assign(2, 10)

    //When first segment removed (up to offset 5)
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()

    //The first entry should have gone from (0,0) => (0,5)
    assertEquals(java.util.Arrays.asList(new EpochEntry(0, 5), new EpochEntry(1, 7), new EpochEntry(2, 10)), cache.epochEntries)
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
        log.appendAsFollower(createRecords(startOffset + i, epoch))
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

    log.appendAsLeader(records, leaderEpoch = 0)
    assertEquals(None, log.firstUnstableOffset)
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

    val firstAppendInfo = log.appendAsLeader(records, leaderEpoch = 0)
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset)

    // add more transactional records
    seq += 3
    log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.NONE, pid, epoch, seq,
      new SimpleRecord("blah".getBytes)), leaderEpoch = 0)

    // LSO should not have changed
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset)

    // now transaction is committed
    val commitAppendInfo = LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.COMMIT, mockTime.milliseconds())

    // first unstable offset is not updated until the high watermark is advanced
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset)
    log.updateHighWatermark(commitAppendInfo.lastOffset + 1)

    // now there should be no first unstable offset
    assertEquals(None, log.firstUnstableOffset)
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

        val readInfo = log.read(
          startOffset = currentLogEndOffset,
          maxLength = Int.MaxValue,
          isolation = FetchIsolation.TXN_COMMITTED,
          minOneMessage = false)

        if (readInfo.records.sizeInBytes() > 0)
          nonEmptyReads += 1

        LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, producerEpoch, ControlRecordType.ABORT, mockTime.milliseconds())
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
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 30
    appendPid2(6) // 36
    appendPid4(3) // 39
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 49
    appendPid3(9) // 58
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 59
    appendPid4(8) // 67
    appendPid2(7) // 74
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT, mockTime.milliseconds()) // 75
    LogTestUtils.appendNonTransactionalAsLeader(log, 10) // 85
    appendPid4(4) // 89
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT, mockTime.milliseconds()) // 90

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
    assertEquals(None, log.firstUnstableOffset)
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

    appendAsFollower(log, MemoryRecords.readableRecords(buffer))

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
    assertEquals(None, log.firstUnstableOffset)
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
    val segmentOpt = log.logSegments(segmentBaseOffset, segmentBaseOffset + 1).headOption
    assertTrue(segmentOpt.isDefined)

    val segment = segmentOpt.get
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
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 1)

    append(5)
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.COMMIT, mockTime.milliseconds(), coordinatorEpoch = 2)

    assertThrows(
      classOf[TransactionCoordinatorFencedException],
      () => LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 1))
  }

  @Test
  def testZombieCoordinatorFencedEmptyTransaction(): Unit = {
    val pid = 1L
    val epoch = 0.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    val buffer = ByteBuffer.allocate(256)
    val append = appendTransactionalToBuffer(buffer, pid, epoch, leaderEpoch = 1)
    append(0, 10)
    appendEndTxnMarkerToBuffer(buffer, pid, epoch, 10L, ControlRecordType.COMMIT, leaderEpoch = 1)

    buffer.flip()
    log.appendAsFollower(MemoryRecords.readableRecords(buffer))

    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 2, leaderEpoch = 1)
    LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 2, leaderEpoch = 1)
    assertThrows(classOf[TransactionCoordinatorFencedException],
      () => LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 1, leaderEpoch = 1))
  }

  @Test
  def testEndTxnWithFencedProducerEpoch(): Unit = {
    val producerId = 1L
    val epoch = 5.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)
    LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 1)

    assertThrows(classOf[InvalidProducerEpochException],
      () => LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, (epoch - 1).toShort, ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 1))
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

    assertEquals(Some(0L), log.firstUnstableOffset)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(5L, LogStartOffsetIncrementReason.ClientRecordDeletion)

    // the first unstable offset should be lower bounded by the log start offset
    assertEquals(Some(5L), log.firstUnstableOffset)
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

    assertEquals(Some(0L), log.firstUnstableOffset)

    log.updateHighWatermark(log.logEndOffset)
    log.maybeIncrementLogStartOffset(8L, LogStartOffsetIncrementReason.ClientRecordDeletion)
    log.updateHighWatermark(log.logEndOffset)
    log.deleteOldSegments()
    assertEquals(1, log.logSegments.size)

    // the first unstable offset should be lower bounded by the log start offset
    assertEquals(Some(8L), log.firstUnstableOffset)
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
      () => LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 1))
    assertEquals(11L, log.logEndOffset)
    assertEquals(0L, log.lastStableOffset)

    // Try the append a second time. The appended offset in the log should not increase
    // because the log dir is marked as failed.  Nor will there be a write to the transaction
    // index.
    assertThrows(
      classOf[KafkaStorageException],
      () => LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), coordinatorEpoch = 1))
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
    assertEquals(None, reopenedLog.firstUnstableOffset)
  }

  @Test
  def testOffsetSnapshot(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = createLog(logDir, logConfig)

    // append a few records
    appendAsFollower(log, MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord("a".getBytes),
      new SimpleRecord("b".getBytes),
      new SimpleRecord("c".getBytes)), 5)


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
      new SimpleRecord("c".getBytes)), leaderEpoch = 0)
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset)

    // mix in some non-transactional data
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord("g".getBytes),
      new SimpleRecord("h".getBytes),
      new SimpleRecord("i".getBytes)), leaderEpoch = 0)

    // append data from a second transactional producer
    val secondAppendInfo = log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.NONE, pid2, epoch, seq2,
      new SimpleRecord("d".getBytes),
      new SimpleRecord("e".getBytes),
      new SimpleRecord("f".getBytes)), leaderEpoch = 0)

    // LSO should not have changed
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset)

    // now first producer's transaction is aborted
    val abortAppendInfo = LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT, mockTime.milliseconds())
    log.updateHighWatermark(abortAppendInfo.lastOffset + 1)

    // LSO should now point to one less than the first offset of the second transaction
    assertEquals(Some(secondAppendInfo.firstOffset), log.firstUnstableOffset)

    // commit the second transaction
    val commitAppendInfo = LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.COMMIT, mockTime.milliseconds())
    log.updateHighWatermark(commitAppendInfo.lastOffset + 1)

    // now there should be no first unstable offset
    assertEquals(None, log.firstUnstableOffset)
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

    val firstAppendInfo = log.appendAsLeader(records, leaderEpoch = 0)
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset)

    // this write should spill to the second segment
    seq = 3
    log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.NONE, pid, epoch, seq,
      new SimpleRecord("d".getBytes),
      new SimpleRecord("e".getBytes),
      new SimpleRecord("f".getBytes)), leaderEpoch = 0)
    assertEquals(Some(firstAppendInfo.firstOffset), log.firstUnstableOffset)
    assertEquals(3L, log.logEndOffsetMetadata.segmentBaseOffset)

    // now abort the transaction
    val abortAppendInfo = LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch, ControlRecordType.ABORT, mockTime.milliseconds())
    log.updateHighWatermark(abortAppendInfo.lastOffset + 1)
    assertEquals(None, log.firstUnstableOffset)

    // now check that a fetch includes the aborted transaction
    val fetchDataInfo = log.read(0L,
      maxLength = 2048,
      isolation = FetchIsolation.TXN_COMMITTED,
      minOneMessage = true)

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
      log.appendAsLeader(records, leaderEpoch = 0)
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
      log.appendAsLeader(records, leaderEpoch = 0)
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
      log.appendAsLeader(records, leaderEpoch = 0)
    }

    log.updateHighWatermark(25L)
    assertThrows(classOf[OffsetOutOfRangeException], () => log.maybeIncrementLogStartOffset(26L, LogStartOffsetIncrementReason.ClientRecordDeletion))
  }

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
    assertTrue(log.leaderEpochCache.isEmpty)
    assertTrue(log.partitionMetadataFile.isEmpty)
    assertEquals(0, log.logEndOffset)
    // verify that records appending can still succeed
    // even with the uninitialized leaderEpochCache and partitionMetadataFile
    val records = TestUtils.records(List(new SimpleRecord(mockTime.milliseconds, "key".getBytes, "value".getBytes)))
    log.appendAsLeader(records, leaderEpoch = 0)
    assertEquals(1, log.logEndOffset)

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
      log.appendAsLeader(records, leaderEpoch = 0)
    }

    assertEquals(Some(99L), log.maybeUpdateHighWatermark(99L))
    assertEquals(None, log.maybeUpdateHighWatermark(99L))

    assertEquals(Some(100L), log.maybeUpdateHighWatermark(100L))
    assertEquals(None, log.maybeUpdateHighWatermark(100L))

    // bound by the log end offset
    assertEquals(None, log.maybeUpdateHighWatermark(101L))
  }

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
        log.appendAsLeader(records, leaderEpoch = 0)
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
      log.appendAsLeader(records, leaderEpoch = 0)
    }

    log.updateHighWatermark(80L)
    val newLogStartOffset = 40L
    log.maybeIncrementLogStartOffset(newLogStartOffset, LogStartOffsetIncrementReason.SegmentDeletion)
    assertEquals(newLogStartOffset, log.logStartOffset)
    assertEquals(log.logStartOffset, log.localLogStartOffset())

    // Truncate the local log and verify that the offsets are updated to expected values
    val newLocalLogStartOffset = 60L
    log.truncateFullyAndStartAt(newLocalLogStartOffset, Option.apply(newLogStartOffset))
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
    log.truncateFullyAndStartAt(4)
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
  def testTransactionIsOngoingAndVerificationGuard(appendOrigin: AppendOrigin): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, true)

    val producerId = 23L
    val producerEpoch = 1.toShort
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

    val verificationGuard = log.maybeStartTransactionVerification(producerId, sequence, producerEpoch)
    assertNotEquals(VerificationGuard.SENTINEL, verificationGuard)

    log.appendAsLeader(idempotentRecords, origin = appendOrigin, leaderEpoch = 0)
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))

    // Since we wrote idempotent records, we keep VerificationGuard.
    assertEquals(verificationGuard, log.verificationGuard(producerId))

    // Now write the transactional records
    assertTrue(log.verificationGuard(producerId).verify(verificationGuard))
    log.appendAsLeader(transactionalRecords, origin = appendOrigin, leaderEpoch = 0, verificationGuard = verificationGuard)
    assertTrue(log.hasOngoingTransaction(producerId, producerEpoch))
    // VerificationGuard should be cleared now.
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))

    // A subsequent maybeStartTransactionVerification will be empty since we are already verified.
    assertEquals(VerificationGuard.SENTINEL, log.maybeStartTransactionVerification(producerId, sequence, producerEpoch))

    val endTransactionMarkerRecord = MemoryRecords.withEndTransactionMarker(
      producerId,
      producerEpoch,
      new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    )

    log.appendAsLeader(endTransactionMarkerRecord, origin = AppendOrigin.COORDINATOR, leaderEpoch = 0)
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))

    if (appendOrigin == AppendOrigin.CLIENT)
      sequence = sequence + 1

    // A new maybeStartTransactionVerification will not be empty, as we need to verify the next transaction.
    val newVerificationGuard = log.maybeStartTransactionVerification(producerId, sequence, producerEpoch)
    assertNotEquals(VerificationGuard.SENTINEL, newVerificationGuard)
    assertNotEquals(verificationGuard, newVerificationGuard)
    assertFalse(verificationGuard.verify(newVerificationGuard))
  }

  @Test
  def testEmptyTransactionStillClearsVerificationGuard(): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, true)

    val producerId = 23L
    val producerEpoch = 1.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig)

    val verificationGuard = log.maybeStartTransactionVerification(producerId, 0, producerEpoch)
    assertNotEquals(VerificationGuard.SENTINEL, verificationGuard)

    val endTransactionMarkerRecord = MemoryRecords.withEndTransactionMarker(
      producerId,
      producerEpoch,
      new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    )

    log.appendAsLeader(endTransactionMarkerRecord, origin = AppendOrigin.COORDINATOR, leaderEpoch = 0)
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))
  }

  @Test
  def testDisabledVerificationClearsVerificationGuard(): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, true)

    val producerId = 23L
    val producerEpoch = 1.toShort
    val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
    val log = createLog(logDir, logConfig, producerStateManagerConfig = producerStateManagerConfig)

    val verificationGuard = log.maybeStartTransactionVerification(producerId, 0, producerEpoch)
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
    log.appendAsLeader(transactionalRecords, leaderEpoch = 0)

    assertTrue(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))
  }

  @Test
  def testEnablingVerificationWhenRequestIsAtLogLayer(): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, false)

    val producerId = 23L
    val producerEpoch = 1.toShort
    val sequence = 4
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
    assertThrows(classOf[InvalidTxnStateException], () => log.appendAsLeader(transactionalRecords, leaderEpoch = 0))
    assertFalse(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))

    val verificationGuard = log.maybeStartTransactionVerification(producerId, sequence, producerEpoch)
    assertNotEquals(VerificationGuard.SENTINEL, verificationGuard)

    log.appendAsLeader(transactionalRecords, leaderEpoch = 0, verificationGuard = verificationGuard)
    assertTrue(log.hasOngoingTransaction(producerId, producerEpoch))
    assertEquals(VerificationGuard.SENTINEL, log.verificationGuard(producerId))
  }

  @Test
  def testAllowNonZeroSequenceOnFirstAppendNonZeroEpoch(): Unit = {
    val producerStateManagerConfig = new ProducerStateManagerConfig(86400000, true)

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

    val verificationGuard = log.maybeStartTransactionVerification(producerId, sequence, producerEpoch)
    // Append should not throw error.
    log.appendAsLeader(transactionalRecords, leaderEpoch = 0, verificationGuard = verificationGuard)
  }

  @Test
  def testRecoveryPointNotIncrementedOnProducerStateSnapshotFlushFailure(): Unit = {
    val logConfig = LogTestUtils.createLogConfig()
    val log = spy(createLog(logDir, logConfig))

    doThrow(new KafkaStorageException("Injected exception")).when(log).flushProducerStateSnapshot(any())

    log.appendAsLeader(TestUtils.singletonRecords("a".getBytes), leaderEpoch = 0)
    try {
      log.roll(Some(1L))
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
      log.appendAsLeader(records, leaderEpoch = 0)
      log.roll()
    }
    log.maybeIncrementHighWatermark(log.logEndOffsetMetadata)

    assertEquals(10, log.logSegments.size())

    {
      val deletable = log.deletableSegments(
        (segment: LogSegment, _: Option[LogSegment]) => segment.baseOffset <= 5)
      val expected = log.nonActiveLogSegmentsFrom(0L).asScala.filter(segment => segment.baseOffset <= 5).toList
      assertEquals(6, expected.length)
      assertEquals(expected, deletable.toList)
    }

    {
      val deletable = log.deletableSegments((_: LogSegment, _: Option[LogSegment]) => true)
      val expected = log.nonActiveLogSegmentsFrom(0L).asScala.toList
      assertEquals(9, expected.length)
      assertEquals(expected, deletable.toList)
    }

    {
      val records = TestUtils.records(List(
        new SimpleRecord(mockTime.milliseconds, "a".getBytes),
      ))
      log.appendAsLeader(records, leaderEpoch = 0)
      log.maybeIncrementHighWatermark(log.logEndOffsetMetadata)
      val deletable = log.deletableSegments((_: LogSegment, _: Option[LogSegment]) => true)
      val expected = log.logSegments.asScala.toList
      assertEquals(10, expected.length)
      assertEquals(expected, deletable.toList)
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
      log.appendAsLeader(records, leaderEpoch = 0)
      log.roll()
    }
    log.maybeIncrementHighWatermark(log.logEndOffsetMetadata)

    assertEquals(10, log.logSegments.size())

    var offset = 0
    val deletableSegments = log.deletableSegments(
      (segment: LogSegment, nextSegmentOpt: Option[LogSegment]) => {
        assertEquals(offset, segment.baseOffset)
        val logSegments = new LogSegments(log.topicPartition)
        log.logSegments.forEach(segment => logSegments.add(segment))
        val floorSegmentOpt = logSegments.floorSegment(offset)
        assertTrue(floorSegmentOpt.isPresent)
        assertEquals(floorSegmentOpt.get, segment)
        if (offset == log.logEndOffset) {
          assertFalse(nextSegmentOpt.isDefined)
        } else {
          assertTrue(nextSegmentOpt.isDefined)
          val higherSegmentOpt = logSegments.higherSegment(segment.baseOffset)
          assertTrue(higherSegmentOpt.isPresent)
          assertEquals(segment.baseOffset + 1, higherSegmentOpt.get.baseOffset)
          assertEquals(higherSegmentOpt.get, nextSegmentOpt.get)
        }
        offset += 1
        true
      })
    assertEquals(10L, log.logSegments.size())
    assertEquals(log.nonActiveLogSegmentsFrom(0L).asScala.toSeq, deletableSegments.toSeq)
  }

  @Test
  def testActiveSegmentDeletionDueToRetentionTimeBreachWithRemoteStorage(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(indexIntervalBytes = 1, segmentIndexBytes = 12,
      retentionMs = 3, localRetentionMs = 1, fileDeleteDelayMs = 0, remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)

    // Append 1 message to the active segment
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes))),
      leaderEpoch = 0)
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
      producerId = pid, producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 1), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 2), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("d".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 3), leaderEpoch = 1)
    log.roll()
    log.appendAsLeader(TestUtils.records(List(new SimpleRecord("e".getBytes)),
      producerId = pid, producerEpoch = epoch, sequence = 4), leaderEpoch = 2)
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
      log.appendAsLeader(createRecords, leaderEpoch = 0)
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
      log.appendAsLeader(createRecords, leaderEpoch = 0)
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
      log.appendAsLeader(createRecords, leaderEpoch = 0)
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
      log.appendAsLeader(createRecords, leaderEpoch = 0)
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
      val info = log.appendAsLeader(records, leaderEpoch = 0)
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
      val info = log.appendAsLeader(records, leaderEpoch = 0)
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

    val segments: java.util.List[LogSegment] = new java.util.ArrayList[LogSegment]()
    val seg1 = LogTestUtils.createSegment(1, logDir, 10, Time.SYSTEM)
    val seg2 = LogTestUtils.createSegment(2, logDir, 10, Time.SYSTEM)
    segments.add(seg1)
    segments.add(seg2)
    assertEquals(Seq(Long.MaxValue, Long.MaxValue), log.getFirstBatchTimestampForSegments(segments).asScala.toSeq)

    seg1.append(1, 1000L, 1, MemoryRecords.withRecords(1, Compression.NONE, new SimpleRecord("one".getBytes)))
    seg2.append(2, 2000L, 1, MemoryRecords.withRecords(2, Compression.NONE, new SimpleRecord("two".getBytes)))
    assertEquals(Seq(1000L, 2000L), log.getFirstBatchTimestampForSegments(segments).asScala.toSeq)

    seg1.close()
    seg2.close()
  }

  @Test
  def testFetchOffsetByTimestampShouldReadOnlyLocalLogWhenLogIsEmpty(): Unit = {
    val logConfig = LogTestUtils.createLogConfig(remoteLogStorageEnable = true)
    val log = createLog(logDir, logConfig, remoteStorageSystemEnable = true)
    val result = log.fetchOffsetByTimestamp(mockTime.milliseconds(), Some(null))
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

  private def appendAsFollower(log: UnifiedLog, records: MemoryRecords, leaderEpoch: Int = 0): Unit = {
    records.batches.forEach(_.setPartitionLeaderEpoch(leaderEpoch))
    log.appendAsFollower(records)
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
                        keepPartitionMetadataFile: Boolean = true,
                        remoteStorageSystemEnable: Boolean = false,
                        remoteLogManager: Option[RemoteLogManager] = None,
                        logOffsetsListener: LogOffsetsListener = LogOffsetsListener.NO_OP_OFFSETS_LISTENER): UnifiedLog = {
    val log = LogTestUtils.createLog(dir, config, brokerTopicStats, scheduler, time, logStartOffset, recoveryPoint,
      maxTransactionTimeoutMs, producerStateManagerConfig, producerIdExpirationCheckIntervalMs,
      lastShutdownClean, topicId, keepPartitionMetadataFile, new ConcurrentHashMap[String, Integer],
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
