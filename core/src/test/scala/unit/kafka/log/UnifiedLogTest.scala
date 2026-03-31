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
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record.internal._
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.common.{RequestLocal, TransactionVersion}
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig
import org.apache.kafka.server.log.remote.storage.RemoteLogManager
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.util.{MockTime, Scheduler}

import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, LogFileUtils, LogOffsetMetadata, LogOffsetsListener, LogSegment, LogSegments, LogStartOffsetIncrementReason, OffsetResultHolder, ProducerStateManagerConfig, UnifiedLog, VerificationGuard}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, ValueSource}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doThrow, spy}

import java.io._
import java.nio.file.Files
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.Optional
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
    assertEquals(0, log.deleteOldSegments())
    assertEquals(2, log.logSegments.size)
    log.logSegments.asScala.zipWithIndex.foreach {
      case (segment, idx) => assertEquals(idx, segment.baseOffset)
    }

    // Once rolled, the segment should be uploaded to remote storage and eligible for deletion
    log.updateHighestOffsetInRemoteStorage(1)
    assertTrue(log.deleteOldSegments > 0, "At least one segment should be deleted")
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
    assertEquals(0, log.deleteOldSegments())
    mockTime.sleep(1)
    assertEquals(2, log.logSegments.size)
    assertFalse(log.isEmpty)

    // Update the log-start-offset from 0 to 3, then the base segment should not be eligible for deletion
    log.updateLogStartOffsetFromRemoteTier(3L)
    assertEquals(0, log.deleteOldSegments())
    mockTime.sleep(1)
    assertEquals(2, log.logSegments.size)
    assertFalse(log.isEmpty)

    // Update the log-start-offset from 3 to 4, then the base segment should be eligible for deletion now even
    // if it is not uploaded to remote storage
    log.updateLogStartOffsetFromRemoteTier(4L)
    assertTrue(log.deleteOldSegments > 0, "At least one segment should be deleted")
    mockTime.sleep(1)
    assertEquals(1, log.logSegments.size)
    assertFalse(log.isEmpty)

    log.updateLogStartOffsetFromRemoteTier(5L)
    assertEquals(0, log.deleteOldSegments())
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
    assertTrue(log.deleteOldSegments > 0, "At least one segment should be deleted")
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
    assertTrue(log.deleteOldSegments > 0, "At least one segment should be deleted")
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
    assertEquals(0, log.deleteOldSegments())
    assertEquals(6, log.logSegments.size())
    assertEquals(0, log.logStartOffset)
    assertEquals(0, log.localLogStartOffset())

    // simulate calls to upload 2 segments to remote storage
    log.updateHighestOffsetInRemoteStorage(1)

    assertTrue(log.deleteOldSegments > 0, "At least one segment should be deleted")
    assertEquals(4, log.logSegments.size())
    assertEquals(0, log.logStartOffset)
    assertEquals(2, log.localLogStartOffset())

    // add remoteCopyDisabled = true
    val copyDisabledLogConfig = LogTestUtils.createLogConfig(segmentBytes = segmentBytes, localRetentionBytes = 1, retentionBytes = segmentBytes * 5,
      fileDeleteDelayMs = 0, remoteLogStorageEnable = true, remoteLogCopyDisable = true)
    log.updateConfig(copyDisabledLogConfig)

    // No local logs will be deleted even though local retention bytes is 1 because we'll adopt retention.ms/bytes
    // when remote.log.copy.disable = true
    assertEquals(0, log.deleteOldSegments())
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
    assertTrue(log.deleteOldSegments > 0, "At least one segment should be deleted")
    assertEquals(5, log.logSegments.size())
    assertEquals(4, log.logStartOffset)
    assertEquals(4, log.localLogStartOffset())

    // add localRetentionMs = 1, retentionMs = 1000
    val retentionMsConfig = LogTestUtils.createLogConfig(segmentBytes = segmentBytes, localRetentionMs = 1, retentionMs = 1000,
      fileDeleteDelayMs = 0, remoteLogStorageEnable = true, remoteLogCopyDisable = true)
    log.updateConfig(retentionMsConfig)

    // Should not delete any logs because no local logs expired using retention.ms = 1000
    mockTime.sleep(10)
    assertEquals(0, log.deleteOldSegments())
    assertEquals(5, log.logSegments.size())
    assertEquals(4, log.logStartOffset)
    assertEquals(4, log.localLogStartOffset())

    // Should delete all logs because all of them are expired based on retentionMs = 1000
    mockTime.sleep(1000)
    assertTrue(log.deleteOldSegments > 0, "At least one segment should be deleted")
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

    assertTrue(log.deleteOldSegments > 0, "At least one segment should be deleted")
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

    assertTrue(log.deleteOldSegments > 0, "At least one segment should be deleted")
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
