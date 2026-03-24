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
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.internal._
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.common.{RequestLocal, TransactionVersion}
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig
import org.apache.kafka.server.log.remote.storage.RemoteLogManager
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.{MockTime, Scheduler}

import org.apache.kafka.common.message.AbortedTxn
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, LogFileUtils, LogOffsetMetadata, LogOffsetSnapshot, LogOffsetsListener, LogSegment, LogSegments, LogStartOffsetIncrementReason, OffsetResultHolder, ProducerStateManagerConfig, UnifiedLog, VerificationGuard}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, _}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, ValueSource}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doThrow, spy}

import java.io._
import java.nio.ByteBuffer
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
      new AbortedTxn().setProducerId(pid1).setFirstOffset(0L).setLastOffset(29L).setLastStableOffset(8L),
      new AbortedTxn().setProducerId(pid2).setFirstOffset(8L).setLastOffset(74L).setLastStableOffset(36L)
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
    assertTrue(log.deleteOldSegments > 0, "At least one segment should be deleted")
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
    assertTrue(log.deleteOldSegments > 0, "At least one segment should be deleted")
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
