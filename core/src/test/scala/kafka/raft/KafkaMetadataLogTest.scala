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
package kafka.raft

import kafka.log.UnifiedLog
import kafka.server.{KafkaConfig, KafkaRaftServer}
import kafka.utils.TestUtils
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.{InvalidConfigurationException, RecordTooLargeException}
import org.apache.kafka.common.protocol
import org.apache.kafka.common.protocol.{ObjectSerializationCache, Writable}
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.raft._
import org.apache.kafka.raft.internals.BatchBuilder
import org.apache.kafka.server.common.serialization.RecordSerde
import org.apache.kafka.server.config.{KRaftConfigs, ServerLogConfigs}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.snapshot.{FileRawSnapshotWriter, RawSnapshotReader, RawSnapshotWriter, SnapshotPath, Snapshots}
import org.apache.kafka.storage.internals.log.{LogConfig, LogStartOffsetIncrementReason}
import org.apache.kafka.test.TestUtils.assertOptional
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.{Files, Path}
import java.util
import java.util.{Collections, Optional, Properties}
import scala.jdk.CollectionConverters._
import scala.util.Using

final class KafkaMetadataLogTest {
  import KafkaMetadataLogTest._

  var tempDir: File = _
  val mockTime = new MockTime()

  @BeforeEach
  def setUp(): Unit = {
    tempDir = TestUtils.tempDir()
  }

  @AfterEach
  def tearDown(): Unit = {
    Utils.delete(tempDir)
  }

  @Test
  def testConfig(): Unit = {
    val props = new Properties()
    props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, util.Arrays.asList("broker"))
    props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9093")
    props.put(KRaftConfigs.NODE_ID_CONFIG, Int.box(2))
    props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
    props.put(KRaftConfigs.METADATA_LOG_SEGMENT_BYTES_CONFIG, Int.box(10240))
    props.put(KRaftConfigs.METADATA_LOG_SEGMENT_MILLIS_CONFIG, Int.box(10 * 1024))
    assertThrows(classOf[InvalidConfigurationException], () => {
      val kafkaConfig = KafkaConfig.fromProps(props)
      val metadataConfig = MetadataLogConfig(kafkaConfig, KafkaRaftClient.MAX_BATCH_SIZE_BYTES, KafkaRaftClient.MAX_FETCH_SIZE_BYTES)
      buildMetadataLog(tempDir, mockTime, metadataConfig)
    })

    props.put(KRaftConfigs.METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG, Int.box(10240))
    val kafkaConfig = KafkaConfig.fromProps(props)
    val metadataConfig = MetadataLogConfig(kafkaConfig, KafkaRaftClient.MAX_BATCH_SIZE_BYTES, KafkaRaftClient.MAX_FETCH_SIZE_BYTES)
    buildMetadataLog(tempDir, mockTime, metadataConfig)
  }

  @Test
  def testUnexpectedAppendOffset(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    val recordFoo = new SimpleRecord("foo".getBytes())
    val currentEpoch = 3
    val initialOffset = log.endOffset().offset

    log.appendAsLeader(
      MemoryRecords.withRecords(initialOffset, Compression.NONE, currentEpoch, recordFoo),
      currentEpoch
    )

    // Throw exception for out of order records
    assertThrows(
      classOf[RuntimeException],
      () => {
        log.appendAsLeader(
          MemoryRecords.withRecords(initialOffset, Compression.NONE, currentEpoch, recordFoo),
          currentEpoch
        )
      }
    )

    assertThrows(
      classOf[RuntimeException],
      () => {
        log.appendAsFollower(
          MemoryRecords.withRecords(initialOffset, Compression.NONE, currentEpoch, recordFoo)
        )
      }
    )
  }

  @Test
  def testCreateSnapshot(): Unit = {
    val numberOfRecords = 10
    val epoch = 1
    val snapshotId = new OffsetAndEpoch(numberOfRecords, epoch)
    val log = buildMetadataLog(tempDir, mockTime)

    append(log, numberOfRecords, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords))
    createNewSnapshot(log, snapshotId)

    assertEquals(0, log.readSnapshot(snapshotId).get().sizeInBytes())
  }

  @Test
  def testCreateSnapshotFromEndOffset(): Unit = {
    val numberOfRecords = 10
    val firstEpoch = 1
    val secondEpoch = 3
    val log = buildMetadataLog(tempDir, mockTime)

    append(log, numberOfRecords, firstEpoch)
    append(log, numberOfRecords, secondEpoch)
    log.updateHighWatermark(new LogOffsetMetadata(2 * numberOfRecords))

    // Test finding the first epoch
    log.createNewSnapshot(new OffsetAndEpoch(numberOfRecords, firstEpoch)).get().close()
    log.createNewSnapshot(new OffsetAndEpoch(numberOfRecords - 1, firstEpoch)).get().close()
    log.createNewSnapshot(new OffsetAndEpoch(1, firstEpoch)).get().close()

    // Test finding the second epoch
    log.createNewSnapshot(new OffsetAndEpoch(2 * numberOfRecords, secondEpoch)).get().close()
    log.createNewSnapshot(new OffsetAndEpoch(2 * numberOfRecords - 1, secondEpoch)).get().close()
    log.createNewSnapshot(new OffsetAndEpoch(numberOfRecords + 1, secondEpoch)).get().close()
  }

  @Test
  def testCreateSnapshotLaterThanHighWatermark(): Unit = {
    val numberOfRecords = 10
    val epoch = 1
    val log = buildMetadataLog(tempDir, mockTime)

    append(log, numberOfRecords, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords))

    assertThrows(
      classOf[IllegalArgumentException],
      () => log.createNewSnapshot(new OffsetAndEpoch(numberOfRecords + 1, epoch))
    )
  }

  @Test
  def testCreateSnapshotMuchLaterEpoch(): Unit = {
    val numberOfRecords = 10
    val epoch = 1
    val log = buildMetadataLog(tempDir, mockTime)

    append(log, numberOfRecords, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords))

    assertThrows(
      classOf[IllegalArgumentException],
      () => log.createNewSnapshot(new OffsetAndEpoch(numberOfRecords, epoch + 1))
    )
  }

  @Test
  def testHighWatermarkOffsetMetadata(): Unit = {
    val numberOfRecords = 10
    val epoch = 1
    val log = buildMetadataLog(tempDir, mockTime)

    append(log, numberOfRecords, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords))

    val highWatermarkMetadata = log.highWatermark
    assertEquals(numberOfRecords, highWatermarkMetadata.offset)
    assertTrue(highWatermarkMetadata.metadata.isPresent)

    val segmentPosition = highWatermarkMetadata.metadata.get().asInstanceOf[SegmentPosition]
    assertEquals(0, segmentPosition.baseOffset)
    assertTrue(segmentPosition.relativePosition > 0)
  }

  @Test
  def testCreateSnapshotBeforeLogStartOffset(): Unit = {
    val numberOfRecords = 10
    val epoch = 1
    val snapshotId = new OffsetAndEpoch(numberOfRecords-4, epoch)
    val log = buildMetadataLog(tempDir, mockTime)

    append(log, numberOfRecords, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords))
    createNewSnapshot(log, snapshotId)

    // Simulate log cleanup that advances the LSO
    log.log.maybeIncrementLogStartOffset(snapshotId.offset - 1, LogStartOffsetIncrementReason.SegmentDeletion)

    assertEquals(Optional.empty(), log.createNewSnapshot(new OffsetAndEpoch(snapshotId.offset - 2, snapshotId.epoch)))
  }

  @Test
  def testCreateSnapshotDivergingEpoch(): Unit = {
    val numberOfRecords = 10
    val epoch = 2
    val snapshotId = new OffsetAndEpoch(numberOfRecords, epoch)
    val log = buildMetadataLog(tempDir, mockTime)

    append(log, numberOfRecords, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords))

    assertThrows(
      classOf[IllegalArgumentException],
      () => log.createNewSnapshot(new OffsetAndEpoch(snapshotId.offset, snapshotId.epoch - 1))
    )
  }

  @Test
  def testCreateSnapshotOlderEpoch(): Unit = {
    val numberOfRecords = 10
    val epoch = 2
    val snapshotId = new OffsetAndEpoch(numberOfRecords, epoch)
    val log = buildMetadataLog(tempDir, mockTime)

    append(log, numberOfRecords, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords))
    createNewSnapshot(log, snapshotId)

    assertThrows(
      classOf[IllegalArgumentException],
      () => log.createNewSnapshot(new OffsetAndEpoch(snapshotId.offset, snapshotId.epoch - 1))
    )
  }

  @Test
  def testCreateSnapshotWithMissingEpoch(): Unit = {
    val firstBatchRecords = 5
    val firstEpoch = 1
    val missingEpoch = firstEpoch + 1
    val secondBatchRecords = 5
    val secondEpoch = missingEpoch + 1

    val numberOfRecords = firstBatchRecords + secondBatchRecords
    val log = buildMetadataLog(tempDir, mockTime)

    append(log, firstBatchRecords, firstEpoch)
    append(log, secondBatchRecords, secondEpoch)
    log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords))

    assertThrows(
      classOf[IllegalArgumentException],
      () => log.createNewSnapshot(new OffsetAndEpoch(1, missingEpoch))
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => log.createNewSnapshot(new OffsetAndEpoch(firstBatchRecords, missingEpoch))
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => log.createNewSnapshot(new OffsetAndEpoch(secondBatchRecords, missingEpoch))
    )
  }

  @Test
  def testCreateExistingSnapshot(): Unit = {
    val numberOfRecords = 10
    val epoch = 1
    val snapshotId = new OffsetAndEpoch(numberOfRecords - 1, epoch)
    val log = buildMetadataLog(tempDir, mockTime)

    append(log, numberOfRecords, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords))
    createNewSnapshot(log, snapshotId)

    assertEquals(Optional.empty(), log.createNewSnapshot(snapshotId),
      "Creating an existing snapshot should not do anything")
  }

  @Test
  def testTopicId(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    assertEquals(KafkaRaftServer.MetadataTopicId, log.topicId())
  }

  @Test
  def testReadMissingSnapshot(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    assertEquals(Optional.empty(), log.readSnapshot(new OffsetAndEpoch(10, 0)))
  }

  @Test
  def testDeleteNonExistentSnapshot(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)
    val offset = 10
    val epoch = 0

    append(log, offset, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(offset))

    assertFalse(log.deleteBeforeSnapshot(new OffsetAndEpoch(2L, epoch)))
    assertEquals(0, log.startOffset)
    assertEquals(epoch, log.lastFetchedEpoch)
    assertEquals(offset, log.endOffset().offset)
    assertEquals(offset, log.highWatermark.offset)
  }

  @Test
  def testTruncateFullyToLatestSnapshot(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)
    val numberOfRecords = 10
    val epoch = 0
    val sameEpochSnapshotId = new OffsetAndEpoch(2 * numberOfRecords, epoch)

    append(log, numberOfRecords, epoch)
    createNewSnapshotUnckecked(log, sameEpochSnapshotId)

    assertTrue(log.truncateToLatestSnapshot())
    assertEquals(sameEpochSnapshotId.offset, log.startOffset)
    assertEquals(sameEpochSnapshotId.epoch, log.lastFetchedEpoch)
    assertEquals(sameEpochSnapshotId.offset, log.endOffset().offset)
    assertEquals(sameEpochSnapshotId.offset, log.highWatermark.offset)

    val greaterEpochSnapshotId = new OffsetAndEpoch(3 * numberOfRecords, epoch + 1)

    append(log, numberOfRecords, epoch)
    createNewSnapshotUnckecked(log, greaterEpochSnapshotId)

    assertTrue(log.truncateToLatestSnapshot())
    assertEquals(greaterEpochSnapshotId.offset, log.startOffset)
    assertEquals(greaterEpochSnapshotId.epoch, log.lastFetchedEpoch)
    assertEquals(greaterEpochSnapshotId.offset, log.endOffset().offset)
    assertEquals(greaterEpochSnapshotId.offset, log.highWatermark.offset)
  }

  @Test
  def testTruncateWillRemoveOlderSnapshot(): Unit = {
    val (logDir, log, config) = buildMetadataLogAndDir(tempDir, mockTime)
    val numberOfRecords = 10
    val epoch = 1

    append(log, 1, epoch - 1)
    val oldSnapshotId1 = new OffsetAndEpoch(1, epoch - 1)
    createNewSnapshotUnckecked(log, oldSnapshotId1)

    append(log, 1, epoch)
    val oldSnapshotId2 = new OffsetAndEpoch(2, epoch)
    createNewSnapshotUnckecked(log, oldSnapshotId2)

    append(log, numberOfRecords - 2, epoch)
    val oldSnapshotId3 = new OffsetAndEpoch(numberOfRecords, epoch)
    createNewSnapshotUnckecked(log, oldSnapshotId3)

    val greaterSnapshotId = new OffsetAndEpoch(3 * numberOfRecords, epoch)
    createNewSnapshotUnckecked(log, greaterSnapshotId)

    assertNotEquals(log.earliestSnapshotId(), log.latestSnapshotId())
    assertTrue(log.truncateToLatestSnapshot())
    assertEquals(log.earliestSnapshotId(), log.latestSnapshotId())
    log.close()

    mockTime.sleep(config.fileDeleteDelayMs)
    // Assert that the log dir doesn't contain any older snapshots
    Files
      .walk(logDir, 1)
      .map[Optional[SnapshotPath]](Snapshots.parse)
      .filter(_.isPresent)
      .forEach { path =>
        assertFalse(path.get.snapshotId.offset < log.startOffset)
      }
  }

  @Test
  def testStartupWithInvalidSnapshotState(): Unit = {
    // Initialize an empty log at offset 100.
    var log = buildMetadataLog(tempDir, mockTime)
    log.log.truncateFullyAndStartAt(newOffset = 100)
    log.close()

    val metadataDir = metadataLogDir(tempDir)
    assertTrue(metadataDir.exists())

    // Initialization should fail unless we have a snapshot  at an offset
    // greater than or equal to 100.
    assertThrows(classOf[IllegalStateException], () => {
      buildMetadataLog(tempDir, mockTime)
    })
    // Snapshots at offsets less than 100 are not sufficient.
    writeEmptySnapshot(metadataDir, new OffsetAndEpoch(50, 1))
    assertThrows(classOf[IllegalStateException], () => {
      buildMetadataLog(tempDir, mockTime)
    })

    // Snapshot at offset 100 should be fine.
    writeEmptySnapshot(metadataDir, new OffsetAndEpoch(100, 1))
    log = buildMetadataLog(tempDir, mockTime)
    log.log.truncateFullyAndStartAt(newOffset = 200)
    log.close()

    // Snapshots at higher offsets are also fine. In this case, the
    // log start offset should advance to the first snapshot offset.
    writeEmptySnapshot(metadataDir, new OffsetAndEpoch(500, 1))
    log = buildMetadataLog(tempDir, mockTime)
    assertEquals(500, log.log.logStartOffset)
  }

  @Test
  def testSnapshotDeletionWithInvalidSnapshotState(): Unit = {
    // Initialize an empty log at offset 100.
    val log = buildMetadataLog(tempDir, mockTime)
    log.log.truncateFullyAndStartAt(newOffset = 100)
    log.close()

    val metadataDir = metadataLogDir(tempDir)
    assertTrue(metadataDir.exists())

    // We have one deleted snapshot at an offset matching the start offset.
    val snapshotId = new OffsetAndEpoch(100, 1)
    writeEmptySnapshot(metadataDir, snapshotId)

    val deletedPath = Snapshots.markForDelete(metadataDir.toPath, snapshotId)
    assertTrue(deletedPath.toFile.exists())

    // Initialization should still fail.
    assertThrows(classOf[IllegalStateException], () => {
      buildMetadataLog(tempDir, mockTime)
    })

    // The snapshot marked for deletion should still exist.
    assertTrue(deletedPath.toFile.exists())
  }

  private def metadataLogDir(
    logDir: File
  ): File = {
    new File(
      logDir.getAbsolutePath,
      UnifiedLog.logDirName(KafkaRaftServer.MetadataPartition)
    )
  }

  private def writeEmptySnapshot(
    metadataDir: File,
    snapshotId: OffsetAndEpoch
  ): Unit = {
    Using.resource(FileRawSnapshotWriter.create(metadataDir.toPath, snapshotId))(_.freeze())
  }

  @Test
  def testDoesntTruncateFully(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)
    val numberOfRecords = 10
    val epoch = 1

    append(log, numberOfRecords, epoch)

    val olderEpochSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch - 1)
    createNewSnapshotUnckecked(log, olderEpochSnapshotId)
    assertFalse(log.truncateToLatestSnapshot())

    append(log, numberOfRecords, epoch)


    val olderOffsetSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch)
    createNewSnapshotUnckecked(log, olderOffsetSnapshotId)

    assertFalse(log.truncateToLatestSnapshot())
  }

  @Test
  def testCleanupPartialSnapshots(): Unit = {
    val (logDir, log, _) = buildMetadataLogAndDir(tempDir, mockTime)
    val numberOfRecords = 10
    val epoch = 1
    val snapshotId = new OffsetAndEpoch(1, epoch)

    append(log, numberOfRecords, epoch)
    createNewSnapshotUnckecked(log, snapshotId)
    log.close()

    // Create a few partial snapshots
    Snapshots.createTempFile(logDir, new OffsetAndEpoch(0, epoch - 1))
    Snapshots.createTempFile(logDir, new OffsetAndEpoch(1, epoch))
    Snapshots.createTempFile(logDir, new OffsetAndEpoch(2, epoch + 1))

    val secondLog = buildMetadataLog(tempDir, mockTime)

    assertEquals(snapshotId, secondLog.latestSnapshotId().get)
    assertEquals(0, log.startOffset)
    assertEquals(epoch, log.lastFetchedEpoch)
    assertEquals(numberOfRecords, log.endOffset().offset)
    assertEquals(0, secondLog.highWatermark.offset)

    // Assert that the log dir doesn't contain any partial snapshots
    Files
      .walk(logDir, 1)
      .map[Optional[SnapshotPath]](Snapshots.parse)
      .filter(_.isPresent)
      .forEach { path =>
        assertFalse(path.get.partial)
      }
  }

  @Test
  def testCleanupOlderSnapshots(): Unit = {
    val (logDir, log, config) = buildMetadataLogAndDir(tempDir, mockTime)
    val numberOfRecords = 10
    val epoch = 1

    append(log, 1, epoch - 1)
    val oldSnapshotId1 = new OffsetAndEpoch(1, epoch - 1)
    createNewSnapshotUnckecked(log, oldSnapshotId1)

    append(log, 1, epoch)
    val oldSnapshotId2 = new OffsetAndEpoch(2, epoch)
    createNewSnapshotUnckecked(log, oldSnapshotId2)

    append(log, numberOfRecords - 2, epoch)
    val oldSnapshotId3 = new OffsetAndEpoch(numberOfRecords, epoch)
    createNewSnapshotUnckecked(log, oldSnapshotId3)

    val greaterSnapshotId = new OffsetAndEpoch(3 * numberOfRecords, epoch)
    append(log, numberOfRecords, epoch)
    createNewSnapshotUnckecked(log, greaterSnapshotId)

    log.close()

    val secondLog = buildMetadataLog(tempDir, mockTime)

    assertEquals(greaterSnapshotId, secondLog.latestSnapshotId().get)
    assertEquals(3 * numberOfRecords, secondLog.startOffset)
    assertEquals(epoch, secondLog.lastFetchedEpoch)
    mockTime.sleep(config.fileDeleteDelayMs)

    // Assert that the log dir doesn't contain any older snapshots
    Files
      .walk(logDir, 1)
      .map[Optional[SnapshotPath]](Snapshots.parse)
      .filter(_.isPresent)
      .forEach { path =>
        assertFalse(path.get.snapshotId.offset < log.startOffset)
      }
  }

  @Test
  def testCreateReplicatedLogTruncatesFully(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)
    val numberOfRecords = 10
    val epoch = 1
    val snapshotId = new OffsetAndEpoch(numberOfRecords + 1, epoch + 1)

    append(log, numberOfRecords, epoch)
    createNewSnapshotUnckecked(log, snapshotId)

    log.close()

    val secondLog = buildMetadataLog(tempDir, mockTime)

    assertEquals(snapshotId, secondLog.latestSnapshotId().get)
    assertEquals(snapshotId.offset, secondLog.startOffset)
    assertEquals(snapshotId.epoch, secondLog.lastFetchedEpoch)
    assertEquals(snapshotId.offset, secondLog.endOffset().offset)
    assertEquals(snapshotId.offset, secondLog.highWatermark.offset)
  }

  @Test
  def testMaxBatchSize(): Unit = {
    val leaderEpoch = 5
    val maxBatchSizeInBytes = 16384
    val recordSize = 64
    val log = buildMetadataLog(tempDir, mockTime, DefaultMetadataLogConfig.copy(maxBatchSizeInBytes = maxBatchSizeInBytes))

    val oversizeBatch = buildFullBatch(leaderEpoch, recordSize, maxBatchSizeInBytes + recordSize)
    assertThrows(classOf[RecordTooLargeException], () => {
      log.appendAsLeader(oversizeBatch, leaderEpoch)
    })

    val undersizeBatch = buildFullBatch(leaderEpoch, recordSize, maxBatchSizeInBytes)
    val appendInfo = log.appendAsLeader(undersizeBatch, leaderEpoch)
    assertEquals(0L, appendInfo.firstOffset)
  }

  @Test
  def testTruncateBelowHighWatermark(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)
    val numRecords = 10
    val epoch = 5

    append(log, numRecords, epoch)
    assertEquals(numRecords.toLong, log.endOffset.offset)

    log.updateHighWatermark(new LogOffsetMetadata(numRecords))
    assertEquals(numRecords.toLong, log.highWatermark.offset)

    assertThrows(classOf[IllegalArgumentException], () => log.truncateTo(5L))
    assertEquals(numRecords.toLong, log.highWatermark.offset)
  }

  private def buildFullBatch(
    leaderEpoch: Int,
    recordSize: Int,
    maxBatchSizeInBytes: Int
  ): MemoryRecords = {
    val buffer = ByteBuffer.allocate(maxBatchSizeInBytes)
    val batchBuilder = new BatchBuilder[Array[Byte]](
      buffer,
      new ByteArraySerde,
      Compression.NONE,
      0L,
      mockTime.milliseconds(),
      leaderEpoch,
      maxBatchSizeInBytes
    )

    val serializationCache = new ObjectSerializationCache
    val records = Collections.singletonList(new Array[Byte](recordSize))
    while (!batchBuilder.bytesNeeded(records, serializationCache).isPresent) {
      batchBuilder.appendRecord(records.get(0), serializationCache)
    }

    batchBuilder.build()
  }

  @Test
  def testValidateEpochGreaterThanLastKnownEpoch(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    val numberOfRecords = 1
    val epoch = 1

    append(log, numberOfRecords, epoch)

    val resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords, epoch + 1)
    assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind)
    assertEquals(new OffsetAndEpoch(log.endOffset.offset, epoch), resultOffsetAndEpoch.offsetAndEpoch())
  }

  @Test
  def testValidateEpochLessThanOldestSnapshotEpoch(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    val numberOfRecords = 10
    val epoch = 1

    append(log, numberOfRecords, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords))

    val snapshotId = new OffsetAndEpoch(numberOfRecords, epoch)
    createNewSnapshot(log, snapshotId)

    val resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords, epoch - 1)
    assertEquals(ValidOffsetAndEpoch.Kind.SNAPSHOT, resultOffsetAndEpoch.kind)
    assertEquals(snapshotId, resultOffsetAndEpoch.offsetAndEpoch())
  }

  @Test
  def testValidateOffsetLessThanOldestSnapshotOffset(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    val offset = 2
    val epoch = 1

    append(log, offset, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(offset))

    val snapshotId = new OffsetAndEpoch(offset, epoch)
    createNewSnapshot(log, snapshotId)

    // Simulate log cleaning advancing the LSO
    log.log.maybeIncrementLogStartOffset(offset, LogStartOffsetIncrementReason.SegmentDeletion)

    val resultOffsetAndEpoch = log.validateOffsetAndEpoch(offset - 1, epoch)
    assertEquals(ValidOffsetAndEpoch.Kind.SNAPSHOT, resultOffsetAndEpoch.kind)
    assertEquals(snapshotId, resultOffsetAndEpoch.offsetAndEpoch())
  }

  @Test
  def testValidateOffsetEqualToOldestSnapshotOffset(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    val offset = 2
    val epoch = 1

    append(log, offset, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(offset))

    val snapshotId = new OffsetAndEpoch(offset, epoch)
    createNewSnapshot(log, snapshotId)

    val resultOffsetAndEpoch = log.validateOffsetAndEpoch(offset, epoch)
    assertEquals(ValidOffsetAndEpoch.Kind.VALID, resultOffsetAndEpoch.kind)
    assertEquals(snapshotId, resultOffsetAndEpoch.offsetAndEpoch())
  }

  @Test
  def testValidateUnknownEpochLessThanLastKnownGreaterThanOldestSnapshot(): Unit = {
    val offset = 10
    val numOfRecords = 5

    val log = buildMetadataLog(tempDir, mockTime)
    log.updateHighWatermark(new LogOffsetMetadata(offset))
    val snapshotId = new OffsetAndEpoch(offset, 1)
    createNewSnapshotUnckecked(log, snapshotId)
    log.truncateToLatestSnapshot()


    append(log, numOfRecords, epoch = 1)
    append(log, numOfRecords, epoch = 2)
    append(log, numOfRecords, epoch = 4)

    // offset is not equal to oldest snapshot's offset
    val resultOffsetAndEpoch = log.validateOffsetAndEpoch(100, 3)
    assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind)
    assertEquals(new OffsetAndEpoch(20, 2), resultOffsetAndEpoch.offsetAndEpoch())
  }

  @Test
  def testValidateEpochLessThanFirstEpochInLog(): Unit = {
    val offset = 10
    val numOfRecords = 5

    val log = buildMetadataLog(tempDir, mockTime)
    log.updateHighWatermark(new LogOffsetMetadata(offset))
    val snapshotId = new OffsetAndEpoch(offset, 1)
    createNewSnapshotUnckecked(log, snapshotId)
    log.truncateToLatestSnapshot()

    append(log, numOfRecords, epoch = 3)

    // offset is not equal to oldest snapshot's offset
    val resultOffsetAndEpoch = log.validateOffsetAndEpoch(100, 2)
    assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind)
    assertEquals(snapshotId, resultOffsetAndEpoch.offsetAndEpoch())
  }

  @Test
  def testValidateOffsetGreatThanEndOffset(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    val numberOfRecords = 1
    val epoch = 1

    append(log, numberOfRecords, epoch)

    val resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords + 1, epoch)
    assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind)
    assertEquals(new OffsetAndEpoch(log.endOffset.offset, epoch), resultOffsetAndEpoch.offsetAndEpoch())
  }

  @Test
  def testValidateOffsetLessThanLEO(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    val numberOfRecords = 10
    val epoch = 1

    append(log, numberOfRecords, epoch)
    append(log, numberOfRecords, epoch + 1)

    val resultOffsetAndEpoch = log.validateOffsetAndEpoch(11, epoch)
    assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind)
    assertEquals(new OffsetAndEpoch(10, epoch), resultOffsetAndEpoch.offsetAndEpoch())
  }

  @Test
  def testValidateValidEpochAndOffset(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    val numberOfRecords = 5
    val epoch = 1

    append(log, numberOfRecords, epoch)

    val resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords - 1, epoch)
    assertEquals(ValidOffsetAndEpoch.Kind.VALID, resultOffsetAndEpoch.kind)
    assertEquals(new OffsetAndEpoch(numberOfRecords - 1, epoch), resultOffsetAndEpoch.offsetAndEpoch())
  }

  @Test
  def testAdvanceLogStartOffsetAfterCleaning(): Unit = {
    val config = MetadataLogConfig(
      logSegmentBytes = 512,
      logSegmentMinBytes = 512,
      logSegmentMillis = 10 * 1000,
      retentionMaxBytes = 256,
      retentionMillis = 60 * 1000,
      maxBatchSizeInBytes = 512,
      maxFetchSizeInBytes = DefaultMetadataLogConfig.maxFetchSizeInBytes,
      fileDeleteDelayMs = ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT,
      nodeId = 1
    )
    config.copy()
    val log = buildMetadataLog(tempDir, mockTime, config)

    // Generate some segments
    for (_ <- 0 to 100) {
      append(log, 47, 1) // An odd number of records to avoid offset alignment
    }
    assertFalse(log.maybeClean(), "Should not clean since HW was still 0")

    log.updateHighWatermark(new LogOffsetMetadata(4000))
    assertFalse(log.maybeClean(), "Should not clean since no snapshots exist")

    val snapshotId1 = new OffsetAndEpoch(1000, 1)
    createNewSnapshotUnckecked(log, snapshotId1)

    val snapshotId2 = new OffsetAndEpoch(2000, 1)
    createNewSnapshotUnckecked(log, snapshotId2)

    val lsoBefore = log.startOffset()
    assertTrue(log.maybeClean(), "Expected to clean since there was at least one snapshot")
    val lsoAfter = log.startOffset()
    assertTrue(lsoAfter > lsoBefore, "Log Start Offset should have increased after cleaning")
    assertTrue(lsoAfter == snapshotId2.offset, "Expected the Log Start Offset to be less than or equal to the snapshot offset")
  }

  @Test
  def testDeleteSnapshots(): Unit = {
    // Generate some logs and a few snapshots, set retention low and verify that cleaning occurs
    val config = DefaultMetadataLogConfig.copy(
      logSegmentBytes = 1024,
      logSegmentMinBytes = 1024,
      logSegmentMillis = 10 * 1000,
      retentionMaxBytes = 1024,
      retentionMillis = 60 * 1000,
      maxBatchSizeInBytes = 100
    )
    val log = buildMetadataLog(tempDir, mockTime, config)

    for (_ <- 0 to 1000) {
      append(log, 1, 1)
    }
    log.updateHighWatermark(new LogOffsetMetadata(1001))

    for (offset <- Seq(100, 200, 300, 400, 500, 600)) {
      val snapshotId = new OffsetAndEpoch(offset, 1)
      createNewSnapshotUnckecked(log, snapshotId)
    }

    assertEquals(6, log.snapshotCount())
    assertTrue(log.maybeClean())
    assertEquals(1, log.snapshotCount(), "Expected only one snapshot after cleaning")
    assertOptional(log.latestSnapshotId(), (snapshotId: OffsetAndEpoch) => {
      assertEquals(600, snapshotId.offset)
    })
    assertEquals(log.startOffset, 600)
  }

  @Test
  def testSoftRetentionLimit(): Unit = {
    // Set retention equal to the segment size and generate slightly more than one segment of logs
    val config = DefaultMetadataLogConfig.copy(
      logSegmentBytes = 10240,
      logSegmentMinBytes = 10240,
      logSegmentMillis = 10 * 1000,
      retentionMaxBytes = 10240,
      retentionMillis = 60 * 1000,
      maxBatchSizeInBytes = 100
    )
    val log = buildMetadataLog(tempDir, mockTime, config)

    for (_ <- 0 to 2000) {
      append(log, 1, 1)
    }
    log.updateHighWatermark(new LogOffsetMetadata(2000))

    // Then generate two snapshots
    val snapshotId1 = new OffsetAndEpoch(1000, 1)
    Using.resource(log.createNewSnapshotUnchecked(snapshotId1).get()) { snapshot =>
      append(snapshot, 500)
      snapshot.freeze()
    }

    // Then generate a snapshot
    val snapshotId2 = new OffsetAndEpoch(2000, 1)
    Using.resource(log.createNewSnapshotUnchecked(snapshotId2).get()) { snapshot =>
      append(snapshot, 500)
      snapshot.freeze()
    }

    // Cleaning should occur, but resulting size will not be under retention limit since we have to keep one snapshot
    assertTrue(log.maybeClean())
    assertEquals(1, log.snapshotCount(), "Expected one snapshot after cleaning")
    assertOptional(log.latestSnapshotId(), (snapshotId: OffsetAndEpoch) => {
      assertEquals(2000, snapshotId.offset, "Unexpected offset for latest snapshot")
      assertOptional(log.readSnapshot(snapshotId), (reader: RawSnapshotReader) => {
        assertTrue(reader.sizeInBytes() + log.log.size > config.retentionMaxBytes)
      })
    })
  }

  @Test
  def testSegmentsLessThanLatestSnapshot(): Unit = {
    val config = DefaultMetadataLogConfig.copy(
      logSegmentBytes = 10240,
      logSegmentMinBytes = 10240,
      logSegmentMillis = 10 * 1000,
      retentionMaxBytes = 10240,
      retentionMillis = 60 * 1000,
      maxBatchSizeInBytes = 200
    )
    val log = buildMetadataLog(tempDir, mockTime, config)

    // Generate enough data to cause a segment roll
    for (_ <- 0 to 2000) {
      append(log, 10, 1)
    }
    log.updateHighWatermark(new LogOffsetMetadata(log.endOffset.offset))

    // The clean up code requires that there are at least two snapshots
    // Generate first snapshots that includes the first segment by using the base offset of the second segment
    val snapshotId1 = new OffsetAndEpoch(
      log.log.logSegments.asScala.drop(1).head.baseOffset,
      1
    )
    createNewSnapshotUnckecked(log, snapshotId1)

    // Generate second snapshots that includes the second segment by using the base offset of the third segment
    val snapshotId2 = new OffsetAndEpoch(
      log.log.logSegments.asScala.drop(2).head.baseOffset,
      1
    )
    createNewSnapshotUnckecked(log, snapshotId2)

    // Sleep long enough to trigger a possible segment delete because of the default retention
    val defaultLogRetentionMs = LogConfig.DEFAULT_RETENTION_MS * 2
    mockTime.sleep(defaultLogRetentionMs)

    assertTrue(log.maybeClean())
    assertEquals(1, log.snapshotCount())
    assertTrue(log.startOffset > 0, s"${log.startOffset} must be greater than 0")
    val latestSnapshotOffset = log.latestSnapshotId().get.offset
    assertTrue(
      latestSnapshotOffset >= log.startOffset,
      s"latest snapshot offset ($latestSnapshotOffset) must be >= log start offset (${log.startOffset})"
    )
  }
}

object KafkaMetadataLogTest {
  class ByteArraySerde extends RecordSerde[Array[Byte]] {
    override def recordSize(data: Array[Byte], serializationCache: ObjectSerializationCache): Int = {
      data.length
    }
    override def write(data: Array[Byte], serializationCache: ObjectSerializationCache, out: Writable): Unit = {
      out.writeByteArray(data)
    }
    override def read(input: protocol.Readable, size: Int): Array[Byte] = input.readArray(size)
  }

  val DefaultMetadataLogConfig = MetadataLogConfig(
    logSegmentBytes = 100 * 1024,
    logSegmentMinBytes = 100 * 1024,
    logSegmentMillis = 10 * 1000,
    retentionMaxBytes = 100 * 1024,
    retentionMillis = 60 * 1000,
    maxBatchSizeInBytes = KafkaRaftClient.MAX_BATCH_SIZE_BYTES,
    maxFetchSizeInBytes = KafkaRaftClient.MAX_FETCH_SIZE_BYTES,
    fileDeleteDelayMs = ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT,
    nodeId = 1
  )

  def buildMetadataLogAndDir(
    tempDir: File,
    time: MockTime,
    metadataLogConfig: MetadataLogConfig = DefaultMetadataLogConfig
  ): (Path, KafkaMetadataLog, MetadataLogConfig) = {

    val logDir = createLogDirectory(
      tempDir,
      UnifiedLog.logDirName(KafkaRaftServer.MetadataPartition)
    )

    val metadataLog = KafkaMetadataLog(
      KafkaRaftServer.MetadataPartition,
      KafkaRaftServer.MetadataTopicId,
      logDir,
      time,
      time.scheduler,
      metadataLogConfig
    )

    (logDir.toPath, metadataLog, metadataLogConfig)
  }

  def buildMetadataLog(
    tempDir: File,
    time: MockTime,
    metadataLogConfig: MetadataLogConfig = DefaultMetadataLogConfig,
  ): KafkaMetadataLog = {
    val (_, log, _) = buildMetadataLogAndDir(tempDir, time, metadataLogConfig)
    log
  }

  def createNewSnapshot(log: KafkaMetadataLog, snapshotId: OffsetAndEpoch): Unit = {
    Using.resource(log.createNewSnapshot(snapshotId).get()) { snapshot =>
      snapshot.freeze()
    }
  }

  def createNewSnapshotUnckecked(log: KafkaMetadataLog, snapshotId: OffsetAndEpoch): Unit = {
    Using.resource(log.createNewSnapshotUnchecked(snapshotId).get()) { snapshot =>
      snapshot.freeze()
    }
  }

  def append(log: ReplicatedLog, numberOfRecords: Int, epoch: Int): LogAppendInfo = {
    log.appendAsLeader(
      MemoryRecords.withRecords(
        log.endOffset().offset,
        Compression.NONE,
        epoch,
        (0 until numberOfRecords).map(number => new SimpleRecord(number.toString.getBytes)): _*
      ),
      epoch
    )
  }

  def append(snapshotWriter: RawSnapshotWriter, numberOfRecords: Int): Unit = {
    snapshotWriter.append(MemoryRecords.withRecords(
      0,
      Compression.NONE,
      0,
      (0 until numberOfRecords).map(number => new SimpleRecord(number.toString.getBytes)): _*
    ))
  }

  private def createLogDirectory(logDir: File, logDirName: String): File = {
    val logDirPath = logDir.getAbsolutePath
    val dir = new File(logDirPath, logDirName)
    if (!Files.exists(dir.toPath)) {
      Files.createDirectories(dir.toPath)
    }
    dir
  }
}