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

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.{Files, Path}
import java.util.{Collections, Optional}

import kafka.log.Log
import kafka.server.KafkaRaftServer
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.errors.{OffsetOutOfRangeException, RecordTooLargeException}
import org.apache.kafka.common.protocol
import org.apache.kafka.common.protocol.{ObjectSerializationCache, Writable}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.raft.internals.BatchBuilder
import org.apache.kafka.raft.{KafkaRaftClient, LogAppendInfo, LogOffsetMetadata, OffsetAndEpoch, RecordSerde, ReplicatedLog}
import org.apache.kafka.snapshot.{SnapshotPath, Snapshots}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

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
  def testUnexpectedAppendOffset(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    val recordFoo = new SimpleRecord("foo".getBytes())
    val currentEpoch = 3
    val initialOffset = log.endOffset().offset

    log.appendAsLeader(
      MemoryRecords.withRecords(initialOffset, CompressionType.NONE, currentEpoch, recordFoo),
      currentEpoch
    )

    // Throw exception for out of order records
    assertThrows(
      classOf[RuntimeException],
      () => {
        log.appendAsLeader(
          MemoryRecords.withRecords(initialOffset, CompressionType.NONE, currentEpoch, recordFoo),
          currentEpoch
        )
      }
    )

    assertThrows(
      classOf[RuntimeException],
      () => {
        log.appendAsFollower(
          MemoryRecords.withRecords(initialOffset, CompressionType.NONE, currentEpoch, recordFoo)
        )
      }
    )
  }

  @Test
  def testCreateSnapshot(): Unit = {
    val numberOfRecords = 10
    val epoch = 0
    val snapshotId = new OffsetAndEpoch(numberOfRecords, epoch)
    val log = buildMetadataLog(tempDir, mockTime)

    append(log, numberOfRecords, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords))

    TestUtils.resource(log.createSnapshot(snapshotId)) { snapshot =>
      snapshot.freeze()
    }

    TestUtils.resource(log.readSnapshot(snapshotId).get()) { snapshot =>
      assertEquals(0, snapshot.sizeInBytes())
    }
  }

  @Test
  def testReadMissingSnapshot(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)

    assertEquals(Optional.empty(), log.readSnapshot(new OffsetAndEpoch(10, 0)))
  }

  @Test
  def testUpdateLogStartOffset(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)
    val offset = 10
    val epoch = 0
    val snapshotId = new OffsetAndEpoch(offset, epoch)

    append(log, offset, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(offset))

    TestUtils.resource(log.createSnapshot(snapshotId)) { snapshot =>
      snapshot.freeze()
    }

    assertTrue(log.deleteBeforeSnapshot(snapshotId))
    assertEquals(offset, log.startOffset)
    assertEquals(epoch, log.lastFetchedEpoch)
    assertEquals(offset, log.endOffset().offset)
    assertEquals(offset, log.highWatermark.offset)

    val newRecords = 10
    append(log, newRecords, epoch + 1, log.endOffset.offset)
    // Start offset should not change since a new snapshot was not generated
    assertFalse(log.deleteBeforeSnapshot(new OffsetAndEpoch(offset + newRecords, epoch)))
    assertEquals(offset, log.startOffset)

    assertEquals(epoch + 1, log.lastFetchedEpoch)
    assertEquals(offset + newRecords, log.endOffset().offset)
    assertEquals(offset, log.highWatermark.offset)
  }

  @Test
  def testUpdateLogStartOffsetWillRemoveOlderSnapshot(): Unit = {
    val (logDir, log) = buildMetadataLogAndDir(tempDir, mockTime)
    val offset = 10
    val epoch = 0

    append(log, offset, epoch)
    val oldSnapshotId = new OffsetAndEpoch(offset, epoch)
    TestUtils.resource(log.createSnapshot(oldSnapshotId)) { snapshot =>
      snapshot.freeze()
    }

    append(log, offset, epoch, log.endOffset.offset)
    val newSnapshotId = new OffsetAndEpoch(offset * 2, epoch)
    TestUtils.resource(log.createSnapshot(newSnapshotId)) { snapshot =>
      snapshot.freeze()
    }

    log.updateHighWatermark(new LogOffsetMetadata(offset * 2))
    assertTrue(log.deleteBeforeSnapshot(newSnapshotId))
    log.close()

    mockTime.sleep(log.fileDeleteDelayMs)
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
  def testUpdateLogStartOffsetWithMissingSnapshot(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)
    val offset = 10
    val epoch = 0

    append(log, offset, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(offset))

    assertFalse(log.deleteBeforeSnapshot(new OffsetAndEpoch(1L, epoch)))
    assertEquals(0, log.startOffset)
    assertEquals(epoch, log.lastFetchedEpoch)
    assertEquals(offset, log.endOffset().offset)
    assertEquals(offset, log.highWatermark.offset)
  }

  @Test
  def testFailToIncreaseLogStartPastHighWatermark(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)
    val offset = 10
    val epoch = 0
    val snapshotId = new OffsetAndEpoch(2 * offset, 1 + epoch)

    append(log, offset, epoch)
    log.updateHighWatermark(new LogOffsetMetadata(offset))

    TestUtils.resource(log.createSnapshot(snapshotId)) { snapshot =>
      snapshot.freeze()
    }

    assertThrows(
      classOf[OffsetOutOfRangeException],
      () => log.deleteBeforeSnapshot(snapshotId)
    )
  }

  @Test
  def testTruncateFullyToLatestSnapshot(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)
    val numberOfRecords = 10
    val epoch = 0
    val sameEpochSnapshotId = new OffsetAndEpoch(2 * numberOfRecords, epoch)

    append(log, numberOfRecords, epoch)

    TestUtils.resource(log.createSnapshot(sameEpochSnapshotId)) { snapshot =>
      snapshot.freeze()
    }

    assertTrue(log.truncateToLatestSnapshot())
    assertEquals(sameEpochSnapshotId.offset, log.startOffset)
    assertEquals(sameEpochSnapshotId.epoch, log.lastFetchedEpoch)
    assertEquals(sameEpochSnapshotId.offset, log.endOffset().offset)
    assertEquals(sameEpochSnapshotId.offset, log.highWatermark.offset)

    val greaterEpochSnapshotId = new OffsetAndEpoch(3 * numberOfRecords, epoch + 1)

    append(log, numberOfRecords, epoch, log.endOffset.offset)

    TestUtils.resource(log.createSnapshot(greaterEpochSnapshotId)) { snapshot =>
      snapshot.freeze()
    }

    assertTrue(log.truncateToLatestSnapshot())
    assertEquals(greaterEpochSnapshotId.offset, log.startOffset)
    assertEquals(greaterEpochSnapshotId.epoch, log.lastFetchedEpoch)
    assertEquals(greaterEpochSnapshotId.offset, log.endOffset().offset)
    assertEquals(greaterEpochSnapshotId.offset, log.highWatermark.offset)
  }

  @Test
  def testTruncateWillRemoveOlderSnapshot(): Unit = {

    val (logDir, log) = buildMetadataLogAndDir(tempDir, mockTime)
    val numberOfRecords = 10
    val epoch = 1

    append(log, 1, epoch - 1)
    val oldSnapshotId1 = new OffsetAndEpoch(1, epoch - 1)
    TestUtils.resource(log.createSnapshot(oldSnapshotId1)) { snapshot =>
      snapshot.freeze()
    }

    append(log, 1, epoch, log.endOffset.offset)
    val oldSnapshotId2 = new OffsetAndEpoch(2, epoch)
    TestUtils.resource(log.createSnapshot(oldSnapshotId2)) { snapshot =>
      snapshot.freeze()
    }

    append(log, numberOfRecords - 2, epoch, log.endOffset.offset)
    val oldSnapshotId3 = new OffsetAndEpoch(numberOfRecords, epoch)
    TestUtils.resource(log.createSnapshot(oldSnapshotId3)) { snapshot =>
      snapshot.freeze()
    }

    val greaterSnapshotId = new OffsetAndEpoch(3 * numberOfRecords, epoch)
    append(log, numberOfRecords, epoch, log.endOffset.offset)
    TestUtils.resource(log.createSnapshot(greaterSnapshotId)) { snapshot =>
      snapshot.freeze()
    }

    assertNotEquals(log.earliestSnapshotId(), log.latestSnapshotId())
    assertTrue(log.truncateToLatestSnapshot())
    assertEquals(log.earliestSnapshotId(), log.latestSnapshotId())
    log.close()

    mockTime.sleep(log.fileDeleteDelayMs)
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
  def testDoesntTruncateFully(): Unit = {
    val log = buildMetadataLog(tempDir, mockTime)
    val numberOfRecords = 10
    val epoch = 1

    append(log, numberOfRecords, epoch)

    val olderEpochSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch - 1)
    TestUtils.resource(log.createSnapshot(olderEpochSnapshotId)) { snapshot =>
      snapshot.freeze()
    }

    assertFalse(log.truncateToLatestSnapshot())

    append(log, numberOfRecords, epoch, log.endOffset.offset)

    val olderOffsetSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch)
    TestUtils.resource(log.createSnapshot(olderOffsetSnapshotId)) { snapshot =>
      snapshot.freeze()
    }

    assertFalse(log.truncateToLatestSnapshot())
  }

  @Test
  def testCleanupPartialSnapshots(): Unit = {
    val (logDir, log) = buildMetadataLogAndDir(tempDir, mockTime)
    val numberOfRecords = 10
    val epoch = 1
    val snapshotId = new OffsetAndEpoch(1, epoch)

    append(log, numberOfRecords, epoch)
    TestUtils.resource(log.createSnapshot(snapshotId)) { snapshot =>
      snapshot.freeze()
    }

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
    val (logDir, log) = buildMetadataLogAndDir(tempDir, mockTime)
    val numberOfRecords = 10
    val epoch = 1

    append(log, 1, epoch - 1)
    val oldSnapshotId1 = new OffsetAndEpoch(1, epoch - 1)
    TestUtils.resource(log.createSnapshot(oldSnapshotId1)) { snapshot =>
      snapshot.freeze()
    }

    append(log, 1, epoch, log.endOffset.offset)
    val oldSnapshotId2 = new OffsetAndEpoch(2, epoch)
    TestUtils.resource(log.createSnapshot(oldSnapshotId2)) { snapshot =>
      snapshot.freeze()
    }

    append(log, numberOfRecords - 2, epoch, log.endOffset.offset)
    val oldSnapshotId3 = new OffsetAndEpoch(numberOfRecords, epoch)
    TestUtils.resource(log.createSnapshot(oldSnapshotId3)) { snapshot =>
      snapshot.freeze()
    }

    val greaterSnapshotId = new OffsetAndEpoch(3 * numberOfRecords, epoch)
    append(log, numberOfRecords, epoch, log.endOffset.offset)
    TestUtils.resource(log.createSnapshot(greaterSnapshotId)) { snapshot =>
      snapshot.freeze()
    }

    log.close()

    val secondLog = buildMetadataLog(tempDir, mockTime)

    assertEquals(greaterSnapshotId, secondLog.latestSnapshotId().get)
    assertEquals(3 * numberOfRecords, secondLog.startOffset)
    assertEquals(epoch, secondLog.lastFetchedEpoch)
    mockTime.sleep(log.fileDeleteDelayMs)

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
    TestUtils.resource(log.createSnapshot(snapshotId)) { snapshot =>
      snapshot.freeze()
    }

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
    val log = buildMetadataLog(tempDir, mockTime, maxBatchSizeInBytes)

    val oversizeBatch = buildFullBatch(leaderEpoch, recordSize, maxBatchSizeInBytes + recordSize)
    assertThrows(classOf[RecordTooLargeException], () => {
      log.appendAsLeader(oversizeBatch, leaderEpoch)
    })

    val undersizeBatch = buildFullBatch(leaderEpoch, recordSize, maxBatchSizeInBytes)
    val appendInfo = log.appendAsLeader(undersizeBatch, leaderEpoch)
    assertEquals(0L, appendInfo.firstOffset)
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
      CompressionType.NONE,
      0L,
      mockTime.milliseconds(),
      false,
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

}

object KafkaMetadataLogTest {
  class ByteArraySerde extends RecordSerde[Array[Byte]] {
    override def recordSize(data: Array[Byte], serializationCache: ObjectSerializationCache): Int = {
      data.length
    }
    override def write(data: Array[Byte], serializationCache: ObjectSerializationCache, out: Writable): Unit = {
      out.writeByteArray(data)
    }
    override def read(input: protocol.Readable, size: Int): Array[Byte] = {
      val array = new Array[Byte](size)
      input.readArray(array)
      array
    }
  }

  def buildMetadataLogAndDir(
    tempDir: File,
    time: MockTime,
    maxBatchSizeInBytes: Int = KafkaRaftClient.MAX_BATCH_SIZE_BYTES,
    maxFetchSizeInBytes: Int = KafkaRaftClient.MAX_FETCH_SIZE_BYTES
  ): (Path, KafkaMetadataLog) = {

    val logDir = createLogDirectory(
      tempDir,
      Log.logDirName(KafkaRaftServer.MetadataPartition)
    )

    val metadataLog = KafkaMetadataLog(
      KafkaRaftServer.MetadataPartition,
      logDir,
      time,
      time.scheduler,
      maxBatchSizeInBytes,
      maxFetchSizeInBytes
    )

    (logDir.toPath, metadataLog)
  }

  def buildMetadataLog(
    tempDir: File,
    time: MockTime,
    maxBatchSizeInBytes: Int = KafkaRaftClient.MAX_BATCH_SIZE_BYTES,
    maxFetchSizeInBytes: Int = KafkaRaftClient.MAX_FETCH_SIZE_BYTES
  ): KafkaMetadataLog = {
    val (_, log) = buildMetadataLogAndDir(tempDir, time, maxBatchSizeInBytes, maxFetchSizeInBytes)
    log
  }

  def append(log: ReplicatedLog, numberOfRecords: Int, epoch: Int, initialOffset: Long = 0L): LogAppendInfo = {
    log.appendAsLeader(
      MemoryRecords.withRecords(
        initialOffset,
        CompressionType.NONE,
        epoch,
        (0 until numberOfRecords).map(number => new SimpleRecord(number.toString.getBytes)): _*
      ),
      epoch
    )
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
