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
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional
import kafka.log.Log
import kafka.log.LogManager
import kafka.log.LogTest
import kafka.server.BrokerTopicStats
import kafka.server.LogDirFailureChannel
import kafka.utils.MockTime
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.raft.LogAppendInfo
import org.apache.kafka.raft.LogOffsetMetadata
import org.apache.kafka.raft.OffsetAndEpoch
import org.apache.kafka.raft.ReplicatedLog
import org.apache.kafka.snapshot.SnapshotPath
import org.apache.kafka.snapshot.Snapshots
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

final class KafkaMetadataLogTest {
  import KafkaMetadataLogTest._

  var tempDir: File = null
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
  def testCreateSnapshot(): Unit = {
    val topicPartition = new TopicPartition("cluster-metadata", 0)
    val numberOfRecords = 10
    val epoch = 0
    val snapshotId = new OffsetAndEpoch(numberOfRecords, epoch)
    val log = buildMetadataLog(tempDir, mockTime, topicPartition)

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
    val topicPartition = new TopicPartition("cluster-metadata", 0)
    val log = buildMetadataLog(tempDir, mockTime, topicPartition)

    assertEquals(Optional.empty(), log.readSnapshot(new OffsetAndEpoch(10, 0)))
  }

  @Test
  def testUpdateLogStartOffset(): Unit = {
    val topicPartition = new TopicPartition("cluster-metadata", 0)
    val log = buildMetadataLog(tempDir, mockTime, topicPartition)
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
    append(log, newRecords, epoch + 1)

    // Start offset should not change since a new snapshot was not generated
    assertFalse(log.deleteBeforeSnapshot(new OffsetAndEpoch(offset + newRecords, epoch)))
    assertEquals(offset, log.startOffset)

    assertEquals(epoch + 1, log.lastFetchedEpoch)
    assertEquals(offset + newRecords, log.endOffset().offset)
    assertEquals(offset, log.highWatermark.offset)
  }

  @Test
  def testUpdateLogStartOffsetWithMissingSnapshot(): Unit = {
    val topicPartition = new TopicPartition("cluster-metadata", 0)
    val log = buildMetadataLog(tempDir, mockTime, topicPartition)
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
    val topicPartition = new TopicPartition("cluster-metadata", 0)
    val log = buildMetadataLog(tempDir, mockTime, topicPartition)
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
    val topicPartition = new TopicPartition("cluster-metadata", 0)
    val log = buildMetadataLog(tempDir, mockTime, topicPartition)
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

    append(log, numberOfRecords, epoch)

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
  def testDoesntTruncateFully(): Unit = {
    val topicPartition = new TopicPartition("cluster-metadata", 0)
    val log = buildMetadataLog(tempDir, mockTime, topicPartition)
    val numberOfRecords = 10
    val epoch = 1

    append(log, numberOfRecords, epoch)

    val olderEpochSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch - 1)
    TestUtils.resource(log.createSnapshot(olderEpochSnapshotId)) { snapshot =>
      snapshot.freeze()
    }

    assertFalse(log.truncateToLatestSnapshot())

    append(log, numberOfRecords, epoch)

    val olderOffsetSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch)
    TestUtils.resource(log.createSnapshot(olderOffsetSnapshotId)) { snapshot =>
      snapshot.freeze()
    }

    assertFalse(log.truncateToLatestSnapshot())
  }

  @Test
  def testCleanupSnapshots(): Unit = {
    val topicPartition = new TopicPartition("cluster-metadata", 0)
    val (logDir, log) = buildMetadataLogAndDir(tempDir, mockTime, topicPartition)
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

    val secondLog = buildMetadataLog(tempDir, mockTime, topicPartition)

    assertEquals(snapshotId, secondLog.latestSnapshotId.get)
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
  def testCreateReplicatedLogTruncatesFully(): Unit = {
    val topicPartition = new TopicPartition("cluster-metadata", 0)
    val (logDir, log) = buildMetadataLogAndDir(tempDir, mockTime, topicPartition)
    val numberOfRecords = 10
    val epoch = 1
    val snapshotId = new OffsetAndEpoch(numberOfRecords + 1, epoch + 1)

    append(log, numberOfRecords, epoch)
    TestUtils.resource(log.createSnapshot(snapshotId)) { snapshot =>
      snapshot.freeze()
    }

    log.close()

    val secondLog = buildMetadataLog(tempDir, mockTime, topicPartition)

    assertEquals(snapshotId, secondLog.latestSnapshotId.get)
    assertEquals(snapshotId.offset, secondLog.startOffset)
    assertEquals(snapshotId.epoch, secondLog.lastFetchedEpoch)
    assertEquals(snapshotId.offset, secondLog.endOffset().offset)
    assertEquals(snapshotId.offset, secondLog.highWatermark.offset)
  }
}

object KafkaMetadataLogTest {
  def buildMetadataLogAndDir(tempDir: File, time: MockTime, topicPartition: TopicPartition): (Path, KafkaMetadataLog) = {

    val logDir = createLogDirectory(tempDir, Log.logDirName(topicPartition))
    val logConfig = LogTest.createLogConfig()

    val log = Log(
      dir = logDir,
      config = logConfig,
      logStartOffset = 0,
      recoveryPoint = 0,
      scheduler = time.scheduler,
      brokerTopicStats = new BrokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(5)
    )

    (logDir.toPath, KafkaMetadataLog(log, topicPartition))
  }

  def buildMetadataLog(tempDir: File, time: MockTime, topicPartition: TopicPartition): KafkaMetadataLog = {
    val (_, log) = buildMetadataLogAndDir(tempDir, time, topicPartition)
    log
  }


  def append(log: ReplicatedLog, numberOfRecords: Int, epoch: Int): LogAppendInfo = {
    log.appendAsLeader(
      MemoryRecords.withRecords(
        CompressionType.NONE,
        (0 until numberOfRecords).map(number => new SimpleRecord(number.toString.getBytes)): _*
      ),
      epoch
    )
  }

  private def createLogDirectory(logDir: File, logDirName: String): File = {
    val logDirPath = logDir.getAbsolutePath
    val dir = new File(logDirPath, logDirName)
    if (!Files.exists((dir.toPath))) {
      Files.createDirectories(dir.toPath)
    }
    dir
  }
}
