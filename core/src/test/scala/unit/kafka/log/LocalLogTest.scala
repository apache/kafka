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

import java.io.File
import java.nio.channels.ClosedChannelException
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern
import java.util.Collections
import kafka.server.{FetchDataInfo, KafkaConfig}
import kafka.utils.{MockTime, Scheduler, TestUtils}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, Record, SimpleRecord}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.server.log.internals.{LogDirFailureChannel, LogOffsetMetadata}
import org.junit.jupiter.api.Assertions.{assertFalse, _}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.jdk.CollectionConverters._

class LocalLogTest {

  var config: KafkaConfig = _
  val tmpDir: File = TestUtils.tempDir()
  val logDir: File = TestUtils.randomPartitionLogDir(tmpDir)
  val topicPartition = new TopicPartition("test_topic", 1)
  val logDirFailureChannel = new LogDirFailureChannel(10)
  val mockTime = new MockTime()
  val log: LocalLog = createLocalLogWithActiveSegment(config = LogTestUtils.createLogConfig())

  @BeforeEach
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    config = KafkaConfig.fromProps(props)
  }

  @AfterEach
  def tearDown(): Unit = {
    try {
      log.close()
    } catch {
      case _: KafkaStorageException => {
        // ignore
      }
    }
    Utils.delete(tmpDir)
  }

  case class KeyValue(key: String, value: String) {
    def toRecord(timestamp: => Long = mockTime.milliseconds): SimpleRecord = {
      new SimpleRecord(timestamp, key.getBytes, value.getBytes)
    }
  }

  object KeyValue {
    def fromRecord(record: Record): KeyValue = {
      val key =
        if (record.hasKey)
          StandardCharsets.UTF_8.decode(record.key()).toString
        else
          ""
      val value =
        if (record.hasValue)
          StandardCharsets.UTF_8.decode(record.value()).toString
        else
          ""
      KeyValue(key, value)
    }
  }

  private def kvsToRecords(keyValues: Iterable[KeyValue]): Iterable[SimpleRecord] = {
    keyValues.map(kv => kv.toRecord())
  }

  private def recordsToKvs(records: Iterable[Record]): Iterable[KeyValue] = {
    records.map(r => KeyValue.fromRecord(r))
  }

  private def appendRecords(records: Iterable[SimpleRecord],
                            log: LocalLog = log,
                            initialOffset: Long = 0L): Unit = {
    log.append(lastOffset = initialOffset + records.size - 1,
      largestTimestamp = records.head.timestamp,
      shallowOffsetOfMaxTimestamp = initialOffset,
      records = MemoryRecords.withRecords(initialOffset, CompressionType.NONE, 0, records.toList : _*))
  }

  private def readRecords(log: LocalLog = log,
                          startOffset: Long = 0L,
                          maxLength: => Int = log.segments.activeSegment.size,
                          minOneMessage: Boolean = false,
                          maxOffsetMetadata: => LogOffsetMetadata = log.logEndOffsetMetadata,
                          includeAbortedTxns: Boolean = false): FetchDataInfo = {
    log.read(startOffset,
             maxLength,
             minOneMessage = minOneMessage,
             maxOffsetMetadata,
             includeAbortedTxns = includeAbortedTxns)
  }

  @Test
  def testLogDeleteSegmentsSuccess(): Unit = {
    val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
    appendRecords(List(record))
    log.roll()
    assertEquals(2, log.segments.numberOfSegments)
    assertFalse(logDir.listFiles.isEmpty)
    val segmentsBeforeDelete = List[LogSegment]() ++ log.segments.values
    val deletedSegments = log.deleteAllSegments()
    assertTrue(log.segments.isEmpty)
    assertEquals(segmentsBeforeDelete, deletedSegments)
    assertThrows(classOf[KafkaStorageException], () => log.checkIfMemoryMappedBufferClosed())
    assertTrue(logDir.exists)
  }

  @Test
  def testRollEmptyActiveSegment(): Unit = {
    val oldActiveSegment = log.segments.activeSegment
    log.roll()
    assertEquals(1, log.segments.numberOfSegments)
    assertNotEquals(oldActiveSegment, log.segments.activeSegment)
    assertFalse(logDir.listFiles.isEmpty)
    assertTrue(oldActiveSegment.hasSuffix(LocalLog.DeletedFileSuffix))
  }

  @Test
  def testLogDeleteDirSuccessWhenEmptyAndFailureWhenNonEmpty(): Unit ={
    val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
    appendRecords(List(record))
    log.roll()
    assertEquals(2, log.segments.numberOfSegments)
    assertFalse(logDir.listFiles.isEmpty)

    assertThrows(classOf[IllegalStateException], () => log.deleteEmptyDir())
    assertTrue(logDir.exists)

    log.deleteAllSegments()
    log.deleteEmptyDir()
    assertFalse(logDir.exists)
  }

  @Test
  def testUpdateConfig(): Unit = {
    val oldConfig = log.config
    assertEquals(oldConfig, log.config)

    val newConfig = LogTestUtils.createLogConfig(segmentBytes = oldConfig.segmentSize + 1)
    log.updateConfig(newConfig)
    assertEquals(newConfig, log.config)
  }

  @Test
  def testLogDirRenameToNewDir(): Unit = {
    val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
    appendRecords(List(record))
    log.roll()
    assertEquals(2, log.segments.numberOfSegments)
    val newLogDir = TestUtils.randomPartitionLogDir(tmpDir)
    assertTrue(log.renameDir(newLogDir.getName))
    assertFalse(logDir.exists())
    assertTrue(newLogDir.exists())
    assertEquals(newLogDir, log.dir)
    assertEquals(newLogDir.getParent, log.parentDir)
    assertEquals(newLogDir.getParent, log.dir.getParent)
    log.segments.values.foreach(segment => assertEquals(newLogDir.getPath, segment.log.file().getParentFile.getPath))
    assertEquals(2, log.segments.numberOfSegments)
  }

  @Test
  def testLogDirRenameToExistingDir(): Unit = {
    assertFalse(log.renameDir(log.dir.getName))
  }

  @Test
  def testLogFlush(): Unit = {
    assertEquals(0L, log.recoveryPoint)
    assertEquals(mockTime.milliseconds, log.lastFlushTime)

    val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
    appendRecords(List(record))
    mockTime.sleep(1)
    val newSegment = log.roll()
    log.flush(newSegment.baseOffset)
    log.markFlushed(newSegment.baseOffset)
    assertEquals(1L, log.recoveryPoint)
    assertEquals(mockTime.milliseconds, log.lastFlushTime)
  }

  @Test
  def testLogAppend(): Unit = {
    val fetchDataInfoBeforeAppend = readRecords(maxLength = 1)
    assertTrue(fetchDataInfoBeforeAppend.records.records.asScala.isEmpty)

    mockTime.sleep(1)
    val keyValues = Seq(KeyValue("abc", "ABC"), KeyValue("de", "DE"))
    appendRecords(kvsToRecords(keyValues))
    assertEquals(2L, log.logEndOffset)
    assertEquals(0L, log.recoveryPoint)
    val fetchDataInfo = readRecords()
    assertEquals(2L, fetchDataInfo.records.records.asScala.size)
    assertEquals(keyValues, recordsToKvs(fetchDataInfo.records.records.asScala))
  }

  @Test
  def testLogCloseSuccess(): Unit = {
    val keyValues = Seq(KeyValue("abc", "ABC"), KeyValue("de", "DE"))
    appendRecords(kvsToRecords(keyValues))
    log.close()
    assertThrows(classOf[ClosedChannelException], () => appendRecords(kvsToRecords(keyValues), initialOffset = 2L))
  }

  @Test
  def testLogCloseIdempotent(): Unit = {
    log.close()
    // Check that LocalLog.close() is idempotent
    log.close()
  }

  @Test
  def testLogCloseFailureWhenInMemoryBufferClosed(): Unit = {
    val keyValues = Seq(KeyValue("abc", "ABC"), KeyValue("de", "DE"))
    appendRecords(kvsToRecords(keyValues))
    log.closeHandlers()
    assertThrows(classOf[KafkaStorageException], () => log.close())
  }

  @Test
  def testLogCloseHandlers(): Unit = {
    val keyValues = Seq(KeyValue("abc", "ABC"), KeyValue("de", "DE"))
    appendRecords(kvsToRecords(keyValues))
    log.closeHandlers()
    assertThrows(classOf[ClosedChannelException],
                 () => appendRecords(kvsToRecords(keyValues), initialOffset = 2L))
  }

  @Test
  def testLogCloseHandlersIdempotent(): Unit = {
    log.closeHandlers()
    // Check that LocalLog.closeHandlers() is idempotent
    log.closeHandlers()
  }

  private def testRemoveAndDeleteSegments(asyncDelete: Boolean): Unit = {
    for (offset <- 0 to 8) {
      val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
      appendRecords(List(record), initialOffset = offset)
      log.roll()
    }

    assertEquals(10L, log.segments.numberOfSegments)

    class TestDeletionReason extends SegmentDeletionReason {
      private var _deletedSegments: Iterable[LogSegment] = List[LogSegment]()

      override def logReason(toDelete: List[LogSegment]): Unit = {
        _deletedSegments = List[LogSegment]() ++ toDelete
      }

      def deletedSegments: Iterable[LogSegment] = _deletedSegments
    }
    val reason = new TestDeletionReason()
    val toDelete = List[LogSegment]() ++ log.segments.values
    log.removeAndDeleteSegments(toDelete, asyncDelete = asyncDelete, reason)
    if (asyncDelete) {
      mockTime.sleep(log.config.fileDeleteDelayMs + 1)
    }
    assertTrue(log.segments.isEmpty)
    assertEquals(toDelete, reason.deletedSegments)
    toDelete.foreach(segment => assertTrue(segment.deleted()))
  }

  @Test
  def testRemoveAndDeleteSegmentsSync(): Unit = {
    testRemoveAndDeleteSegments(asyncDelete = false)
  }

  @Test
  def testRemoveAndDeleteSegmentsAsync(): Unit = {
    testRemoveAndDeleteSegments(asyncDelete = true)
  }

  private def testDeleteSegmentFiles(asyncDelete: Boolean): Unit = {
    for (offset <- 0 to 8) {
      val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
      appendRecords(List(record), initialOffset = offset)
      log.roll()
    }

    assertEquals(10L, log.segments.numberOfSegments)

    val toDelete = List[LogSegment]() ++ log.segments.values
    LocalLog.deleteSegmentFiles(toDelete, asyncDelete = asyncDelete, log.dir, log.topicPartition, log.config, log.scheduler, log.logDirFailureChannel, "")
    if (asyncDelete) {
      toDelete.foreach {
        segment =>
          assertFalse(segment.deleted())
          assertTrue(segment.hasSuffix(LocalLog.DeletedFileSuffix))
      }
      mockTime.sleep(log.config.fileDeleteDelayMs + 1)
    }
    toDelete.foreach(segment => assertTrue(segment.deleted()))
  }

  @Test
  def testDeleteSegmentFilesSync(): Unit = {
    testDeleteSegmentFiles(asyncDelete = false)
  }

  @Test
  def testDeleteSegmentFilesAsync(): Unit = {
    testDeleteSegmentFiles(asyncDelete = true)
  }

  @Test
  def testDeletableSegmentsFilter(): Unit = {
    for (offset <- 0 to 8) {
      val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
      appendRecords(List(record), initialOffset = offset)
      log.roll()
    }

    assertEquals(10, log.segments.numberOfSegments)

    {
      val deletable = log.deletableSegments(
        (segment: LogSegment, _: Option[LogSegment]) => segment.baseOffset <= 5)
      val expected = log.segments.nonActiveLogSegmentsFrom(0L).filter(segment => segment.baseOffset <= 5).toList
      assertEquals(6, expected.length)
      assertEquals(expected, deletable.toList)
    }

    {
      val deletable = log.deletableSegments((_: LogSegment, _: Option[LogSegment]) => true)
      val expected = log.segments.nonActiveLogSegmentsFrom(0L).toList
      assertEquals(9, expected.length)
      assertEquals(expected, deletable.toList)
    }

    {
      val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
      appendRecords(List(record), initialOffset = 9L)
      val deletable = log.deletableSegments((_: LogSegment, _: Option[LogSegment]) => true)
      val expected = log.segments.values.toList
      assertEquals(10, expected.length)
      assertEquals(expected, deletable.toList)
    }
  }

  @Test
  def testDeletableSegmentsIteration(): Unit = {
    for (offset <- 0 to 8) {
      val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
      appendRecords(List(record), initialOffset = offset)
      log.roll()
    }

    assertEquals(10L, log.segments.numberOfSegments)

    var offset = 0
    val deletableSegments = log.deletableSegments(
      (segment: LogSegment, nextSegmentOpt: Option[LogSegment]) => {
        assertEquals(offset, segment.baseOffset)
        val floorSegmentOpt = log.segments.floorSegment(offset)
        assertTrue(floorSegmentOpt.isDefined)
        assertEquals(floorSegmentOpt.get, segment)
        if (offset == log.logEndOffset) {
          assertFalse(nextSegmentOpt.isDefined)
        } else {
          assertTrue(nextSegmentOpt.isDefined)
          val higherSegmentOpt = log.segments.higherSegment(segment.baseOffset)
          assertTrue(higherSegmentOpt.isDefined)
          assertEquals(segment.baseOffset + 1, higherSegmentOpt.get.baseOffset)
          assertEquals(higherSegmentOpt.get, nextSegmentOpt.get)
        }
        offset += 1
        true
      })
    assertEquals(10L, log.segments.numberOfSegments)
    assertEquals(log.segments.nonActiveLogSegmentsFrom(0L).toSeq, deletableSegments.toSeq)
  }

  @Test
  def testCreateAndDeleteSegment(): Unit = {
    val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
    appendRecords(List(record))
    val newOffset = log.segments.activeSegment.baseOffset + 1
    val oldActiveSegment = log.segments.activeSegment
    val newActiveSegment = log.createAndDeleteSegment(newOffset, log.segments.activeSegment, asyncDelete = true, LogTruncation(log))
    assertEquals(1, log.segments.numberOfSegments)
    assertEquals(newActiveSegment, log.segments.activeSegment)
    assertNotEquals(oldActiveSegment, log.segments.activeSegment)
    assertTrue(oldActiveSegment.hasSuffix(LocalLog.DeletedFileSuffix))
    assertEquals(newOffset, log.segments.activeSegment.baseOffset)
    assertEquals(0L, log.recoveryPoint)
    assertEquals(newOffset, log.logEndOffset)
    val fetchDataInfo = readRecords(startOffset = newOffset)
    assertTrue(fetchDataInfo.records.records.asScala.isEmpty)
  }

  @Test
  def testTruncateFullyAndStartAt(): Unit = {
    val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
    for (offset <- 0 to 7) {
      appendRecords(List(record), initialOffset = offset)
      if (offset % 2 != 0)
        log.roll()
    }
    for (offset <- 8 to 12) {
      val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
      appendRecords(List(record), initialOffset = offset)
    }
    assertEquals(5, log.segments.numberOfSegments)
    assertNotEquals(10L, log.segments.activeSegment.baseOffset)
    val expected = List[LogSegment]() ++ log.segments.values
    val deleted = log.truncateFullyAndStartAt(10L)
    assertEquals(expected, deleted)
    assertEquals(1, log.segments.numberOfSegments)
    assertEquals(10L, log.segments.activeSegment.baseOffset)
    assertEquals(0L, log.recoveryPoint)
    assertEquals(10L, log.logEndOffset)
    val fetchDataInfo = readRecords(startOffset = 10L)
    assertTrue(fetchDataInfo.records.records.asScala.isEmpty)
  }

  @Test
  def testTruncateTo(): Unit = {
    for (offset <- 0 to 11) {
      val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
      appendRecords(List(record), initialOffset = offset)
      if (offset % 3 == 2)
        log.roll()
    }
    assertEquals(5, log.segments.numberOfSegments)
    assertEquals(12L, log.logEndOffset)

    val expected = List[LogSegment]() ++ log.segments.values(9L, log.logEndOffset + 1)
    // Truncate to an offset before the base offset of the active segment
    val deleted = log.truncateTo(7L)
    assertEquals(expected, deleted)
    assertEquals(3, log.segments.numberOfSegments)
    assertEquals(6L, log.segments.activeSegment.baseOffset)
    assertEquals(0L, log.recoveryPoint)
    assertEquals(7L, log.logEndOffset)
    val fetchDataInfo = readRecords(startOffset = 6L)
    assertEquals(1, fetchDataInfo.records.records.asScala.size)
    assertEquals(Seq(KeyValue("", "a")), recordsToKvs(fetchDataInfo.records.records.asScala))

    // Verify that we can still append to the active segment
    val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
    appendRecords(List(record), initialOffset = 7L)
    assertEquals(8L, log.logEndOffset)
  }

  @Test
  def testNonActiveSegmentsFrom(): Unit = {
    for (i <- 0 until 5) {
      val keyValues = Seq(KeyValue(i.toString, i.toString))
      appendRecords(kvsToRecords(keyValues), initialOffset = i)
      log.roll()
    }

    def nonActiveBaseOffsetsFrom(startOffset: Long): Seq[Long] = {
      log.segments.nonActiveLogSegmentsFrom(startOffset).map(_.baseOffset).toSeq
    }

    assertEquals(5L, log.segments.activeSegment.baseOffset)
    assertEquals(0 until 5, nonActiveBaseOffsetsFrom(0L))
    assertEquals(Seq.empty, nonActiveBaseOffsetsFrom(5L))
    assertEquals(2 until 5, nonActiveBaseOffsetsFrom(2L))
    assertEquals(Seq.empty, nonActiveBaseOffsetsFrom(6L))
  }

  private def topicPartitionName(topic: String, partition: String): String = topic + "-" + partition

  @Test
  def testParseTopicPartitionName(): Unit = {
    val topic = "test_topic"
    val partition = "143"
    val dir = new File(logDir, topicPartitionName(topic, partition))
    val topicPartition = LocalLog.parseTopicPartitionName(dir)
    assertEquals(topic, topicPartition.topic)
    assertEquals(partition.toInt, topicPartition.partition)
  }

  /**
   * Tests that log directories with a period in their name that have been marked for deletion
   * are parsed correctly by `Log.parseTopicPartitionName` (see KAFKA-5232 for details).
   */
  @Test
  def testParseTopicPartitionNameWithPeriodForDeletedTopic(): Unit = {
    val topic = "foo.bar-testtopic"
    val partition = "42"
    val dir = new File(logDir, LocalLog.logDeleteDirName(new TopicPartition(topic, partition.toInt)))
    val topicPartition = LocalLog.parseTopicPartitionName(dir)
    assertEquals(topic, topicPartition.topic, "Unexpected topic name parsed")
    assertEquals(partition.toInt, topicPartition.partition, "Unexpected partition number parsed")
  }

  @Test
  def testParseTopicPartitionNameForEmptyName(): Unit = {
    val dir = new File("")
    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
      () => "KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
  }

  @Test
  def testParseTopicPartitionNameForNull(): Unit = {
    val dir: File = null
    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
      () => "KafkaException should have been thrown for dir: " + dir)
  }

  @Test
  def testParseTopicPartitionNameForMissingSeparator(): Unit = {
    val topic = "test_topic"
    val partition = "1999"
    val dir = new File(logDir, topic + partition)
    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
      () => "KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    // also test the "-delete" marker case
    val deleteMarkerDir = new File(logDir, topic + partition + "." + LocalLog.DeleteDirSuffix)
    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(deleteMarkerDir),
      () => "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
  }

  @Test
  def testParseTopicPartitionNameForMissingTopic(): Unit = {
    val topic = ""
    val partition = "1999"
    val dir = new File(logDir, topicPartitionName(topic, partition))
    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
      () => "KafkaException should have been thrown for dir: " + dir.getCanonicalPath)

    // also test the "-delete" marker case
    val deleteMarkerDir = new File(logDir, LocalLog.logDeleteDirName(new TopicPartition(topic, partition.toInt)))

    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(deleteMarkerDir),
      () => "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
  }

  @Test
  def testParseTopicPartitionNameForMissingPartition(): Unit = {
    val topic = "test_topic"
    val partition = ""
    val dir = new File(logDir.getPath + topicPartitionName(topic, partition))
    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
      () => "KafkaException should have been thrown for dir: " + dir.getCanonicalPath)

    // also test the "-delete" marker case
    val deleteMarkerDir = new File(logDir, topicPartitionName(topic, partition) + "." + LocalLog.DeleteDirSuffix)
    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(deleteMarkerDir),
      () => "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
  }

  @Test
  def testParseTopicPartitionNameForInvalidPartition(): Unit = {
    val topic = "test_topic"
    val partition = "1999a"
    val dir = new File(logDir, topicPartitionName(topic, partition))
    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
      () => "KafkaException should have been thrown for dir: " + dir.getCanonicalPath)

    // also test the "-delete" marker case
    val deleteMarkerDir = new File(logDir, topic + partition + "." + LocalLog.DeleteDirSuffix)
    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(deleteMarkerDir),
      () => "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
  }

  @Test
  def testParseTopicPartitionNameForExistingInvalidDir(): Unit = {
    val dir1 = new File(logDir.getPath + "/non_kafka_dir")
    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir1),
      () => "KafkaException should have been thrown for dir: " + dir1.getCanonicalPath)
    val dir2 = new File(logDir.getPath + "/non_kafka_dir-delete")
    assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir2),
      () => "KafkaException should have been thrown for dir: " + dir2.getCanonicalPath)
  }

  @Test
  def testLogDeleteDirName(): Unit = {
    val name1 = LocalLog.logDeleteDirName(new TopicPartition("foo", 3))
    assertTrue(name1.length <= 255)
    assertTrue(Pattern.compile("foo-3\\.[0-9a-z]{32}-delete").matcher(name1).matches())
    assertTrue(LocalLog.DeleteDirPattern.matcher(name1).matches())
    assertFalse(LocalLog.FutureDirPattern.matcher(name1).matches())
    val name2 = LocalLog.logDeleteDirName(
      new TopicPartition("n" + String.join("", Collections.nCopies(248, "o")), 5))
    assertEquals(255, name2.length)
    assertTrue(Pattern.compile("n[o]{212}-5\\.[0-9a-z]{32}-delete").matcher(name2).matches())
    assertTrue(LocalLog.DeleteDirPattern.matcher(name2).matches())
    assertFalse(LocalLog.FutureDirPattern.matcher(name2).matches())
  }

  @Test
  def testOffsetFromFile(): Unit = {
    val offset = 23423423L

    val logFile = LocalLog.logFile(tmpDir, offset)
    assertEquals(offset, LocalLog.offsetFromFile(logFile))

    val offsetIndexFile = LocalLog.offsetIndexFile(tmpDir, offset)
    assertEquals(offset, LocalLog.offsetFromFile(offsetIndexFile))

    val timeIndexFile = LocalLog.timeIndexFile(tmpDir, offset)
    assertEquals(offset, LocalLog.offsetFromFile(timeIndexFile))
  }

  @Test
  def testRollSegmentThatAlreadyExists(): Unit = {
    assertEquals(1, log.segments.numberOfSegments, "Log begins with a single empty segment.")

    // roll active segment with the same base offset of size zero should recreate the segment
    log.roll(Some(0L))
    assertEquals(1, log.segments.numberOfSegments, "Expect 1 segment after roll() empty segment with base offset.")

    // should be able to append records to active segment
    val keyValues1 = List(KeyValue("k1", "v1"))
    appendRecords(kvsToRecords(keyValues1))
    assertEquals(0L, log.segments.activeSegment.baseOffset)
    // make sure we can append more records
    val keyValues2 = List(KeyValue("k2", "v2"))
    appendRecords(keyValues2.map(_.toRecord(mockTime.milliseconds + 10)), initialOffset = 1L)
    assertEquals(2, log.logEndOffset, "Expect two records in the log")
    val readResult = readRecords()
    assertEquals(2L, readResult.records.records.asScala.size)
    assertEquals(keyValues1 ++ keyValues2, recordsToKvs(readResult.records.records.asScala))

    // roll so that active segment is empty
    log.roll()
    assertEquals(2L, log.segments.activeSegment.baseOffset, "Expect base offset of active segment to be LEO")
    assertEquals(2, log.segments.numberOfSegments, "Expect two segments.")
    assertEquals(2L, log.logEndOffset)
  }

  @Test
  def testNewSegmentsAfterRoll(): Unit = {
    assertEquals(1, log.segments.numberOfSegments, "Log begins with a single empty segment.")

    // roll active segment with the same base offset of size zero should recreate the segment
    {
      val newSegment = log.roll()
      assertEquals(0L, newSegment.baseOffset)
      assertEquals(1, log.segments.numberOfSegments)
      assertEquals(0L, log.logEndOffset)
    }

    appendRecords(List(KeyValue("k1", "v1").toRecord()))

    {
      val newSegment = log.roll()
      assertEquals(1L, newSegment.baseOffset)
      assertEquals(2, log.segments.numberOfSegments)
      assertEquals(1L, log.logEndOffset)
    }

    appendRecords(List(KeyValue("k2", "v2").toRecord()), initialOffset = 1L)

    {
      val newSegment = log.roll(Some(1L))
      assertEquals(2L, newSegment.baseOffset)
      assertEquals(3, log.segments.numberOfSegments)
      assertEquals(2L, log.logEndOffset)
    }
  }

  @Test
  def testRollSegmentErrorWhenNextOffsetIsIllegal(): Unit = {
    assertEquals(1, log.segments.numberOfSegments, "Log begins with a single empty segment.")

    val keyValues = List(KeyValue("k1", "v1"), KeyValue("k2", "v2"), KeyValue("k3", "v3"))
    appendRecords(kvsToRecords(keyValues))
    assertEquals(0L, log.segments.activeSegment.baseOffset)
    assertEquals(3, log.logEndOffset, "Expect two records in the log")

    // roll to create an empty active segment
    log.roll()
    assertEquals(3L, log.segments.activeSegment.baseOffset)

    // intentionally setup the logEndOffset to introduce an error later
    log.updateLogEndOffset(1L)

    // expect an error because of attempt to roll to a new offset (1L) that's lower than the
    // base offset (3L) of the active segment
    assertThrows(classOf[KafkaException], () => log.roll())
  }

  private def createLocalLogWithActiveSegment(dir: File = logDir,
                                              config: LogConfig,
                                              segments: LogSegments = new LogSegments(topicPartition),
                                              recoveryPoint: Long = 0L,
                                              nextOffsetMetadata: LogOffsetMetadata = new LogOffsetMetadata(0L, 0L, 0),
                                              scheduler: Scheduler = mockTime.scheduler,
                                              time: Time = mockTime,
                                              topicPartition: TopicPartition = topicPartition,
                                              logDirFailureChannel: LogDirFailureChannel = logDirFailureChannel): LocalLog = {
    segments.add(LogSegment.open(dir = dir,
                                 baseOffset = 0L,
                                 config,
                                 time = time,
                                 initFileSize = config.initFileSize,
                                 preallocate = config.preallocate))
    new LocalLog(_dir = dir,
                 config = config,
                 segments = segments,
                 recoveryPoint = recoveryPoint,
                 nextOffsetMetadata = nextOffsetMetadata,
                 scheduler = scheduler,
                 time = time,
                 topicPartition = topicPartition,
                 logDirFailureChannel = logDirFailureChannel)
  }
}
