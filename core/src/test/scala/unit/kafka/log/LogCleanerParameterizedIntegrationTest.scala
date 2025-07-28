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
import java.util.{Optional, Properties}
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.config.ServerConfigs
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogCleanerManager, LogConfig, UnifiedLog}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, ArgumentsProvider, ArgumentsSource}

import scala.collection._
import scala.jdk.CollectionConverters._

/**
 * This is an integration test that tests the fully integrated log cleaner
 */
class LogCleanerParameterizedIntegrationTest extends AbstractLogCleanerIntegrationTest {

  val time = new MockTime()

  val topicPartitions = Array(new TopicPartition("log", 0), new TopicPartition("log", 1), new TopicPartition("log", 2))

  @ParameterizedTest
  @ArgumentsSource(classOf[LogCleanerParameterizedIntegrationTest.AllCompressions])
  def cleanerTest(compressionType: CompressionType): Unit = {
    val codec: Compression = Compression.of(compressionType).build()
    val largeMessageKey = 20
    val (largeMessageValue, largeMessageSet) = createLargeSingleMessageSet(largeMessageKey, RecordBatch.CURRENT_MAGIC_VALUE, codec)
    val maxMessageSize = largeMessageSet.sizeInBytes

    cleaner = makeCleaner(partitions = topicPartitions, maxMessageSize = maxMessageSize)
    val log = cleaner.logs.get(topicPartitions(0))

    val appends = writeDups(numKeys = 100, numDups = 3, log = log, codec = codec)
    val startSize = log.size
    cleaner.startup()

    val firstDirty = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty)
    val compactedSize = log.logSegments.asScala.map(_.size).sum
    assertTrue(startSize > compactedSize, s"log should have been compacted: startSize=$startSize compactedSize=$compactedSize")

    checkLogAfterAppendingDups(log, startSize, appends)

    val appendInfo = log.appendAsLeader(largeMessageSet, 0)
    // move LSO forward to increase compaction bound
    log.updateHighWatermark(log.logEndOffset)
    val largeMessageOffset = appendInfo.firstOffset

    val dups = writeDups(startKey = largeMessageKey + 1, numKeys = 100, numDups = 3, log = log, codec = codec)
    val appends2 = appends ++ Seq((largeMessageKey, largeMessageValue, largeMessageOffset)) ++ dups
    val firstDirty2 = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty2)

    checkLogAfterAppendingDups(log, startSize, appends2)

    // simulate deleting a partition, by removing it from logs
    // force a checkpoint
    // and make sure its gone from checkpoint file
    cleaner.logs.remove(topicPartitions(0))
    cleaner.updateCheckpoints(logDir, Optional.of(topicPartitions(0)))
    val checkpoints = new OffsetCheckpointFile(new File(logDir, LogCleanerManager.OFFSET_CHECKPOINT_FILE), null).read()
    // we expect partition 0 to be gone
    assertFalse(checkpoints.containsKey(topicPartitions(0)))
  }

  @ParameterizedTest
  @ArgumentsSource(classOf[LogCleanerParameterizedIntegrationTest.AllCompressions])
  def testCleansCombinedCompactAndDeleteTopic(compressionType: CompressionType): Unit = {
    val logProps  = new Properties()
    val retentionMs: Integer = 100000
    logProps.put(TopicConfig.RETENTION_MS_CONFIG, retentionMs: Integer)
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete")

    def runCleanerAndCheckCompacted(numKeys: Int): (UnifiedLog, Seq[(Int, String, Long)]) = {
      cleaner = makeCleaner(partitions = topicPartitions.take(1), propertyOverrides = logProps, backoffMs = 100L)
      val log = cleaner.logs.get(topicPartitions(0))

      val messages = writeDups(numKeys = numKeys, numDups = 3, log = log, codec = Compression.of(compressionType).build())
      val startSize = log.size

      log.updateHighWatermark(log.logEndOffset)

      val firstDirty = log.activeSegment.baseOffset
      cleaner.startup()

      // should compact the log
      checkLastCleaned("log", 0, firstDirty)
      val compactedSize = log.logSegments.asScala.map(_.size).sum
      assertTrue(startSize > compactedSize, s"log should have been compacted: startSize=$startSize compactedSize=$compactedSize")
      (log, messages)
    }

    val (log, _) = runCleanerAndCheckCompacted(100)

    // Set the last modified time to an old value to force deletion of old segments
    val endOffset = log.logEndOffset
    log.logSegments.forEach(_.setLastModified(time.milliseconds - (2 * retentionMs)))
    TestUtils.waitUntilTrue(() => log.logStartOffset == endOffset && log.numberOfSegments == 1,
      "Timed out waiting for deletion of old segments")

    cleaner.shutdown()
    closeLog(log)

    // run the cleaner again to make sure if there are no issues post deletion
    val (log2, messages) = runCleanerAndCheckCompacted(20)
    val read = readFromLog(log2)
    assertEquals(toMap(messages), toMap(read), "Contents of the map shouldn't change")
  }

  @ParameterizedTest
  @ArgumentsSource(classOf[LogCleanerParameterizedIntegrationTest.ExcludeZstd])
  def testCleanerWithMessageFormatV0V1V2(compressionType: CompressionType): Unit = {
    val compression = Compression.of(compressionType).build()
    val largeMessageKey = 20
    val (largeMessageValue, largeMessageSet) = createLargeSingleMessageSet(largeMessageKey, RecordBatch.MAGIC_VALUE_V0, compression)
    val maxMessageSize = compression match {
      case Compression.NONE => largeMessageSet.sizeInBytes
      case _ =>
        // the broker assigns absolute offsets for message format 0 which potentially causes the compressed size to
        // increase because the broker offsets are larger than the ones assigned by the client
        // adding `6` to the message set size is good enough for this test: it covers the increased message size while
        // still being less than the overhead introduced by the conversion from message format version 0 to 1
        largeMessageSet.sizeInBytes + 6
    }

    cleaner = makeCleaner(partitions = topicPartitions, maxMessageSize = maxMessageSize)

    val log = cleaner.logs.get(topicPartitions(0))
    val props = logConfigProperties(maxMessageSize = maxMessageSize)
    props.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.name)
    val logConfig = new LogConfig(props)
    log.updateConfig(logConfig)

    val appends1 = writeDups(numKeys = 100, numDups = 3, log = log, codec = compression, magicValue = RecordBatch.MAGIC_VALUE_V0)
    val startSize = log.size
    cleaner.startup()

    val firstDirty = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty)
    val compactedSize = log.logSegments.asScala.map(_.size).sum
    assertTrue(startSize > compactedSize, s"log should have been compacted: startSize=$startSize compactedSize=$compactedSize")

    checkLogAfterAppendingDups(log, startSize, appends1)

    val dupsV0 = writeDups(numKeys = 40, numDups = 3, log = log, codec = compression, magicValue = RecordBatch.MAGIC_VALUE_V0)
    val appendInfo = log.appendAsLeaderWithRecordVersion(largeMessageSet, 0, RecordVersion.V0)
    // move LSO forward to increase compaction bound
    log.updateHighWatermark(log.logEndOffset)
    val largeMessageOffset = appendInfo.firstOffset

    // also add some messages with version 1 and version 2 to check that we handle mixed format versions correctly
    val dupsV1 = writeDups(startKey = 30, numKeys = 40, numDups = 3, log = log, codec = compression, magicValue = RecordBatch.MAGIC_VALUE_V1)
    val dupsV2 = writeDups(startKey = 15, numKeys = 5, numDups = 3, log = log, codec = compression, magicValue = RecordBatch.MAGIC_VALUE_V2)

    val v0RecordKeysWithNoV1V2Updates = (appends1.map(_._1).toSet -- dupsV1.map(_._1) -- dupsV2.map(_._1)).map(_.toString)
    val appends2: Seq[(Int, String, Long)] =
      appends1 ++ dupsV0 ++ Seq((largeMessageKey, largeMessageValue, largeMessageOffset)) ++ dupsV1 ++ dupsV2

    // roll the log so that all appended messages can be compacted
    log.roll()
    val firstDirty2 = log.activeSegment.baseOffset
    checkLastCleaned("log", 0, firstDirty2)

    checkLogAfterAppendingDups(log, startSize, appends2)
    checkLogAfterConvertingToV2(compressionType, log, logConfig.messageTimestampType, v0RecordKeysWithNoV1V2Updates)
  }

  @ParameterizedTest
  @ArgumentsSource(classOf[LogCleanerParameterizedIntegrationTest.ExcludeZstd])
  def testCleaningNestedMessagesWithV0V1(compressionType: CompressionType): Unit = {
    val compression = Compression.of(compressionType).build()
    val maxMessageSize = 192
    cleaner = makeCleaner(partitions = topicPartitions, maxMessageSize = maxMessageSize, segmentSize = 256)

    val log = cleaner.logs.get(topicPartitions(0))
    val logConfig = new LogConfig(logConfigProperties(maxMessageSize = maxMessageSize, segmentSize = 256))
    log.updateConfig(logConfig)

    // with compression enabled, these messages will be written as a single message containing all the individual messages
    var appendsV0 = writeDupsSingleMessageSet(numKeys = 2, numDups = 3, log = log, codec = compression, magicValue = RecordBatch.MAGIC_VALUE_V0)
    appendsV0 ++= writeDupsSingleMessageSet(numKeys = 2, startKey = 3, numDups = 2, log = log, codec = compression, magicValue = RecordBatch.MAGIC_VALUE_V0)

    var appendsV1 = writeDupsSingleMessageSet(startKey = 4, numKeys = 2, numDups = 2, log = log, codec = compression, magicValue = RecordBatch.MAGIC_VALUE_V1)
    appendsV1 ++= writeDupsSingleMessageSet(startKey = 4, numKeys = 2, numDups = 2, log = log, codec = compression, magicValue = RecordBatch.MAGIC_VALUE_V1)
    appendsV1 ++= writeDupsSingleMessageSet(startKey = 6, numKeys = 2, numDups = 2, log = log, codec = compression, magicValue = RecordBatch.MAGIC_VALUE_V1)

    val appends = appendsV0 ++ appendsV1

    val v0RecordKeysWithNoV1V2Updates = (appendsV0.map(_._1).toSet -- appendsV1.map(_._1)).map(_.toString)

    // roll the log so that all appended messages can be compacted
    log.roll()
    val startSize = log.size
    cleaner.startup()

    val firstDirty = log.activeSegment.baseOffset
    assertTrue(firstDirty >= appends.size) // ensure we clean data from V0 and V1

    checkLastCleaned("log", 0, firstDirty)
    val compactedSize = log.logSegments.asScala.map(_.size).sum
    assertTrue(startSize > compactedSize, s"log should have been compacted: startSize=$startSize compactedSize=$compactedSize")

    checkLogAfterAppendingDups(log, startSize, appends)
    checkLogAfterConvertingToV2(compressionType, log, logConfig.messageTimestampType, v0RecordKeysWithNoV1V2Updates)
  }

  private def checkLogAfterConvertingToV2(compressionType: CompressionType, log: UnifiedLog, timestampType: TimestampType,
                                          keysForV0RecordsWithNoV1V2Updates: Set[String]): Unit = {
    for (segment <- log.logSegments.asScala; recordBatch <- segment.log.batches.asScala) {
      // Uncompressed v0/v1 records are always converted into single record v2 batches via compaction if they are retained
      // Compressed v0/v1 record batches are converted into record batches v2 with one or more records (depending on the
      // number of retained records after compaction)
      assertEquals(RecordVersion.V2.value, recordBatch.magic)
      if (compressionType == CompressionType.NONE)
        assertEquals(1, recordBatch.iterator().asScala.size)
      else
        assertTrue(recordBatch.iterator().asScala.nonEmpty)

      val firstRecordKey = TestUtils.readString(recordBatch.iterator().next().key())
      if (keysForV0RecordsWithNoV1V2Updates.contains(firstRecordKey))
        assertEquals(TimestampType.CREATE_TIME, recordBatch.timestampType)
      else
        assertEquals(timestampType, recordBatch.timestampType)

      recordBatch.iterator.asScala.foreach { record =>
        val recordKey = TestUtils.readString(record.key)
        if (keysForV0RecordsWithNoV1V2Updates.contains(recordKey))
          assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp, "Record " + recordKey + " with unexpected timestamp ")
        else
          assertNotEquals(RecordBatch.NO_TIMESTAMP, record.timestamp, "Record " + recordKey + " with unexpected timestamp " + RecordBatch.NO_TIMESTAMP)
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(classOf[LogCleanerParameterizedIntegrationTest.AllCompressions])
  def cleanerConfigUpdateTest(compressionType: CompressionType): Unit = {
    val codec: Compression = Compression.of(compressionType).build()
    val largeMessageKey = 20
    val (_, largeMessageSet) = createLargeSingleMessageSet(largeMessageKey, RecordBatch.CURRENT_MAGIC_VALUE, codec)
    val maxMessageSize = largeMessageSet.sizeInBytes

    cleaner = makeCleaner(partitions = topicPartitions, backoffMs = 1, maxMessageSize = maxMessageSize,
      cleanerIoBufferSize = Some(1))
    val log = cleaner.logs.get(topicPartitions(0))

    writeDups(numKeys = 100, numDups = 3, log = log, codec = codec)
    val startSize = log.size
    cleaner.startup()
    assertEquals(1, cleaner.cleanerCount)

    // Verify no cleaning with LogCleanerIoBufferSizeProp=1
    val firstDirty = log.activeSegment.baseOffset
    val topicPartition = new TopicPartition("log", 0)
    cleaner.awaitCleaned(topicPartition, firstDirty, 10)
    assertTrue(cleaner.cleanerManager.allCleanerCheckpoints.isEmpty, "Should not have cleaned")

    def kafkaConfigWithCleanerConfig(cleanerConfig: CleanerConfig): KafkaConfig = {
      val props = TestUtils.createBrokerConfig(0)
      props.put(CleanerConfig.LOG_CLEANER_THREADS_PROP, cleanerConfig.numThreads.toString)
      props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, cleanerConfig.dedupeBufferSize.toString)
      props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP, cleanerConfig.dedupeBufferLoadFactor.toString)
      props.put(CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP, cleanerConfig.ioBufferSize.toString)
      props.put(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, cleanerConfig.maxMessageSize.toString)
      props.put(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP, cleanerConfig.backoffMs.toString)
      props.put(CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP, cleanerConfig.maxIoBytesPerSecond.toString)
      KafkaConfig.fromProps(props)
    }

    // Verify cleaning done with larger LogCleanerIoBufferSizeProp
    val oldConfig = kafkaConfigWithCleanerConfig(cleaner.currentConfig)
    val newConfig = kafkaConfigWithCleanerConfig(new CleanerConfig(2,
      cleaner.currentConfig.dedupeBufferSize,
      cleaner.currentConfig.dedupeBufferLoadFactor,
      100000,
      cleaner.currentConfig.maxMessageSize,
      cleaner.currentConfig.maxIoBytesPerSecond,
      cleaner.currentConfig.backoffMs,
      true))
    cleaner.reconfigure(oldConfig, newConfig)

    assertEquals(2, cleaner.cleanerCount)
    checkLastCleaned("log", 0, firstDirty)
    val compactedSize = log.logSegments.asScala.map(_.size).sum
    assertTrue(startSize > compactedSize, s"log should have been compacted: startSize=$startSize compactedSize=$compactedSize")
  }

  private def checkLastCleaned(topic: String, partitionId: Int, firstDirty: Long): Unit = {
    // wait until cleaning up to base_offset, note that cleaning happens only when "log dirty ratio" is higher than
    // TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG
    val topicPartition = new TopicPartition(topic, partitionId)
    cleaner.awaitCleaned(topicPartition, firstDirty, 60000L)
    val lastCleaned = cleaner.cleanerManager.allCleanerCheckpoints.get(topicPartition)
    assertTrue(lastCleaned >= firstDirty, s"log cleaner should have processed up to offset $firstDirty, but lastCleaned=$lastCleaned")
  }

  private def checkLogAfterAppendingDups(log: UnifiedLog, startSize: Long, appends: Seq[(Int, String, Long)]): Unit = {
    val read = readFromLog(log)
    assertEquals(toMap(appends), toMap(read), "Contents of the map shouldn't change")
    assertTrue(startSize > log.size)
  }

  private def toMap(messages: Iterable[(Int, String, Long)]): Map[Int, (String, Long)] = {
    messages.map { case (key, value, offset) => key -> (value, offset) }.toMap
  }

  private def readFromLog(log: UnifiedLog): Iterable[(Int, String, Long)] = {
    for (segment <- log.logSegments.asScala; deepLogEntry <- segment.log.records.asScala) yield {
      val key = TestUtils.readString(deepLogEntry.key).toInt
      val value = TestUtils.readString(deepLogEntry.value)
      (key, value, deepLogEntry.offset)
    }
  }

  private def writeDupsSingleMessageSet(numKeys: Int, numDups: Int, log: UnifiedLog, codec: Compression,
                                        startKey: Int = 0, magicValue: Byte): Seq[(Int, String, Long)] = {
    val kvs = for (_ <- 0 until numDups; key <- startKey until (startKey + numKeys)) yield {
      val payload = counter.toString
      incCounter()
      (key, payload)
    }

    val records = kvs.map { case (key, payload) =>
      new SimpleRecord(Time.SYSTEM.milliseconds(), key.toString.getBytes, payload.getBytes)
    }

    val appendInfo = log.appendAsLeaderWithRecordVersion(MemoryRecords.withRecords(magicValue, codec, records: _*),
      0, RecordVersion.lookup(magicValue))
    // move LSO forward to increase compaction bound
    log.updateHighWatermark(log.logEndOffset)
    val offsets = appendInfo.firstOffset to appendInfo.lastOffset

    kvs.zip(offsets).map { case (kv, offset) => (kv._1, kv._2, offset) }
  }

}

object LogCleanerParameterizedIntegrationTest {

  class AllCompressions extends ArgumentsProvider {
    override def provideArguments(context: ExtensionContext): java.util.stream.Stream[_ <: Arguments] =
      java.util.Arrays.stream(CompressionType.values.map(codec => Arguments.of(codec)))
  }

  // zstd compression is not supported with older message formats (i.e supported by V0 and V1)
  class ExcludeZstd extends ArgumentsProvider {
    override def provideArguments(context: ExtensionContext): java.util.stream.Stream[_ <: Arguments] =
      java.util.Arrays.stream(CompressionType.values.filter(_ != CompressionType.ZSTD).map(codec => Arguments.of(codec)))
  }
}
