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

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import kafka.server.{BrokerTopicStats, RequestLocal}
import kafka.utils.TestUtils.meterCount
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.{CorruptRecordException, InvalidTimestampException, UnsupportedCompressionTypeException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{PrimitiveRef, Time}
import org.apache.kafka.common.{InvalidRecordException, TopicPartition}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.storage.internals.log.LogValidator.ValidationResult
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogValidator, RecordValidationException}
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.jdk.CollectionConverters._

class LogValidatorTest {

  val time = Time.SYSTEM
  val topicPartition = new TopicPartition("topic", 0)
  val metricsKeySet = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala
  val metricsRecorder = UnifiedLog.newValidatorMetricsRecorder(new BrokerTopicStats().allTopicsStats)

  @Test
  def testOnlyOneBatch(): Unit = {
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V0, Compression.gzip().build(), Compression.gzip().build())
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V1, Compression.gzip().build(), Compression.gzip().build())
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V2, Compression.gzip().build(), Compression.gzip().build())
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V0, Compression.gzip().build(), Compression.NONE)
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V1, Compression.gzip().build(), Compression.NONE)
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V2, Compression.gzip().build(), Compression.NONE)
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V2, Compression.NONE, Compression.NONE)
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V2, Compression.NONE, Compression.gzip().build())
  }

  @Test
  def testAllowMultiBatch(): Unit = {
    checkAllowMultiBatch(RecordBatch.MAGIC_VALUE_V0, Compression.NONE, Compression.NONE)
    checkAllowMultiBatch(RecordBatch.MAGIC_VALUE_V1, Compression.NONE, Compression.NONE)
    checkAllowMultiBatch(RecordBatch.MAGIC_VALUE_V0, Compression.NONE, Compression.gzip().build())
    checkAllowMultiBatch(RecordBatch.MAGIC_VALUE_V1, Compression.NONE, Compression.gzip().build())
  }

  @Test
  def testValidationOfBatchesWithNonSequentialInnerOffsets(): Unit = {
    def testMessageValidation(magicValue: Byte): Unit = {
      val numRecords = 20
      val compression: Compression = Compression.gzip().build()
      val invalidRecords = recordsWithNonSequentialInnerOffsets(magicValue, compression, numRecords)

      // Validation for v2 and above is strict for this case. For older formats, we fix invalid
      // internal offsets by rewriting the batch.
      if (magicValue >= RecordBatch.MAGIC_VALUE_V2) {
        assertThrows(classOf[InvalidRecordException],
          () => validateMessages(invalidRecords, magicValue, CompressionType.GZIP, compression)
        )
      } else {
        val result = validateMessages(invalidRecords, magicValue, CompressionType.GZIP, compression)
        assertEquals(0 until numRecords, result.validatedRecords.records.asScala.map(_.offset))
      }
    }

    for (version <- RecordVersion.values) {
      testMessageValidation(version.value)
    }
  }

  @Test
  def testMisMatchMagic(): Unit = {
    val compression: Compression = Compression.gzip().build()
    checkMismatchMagic(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, compression)
    checkMismatchMagic(RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V0, compression)
  }

  private def checkOnlyOneBatch(magic: Byte, sourceCompression: Compression, targetCompression: Compression): Unit = {
    assertThrows(classOf[InvalidRecordException],
      () => validateMessages(createTwoBatchedRecords(magic, sourceCompression), magic, sourceCompression.`type`(), targetCompression)
    )
  }

  private def checkAllowMultiBatch(magic: Byte, sourceCompression: Compression, targetCompression: Compression): Unit = {
    validateMessages(createTwoBatchedRecords(magic, sourceCompression), magic, sourceCompression.`type`(), targetCompression)
  }

  private def checkMismatchMagic(batchMagic: Byte, recordMagic: Byte, compression: Compression): Unit = {
    assertThrows(classOf[RecordValidationException],
      () => validateMessages(recordsWithInvalidInnerMagic(batchMagic, recordMagic, compression), batchMagic, compression.`type`(), compression)
    )
    assertEquals(metricsKeySet.count(_.getMBeanName.endsWith(s"${BrokerTopicStats.InvalidMagicNumberRecordsPerSec}")), 1)
    assertTrue(meterCount(s"${BrokerTopicStats.InvalidMagicNumberRecordsPerSec}") > 0)
  }

  private def validateMessages(records: MemoryRecords,
                               magic: Byte,
                               sourceCompressionType: CompressionType,
                               targetCompression: Compression): ValidationResult = {
    val mockTime = new MockTime(0L, 0L)
    new LogValidator(records,
      topicPartition,
      mockTime,
      sourceCompressionType,
      targetCompression,
      false,
      magic,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PRODUCER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.IBP_2_3_IV1
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier)
  }

  @Test
  def testLogAppendTimeNonCompressedV0(): Unit = {
    checkLogAppendTimeNonCompressed(RecordBatch.MAGIC_VALUE_V0)
  }


  @Test
  def testLogAppendTimeNonCompressedV1(): Unit = {
    checkLogAppendTimeNonCompressed(RecordBatch.MAGIC_VALUE_V1)
  }

  @Test
  def testLogAppendTimeNonCompressedV2(): Unit = {
    checkLogAppendTimeNonCompressed(RecordBatch.MAGIC_VALUE_V2)
  }

  private def checkLogAppendTimeNonCompressed(magic: Byte): Unit = {
    val mockTime = new MockTime
    // The timestamps should be overwritten
    val records = createRecords(magicValue = magic, timestamp = 1234L, codec = Compression.NONE)
    val offsetCounter = PrimitiveRef.ofLong(0)
    val validatedResults = new LogValidator(records,
      topicPartition,
      mockTime,
      CompressionType.NONE,
      Compression.NONE,
      false,
      magic,
      TimestampType.LOG_APPEND_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      offsetCounter,
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    )

    assertEquals(offsetCounter.value, records.records.asScala.size)
    val validatedRecords = validatedResults.validatedRecords
    assertEquals(records.records.asScala.size, validatedRecords.records.asScala.size, "message set size should not change")
    val now = mockTime.milliseconds
    if (magic >= RecordBatch.MAGIC_VALUE_V1)
      validatedRecords.batches.forEach(batch => validateLogAppendTime(now, 1234L, batch))
    assertEquals(if (magic == RecordBatch.MAGIC_VALUE_V0) RecordBatch.NO_TIMESTAMP else now, validatedResults.maxTimestampMs)
    assertFalse(validatedResults.messageSizeMaybeChanged, "Message size should not have been changed")

    // If it's LOG_APPEND_TIME, the offset will be the offset of the first record
    val expectedMaxTimestampOffset = magic match {
      case RecordBatch.MAGIC_VALUE_V0 => -1
      case RecordBatch.MAGIC_VALUE_V1 => 0
      case _ => 2
    }
    assertEquals(expectedMaxTimestampOffset, validatedResults.shallowOffsetOfMaxTimestamp)
    verifyRecordValidationStats(validatedResults.recordValidationStats, numConvertedRecords = 0, records,
      compressed = false)
  }

  @Test
  def testLogAppendTimeWithRecompressionV1(): Unit = {
    checkLogAppendTimeWithRecompression(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkLogAppendTimeWithRecompression(targetMagic: Byte): Unit = {
    val compression: Compression = Compression.gzip().build()
    val mockTime = new MockTime
    // The timestamps should be overwritten
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = compression)
    val validatedResults = new LogValidator(
      records,
      topicPartition,
      mockTime,
      CompressionType.GZIP,
      compression,
      false,
      targetMagic,
      TimestampType.LOG_APPEND_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0),
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    )

    val validatedRecords = validatedResults.validatedRecords
    assertEquals(records.records.asScala.size, validatedRecords.records.asScala.size,
      "message set size should not change")
    val now = mockTime.milliseconds()
    validatedRecords.batches.forEach(batch => validateLogAppendTime(now, -1, batch))
    assertTrue(validatedRecords.batches.iterator.next().isValid,
      "MessageSet should still valid")
    assertEquals(now, validatedResults.maxTimestampMs,
      s"Max timestamp should be $now")
    assertEquals(2, validatedResults.shallowOffsetOfMaxTimestamp,
      s"The shallow offset of max timestamp should be 2 if logAppendTime is used")
    assertTrue(validatedResults.messageSizeMaybeChanged,
      "Message size may have been changed")

    val stats = validatedResults.recordValidationStats
    verifyRecordValidationStats(stats, numConvertedRecords = 3, records, compressed = true)
  }

  @Test
  def testLogAppendTimeWithRecompressionV2(): Unit = {
    checkLogAppendTimeWithRecompression(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testLogAppendTimeWithoutRecompressionV1(): Unit = {
    checkLogAppendTimeWithoutRecompression(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkLogAppendTimeWithoutRecompression(magic: Byte): Unit = {
    val compression: Compression = Compression.gzip().build()
    val mockTime = new MockTime
    // The timestamps should be overwritten
    val records = createRecords(magicValue = magic, timestamp = 1234L, codec = compression)
    val validatedResults = new LogValidator(
      records,
      topicPartition,
      mockTime,
      CompressionType.GZIP,
      compression,
      false,
      magic,
      TimestampType.LOG_APPEND_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0),
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    val validatedRecords = validatedResults.validatedRecords

    assertEquals(records.records.asScala.size, validatedRecords.records.asScala.size,
      "message set size should not change")
    val now = mockTime.milliseconds()
    validatedRecords.batches.forEach(batch => validateLogAppendTime(now, 1234L, batch))
    assertTrue(validatedRecords.batches.iterator.next().isValid,
      "MessageSet should still valid")
    assertEquals(now, validatedResults.maxTimestampMs,
      s"Max timestamp should be $now")
    assertEquals(2, validatedResults.shallowOffsetOfMaxTimestamp,
      s"The shallow offset of max timestamp should be the last offset 2 if logAppendTime is used")
    assertFalse(validatedResults.messageSizeMaybeChanged,
      "Message size should not have been changed")

    verifyRecordValidationStats(validatedResults.recordValidationStats, numConvertedRecords = 0, records,
      compressed = true)
  }

  @Test
  def testInvalidOffsetRangeAndRecordCount(): Unit = {
    // The batch to be written contains 3 records, so the correct lastOffsetDelta is 2
    validateRecordBatchWithCountOverrides(lastOffsetDelta = 2, count = 3)

    // Count and offset range are inconsistent or invalid
    assertInvalidBatchCountOverrides(lastOffsetDelta = 0, count = 3)
    assertInvalidBatchCountOverrides(lastOffsetDelta = 15, count = 3)
    assertInvalidBatchCountOverrides(lastOffsetDelta = -3, count = 3)
    assertInvalidBatchCountOverrides(lastOffsetDelta = 2, count = -3)
    assertInvalidBatchCountOverrides(lastOffsetDelta = 2, count = 6)
    assertInvalidBatchCountOverrides(lastOffsetDelta = 2, count = 0)
    assertInvalidBatchCountOverrides(lastOffsetDelta = -3, count = -2)

    // Count and offset range are consistent, but do not match the actual number of records
    assertInvalidBatchCountOverrides(lastOffsetDelta = 5, count = 6)
    assertInvalidBatchCountOverrides(lastOffsetDelta = 1, count = 2)
  }

  private def assertInvalidBatchCountOverrides(lastOffsetDelta: Int, count: Int): Unit = {
    assertThrows(classOf[InvalidRecordException],
      () => validateRecordBatchWithCountOverrides(lastOffsetDelta, count))
  }

  private def validateRecordBatchWithCountOverrides(lastOffsetDelta: Int, count: Int): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = 1234L, codec = Compression.NONE)
    records.buffer.putInt(DefaultRecordBatch.RECORDS_COUNT_OFFSET, count)
    records.buffer.putInt(DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta)
    new LogValidator(
      records,
      topicPartition,
      time,
      CompressionType.GZIP,
      Compression.gzip().build(),
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.LOG_APPEND_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0),
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
  }

  @Test
  def testLogAppendTimeWithoutRecompressionV2(): Unit = {
    checkLogAppendTimeWithoutRecompression(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testNonCompressedV1(): Unit = {
    checkNonCompressed(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkNonCompressed(magic: Byte): Unit = {
    val now = System.currentTimeMillis()
    // set the timestamp of seq(1) (i.e. offset 1) as the max timestamp
    val timestampSeq = Seq(now - 1, now + 1, now)

    val (producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch) =
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        (1324L, 10.toShort, 984, true, 40)
      else
        (RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false,
          RecordBatch.NO_PARTITION_LEADER_EPOCH)

    val recordList = List(
      new SimpleRecord(timestampSeq(0), "hello".getBytes),
      new SimpleRecord(timestampSeq(1), "there".getBytes),
      new SimpleRecord(timestampSeq(2), "beautiful".getBytes)
    )

    val records = MemoryRecords.withRecords(magic, 0L, Compression.NONE, TimestampType.CREATE_TIME, producerId,
      producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional, recordList: _*)

    val offsetCounter = PrimitiveRef.ofLong(0)
    val validatingResults = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      magic,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      partitionLeaderEpoch,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      offsetCounter,
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    )

    val validatedRecords = validatingResults.validatedRecords

    var i = 0
    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      assertEquals(batch.timestampType, TimestampType.CREATE_TIME)
      maybeCheckBaseTimestamp(timestampSeq(0), batch)
      assertEquals(batch.maxTimestamp, batch.asScala.map(_.timestamp).max)
      assertEquals(producerEpoch, batch.producerEpoch)
      assertEquals(producerId, batch.producerId)
      assertEquals(baseSequence, batch.baseSequence)
      assertEquals(isTransactional, batch.isTransactional)
      assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch)
      for (record <- batch.asScala) {
        record.ensureValid()
        assertEquals(timestampSeq(i), record.timestamp)
        i += 1
      }
    }

    assertEquals(i, offsetCounter.value)
    assertEquals(now + 1, validatingResults.maxTimestampMs,
      s"Max timestamp should be ${now + 1}")

    // V2: Only one batch is in the records, so the shallow OffsetOfMaxTimestamp is the last offset of the single batch
    // V1: 3 batches are in the records, so the shallow OffsetOfMaxTimestamp is the timestamp of batch-1
    if (magic >= RecordBatch.MAGIC_VALUE_V2) {
      assertEquals(1, records.batches().asScala.size)
      assertEquals(2, validatingResults.shallowOffsetOfMaxTimestamp)
    } else {
      assertEquals(3, records.batches().asScala.size)
      assertEquals(1, validatingResults.shallowOffsetOfMaxTimestamp)
    }

    assertFalse(validatingResults.messageSizeMaybeChanged,
      "Message size should not have been changed")
    verifyRecordValidationStats(validatingResults.recordValidationStats, numConvertedRecords = 0, records,
      compressed = false)
  }

  @Test
  def testNonCompressedV2(): Unit = {
    checkNonCompressed(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testRecompressionV1(): Unit = {
    checkRecompression(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkRecompression(magic: Byte): Unit = {
    val now = System.currentTimeMillis()
    // set the timestamp of seq(1) (i.e. offset 1) as the max timestamp
    val timestampSeq = Seq(now - 1, now + 1, now)

    val (producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch) =
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        (1324L, 10.toShort, 984, true, 40)
      else
        (RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false,
          RecordBatch.NO_PARTITION_LEADER_EPOCH)

    val records = MemoryRecords.withRecords(magic, 0L, Compression.NONE, TimestampType.CREATE_TIME, producerId,
      producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional,
      new SimpleRecord(timestampSeq(0), "hello".getBytes),
      new SimpleRecord(timestampSeq(1), "there".getBytes),
      new SimpleRecord(timestampSeq(2), "beautiful".getBytes))

    // V2 has single batch, and other versions has many single-record batches
    assertEquals(if (magic >= RecordBatch.MAGIC_VALUE_V2) 1 else 3, records.batches().asScala.size)

    val validatingResults = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.gzip().build(),
      false,
      magic,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      partitionLeaderEpoch,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0),
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    val validatedRecords = validatingResults.validatedRecords

    var i = 0
    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      assertEquals(batch.timestampType, TimestampType.CREATE_TIME)
      maybeCheckBaseTimestamp(timestampSeq(0), batch)
      assertEquals(batch.maxTimestamp, batch.asScala.map(_.timestamp).max)
      assertEquals(producerEpoch, batch.producerEpoch)
      assertEquals(producerId, batch.producerId)
      assertEquals(baseSequence, batch.baseSequence)
      assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch)
      for (record <- batch.asScala) {
        record.ensureValid()
        assertEquals(timestampSeq(i), record.timestamp)
        i += 1
      }
    }
    assertEquals(now + 1, validatingResults.maxTimestampMs,
      s"Max timestamp should be ${now + 1}")

    // Both V2 and V1 has single batch in the validated records when compression is enable, and hence their shallow
    // OffsetOfMaxTimestamp is the last offset of the single batch
    assertEquals(1, validatedRecords.batches().asScala.size)
    assertEquals(2, validatingResults.shallowOffsetOfMaxTimestamp)
    assertTrue(validatingResults.messageSizeMaybeChanged,
      "Message size should have been changed")

    verifyRecordValidationStats(validatingResults.recordValidationStats, numConvertedRecords = 3, records,
      compressed = true)
  }

  @Test
  def testRecompressionV2(): Unit = {
    checkRecompression(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testCreateTimeUpConversionV0ToV1(): Unit = {
    checkCreateTimeUpConversionFromV0(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkCreateTimeUpConversionFromV0(toMagic: Byte): Unit = {
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = compression)
    val validatedResults = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      toMagic,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0),
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    val validatedRecords = validatedResults.validatedRecords

    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      maybeCheckBaseTimestamp(RecordBatch.NO_TIMESTAMP, batch)
      assertEquals(RecordBatch.NO_TIMESTAMP, batch.maxTimestamp)
      assertEquals(TimestampType.CREATE_TIME, batch.timestampType)
      assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch)
      assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId)
      assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence)
    }
    assertEquals(validatedResults.maxTimestampMs, RecordBatch.NO_TIMESTAMP,
      s"Max timestamp should be ${RecordBatch.NO_TIMESTAMP}")
    assertEquals(-1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertTrue(validatedResults.messageSizeMaybeChanged, "Message size should have been changed")

    verifyRecordValidationStats(validatedResults.recordValidationStats, numConvertedRecords = 3, records,
      compressed = true)
  }

  @Test
  def testCreateTimeUpConversionV0ToV2(): Unit = {
    checkCreateTimeUpConversionFromV0(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testCreateTimeUpConversionV1ToV2(): Unit = {
    val timestamp = System.currentTimeMillis()
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, codec = compression, timestamp = timestamp)
    val validatedResults = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting,
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0),
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    val validatedRecords = validatedResults.validatedRecords

    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      maybeCheckBaseTimestamp(timestamp, batch)
      assertEquals(timestamp, batch.maxTimestamp)
      assertEquals(TimestampType.CREATE_TIME, batch.timestampType)
      assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch)
      assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId)
      assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence)
    }
    assertEquals(timestamp, validatedResults.maxTimestampMs)
    assertEquals(2, validatedResults.shallowOffsetOfMaxTimestamp,
      s"Offset of max timestamp should be the last offset 2.")
    assertTrue(validatedResults.messageSizeMaybeChanged, "Message size should have been changed")

    verifyRecordValidationStats(validatedResults.recordValidationStats, numConvertedRecords = 3, records,
      compressed = true)
  }

  @Test
  def testCompressedV1(): Unit = {
    checkCompressed(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkCompressed(magic: Byte): Unit = {
    val now = System.currentTimeMillis()
    // set the timestamp of seq(1) (i.e. offset 1) as the max timestamp
    val timestampSeq = Seq(now - 1, now + 1, now)

    val (producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch) =
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        (1324L, 10.toShort, 984, true, 40)
      else
        (RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false,
          RecordBatch.NO_PARTITION_LEADER_EPOCH)

    val recordList = List(
      new SimpleRecord(timestampSeq(0), "hello".getBytes),
      new SimpleRecord(timestampSeq(1), "there".getBytes),
      new SimpleRecord(timestampSeq(2), "beautiful".getBytes)
    )

    val records = MemoryRecords.withRecords(magic, 0L, Compression.gzip().build(), TimestampType.CREATE_TIME, producerId,
      producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional, recordList: _*)

    val validatedResults = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      Compression.gzip().build(),
      false,
      magic,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      partitionLeaderEpoch,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0),
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    val validatedRecords = validatedResults.validatedRecords

    var i = 0
    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      assertEquals(batch.timestampType, TimestampType.CREATE_TIME)
      maybeCheckBaseTimestamp(timestampSeq(0), batch)
      assertEquals(batch.maxTimestamp, batch.asScala.map(_.timestamp).max)
      assertEquals(producerEpoch, batch.producerEpoch)
      assertEquals(producerId, batch.producerId)
      assertEquals(baseSequence, batch.baseSequence)
      assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch)
      for (record <- batch.asScala) {
        record.ensureValid()
        assertEquals(timestampSeq(i), record.timestamp)
        i += 1
      }
    }
    assertEquals(now + 1, validatedResults.maxTimestampMs, s"Max timestamp should be ${now + 1}")

    val expectedShallowOffsetOfMaxTimestamp = 2
    assertEquals(expectedShallowOffsetOfMaxTimestamp, validatedResults.shallowOffsetOfMaxTimestamp,
      s"Shallow offset of max timestamp should be 2")
    assertFalse(validatedResults.messageSizeMaybeChanged, "Message size should not have been changed")

    verifyRecordValidationStats(validatedResults.recordValidationStats, numConvertedRecords = 0, records,
      compressed = true)
  }
  @ParameterizedTest
  @CsvSource(Array("0,gzip", "1,gzip", "0,lz4", "1,lz4", "0,snappy", "1,snappy"))
  def testInvalidChecksum(code: Byte, compression: String): Unit = {
    checkInvalidChecksum(code, Compression.of(compression).build(), CompressionType.forName(compression))
  }

  private def checkInvalidChecksum(magic: Byte, compression: Compression , compressionType: CompressionType): Unit = {
    val record: LegacyRecord = LegacyRecord.create(magic, 0L, null, "hello".getBytes)
    val buf: ByteBuffer = record.buffer

    // enforce modify crc to make checksum error
    buf.put(LegacyRecord.CRC_OFFSET, 0.toByte)

    val buffer: ByteBuffer = ByteBuffer.allocate(1024)
    val builder: MemoryRecordsBuilder = MemoryRecords.builder(buffer, magic, compression,
      TimestampType.CREATE_TIME, 0L)
    builder.appendUncheckedWithOffset(0, record)

    val memoryRecords: MemoryRecords = builder.build
    val logValidator: LogValidator = new LogValidator(
      memoryRecords,
      topicPartition,
      time,
      compressionType,
      compression,
      false,
      magic,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    )

    assertThrows(classOf[CorruptRecordException], () =>
        logValidator.validateMessagesAndAssignOffsets(
          PrimitiveRef.ofLong(0),
          metricsRecorder,
          RequestLocal.withThreadConfinedCaching.bufferSupplier
        )
    )

    assertEquals(metricsKeySet.count(_.getMBeanName.endsWith(s"${BrokerTopicStats.InvalidMessageCrcRecordsPerSec}")), 1)
    assertTrue(meterCount(s"${BrokerTopicStats.InvalidMessageCrcRecordsPerSec}") > 0)
  }


  @Test
  def testCompressedV2(): Unit = {
    checkCompressed(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testInvalidCreateTimeNonCompressedV1(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now - 1001L,
      codec = Compression.NONE)
    assertThrows(classOf[RecordValidationException], () => new LogValidator(
      records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.MAGIC_VALUE_V1,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0),
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    ))
  }

  @Test
  def testInvalidCreateTimeNonCompressedV2(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now - 1001L,
      codec = Compression.NONE)
    assertThrows(classOf[RecordValidationException], () => new LogValidator(
      records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0),
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    ))
  }

  @Test
  def testInvalidCreateTimeCompressedV1(): Unit = {
    val now = System.currentTimeMillis()
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now - 1001L,
      codec = compression)
    assertThrows(classOf[RecordValidationException], () => new LogValidator(
      records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V1,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0),
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    ))
  }

  @Test
  def testInvalidCreateTimeCompressedV2(): Unit = {
    val now = System.currentTimeMillis()
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now - 1001L,
      codec = compression)
    assertThrows(classOf[RecordValidationException], () => new LogValidator(
      records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V1,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0),
      metricsRecorder,
      RequestLocal.withThreadConfinedCaching.bufferSupplier
    ))
  }

  @Test
  def testAbsoluteOffsetAssignmentNonCompressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = Compression.NONE)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.MAGIC_VALUE_V0,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting,
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords, offset)
  }

  @Test
  def testAbsoluteOffsetAssignmentCompressed(): Unit = {
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = compression)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V0,
      TimestampType.CREATE_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords, offset)
  }

  @Test
  def testRelativeOffsetAssignmentNonCompressedV1(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now, codec = Compression.NONE)
    val offset = 1234567
    checkOffsets(records, 0)
    val messageWithOffset = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.MAGIC_VALUE_V1,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords
    checkOffsets(messageWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentNonCompressedV2(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now, codec = Compression.NONE)
    val offset = 1234567
    checkOffsets(records, 0)
    val messageWithOffset = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords
    checkOffsets(messageWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentCompressedV1(): Unit = {
    val now = System.currentTimeMillis()
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now, codec = compression)
    val offset = 1234567
    checkOffsets(records, 0)
    val compressedMessagesWithOffset = new LogValidator(
      records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V1,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentCompressedV2(): Unit = {
    val now = System.currentTimeMillis()
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now, codec = compression)
    val offset = 1234567
    checkOffsets(records, 0)
    val compressedMessagesWithOffset = new LogValidator(
      records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV0ToV1NonCompressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = Compression.NONE)
    checkOffsets(records, 0)
    val offset = 1234567
    val validatedResults = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.MAGIC_VALUE_V1,
      TimestampType.LOG_APPEND_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    checkOffsets(validatedResults.validatedRecords, offset)
    verifyRecordValidationStats(validatedResults.recordValidationStats, numConvertedRecords = 3, records,
      compressed = false)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV0ToV2NonCompressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = Compression.NONE)
    checkOffsets(records, 0)
    val offset = 1234567
    val validatedResults = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.LOG_APPEND_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    checkOffsets(validatedResults.validatedRecords, offset)
    verifyRecordValidationStats(validatedResults.recordValidationStats, numConvertedRecords = 3, records,
      compressed = false)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV0ToV1Compressed(): Unit = {
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = compression)
    val offset = 1234567
    checkOffsets(records, 0)
    val validatedResults = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V1,
      TimestampType.LOG_APPEND_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    checkOffsets(validatedResults.validatedRecords, offset)
    verifyRecordValidationStats(validatedResults.recordValidationStats, numConvertedRecords = 3, records,
      compressed = true)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV0ToV2Compressed(): Unit = {
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = compression)
    val offset = 1234567
    checkOffsets(records, 0)
    val validatedResults = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.LOG_APPEND_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    checkOffsets(validatedResults.validatedRecords, offset)
    verifyRecordValidationStats(validatedResults.recordValidationStats, numConvertedRecords = 3, records,
      compressed = true)
  }

  @Test
  def testControlRecordsNotAllowedFromClients(): Unit = {
    val offset = 1234567
    val endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    val records = MemoryRecords.withEndTransactionMarker(23423L, 5, endTxnMarker)
    assertThrows(classOf[InvalidRecordException], () => new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.CURRENT_MAGIC_VALUE,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ))
  }

  @Test
  def testControlRecordsNotCompressed(): Unit = {
    val offset = 1234567
    val endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    val records = MemoryRecords.withEndTransactionMarker(23423L, 5, endTxnMarker)
    val result = new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.snappy().build(),
      false,
      RecordBatch.CURRENT_MAGIC_VALUE,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.COORDINATOR,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    val batches = TestUtils.toList(result.validatedRecords.batches)
    assertEquals(1, batches.size)
    val batch = batches.get(0)
    assertFalse(batch.isCompressed)
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV1ToV0NonCompressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V1, now, codec = Compression.NONE)
    checkOffsets(records, 0)
    checkOffsets(new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.MAGIC_VALUE_V0,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV1ToV0Compressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V1, now, compression)
    checkOffsets(records, 0)
    checkOffsets(new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V0,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV1ToV2NonCompressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, codec = Compression.NONE)
    checkOffsets(records, 0)
    val offset = 1234567
    checkOffsets(new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.LOG_APPEND_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV1ToV2Compressed(): Unit = {
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, codec = compression)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.LOG_APPEND_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV2ToV1NonCompressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, codec = Compression.NONE)
    checkOffsets(records, 0)
    checkOffsets(new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.MAGIC_VALUE_V1,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV2ToV1Compressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, compression)
    checkOffsets(records, 0)
    checkOffsets(new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V1,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords, offset)
  }

  @Test
  def testDownConversionOfTransactionalRecordsNotPermitted(): Unit = {
    val offset = 1234567
    val producerId = 1344L
    val producerEpoch = 16.toShort
    val sequence = 0
    val records = MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
      new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes), new SimpleRecord("beautiful".getBytes))
    assertThrows(classOf[UnsupportedForMessageFormatException], () => new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      Compression.gzip().build(),
      false,
      RecordBatch.MAGIC_VALUE_V1,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ))
  }

  @Test
  def testDownConversionOfIdempotentRecordsNotPermitted(): Unit = {
    val offset = 1234567
    val producerId = 1344L
    val producerEpoch = 16.toShort
    val sequence = 0
    val records = MemoryRecords.withIdempotentRecords(Compression.NONE, producerId, producerEpoch, sequence,
      new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes), new SimpleRecord("beautiful".getBytes))
    assertThrows(classOf[UnsupportedForMessageFormatException], () => new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      Compression.gzip().build(),
      false,
      RecordBatch.MAGIC_VALUE_V1,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ))
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV2ToV0NonCompressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, codec = Compression.NONE)
    checkOffsets(records, 0)
    checkOffsets(new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.NONE,
      false,
      RecordBatch.MAGIC_VALUE_V0,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV2ToV0Compressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, compression)
    checkOffsets(records, 0)
    checkOffsets(new LogValidator(records,
      topicPartition,
      time,
      CompressionType.GZIP,
      compression,
      false,
      RecordBatch.MAGIC_VALUE_V0,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ).validatedRecords, offset)
  }

  @Test
  def testNonIncreasingOffsetRecordBatchHasMetricsLogged(): Unit = {
    val records = createNonIncreasingOffsetRecords(RecordBatch.MAGIC_VALUE_V2)
    records.batches().asScala.head.setLastOffset(2)
    assertThrows(classOf[InvalidRecordException], () => new LogValidator(records,
        topicPartition,
        time,
        CompressionType.GZIP,
        Compression.gzip().build(),
        false,
        RecordBatch.MAGIC_VALUE_V0,
        TimestampType.CREATE_TIME,
        5000L,
        5000L,
        RecordBatch.NO_PARTITION_LEADER_EPOCH,
        AppendOrigin.CLIENT,
        MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ))
    assertEquals(metricsKeySet.count(_.getMBeanName.endsWith(s"${BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec}")), 1)
    assertTrue(meterCount(s"${BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec}") > 0)
  }

  @Test
  def testCompressedBatchWithoutRecordsNotAllowed(): Unit = {
    testBatchWithoutRecordsNotAllowed(CompressionType.GZIP, Compression.gzip().build())
  }

  @Test
  def testZStdCompressedWithUnavailableIBPVersion(): Unit = {
    // The timestamps should be overwritten
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = 1234L, codec = Compression.NONE)
    assertThrows(classOf[UnsupportedCompressionTypeException], () => new LogValidator(records,
      topicPartition,
      time,
      CompressionType.NONE,
      Compression.zstd().build(),
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.LOG_APPEND_TIME,
      1000L,
      1000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.IBP_2_0_IV1
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ))
  }

  @Test
  def testUncompressedBatchWithoutRecordsNotAllowed(): Unit = {
    testBatchWithoutRecordsNotAllowed(CompressionType.NONE, Compression.NONE)
  }

  @Test
  def testRecompressedBatchWithoutRecordsNotAllowed(): Unit = {
    testBatchWithoutRecordsNotAllowed(CompressionType.NONE, Compression.gzip().build())
  }

  @Test
  def testInvalidTimestampExceptionHasBatchIndex(): Unit = {
    val now = System.currentTimeMillis()
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now - 1001L,
      codec = compression)
    val e = assertThrows(classOf[RecordValidationException],
      () => new LogValidator(
        records,
        topicPartition,
        time,
        CompressionType.GZIP,
        compression,
        false,
        RecordBatch.MAGIC_VALUE_V1,
        TimestampType.CREATE_TIME,
        1000L,
        1000L,
        RecordBatch.NO_PARTITION_LEADER_EPOCH,
        AppendOrigin.CLIENT,
        MetadataVersion.latestTesting
      ).validateMessagesAndAssignOffsets(
        PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
      )
    )

    assertTrue(e.invalidException.isInstanceOf[InvalidTimestampException])
    assertFalse(e.recordErrors.isEmpty)
    assertEquals(e.recordErrors.size, 3)
  }

  @Test
  def testInvalidRecordExceptionHasBatchIndex(): Unit = {
    val e = assertThrows(classOf[RecordValidationException],
      () => {
        val compression: Compression = Compression.gzip().build()
        validateMessages(recordsWithInvalidInnerMagic(
          RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, compression),
          RecordBatch.MAGIC_VALUE_V0, CompressionType.GZIP, compression)
      }
    )

    assertTrue(e.invalidException.isInstanceOf[InvalidRecordException])
    assertFalse(e.recordErrors.isEmpty)
    // recordsWithInvalidInnerMagic creates 20 records
    assertEquals(e.recordErrors.size, 20)
    e.recordErrors.asScala.foreach(assertNotNull(_))
  }

  @Test
  def testBatchWithInvalidRecordsAndInvalidTimestamp(): Unit = {
    val compression: Compression = Compression.gzip().build()
    val records = (0 until 5).map(id =>
      LegacyRecord.create(RecordBatch.MAGIC_VALUE_V0, 0L, null, id.toString.getBytes())
    )

    val buffer = ByteBuffer.allocate(1024)
    val builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, compression,
      TimestampType.CREATE_TIME, 0L)
    var offset = 0

    // we want to mix in a record with invalid timestamp range
    builder.appendUncheckedWithOffset(offset, LegacyRecord.create(RecordBatch.MAGIC_VALUE_V1,
      1200L, null, "timestamp".getBytes))
    records.foreach { record =>
      offset += 30
      builder.appendUncheckedWithOffset(offset, record)
    }
    val invalidOffsetTimestampRecords = builder.build()

    val e = assertThrows(classOf[RecordValidationException],
      () => validateMessages(invalidOffsetTimestampRecords,
        RecordBatch.MAGIC_VALUE_V0, CompressionType.GZIP, compression)
    )
    // if there is a mix of both regular InvalidRecordException and InvalidTimestampException,
    // InvalidTimestampException takes precedence
    assertTrue(e.invalidException.isInstanceOf[InvalidTimestampException])
    assertFalse(e.recordErrors.isEmpty)
    assertEquals(6, e.recordErrors.size)
  }

  @Test
  def testRecordWithPastTimestampIsRejected(): Unit = {
    val timestampBeforeMaxConfig = 24 * 60 * 60 * 1000L //24 hrs
    val timestampAfterMaxConfig = 1 * 60 * 60 * 1000L //1 hr
    val now = System.currentTimeMillis()
    val fiveMinutesBeforeThreshold = now - timestampBeforeMaxConfig - (5 * 60 * 1000L)
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = fiveMinutesBeforeThreshold,
      codec = compression)
    val e = assertThrows(classOf[RecordValidationException],
      () => new LogValidator(
        records,
        topicPartition,
        time,
        CompressionType.GZIP,
        compression,
        false,
        RecordBatch.MAGIC_VALUE_V2,
        TimestampType.CREATE_TIME,
        timestampBeforeMaxConfig,
        timestampAfterMaxConfig,
        RecordBatch.NO_PARTITION_LEADER_EPOCH,
        AppendOrigin.CLIENT,
        MetadataVersion.latestTesting
      ).validateMessagesAndAssignOffsets(
        PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
      )
    )

    assertTrue(e.invalidException.isInstanceOf[InvalidTimestampException])
    assertFalse(e.recordErrors.isEmpty)
    assertEquals(e.recordErrors.size, 3)
  }


  @Test
  def testRecordWithFutureTimestampIsRejected(): Unit = {
    val timestampBeforeMaxConfig = 24 * 60 * 60 * 1000L //24 hrs
    val timestampAfterMaxConfig = 1 * 60 * 60 * 1000L //1 hr
    val now = System.currentTimeMillis()
    val fiveMinutesAfterThreshold = now + timestampAfterMaxConfig + (5 * 60 * 1000L)
    val compression: Compression = Compression.gzip().build()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = fiveMinutesAfterThreshold,
      codec = compression)
    val e = assertThrows(classOf[RecordValidationException],
      () => new LogValidator(
        records,
        topicPartition,
        time,
        CompressionType.GZIP,
        compression,
        false,
        RecordBatch.MAGIC_VALUE_V2,
        TimestampType.CREATE_TIME,
        timestampBeforeMaxConfig,
        timestampAfterMaxConfig,
        RecordBatch.NO_PARTITION_LEADER_EPOCH,
        AppendOrigin.CLIENT,
        MetadataVersion.latestTesting
      ).validateMessagesAndAssignOffsets(
        PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
      )
    )

    assertTrue(e.invalidException.isInstanceOf[InvalidTimestampException])
    assertFalse(e.recordErrors.isEmpty)
    assertEquals(e.recordErrors.size, 3)
  }

  @Test
  def testDifferentLevelDoesNotCauseRecompression(): Unit = {
    val records = List(
      List.fill(256)("some").mkString("").getBytes,
      List.fill(256)("data").mkString("").getBytes
    )
    // Records from the producer were created with gzip max level
    val gzipMax: Compression = Compression.gzip().level(CompressionType.GZIP.maxLevel()).build()
    val recordsGzipMax = createRecords(records, RecordBatch.MAGIC_VALUE_V2, RecordBatch.NO_TIMESTAMP, gzipMax)

    // The topic is configured with gzip min level
    val gzipMin: Compression = Compression.gzip().level(CompressionType.GZIP.minLevel()).build()
    val recordsGzipMin = createRecords(records, RecordBatch.MAGIC_VALUE_V2, RecordBatch.NO_TIMESTAMP, gzipMin)

    // ensure data compressed with gzip max and min is different
    assertNotEquals(recordsGzipMax, recordsGzipMin)
    val validator = new LogValidator(recordsGzipMax,
      topicPartition,
      time,
      gzipMax.`type`(),
      gzipMin,
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    )
    val result = validator.validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    // ensure validated records have not been changed so they are the same as the producer records
    assertEquals(recordsGzipMax, result.validatedRecords)
    assertNotEquals(recordsGzipMin, result.validatedRecords)
  }

  @Test
  def testDifferentCodecCausesRecompression(): Unit = {
    val records = List(
      List.fill(256)("some").mkString("").getBytes,
      List.fill(256)("data").mkString("").getBytes
    )
    // Records from the producer were created with gzip max level
    val gzipMax: Compression = Compression.gzip().level(CompressionType.GZIP.maxLevel()).build()
    val recordsGzipMax = createRecords(records, RecordBatch.MAGIC_VALUE_V2, RecordBatch.NO_TIMESTAMP, gzipMax)

    // The topic is configured with lz4 min level
    val lz4Min: Compression = Compression.lz4().level(CompressionType.LZ4.minLevel()).build()
    val recordsLz4Min = createRecords(records, RecordBatch.MAGIC_VALUE_V2, RecordBatch.NO_TIMESTAMP, lz4Min)

    val validator = new LogValidator(recordsGzipMax,
      topicPartition,
      time,
      gzipMax.`type`(),
      lz4Min,
      false,
      RecordBatch.MAGIC_VALUE_V2,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    )
    val result = validator.validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(0L), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    )
    // ensure validated records have been recompressed and match lz4 min level
    assertEquals(recordsLz4Min, result.validatedRecords)
  }

  private def testBatchWithoutRecordsNotAllowed(sourceCompression: CompressionType, targetCompression: Compression): Unit = {
    val offset = 1234567
    val (producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch) =
      (1324L, 10.toShort, 984, true, 40)
    val buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD)
    DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.CURRENT_MAGIC_VALUE, producerId, producerEpoch,
      baseSequence, 0L, 5L, partitionLeaderEpoch, TimestampType.CREATE_TIME, System.currentTimeMillis(),
      isTransactional, false)
    buffer.flip()
    val records = MemoryRecords.readableRecords(buffer)
    assertThrows(classOf[InvalidRecordException], () => new LogValidator(records,
      topicPartition,
      time,
      sourceCompression,
      targetCompression,
      false,
      RecordBatch.CURRENT_MAGIC_VALUE,
      TimestampType.CREATE_TIME,
      5000L,
      5000L,
      RecordBatch.NO_PARTITION_LEADER_EPOCH,
      AppendOrigin.CLIENT,
      MetadataVersion.latestTesting
    ).validateMessagesAndAssignOffsets(
      PrimitiveRef.ofLong(offset), metricsRecorder, RequestLocal.withThreadConfinedCaching.bufferSupplier
    ))
  }

  private def createRecords(magicValue: Byte,
                            timestamp: Long = RecordBatch.NO_TIMESTAMP,
                            codec: Compression): MemoryRecords = {
    val records = List("hello".getBytes, "there".getBytes, "beautiful".getBytes)
    createRecords(records = records, magicValue = magicValue, timestamp = timestamp, codec = codec)
  }

  private def createRecords(records: List[Array[Byte]],
                            magicValue: Byte,
                            timestamp: Long,
                            codec: Compression): MemoryRecords = {
    val buf = ByteBuffer.allocate(512)
    val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L)
    records.indices.foreach { offset =>
      builder.appendWithOffset(offset, timestamp, null, records(offset))
    }
    builder.build()
  }

  private def createNonIncreasingOffsetRecords(magicValue: Byte,
                                               timestamp: Long = RecordBatch.NO_TIMESTAMP,
                                               codec: Compression = Compression.NONE): MemoryRecords = {
    val buf = ByteBuffer.allocate(512)
    val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L)
    builder.appendWithOffset(0, timestamp, null, "hello".getBytes)
    builder.appendWithOffset(2, timestamp, null, "there".getBytes)
    builder.appendWithOffset(3, timestamp, null, "beautiful".getBytes)
    builder.build()
  }

  private def createTwoBatchedRecords(magicValue: Byte, codec: Compression): MemoryRecords = {
    val buf = ByteBuffer.allocate(2048)
    var builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L)
    builder.append(10L, "1".getBytes(), "a".getBytes())
    builder.close()
    builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 1L)
    builder.append(11L, "2".getBytes(), "b".getBytes())
    builder.append(12L, "3".getBytes(), "c".getBytes())
    builder.close()

    buf.flip()
    MemoryRecords.readableRecords(buf.slice())
  }

  /* check that offsets are assigned consecutively from the given base offset */
  def checkOffsets(records: MemoryRecords, baseOffset: Long): Unit = {
    assertTrue(records.records.asScala.nonEmpty, "Message set should not be empty")
    var offset = baseOffset
    for (entry <- records.records.asScala) {
      assertEquals(offset, entry.offset, "Unexpected offset in message set iterator")
      offset += 1
    }
  }

  private def recordsWithNonSequentialInnerOffsets(magicValue: Byte,
                                                   compression: Compression,
                                                   numRecords: Int): MemoryRecords = {
    val records = (0 until numRecords).map { id =>
      new SimpleRecord(id.toString.getBytes)
    }

    val buffer = ByteBuffer.allocate(1024)
    val builder = MemoryRecords.builder(buffer, magicValue, compression, TimestampType.CREATE_TIME, 0L)

    records.foreach { record =>
      builder.appendUncheckedWithOffset(0, record)
    }

    builder.build()
  }

  private def recordsWithInvalidInnerMagic(batchMagicValue: Byte,
                                           recordMagicValue: Byte,
                                           codec: Compression): MemoryRecords = {
    val records = (0 until 20).map(id =>
      LegacyRecord.create(recordMagicValue,
        RecordBatch.NO_TIMESTAMP,
        id.toString.getBytes,
        id.toString.getBytes))

    val buffer = ByteBuffer.allocate(math.min(math.max(records.map(_.sizeInBytes()).sum / 2, 1024), 1 << 16))
    val builder = MemoryRecords.builder(buffer, batchMagicValue, codec,
      TimestampType.CREATE_TIME, 0L)

    var offset = 1234567
    records.foreach { record =>
      builder.appendUncheckedWithOffset(offset, record)
      offset += 1
    }

    builder.build()
  }

  def maybeCheckBaseTimestamp(expected: Long, batch: RecordBatch): Unit = {
    batch match {
      case b: DefaultRecordBatch =>
        assertEquals(expected, b.baseTimestamp, s"Unexpected base timestamp of batch $batch")
      case _ => // no-op
    }
  }

  /**
    * expectedLogAppendTime is only checked if batch.magic is V2 or higher
    */
  def validateLogAppendTime(expectedLogAppendTime: Long, expectedBaseTimestamp: Long, batch: RecordBatch): Unit = {
    assertTrue(batch.isValid)
    assertTrue(batch.timestampType == TimestampType.LOG_APPEND_TIME)
    assertEquals(expectedLogAppendTime, batch.maxTimestamp, s"Unexpected max timestamp of batch $batch")
    maybeCheckBaseTimestamp(expectedBaseTimestamp, batch)
    for (record <- batch.asScala) {
      record.ensureValid()
      assertEquals(expectedLogAppendTime, record.timestamp, s"Unexpected timestamp of record $record")
    }
  }

  def verifyRecordValidationStats(stats: RecordValidationStats, numConvertedRecords: Int, records: MemoryRecords,
                                  compressed: Boolean): Unit = {
    assertNotNull(stats, "Records processing info is null")
    assertEquals(numConvertedRecords, stats.numRecordsConverted)
    if (numConvertedRecords > 0) {
      assertTrue(stats.conversionTimeNanos >= 0, s"Conversion time not recorded $stats")
      assertTrue(stats.conversionTimeNanos <= TimeUnit.MINUTES.toNanos(1), s"Conversion time not valid $stats")
    }
    val originalSize = records.sizeInBytes
    val tempBytes = stats.temporaryMemoryBytes
    if (numConvertedRecords > 0 && compressed)
      assertTrue(tempBytes > originalSize, s"Temp bytes too small, orig=$originalSize actual=$tempBytes")
    else if (numConvertedRecords > 0 || compressed)
      assertTrue(tempBytes > 0, "Temp bytes not updated")
    else
      assertEquals(0, tempBytes)
  }
}
