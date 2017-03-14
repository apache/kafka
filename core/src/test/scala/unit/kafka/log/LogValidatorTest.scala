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

import kafka.common.LongRef
import kafka.message._
import org.apache.kafka.common.errors.InvalidTimestampException
import org.apache.kafka.common.record._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._

class LogValidatorTest extends JUnitSuite {

  @Test
  def testLogAppendTimeNonCompressedV1() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = 0L, codec = CompressionType.NONE)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords
    assertEquals("message set size should not change", records.records.asScala.size, validatedRecords.records.asScala.size)
    validatedRecords.batches.asScala.foreach(batch => validateLogAppendTime(now, batch))
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be 0", 0, validatedResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeNonCompressedV2() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = 0L, codec = CompressionType.NONE)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V2,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords
    assertEquals("message set size should not change", records.records.asScala.size, validatedRecords.records.asScala.size)
    validatedRecords.batches.asScala.foreach(batch => validateLogAppendTime(now, batch))
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals("The offset of max timestamp should be 0", 0, validatedResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeWithRecompressionV1() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords

    assertEquals("message set size should not change", records.records.asScala.size, validatedRecords.records.asScala.size)
    validatedRecords.batches.asScala.foreach(batch => validateLogAppendTime(now, batch))
    assertTrue("MessageSet should still valid", validatedRecords.batches.iterator.next().isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${records.records.asScala.size - 1}",
      records.records.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertTrue("Message size may have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeWithRecompressionV2() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V2,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords

    assertEquals("message set size should not change", records.records.asScala.size, validatedRecords.records.asScala.size)
    validatedRecords.batches.asScala.foreach(batch => validateLogAppendTime(now, batch))
    assertTrue("MessageSet should still valid", validatedRecords.batches.iterator.next().isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${records.records.asScala.size - 1}",
      records.records.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertTrue("Message size may have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeWithoutRecompression() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = 0L, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords

    assertEquals("message set size should not change", records.records.asScala.size,
      validatedRecords.records.asScala.size)
    validatedRecords.batches.asScala.foreach(batch => validateLogAppendTime(now, batch))
    assertTrue("MessageSet should still valid", validatedRecords.batches.iterator.next().isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${records.records.asScala.size - 1}",
      records.records.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeNonCompressedV1() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val records =
      MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, CompressionType.NONE,
        new KafkaRecord(timestampSeq(0), "hello".getBytes),
        new KafkaRecord(timestampSeq(1), "there".getBytes),
        new KafkaRecord(timestampSeq(2), "beautiful".getBytes))

    val validatingResults = LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatingResults.validatedRecords

    var i = 0
    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      assertEquals(batch.timestampType, TimestampType.CREATE_TIME)
      assertEquals(batch.maxTimestamp, batch.asScala.map(_.timestamp).max)
      for (record <- batch.asScala) {
        assertTrue(record.isValid)
        assertEquals(timestampSeq(i), record.timestamp)
        i += 1
      }
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be 1", 1, validatingResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeUpConversion() {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
        offsetCounter = new LongRef(0),
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
        messageTimestampType = TimestampType.CREATE_TIME,
        messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords

    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      assertEquals(RecordBatch.NO_TIMESTAMP, batch.maxTimestamp)
      assertEquals(TimestampType.CREATE_TIME, batch.timestampType)
    }
    assertEquals(s"Max timestamp should be ${RecordBatch.NO_TIMESTAMP}", RecordBatch.NO_TIMESTAMP, validatedResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedRecords.records.asScala.size - 1}",
      validatedRecords.records.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertTrue("Message size should have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeNonCompressedV2() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val buf = ByteBuffer.allocate(512)
    val builder = MemoryRecords.builder(buf, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, 0L)
    builder.appendWithOffset(0, timestampSeq(0), null, "hello".getBytes)
    builder.appendWithOffset(1, timestampSeq(1), null, "there".getBytes)
    builder.appendWithOffset(2, timestampSeq(2), null, "beautiful".getBytes)
    val records = builder.build()

    val validatingResults = LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V2,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatingResults.validatedRecords

    var i = 0
    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      assertEquals(batch.timestampType, TimestampType.CREATE_TIME)
      assertEquals(batch.maxTimestamp, batch.asScala.map(_.timestamp).max)
      for (record <- batch.asScala) {
        assertTrue(record.isValid)
        assertEquals(record.timestamp, timestampSeq(i))
        i += 1
      }
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be 1", 1, validatingResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeCompressedV1() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val records =
      MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, CompressionType.GZIP,
        new KafkaRecord(timestampSeq(0), "hello".getBytes),
        new KafkaRecord(timestampSeq(1), "there".getBytes),
        new KafkaRecord(timestampSeq(2), "beautiful".getBytes))

    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
        offsetCounter = new LongRef(0),
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
        messageTimestampType = TimestampType.CREATE_TIME,
        messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords

    var i = 0
    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      assertEquals(batch.timestampType, TimestampType.CREATE_TIME)
      assertEquals(batch.maxTimestamp, batch.asScala.map(_.timestamp).max)
      for (record <- batch.asScala) {
        assertTrue(record.isValid)
        assertEquals(timestampSeq(i), record.timestamp)
        i += 1
      }
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatedResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedRecords.records.asScala.size - 1}",
      validatedRecords.records.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeCompressedV2() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val buf = ByteBuffer.allocate(512)
    val builder = MemoryRecords.builder(buf, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, 0L)
    builder.appendWithOffset(0, timestampSeq(0), null, "hello".getBytes)
    builder.appendWithOffset(1, timestampSeq(1), null, "there".getBytes)
    builder.appendWithOffset(2, timestampSeq(2), null, "beautiful".getBytes)
    val records = builder.build()

    val validatedResults =
      LogValidator.validateMessagesAndAssignOffsets(records,
        offsetCounter = new LongRef(0),
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = RecordBatch.MAGIC_VALUE_V2,
        messageTimestampType = TimestampType.CREATE_TIME,
        messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords

    var i = 0
    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      assertEquals(batch.timestampType, TimestampType.CREATE_TIME)
      assertEquals(batch.maxTimestamp, batch.asScala.map(_.timestamp).max)
      for (record <- batch.asScala) {
        assertTrue(record.isValid)
        assertEquals(record.timestamp, timestampSeq(i))
        i += 1
      }
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatedResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedRecords.records.asScala.size - 1}",
      validatedRecords.records.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test(expected = classOf[InvalidTimestampException])
  def testInvalidCreateTimeNonCompressed() {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now - 1001L,
      codec = CompressionType.NONE)
    LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
  }

  @Test(expected = classOf[InvalidTimestampException])
  def testInvalidCreateTimeCompressed() {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now - 1001L,
      codec = CompressionType.GZIP)
    LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
  }
  @Test
  def testAbsoluteOffsetAssignmentNonCompressed() {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)
  }

  @Test
  def testAbsoluteOffsetAssignmentCompressed() {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)
  }

  @Test
  def testRelativeOffsetAssignmentNonCompressedV1() {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(records, 0)
    val messageWithOffset = LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords
    checkOffsets(messageWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentNonCompressedV2() {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(records, 0)
    val messageWithOffset = LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V2,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords
    checkOffsets(messageWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentCompressedV1() {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    val compressedMessagesWithOffset = LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentCompressedV2() {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    val compressedMessagesWithOffset = LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V2,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV0NonCompressed() {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.NONE)
    checkOffsets(records, 0)
    val offset = 1234567
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV0Compressed() {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV1NonCompressed() {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V1, now, codec = CompressionType.NONE)
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV1Compressed() {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V1, now, CompressionType.GZIP)
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords, offset)
  }

  @Test(expected = classOf[InvalidRecordException])
  def testInvalidInnerMagicVersion(): Unit = {
    val offset = 1234567
    val records = recordsWithInvalidInnerMagic(offset)
    LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = SnappyCompressionCodec,
      targetCodec = SnappyCompressionCodec,
      messageFormatVersion = RecordBatch.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L)
  }

  private def createRecords(magicValue: Byte = Message.CurrentMagicValue,
                            timestamp: Long = Message.NoTimestamp,
                            codec: CompressionType = CompressionType.NONE): MemoryRecords = {
    val buf = ByteBuffer.allocate(512)
    val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L)
    builder.appendWithOffset(0, timestamp, null, "hello".getBytes)
    builder.appendWithOffset(1, timestamp, null, "there".getBytes)
    builder.appendWithOffset(2, timestamp, null, "beautiful".getBytes)
    builder.build()
  }

  /* check that offsets are assigned consecutively from the given base offset */
  def checkOffsets(records: MemoryRecords, baseOffset: Long) {
    assertTrue("Message set should not be empty", records.records.asScala.nonEmpty)
    var offset = baseOffset
    for (entry <- records.records.asScala) {
      assertEquals("Unexpected offset in message set iterator", offset, entry.offset)
      offset += 1
    }
  }

  private def recordsWithInvalidInnerMagic(initialOffset: Long): MemoryRecords = {
    val records = (0 until 20).map(id =>
      LegacyRecord.create(RecordBatch.MAGIC_VALUE_V0,
        RecordBatch.NO_TIMESTAMP,
        id.toString.getBytes,
        id.toString.getBytes))

    val buffer = ByteBuffer.allocate(math.min(math.max(records.map(_.sizeInBytes()).sum / 2, 1024), 1 << 16))
    val builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, CompressionType.GZIP,
      TimestampType.CREATE_TIME, 0L)

    var offset = initialOffset
    records.foreach { record =>
      builder.appendUncheckedWithOffset(offset, record)
      offset += 1
    }

    builder.build()
  }

  def validateLogAppendTime(now: Long, entry: RecordBatch) {
    assertTrue(entry.isValid)
    assertTrue(entry.timestampType() == TimestampType.LOG_APPEND_TIME)
    assertEquals(s"Timestamp of message $entry should be $now", now, entry.maxTimestamp)
    for (record <- entry.asScala) {
      assertTrue(record.isValid)
      assertEquals(s"Timestamp of message $record should be $now", now, record.timestamp)
    }
  }

}
