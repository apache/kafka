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
  def testLogAppendTimeNonCompressed() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V1, timestamp = 0L, codec = CompressionType.NONE)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords
    assertEquals("number of messages should not change", records.deepEntries.asScala.size, validatedRecords.deepEntries.asScala.size)
    validatedRecords.deepEntries.asScala.foreach(logEntry => validateLogAppendTime(now, logEntry.record))
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be 0", 0, validatedResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeWithRecompression() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords

    assertEquals("number of messages should not change", records.deepEntries.asScala.size, validatedRecords.deepEntries.asScala.size)
    validatedRecords.deepEntries.asScala.foreach(logEntry => validateLogAppendTime(now, logEntry.record))
    assertTrue("MessageSet should still valid", validatedRecords.shallowEntries.iterator.next().record.isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${records.deepEntries.asScala.size - 1}",
      records.deepEntries.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertTrue("Message size may have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeWithoutRecompression() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V1, timestamp = 0L, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords

    assertEquals("number of messages should not change", records.deepEntries.asScala.size,
      validatedRecords.deepEntries.asScala.size)
    validatedRecords.deepEntries.asScala.foreach(logEntry => validateLogAppendTime(now, logEntry.record))
    assertTrue("MessageSet should still valid", validatedRecords.shallowEntries.iterator.next().record.isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${records.deepEntries.asScala.size - 1}",
      records.deepEntries.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeNonCompressed() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val records = MemoryRecords.withRecords(CompressionType.NONE,
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(0), "hello".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(1), "there".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(2), "beautiful".getBytes))

    val validatingResults = LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatingResults.validatedRecords

    var i = 0
    for (logEntry <- validatedRecords.deepEntries.asScala) {
      assertTrue(logEntry.record.isValid)
      assertEquals(timestampSeq(i), logEntry.record.timestamp)
      assertEquals(TimestampType.CREATE_TIME, logEntry.record.timestampType)
      i += 1
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be 1", 1, validatingResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeUpConversion() {
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
        offsetCounter = new LongRef(0),
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = Record.MAGIC_VALUE_V1,
        messageTimestampType = TimestampType.CREATE_TIME,
        messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords

    for (logEntry <- validatedRecords.deepEntries.asScala) {
      assertTrue(logEntry.record.isValid)
      assertEquals(Record.NO_TIMESTAMP, logEntry.record.timestamp)
      assertEquals(TimestampType.CREATE_TIME, logEntry.record.timestampType)
    }
    assertEquals(s"Max timestamp should be ${Record.NO_TIMESTAMP}", Record.NO_TIMESTAMP, validatedResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedRecords.deepEntries.asScala.size - 1}",
      validatedRecords.deepEntries.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertTrue("Message size should have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeCompressed() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val records = MemoryRecords.withRecords(CompressionType.GZIP,
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(0), "hello".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(1), "there".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(2), "beautiful".getBytes))

    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
        offsetCounter = new LongRef(0),
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = Record.MAGIC_VALUE_V1,
        messageTimestampType = TimestampType.CREATE_TIME,
        messageTimestampDiffMaxMs = 1000L)
    val validatedRecords = validatedResults.validatedRecords

    var i = 0
    for (logEntry <- validatedRecords.deepEntries.asScala) {
      assertTrue(logEntry.record.isValid)
      assertEquals(timestampSeq(i), logEntry.record.timestamp)
      assertEquals(TimestampType.CREATE_TIME, logEntry.record.timestampType)
      i += 1
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatedResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedRecords.deepEntries.asScala.size - 1}",
      validatedRecords.deepEntries.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test(expected = classOf[InvalidTimestampException])
  def testInvalidCreateTimeNonCompressed() {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V1, timestamp = now - 1001L,
      codec = CompressionType.NONE)
    LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
  }

  @Test(expected = classOf[InvalidTimestampException])
  def testInvalidCreateTimeCompressed() {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V1, timestamp = now - 1001L,
      codec = CompressionType.GZIP)
    LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
  }
  @Test
  def testAbsoluteOffsetAssignmentNonCompressed() {
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)
  }

  @Test
  def testAbsoluteOffsetAssignmentCompressed() {
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)
  }

  @Test
  def testRelativeOffsetAssignmentNonCompressed() {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V1, timestamp = now, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(records, 0)
    val messageWithOffset = LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords
    checkOffsets(messageWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentCompressed() {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V1, timestamp = now, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    val compressedMessagesWithOffset = LogValidator.validateMessagesAndAssignOffsets(
      records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV0NonCompressed() {
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.NONE)
    checkOffsets(records, 0)
    val offset = 1234567
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV0Compressed() {
    val records = createRecords(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV1NonCompressed() {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(Record.MAGIC_VALUE_V1, now, codec = CompressionType.NONE)
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV1Compressed() {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(Record.MAGIC_VALUE_V1, now, CompressionType.GZIP)
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V0,
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
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L)
  }

  private def createRecords(magicValue: Byte = Message.CurrentMagicValue,
                              timestamp: Long = Message.NoTimestamp,
                              codec: CompressionType = CompressionType.NONE): MemoryRecords = {
    if (magicValue == Record.MAGIC_VALUE_V0) {
      MemoryRecords.withRecords(
        codec,
        Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, "hello".getBytes),
        Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, "there".getBytes),
        Record.create(Record.MAGIC_VALUE_V0, Record.NO_TIMESTAMP, "beautiful".getBytes))
    } else {
      MemoryRecords.withRecords(
        codec,
        Record.create(Record.MAGIC_VALUE_V1, timestamp, "hello".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestamp, "there".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestamp, "beautiful".getBytes))
    }
  }

  /* check that offsets are assigned consecutively from the given base offset */
  private def checkOffsets(records: MemoryRecords, baseOffset: Long) {
    assertTrue("Message set should not be empty", records.deepEntries.asScala.nonEmpty)
    var offset = baseOffset
    for (entry <- records.deepEntries.asScala) {
      assertEquals("Unexpected offset in message set iterator", offset, entry.offset)
      offset += 1
    }
  }

  private def recordsWithInvalidInnerMagic(initialOffset: Long): MemoryRecords = {
    val records = (0 until 20).map(id =>
      Record.create(Record.MAGIC_VALUE_V0,
        Record.NO_TIMESTAMP,
        id.toString.getBytes,
        id.toString.getBytes))

    val buffer = ByteBuffer.allocate(math.min(math.max(records.map(_.sizeInBytes()).sum / 2, 1024), 1 << 16))
    val builder = MemoryRecords.builder(buffer, Record.MAGIC_VALUE_V1, CompressionType.GZIP,
      TimestampType.CREATE_TIME)

    var offset = initialOffset
    records.foreach { record =>
      builder.appendUnchecked(offset, record)
      offset += 1
    }

    builder.build()
  }

  def validateLogAppendTime(now: Long, record: Record) {
    record.ensureValid()
    assertEquals(s"Timestamp of message $record should be $now", now, record.timestamp)
    assertEquals(TimestampType.LOG_APPEND_TIME, record.timestampType)
  }

}
