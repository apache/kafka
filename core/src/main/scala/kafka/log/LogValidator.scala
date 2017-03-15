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
import kafka.message.{CompressionCodec, InvalidMessageException, NoCompressionCodec}
import org.apache.kafka.common.errors.InvalidTimestampException
import org.apache.kafka.common.record._

import scala.collection.mutable
import scala.collection.JavaConverters._

private[kafka] object LogValidator {

  /**
   * Update the offsets for this message set and do further validation on messages including:
   * 1. Messages for compacted topics must have keys
   * 2. When magic value = 1, inner messages of a compressed message set must have monotonically increasing offsets
   *    starting from 0.
   * 3. When magic value = 1, validate and maybe overwrite timestamps of messages.
   *
   * This method will convert the messages in the following scenarios:
   * A. Magic value of a message = 0 and messageFormatVersion is 1
   * B. Magic value of a message = 1 and messageFormatVersion is 0
   *
   * If no format conversion or value overwriting is required for messages, this method will perform in-place
   * operations and avoid re-compression.
   *
   * Returns a ValidationAndOffsetAssignResult containing the validated message set, maximum timestamp, the offset
   * of the shallow message with the max timestamp and a boolean indicating whether the message sizes may have changed.
   */
  private[kafka] def validateMessagesAndAssignOffsets(records: MemoryRecords,
                                                      offsetCounter: LongRef,
                                                      now: Long,
                                                      sourceCodec: CompressionCodec,
                                                      targetCodec: CompressionCodec,
                                                      compactedTopic: Boolean = false,
                                                      messageFormatVersion: Byte = Record.CURRENT_MAGIC_VALUE,
                                                      messageTimestampType: TimestampType,
                                                      messageTimestampDiffMaxMs: Long): ValidationAndOffsetAssignResult = {
    if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) {
      // check the magic value
      if (!records.hasMatchingShallowMagic(messageFormatVersion))
        convertAndAssignOffsetsNonCompressed(records, offsetCounter, compactedTopic, now, messageTimestampType,
          messageTimestampDiffMaxMs, messageFormatVersion)
      else
        // Do in-place validation, offset assignment and maybe set timestamp
        assignOffsetsNonCompressed(records, offsetCounter, now, compactedTopic, messageTimestampType,
          messageTimestampDiffMaxMs)
    } else {
      validateMessagesAndAssignOffsetsCompressed(records, offsetCounter, now, sourceCodec, targetCodec, compactedTopic,
        messageFormatVersion, messageTimestampType, messageTimestampDiffMaxMs)
    }
  }

  private def convertAndAssignOffsetsNonCompressed(records: MemoryRecords,
                                                   offsetCounter: LongRef,
                                                   compactedTopic: Boolean,
                                                   now: Long,
                                                   timestampType: TimestampType,
                                                   messageTimestampDiffMaxMs: Long,
                                                   toMagicValue: Byte): ValidationAndOffsetAssignResult = {
    val sizeInBytesAfterConversion = records.shallowEntries.asScala.map { logEntry =>
      logEntry.record.convertedSize(toMagicValue)
    }.sum

    val newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion)
    val builder = MemoryRecords.builder(newBuffer, toMagicValue, CompressionType.NONE, timestampType,
      offsetCounter.value, now)

    records.shallowEntries.asScala.foreach { logEntry =>
      val record = logEntry.record
      validateKey(record, compactedTopic)
      validateTimestamp(record, now, timestampType, messageTimestampDiffMaxMs)
      builder.convertAndAppendWithOffset(offsetCounter.getAndIncrement(), record)
    }

    val convertedRecords = builder.build()
    val info = builder.info
    ValidationAndOffsetAssignResult(
      validatedRecords = convertedRecords,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true)
  }

  private def assignOffsetsNonCompressed(records: MemoryRecords,
                                         offsetCounter: LongRef,
                                         now: Long,
                                         compactedTopic: Boolean,
                                         timestampType: TimestampType,
                                         timestampDiffMaxMs: Long): ValidationAndOffsetAssignResult = {
    var maxTimestamp = Record.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    val firstOffset = offsetCounter.value

    for (entry <- records.shallowEntries.asScala) {
      val record = entry.record
      validateKey(record, compactedTopic)

      val offset = offsetCounter.getAndIncrement()
      entry.setOffset(offset)

      if (record.magic > Record.MAGIC_VALUE_V0) {
        validateTimestamp(record, now, timestampType, timestampDiffMaxMs)

        if (timestampType == TimestampType.LOG_APPEND_TIME)
          entry.setLogAppendTime(now)
        else if (record.timestamp > maxTimestamp) {
          maxTimestamp = record.timestamp
          offsetOfMaxTimestamp = offset
        }
      }
    }

    if (timestampType == TimestampType.LOG_APPEND_TIME) {
      maxTimestamp = now
      offsetOfMaxTimestamp = firstOffset
    }

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = maxTimestamp,
      shallowOffsetOfMaxTimestamp = offsetOfMaxTimestamp,
      messageSizeMaybeChanged = false)
  }

  /**
   * We cannot do in place assignment in one of the following situations:
   * 1. Source and target compression codec are different
   * 2. When magic value to use is 0 because offsets need to be overwritten
   * 3. When magic value to use is above 0, but some fields of inner messages need to be overwritten.
   * 4. Message format conversion is needed.
   */
  private def validateMessagesAndAssignOffsetsCompressed(records: MemoryRecords,
                                                         offsetCounter: LongRef,
                                                         now: Long,
                                                         sourceCodec: CompressionCodec,
                                                         targetCodec: CompressionCodec,
                                                         compactedTopic: Boolean = false,
                                                         messageFormatVersion: Byte = Record.CURRENT_MAGIC_VALUE,
                                                         messageTimestampType: TimestampType,
                                                         messageTimestampDiffMaxMs: Long): ValidationAndOffsetAssignResult = {
    // No in place assignment situation 1 and 2
    var inPlaceAssignment = sourceCodec == targetCodec && messageFormatVersion > Record.MAGIC_VALUE_V0

    var maxTimestamp = Record.NO_TIMESTAMP
    val expectedInnerOffset = new LongRef(0)
    val validatedRecords = new mutable.ArrayBuffer[Record]

    records.deepEntries(true).asScala.foreach { logEntry =>
      val record = logEntry.record
      validateKey(record, compactedTopic)

      if (record.magic > Record.MAGIC_VALUE_V0 && messageFormatVersion > Record.MAGIC_VALUE_V0) {
        // Validate the timestamp
        validateTimestamp(record, now, messageTimestampType, messageTimestampDiffMaxMs)
        // Check if we need to overwrite offset, no in place assignment situation 3
        if (logEntry.offset != expectedInnerOffset.getAndIncrement())
          inPlaceAssignment = false
        if (record.timestamp > maxTimestamp)
          maxTimestamp = record.timestamp
      }

      if (sourceCodec != NoCompressionCodec && logEntry.isCompressed)
        throw new InvalidMessageException("Compressed outer record should not have an inner record with a " +
          s"compression attribute set: $record")

      // No in place assignment situation 4
      if (record.magic != messageFormatVersion)
        inPlaceAssignment = false

      validatedRecords += record.convert(messageFormatVersion, messageTimestampType)
    }

    if (!inPlaceAssignment) {
      val entries = validatedRecords.map(record => LogEntry.create(offsetCounter.getAndIncrement(), record))
      val builder = MemoryRecords.builderWithEntries(messageTimestampType, CompressionType.forId(targetCodec.codec),
        now, entries.asJava)
      val updatedRecords = builder.build()
      val info = builder.info
      ValidationAndOffsetAssignResult(
        validatedRecords = updatedRecords,
        maxTimestamp = info.maxTimestamp,
        shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
        messageSizeMaybeChanged = true)
    } else {
      // ensure the inner messages are valid
      validatedRecords.foreach(_.ensureValid)

      // we can update the wrapper message only and write the compressed payload as is
      val entry = records.shallowEntries.iterator.next()
      val offset = offsetCounter.addAndGet(validatedRecords.size) - 1
      entry.setOffset(offset)

      val shallowTimestamp = if (messageTimestampType == TimestampType.LOG_APPEND_TIME) now else maxTimestamp
      if (messageTimestampType == TimestampType.LOG_APPEND_TIME)
        entry.setLogAppendTime(shallowTimestamp)
      else if (messageTimestampType == TimestampType.CREATE_TIME)
        entry.setCreateTime(shallowTimestamp)

      ValidationAndOffsetAssignResult(validatedRecords = records,
        maxTimestamp = shallowTimestamp,
        shallowOffsetOfMaxTimestamp = offset,
        messageSizeMaybeChanged = false)
    }
  }

  private def validateKey(record: Record, compactedTopic: Boolean) {
    if (compactedTopic && !record.hasKey)
      throw new InvalidMessageException("Compacted topic cannot accept message without key.")
  }

  /**
   * This method validates the timestamps of a message.
   * If the message is using create time, this method checks if it is within acceptable range.
   */
  private def validateTimestamp(record: Record,
                                now: Long,
                                timestampType: TimestampType,
                                timestampDiffMaxMs: Long) {
    if (timestampType == TimestampType.CREATE_TIME && math.abs(record.timestamp - now) > timestampDiffMaxMs)
      throw new InvalidTimestampException(s"Timestamp ${record.timestamp} of message is out of range. " +
        s"The timestamp should be within [${now - timestampDiffMaxMs}, ${now + timestampDiffMaxMs}")
    if (record.timestampType == TimestampType.LOG_APPEND_TIME)
      throw new InvalidTimestampException(s"Invalid timestamp type in message $record. Producer should not set " +
        s"timestamp type to LogAppendTime.")
  }

  case class ValidationAndOffsetAssignResult(validatedRecords: MemoryRecords,
                                             maxTimestamp: Long,
                                             shallowOffsetOfMaxTimestamp: Long,
                                             messageSizeMaybeChanged: Boolean)

}
