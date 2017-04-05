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
import kafka.utils.Logging
import org.apache.kafka.common.errors.InvalidTimestampException
import org.apache.kafka.common.record._

import scala.collection.mutable
import scala.collection.JavaConverters._

private[kafka] object LogValidator extends Logging {

  /**
   * Update the offsets for this message set and do further validation on messages including:
   * 1. Messages for compacted topics must have keys
   * 2. When magic value >= 1, inner messages of a compressed message set must have monotonically increasing offsets
   *    starting from 0.
   * 3. When magic value >= 1, validate and maybe overwrite timestamps of messages.
   *
   * This method will convert messages as necessary to the topic's configured message format version. If no format
   * conversion or value overwriting is required for messages, this method will perform in-place operations to
   * avoid expensive re-compression.
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
                                                      messageFormatVersion: Byte = RecordBatch.CURRENT_MAGIC_VALUE,
                                                      messageTimestampType: TimestampType,
                                                      messageTimestampDiffMaxMs: Long): ValidationAndOffsetAssignResult = {
    if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) {
      // check the magic value
      if (!records.hasMatchingMagic(messageFormatVersion))
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
    val sizeInBytesAfterConversion = AbstractRecords.estimateSizeInBytes(toMagicValue, offsetCounter.value,
      CompressionType.NONE, records.records)

    val (pid, epoch, sequence) = {
      val first = records.batches.asScala.head
      (first.producerId, first.producerEpoch, first.baseSequence)
    }

    val newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion)
    val builder = MemoryRecords.builder(newBuffer, toMagicValue, CompressionType.NONE, timestampType,
      offsetCounter.value, now, pid, epoch, sequence)

    for (batch <- records.batches.asScala) {
      ensureNonTransactional(batch)

      for (record <- batch.asScala) {
        ensureNotControlRecord(record)
        validateKey(record, compactedTopic)
        validateTimestamp(batch, record, now, timestampType, messageTimestampDiffMaxMs)
        builder.appendWithOffset(offsetCounter.getAndIncrement(), record)
      }
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
                                         currentTimestamp: Long,
                                         compactedTopic: Boolean,
                                         timestampType: TimestampType,
                                         timestampDiffMaxMs: Long): ValidationAndOffsetAssignResult = {
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    val initialOffset = offsetCounter.value

    for (batch <- records.batches.asScala) {
      ensureNonTransactional(batch)

      for (record <- batch.asScala) {
        record.ensureValid()
        ensureNotControlRecord(record)
        validateKey(record, compactedTopic)

        val offset = offsetCounter.getAndIncrement()
        if (batch.magic > RecordBatch.MAGIC_VALUE_V0) {
          validateTimestamp(batch, record, currentTimestamp, timestampType, timestampDiffMaxMs)

          if (record.timestamp > maxTimestamp) {
            maxTimestamp = record.timestamp
            offsetOfMaxTimestamp = offset
          }
        }
      }

      batch.setLastOffset(offsetCounter.value - 1)

      // TODO: in the compressed path, we ensure that the batch max timestamp is correct.
      //       We should either do the same or (better) let those two paths converge.
      if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && timestampType == TimestampType.LOG_APPEND_TIME)
        batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, currentTimestamp)
    }

    if (timestampType == TimestampType.LOG_APPEND_TIME) {
      maxTimestamp = currentTimestamp
      offsetOfMaxTimestamp = initialOffset
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
  def validateMessagesAndAssignOffsetsCompressed(records: MemoryRecords,
                                                 offsetCounter: LongRef,
                                                 currentTimestamp: Long,
                                                 sourceCodec: CompressionCodec,
                                                 targetCodec: CompressionCodec,
                                                 compactedTopic: Boolean = false,
                                                 messageFormatVersion: Byte = RecordBatch.CURRENT_MAGIC_VALUE,
                                                 messageTimestampType: TimestampType,
                                                 messageTimestampDiffMaxMs: Long): ValidationAndOffsetAssignResult = {

      // No in place assignment situation 1 and 2
      var inPlaceAssignment = sourceCodec == targetCodec && messageFormatVersion > RecordBatch.MAGIC_VALUE_V0

      var maxTimestamp = RecordBatch.NO_TIMESTAMP
      val expectedInnerOffset = new LongRef(0)
      val validatedRecords = new mutable.ArrayBuffer[Record]

      for (batch <- records.batches.asScala) {
        ensureNonTransactional(batch)

        for (record <- batch.asScala) {
          if (!record.hasMagic(batch.magic))
            throw new InvalidRecordException(s"Log record magic does not match outer magic ${batch.magic}")

          record.ensureValid()
          ensureNotControlRecord(record)
          validateKey(record, compactedTopic)

          if (!record.hasMagic(RecordBatch.MAGIC_VALUE_V0) && messageFormatVersion > RecordBatch.MAGIC_VALUE_V0) {
            // Validate the timestamp
            validateTimestamp(batch, record, currentTimestamp, messageTimestampType, messageTimestampDiffMaxMs)
            // Check if we need to overwrite offset
            // No in place assignment situation 3
            if (record.offset != expectedInnerOffset.getAndIncrement())
              inPlaceAssignment = false
            if (record.timestamp > maxTimestamp)
              maxTimestamp = record.timestamp
          }

          if (sourceCodec != NoCompressionCodec && record.isCompressed)
            throw new InvalidMessageException("Compressed outer record should not have an inner record with a " +
              s"compression attribute set: $record")

          // No in place assignment situation 4
          if (!record.hasMagic(messageFormatVersion))
            inPlaceAssignment = false

          validatedRecords += record
        }
      }

      if (!inPlaceAssignment) {
        val (pid, epoch, sequence) = {
          // note that we only reassign offsets for requests coming straight from a producer. For records with MagicV2,
          // there should be exactly one RecordBatch per request, so the following is all we need to do. For Records
          // with older magic versions, this will always be NO_PRODUCER_ID, etc.
          val first = records.batches.asScala.head
          (first.producerId, first.producerEpoch, first.baseSequence)
        }
        buildRecordsAndAssignOffsets(messageFormatVersion, offsetCounter, messageTimestampType,
          CompressionType.forId(targetCodec.codec), currentTimestamp, validatedRecords, pid, epoch, sequence)
      } else {
        // we can update the batch only and write the compressed payload as is
        val batch = records.batches.iterator.next()
        val lastOffset = offsetCounter.addAndGet(validatedRecords.size) - 1

        batch.setLastOffset(lastOffset)

        if (messageTimestampType == TimestampType.LOG_APPEND_TIME)
          maxTimestamp = currentTimestamp

        if (messageFormatVersion >= RecordBatch.MAGIC_VALUE_V1)
          batch.setMaxTimestamp(messageTimestampType, maxTimestamp)

        ValidationAndOffsetAssignResult(validatedRecords = records,
          maxTimestamp = maxTimestamp,
          shallowOffsetOfMaxTimestamp = lastOffset,
          messageSizeMaybeChanged = false)
      }
  }

  private def buildRecordsAndAssignOffsets(magic: Byte, offsetCounter: LongRef, timestampType: TimestampType,
                                           compressionType: CompressionType, logAppendTime: Long,
                                           validatedRecords: Seq[Record],
                                           producerId: Long, epoch: Short, baseSequence: Int): ValidationAndOffsetAssignResult = {
    val estimatedSize = AbstractRecords.estimateSizeInBytes(magic, offsetCounter.value, compressionType, validatedRecords.asJava)
    val buffer = ByteBuffer.allocate(estimatedSize)
    val builder = MemoryRecords.builder(buffer, magic, compressionType, timestampType, offsetCounter.value,
      logAppendTime, producerId, epoch, baseSequence)

    validatedRecords.foreach { record =>
      builder.appendWithOffset(offsetCounter.getAndIncrement(), record)
    }

    val records = builder.build()
    val info = builder.info

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true)
  }

  private def ensureNonTransactional(batch: RecordBatch) {
    if (batch.isTransactional)
      throw new InvalidRecordException("Transactional messages are not currently supported")
  }

  private def ensureNotControlRecord(record: Record) {
    // Until we have implemented transaction support, we do not permit control records to be written
    if (record.isControlRecord)
      throw new InvalidRecordException("Control messages are not currently supported")
  }

  private def validateKey(record: Record, compactedTopic: Boolean) {
    if (compactedTopic && !record.hasKey)
      throw new InvalidMessageException("Compacted topic cannot accept message without key.")
  }

  /**
   * This method validates the timestamps of a message.
   * If the message is using create time, this method checks if it is within acceptable range.
   */
  private def validateTimestamp(batch: RecordBatch,
                                record: Record,
                                now: Long,
                                timestampType: TimestampType,
                                timestampDiffMaxMs: Long) {
    if (timestampType == TimestampType.CREATE_TIME
      && record.timestamp != RecordBatch.NO_TIMESTAMP
      && math.abs(record.timestamp - now) > timestampDiffMaxMs)
      throw new InvalidTimestampException(s"Timestamp ${record.timestamp} of message is out of range. " +
        s"The timestamp should be within [${now - timestampDiffMaxMs}, ${now + timestampDiffMaxMs}]")
    if (batch.timestampType == TimestampType.LOG_APPEND_TIME)
      throw new InvalidTimestampException(s"Invalid timestamp type in message $record. Producer should not set " +
        s"timestamp type to LogAppendTime.")
  }

  case class ValidationAndOffsetAssignResult(validatedRecords: MemoryRecords,
                                             maxTimestamp: Long,
                                             shallowOffsetOfMaxTimestamp: Long,
                                             messageSizeMaybeChanged: Boolean)

}
