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

import kafka.api.{ApiVersion, KAFKA_2_1_IV0}
import kafka.common.{LongRef, RecordValidationException}
import kafka.message.{CompressionCodec, NoCompressionCodec, ZStdCompressionCodec}
import kafka.server.BrokerTopicStats
import kafka.utils.Logging
import org.apache.kafka.common.errors.{CorruptRecordException, InvalidTimestampException, UnsupportedCompressionTypeException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record.{AbstractRecords, BufferSupplier, CompressionType, MemoryRecords, Record, RecordBatch, RecordConversionStats, TimestampType}
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.RecordError
import org.apache.kafka.common.utils.Time

import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * The source of an append to the log. This is used when determining required validations.
 */
private[kafka] sealed trait AppendOrigin
private[kafka] object AppendOrigin {

  /**
   * The log append came through replication from the leader. This typically implies minimal validation.
   * Particularly, we do not decompress record batches in order to validate records individually.
   */
  case object Replication extends AppendOrigin

  /**
   * The log append came from either the group coordinator or the transaction coordinator. We validate
   * producer epochs for normal log entries (specifically offset commits from the group coordinator) and
   * we validate coordinate end transaction markers from the transaction coordinator.
   */
  case object Coordinator extends AppendOrigin

  /**
   * The log append came from the client, which implies full validation.
   */
  case object Client extends AppendOrigin
}

private[log] object LogValidator extends Logging {

  /**
   * Update the offsets for this message set and do further validation on messages including:
   * 1. Messages for compacted topics must have keys
   * 2. When magic value >= 1, inner messages of a compressed message set must have monotonically increasing offsets
   *    starting from 0.
   * 3. When magic value >= 1, validate and maybe overwrite timestamps of messages.
   * 4. Declared count of records in DefaultRecordBatch must match number of valid records contained therein.
   *
   * This method will convert messages as necessary to the topic's configured message format version. If no format
   * conversion or value overwriting is required for messages, this method will perform in-place operations to
   * avoid expensive re-compression.
   *
   * Returns a ValidationAndOffsetAssignResult containing the validated message set, maximum timestamp, the offset
   * of the shallow message with the max timestamp and a boolean indicating whether the message sizes may have changed.
   */
  private[log] def validateMessagesAndAssignOffsets(records: MemoryRecords,
                                                    topicPartition: TopicPartition,
                                                    offsetCounter: LongRef,
                                                    time: Time,
                                                    now: Long,
                                                    sourceCodec: CompressionCodec,
                                                    targetCodec: CompressionCodec,
                                                    compactedTopic: Boolean,
                                                    magic: Byte,
                                                    timestampType: TimestampType,
                                                    timestampDiffMaxMs: Long,
                                                    partitionLeaderEpoch: Int,
                                                    origin: AppendOrigin,
                                                    interBrokerProtocolVersion: ApiVersion,
                                                    brokerTopicStats: BrokerTopicStats): ValidationAndOffsetAssignResult = {
    if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) {
      // check the magic value
      if (!records.hasMatchingMagic(magic))
        convertAndAssignOffsetsNonCompressed(records, topicPartition, offsetCounter, compactedTopic, time, now, timestampType,
          timestampDiffMaxMs, magic, partitionLeaderEpoch, origin, brokerTopicStats)
      else
        // Do in-place validation, offset assignment and maybe set timestamp
        assignOffsetsNonCompressed(records, topicPartition, offsetCounter, now, compactedTopic, timestampType, timestampDiffMaxMs,
          partitionLeaderEpoch, origin, magic, brokerTopicStats)
    } else {
      validateMessagesAndAssignOffsetsCompressed(records, topicPartition, offsetCounter, time, now, sourceCodec, targetCodec, compactedTopic,
        magic, timestampType, timestampDiffMaxMs, partitionLeaderEpoch, origin, interBrokerProtocolVersion, brokerTopicStats)
    }
  }

  private def getFirstBatchAndMaybeValidateNoMoreBatches(records: MemoryRecords, sourceCodec: CompressionCodec): RecordBatch = {
    val batchIterator = records.batches.iterator

    if (!batchIterator.hasNext) {
      throw new InvalidRecordException("Record batch has no batches at all")
    }

    val batch = batchIterator.next()

    // if the format is v2 and beyond, or if the messages are compressed, we should check there's only one batch.
    if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || sourceCodec != NoCompressionCodec) {
      if (batchIterator.hasNext) {
        throw new InvalidRecordException("Compressed outer record has more than one batch")
      }
    }

    batch
  }

  private def validateBatch(topicPartition: TopicPartition,
                            firstBatch: RecordBatch,
                            batch: RecordBatch,
                            origin: AppendOrigin,
                            toMagic: Byte,
                            brokerTopicStats: BrokerTopicStats): Unit = {
    // batch magic byte should have the same magic as the first batch
    if (firstBatch.magic() != batch.magic()) {
      brokerTopicStats.allTopicsStats.invalidMagicNumberRecordsPerSec.mark()
      throw new InvalidRecordException(s"Batch magic ${batch.magic()} is not the same as the first batch'es magic byte ${firstBatch.magic()} in topic partition $topicPartition.")
    }

    if (origin == AppendOrigin.Client) {
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
        val countFromOffsets = batch.lastOffset - batch.baseOffset + 1
        if (countFromOffsets <= 0) {
          brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()
          throw new InvalidRecordException(s"Batch has an invalid offset range: [${batch.baseOffset}, ${batch.lastOffset}] in topic partition $topicPartition.")
        }

        // v2 and above messages always have a non-null count
        val count = batch.countOrNull
        if (count <= 0) {
          brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()
          throw new InvalidRecordException(s"Invalid reported count for record batch: $count in topic partition $topicPartition.")
        }

        if (countFromOffsets != batch.countOrNull) {
          brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()
          throw new InvalidRecordException(s"Inconsistent batch offset range [${batch.baseOffset}, ${batch.lastOffset}] " +
            s"and count of records $count in topic partition $topicPartition.")
        }
      }

      if (batch.isControlBatch) {
        brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()
        throw new InvalidRecordException(s"Clients are not allowed to write control records in topic partition $topicPartition.")
      }

      if (batch.hasProducerId && batch.baseSequence < 0) {
        brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()
        throw new InvalidRecordException(s"Invalid sequence number ${batch.baseSequence} in record batch " +
          s"with producerId ${batch.producerId} in topic partition $topicPartition.")
      }
    }

    if (batch.isTransactional && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Transactional records cannot be used with magic version $toMagic")

    if (batch.hasProducerId && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Idempotent records cannot be used with magic version $toMagic")
  }

  private def validateRecord(batch: RecordBatch, topicPartition: TopicPartition, record: Record, batchIndex: Int, now: Long,
                             timestampType: TimestampType, timestampDiffMaxMs: Long, compactedTopic: Boolean,
                             brokerTopicStats: BrokerTopicStats): Option[ApiRecordError] = {
    if (!record.hasMagic(batch.magic)) {
      brokerTopicStats.allTopicsStats.invalidMagicNumberRecordsPerSec.mark()
      return Some(ApiRecordError(Errors.INVALID_RECORD, new RecordError(batchIndex,
        s"Record $record's magic does not match outer magic ${batch.magic} in topic partition $topicPartition.")))
    }

    // verify the record-level CRC only if this is one of the deep entries of a compressed message
    // set for magic v0 and v1. For non-compressed messages, there is no inner record for magic v0 and v1,
    // so we depend on the batch-level CRC check in Log.analyzeAndValidateRecords(). For magic v2 and above,
    // there is no record-level CRC to check.
    if (batch.magic <= RecordBatch.MAGIC_VALUE_V1 && batch.isCompressed) {
      try {
        record.ensureValid()
      } catch {
        case e: InvalidRecordException =>
          brokerTopicStats.allTopicsStats.invalidMessageCrcRecordsPerSec.mark()
          throw new CorruptRecordException(e.getMessage + s" in topic partition $topicPartition.")
      }
    }

    validateKey(record, batchIndex, topicPartition, compactedTopic, brokerTopicStats).orElse {
      validateTimestamp(batch, record, batchIndex, now, timestampType, timestampDiffMaxMs)
    }
  }

  private def convertAndAssignOffsetsNonCompressed(records: MemoryRecords,
                                                   topicPartition: TopicPartition,
                                                   offsetCounter: LongRef,
                                                   compactedTopic: Boolean,
                                                   time: Time,
                                                   now: Long,
                                                   timestampType: TimestampType,
                                                   timestampDiffMaxMs: Long,
                                                   toMagicValue: Byte,
                                                   partitionLeaderEpoch: Int,
                                                   origin: AppendOrigin,
                                                   brokerTopicStats: BrokerTopicStats): ValidationAndOffsetAssignResult = {
    val startNanos = time.nanoseconds
    val sizeInBytesAfterConversion = AbstractRecords.estimateSizeInBytes(toMagicValue, offsetCounter.value,
      CompressionType.NONE, records.records)

    val (producerId, producerEpoch, sequence, isTransactional) = {
      val first = records.batches.asScala.head
      (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional)
    }

    val newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion)
    val builder = MemoryRecords.builder(newBuffer, toMagicValue, CompressionType.NONE, timestampType,
      offsetCounter.value, now, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch)

    val firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, NoCompressionCodec)

    records.batches.forEach { batch =>
      validateBatch(topicPartition, firstBatch, batch, origin, toMagicValue, brokerTopicStats)

      val recordErrors = new ArrayBuffer[ApiRecordError](0)
      for ((record, batchIndex) <- batch.asScala.view.zipWithIndex) {
        validateRecord(batch, topicPartition, record, batchIndex, now, timestampType,
          timestampDiffMaxMs, compactedTopic, brokerTopicStats).foreach(recordError => recordErrors += recordError)
        // we fail the batch if any record fails, so we stop appending if any record fails
        if (recordErrors.isEmpty)
          builder.appendWithOffset(offsetCounter.getAndIncrement(), record)
      }

      processRecordErrors(recordErrors)
    }

    val convertedRecords = builder.build()

    val info = builder.info
    val recordConversionStats = new RecordConversionStats(builder.uncompressedBytesWritten,
      builder.numRecords, time.nanoseconds - startNanos)
    ValidationAndOffsetAssignResult(
      validatedRecords = convertedRecords,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true,
      recordConversionStats = recordConversionStats)
  }

  def assignOffsetsNonCompressed(records: MemoryRecords,
                                         topicPartition: TopicPartition,
                                         offsetCounter: LongRef,
                                         now: Long,
                                         compactedTopic: Boolean,
                                         timestampType: TimestampType,
                                         timestampDiffMaxMs: Long,
                                         partitionLeaderEpoch: Int,
                                         origin: AppendOrigin,
                                         magic: Byte,
                                         brokerTopicStats: BrokerTopicStats): ValidationAndOffsetAssignResult = {
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    val initialOffset = offsetCounter.value

    val firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, NoCompressionCodec)

    records.batches.forEach { batch =>
      validateBatch(topicPartition, firstBatch, batch, origin, magic, brokerTopicStats)

      var maxBatchTimestamp = RecordBatch.NO_TIMESTAMP
      var offsetOfMaxBatchTimestamp = -1L

      val recordErrors = new ArrayBuffer[ApiRecordError](0)
      // this is a hot path and we want to avoid any unnecessary allocations.
      var batchIndex = 0
      batch.forEach { record =>
        validateRecord(batch, topicPartition, record, batchIndex, now, timestampType,
          timestampDiffMaxMs, compactedTopic, brokerTopicStats).foreach(recordError => recordErrors += recordError)

        val offset = offsetCounter.getAndIncrement()
        if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && record.timestamp > maxBatchTimestamp) {
          maxBatchTimestamp = record.timestamp
          offsetOfMaxBatchTimestamp = offset
        }
        batchIndex += 1
      }

      processRecordErrors(recordErrors)

      if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && maxBatchTimestamp > maxTimestamp) {
        maxTimestamp = maxBatchTimestamp
        offsetOfMaxTimestamp = offsetOfMaxBatchTimestamp
      }

      batch.setLastOffset(offsetCounter.value - 1)

      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
        batch.setPartitionLeaderEpoch(partitionLeaderEpoch)

      if (batch.magic > RecordBatch.MAGIC_VALUE_V0) {
        if (timestampType == TimestampType.LOG_APPEND_TIME)
          batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, now)
        else
          batch.setMaxTimestamp(timestampType, maxBatchTimestamp)
      }
    }

    if (timestampType == TimestampType.LOG_APPEND_TIME) {
      maxTimestamp = now
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        offsetOfMaxTimestamp = offsetCounter.value - 1
      else
        offsetOfMaxTimestamp = initialOffset
    }

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = maxTimestamp,
      shallowOffsetOfMaxTimestamp = offsetOfMaxTimestamp,
      messageSizeMaybeChanged = false,
      recordConversionStats = RecordConversionStats.EMPTY)
  }

  /**
   * We cannot do in place assignment in one of the following situations:
   * 1. Source and target compression codec are different
   * 2. When the target magic is not equal to batches' magic, meaning format conversion is needed.
   * 3. When the target magic is equal to V0, meaning absolute offsets need to be re-assigned.
   */
  def validateMessagesAndAssignOffsetsCompressed(records: MemoryRecords,
                                                 topicPartition: TopicPartition,
                                                 offsetCounter: LongRef,
                                                 time: Time,
                                                 now: Long,
                                                 sourceCodec: CompressionCodec,
                                                 targetCodec: CompressionCodec,
                                                 compactedTopic: Boolean,
                                                 toMagic: Byte,
                                                 timestampType: TimestampType,
                                                 timestampDiffMaxMs: Long,
                                                 partitionLeaderEpoch: Int,
                                                 origin: AppendOrigin,
                                                 interBrokerProtocolVersion: ApiVersion,
                                                 brokerTopicStats: BrokerTopicStats): ValidationAndOffsetAssignResult = {

    if (targetCodec == ZStdCompressionCodec && interBrokerProtocolVersion < KAFKA_2_1_IV0)
      throw new UnsupportedCompressionTypeException("Produce requests to inter.broker.protocol.version < 2.1 broker " +
        "are not allowed to use ZStandard compression")

    def validateRecordCompression(batchIndex: Int, record: Record): Option[ApiRecordError] = {
      if (sourceCodec != NoCompressionCodec && record.isCompressed)
        Some(ApiRecordError(Errors.INVALID_RECORD, new RecordError(batchIndex,
          s"Compressed outer record should not have an inner record with a compression attribute set: $record")))
      else None
    }

    // No in place assignment situation 1
    var inPlaceAssignment = sourceCodec == targetCodec

    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    val expectedInnerOffset = new LongRef(0)
    val validatedRecords = new mutable.ArrayBuffer[Record]

    var uncompressedSizeInBytes = 0

    // Assume there's only one batch with compressed memory records; otherwise, return InvalidRecordException
    // One exception though is that with format smaller than v2, if sourceCodec is noCompression, then each batch is actually
    // a single record so we'd need to special handle it by creating a single wrapper batch that includes all the records
    val firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, sourceCodec)

    // No in place assignment situation 2 and 3: we only need to check for the first batch because:
    //  1. For most cases (compressed records, v2, for example), there's only one batch anyways.
    //  2. For cases that there may be multiple batches, all batches' magic should be the same.
    if (firstBatch.magic != toMagic || toMagic == RecordBatch.MAGIC_VALUE_V0)
      inPlaceAssignment = false

    // Do not compress control records unless they are written compressed
    if (sourceCodec == NoCompressionCodec && firstBatch.isControlBatch)
      inPlaceAssignment = true

    records.batches.forEach { batch =>
      validateBatch(topicPartition, firstBatch, batch, origin, toMagic, brokerTopicStats)
      uncompressedSizeInBytes += AbstractRecords.recordBatchHeaderSizeInBytes(toMagic, batch.compressionType())

      // if we are on version 2 and beyond, and we know we are going for in place assignment,
      // then we can optimize the iterator to skip key / value / headers since they would not be used at all
      val recordsIterator = if (inPlaceAssignment && firstBatch.magic >= RecordBatch.MAGIC_VALUE_V2)
        batch.skipKeyValueIterator(BufferSupplier.NO_CACHING)
      else
        batch.streamingIterator(BufferSupplier.NO_CACHING)

      try {
        val recordErrors = new ArrayBuffer[ApiRecordError](0)
        // this is a hot path and we want to avoid any unnecessary allocations.
        var batchIndex = 0
        recordsIterator.forEachRemaining { record =>
          val expectedOffset = expectedInnerOffset.getAndIncrement()
          val recordError = validateRecordCompression(batchIndex, record).orElse {
            validateRecord(batch, topicPartition, record, batchIndex, now,
              timestampType, timestampDiffMaxMs, compactedTopic, brokerTopicStats).orElse {
              if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && toMagic > RecordBatch.MAGIC_VALUE_V0) {
                if (record.timestamp > maxTimestamp)
                  maxTimestamp = record.timestamp

                // Some older clients do not implement the V1 internal offsets correctly.
                // Historically the broker handled this by rewriting the batches rather
                // than rejecting the request. We must continue this handling here to avoid
                // breaking these clients.
                if (record.offset != expectedOffset)
                  inPlaceAssignment = false
              }
              None
            }
          }

          recordError match {
            case Some(e) => recordErrors += e
            case None =>
              uncompressedSizeInBytes += record.sizeInBytes()
              validatedRecords += record
          }
         batchIndex += 1
        }
        processRecordErrors(recordErrors)
      } finally {
        recordsIterator.close()
      }
    }

    if (!inPlaceAssignment) {
      val (producerId, producerEpoch, sequence, isTransactional) = {
        // note that we only reassign offsets for requests coming straight from a producer. For records with magic V2,
        // there should be exactly one RecordBatch per request, so the following is all we need to do. For Records
        // with older magic versions, there will never be a producer id, etc.
        val first = records.batches.asScala.head
        (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional)
      }
      buildRecordsAndAssignOffsets(toMagic, offsetCounter, time, timestampType, CompressionType.forId(targetCodec.codec),
        now, validatedRecords, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch,
        uncompressedSizeInBytes)
    } else {
      // we can update the batch only and write the compressed payload as is;
      // again we assume only one record batch within the compressed set
      val batch = records.batches.iterator.next()
      val lastOffset = offsetCounter.addAndGet(validatedRecords.size) - 1

      batch.setLastOffset(lastOffset)

      if (timestampType == TimestampType.LOG_APPEND_TIME)
        maxTimestamp = now

      if (toMagic >= RecordBatch.MAGIC_VALUE_V1)
        batch.setMaxTimestamp(timestampType, maxTimestamp)

      if (toMagic >= RecordBatch.MAGIC_VALUE_V2)
        batch.setPartitionLeaderEpoch(partitionLeaderEpoch)

      val recordConversionStats = new RecordConversionStats(uncompressedSizeInBytes, 0, 0)
      ValidationAndOffsetAssignResult(validatedRecords = records,
        maxTimestamp = maxTimestamp,
        shallowOffsetOfMaxTimestamp = lastOffset,
        messageSizeMaybeChanged = false,
        recordConversionStats = recordConversionStats)
    }
  }

  private def buildRecordsAndAssignOffsets(magic: Byte,
                                           offsetCounter: LongRef,
                                           time: Time,
                                           timestampType: TimestampType,
                                           compressionType: CompressionType,
                                           logAppendTime: Long,
                                           validatedRecords: Seq[Record],
                                           producerId: Long,
                                           producerEpoch: Short,
                                           baseSequence: Int,
                                           isTransactional: Boolean,
                                           partitionLeaderEpoch: Int,
                                           uncompressedSizeInBytes: Int): ValidationAndOffsetAssignResult = {
    val startNanos = time.nanoseconds
    val estimatedSize = AbstractRecords.estimateSizeInBytes(magic, offsetCounter.value, compressionType,
      validatedRecords.asJava)
    val buffer = ByteBuffer.allocate(estimatedSize)
    val builder = MemoryRecords.builder(buffer, magic, compressionType, timestampType, offsetCounter.value,
      logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch)

    validatedRecords.foreach { record =>
      builder.appendWithOffset(offsetCounter.getAndIncrement(), record)
    }

    val records = builder.build()

    val info = builder.info

    // This is not strictly correct, it represents the number of records where in-place assignment is not possible
    // instead of the number of records that were converted. It will over-count cases where the source and target are
    // message format V0 or if the inner offsets are not consecutive. This is OK since the impact is the same: we have
    // to rebuild the records (including recompression if enabled).
    val conversionCount = builder.numRecords
    val recordConversionStats = new RecordConversionStats(uncompressedSizeInBytes + builder.uncompressedBytesWritten,
      conversionCount, time.nanoseconds - startNanos)

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true,
      recordConversionStats = recordConversionStats)
  }

  private def validateKey(record: Record,
                          batchIndex: Int,
                          topicPartition: TopicPartition,
                          compactedTopic: Boolean,
                          brokerTopicStats: BrokerTopicStats): Option[ApiRecordError] = {
    if (compactedTopic && !record.hasKey) {
      brokerTopicStats.allTopicsStats.noKeyCompactedTopicRecordsPerSec.mark()
      Some(ApiRecordError(Errors.INVALID_RECORD, new RecordError(batchIndex,
        s"Compacted topic cannot accept message without key in topic partition $topicPartition.")))
    } else None
  }

  private def validateTimestamp(batch: RecordBatch,
                                record: Record,
                                batchIndex: Int,
                                now: Long,
                                timestampType: TimestampType,
                                timestampDiffMaxMs: Long): Option[ApiRecordError] = {
    if (timestampType == TimestampType.CREATE_TIME
      && record.timestamp != RecordBatch.NO_TIMESTAMP
      && math.abs(record.timestamp - now) > timestampDiffMaxMs)
      Some(ApiRecordError(Errors.INVALID_TIMESTAMP, new RecordError(batchIndex,
        s"Timestamp ${record.timestamp} of message with offset ${record.offset} is " +
          s"out of range. The timestamp should be within [${now - timestampDiffMaxMs}, " +
          s"${now + timestampDiffMaxMs}]")))
    else if (batch.timestampType == TimestampType.LOG_APPEND_TIME)
      Some(ApiRecordError(Errors.INVALID_TIMESTAMP, new RecordError(batchIndex,
        s"Invalid timestamp type in message $record. Producer should not set timestamp " +
          "type to LogAppendTime.")))
    else None
  }

  private def processRecordErrors(recordErrors: Seq[ApiRecordError]): Unit = {
    if (recordErrors.nonEmpty) {
      val errors = recordErrors.map(_.recordError)
      if (recordErrors.exists(_.apiError == Errors.INVALID_TIMESTAMP)) {
        throw new RecordValidationException(new InvalidTimestampException(
          "One or more records have been rejected due to invalid timestamp"), errors)
      } else {
        throw new RecordValidationException(new InvalidRecordException(
          "One or more records have been rejected"), errors)
      }
    }
  }

  case class ValidationAndOffsetAssignResult(validatedRecords: MemoryRecords,
                                             maxTimestamp: Long,
                                             shallowOffsetOfMaxTimestamp: Long,
                                             messageSizeMaybeChanged: Boolean,
                                             recordConversionStats: RecordConversionStats)

  private case class ApiRecordError(apiError: Errors, recordError: RecordError)
}
