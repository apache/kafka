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
package org.apache.kafka.server.log.internals;

import static org.apache.kafka.server.common.MetadataVersion.IBP_2_1_IV0;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MemoryRecordsBuilder.RecordsInfo;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordConversionStats;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.RecordError;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.PrimitiveRef.LongRef;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Iterator;

public class LogValidator {

    public interface MetricsRecorder {
        void recordInvalidMagic();

        void recordInvalidOffset();

        void recordInvalidSequence();

        void recordInvalidChecksums();

        void recordNoKeyCompactedTopic();
    }

    public static class ValidationResult {
        public final long logAppendTimeMs;
        public final MemoryRecords validatedRecords;
        public final long maxTimestampMs;
        public final long shallowOffsetOfMaxTimestampMs;
        public final boolean messageSizeMaybeChanged;
        public final RecordConversionStats recordConversionStats;

        public ValidationResult(long logAppendTimeMs, MemoryRecords validatedRecords, long maxTimestampMs,
                long shallowOffsetOfMaxTimestampMs, boolean messageSizeMaybeChanged,
                RecordConversionStats recordConversionStats) {
            this.logAppendTimeMs = logAppendTimeMs;
            this.validatedRecords = validatedRecords;
            this.maxTimestampMs = maxTimestampMs;
            this.shallowOffsetOfMaxTimestampMs = shallowOffsetOfMaxTimestampMs;
            this.messageSizeMaybeChanged = messageSizeMaybeChanged;
            this.recordConversionStats = recordConversionStats;
        }
    }

    private static class ApiRecordError {
        final Errors apiError;
        final RecordError recordError;

        private ApiRecordError(Errors apiError, RecordError recordError) {
            this.apiError = apiError;
            this.recordError = recordError;
        }
    }

    private final MemoryRecords records;
    private final TopicPartition topicPartition;
    private final Time time;
    private final CompressionType sourceCompression;
    private final CompressionType targetCompression;
    private final boolean compactedTopic;
    private final byte toMagic;
    private final TimestampType timestampType;
    private final long timestampDiffMaxMs;
    private final int partitionLeaderEpoch;
    private final AppendOrigin origin;
    private final MetadataVersion interBrokerProtocolVersion;

    public LogValidator(MemoryRecords records,
                        TopicPartition topicPartition,
                        Time time,
                        CompressionType sourceCompression,
                        CompressionType targetCompression,
                        boolean compactedTopic,
                        byte toMagic,
                        TimestampType timestampType,
                        long timestampDiffMaxMs,
                        int partitionLeaderEpoch,
                        AppendOrigin origin,
                        MetadataVersion interBrokerProtocolVersion) {
        this.records = records;
        this.topicPartition = topicPartition;
        this.time = time;
        this.sourceCompression = sourceCompression;
        this.targetCompression = targetCompression;
        this.compactedTopic = compactedTopic;
        this.toMagic = toMagic;
        this.timestampType = timestampType;
        this.timestampDiffMaxMs = timestampDiffMaxMs;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.origin = origin;
        this.interBrokerProtocolVersion = interBrokerProtocolVersion;
    }

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
    public ValidationResult validateMessagesAndAssignOffsets(PrimitiveRef.LongRef offsetCounter,
                                                             MetricsRecorder metricsRecorder,
                                                             BufferSupplier bufferSupplier) {
        if (sourceCompression == CompressionType.NONE && targetCompression == CompressionType.NONE) {
            // check the magic value
            if (!records.hasMatchingMagic(toMagic))
                return convertAndAssignOffsetsNonCompressed(offsetCounter, metricsRecorder);
            else
                // Do in-place validation, offset assignment and maybe set timestamp
                return assignOffsetsNonCompressed(offsetCounter, metricsRecorder);
        } else
            return validateMessagesAndAssignOffsetsCompressed(offsetCounter, metricsRecorder, bufferSupplier);

    }

    private static MutableRecordBatch getFirstBatchAndMaybeValidateNoMoreBatches(MemoryRecords records,
                                                                                 CompressionType sourceCompression) {
        Iterator<MutableRecordBatch> batchIterator = records.batches().iterator();

        if (!batchIterator.hasNext())
            throw new InvalidRecordException("Record batch has no batches at all");

        MutableRecordBatch batch = batchIterator.next();

        // if the format is v2 and beyond, or if the messages are compressed, we should check there's only one batch.
        if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || sourceCompression != CompressionType.NONE) {
            if (batchIterator.hasNext())
                throw new InvalidRecordException("Compressed outer record has more than one batch");
        }

        return batch;
    }

    private ValidationResult convertAndAssignOffsetsNonCompressed(LongRef offsetCounter,
                                                                  MetricsRecorder metricsRecorder) {
        long now = time.milliseconds();
        long startNanos = time.nanoseconds();
        int sizeInBytesAfterConversion = AbstractRecords.estimateSizeInBytes(toMagic, offsetCounter.value,
            CompressionType.NONE, records.records());

        RecordBatch firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, CompressionType.NONE);
        long producerId = firstBatch.producerId();
        short producerEpoch = firstBatch.producerEpoch();
        int sequence = firstBatch.baseSequence();
        boolean isTransactional = firstBatch.isTransactional();

        // The current implementation of BufferSupplier is naive and works best when the buffer size
        // cardinality is low, so don't use it here
        ByteBuffer newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion);
        MemoryRecordsBuilder builder = MemoryRecords.builder(newBuffer, toMagic, CompressionType.NONE,
            timestampType, offsetCounter.value, now, producerId, producerEpoch, sequence, isTransactional,
            partitionLeaderEpoch);

        for (RecordBatch batch : records.batches()) {
            validateBatch(topicPartition, firstBatch, batch, origin, toMagic, metricsRecorder);

            List<ApiRecordError> recordErrors = new ArrayList<>(0);
            int batchIndex = 0;
            for (Record record : batch) {
                Optional<ApiRecordError> recordError = validateRecord(batch, topicPartition,
                    record, batchIndex, now, timestampType, timestampDiffMaxMs, compactedTopic,
                    metricsRecorder);
                recordError.ifPresent(e -> recordErrors.add(e));
                // we fail the batch if any record fails, so we stop appending if any record fails
                if (recordErrors.isEmpty())
                    builder.appendWithOffset(offsetCounter.value++, record);
                ++batchIndex;
            }

            processRecordErrors(recordErrors);
        }

        MemoryRecords convertedRecords = builder.build();

        RecordsInfo info = builder.info();
        RecordConversionStats recordConversionStats = new RecordConversionStats(
            builder.uncompressedBytesWritten(), builder.numRecords(), time.nanoseconds() - startNanos);
        return new ValidationResult(
            now,
            convertedRecords,
            info.maxTimestamp,
            info.shallowOffsetOfMaxTimestamp,
            true,
            recordConversionStats);
    }

    // Visible for benchmarking
    public ValidationResult assignOffsetsNonCompressed(LongRef offsetCounter,
                                                       MetricsRecorder metricsRecorder) {
        long now = time.milliseconds();
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long offsetOfMaxTimestamp = -1L;
        long initialOffset = offsetCounter.value;

        RecordBatch firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, CompressionType.NONE);

        for (MutableRecordBatch batch : records.batches()) {
            validateBatch(topicPartition, firstBatch, batch, origin, toMagic, metricsRecorder);

            long maxBatchTimestamp = RecordBatch.NO_TIMESTAMP;
            long offsetOfMaxBatchTimestamp = -1L;

            List<ApiRecordError> recordErrors = new ArrayList<>(0);
            // This is a hot path and we want to avoid any unnecessary allocations.
            // That said, there is no benefit in using `skipKeyValueIterator` for the uncompressed
            // case since we don't do key/value copies in this path (we just slice the ByteBuffer)
            int batchIndex = 0;
            for (Record record : batch) {
                Optional<ApiRecordError> recordError = validateRecord(batch, topicPartition, record,
                    batchIndex, now, timestampType, timestampDiffMaxMs, compactedTopic, metricsRecorder);
                recordError.ifPresent(e -> recordErrors.add(e));

                long offset = offsetCounter.value++;
                if (batch.magic() > RecordBatch.MAGIC_VALUE_V0 && record.timestamp() > maxBatchTimestamp) {
                    maxBatchTimestamp = record.timestamp();
                    offsetOfMaxBatchTimestamp = offset;
                }
                ++batchIndex;
            }

            processRecordErrors(recordErrors);

            if (batch.magic() > RecordBatch.MAGIC_VALUE_V0 && maxBatchTimestamp > maxTimestamp) {
                maxTimestamp = maxBatchTimestamp;
                offsetOfMaxTimestamp = offsetOfMaxBatchTimestamp;
            }

            batch.setLastOffset(offsetCounter.value - 1);

            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2)
                batch.setPartitionLeaderEpoch(partitionLeaderEpoch);

            if (batch.magic() > RecordBatch.MAGIC_VALUE_V0) {
                if (timestampType == TimestampType.LOG_APPEND_TIME)
                    batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, now);
                else
                    batch.setMaxTimestamp(timestampType, maxBatchTimestamp);
            }
        }

        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            maxTimestamp = now;
            if (toMagic >= RecordBatch.MAGIC_VALUE_V2)
                offsetOfMaxTimestamp = offsetCounter.value - 1;
            else
                offsetOfMaxTimestamp = initialOffset;
        }

        return new ValidationResult(
            now,
            records,
            maxTimestamp,
            offsetOfMaxTimestamp,
            false,
            RecordConversionStats.EMPTY);
    }

    /**
     * We cannot do in place assignment in one of the following situations:
     * 1. Source and target compression codec are different
     * 2. When the target magic is not equal to batches' magic, meaning format conversion is needed.
     * 3. When the target magic is equal to V0, meaning absolute offsets need to be re-assigned.
     */
    // Visible for benchmarking
    public ValidationResult validateMessagesAndAssignOffsetsCompressed(LongRef offsetCounter,
                                                                       MetricsRecorder metricsRecorder,
                                                                       BufferSupplier bufferSupplier) {
        if (targetCompression == CompressionType.ZSTD && interBrokerProtocolVersion.isLessThan(IBP_2_1_IV0))
            throw new UnsupportedCompressionTypeException("Produce requests to inter.broker.protocol.version < 2.1 broker " +
                "are not allowed to use ZStandard compression");

        // No in place assignment situation 1
        boolean inPlaceAssignment = sourceCompression == targetCompression;
        long now = time.milliseconds();

        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        LongRef expectedInnerOffset = PrimitiveRef.ofLong(0);
        List<Record> validatedRecords = new ArrayList<>();

        int uncompressedSizeInBytes = 0;

        // Assume there's only one batch with compressed memory records; otherwise, return InvalidRecordException
        // One exception though is that with format smaller than v2, if sourceCompression is noCompression, then each batch is actually
        // a single record so we'd need to special handle it by creating a single wrapper batch that includes all the records
        MutableRecordBatch firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, sourceCompression);

        // No in place assignment situation 2 and 3: we only need to check for the first batch because:
        //  1. For most cases (compressed records, v2, for example), there's only one batch anyways.
        //  2. For cases that there may be multiple batches, all batches' magic should be the same.
        if (firstBatch.magic() != toMagic || toMagic == RecordBatch.MAGIC_VALUE_V0)
            inPlaceAssignment = false;

        // Do not compress control records unless they are written compressed
        if (sourceCompression == CompressionType.NONE && firstBatch.isControlBatch())
            inPlaceAssignment = true;

        for (MutableRecordBatch batch : records.batches()) {
            validateBatch(topicPartition, firstBatch, batch, origin, toMagic, metricsRecorder);
            uncompressedSizeInBytes += AbstractRecords.recordBatchHeaderSizeInBytes(toMagic, batch.compressionType());

            // if we are on version 2 and beyond, and we know we are going for in place assignment,
            // then we can optimize the iterator to skip key / value / headers since they would not be used at all
            CloseableIterator<Record> recordsIterator;
            if (inPlaceAssignment && firstBatch.magic() >= RecordBatch.MAGIC_VALUE_V2)
                recordsIterator = batch.skipKeyValueIterator(bufferSupplier);
            else
                recordsIterator = batch.streamingIterator(bufferSupplier);

            try {
                List<ApiRecordError> recordErrors = new ArrayList<>(0);
                // this is a hot path and we want to avoid any unnecessary allocations.
                int batchIndex = 0;
                while (recordsIterator.hasNext()) {
                    Record record = recordsIterator.next();
                    long expectedOffset = expectedInnerOffset.value++;

                    Optional<ApiRecordError> recordError = validateRecordCompression(sourceCompression,
                        batchIndex, record);
                    if (!recordError.isPresent()) {
                        recordError = validateRecord(batch, topicPartition, record, batchIndex, now,
                            timestampType, timestampDiffMaxMs, compactedTopic, metricsRecorder);
                    }

                    if (!recordError.isPresent()
                            && batch.magic() > RecordBatch.MAGIC_VALUE_V0
                            && toMagic > RecordBatch.MAGIC_VALUE_V0) {

                        if (record.timestamp() > maxTimestamp)
                            maxTimestamp = record.timestamp();

                        // Some older clients do not implement the V1 internal offsets correctly.
                        // Historically the broker handled this by rewriting the batches rather
                        // than rejecting the request. We must continue this handling here to avoid
                        // breaking these clients.
                        if (record.offset() != expectedOffset)
                            inPlaceAssignment = false;
                    }

                    if (recordError.isPresent())
                        recordErrors.add(recordError.get());
                    else {
                        uncompressedSizeInBytes += record.sizeInBytes();
                        validatedRecords.add(record);
                    }

                    ++batchIndex;
                }

                processRecordErrors(recordErrors);

            } finally {
                recordsIterator.close();
            }
        }

        if (!inPlaceAssignment) {
            return buildRecordsAndAssignOffsets(offsetCounter, now, firstBatch, validatedRecords,
                uncompressedSizeInBytes);
        } else {
            // we can update the batch only and write the compressed payload as is;
            // again we assume only one record batch within the compressed set
            offsetCounter.value += validatedRecords.size();
            long lastOffset = offsetCounter.value - 1;
            firstBatch.setLastOffset(lastOffset);

            if (timestampType == TimestampType.LOG_APPEND_TIME)
                maxTimestamp = now;

            if (toMagic >= RecordBatch.MAGIC_VALUE_V1)
                firstBatch.setMaxTimestamp(timestampType, maxTimestamp);

            if (toMagic >= RecordBatch.MAGIC_VALUE_V2)
                firstBatch.setPartitionLeaderEpoch(partitionLeaderEpoch);

            RecordConversionStats recordConversionStats = new RecordConversionStats(uncompressedSizeInBytes, 0, 0);
            return new ValidationResult(
                now,
                records,
                maxTimestamp,
                lastOffset,
                false,
                recordConversionStats);
        }
    }

    private ValidationResult buildRecordsAndAssignOffsets(LongRef offsetCounter,
                                                          long logAppendTime,
                                                          RecordBatch firstBatch,
                                                          List<Record> validatedRecords,
                                                          int uncompressedSizeInBytes) {
        long startNanos = time.nanoseconds();
        int estimatedSize = AbstractRecords.estimateSizeInBytes(toMagic, offsetCounter.value, targetCompression,
            validatedRecords);
        // The current implementation of BufferSupplier is naive and works best when the buffer size
        // cardinality is low, so don't use it here
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, toMagic, targetCompression,
            timestampType, offsetCounter.value, logAppendTime, firstBatch.producerId(),
            firstBatch.producerEpoch(), firstBatch.baseSequence(), firstBatch.isTransactional(),
            partitionLeaderEpoch);

        for (Record record : validatedRecords)
            builder.appendWithOffset(offsetCounter.value++, record);

        MemoryRecords records = builder.build();

        RecordsInfo info = builder.info();

        // This is not strictly correct, it represents the number of records where in-place assignment is not possible
        // instead of the number of records that were converted. It will over-count cases where the source and target are
        // message format V0 or if the inner offsets are not consecutive. This is OK since the impact is the same: we have
        // to rebuild the records (including recompression if enabled).
        int conversionCount = builder.numRecords();
        RecordConversionStats recordConversionStats = new RecordConversionStats(
            uncompressedSizeInBytes + builder.uncompressedBytesWritten(), conversionCount,
            time.nanoseconds() - startNanos);

        return new ValidationResult(
            logAppendTime,
            records,
            info.maxTimestamp,
            info.shallowOffsetOfMaxTimestamp,
            true,
            recordConversionStats);
    }


    private static void validateBatch(TopicPartition topicPartition,
                                      RecordBatch firstBatch,
                                      RecordBatch batch,
                                      AppendOrigin origin,
                                      byte toMagic,
                                      MetricsRecorder metricsRecorder) {
        // batch magic byte should have the same magic as the first batch
        if (firstBatch.magic() != batch.magic()) {
            metricsRecorder.recordInvalidMagic();
            throw new InvalidRecordException("Batch magic " + batch.magic() + " is not the same as the first batch's magic byte "
                + firstBatch.magic() + " in topic partition " + topicPartition);
        }

        if (origin == AppendOrigin.CLIENT) {
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                long countFromOffsets = batch.lastOffset() - batch.baseOffset() + 1;
                if (countFromOffsets <= 0) {
                    metricsRecorder.recordInvalidOffset();
                    throw new InvalidRecordException("Batch has an invalid offset range: [" + batch.baseOffset() + ", "
                        + batch.lastOffset() + "] in topic partition " + topicPartition);
                }

                // v2 and above messages always have a non-null count
                long count = batch.countOrNull();
                if (count <= 0) {
                    metricsRecorder.recordInvalidOffset();
                    throw new InvalidRecordException("Invalid reported count for record batch: " + count
                        + " in topic partition " + topicPartition);
                }

                if (countFromOffsets != count) {
                    metricsRecorder.recordInvalidOffset();
                    throw new InvalidRecordException("Inconsistent batch offset range [" + batch.baseOffset() + ", "
                        + batch.lastOffset() + "] and count of records " + count + " in topic partition " + topicPartition);
                }
            }

            if (batch.isControlBatch()) {
                metricsRecorder.recordInvalidOffset();
                throw new InvalidRecordException("Clients are not allowed to write control records in topic partition " + topicPartition);
            }

            if (batch.hasProducerId() && batch.baseSequence() < 0) {
                metricsRecorder.recordInvalidSequence();
                throw new InvalidRecordException("Invalid sequence number " + batch.baseSequence() + " in record batch with producerId "
                    + batch.producerId() + " in topic partition " + topicPartition);
            }
        }

        if (batch.isTransactional() && toMagic < RecordBatch.MAGIC_VALUE_V2)
            throw new UnsupportedForMessageFormatException("Transactional records cannot be used with magic version " + toMagic);

        if (batch.hasProducerId() && toMagic < RecordBatch.MAGIC_VALUE_V2)
            throw new UnsupportedForMessageFormatException("Idempotent records cannot be used with magic version " + toMagic);
    }

    private static Optional<ApiRecordError> validateRecord(RecordBatch batch,
                                                           TopicPartition topicPartition,
                                                           Record record,
                                                           int batchIndex,
                                                           long now,
                                                           TimestampType timestampType,
                                                           long timestampDiffMaxMs,
                                                           boolean compactedTopic,
                                                           MetricsRecorder metricsRecorder) {
        if (!record.hasMagic(batch.magic())) {
            metricsRecorder.recordInvalidMagic();
            return Optional.of(new ApiRecordError(Errors.INVALID_RECORD,
                new RecordError(batchIndex, "Record " + record
                    + "'s magic does not match outer magic " + batch.magic() + " in topic partition "
                    + topicPartition)));
        }

        // verify the record-level CRC only if this is one of the deep entries of a compressed message
        // set for magic v0 and v1. For non-compressed messages, there is no inner record for magic v0 and v1,
        // so we depend on the batch-level CRC check in Log.analyzeAndValidateRecords(). For magic v2 and above,
        // there is no record-level CRC to check.
        if (batch.magic() <= RecordBatch.MAGIC_VALUE_V1 && batch.isCompressed()) {
            try {
                record.ensureValid();
            } catch (InvalidRecordException e) {
                metricsRecorder.recordInvalidChecksums();
                throw new CorruptRecordException(e.getMessage() + " in topic partition " + topicPartition);
            }
        }

        Optional<ApiRecordError> keyError = validateKey(record, batchIndex, topicPartition,
            compactedTopic, metricsRecorder);
        if (keyError.isPresent())
            return keyError;
        else
            return validateTimestamp(batch, record, batchIndex, now, timestampType, timestampDiffMaxMs);
    }

    private static Optional<ApiRecordError> validateKey(Record record,
                                                        int batchIndex,
                                                        TopicPartition topicPartition,
                                                        boolean compactedTopic,
                                                        MetricsRecorder metricsRecorder) {
        if (compactedTopic && !record.hasKey()) {
            metricsRecorder.recordNoKeyCompactedTopic();
            return Optional.of(new ApiRecordError(Errors.INVALID_RECORD, new RecordError(batchIndex,
                "Compacted topic cannot accept message without key in topic partition "
                + topicPartition)));
        } else
            return Optional.empty();
    }

    private static Optional<ApiRecordError> validateTimestamp(RecordBatch batch,
                                                              Record record,
                                                              int batchIndex,
                                                              long now,
                                                              TimestampType timestampType,
                                                              long timestampDiffMaxMs) {
        if (timestampType == TimestampType.CREATE_TIME
                && record.timestamp() != RecordBatch.NO_TIMESTAMP
                && Math.abs(record.timestamp() - now) > timestampDiffMaxMs)
            return Optional.of(new ApiRecordError(Errors.INVALID_TIMESTAMP, new RecordError(batchIndex,
                "Timestamp " + record.timestamp() + " of message with offset " + record.offset()
                + " is out of range. The timestamp should be within [" + (now - timestampDiffMaxMs)
                + ", " + (now + timestampDiffMaxMs) + "]")));
        else if (batch.timestampType() == TimestampType.LOG_APPEND_TIME)
            return Optional.of(new ApiRecordError(Errors.INVALID_TIMESTAMP, new RecordError(batchIndex,
                "Invalid timestamp type in message " + record + ". Producer should not set timestamp "
                + "type to LogAppendTime.")));
        else
            return Optional.empty();
    }

    private static Optional<ApiRecordError> validateRecordCompression(CompressionType sourceCompression,
                                                                      int batchIndex,
                                                                      Record record) {
        if (sourceCompression != CompressionType.NONE && record.isCompressed())
            return Optional.of(new ApiRecordError(Errors.INVALID_RECORD, new RecordError(batchIndex,
                "Compressed outer record should not have an inner record with a compression attribute set: "
                + record)));
        else
            return Optional.empty();
    }

    private static void processRecordErrors(List<ApiRecordError> recordErrors) {
        if (!recordErrors.isEmpty()) {
            List<RecordError> errors = recordErrors.stream().map(e -> e.recordError).collect(Collectors.toList());
            if (recordErrors.stream().anyMatch(e -> e.apiError == Errors.INVALID_TIMESTAMP)) {
                throw new RecordValidationException(new InvalidTimestampException(
                    "One or more records have been rejected due to invalid timestamp"), errors);
            } else {
                throw new RecordValidationException(new InvalidRecordException(
                    "One or more records have been rejected due to " + errors.size() + " record errors "
                    + "in total, and only showing the first three errors at most: " + errors.subList(0, Math.min(errors.size(), 3))), errors);
            }
        }
    }
}
