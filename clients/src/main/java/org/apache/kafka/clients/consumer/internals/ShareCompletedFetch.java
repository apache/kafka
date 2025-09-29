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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;

/**
 * {@link ShareCompletedFetch} represents a {@link RecordBatch batch} of {@link Record records}
 * that was returned from the broker via a {@link ShareFetchRequest}. It contains logic to maintain
 * state between calls to {@link #fetchRecords(Deserializers, int, boolean)}. Although it has
 * similarities with {@link CompletedFetch}, the details are quite different, such as not needing
 * to keep track of aborted transactions or the need to keep track of fetch position.
 */
public class ShareCompletedFetch {

    final TopicIdPartition partition;
    final ShareFetchResponseData.PartitionData partitionData;
    final short requestVersion;

    private final Logger log;
    private final BufferSupplier decompressionBufferSupplier;
    private final Iterator<? extends RecordBatch> batches;
    private int recordsRead;
    private int bytesRead;
    private RecordBatch currentBatch;
    private Record lastRecord;
    private CloseableIterator<Record> records;
    private KafkaException cachedBatchException = null;
    private KafkaException cachedRecordException = null;
    private boolean isConsumed = false;
    private boolean initialized = false;
    private final List<OffsetAndDeliveryCount> acquiredRecordList;
    private ListIterator<OffsetAndDeliveryCount> acquiredRecordIterator;
    private OffsetAndDeliveryCount nextAcquired;
    private final ShareFetchMetricsAggregator metricAggregator;

    ShareCompletedFetch(final LogContext logContext,
                        final BufferSupplier decompressionBufferSupplier,
                        final TopicIdPartition partition,
                        final ShareFetchResponseData.PartitionData partitionData,
                        final ShareFetchMetricsAggregator metricAggregator,
                        final short requestVersion) {
        this.log = logContext.logger(org.apache.kafka.clients.consumer.internals.ShareCompletedFetch.class);
        this.decompressionBufferSupplier = decompressionBufferSupplier;
        this.partition = partition;
        this.partitionData = partitionData;
        this.metricAggregator = metricAggregator;
        this.requestVersion = requestVersion;
        this.batches = ShareFetchResponse.recordsOrFail(partitionData).batches().iterator();
        this.acquiredRecordList = buildAcquiredRecordList(partitionData.acquiredRecords());
        this.nextAcquired = null;
    }

    private List<OffsetAndDeliveryCount> buildAcquiredRecordList(List<ShareFetchResponseData.AcquiredRecords> partitionAcquiredRecords) {
        List<OffsetAndDeliveryCount> acquiredRecordList = new LinkedList<>();
        partitionAcquiredRecords.forEach(acquiredRecords -> {
            for (long offset = acquiredRecords.firstOffset(); offset <= acquiredRecords.lastOffset(); offset++) {
                acquiredRecordList.add(new OffsetAndDeliveryCount(offset, acquiredRecords.deliveryCount()));
            }
        });
        return acquiredRecordList;
    }

    boolean isInitialized() {
        return initialized;
    }

    void setInitialized() {
        this.initialized = true;
    }

    public boolean isConsumed() {
        return isConsumed;
    }

    /**
     * Draining a {@link ShareCompletedFetch} will signal that the data has been consumed and the underlying resources
     * are closed. This is somewhat analogous to {@link Closeable#close() closing}, though no error will result if a
     * caller invokes {@link #fetchRecords(Deserializers, int, boolean)}; an empty {@link List list} will be
     * returned instead.
     */
    void drain() {
        if (!isConsumed) {
            maybeCloseRecordStream();
            cachedRecordException = null;
            cachedBatchException = null;
            this.isConsumed = true;
            recordAggregatedMetrics(bytesRead, recordsRead);
        }
    }

    /**
     * After each partition is parsed, we update the current metric totals with the total bytes
     * and number of records parsed. After all partitions have reported, we write the metric.
     */
    void recordAggregatedMetrics(int bytes, int records) {
        metricAggregator.record(partition.topicPartition(), bytes, records);
    }

    /**
     * The {@link RecordBatch batch} of {@link Record records} is converted to a {@link List list} of
     * {@link ConsumerRecord consumer records} and returned. {@link BufferSupplier Decompression} and
     * {@link Deserializer deserialization} of the {@link Record record's} key and value are performed in
     * this step.
     *
     * @param deserializers {@link Deserializer}s to use to convert the raw bytes to the expected key and value types
     * @param maxRecords The number of records to return; the number returned may be {@code 0 <= maxRecords}
     * @param checkCrcs Whether to check the CRC of fetched records
     *
     * @return {@link ShareInFlightBatch The ShareInFlightBatch containing records and their acknowledgments}
     */
    <K, V> ShareInFlightBatch<K, V> fetchRecords(final Deserializers<K, V> deserializers,
                                                 final int maxRecords,
                                                 final boolean checkCrcs) {
        // Creating an empty ShareInFlightBatch
        ShareInFlightBatch<K, V> inFlightBatch = new ShareInFlightBatch<>(partition);

        if (cachedBatchException != null) {
            // If the event that a CRC check fails, reject the entire record batch because it is corrupt.
            rejectRecordBatch(inFlightBatch, currentBatch);
            inFlightBatch.setException(cachedBatchException);
            cachedBatchException = null;
            return inFlightBatch;
        }

        if (cachedRecordException != null) {
            inFlightBatch.addAcknowledgement(lastRecord.offset(), AcknowledgeType.RELEASE);
            inFlightBatch.setException(cachedRecordException);
            cachedRecordException = null;
            return inFlightBatch;
        }

        if (isConsumed)
            return inFlightBatch;

        initializeNextAcquired();

        try {
            int recordsInBatch = 0;
            boolean currentBatchHasMoreRecords = false;

            while (recordsInBatch < maxRecords || currentBatchHasMoreRecords) {
                currentBatchHasMoreRecords = nextFetchedRecord(checkCrcs);
                if (lastRecord == null) {
                    // Any remaining acquired records are gaps
                    while (nextAcquired != null) {
                        inFlightBatch.addGap(nextAcquired.offset);
                        nextAcquired = nextAcquiredRecord();
                    }
                    break;
                }

                while (nextAcquired != null) {
                    if (lastRecord.offset() == nextAcquired.offset) {
                        // It's acquired, so we parse it and add it to the batch
                        Optional<Integer> leaderEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch());
                        TimestampType timestampType = currentBatch.timestampType();
                        ConsumerRecord<K, V> record = parseRecord(deserializers, partition, leaderEpoch,
                                timestampType, lastRecord, nextAcquired.deliveryCount);
                        inFlightBatch.addRecord(record);
                        recordsRead++;
                        bytesRead += lastRecord.sizeInBytes();
                        recordsInBatch++;

                        nextAcquired = nextAcquiredRecord();
                        break;
                    } else if (lastRecord.offset() < nextAcquired.offset) {
                        // It's not acquired, so we skip it
                        break;
                    } else {
                        // It's acquired, but there's no non-control record at this offset, so it's a gap
                        inFlightBatch.addGap(nextAcquired.offset);
                    }

                    nextAcquired = nextAcquiredRecord();
                }
            }
        } catch (SerializationException se) {
            nextAcquired = nextAcquiredRecord();
            if (inFlightBatch.isEmpty()) {
                inFlightBatch.addAcknowledgement(lastRecord.offset(), AcknowledgeType.RELEASE);
                inFlightBatch.setException(se);
            } else {
                cachedRecordException = se;
                inFlightBatch.setHasCachedException(true);
            }
        } catch (CorruptRecordException e) {
            if (inFlightBatch.isEmpty()) {
                // If the event that a CRC check fails, reject the entire record batch because it is corrupt.
                rejectRecordBatch(inFlightBatch, currentBatch);
                inFlightBatch.setException(e);
            } else {
                cachedBatchException = e;
                inFlightBatch.setHasCachedException(true);
            }
        }

        return inFlightBatch;
    }

    private void initializeNextAcquired() {
        if (nextAcquired == null) {
            if (acquiredRecordIterator == null) {
                acquiredRecordIterator = acquiredRecordList.listIterator();
            }
            if (acquiredRecordIterator.hasNext()) {
                nextAcquired = acquiredRecordIterator.next();
            }
        }
    }

    private OffsetAndDeliveryCount nextAcquiredRecord() {
        if (acquiredRecordIterator.hasNext()) {
            return acquiredRecordIterator.next();
        }
        return null;
    }

    private <K, V> void rejectRecordBatch(final ShareInFlightBatch<K, V> inFlightBatch,
                                          final RecordBatch currentBatch) {
        // Rewind the acquiredRecordIterator to the start, so we are in a known state
        acquiredRecordIterator = acquiredRecordList.listIterator();

        OffsetAndDeliveryCount nextAcquired = nextAcquiredRecord();
        for (long offset = currentBatch.baseOffset(); offset <= currentBatch.lastOffset(); offset++) {
            if (nextAcquired == null) {
                // No more acquired records, so we are done
                break;
            } else if (offset == nextAcquired.offset) {
                // It's acquired, so we reject it
                inFlightBatch.addAcknowledgement(offset, AcknowledgeType.REJECT);
            } else if (offset < nextAcquired.offset) {
                // It's not acquired, so we skip it
                continue;
            }

            nextAcquired = nextAcquiredRecord();
        }
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    <K, V> ConsumerRecord<K, V> parseRecord(final Deserializers<K, V> deserializers,
                                            final TopicIdPartition partition,
                                            final Optional<Integer> leaderEpoch,
                                            final TimestampType timestampType,
                                            final Record record,
                                            final short deliveryCount) {
        Headers headers = new RecordHeaders(record.headers());
        ByteBuffer keyBytes = record.key();
        ByteBuffer valueBytes = record.value();
        K key;
        V value;
        try {
            key = keyBytes == null ? null : deserializers.keyDeserializer.deserialize(partition.topic(), headers, keyBytes);
        } catch (RuntimeException e) {
            log.error("Key Deserializers with error: {}", deserializers);
            throw newRecordDeserializationException(RecordDeserializationException.DeserializationExceptionOrigin.KEY, partition.topicPartition(), timestampType, record, e, headers);
        }
        try {
            value = valueBytes == null ? null : deserializers.valueDeserializer.deserialize(partition.topic(), headers, valueBytes);
        } catch (RuntimeException e) {
            log.error("Value Deserializers with error: {}", deserializers);
            throw newRecordDeserializationException(RecordDeserializationException.DeserializationExceptionOrigin.VALUE, partition.topicPartition(), timestampType, record, e, headers);
        }
        return new ConsumerRecord<>(partition.topic(), partition.partition(), record.offset(),
                record.timestamp(), timestampType,
                keyBytes == null ? ConsumerRecord.NULL_SIZE : keyBytes.remaining(),
                valueBytes == null ? ConsumerRecord.NULL_SIZE : valueBytes.remaining(),
                key, value, headers, leaderEpoch, Optional.of(deliveryCount));
    }

    private static RecordDeserializationException newRecordDeserializationException(RecordDeserializationException.DeserializationExceptionOrigin origin,
                                                                                    TopicPartition partition,
                                                                                    TimestampType timestampType,
                                                                                    Record record,
                                                                                    RuntimeException e,
                                                                                    Headers headers) {
        return new RecordDeserializationException(origin, partition, record.offset(), record.timestamp(), timestampType, record.key(), record.value(), headers,
                "Error deserializing " + origin.name() + " for partition " + partition + " at offset " + record.offset()
                        + ". The record has been released.", e);
    }

    /**
     * Scans for the next record in the available batches, skipping control records
     *
     * @param checkCrcs Whether to check the CRC of fetched records
     *
     * @return true if the current batch has more records, else false
     */
    private boolean nextFetchedRecord(final boolean checkCrcs) {
        while (true) {
            if (records == null || !records.hasNext()) {
                maybeCloseRecordStream();

                if (!batches.hasNext()) {
                    drain();
                    lastRecord = null;
                    break;
                }

                currentBatch = batches.next();
                maybeEnsureValid(currentBatch, checkCrcs);

                records = currentBatch.streamingIterator(decompressionBufferSupplier);
            } else {
                Record record = records.next();
                maybeEnsureValid(record, checkCrcs);

                // control records are not returned to the user
                if (!currentBatch.isControlBatch()) {
                    lastRecord = record;
                    break;
                }
            }
        }

        return records != null && records.hasNext();
    }

    private Optional<Integer> maybeLeaderEpoch(final int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }

    private void maybeEnsureValid(RecordBatch batch, boolean checkCrcs) {
        if (checkCrcs && batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            try {
                batch.ensureValid();
            } catch (CorruptRecordException e) {
                throw new CorruptRecordException("Record batch for partition " + partition.topicPartition()
                        + " at offset " + batch.baseOffset() + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    private void maybeEnsureValid(final Record record, final boolean checkCrcs) {
        if (checkCrcs) {
            try {
                record.ensureValid();
            } catch (CorruptRecordException e) {
                throw new CorruptRecordException("Record for partition " + partition.topicPartition()
                        + " at offset " + record.offset() + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    private void maybeCloseRecordStream() {
        if (records != null) {
            records.close();
            records = null;
        }
    }

    private static class OffsetAndDeliveryCount {
        final long offset;
        final short deliveryCount;

        OffsetAndDeliveryCount(long offset, short deliveryCount) {
            this.offset = offset;
            this.deliveryCount = deliveryCount;
        }

        @Override
        public String toString() {
            return "OffsetAndDeliveryCount{" +
                    "offset=" + offset +
                    ", deliveryCount=" + deliveryCount +
                    "}";
        }
    }
}
