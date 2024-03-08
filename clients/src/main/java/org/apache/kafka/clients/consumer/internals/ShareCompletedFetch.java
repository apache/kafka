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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
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
import java.util.List;
import java.util.Optional;

public class ShareCompletedFetch {

    /**
     * {@link ShareCompletedFetch} represents a {@link RecordBatch batch} of {@link Record records}
     * that was returned from the broker via a {@link ShareFetchRequest}. It contains logic to maintain
     * state between calls to {@link #fetchRecords(FetchConfig, Deserializers, int)}. Although it has
     * similarities with {@link CompletedFetch}, the details are quite different, such as not needing
     * to keep track of aborted transactions or the need to keep track of fetch position.
     */
    final TopicIdPartition partition;

    final ShareFetchResponseData.PartitionData partitionData;

    final short requestVersion;

    private final Logger log;
    private final BufferSupplier decompressionBufferSupplier;
    private final Iterator<? extends RecordBatch> batches;
    private RecordBatch currentBatch;
    private Record lastRecord;
    private CloseableIterator<Record> records;
    private Exception cachedRecordException = null;
    private boolean corruptLastRecord = false;
    private boolean isConsumed = false;
    private boolean initialized = false;
    public List<ShareFetchResponseData.AcquiredRecords> acquiredRecords;
    private int currentIndex;

    ShareCompletedFetch(LogContext logContext,
                        BufferSupplier decompressionBufferSupplier,
                        TopicIdPartition partition,
                        ShareFetchResponseData.PartitionData partitionData,
                        short requestVersion) {
        this.log = logContext.logger(org.apache.kafka.clients.consumer.internals.ShareCompletedFetch.class);
        this.decompressionBufferSupplier = decompressionBufferSupplier;
        this.partition = partition;
        this.partitionData = partitionData;
        this.requestVersion = requestVersion;
        this.batches = ShareFetchResponse.recordsOrFail(partitionData).batches().iterator();
        this.acquiredRecords = partitionData.acquiredRecords();
        this.currentIndex = 0;
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
     * caller invokes {@link #fetchRecords(FetchConfig, Deserializers, int)}; an empty {@link List list} will be
     * returned instead.
     */
    void drain() {
        if (!isConsumed) {
            maybeCloseRecordStream();
            cachedRecordException = null;
            this.isConsumed = true;
        }
    }

    /**
     * The {@link RecordBatch batch} of {@link Record records} is converted to a {@link List list} of
     * {@link ConsumerRecord consumer records} and returned. {@link BufferSupplier Decompression} and
     * {@link Deserializer deserialization} of the {@link Record record's} key and value are performed in
     * this step.
     *
     * @param fetchConfig {@link FetchConfig Configuration} to use
     * @param deserializers {@link Deserializer}s to use to convert the raw bytes to the expected key and value types
     * @param maxRecords The number of records to return; the number returned may be {@code 0 <= maxRecords}
     * @return {@link ShareInFlightBatch The ShareInFlightBatch containing records and their acknowledgments}
     */
    <K, V> ShareInFlightBatch<K, V> fetchRecords(FetchConfig fetchConfig,
                                                   Deserializers<K, V> deserializers,
                                                   int maxRecords) {
        // Error when fetching the next record before deserialization.
        if (corruptLastRecord)
            throw new KafkaException("Received exception when fetching the next record from " + partition
                    + ". If needed, please seek past the record to "
                    + "continue consumption.", cachedRecordException);

        // Creating an empty shareInFlightBatch
        ShareInFlightBatch<K, V> shareInFlightBatch = new ShareInFlightBatch<>(partition);

        if (isConsumed)
            return shareInFlightBatch;

        try {
            for (int i = 0; i < maxRecords; i++) {
                // Only move to next record if there was no exception in the last fetch. Otherwise, we should
                // use the last record to do deserialization again.
                if (cachedRecordException == null) {
                    corruptLastRecord = true;
                    lastRecord = nextFetchedRecord(fetchConfig);
                    corruptLastRecord = false;
                }

                if (lastRecord == null)
                    break;

                Optional<Integer> leaderEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch());
                TimestampType timestampType = currentBatch.timestampType();
                ConsumerRecord<K, V> record = parseRecord(deserializers, partition, leaderEpoch, timestampType, lastRecord);
                // Check if the record is in acquired records.
                if (acquiredRecords.isEmpty() || isAcquired(record)) {
                    shareInFlightBatch.addRecord(record);
                }

                // In some cases, the deserialization may have thrown an exception and the retry may succeed,
                // we allow user to move forward in this case.
                cachedRecordException = null;
            }
        } catch (SerializationException se) {
            cachedRecordException = se;
            if (shareInFlightBatch.isEmpty())
                throw se;
        } catch (KafkaException e) {
            cachedRecordException = e;
            if (shareInFlightBatch.isEmpty())
                throw new KafkaException("Received exception when fetching the next record from " + partition
                        + ". If needed, please seek past the record to "
                        + "continue consumption.", e);
        }

        return shareInFlightBatch;
    }

    /**
     * Check if the record is part of the acquired records.
     */
    private <K, V> boolean isAcquired(ConsumerRecord<K, V> record) {
        if (currentIndex >= acquiredRecords.size()) return false;
        ShareFetchResponseData.AcquiredRecords acqRecord = acquiredRecords.get(currentIndex);
        if (record.offset() >= acqRecord.baseOffset() && record.offset() <= acqRecord.lastOffset()) {
            return true;
        } else if (record.offset() > acqRecord.lastOffset()) {
            currentIndex += 1;
            return isAcquired(record);
        } else {
            return false;
        }
    }


    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    <K, V> ConsumerRecord<K, V> parseRecord(Deserializers<K, V> deserializers,
                                            TopicIdPartition partition,
                                            Optional<Integer> leaderEpoch,
                                            TimestampType timestampType,
                                            Record record) {
        try {
            long offset = record.offset();
            long timestamp = record.timestamp();
            Headers headers = new RecordHeaders(record.headers());
            ByteBuffer keyBytes = record.key();
            K key = keyBytes == null ? null : deserializers.keyDeserializer.deserialize(partition.topic(), headers, keyBytes);
            ByteBuffer valueBytes = record.value();
            V value = valueBytes == null ? null : deserializers.valueDeserializer.deserialize(partition.topic(), headers, valueBytes);
            return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                    timestamp, timestampType,
                    keyBytes == null ? ConsumerRecord.NULL_SIZE : keyBytes.remaining(),
                    valueBytes == null ? ConsumerRecord.NULL_SIZE : valueBytes.remaining(),
                    key, value, headers, leaderEpoch);
        } catch (RuntimeException e) {
            log.error("Deserializers with error: {}", deserializers);
            throw new RecordDeserializationException(partition.topicPartition(), record.offset(),
                    "Error deserializing key/value for partition " + partition +
                            " at offset " + record.offset() + ". If needed, please seek past the record to continue consumption.", e);
        }
    }

    private Record nextFetchedRecord(FetchConfig fetchConfig) {
        while (true) {
            if (records == null || !records.hasNext()) {
                maybeCloseRecordStream();

                if (!batches.hasNext()) {
                    drain();
                    return null;
                }

                currentBatch = batches.next();
                maybeEnsureValid(fetchConfig, currentBatch);

                records = currentBatch.streamingIterator(decompressionBufferSupplier);
            } else {
                Record record = records.next();
                maybeEnsureValid(fetchConfig, record);

                // control records are not returned to the user
                if (!currentBatch.isControlBatch()) {
                    return record;
                }
            }
        }
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }

    private void maybeEnsureValid(FetchConfig fetchConfig, RecordBatch batch) {
        if (fetchConfig.checkCrcs && batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            try {
                batch.ensureValid();
            } catch (CorruptRecordException e) {
                throw new KafkaException("Record batch for partition " + partition.topicPartition()
                        + " at offset " + batch.baseOffset() + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    private void maybeEnsureValid(FetchConfig fetchConfig, Record record) {
        if (fetchConfig.checkCrcs) {
            try {
                record.ensureValid();
            } catch (CorruptRecordException e) {
                throw new KafkaException("Record for partition " + partition.topicPartition()
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

}
