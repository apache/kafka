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
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * {@link CompletedFetch} represents a {@link RecordBatch batch} of {@link Record records} that was returned from the
 * broker via a {@link FetchRequest}. It contains logic to maintain state between calls to
 * {@link #fetchRecords(FetchConfig, Deserializers, int)}.
 */
public class CompletedFetch {

    final TopicPartition partition;
    final FetchResponseData.PartitionData partitionData;
    final short requestVersion;

    private final Logger log;
    private final SubscriptionState subscriptions;
    private final BufferSupplier decompressionBufferSupplier;
    private final Iterator<? extends RecordBatch> batches;
    private final Set<Long> abortedProducerIds;
    private final PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions;
    private final FetchMetricsAggregator metricAggregator;

    private int recordsRead;
    private int bytesRead;
    private RecordBatch currentBatch;
    private Record lastRecord;
    private CloseableIterator<Record> records;
    private Exception cachedRecordException = null;
    private boolean corruptLastRecord = false;
    private long nextFetchOffset;
    private Optional<Integer> lastEpoch;
    private boolean isConsumed = false;
    private boolean initialized = false;

    CompletedFetch(LogContext logContext,
                   SubscriptionState subscriptions,
                   BufferSupplier decompressionBufferSupplier,
                   TopicPartition partition,
                   FetchResponseData.PartitionData partitionData,
                   FetchMetricsAggregator metricAggregator,
                   Long fetchOffset,
                   short requestVersion) {
        this.log = logContext.logger(CompletedFetch.class);
        this.subscriptions = subscriptions;
        this.decompressionBufferSupplier = decompressionBufferSupplier;
        this.partition = partition;
        this.partitionData = partitionData;
        this.metricAggregator = metricAggregator;
        this.batches = FetchResponse.recordsOrFail(partitionData).batches().iterator();
        this.nextFetchOffset = fetchOffset;
        this.requestVersion = requestVersion;
        this.lastEpoch = Optional.empty();
        this.abortedProducerIds = new HashSet<>();
        this.abortedTransactions = abortedTransactions(partitionData);
    }

    long nextFetchOffset() {
        return nextFetchOffset;
    }

    Optional<Integer> lastEpoch() {
        return lastEpoch;
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
     * After each partition is parsed, we update the current metric totals with the total bytes
     * and number of records parsed. After all partitions have reported, we write the metric.
     */
    void recordAggregatedMetrics(int bytes, int records) {
        metricAggregator.record(partition, bytes, records);
    }

    /**
     * Draining a {@link CompletedFetch} will signal that the data has been consumed and the underlying resources
     * are closed. This is somewhat analogous to {@link Closeable#close() closing}, though no error will result if a
     * caller invokes {@link #fetchRecords(FetchConfig, Deserializers, int)}; an empty {@link List list} will be
     * returned instead.
     */
    void drain() {
        if (!isConsumed) {
            maybeCloseRecordStream();
            cachedRecordException = null;
            this.isConsumed = true;
            recordAggregatedMetrics(bytesRead, recordsRead);

            // we move the partition to the end if we received some bytes. This way, it's more likely that partitions
            // for the same topic can remain together (allowing for more efficient serialization).
            if (bytesRead > 0)
                subscriptions.movePartitionToEnd(partition);
        }
    }

    private void maybeEnsureValid(FetchConfig fetchConfig, RecordBatch batch) {
        if (fetchConfig.checkCrcs && batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            try {
                batch.ensureValid();
            } catch (CorruptRecordException e) {
                throw new KafkaException("Record batch for partition " + partition + " at offset " +
                        batch.baseOffset() + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    private void maybeEnsureValid(FetchConfig fetchConfig, Record record) {
        if (fetchConfig.checkCrcs) {
            try {
                record.ensureValid();
            } catch (CorruptRecordException e) {
                throw new KafkaException("Record for partition " + partition + " at offset " + record.offset()
                        + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    private void maybeCloseRecordStream() {
        if (records != null) {
            records.close();
            records = null;
        }
    }

    private Record nextFetchedRecord(FetchConfig fetchConfig) {
        while (true) {
            if (records == null || !records.hasNext()) {
                maybeCloseRecordStream();

                if (!batches.hasNext()) {
                    // Message format v2 preserves the last offset in a batch even if the last record is removed
                    // through compaction. By using the next offset computed from the last offset in the batch,
                    // we ensure that the offset of the next fetch will point to the next batch, which avoids
                    // unnecessary re-fetching of the same batch (in the worst case, the consumer could get stuck
                    // fetching the same batch repeatedly).
                    if (currentBatch != null)
                        nextFetchOffset = currentBatch.nextOffset();
                    drain();
                    return null;
                }

                currentBatch = batches.next();
                lastEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch());
                maybeEnsureValid(fetchConfig, currentBatch);

                if (fetchConfig.isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                    // remove from the aborted transaction queue all aborted transactions which have begun
                    // before the current batch's last offset and add the associated producerIds to the
                    // aborted producer set
                    consumeAbortedTransactionsUpTo(currentBatch.lastOffset());

                    long producerId = currentBatch.producerId();
                    if (containsAbortMarker(currentBatch)) {
                        abortedProducerIds.remove(producerId);
                    } else if (isBatchAborted(currentBatch)) {
                        log.debug("Skipping aborted record batch from partition {} with producerId {} and " +
                                        "offsets {} to {}",
                                partition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset());
                        nextFetchOffset = currentBatch.nextOffset();
                        continue;
                    }
                }

                records = currentBatch.streamingIterator(decompressionBufferSupplier);
            } else {
                Record record = records.next();
                // skip any records out of range
                if (record.offset() >= nextFetchOffset) {
                    // we only do validation when the message should not be skipped.
                    maybeEnsureValid(fetchConfig, record);

                    // control records are not returned to the user
                    if (!currentBatch.isControlBatch()) {
                        return record;
                    } else {
                        // Increment the next fetch offset when we skip a control batch.
                        nextFetchOffset = record.offset() + 1;
                    }
                }
            }
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
     * @return {@link ConsumerRecord Consumer records}
     */
    <K, V> List<ConsumerRecord<K, V>> fetchRecords(FetchConfig fetchConfig,
                                                   Deserializers<K, V> deserializers,
                                                   int maxRecords) {
        // Error when fetching the next record before deserialization.
        if (corruptLastRecord)
            throw new KafkaException("Received exception when fetching the next record from " + partition
                    + ". If needed, please seek past the record to "
                    + "continue consumption.", cachedRecordException);

        if (isConsumed)
            return Collections.emptyList();

        List<ConsumerRecord<K, V>> records = new ArrayList<>();

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
                records.add(record);
                recordsRead++;
                bytesRead += lastRecord.sizeInBytes();
                nextFetchOffset = lastRecord.offset() + 1;
                // In some cases, the deserialization may have thrown an exception and the retry may succeed,
                // we allow user to move forward in this case.
                cachedRecordException = null;
            }
        } catch (SerializationException se) {
            cachedRecordException = se;
            if (records.isEmpty())
                throw se;
        } catch (KafkaException e) {
            cachedRecordException = e;
            if (records.isEmpty())
                throw new KafkaException("Received exception when fetching the next record from " + partition
                        + ". If needed, please seek past the record to "
                        + "continue consumption.", e);
        }
        return records;
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    <K, V> ConsumerRecord<K, V> parseRecord(Deserializers<K, V> deserializers,
                                            TopicPartition partition,
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
            throw new RecordDeserializationException(partition, record.offset(),
                    "Error deserializing key/value for partition " + partition +
                            " at offset " + record.offset() + ". If needed, please seek past the record to continue consumption.", e);
        }
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }

    private void consumeAbortedTransactionsUpTo(long offset) {
        if (abortedTransactions == null)
            return;

        while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset() <= offset) {
            FetchResponseData.AbortedTransaction abortedTransaction = abortedTransactions.poll();
            abortedProducerIds.add(abortedTransaction.producerId());
        }
    }

    private boolean isBatchAborted(RecordBatch batch) {
        return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
    }

    private PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions(FetchResponseData.PartitionData partition) {
        if (partition.abortedTransactions() == null || partition.abortedTransactions().isEmpty())
            return null;

        PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions = new PriorityQueue<>(
                partition.abortedTransactions().size(), Comparator.comparingLong(FetchResponseData.AbortedTransaction::firstOffset)
        );
        abortedTransactions.addAll(partition.abortedTransactions());
        return abortedTransactions;
    }

    private boolean containsAbortMarker(RecordBatch batch) {
        if (!batch.isControlBatch())
            return false;

        Iterator<Record> batchIterator = batch.iterator();
        if (!batchIterator.hasNext())
            return false;

        Record firstRecord = batchIterator.next();
        return ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key());
    }
}
