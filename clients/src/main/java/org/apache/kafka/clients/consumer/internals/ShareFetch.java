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
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * {@link ShareFetch} represents the records fetched from the broker to be returned to the consumer
 * to satisfy a {@link KafkaShareConsumer#poll(Duration)} call. The records can come from multiple
 * topic-partitions.
 *
 * @param <K> The record key
 * @param <V> The record value
 */
public class ShareFetch<K, V> {
    private final Map<TopicIdPartition, List<ShareInFlightBatch<K, V>>> batches;
    private Optional<Integer> acquisitionLockTimeoutMs;
    private Optional<Integer> acquisitionLockTimeoutMsRenewed;

    public static <K, V> ShareFetch<K, V> empty() {
        return new ShareFetch<>(new HashMap<>(), Optional.empty());
    }

    private ShareFetch(Map<TopicIdPartition, List<ShareInFlightBatch<K, V>>> batches, Optional<Integer> acquisitionLockTimeoutMs) {
        this.batches = batches;
        this.acquisitionLockTimeoutMs = acquisitionLockTimeoutMs;
        this.acquisitionLockTimeoutMsRenewed = Optional.empty();
    }

    /**
     * Add another {@link ShareInFlightBatch} to this one; all of its records will be added to this object's
     * {@link #records() records}. Generally, we will only have one {@link ShareInFlightBatch} for a partition
     * at a time, but in some cases (such as a repeated request after an empty response, or partition leader
     * changes), there might be more than one.
     *
     * @param partition the topic-partition
     * @param batch the batch to add; may not be null
     */
    public void add(TopicIdPartition partition, ShareInFlightBatch<K, V> batch) {
        Objects.requireNonNull(batch);
        batches.computeIfAbsent(partition, k -> new LinkedList<>()).add(batch);
        if (batch.getAcquisitionLockTimeoutMs().isPresent()) {
            acquisitionLockTimeoutMs = batch.getAcquisitionLockTimeoutMs();
        }
    }

    /**
     * @return all the non-control messages for this fetch, grouped by partition
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> records() {
        final LinkedHashMap<TopicPartition, List<ConsumerRecord<K, V>>> result = new LinkedHashMap<>();
        batches.forEach((tip, batchList) -> {
            List<ConsumerRecord<K, V>> records = new ArrayList<>();
            for (ShareInFlightBatch<K, V> batch : batchList) {
                records.addAll(batch.getInFlightRecords());
            }
            result.put(tip.topicPartition(), records);
        });
        return Map.copyOf(result);
    }

    /**
     * @return the total number of non-control messages for this fetch, across all partitions
     */
    public int numRecords() {
        int numRecords = 0;
        if (!batches.isEmpty()) {
            Iterator<Map.Entry<TopicIdPartition, List<ShareInFlightBatch<K, V>>>> iterator = batches.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<TopicIdPartition, List<ShareInFlightBatch<K, V>>> entry = iterator.next();
                List<ShareInFlightBatch<K, V>> batchList = entry.getValue();
                batchList.removeIf(batch -> batch.isEmpty() && !batch.hasRenewals());
                for (ShareInFlightBatch<K, V> batch : batchList) {
                    numRecords += batch.numRecords();
                }
                if (batchList.isEmpty()) {
                    iterator.remove();
                }
            }
        }

        return numRecords;
    }

    /**
     * @return {@code true} if and only if this fetch did not return any non-control records
     */
    public boolean isEmpty() {
        return numRecords() == 0;
    }

    /**
     * @return The most up-to-date value of acquisition lock timeout, if available
     */
    public Optional<Integer> acquisitionLockTimeoutMs() {
        return acquisitionLockTimeoutMs;
    }

    /**
     * @return {@code true} if this fetch contains records being renewed
     */
    public boolean hasRenewals() {
        for (Map.Entry<TopicIdPartition, List<ShareInFlightBatch<K, V>>> entry : batches.entrySet()) {
            for (ShareInFlightBatch<K, V> batch : entry.getValue()) {
                if (batch.hasRenewals()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Take any renewed records and move them back into in-flight state.
     */
    public void takeRenewedRecords() {
        for (Map.Entry<TopicIdPartition, List<ShareInFlightBatch<K, V>>> entry : batches.entrySet()) {
            for (ShareInFlightBatch<K, V> batch : entry.getValue()) {
                batch.takeRenewals();
            }
        }
        // Any acquisition lock timeout updated by renewal is applied as the renewed records are move back to in-flight
        if (acquisitionLockTimeoutMsRenewed.isPresent()) {
            acquisitionLockTimeoutMs = acquisitionLockTimeoutMsRenewed;
        }
    }

    /**
     * Acknowledge a single record in the current batch.
     *
     * @param record The record to acknowledge
     * @param type The acknowledge type which indicates whether it was processed successfully
     */
    public void acknowledge(final ConsumerRecord<K, V> record, final AcknowledgeType type) {
        for (Map.Entry<TopicIdPartition, List<ShareInFlightBatch<K, V>>> entry : batches.entrySet()) {
            TopicIdPartition tip = entry.getKey();
            if (tip.topic().equals(record.topic()) && (tip.partition() == record.partition())) {
                for (ShareInFlightBatch<K, V> batch : entry.getValue()) {
                    if (batch.isInFlight(record.offset())) {
                        batch.acknowledge(record, type);
                        return;
                    }
                }
            }
        }
        throw new IllegalStateException("The record cannot be acknowledged.");
    }

    /**
     * Acknowledge a single record which experienced an exception during its delivery by its topic, partition
     * and offset in the current batch. This method is specifically for overriding the default acknowledge
     * type for records whose delivery failed.
     *
     * @param topic     The topic of the record to acknowledge
     * @param partition The partition of the record
     * @param offset    The offset of the record
     * @param type      The acknowledge type which indicates whether it was processed successfully
     */
    public void acknowledge(final String topic, final int partition, final long offset, final AcknowledgeType type) {
        for (Map.Entry<TopicIdPartition, List<ShareInFlightBatch<K, V>>> entry : batches.entrySet()) {
            TopicIdPartition tip = entry.getKey();
            if (tip.topic().equals(topic) && (tip.partition() == partition)) {
                for (ShareInFlightBatch<K, V> batch : entry.getValue()) {
                    ShareInFlightBatchException exception = batch.getException();
                    if (exception != null && exception.offsets().contains(offset)) {
                        batch.addAcknowledgement(offset, type);
                        return;
                    }
                }
            }
        }
        throw new IllegalStateException("The record cannot be acknowledged.");
    }

    /**
     * Acknowledge all records in the current batch. If any records in the batch already have
     * been acknowledged, those acknowledgements are not overwritten.
     *
     * @param type The acknowledge type which indicates whether it was processed successfully
     */
    public void acknowledgeAll(final AcknowledgeType type) {
        batches.forEach((tip, batchList) -> batchList.forEach(batch -> batch.acknowledgeAll(type)));
    }

    /**
     * Checks whether all in-flight records have been acknowledged. This is required for explicit
     * acknowledgement mode.
     *
     * @return Whether all in-flight records have been acknowledged
     */
    public boolean checkAllInFlightAreAcknowledged() {
        for (Map.Entry<TopicIdPartition, List<ShareInFlightBatch<K, V>>> entry : batches.entrySet()) {
            for (ShareInFlightBatch<K, V> batch : entry.getValue()) {
                if (!batch.checkAllInFlightAreAcknowledged()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Removes all acknowledged records from the in-flight records and returns the map of acknowledgements
     * to send. If some records were not acknowledged, the in-flight records will not be empty after this
     * method.
     *
     * @return The map of acknowledgements to send, along with node information
     */
    public Map<TopicIdPartition, NodeAcknowledgements> takeAcknowledgedRecords() {
        Map<TopicIdPartition, NodeAcknowledgements> acknowledgementMap = new LinkedHashMap<>();
        batches.forEach((tip, batchList) -> {
            if (!batchList.isEmpty()) {
                Acknowledgements acknowledgements = Acknowledgements.empty();
                int nodeId = batchList.get(0).nodeId();
                for (ShareInFlightBatch<K, V> batch : batchList) {
                    acknowledgements.merge(batch.takeAcknowledgedRecords());
                }
                if (!acknowledgements.isEmpty())
                    acknowledgementMap.put(tip, new NodeAcknowledgements(nodeId, acknowledgements));
            }
        });
        return acknowledgementMap;
    }

    /**
     * Handles completed renew acknowledgements by returning successfully renewed records
     * to the set of in-flight records.
     *
     * @param acknowledgementsMap      Map from topic-partition to acknowledgements for
     *                                 completed renew acknowledgements
     * @param acquisitionLockTimeoutMs Optional updated acquisition lock timeout
     *
     * @return The number of records renewed
     */
    public int renew(Map<TopicIdPartition, Acknowledgements> acknowledgementsMap, Optional<Integer> acquisitionLockTimeoutMs) {
        int recordsRenewed = 0;
        for (Map.Entry<TopicIdPartition, Acknowledgements> entry : acknowledgementsMap.entrySet()) {
            List<ShareInFlightBatch<K, V>> batchList = batches.get(entry.getKey());
            if (batchList != null) {
                for (ShareInFlightBatch<K, V> batch : batchList) {
                    recordsRenewed += batch.renew(entry.getValue());
                }
            }
        }
        acquisitionLockTimeoutMsRenewed = acquisitionLockTimeoutMs;
        return recordsRenewed;
    }
}
