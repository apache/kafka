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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link ShareFetch} represents the records fetched from the broker to be returned to the consumer
 * to satisfy a {@link KafkaShareConsumer#poll(Duration)} call. The records can come from multiple
 * topic-partitions.
 *
 * @param <K> The record key
 * @param <V> The record value
 */
public class ShareFetch<K, V> {
    private final Map<TopicIdPartition, ShareInFlightBatch<K, V>> batches;

    public static <K, V> ShareFetch<K, V> empty() {
        return new ShareFetch<>(new HashMap<>());
    }

    private ShareFetch(Map<TopicIdPartition, ShareInFlightBatch<K, V>> batches) {
        this.batches = batches;
    }

    /**
     * Add another {@link ShareInFlightBatch} to this one; all of its records will be added to this object's
     * {@link #records() records}.
     *
     * @param partition the topic-partition
     * @param batch the batch to add; may not be null
     */
    public void add(TopicIdPartition partition, ShareInFlightBatch<K, V> batch) {
        Objects.requireNonNull(batch);
        ShareInFlightBatch<K, V> currentBatch = this.batches.get(partition);
        if (currentBatch == null) {
            this.batches.put(partition, batch);
        } else {
            // This case shouldn't usually happen because we only send one fetch at a time per partition,
            // but it might conceivably happen in some rare cases (such as partition leader changes).
            currentBatch.merge(batch);
        }
    }

    /**
     * @return all the non-control messages for this fetch, grouped by partition
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> records() {
        final LinkedHashMap<TopicPartition, List<ConsumerRecord<K, V>>> result = new LinkedHashMap<>();
        batches.forEach((tip, batch) -> result.put(tip.topicPartition(), batch.getInFlightRecords()));
        return Collections.unmodifiableMap(result);
    }

    /**
     * @return the total number of non-control messages for this fetch, across all partitions
     */
    public int numRecords() {
        int numRecords = 0;
        if (!batches.isEmpty()) {
            Iterator<Map.Entry<TopicIdPartition, ShareInFlightBatch<K, V>>> iterator = batches.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<TopicIdPartition, ShareInFlightBatch<K, V>> entry = iterator.next();
                ShareInFlightBatch<K, V> batch = entry.getValue();
                if (batch.isEmpty()) {
                    iterator.remove();
                } else {
                    numRecords += batch.numRecords();
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
     * Acknowledge a single record in the current batch.
     *
     * @param record The record to acknowledge
     * @param type The acknowledge type which indicates whether it was processed successfully
     */
    public void acknowledge(final ConsumerRecord<K, V> record, AcknowledgeType type) {
        for (Map.Entry<TopicIdPartition, ShareInFlightBatch<K, V>> tipBatch : batches.entrySet()) {
            TopicIdPartition tip = tipBatch.getKey();
            if (tip.topic().equals(record.topic()) && (tip.partition() == record.partition())) {
                tipBatch.getValue().acknowledge(record, type);
                return;
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
        batches.forEach((tip, batch) -> batch.acknowledgeAll(type));
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
        batches.forEach((tip, batch) -> {
            int nodeId = batch.nodeId();
            Acknowledgements acknowledgements = batch.takeAcknowledgedRecords();
            if (!acknowledgements.isEmpty())
                acknowledgementMap.put(tip, new NodeAcknowledgements(nodeId, acknowledgements));
        });
        return acknowledgementMap;
    }
}
