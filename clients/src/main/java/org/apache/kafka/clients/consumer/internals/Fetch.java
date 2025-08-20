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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public class Fetch<K, V> {
    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records;
    private final Map<TopicPartition, OffsetAndMetadata> nextOffsetAndMetadata;
    private boolean positionAdvanced;
    private int numRecords;

    public static <K, V> Fetch<K, V> empty() {
        return new Fetch<>(new HashMap<>(), false, 0, new HashMap<>());
    }

    public static <K, V> Fetch<K, V> forPartition(
            TopicPartition partition,
            List<ConsumerRecord<K, V>> records,
            boolean positionAdvanced,
            OffsetAndMetadata nextOffsetAndMetadata
    ) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> recordsMap = records.isEmpty()
                ? new HashMap<>()
                : mkMap(mkEntry(partition, records));
        Map<TopicPartition, OffsetAndMetadata> nextOffsetAndMetadataMap = mkMap(mkEntry(partition, nextOffsetAndMetadata));
        return new Fetch<>(recordsMap, positionAdvanced, records.size(), nextOffsetAndMetadataMap);
    }

    private Fetch(
            Map<TopicPartition, List<ConsumerRecord<K, V>>> records,
            boolean positionAdvanced,
            int numRecords,
            Map<TopicPartition, OffsetAndMetadata> nextOffsetAndMetadata
    ) {
        this.records = records;
        this.positionAdvanced = positionAdvanced;
        this.numRecords = numRecords;
        this.nextOffsetAndMetadata = nextOffsetAndMetadata;
    }

    /**
     * Add another {@link Fetch} to this one; all of its records will be added to this fetch's
     * {@link #records() records}, and if the other fetch
     * {@link #positionAdvanced() advanced the consume position for any topic partition},
     * this fetch will be marked as having advanced the consume position as well.
     * @param fetch the other fetch to add; may not be null
     */
    public void add(Fetch<K, V> fetch) {
        Objects.requireNonNull(fetch);
        addRecords(fetch.records);
        this.positionAdvanced |= fetch.positionAdvanced;
        this.nextOffsetAndMetadata.putAll(fetch.nextOffsetAndMetadata);
    }

    /**
     * @return all of the non-control messages for this fetch, grouped by partition
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> records() {
        return Collections.unmodifiableMap(records);
    }

    /**
     * @return whether the fetch caused the consumer's
     * {@link org.apache.kafka.clients.consumer.KafkaConsumer#position(TopicPartition) position} to advance for at
     * least one of the topic partitions in this fetch
     */
    public boolean positionAdvanced() {
        return positionAdvanced;
    }

    /**
     * @return the total number of non-control messages for this fetch, across all partitions
     */
    public int numRecords() {
        return numRecords;
    }

    /**
     * @return the next offsets and metadata that the consumer will consume (last epoch is included)
     */
    public Map<TopicPartition, OffsetAndMetadata> nextOffsets() {
        return Map.copyOf(nextOffsetAndMetadata);
    }

    /**
     * @return {@code true} if and only if this fetch did not return any user-visible (i.e., non-control) records, and
     * did not cause the consumer position to advance for any topic partitions
     */
    public boolean isEmpty() {
        return numRecords == 0 && !positionAdvanced;
    }

    private void addRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        records.forEach((partition, partRecords) -> {
            this.numRecords += partRecords.size();
            List<ConsumerRecord<K, V>> currentRecords = this.records.get(partition);
            if (currentRecords == null) {
                this.records.put(partition, partRecords);
            } else {
                // this case shouldn't usually happen because we only send one fetch at a time per partition,
                // but it might conceivably happen in some rare cases (such as partition leader changes).
                // we have to copy to a new list because the old one may be immutable
                List<ConsumerRecord<K, V>> newRecords = new ArrayList<>(currentRecords.size() + partRecords.size());
                newRecords.addAll(currentRecords);
                newRecords.addAll(partRecords);
                this.records.put(partition, newRecords);
            }
        });
    }
}
