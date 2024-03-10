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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public class ShareFetch<K, V> {
    private final Map<TopicIdPartition, ShareInFlightBatch<K, V>> batches;
    private int numRecords;

    public static <K, V> ShareFetch<K, V> empty() {
        return new ShareFetch<>(new HashMap<>(), 0);
    }

    public static <K, V> ShareFetch<K, V> forInFlightBatch(
        TopicIdPartition partition,
        ShareInFlightBatch<K, V> batch) {
        Map<TopicIdPartition, ShareInFlightBatch<K, V>> batchesMap = batch.isEmpty()
                ? new HashMap<>()
                : mkMap(mkEntry(partition, batch));
        return new ShareFetch<>(batchesMap, batch.getInFlightRecords().size());
    }

    private ShareFetch(
            Map<TopicIdPartition, ShareInFlightBatch<K, V>> batches,
            int numRecords) {
        this.batches = batches;
        this.numRecords = numRecords;
    }

    /**
     * Add another {@link ShareFetch} to this one; all of its records will be added to this object's
     * {@link #records() records}.
     *
     * @param fetch the other fetch to add; may not be null
     */
    public void add(ShareFetch<K, V> fetch) {
        Objects.requireNonNull(fetch);
        fetch.batches.forEach((partition, batch) -> {
            this.numRecords += fetch.numRecords();
            ShareInFlightBatch<K, V> currentBatch = this.batches.get(partition);
            if (currentBatch == null) {
                this.batches.put(partition, batch);
            } else {
                // This case shouldn't usually happen because we only send one fetch at a time per partition,
                // but it might conceivably happen in some rare cases (such as partition leader changes).
                currentBatch.merge(batch);
            }
        });
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
        return numRecords;
    }

    /**
     * @return {@code true} if and only if this fetch did not return any user-visible (i.e., non-control) records, and
     * did not cause the consumer position to advance for any topic partitions
     */
    public boolean isEmpty() {
        return numRecords == 0;
    }

    public void acknowledgeAll(final AcknowledgeType type) {
        // This is where we hook into building the acknowledgement batches
        batches.forEach((tip, batch) -> batch.acknowledgeAll(type));
    }
}
