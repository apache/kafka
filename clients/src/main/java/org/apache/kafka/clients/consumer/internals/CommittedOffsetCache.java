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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CommittedOffsetCache {
    private final SubscriptionState subscriptions;
    private final Map<TopicPartition, OffsetAndMetadata> latestCommittedOffsets;

    CommittedOffsetCache(SubscriptionState subscriptions) {
        this.subscriptions = Objects.requireNonNull(subscriptions);
        this.latestCommittedOffsets = new ConcurrentHashMap<>();
    }

    public void tryAddToCache(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (subscriptions.hasAutoAssignedPartitions() && offsets != null && !offsets.isEmpty()) {
            latestCommittedOffsets.putAll(offsets);
        }
    }

    public void tryAddToCache(TopicPartition tp, OffsetAndMetadata metadata) {
        if (subscriptions.hasAutoAssignedPartitions() && tp != null && metadata != null) {
            latestCommittedOffsets.put(tp, metadata);
        }
    }

    public boolean isHitCache(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // If the current consumer mode is not subscribe, or there are in-flight async commits, simply return false
        if (!subscriptions.hasAutoAssignedPartitions() || offsets == null || offsets.isEmpty()) {
            return false;
        }

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata cachedOffset = latestCommittedOffsets.get(entry.getKey());
            if (cachedOffset == null || !cachedOffset.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    public void clear(Set<TopicPartition> topicPartitions) {
        if (topicPartitions != null && !topicPartitions.isEmpty()) {
            for (TopicPartition topicPartition : topicPartitions) {
                latestCommittedOffsets.remove(topicPartition);
            }
        }
    }
}
