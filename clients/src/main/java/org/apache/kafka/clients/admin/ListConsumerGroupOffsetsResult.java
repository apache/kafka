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

package org.apache.kafka.clients.admin;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The result of the {@link Admin#listConsumerGroupOffsets(Map)} and
 * {@link Admin#listConsumerGroupOffsets(String)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListConsumerGroupOffsetsResult {

    final Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures;

    ListConsumerGroupOffsetsResult(final Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures) {
        this.futures = futures.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().idValue, Entry::getValue));
    }

    /**
     * Return a future which yields a map of topic partitions to OffsetAndMetadata objects.
     * If the group does not have a committed offset for this partition, the corresponding value in the returned map will be null.
     */
    public KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> partitionsToOffsetAndMetadata() {
        if (futures.size() != 1) {
            throw new IllegalStateException("Offsets from multiple consumer groups were requested. " +
                    "Use partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.");
        }
        return futures.values().iterator().next();
    }

    /**
     * Return a future which yields a map of topic partitions to OffsetAndMetadata objects for
     * the specified group. If the group doesn't have a committed offset for a specific
     * partition, the corresponding value in the returned map will be null.
     */
    public KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> partitionsToOffsetAndMetadata(String groupId) {
        if (!futures.containsKey(groupId))
            throw new IllegalArgumentException("Offsets for consumer group '" + groupId + "' were not requested.");
        return futures.get(groupId);
    }

    /**
     * Return a future which yields all {@code Map<String, Map<TopicPartition, OffsetAndMetadata>} objects,
     * if requests for all the groups succeed.
     */
    public KafkaFuture<Map<String, Map<TopicPartition, OffsetAndMetadata>>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(
            nil -> {
                Map<String, Map<TopicPartition, OffsetAndMetadata>> listedConsumerGroupOffsets = new HashMap<>(futures.size());
                futures.forEach((key, future) -> {
                    try {
                        listedConsumerGroupOffsets.put(key, future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        // This should be unreachable, since the KafkaFuture#allOf already ensured
                        // that all of the futures completed successfully.
                        throw new RuntimeException(e);
                    }
                });
                return listedConsumerGroupOffsets;
            });
    }
}
