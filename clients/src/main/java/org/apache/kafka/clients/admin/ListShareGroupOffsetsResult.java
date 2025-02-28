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

import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * The result of the {@link Admin#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListShareGroupOffsetsResult {

    private final Map<String, KafkaFuture<Map<TopicPartition, Long>>> futures;

    ListShareGroupOffsetsResult(final Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, Long>>> futures) {
        this.futures = futures.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().idValue, Map.Entry::getValue));
    }

    /**
     * Return the future when the requests for all groups succeed.
     *
     * @return Future which yields all {@code Map<String, Map<TopicPartition, Long>>} objects, if requests for all the groups succeed.
     */
    public KafkaFuture<Map<String, Map<TopicPartition, Long>>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture<?>[0])).thenApply(
            nil -> {
                Map<String, Map<TopicPartition, Long>> offsets = new HashMap<>(futures.size());
                futures.forEach((groupId, future) -> {
                    try {
                        offsets.put(groupId, future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        // This should be unreachable, since the KafkaFuture#allOf already ensured
                        // that all the futures completed successfully.
                        throw new RuntimeException(e);
                    }
                });
                return offsets;
            });
    }

    /**
     * Return a future which yields a map of topic partitions to offsets for the specified group.
     *
     * @param groupId The group ID.
     * @return Future which yields a map of topic partitions to offsets for the specified group.
     */
    public KafkaFuture<Map<TopicPartition, Long>> partitionsToOffset(String groupId) {
        if (!futures.containsKey(groupId)) {
            throw new IllegalArgumentException("Group ID not found: " + groupId);
        }
        return futures.get(groupId);
    }
}
