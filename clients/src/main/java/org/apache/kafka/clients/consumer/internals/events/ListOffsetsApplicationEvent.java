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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Event for retrieving partition offsets from the partition leader
 * by performing a {@link org.apache.kafka.common.requests.ListOffsetsRequest}
 */
public class ListOffsetsApplicationEvent extends CompletableApplicationEvent<Map<TopicPartition, Long>> {
    private final CompletableFuture<Map<TopicPartition, Long>> future;

    final Set<TopicPartition> partitions;
    final long timestamp;
    final boolean requireTimestamps;

    public ListOffsetsApplicationEvent(final Set<TopicPartition> partitions,
                                       long timestamp,
                                       boolean requireTimestamps) {
        super(Type.LIST_OFFSETS);
        this.partitions = partitions;
        this.timestamp = timestamp;
        this.requireTimestamps = requireTimestamps;
        this.future = new CompletableFuture<>();
    }

    public CompletableFuture<Map<TopicPartition, Long>> future() {
        return future;
    }

    @Override
    public String toString() {
        return "ListOffsetsApplicationEvent {" +
                "partitions=" + partitions + ", " +
                "target timestamp=" + timestamp + ", " +
                "requireTimestamps=" + requireTimestamps + '}';
    }
}
