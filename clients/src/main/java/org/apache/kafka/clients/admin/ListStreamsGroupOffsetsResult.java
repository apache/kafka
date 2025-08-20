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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * The result of the {@link Admin#listStreamsGroupOffsets(Map, ListStreamsGroupOffsetsOptions)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListStreamsGroupOffsetsResult {
    private final ListConsumerGroupOffsetsResult delegate;

    ListStreamsGroupOffsetsResult(final Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures) {
        delegate = new ListConsumerGroupOffsetsResult(futures);
    }

    ListStreamsGroupOffsetsResult(final ListConsumerGroupOffsetsResult delegate) {
        this.delegate = delegate;
    }

    /**
     * Return a future which yields all {@code Map<String, Map<TopicPartition, OffsetAndMetadata>>} objects, if requests for all the groups succeed.
     */
    public KafkaFuture<Map<String, Map<TopicPartition, OffsetAndMetadata>>> all() {
        return delegate.all();
    }

    /**
     * Return a future which yields a map of topic partitions to offsets for the specified group. If the group doesn't
     * have a committed offset for a specific partition, the corresponding value in the returned map will be null.
     */
    public KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> partitionsToOffsetAndMetadata(String groupId) {
        return delegate.partitionsToOffsetAndMetadata(groupId);
    }
}
