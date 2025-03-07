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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.protocol.Errors;

import java.util.Map;
import java.util.Set;

/**
 * The result of the {@link Admin#deleteStreamsGroupOffsets(String, Set, DeleteStreamsGroupOffsetsOptions)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DeleteStreamsGroupOffsetsResult {
    private final DeleteConsumerGroupOffsetsResult delegate;

    DeleteStreamsGroupOffsetsResult(KafkaFuture<Map<TopicPartition, Errors>> future, Set<TopicPartition> partitions) {
        delegate = new DeleteConsumerGroupOffsetsResult(future, partitions);
    }

    DeleteStreamsGroupOffsetsResult(final DeleteConsumerGroupOffsetsResult delegate) {
        this.delegate = delegate;
    }

    /**
     * Return a future which succeeds only if all the deletions succeed.
     */
    public KafkaFuture<Void> all() {
        return delegate.all();
    }

    /**
     * Return a future which can be used to check the result for a given topic.
     */
    public KafkaFuture<Void> partitionResult(final TopicPartition topicPartition) {
        return delegate.partitionResult(topicPartition);
    }
}
