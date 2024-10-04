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

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Event to perform {@link AsyncKafkaConsumer#seekToBeginning(Collection)} and {@link AsyncKafkaConsumer#seekToEnd(Collection)}
 * in the background thread. This can avoid race conditions when subscription state is updated. When a ResetOffsetEvent is completed
 * successfully, it does not guarantee the position reset request have been sent.
 */
public class ResetOffsetEvent extends CompletableApplicationEvent<Void> {

    private final Collection<TopicPartition> topicPartitions;

    private final OffsetResetStrategy offsetResetStrategy;

    public ResetOffsetEvent(Collection<TopicPartition> topicPartitions, OffsetResetStrategy offsetResetStrategy, long deadline) {
        super(Type.RESET_OFFSET, deadline);
        this.topicPartitions = Collections.unmodifiableCollection(topicPartitions);
        this.offsetResetStrategy = Objects.requireNonNull(offsetResetStrategy);
    }

    public Collection<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    public OffsetResetStrategy offsetResetStrategy() {
        return offsetResetStrategy;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", topicPartitions=" + topicPartitions + ", offsetStrategy=" + offsetResetStrategy;
    }
}
