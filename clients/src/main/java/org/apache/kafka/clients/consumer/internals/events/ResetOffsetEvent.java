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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * This event indicates that partition offsets should be reset according to the specified strategy. The actual reset of
 * the positions will occur during the next update of fetch positions via {@link KafkaConsumer#poll(Duration)} or
 * {@link KafkaConsumer#position(TopicPartition)}.
 * This mechanism is used to execute {@link AsyncKafkaConsumer#seekToBeginning(Collection)} and {@link AsyncKafkaConsumer#seekToEnd(Collection)}.
 */
public class ResetOffsetEvent extends CompletableApplicationEvent<Void> {

    private final Collection<TopicPartition> topicPartitions;

    private final AutoOffsetResetStrategy offsetResetStrategy;

    public ResetOffsetEvent(Collection<TopicPartition> topicPartitions, AutoOffsetResetStrategy offsetResetStrategy, long deadline) {
        super(Type.RESET_OFFSET, deadline);
        this.topicPartitions = Collections.unmodifiableCollection(topicPartitions);
        this.offsetResetStrategy = Objects.requireNonNull(offsetResetStrategy);
    }

    public Collection<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    public AutoOffsetResetStrategy offsetResetStrategy() {
        return offsetResetStrategy;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", topicPartitions=" + topicPartitions + ", offsetStrategy=" + offsetResetStrategy;
    }
}
