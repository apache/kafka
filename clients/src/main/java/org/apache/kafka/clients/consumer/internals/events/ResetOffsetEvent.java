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

/**
 * Event to perform {@link AsyncKafkaConsumer#seekToBeginning(Collection)}
 * in the background thread. This can avoid race conditions when subscription state is updated.
 */
public class ResetOffsetEvent extends ApplicationEvent {

    private final Collection<TopicPartition> topicPartitions;

    private final OffsetResetStrategy offsetStrategy;


    public ResetOffsetEvent(Collection<TopicPartition> topicPartitions, OffsetResetStrategy offsetStrategy) {
        super(Type.RESET_OFFSET);
        this.topicPartitions = topicPartitions;
        this.offsetStrategy = offsetStrategy;
    }

    public Collection<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    public OffsetResetStrategy offsetResetStrategy() {
        return offsetStrategy;
    }
}
