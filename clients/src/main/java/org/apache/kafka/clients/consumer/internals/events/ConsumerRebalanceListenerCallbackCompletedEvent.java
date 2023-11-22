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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerCallbackName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaException;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;

/**
 * Event that signifies that the application thread has executed the {@link ConsumerRebalanceListener} callback. If
 * the callback execution threw an error, it is included in the event should any event listener want to know.
 */
public class ConsumerRebalanceListenerCallbackCompletedEvent extends ApplicationEvent {

    private final ConsumerRebalanceListenerCallbackName callbackName;
    private final SortedSet<TopicPartition> partitions;
    private final Optional<KafkaException> error;

    public ConsumerRebalanceListenerCallbackCompletedEvent(ConsumerRebalanceListenerCallbackName callbackName,
                                                           SortedSet<TopicPartition> partitions,
                                                           Optional<KafkaException> error) {
        super(Type.CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED);
        this.callbackName = Objects.requireNonNull(callbackName);
        this.partitions = Collections.unmodifiableSortedSet(partitions);
        this.error = Objects.requireNonNull(error);
    }

    public ConsumerRebalanceListenerCallbackName callbackName() {
        return callbackName;
    }

    public SortedSet<TopicPartition> partitions() {
        return partitions;
    }

    public Optional<KafkaException> error() {
        return error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ConsumerRebalanceListenerCallbackCompletedEvent that = (ConsumerRebalanceListenerCallbackCompletedEvent) o;

        return callbackName == that.callbackName && partitions.equals(that.partitions) && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(callbackName, partitions, error);
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() +
                ", callbackName=" + callbackName +
                ", partitions=" + partitions +
                ", error=" + error;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                toStringBase() +
                '}';
    }
}
