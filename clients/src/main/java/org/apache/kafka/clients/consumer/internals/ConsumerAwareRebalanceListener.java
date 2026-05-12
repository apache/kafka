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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * Internal wrapper around a user-provided {@link ConsumerRebalanceListener} that manages the
 * lifecycle of {@link DelegatingRebalanceConsumer} views. Each callback invocation creates a
 * fresh view that is automatically closed when the callback returns.
 *
 * <p>This class implements {@link ConsumerRebalanceListener} so that it can be stored and
 * returned in place of the user's listener without changing the type of
 * {@link SubscriptionState#rebalanceListener()}. The 1-arg methods create a
 * {@link DelegatingRebalanceConsumer} view and delegate to the user listener's 2-arg methods.
 *
 * <p>This class exists so that the {@link Consumer} reference does not need to be passed through
 * constructors (avoiding this-escape warnings). Instead, it is provided at the point where the
 * listener is registered, after the consumer is fully constructed.
 */
public class ConsumerAwareRebalanceListener implements ConsumerRebalanceListener {

    private final ConsumerRebalanceListener delegate;
    private final Consumer<?, ?> consumer;

    public ConsumerAwareRebalanceListener(ConsumerRebalanceListener delegate, Consumer<?, ?> consumer) {
        this.delegate = delegate;
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        try (var view = new DelegatingRebalanceConsumer(consumer)) {
            delegate.onPartitionsAssigned(partitions, view);
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        try (var view = new DelegatingRebalanceConsumer(consumer)) {
            delegate.onPartitionsRevoked(partitions, view);
        }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        try (var view = new DelegatingRebalanceConsumer(consumer)) {
            delegate.onPartitionsLost(partitions, view);
        }
    }
}
