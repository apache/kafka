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

import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;

import java.util.Objects;
import java.util.Optional;
import java.util.Queue;

/**
 * An event handler that receives {@link BackgroundEvent background events} from the
 * {@link ConsumerNetworkThread network thread} which are then made available to the application thread
 * via an {@link EventProcessor}.
 */

public class BackgroundEventHandler {

    private final Queue<BackgroundEvent> backgroundEventQueue;
    private final Optional<KafkaConsumerMetrics> kafkaConsumerMetrics;

    public BackgroundEventHandler(final Queue<BackgroundEvent> backgroundEventQueue, KafkaConsumerMetrics kafkaConsumerMetrics) {
        this.backgroundEventQueue = backgroundEventQueue;
        this.kafkaConsumerMetrics = Optional.of(kafkaConsumerMetrics);
        recordInitialEvents();
    }

    /**
     * Add a {@link BackgroundEvent} to the handler.
     *
     * @param event A {@link BackgroundEvent} created by the {@link ConsumerNetworkThread network thread}
     */
    public void add(BackgroundEvent event) {
        Objects.requireNonNull(event, "BackgroundEvent provided to add must be non-null");
        backgroundEventQueue.add(event);
        kafkaConsumerMetrics.ifPresent(consumerMetrics -> consumerMetrics.recordBackgroundEventQueueChange(event.id(), System.currentTimeMillis(), true));
    }

    private void recordInitialEvents() {
        for (BackgroundEvent event : backgroundEventQueue) {
            kafkaConsumerMetrics.ifPresent(consumerMetrics -> consumerMetrics.recordBackgroundEventQueueChange(event.id(), System.currentTimeMillis(), true));
        }
    }
}
