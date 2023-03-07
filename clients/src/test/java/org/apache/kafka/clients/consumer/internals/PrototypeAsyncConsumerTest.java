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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

public class PrototypeAsyncConsumerTest {

    private Consumer<?, ?> consumer;
    private Map<String, Object> consumerProps = new HashMap<>();

    private final Time time = new MockTime();
    private LogContext logContext;
    private SubscriptionState subscriptions;
    private EventHandler eventHandler;
    private Metrics metrics;
    private ClusterResourceListeners clusterResourceListeners;

    private String groupId;
    private String clientId = "client-1";
    private ConsumerConfig config;

    @BeforeEach
    public void setup() {
        injectConsumerConfigs();
        this.config = new ConsumerConfig(consumerProps);
        this.logContext = new LogContext();
        this.subscriptions = Mockito.mock(SubscriptionState.class);
        this.eventHandler = Mockito.mock(DefaultEventHandler.class);
        this.metrics = new Metrics(time);
        this.clusterResourceListeners = new ClusterResourceListeners();
    }

    @AfterEach
    public void cleanup() {
        if (consumer != null) {
            consumer.close(Duration.ZERO);
        }
    }

    @Test
    public void testSuccessfulStartupShutdown() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertDoesNotThrow(() -> consumer.close());
    }

    @Test
    public void testCommitAsync() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        consumer.commitAsync();
        verify(eventHandler).add(any());
    }

    @Test
    public void testUnimplementedException() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertThrows(KafkaException.class, consumer::assignment, "not implemented exception");
    }

    private ConsumerMetadata createMetadata(SubscriptionState subscription) {
        return new ConsumerMetadata(0, Long.MAX_VALUE, false, false,
                subscription, new LogContext(), new ClusterResourceListeners());
    }

    private void injectConsumerConfigs() {
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        consumerProps.put(DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    private PrototypeAsyncConsumer<?, ?> newConsumer(final Time time,
                                                     final Deserializer<?> keyDeserializer,
                                                     final Deserializer<?> valueDeserializer) {
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());

        return new PrototypeAsyncConsumer<>(
                time,
                logContext,
                config,
                subscriptions,
                eventHandler,
                metrics,
                clusterResourceListeners,
                Optional.ofNullable(this.groupId),
                clientId,
                config.getInt(DEFAULT_API_TIMEOUT_MS_CONFIG));
    }
}

