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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PrototypeAsyncConsumerTest {
    private Map<String, Object> properties;
    private SubscriptionState subscriptionState;
    private MockTime time;
    private LogContext logContext;
    private Metrics metrics;
    private ClusterResourceListeners clusterResourceListeners;
    private Optional<String> groupId;
    private String clientId;
    private EventHandler eventHandler;

    @BeforeEach
    public void setup() {
        this.subscriptionState = Mockito.mock(SubscriptionState.class);
        this.eventHandler = Mockito.mock(DefaultEventHandler.class);
        this.logContext = new LogContext();
        this.time = new MockTime();
        this.metrics = new Metrics(time);
        this.groupId = Optional.empty();
        this.clientId = "client-1";
        this.clusterResourceListeners = new ClusterResourceListeners();
        this.properties = new HashMap<>();
        this.properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost" +
                ":9999");
        this.properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.properties.put(CLIENT_ID_CONFIG, "test-client");
    }
    @Test
    public void testSubscription() {
        this.subscriptionState =
                new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
        PrototypeAsyncConsumer<String, String> consumer =
                setupConsumerWithDefault();
        subscriptionState.subscribe(singleton("t1"),
                new NoOpConsumerRebalanceListener());
        assertEquals(1, consumer.subscription().size());
    }

    @Test
    public void testUnimplementedException() {
        PrototypeAsyncConsumer<String, String> consumer =
                setupConsumerWithDefault();
        assertThrows(KafkaException.class, consumer::assignment, "not implemented exception");
    }

    public PrototypeAsyncConsumer<String, String> setupConsumerWithDefault() {
        ConsumerConfig config = new ConsumerConfig(properties);
        return new PrototypeAsyncConsumer<>(
                this.time,
                this.logContext,
                config,
                this.subscriptionState,
                this.eventHandler,
                this.metrics,
                this.clusterResourceListeners,
                this.groupId,
                this.clientId,
                0);

    }
}
