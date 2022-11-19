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

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class CoordinatorManagerTest {
    private MockTime time;
    private MockClient client;
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private LogContext logContext;
    private LinkedBlockingDeque<BackgroundEvent> eventQueue;
    private Node node;
    private final Properties properties = new Properties();
    private Optional<String> groupId;
    private int rebalanceTimeoutMs;


    @BeforeEach
    public void setup() {
        this.logContext = new LogContext();
        this.time = new MockTime();
        this.subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        this.metadata = new ConsumerMetadata(0, Long.MAX_VALUE, false,
                false, subscriptions, logContext, new ClusterResourceListeners());
        this.client = new MockClient(time, metadata);
        this.client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)));
        this.node = metadata.fetch().nodes().get(0);
        this.eventQueue = new LinkedBlockingDeque<>();
        properties.put(RETRY_BACKOFF_MS_CONFIG, "100");
        this.groupId = Optional.of("");
        this.rebalanceTimeoutMs = 60 * 1000;
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, 100);
    }
    
    @Test
    public void testTestFindCoordinator() {

    }

    @Test
    public void testTestFindCoordinatorError() {
    }
    
    private CoordinatorManager setupCoordinatorManager() {
        return new CoordinatorManager(
                this.time,
                this.logContext,
                new ConsumerConfig(properties),
                this.eventQueue,
                this.groupId,
                this.rebalanceTimeoutMs);
    }

}
