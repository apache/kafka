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
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultBackgroundThreadRunnableTest {
    private long refreshBackoffMs = 100;
    private long expireMs = 1000;
    private final Properties properties = new Properties();
    private MockTime time;
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private MockClient client;
    private LogContext context;
    private ConsumerNetworkClient consumerClient;

    @BeforeEach
    public void setup() {
        this.time = new MockTime();
        this.subscriptions = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        this.metadata = new ConsumerMetadata(refreshBackoffMs, expireMs, false, false,
                subscriptions, new LogContext(), new ClusterResourceListeners());
        this.client = new MockClient(time, metadata);
        this.context = new LogContext();
        this.consumerClient = new ConsumerNetworkClient(context, client, metadata, time,
                100, 1000, 100);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Test
    public void testStartupAndTearDown() {
        DefaultBackgroundThreadRunnable runnable = setupMockHandler();
        KafkaThread thread = new KafkaThread("test-thread", runnable, true);
        thread.start();
        assertTrue(client.active());
        runnable.close();
        assertFalse(client.active());
    }

    private DefaultBackgroundThreadRunnable setupMockHandler() {
        DefaultBackgroundThreadRunnable runnable = new DefaultBackgroundThreadRunnable(
                time,
                new ConsumerConfig(properties),
                new LinkedBlockingDeque<>(),
                new LinkedBlockingQueue<>(),
                subscriptions,
                metadata,
                this.consumerClient);
        return runnable;
    }
}
