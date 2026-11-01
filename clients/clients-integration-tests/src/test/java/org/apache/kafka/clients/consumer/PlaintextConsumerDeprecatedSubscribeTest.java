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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.ClientsTestUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import org.junit.jupiter.api.BeforeEach;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.GroupProtocol.CLASSIC;
import static org.apache.kafka.clients.consumer.GroupProtocol.CONSUMER;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the deprecated {@code subscribe} overloads that take a {@link ConsumerRebalanceListener}.
 * Remove this class together with those overloads in 5.0.
 */
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = PlaintextConsumerDeprecatedSubscribeTest.BROKER_COUNT
)
@SuppressWarnings("removal")
public class PlaintextConsumerDeprecatedSubscribeTest {

    public static final int BROKER_COUNT = 3;
    private final ClusterInstance cluster;
    private final String topic = "topic";
    private final TopicPartition tp = new TopicPartition(topic, 0);

    public PlaintextConsumerDeprecatedSubscribeTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setup() {
        cluster.createTopic(topic, 1, (short) BROKER_COUNT);
    }

    @ClusterTest
    public void testClassicConsumerSubscribeTopicsWithListener() throws InterruptedException {
        testSubscribeWithListener(CLASSIC, (consumer, listener) -> consumer.subscribe(List.of(topic), listener));
    }

    @ClusterTest
    public void testAsyncConsumerSubscribeTopicsWithListener() throws InterruptedException {
        testSubscribeWithListener(CONSUMER, (consumer, listener) -> consumer.subscribe(List.of(topic), listener));
    }

    @ClusterTest
    public void testClassicConsumerSubscribePatternWithListener() throws InterruptedException {
        testSubscribeWithListener(CLASSIC, (consumer, listener) -> consumer.subscribe(Pattern.compile(topic), listener));
    }

    @ClusterTest
    public void testAsyncConsumerSubscribePatternWithListener() throws InterruptedException {
        testSubscribeWithListener(CONSUMER, (consumer, listener) -> consumer.subscribe(Pattern.compile(topic), listener));
    }

    @ClusterTest
    public void testAsyncConsumerSubscribeSubscriptionPatternWithListener() throws InterruptedException {
        testSubscribeWithListener(CONSUMER, (consumer, listener) -> consumer.subscribe(new SubscriptionPattern(topic), listener));
    }

    @ClusterTest
    public void testClassicConsumerSubscribeWithNullListener() {
        try (var consumer = createConsumer(CLASSIC)) {
            assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(List.of(topic), null));
            assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(Pattern.compile(topic), null));
        }
    }

    @ClusterTest
    public void testAsyncConsumerSubscribeWithNullListener() {
        try (var consumer = createConsumer(CONSUMER)) {
            assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(List.of(topic), null));
            assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(Pattern.compile(topic), null));
            assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(new SubscriptionPattern(topic), null));
        }
    }

    private void testSubscribeWithListener(
        GroupProtocol groupProtocol,
        BiConsumer<Consumer<byte[], byte[]>, ConsumerRebalanceListener> subscribe
    ) throws InterruptedException {
        var partitionsAssigned = new AtomicBoolean(false);
        var partitionsRevoked = new AtomicBoolean(false);
        // Only the one-argument callbacks are implemented, so they must be reached through the two-argument defaults.
        var listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (partitions.contains(tp)) {
                    partitionsAssigned.set(true);
                }
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (partitions.contains(tp)) {
                    partitionsRevoked.set(true);
                }
            }
        };
        try (var consumer = createConsumer(groupProtocol)) {
            subscribe.accept(consumer, listener);
            ClientsTestUtils.pollUntilTrue(
                consumer,
                partitionsAssigned::get,
                "Timed out before onPartitionsAssigned was invoked"
            );

            consumer.unsubscribe();
            assertTrue(partitionsRevoked.get());
        }
    }

    private Consumer<byte[], byte[]> createConsumer(GroupProtocol protocol) {
        return cluster.consumer(Map.of(
            GROUP_PROTOCOL_CONFIG, protocol.name().toLowerCase(Locale.ROOT),
            ENABLE_AUTO_COMMIT_CONFIG, "false"
        ));
    }
}
