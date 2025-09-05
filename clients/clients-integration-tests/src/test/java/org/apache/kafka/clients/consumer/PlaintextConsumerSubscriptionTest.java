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

import org.apache.kafka.clients.ClientsTestUtils.TestConsumerReassignmentListener;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidRegularExpression;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.ClientsTestUtils.awaitAssignment;
import static org.apache.kafka.clients.ClientsTestUtils.awaitRebalance;
import static org.apache.kafka.clients.ClientsTestUtils.consumeAndVerifyRecords;
import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
import static org.apache.kafka.clients.ClientsTestUtils.waitForPollThrowException;
import static org.apache.kafka.clients.CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = PlaintextConsumerSubscriptionTest.BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
        @ClusterConfigProperty(key = GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, value = "60000"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "10"),
    }
)
public class PlaintextConsumerSubscriptionTest {

    public static final int BROKER_COUNT = 3;
    private final ClusterInstance cluster;
    private final String topic = "topic";
    private final TopicPartition tp = new TopicPartition(topic, 0);

    public PlaintextConsumerSubscriptionTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        cluster.createTopic(topic, 2, (short) BROKER_COUNT);
    }

    @ClusterTest
    public void testClassicConsumerPatternSubscription() throws InterruptedException {
        testPatternSubscription(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerPatternSubscription() throws InterruptedException {
        testPatternSubscription(GroupProtocol.CONSUMER);
    }

    /**
     * Verifies that pattern subscription performs as expected.
     * The pattern matches the topics 'topic' and 'tblablac', but not 'tblablak' or 'tblab1'.
     * It is expected that the consumer is subscribed to all partitions of 'topic' and 'tblablac' after the subscription 
     * when metadata is refreshed.
     * When a new topic 'tsomec' is added afterward, it is expected that upon the next metadata refresh the consumer 
     * becomes subscribed to this new topic and all partitions of that topic are assigned to it.
     */
    public void testPatternSubscription(GroupProtocol groupProtocol) throws InterruptedException {
        var numRecords = 10000;
        Map<String, Object> config = Map.of(
            MAX_POLL_INTERVAL_MS_CONFIG, 6000,
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            ENABLE_AUTO_COMMIT_CONFIG, false,
            METADATA_MAX_AGE_CONFIG, 100
        );
        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(config)
        ) {
            sendRecords(producer, tp, numRecords, System.currentTimeMillis());

            var topic1 = "tblablac"; // matches subscribed pattern
            cluster.createTopic(topic1, 2, (short) BROKER_COUNT);
            sendRecords(producer, new TopicPartition(topic1, 0), 1000, System.currentTimeMillis());
            sendRecords(producer, new TopicPartition(topic1, 1), 1000, System.currentTimeMillis());

            var topic2 = "tblablak"; // does not match subscribed pattern
            cluster.createTopic(topic2, 2, (short) BROKER_COUNT);
            sendRecords(producer, new TopicPartition(topic2, 0), 1000, System.currentTimeMillis());
            sendRecords(producer, new TopicPartition(topic2, 1), 1000, System.currentTimeMillis());

            var topic3 = "tblab1"; // does not match subscribed pattern
            cluster.createTopic(topic3, 2, (short) BROKER_COUNT);
            sendRecords(producer, new TopicPartition(topic3, 0), 1000, System.currentTimeMillis());
            sendRecords(producer, new TopicPartition(topic3, 1), 1000, System.currentTimeMillis());

            assertEquals(0, consumer.assignment().size());
            var pattern = Pattern.compile("t.*c");
            consumer.subscribe(pattern, new TestConsumerReassignmentListener());

            Set<TopicPartition> assignment = new HashSet<>();
            assignment.add(new TopicPartition(topic, 0));
            assignment.add(new TopicPartition(topic, 1));
            assignment.add(new TopicPartition(topic1, 0));
            assignment.add(new TopicPartition(topic1, 1));

            awaitAssignment(consumer, assignment);

            var topic4 = "tsomec"; // matches subscribed pattern
            cluster.createTopic(topic4, 2, (short) BROKER_COUNT);
            sendRecords(producer, new TopicPartition(topic4, 0), 1000, System.currentTimeMillis());
            sendRecords(producer, new TopicPartition(topic4, 1), 1000, System.currentTimeMillis());

            assignment.add(new TopicPartition(topic4, 0));
            assignment.add(new TopicPartition(topic4, 1));

            awaitAssignment(consumer, assignment);

            consumer.unsubscribe();
            assertEquals(0, consumer.assignment().size());
        }
    }

    @ClusterTest
    public void testClassicConsumerSubsequentPatternSubscription() throws InterruptedException {
        testSubsequentPatternSubscription(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerSubsequentPatternSubscription() throws InterruptedException {
        testSubsequentPatternSubscription(GroupProtocol.CONSUMER);
    }

    /**
     * Verifies that a second call to pattern subscription succeeds and performs as expected.
     * The initial subscription is to a pattern that matches two topics 'topic' and 'foo'.
     * The second subscription is to a pattern that matches 'foo' and a new topic 'bar'.
     * It is expected that the consumer is subscribed to all partitions of 'topic' and 'foo' after
     * the first subscription, and to all partitions of 'foo' and 'bar' after the second.
     * The metadata refresh interval is intentionally increased to a large enough value to guarantee
     * that it is the subscription call that triggers a metadata refresh, and not the timeout.
     */
    public void testSubsequentPatternSubscription(GroupProtocol groupProtocol) throws InterruptedException {
        var numRecords = 10000;
        Map<String, Object> config = Map.of(
            MAX_POLL_INTERVAL_MS_CONFIG, 6000,
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            ENABLE_AUTO_COMMIT_CONFIG, false,
            METADATA_MAX_AGE_CONFIG, 30000
        );
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config);
             Producer<byte[], byte[]> producer = cluster.producer()
        ) {
            sendRecords(producer, tp, numRecords, System.currentTimeMillis());

            // the first topic ('topic') matches first subscription pattern only
            var fooTopic = "foo"; // matches both subscription patterns
            cluster.createTopic(fooTopic, 1, (short) BROKER_COUNT);
            sendRecords(producer, new TopicPartition(fooTopic, 0), 1000, System.currentTimeMillis());

            assertEquals(0, consumer.assignment().size());

            var pattern = Pattern.compile(".*o.*"); // only 'topic' and 'foo' match this
            consumer.subscribe(pattern, new TestConsumerReassignmentListener());

            Set<TopicPartition> assignment = new HashSet<>();
            assignment.add(new TopicPartition(topic, 0));
            assignment.add(new TopicPartition(topic, 1));
            assignment.add(new TopicPartition(fooTopic, 0));

            awaitAssignment(consumer, assignment);

            var barTopic = "bar"; // matches the next subscription pattern
            cluster.createTopic(barTopic, 1, (short) BROKER_COUNT);
            sendRecords(producer, new TopicPartition(barTopic, 0), 1000, System.currentTimeMillis());

            var pattern2 = Pattern.compile("..."); // only 'foo' and 'bar' match this
            consumer.subscribe(pattern2, new TestConsumerReassignmentListener());

            // Remove topic partitions from assignment
            assignment.remove(new TopicPartition(topic, 0));
            assignment.remove(new TopicPartition(topic, 1));

            // Add bar topic partition to assignment
            assignment.add(new TopicPartition(barTopic, 0));

            awaitAssignment(consumer, assignment);

            consumer.unsubscribe();
            assertEquals(0, consumer.assignment().size());
        }
    }

    @ClusterTest
    public void testClassicConsumerPatternUnsubscription() throws InterruptedException {
        testPatternUnsubscription(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerPatternUnsubscription() throws InterruptedException {
        testPatternUnsubscription(GroupProtocol.CONSUMER);
    }

    /**
     * Verifies that pattern unsubscription performs as expected.
     * The pattern matches the topics 'topic' and 'tblablac'.
     * It is expected that the consumer is subscribed to all partitions of 'topic' and 'tblablac' after the subscription 
     * when metadata is refreshed.
     * When consumer unsubscribes from all its subscriptions, it is expected that its assignments are cleared right away.
     */
    public void testPatternUnsubscription(GroupProtocol groupProtocol) throws InterruptedException {
        var numRecords = 10000;
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
        try (Producer<byte[], byte[]> producer = cluster.producer();
             Consumer<byte[], byte[]> consumer = cluster.consumer(config)
        ) {
            sendRecords(producer, tp, numRecords, System.currentTimeMillis());

            var topic1 = "tblablac"; // matches the subscription pattern
            cluster.createTopic(topic1, 2, (short) BROKER_COUNT);
            sendRecords(producer, new TopicPartition(topic1, 0), 1000, System.currentTimeMillis());
            sendRecords(producer, new TopicPartition(topic1, 1), 1000, System.currentTimeMillis());

            assertEquals(0, consumer.assignment().size());

            consumer.subscribe(Pattern.compile("t.*c"), new TestConsumerReassignmentListener());

            Set<TopicPartition> assignment = Set.of(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic1, 0),
                new TopicPartition(topic1, 1)
            );
            awaitAssignment(consumer, assignment);

            consumer.unsubscribe();
            assertEquals(0, consumer.assignment().size());
        }
    }

    @ClusterTest
    public void testAsyncConsumerRe2JPatternSubscription() throws InterruptedException {
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT));
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var topic1 = "tblablac"; // matches subscribed pattern
            cluster.createTopic(topic1, 2, (short) BROKER_COUNT);

            var topic2 = "tblablak"; // does not match subscribed pattern
            cluster.createTopic(topic2, 2, (short) BROKER_COUNT);

            var topic3 = "tblab1"; // does not match subscribed pattern
            cluster.createTopic(topic3, 2, (short) BROKER_COUNT);

            assertEquals(0, consumer.assignment().size());
            var pattern = new SubscriptionPattern("t.*c");
            consumer.subscribe(pattern);

            Set<TopicPartition> assignment = Set.of(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic1, 0),
                new TopicPartition(topic1, 1)
            );
            awaitAssignment(consumer, assignment);
            
            consumer.unsubscribe();
            assertEquals(0, consumer.assignment().size());
            // Subscribe to a different pattern to match topic2 (that did not match before)
            pattern = new SubscriptionPattern(topic2 + ".*");
            consumer.subscribe(pattern);

            assignment = Set.of(
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1)
            );
            awaitAssignment(consumer, assignment);
        }
    }

    @ClusterTest
    public void testAsyncConsumerRe2JPatternSubscriptionFetch() throws InterruptedException {
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT));
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var topic1 = "topic1"; // matches subscribed pattern
            cluster.createTopic(topic1, 2, (short) BROKER_COUNT);
            assertEquals(0, consumer.assignment().size());

            var pattern = new SubscriptionPattern("topic.*");
            consumer.subscribe(pattern);

            Set<TopicPartition> assignment = Set.of(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic1, 0),
                new TopicPartition(topic1, 1)
            );
            awaitAssignment(consumer, assignment);

            var totalRecords = 10;
            var startingTimestamp = System.currentTimeMillis();
            var tp = new TopicPartition(topic1, 0);
            sendRecords(cluster, tp, totalRecords, startingTimestamp);
            consumeAndVerifyRecords(consumer, tp, totalRecords, 0, 0, startingTimestamp);
        }
    }

    @ClusterTest
    public void testAsyncConsumerRe2JPatternExpandSubscription() throws InterruptedException {
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT));
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var topic1 = "topic1"; // matches first pattern
            cluster.createTopic(topic1, 2, (short) BROKER_COUNT);

            var topic2 = "topic2"; // does not match first pattern
            cluster.createTopic(topic2, 2, (short) BROKER_COUNT);
            
            assertEquals(0, consumer.assignment().size());
            var pattern = new SubscriptionPattern("topic1.*");
            consumer.subscribe(pattern);

            Set<TopicPartition> assignment = Set.of(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic1, 1)
            );
            awaitAssignment(consumer, assignment);

            consumer.unsubscribe();
            assertEquals(0, consumer.assignment().size());

            // Subscribe to a different pattern that should match
            // the same topics the member already had plus new ones
            pattern = new SubscriptionPattern("topic1|topic2");
            consumer.subscribe(pattern);

            Set<TopicPartition> expandedAssignment = new HashSet<>(assignment);
            expandedAssignment.add(new TopicPartition(topic2, 0));
            expandedAssignment.add(new TopicPartition(topic2, 1));
            awaitAssignment(consumer, expandedAssignment);
        }
    }

    @ClusterTest
    public void testTopicIdSubscriptionWithRe2JRegexAndOffsetsFetch() throws InterruptedException {
        var topic1 = "topic1"; // matches subscribed pattern
        cluster.createTopic(topic1, 2, (short) BROKER_COUNT);

        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT));
        try (
                Producer<byte[], byte[]> producer = cluster.producer();
                Consumer<byte[], byte[]> consumer = cluster.consumer(config)
        ) {
            assertEquals(0, consumer.assignment().size());

            // Subscribe to broker-side regex and fetch. This will require metadata for topic IDs.
            var pattern = new SubscriptionPattern("topic.*");
            consumer.subscribe(pattern);
            var assignment = Set.of(
                    new TopicPartition(topic, 0),
                    new TopicPartition(topic, 1),
                    new TopicPartition(topic1, 0),
                    new TopicPartition(topic1, 1));
            awaitAssignment(consumer, assignment);
            var totalRecords = 10;
            var startingTimestamp = System.currentTimeMillis();
            var tp = new TopicPartition(topic1, 0);
            sendRecords(producer, tp, totalRecords, startingTimestamp);
            consumeAndVerifyRecords(consumer, tp, totalRecords, 0, 0, startingTimestamp);

            // Fetch offsets for known and unknown topics. This will require metadata for topic names temporarily (transient topics)
            var topic2 = "newTopic2";
            cluster.createTopic(topic2, 2, (short) BROKER_COUNT);
            var unassignedPartition = new TopicPartition(topic2, 0);
            var offsets = consumer.endOffsets(List.of(unassignedPartition, tp));
            var expectedOffsets = Map.of(
                    unassignedPartition, 0L,
                    tp, (long) totalRecords);
            assertEquals(expectedOffsets, offsets);

            // Fetch records again with the regex subscription. This will require metadata for topic IDs again.
            sendRecords(producer, tp, totalRecords, startingTimestamp);
            consumeAndVerifyRecords(consumer, tp, totalRecords, totalRecords, 0, startingTimestamp);
        }
    }

    @ClusterTest
    public void testRe2JPatternSubscriptionAndTopicSubscription() throws InterruptedException {
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT));
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var topic1 = "topic1"; // matches subscribed pattern
            cluster.createTopic(topic1, 2, (short) BROKER_COUNT);

            var topic11 = "topic11"; // matches subscribed pattern
            cluster.createTopic(topic11, 2, (short) BROKER_COUNT);

            var topic2 = "topic2"; // does not match subscribed pattern
            cluster.createTopic(topic2, 2, (short) BROKER_COUNT);

            assertEquals(0, consumer.assignment().size());
            // Subscribe to pattern
            var pattern = new SubscriptionPattern("topic1.*");
            consumer.subscribe(pattern);

            Set<TopicPartition> patternAssignment = Set.of(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic1, 1),
                new TopicPartition(topic11, 0),
                new TopicPartition(topic11, 1)
            );
            awaitAssignment(consumer, patternAssignment);
            consumer.unsubscribe();
            assertEquals(0, consumer.assignment().size());

            // Subscribe to explicit topic names
            consumer.subscribe(List.of(topic2));

            Set<TopicPartition> assignment = Set.of(
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1)
            );
            awaitAssignment(consumer, assignment);
            consumer.unsubscribe();

            // Subscribe to pattern again
            consumer.subscribe(pattern);
            awaitAssignment(consumer, patternAssignment);
        }
    }
    

    @ClusterTest
    public void testRe2JPatternSubscriptionInvalidRegex() throws InterruptedException {
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT));
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            assertEquals(0, consumer.assignment().size());

            var pattern = new SubscriptionPattern("(t.*c");
            consumer.subscribe(pattern);

            waitForPollThrowException(consumer, InvalidRegularExpression.class);
            consumer.unsubscribe();
        }
    }

    @ClusterTest
    public void testClassicConsumerExpandingTopicSubscriptions() throws InterruptedException {
        testExpandingTopicSubscriptions(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerExpandingTopicSubscriptions() throws InterruptedException {
        testExpandingTopicSubscriptions(GroupProtocol.CONSUMER);
    }

    public void testExpandingTopicSubscriptions(GroupProtocol groupProtocol) throws InterruptedException {
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var otherTopic = "other";

            Set<TopicPartition> initialAssignment = Set.of(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1)
            );
            consumer.subscribe(List.of(topic));
            awaitAssignment(consumer, initialAssignment);

            cluster.createTopic(otherTopic, 2, (short) BROKER_COUNT);

            Set<TopicPartition> expandedAssignment = new HashSet<>(initialAssignment);
            expandedAssignment.add(new TopicPartition(otherTopic, 0));
            expandedAssignment.add(new TopicPartition(otherTopic, 1));

            consumer.subscribe(List.of(topic, otherTopic));
            awaitAssignment(consumer, expandedAssignment);
        }
    }

    @ClusterTest
    public void testClassicConsumerShrinkingTopicSubscriptions() throws InterruptedException {
        testShrinkingTopicSubscriptions(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerShrinkingTopicSubscriptions() throws InterruptedException {
        testShrinkingTopicSubscriptions(GroupProtocol.CONSUMER);
    }

    public void testShrinkingTopicSubscriptions(GroupProtocol groupProtocol) throws InterruptedException {
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var otherTopic = "other";
            cluster.createTopic(otherTopic, 2, (short) BROKER_COUNT);

            Set<TopicPartition> initialAssignment = Set.of(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(otherTopic, 0),
                new TopicPartition(otherTopic, 1)
            );
            consumer.subscribe(List.of(topic, otherTopic));
            awaitAssignment(consumer, initialAssignment);

            Set<TopicPartition> shrunkenAssignment = Set.of(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1)
            );
            consumer.subscribe(List.of(topic));
            awaitAssignment(consumer, shrunkenAssignment);
        }
    }

    @ClusterTest
    public void testClassicConsumerUnsubscribeTopic() throws InterruptedException {
        testUnsubscribeTopic(Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            SESSION_TIMEOUT_MS_CONFIG, 100, // timeout quickly to avoid slow test
            HEARTBEAT_INTERVAL_MS_CONFIG, 30
        ));
    }

    @ClusterTest
    public void testAsyncConsumerUnsubscribeTopic() throws InterruptedException {
        testUnsubscribeTopic(Map.of(GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)));
    }

    public void testUnsubscribeTopic(Map<String, Object> config) throws InterruptedException {
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            var listener = new TestConsumerReassignmentListener();
            consumer.subscribe(List.of(topic), listener);

            // the initial subscription should cause a callback execution
            awaitRebalance(consumer, listener);

            consumer.subscribe(List.of());
            assertEquals(0, consumer.assignment().size());
        }
    }

    @ClusterTest
    public void testClassicConsumerSubscribeInvalidTopicCanUnsubscribe() throws InterruptedException {
        testSubscribeInvalidTopicCanUnsubscribe(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerSubscribeInvalidTopicCanUnsubscribe() throws InterruptedException {
        testSubscribeInvalidTopicCanUnsubscribe(GroupProtocol.CONSUMER);
    }

    public void testSubscribeInvalidTopicCanUnsubscribe(GroupProtocol groupProtocol) throws InterruptedException {
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            setupSubscribeInvalidTopic(consumer);
            assertDoesNotThrow(consumer::unsubscribe);
        }
    }

    @ClusterTest
    public void testClassicConsumerSubscribeInvalidTopicCanClose() throws InterruptedException {
        testSubscribeInvalidTopicCanClose(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testAsyncConsumerSubscribeInvalidTopicCanClose() throws InterruptedException {
        testSubscribeInvalidTopicCanClose(GroupProtocol.CONSUMER);
    }

    public void testSubscribeInvalidTopicCanClose(GroupProtocol groupProtocol) throws InterruptedException {
        Map<String, Object> config = Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            setupSubscribeInvalidTopic(consumer);
            assertDoesNotThrow(() -> consumer.close());
        }
    }

    private void setupSubscribeInvalidTopic(Consumer<byte[], byte[]> consumer) throws InterruptedException {
        // Invalid topic name due to space
        var invalidTopicName = "topic abc";
        consumer.subscribe(List.of(invalidTopicName));

        InvalidTopicException[] exception = {null};
        TestUtils.waitForCondition(() -> {
            try {
                consumer.poll(Duration.ofMillis(500));
            } catch (InvalidTopicException e) {
                exception[0] = e;
            } catch (Throwable e) {
                fail("An InvalidTopicException should be thrown. But " + e.getClass() + " is thrown");
            }
            return exception[0] != null;
        }, 5000, "An InvalidTopicException should be thrown.");

        assertEquals("Invalid topics: [" + invalidTopicName + "]", exception[0].getMessage());
    }
}
