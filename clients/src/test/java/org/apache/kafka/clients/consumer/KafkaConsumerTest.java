/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FetchResponse.PartitionData;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.MockConsumerInterceptor;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaConsumerTest {
    private final String topic = "test";
    private final TopicPartition tp0 = new TopicPartition(topic, 0);
    private final TopicPartition tp1 = new TopicPartition(topic, 1);

    private final String topic2 = "test2";
    private final TopicPartition t2p0 = new TopicPartition(topic2, 0);

    private final String topic3 = "test3";
    private final TopicPartition t3p0 = new TopicPartition(topic3, 0);

    @Test
    public void testConstructorClose() throws Exception {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "some.invalid.hostname.foo.bar:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
        try {
            new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        } catch (KafkaException e) {
            assertEquals(oldInitCount + 1, MockMetricsReporter.INIT_COUNT.get());
            assertEquals(oldCloseCount + 1, MockMetricsReporter.CLOSE_COUNT.get());
            assertEquals("Failed to construct kafka consumer", e.getMessage());
            return;
        }
        Assert.fail("should have caught an exception and returned");
    }

    @Test
    public void testOsDefaultSocketBufferSizes() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(
                config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.close();
    }

    @Test(expected = KafkaException.class)
    public void testInvalidSocketSendBufferSize() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, -2);
        new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test(expected = KafkaException.class)
    public void testInvalidSocketReceiveBufferSize() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, -2);
        new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test
    public void testSubscription() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();

        consumer.subscribe(singletonList(topic));
        assertEquals(singleton(topic), consumer.subscription());
        assertTrue(consumer.assignment().isEmpty());

        consumer.subscribe(Collections.<String>emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());

        consumer.assign(singletonList(tp0));
        assertTrue(consumer.subscription().isEmpty());
        assertEquals(singleton(tp0), consumer.assignment());

        consumer.unsubscribe();
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());

        consumer.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubscriptionOnNullTopicCollection() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();

        try {
            consumer.subscribe(null);
        } finally {
            consumer.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubscriptionOnNullTopic() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        String nullTopic = null;

        try {
            consumer.subscribe(singletonList(nullTopic));
        } finally {
            consumer.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubscriptionOnEmptyTopic() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        String emptyTopic = "  ";

        try {
            consumer.subscribe(singletonList(emptyTopic));
        } finally {
            consumer.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubscriptionOnNullPattern() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        Pattern pattern = null;

        try {
            consumer.subscribe(pattern, new NoOpConsumerRebalanceListener());
        } finally {
            consumer.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSeekNegative() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testSeekNegative");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        try {
            consumer.assign(Arrays.asList(new TopicPartition("nonExistTopic", 0)));
            consumer.seek(new TopicPartition("nonExistTopic", 0), -1);
        } finally {
            consumer.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssignOnNullTopicPartition() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testAssignOnNullTopicPartition");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        try {
            consumer.assign(null);
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testAssignOnEmptyTopicPartition() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();

        consumer.assign(Collections.<TopicPartition>emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());

        consumer.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssignOnNullTopicInPartition() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testAssignOnNullTopicInPartition");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        try {
            consumer.assign(Arrays.asList(new TopicPartition(null, 0)));
        } finally {
            consumer.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssignOnEmptyTopicInPartition() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testAssignOnEmptyTopicInPartition");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        try {
            consumer.assign(Arrays.asList(new TopicPartition("  ", 0)));
        } finally {
            consumer.close();
        }
    }

    @Test
    public void testInterceptorConstructorClose() throws Exception {
        try {
            Properties props = new Properties();
            // test with client ID assigned by KafkaConsumer
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            props.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName());

            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
                    props, new StringDeserializer(), new StringDeserializer());
            assertEquals(1, MockConsumerInterceptor.INIT_COUNT.get());
            assertEquals(0, MockConsumerInterceptor.CLOSE_COUNT.get());

            consumer.close();
            assertEquals(1, MockConsumerInterceptor.INIT_COUNT.get());
            assertEquals(1, MockConsumerInterceptor.CLOSE_COUNT.get());
            // Cluster metadata will only be updated on calling poll.
            Assert.assertNull(MockConsumerInterceptor.CLUSTER_META.get());

        } finally {
            // cleanup since we are using mutable static variables in MockConsumerInterceptor
            MockConsumerInterceptor.resetCounters();
        }
    }

    @Test
    public void testPause() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();

        consumer.assign(singletonList(tp0));
        assertEquals(singleton(tp0), consumer.assignment());
        assertTrue(consumer.paused().isEmpty());

        consumer.pause(singleton(tp0));
        assertEquals(singleton(tp0), consumer.paused());

        consumer.resume(singleton(tp0));
        assertTrue(consumer.paused().isEmpty());

        consumer.unsubscribe();
        assertTrue(consumer.paused().isEmpty());

        consumer.close();
    }

    private KafkaConsumer<byte[], byte[]> newConsumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my.consumer");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test
    public void verifyHeartbeatSent() throws Exception {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 1000;
        int autoCommitIntervalMs = 10000;

        Time time = new MockTime();
        Cluster cluster = TestUtils.singletonCluster(topic, 1);
        Node node = cluster.nodes().get(0);

        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());

        MockClient client = new MockClient(time);
        client.setNode(node);
        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, true, autoCommitIntervalMs);

        consumer.subscribe(Arrays.asList(topic), getConsumerRebalanceListener(consumer));
        Node coordinator = prepareRebalance(client, node, assignor, Arrays.asList(tp0), null);

        // initial fetch
        client.prepareResponseFrom(fetchResponse(tp0, 0, 0), node);

        consumer.poll(0);
        assertEquals(Collections.singleton(tp0), consumer.assignment());

        AtomicBoolean heartbeatReceived = prepareHeartbeatResponse(client, coordinator);

        // heartbeat interval is 2 seconds
        time.sleep(heartbeatIntervalMs);
        Thread.sleep(heartbeatIntervalMs);

        consumer.poll(0);

        assertTrue(heartbeatReceived.get());
    }

    @Test
    public void verifyHeartbeatSentWhenFetchedDataReady() throws Exception {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 1000;
        int autoCommitIntervalMs = 10000;

        Time time = new MockTime();
        Cluster cluster = TestUtils.singletonCluster(topic, 1);
        Node node = cluster.nodes().get(0);

        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);
        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, true, autoCommitIntervalMs);

        consumer.subscribe(Arrays.asList(topic), getConsumerRebalanceListener(consumer));
        Node coordinator = prepareRebalance(client, node, assignor, Arrays.asList(tp0), null);

        consumer.poll(0);

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(fetchResponse(tp0, 0, 5), node);
        client.poll(0, time.milliseconds());

        client.prepareResponseFrom(fetchResponse(tp0, 5, 0), node);
        AtomicBoolean heartbeatReceived = prepareHeartbeatResponse(client, coordinator);

        time.sleep(heartbeatIntervalMs);
        Thread.sleep(heartbeatIntervalMs);

        consumer.poll(0);

        assertTrue(heartbeatReceived.get());
    }

    @Test
    public void verifyNoCoordinatorLookupForManualAssignmentWithSeek() {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 3000;
        int heartbeatIntervalMs = 2000;
        int autoCommitIntervalMs = 1000;

        Time time = new MockTime();
        Cluster cluster = TestUtils.singletonCluster(topic, 1);
        Node node = cluster.nodes().get(0);

        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);
        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, true, autoCommitIntervalMs);
        consumer.assign(Arrays.asList(tp0));
        consumer.seekToBeginning(Arrays.asList(tp0));

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(listOffsetsResponse(Collections.singletonMap(tp0, 50L), Errors.NONE.code()));
        client.prepareResponse(fetchResponse(tp0, 50L, 5));

        ConsumerRecords<String, String> records = consumer.poll(0);
        assertEquals(5, records.count());
        assertEquals(55L, consumer.position(tp0));
    }

    @Test
    public void testCommitsFetchedDuringAssign() {
        long offset1 = 10000;
        long offset2 = 20000;

        int rebalanceTimeoutMs = 6000;
        int sessionTimeoutMs = 3000;
        int heartbeatIntervalMs = 2000;
        int autoCommitIntervalMs = 1000;

        Time time = new MockTime();
        Cluster cluster = TestUtils.singletonCluster(topic, 1);
        Node node = cluster.nodes().get(0);

        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);
        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, true, autoCommitIntervalMs);
        consumer.assign(Arrays.asList(tp0));

        // lookup coordinator
        client.prepareResponseFrom(new GroupCoordinatorResponse(Errors.NONE.code(), node).toStruct(), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // fetch offset for one topic
        client.prepareResponseFrom(
                offsetResponse(Collections.singletonMap(tp0, offset1), Errors.NONE.code()),
                coordinator);

        assertEquals(offset1, consumer.committed(tp0).offset());

        consumer.assign(Arrays.asList(tp0, tp1));

        // fetch offset for two topics
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(tp0, offset1);
        offsets.put(tp1, offset2);
        client.prepareResponseFrom(offsetResponse(offsets, Errors.NONE.code()), coordinator);

        assertEquals(offset1, consumer.committed(tp0).offset());
        assertEquals(offset2, consumer.committed(tp1).offset());
    }

    @Test
    public void testAutoCommitSentBeforePositionUpdate() {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;

        // adjust auto commit interval lower than heartbeat so we don't need to deal with
        // a concurrent heartbeat request
        int autoCommitIntervalMs = 1000;

        Time time = new MockTime();
        Cluster cluster = TestUtils.singletonCluster(topic, 1);
        Node node = cluster.nodes().get(0);

        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);
        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, true, autoCommitIntervalMs);

        consumer.subscribe(Arrays.asList(topic), getConsumerRebalanceListener(consumer));
        Node coordinator = prepareRebalance(client, node, assignor, Arrays.asList(tp0), null);

        consumer.poll(0);

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(fetchResponse(tp0, 0, 5), node);
        client.poll(0, time.milliseconds());

        time.sleep(autoCommitIntervalMs);

        client.prepareResponseFrom(fetchResponse(tp0, 5, 0), node);

        // no data has been returned to the user yet, so the committed offset should be 0
        AtomicBoolean commitReceived = prepareOffsetCommitResponse(client, coordinator, tp0, 0);

        consumer.poll(0);

        assertTrue(commitReceived.get());
    }

    @Test
    public void testRegexSubscription() {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;
        int autoCommitIntervalMs = 1000;

        String unmatchedTopic = "unmatched";

        Time time = new MockTime();

        Map<String, Integer> topicMetadata = new HashMap<>();
        topicMetadata.put(topic, 1);
        topicMetadata.put(unmatchedTopic, 1);

        Cluster cluster = TestUtils.clusterWith(1, topicMetadata);
        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        Node node = cluster.nodes().get(0);

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);
        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, true, autoCommitIntervalMs);


        prepareRebalance(client, node, singleton(topic), assignor, singletonList(tp0), null);

        consumer.subscribe(Pattern.compile(topic), getConsumerRebalanceListener(consumer));

        client.prepareMetadataUpdate(cluster);

        consumer.poll(0);
        assertEquals(singleton(topic), consumer.subscription());
        assertEquals(singleton(tp0), consumer.assignment());
    }

    @Test
    public void testChangingRegexSubscription() {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;
        int autoCommitIntervalMs = 1000;
        PartitionAssignor assignor = new RoundRobinAssignor();

        String otherTopic = "other";
        TopicPartition otherTopicPartition = new TopicPartition(otherTopic, 0);

        Time time = new MockTime();

        Map<String, Integer> topicMetadata = new HashMap<>();
        topicMetadata.put(topic, 1);
        topicMetadata.put(otherTopic, 1);

        Cluster cluster = TestUtils.clusterWith(1, topicMetadata);
        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        Node node = cluster.nodes().get(0);

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);

        metadata.update(cluster, time.milliseconds());

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, false, autoCommitIntervalMs);

        Node coordinator = prepareRebalance(client, node, singleton(topic), assignor, singletonList(tp0), null);
        consumer.subscribe(Pattern.compile(topic), getConsumerRebalanceListener(consumer));

        client.prepareMetadataUpdate(cluster);

        consumer.poll(0);
        assertEquals(singleton(topic), consumer.subscription());

        consumer.subscribe(Pattern.compile(otherTopic), getConsumerRebalanceListener(consumer));

        client.prepareMetadataUpdate(cluster);

        prepareRebalance(client, node, singleton(otherTopic), assignor, singletonList(otherTopicPartition), coordinator);
        consumer.poll(0);

        assertEquals(singleton(otherTopic), consumer.subscription());
    }

    @Test
    public void testWakeupWithFetchDataAvailable() {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;

        // adjust auto commit interval lower than heartbeat so we don't need to deal with
        // a concurrent heartbeat request
        int autoCommitIntervalMs = 1000;

        Time time = new MockTime();
        Cluster cluster = TestUtils.singletonCluster(topic, 1);
        Node node = cluster.nodes().get(0);

        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);
        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, true, autoCommitIntervalMs);

        consumer.subscribe(Arrays.asList(topic), getConsumerRebalanceListener(consumer));
        prepareRebalance(client, node, assignor, Arrays.asList(tp0), null);

        consumer.poll(0);

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(fetchResponse(tp0, 0, 5), node);
        client.poll(0, time.milliseconds());

        consumer.wakeup();

        try {
            consumer.poll(0);
            fail();
        } catch (WakeupException e) {
        }

        // make sure the position hasn't been updated
        assertEquals(0, consumer.position(tp0));

        // the next poll should return the completed fetch
        ConsumerRecords<String, String> records = consumer.poll(0);
        assertEquals(5, records.count());
    }

    @Test
    public void fetchResponseWithUnexpectedPartitionIsIgnored() {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;

        // adjust auto commit interval lower than heartbeat so we don't need to deal with
        // a concurrent heartbeat request
        int autoCommitIntervalMs = 1000;

        Time time = new MockTime();
        Cluster cluster = TestUtils.singletonCluster(singletonMap(topic, 1));
        Node node = cluster.nodes().get(0);

        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);
        PartitionAssignor assignor = new RangeAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, true, autoCommitIntervalMs);

        consumer.subscribe(singletonList(topic), getConsumerRebalanceListener(consumer));

        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        Map<TopicPartition, FetchInfo> fetches1 = new HashMap<>();
        fetches1.put(tp0, new FetchInfo(0, 1));
        fetches1.put(t2p0, new FetchInfo(0, 10)); // not assigned and not fetched
        client.prepareResponseFrom(fetchResponse(fetches1), node);

        ConsumerRecords<String, String> records = consumer.poll(0);
        assertEquals(0, records.count());
    }

    /**
     * Verify that when a consumer changes its topic subscription its assigned partitions
     * do not immediately change, and the latest consumed offsets of its to-be-revoked
     * partitions are properly committed (when auto-commit is enabled).
     * Upon unsubscribing from subscribed topics the consumer subscription and assignment
     * are both updated right away but its consumed offsets are not auto committed.
     */
    @Test
    public void testSubscriptionChangesWithAutoCommitEnabled() {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;

        // adjust auto commit interval lower than heartbeat so we don't need to deal with
        // a concurrent heartbeat request
        int autoCommitIntervalMs = 1000;

        Time time = new MockTime();
        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        tpCounts.put(topic3, 1);
        Cluster cluster = TestUtils.singletonCluster(tpCounts);
        Node node = cluster.nodes().get(0);

        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);
        PartitionAssignor assignor = new RangeAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, true, autoCommitIntervalMs);

        // initial subscription
        consumer.subscribe(Arrays.asList(topic, topic2), getConsumerRebalanceListener(consumer));

        // verify that subscription has changed but assignment is still unchanged
        assertTrue(consumer.subscription().size() == 2);
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic2));
        assertTrue(consumer.assignment().isEmpty());

        // mock rebalance responses
        Node coordinator = prepareRebalance(client, node, assignor, Arrays.asList(tp0, t2p0), null);

        consumer.poll(0);

        // verify that subscription is still the same, and now assignment has caught up
        assertTrue(consumer.subscription().size() == 2);
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic2));
        assertTrue(consumer.assignment().size() == 2);
        assertTrue(consumer.assignment().contains(tp0) && consumer.assignment().contains(t2p0));

        // mock a response to the outstanding fetch so that we have data available on the next poll
        Map<TopicPartition, FetchInfo> fetches1 = new HashMap<>();
        fetches1.put(tp0, new FetchInfo(0, 1));
        fetches1.put(t2p0, new FetchInfo(0, 10));
        client.respondFrom(fetchResponse(fetches1), node);
        client.poll(0, time.milliseconds());

        ConsumerRecords<String, String> records = consumer.poll(0);

        // clear out the prefetch so it doesn't interfere with the rest of the test
        fetches1.put(tp0, new FetchInfo(1, 0));
        fetches1.put(t2p0, new FetchInfo(10, 0));
        client.respondFrom(fetchResponse(fetches1), node);
        client.poll(0, time.milliseconds());

        // verify that the fetch occurred as expected
        assertEquals(11, records.count());
        assertEquals(1L, consumer.position(tp0));
        assertEquals(10L, consumer.position(t2p0));

        // subscription change
        consumer.subscribe(Arrays.asList(topic, topic3), getConsumerRebalanceListener(consumer));

        // verify that subscription has changed but assignment is still unchanged
        assertTrue(consumer.subscription().size() == 2);
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic3));
        assertTrue(consumer.assignment().size() == 2);
        assertTrue(consumer.assignment().contains(tp0) && consumer.assignment().contains(t2p0));

        // mock the offset commit response for to be revoked partitions
        Map<TopicPartition, Long> partitionOffsets1 = new HashMap<>();
        partitionOffsets1.put(tp0, 1L);
        partitionOffsets1.put(t2p0, 10L);
        AtomicBoolean commitReceived = prepareOffsetCommitResponse(client, coordinator, partitionOffsets1);

        // mock rebalance responses
        prepareRebalance(client, node, assignor, Arrays.asList(tp0, t3p0), coordinator);

        // mock a response to the next fetch from the new assignment
        Map<TopicPartition, FetchInfo> fetches2 = new HashMap<>();
        fetches2.put(tp0, new FetchInfo(1, 1));
        fetches2.put(t3p0, new FetchInfo(0, 100));
        client.prepareResponse(fetchResponse(fetches2));

        records = consumer.poll(0);

        // verify that the fetch occurred as expected
        assertEquals(101, records.count());
        assertEquals(2L, consumer.position(tp0));
        assertEquals(100L, consumer.position(t3p0));

        // verify that the offset commits occurred as expected
        assertTrue(commitReceived.get());

        // verify that subscription is still the same, and now assignment has caught up
        assertTrue(consumer.subscription().size() == 2);
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic3));
        assertTrue(consumer.assignment().size() == 2);
        assertTrue(consumer.assignment().contains(tp0) && consumer.assignment().contains(t3p0));

        consumer.unsubscribe();

        // verify that subscription and assignment are both cleared
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());

        consumer.close();
    }

    /**
     * Verify that when a consumer changes its topic subscription its assigned partitions
     * do not immediately change, and the consumed offsets of its to-be-revoked partitions
     * are not committed (when auto-commit is disabled).
     * Upon unsubscribing from subscribed topics, the assigned partitions immediately
     * change but if auto-commit is disabled the consumer offsets are not committed.
     */
    @Test
    public void testSubscriptionChangesWithAutoCommitDisabled() {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;
        int autoCommitIntervalMs = 1000;

        Time time = new MockTime();
        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        Cluster cluster = TestUtils.singletonCluster(tpCounts);
        Node node = cluster.nodes().get(0);

        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);
        PartitionAssignor assignor = new RangeAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, false, autoCommitIntervalMs);

        // initial subscription
        consumer.subscribe(Arrays.asList(topic), getConsumerRebalanceListener(consumer));

        // verify that subscription has changed but assignment is still unchanged
        assertTrue(consumer.subscription().equals(Collections.singleton(topic)));
        assertTrue(consumer.assignment().isEmpty());

        // mock rebalance responses
        prepareRebalance(client, node, assignor, Arrays.asList(tp0), null);

        consumer.poll(0);

        // verify that subscription is still the same, and now assignment has caught up
        assertTrue(consumer.subscription().equals(Collections.singleton(topic)));
        assertTrue(consumer.assignment().equals(Collections.singleton(tp0)));

        consumer.poll(0);

        // subscription change
        consumer.subscribe(Arrays.asList(topic2), getConsumerRebalanceListener(consumer));

        // verify that subscription has changed but assignment is still unchanged
        assertTrue(consumer.subscription().equals(Collections.singleton(topic2)));
        assertTrue(consumer.assignment().equals(Collections.singleton(tp0)));

        // the auto commit is disabled, so no offset commit request should be sent
        for (ClientRequest req: client.requests())
            assertTrue(req.request().header().apiKey() != ApiKeys.OFFSET_COMMIT.id);

        // subscription change
        consumer.unsubscribe();

        // verify that subscription and assignment are both updated
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());

        // the auto commit is disabled, so no offset commit request should be sent
        for (ClientRequest req: client.requests())
            assertTrue(req.request().header().apiKey() != ApiKeys.OFFSET_COMMIT.id);

        consumer.close();
    }

    @Test
    public void testManualAssignmentChangeWithAutoCommitEnabled() {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;
        int autoCommitIntervalMs = 1000;

        Time time = new MockTime();
        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        Cluster cluster = TestUtils.singletonCluster(tpCounts);
        Node node = cluster.nodes().get(0);

        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);
        PartitionAssignor assignor = new RangeAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, true, autoCommitIntervalMs);

        // lookup coordinator
        client.prepareResponseFrom(new GroupCoordinatorResponse(Errors.NONE.code(), node).toStruct(), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // manual assignment
        consumer.assign(Arrays.asList(tp0));
        consumer.seekToBeginning(Arrays.asList(tp0));

        // fetch offset for one topic
        client.prepareResponseFrom(
                offsetResponse(Collections.singletonMap(tp0, 0L), Errors.NONE.code()),
                coordinator);
        assertEquals(0, consumer.committed(tp0).offset());

        // verify that assignment immediately changes
        assertTrue(consumer.assignment().equals(Collections.singleton(tp0)));

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(listOffsetsResponse(Collections.singletonMap(tp0, 10L), Errors.NONE.code()));
        client.prepareResponse(fetchResponse(tp0, 10L, 1));

        ConsumerRecords<String, String> records = consumer.poll(0);
        assertEquals(1, records.count());
        assertEquals(11L, consumer.position(tp0));

        // mock the offset commit response for to be revoked partitions
        AtomicBoolean commitReceived = prepareOffsetCommitResponse(client, coordinator, tp0, 11);

        // new manual assignment
        consumer.assign(Arrays.asList(t2p0));

        // verify that assignment immediately changes
        assertTrue(consumer.assignment().equals(Collections.singleton(t2p0)));
        // verify that the offset commits occurred as expected
        assertTrue(commitReceived.get());

        consumer.close();
    }

    @Test
    public void testManualAssignmentChangeWithAutoCommitDisabled() {
        int rebalanceTimeoutMs = 60000;
        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;
        int autoCommitIntervalMs = 1000;

        Time time = new MockTime();
        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        Cluster cluster = TestUtils.singletonCluster(tpCounts);
        Node node = cluster.nodes().get(0);

        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());

        MockClient client = new MockClient(time, metadata);
        client.setNode(node);
        PartitionAssignor assignor = new RangeAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                rebalanceTimeoutMs, sessionTimeoutMs, heartbeatIntervalMs, false, autoCommitIntervalMs);

        // lookup coordinator
        client.prepareResponseFrom(new GroupCoordinatorResponse(Errors.NONE.code(), node).toStruct(), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // manual assignment
        consumer.assign(Arrays.asList(tp0));
        consumer.seekToBeginning(Arrays.asList(tp0));

        // fetch offset for one topic
        client.prepareResponseFrom(
                offsetResponse(Collections.singletonMap(tp0, 0L), Errors.NONE.code()),
                coordinator);
        assertEquals(0, consumer.committed(tp0).offset());

        // verify that assignment immediately changes
        assertTrue(consumer.assignment().equals(Collections.singleton(tp0)));

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(listOffsetsResponse(Collections.singletonMap(tp0, 10L), Errors.NONE.code()));
        client.prepareResponse(fetchResponse(tp0, 10L, 1));

        ConsumerRecords<String, String> records = consumer.poll(0);
        assertEquals(1, records.count());
        assertEquals(11L, consumer.position(tp0));

        // new manual assignment
        consumer.assign(Arrays.asList(t2p0));

        // verify that assignment immediately changes
        assertTrue(consumer.assignment().equals(Collections.singleton(t2p0)));

        // the auto commit is disabled, so no offset commit request should be sent
        for (ClientRequest req: client.requests())
            assertTrue(req.request().header().apiKey() != ApiKeys.OFFSET_COMMIT.id);

        consumer.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testPollWithNoSubscription() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        try {
            consumer.poll(0);
        } finally {
            consumer.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testPollWithEmptySubscription() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        consumer.subscribe(Collections.<String>emptyList());
        try {
            consumer.poll(0);
        } finally {
            consumer.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testPollWithEmptyUserAssignment() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        consumer.assign(Collections.<TopicPartition>emptySet());
        try {
            consumer.poll(0);
        } finally {
            consumer.close();
        }
    }

    private ConsumerRebalanceListener getConsumerRebalanceListener(final KafkaConsumer<String, String> consumer) {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // set initial position so we don't need a lookup
                for (TopicPartition partition : partitions)
                    consumer.seek(partition, 0);
            }
        };
    }

    private Node prepareRebalance(MockClient client, Node node, final Set<String> subscribedTopics, PartitionAssignor assignor, List<TopicPartition> partitions, Node coordinator) {
        if (coordinator == null) {
            // lookup coordinator
            client.prepareResponseFrom(new GroupCoordinatorResponse(Errors.NONE.code(), node).toStruct(), node);
            coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        }

        // join group
        client.prepareResponseFrom(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                JoinGroupRequest joinGroupRequest = new JoinGroupRequest(request.request().body());
                PartitionAssignor.Subscription subscription = ConsumerProtocol.deserializeSubscription(joinGroupRequest.groupProtocols().get(0).metadata());
                return subscribedTopics.equals(new HashSet<>(subscription.topics()));
            }
        }, joinGroupFollowerResponse(assignor, 1, "memberId", "leaderId", Errors.NONE.code()), coordinator);

        // sync group
        client.prepareResponseFrom(syncGroupResponse(partitions, Errors.NONE.code()), coordinator);

        return coordinator;
    }

    private Node prepareRebalance(MockClient client, Node node, PartitionAssignor assignor, List<TopicPartition> partitions, Node coordinator) {
        if (coordinator == null) {
            // lookup coordinator
            client.prepareResponseFrom(new GroupCoordinatorResponse(Errors.NONE.code(), node).toStruct(), node);
            coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        }

        // join group
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, "memberId", "leaderId", Errors.NONE.code()), coordinator);

        // sync group
        client.prepareResponseFrom(syncGroupResponse(partitions, Errors.NONE.code()), coordinator);

        return coordinator;
    }

    private AtomicBoolean prepareHeartbeatResponse(MockClient client, Node coordinator) {
        final AtomicBoolean heartbeatReceived = new AtomicBoolean(false);
        client.prepareResponseFrom(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                heartbeatReceived.set(true);
                return true;
            }
        }, new HeartbeatResponse(Errors.NONE.code()).toStruct(), coordinator);
        return heartbeatReceived;
    }

    private AtomicBoolean prepareOffsetCommitResponse(MockClient client, Node coordinator, final Map<TopicPartition, Long> partitionOffsets) {
        final AtomicBoolean commitReceived = new AtomicBoolean(true);
        Map<TopicPartition, Short> response = new HashMap<>();
        for (TopicPartition partition : partitionOffsets.keySet())
            response.put(partition, Errors.NONE.code());

        client.prepareResponseFrom(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                OffsetCommitRequest commitRequest = new OffsetCommitRequest(request.request().body());
                for (Map.Entry<TopicPartition, Long> partitionOffset : partitionOffsets.entrySet()) {
                    OffsetCommitRequest.PartitionData partitionData = commitRequest.offsetData().get(partitionOffset.getKey());
                    // verify that the expected offset has been committed
                    if (partitionData.offset != partitionOffset.getValue()) {
                        commitReceived.set(false);
                        return false;
                    }
                }
                return true;
            }
        }, offsetCommitResponse(response), coordinator);
        return commitReceived;
    }

    private AtomicBoolean prepareOffsetCommitResponse(MockClient client, Node coordinator, final TopicPartition partition, final long offset) {
        return prepareOffsetCommitResponse(client, coordinator, Collections.singletonMap(partition, offset));
    }

    private Struct offsetCommitResponse(Map<TopicPartition, Short> responseData) {
        OffsetCommitResponse response = new OffsetCommitResponse(responseData);
        return response.toStruct();
    }

    private Struct joinGroupFollowerResponse(PartitionAssignor assignor, int generationId, String memberId, String leaderId, short error) {
        return new JoinGroupResponse(error, generationId, assignor.name(), memberId, leaderId,
                Collections.<String, ByteBuffer>emptyMap()).toStruct();
    }

    private Struct syncGroupResponse(List<TopicPartition> partitions, short error) {
        ByteBuffer buf = ConsumerProtocol.serializeAssignment(new PartitionAssignor.Assignment(partitions));
        return new SyncGroupResponse(error, buf).toStruct();
    }

    private Struct offsetResponse(Map<TopicPartition, Long> offsets, short error) {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> partitionData = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            partitionData.put(entry.getKey(), new OffsetFetchResponse.PartitionData(entry.getValue(), "", error));
        }
        return new OffsetFetchResponse(partitionData).toStruct();
    }

    private Struct listOffsetsResponse(Map<TopicPartition, Long> offsets, short error) {
        Map<TopicPartition, ListOffsetResponse.PartitionData> partitionData = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> partitionOffset : offsets.entrySet()) {
            partitionData.put(partitionOffset.getKey(), new ListOffsetResponse.PartitionData(error,
                    1L, partitionOffset.getValue()));
        }
        return new ListOffsetResponse(partitionData, 1).toStruct();
    }

    private Struct fetchResponse(Map<TopicPartition, FetchInfo> fetches) {
        Map<TopicPartition, PartitionData> tpResponses = new HashMap<>();
        for (Map.Entry<TopicPartition, FetchInfo> fetchEntry : fetches.entrySet()) {
            TopicPartition partition = fetchEntry.getKey();
            long fetchOffset = fetchEntry.getValue().offset;
            int fetchCount = fetchEntry.getValue().count;
            MemoryRecords records = MemoryRecords.emptyRecords(ByteBuffer.allocate(1024), CompressionType.NONE);
            for (int i = 0; i < fetchCount; i++)
                records.append(fetchOffset + i, 0L, ("key-" + i).getBytes(), ("value-" + i).getBytes());
            records.close();
            tpResponses.put(partition, new FetchResponse.PartitionData(Errors.NONE.code(), 0, records.buffer()));
        }
        FetchResponse response = new FetchResponse(tpResponses, 0);
        return response.toStruct();
    }

    private Struct fetchResponse(TopicPartition partition, long fetchOffset, int count) {
        FetchInfo fetchInfo = new FetchInfo(fetchOffset, count);
        return fetchResponse(Collections.singletonMap(partition, fetchInfo));
    }

    private KafkaConsumer<String, String> newConsumer(Time time,
                                                      KafkaClient client,
                                                      Metadata metadata,
                                                      PartitionAssignor assignor,
                                                      int rebalanceTimeoutMs,
                                                      int sessionTimeoutMs,
                                                      int heartbeatIntervalMs,
                                                      boolean autoCommitEnabled,
                                                      int autoCommitIntervalMs) {
        // create a consumer with mocked time and mocked network

        String clientId = "mock-consumer";
        String groupId = "mock-group";
        String metricGroupPrefix = "consumer";
        long retryBackoffMs = 100;
        long requestTimeoutMs = 30000;
        boolean excludeInternalTopics = true;
        int minBytes = 1;
        int maxBytes = Integer.MAX_VALUE;
        int maxWaitMs = 500;
        int fetchSize = 1024 * 1024;
        int maxPollRecords = Integer.MAX_VALUE;
        boolean checkCrcs = true;

        Deserializer<String> keyDeserializer = new StringDeserializer();
        Deserializer<String> valueDeserializer = new StringDeserializer();

        OffsetResetStrategy autoResetStrategy = OffsetResetStrategy.EARLIEST;
        OffsetCommitCallback defaultCommitCallback = new ConsumerCoordinator.DefaultOffsetCommitCallback();
        List<PartitionAssignor> assignors = Arrays.asList(assignor);
        ConsumerInterceptors<String, String> interceptors = null;

        Metrics metrics = new Metrics();
        SubscriptionState subscriptions = new SubscriptionState(autoResetStrategy);
        ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(client, metadata, time, retryBackoffMs, requestTimeoutMs);
        ConsumerCoordinator consumerCoordinator = new ConsumerCoordinator(
                consumerClient,
                groupId,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                assignors,
                metadata,
                subscriptions,
                metrics,
                metricGroupPrefix,
                time,
                retryBackoffMs,
                defaultCommitCallback,
                autoCommitEnabled,
                autoCommitIntervalMs,
                interceptors,
                excludeInternalTopics);

        Fetcher<String, String> fetcher = new Fetcher<>(
                consumerClient,
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                maxPollRecords,
                checkCrcs,
                keyDeserializer,
                valueDeserializer,
                metadata,
                subscriptions,
                metrics,
                metricGroupPrefix,
                time,
                retryBackoffMs);

        return new KafkaConsumer<>(
                clientId,
                consumerCoordinator,
                keyDeserializer,
                valueDeserializer,
                fetcher,
                interceptors,
                time,
                consumerClient,
                metrics,
                subscriptions,
                metadata,
                retryBackoffMs,
                requestTimeoutMs);
    }

    private static class FetchInfo {
        long offset;
        int count;

        FetchInfo(long offset, int count) {
            this.offset = offset;
            this.count = count;
        }
    }
}
