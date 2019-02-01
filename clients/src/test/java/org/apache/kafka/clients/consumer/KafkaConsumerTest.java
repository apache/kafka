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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.ConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.Heartbeat;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.MockConsumerInterceptor;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class KafkaConsumerTest {
    private final String topic = "test";
    private final TopicPartition tp0 = new TopicPartition(topic, 0);
    private final TopicPartition tp1 = new TopicPartition(topic, 1);

    private final String topic2 = "test2";
    private final TopicPartition t2p0 = new TopicPartition(topic2, 0);

    private final String topic3 = "test3";
    private final TopicPartition t3p0 = new TopicPartition(topic3, 0);

    private final int sessionTimeoutMs = 10000;
    private final int heartbeatIntervalMs = 1000;

    // Set auto commit interval lower than heartbeat so we don't need to deal with
    // a concurrent heartbeat request
    private final int autoCommitIntervalMs = 500;

    private final String groupId = "mock-group";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMetricsReporterAutoGeneratedClientId() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                props, new StringDeserializer(), new StringDeserializer());

        MockMetricsReporter mockMetricsReporter = (MockMetricsReporter) consumer.metrics.reporters().get(0);

        Assert.assertEquals(consumer.getClientId(), mockMetricsReporter.clientId);
        consumer.close();
    }

    @Test
    public void testConstructorClose() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "invalid-23-8409-adsfsdj");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
        try {
            new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            Assert.fail("should have caught an exception and returned");
        } catch (KafkaException e) {
            assertEquals(oldInitCount + 1, MockMetricsReporter.INIT_COUNT.get());
            assertEquals(oldCloseCount + 1, MockMetricsReporter.CLOSE_COUNT.get());
            assertEquals("Failed to construct kafka consumer", e.getMessage());
        }
    }

    @Test
    public void testOsDefaultSocketBufferSizes() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(
                config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.close();
    }

    @Test(expected = KafkaException.class)
    public void testInvalidSocketSendBufferSize() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, -2);
        new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test(expected = KafkaException.class)
    public void testInvalidSocketReceiveBufferSize() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, -2);
        new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test
    public void testSubscription() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer(groupId);

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
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer(groupId)) {
            consumer.subscribe((List<String>) null);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubscriptionOnNullTopic() {
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer(groupId)) {
            consumer.subscribe(singletonList((String) null));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubscriptionOnEmptyTopic() {
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer(groupId)) {
            String emptyTopic = "  ";
            consumer.subscribe(singletonList(emptyTopic));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubscriptionOnNullPattern() {
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer(groupId)) {
            consumer.subscribe((Pattern) null);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testSubscriptionWithEmptyPartitionAssignment() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer(props)) {
            consumer.subscribe(singletonList(topic));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSeekNegative() {
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer((String) null)) {
            consumer.assign(singleton(new TopicPartition("nonExistTopic", 0)));
            consumer.seek(new TopicPartition("nonExistTopic", 0), -1);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssignOnNullTopicPartition() {
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer((String) null)) {
            consumer.assign(null);
        }
    }

    @Test
    public void testAssignOnEmptyTopicPartition() {
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer(groupId)) {
            consumer.assign(Collections.<TopicPartition>emptyList());
            assertTrue(consumer.subscription().isEmpty());
            assertTrue(consumer.assignment().isEmpty());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssignOnNullTopicInPartition() {
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer((String) null)) {
            consumer.assign(singleton(new TopicPartition(null, 0)));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAssignOnEmptyTopicInPartition() {
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer((String) null)) {
            consumer.assign(singleton(new TopicPartition("  ", 0)));
        }
    }

    @Test
    public void testInterceptorConstructorClose() throws Exception {
        try {
            Properties props = new Properties();
            // test with client ID assigned by KafkaConsumer
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            props.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName());

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
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
        KafkaConsumer<byte[], byte[]> consumer = newConsumer(groupId);

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

    private KafkaConsumer<byte[], byte[]> newConsumer(String groupId) {
        return newConsumer(groupId, Optional.empty());
    }

    private KafkaConsumer<byte[], byte[]> newConsumer(String groupId, Optional<Boolean> enableAutoCommit) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my.consumer");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        if (groupId != null)
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        if (enableAutoCommit.isPresent())
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.get().toString());
        return newConsumer(props);
    }

    private KafkaConsumer<byte[], byte[]> newConsumer(Properties props) {
        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test
    public void verifyHeartbeatSent() throws Exception {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        Node coordinator = prepareRebalance(client, node, assignor, singletonList(tp0), null);

        // initial fetch
        client.prepareResponseFrom(fetchResponse(tp0, 0, 0), node);
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(singleton(tp0), consumer.assignment());

        AtomicBoolean heartbeatReceived = prepareHeartbeatResponse(client, coordinator);

        // heartbeat interval is 2 seconds
        time.sleep(heartbeatIntervalMs);
        Thread.sleep(heartbeatIntervalMs);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));

        assertTrue(heartbeatReceived.get());
        consumer.close(Duration.ofMillis(0));
    }

    @Test
    public void verifyHeartbeatSentWhenFetchedDataReady() throws Exception {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);
        PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        Node coordinator = prepareRebalance(client, node, assignor, singletonList(tp0), null);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(fetchResponse(tp0, 0, 5), node);
        client.poll(0, time.milliseconds());

        client.prepareResponseFrom(fetchResponse(tp0, 5, 0), node);
        AtomicBoolean heartbeatReceived = prepareHeartbeatResponse(client, coordinator);

        time.sleep(heartbeatIntervalMs);
        Thread.sleep(heartbeatIntervalMs);

        consumer.poll(Duration.ZERO);

        assertTrue(heartbeatReceived.get());
        consumer.close(Duration.ofMillis(0));
    }

    @Test
    public void verifyPollTimesOutDuringMetadataUpdate() {
        final Time time = new MockTime();
        Metadata metadata = createMetadata();
        final MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        final PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        consumer.poll(Duration.ZERO);

        // The underlying client should NOT get a fetch request
        final Queue<ClientRequest> requests = client.requests();
        Assert.assertEquals(0, requests.size());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void verifyDeprecatedPollDoesNotTimeOutDuringMetadataUpdate() {
        final Time time = new MockTime();
        Metadata metadata = createMetadata();
        final MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        final PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        consumer.poll(0L);

        // The underlying client SHOULD get a fetch request
        final Queue<ClientRequest> requests = client.requests();
        Assert.assertEquals(1, requests.size());
        final Class<? extends AbstractRequest.Builder> aClass = requests.peek().requestBuilder().getClass();
        Assert.assertEquals(FetchRequest.Builder.class, aClass);
    }

    @Test
    public void verifyNoCoordinatorLookupForManualAssignmentWithSeek() {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);
        consumer.assign(singleton(tp0));
        consumer.seekToBeginning(singleton(tp0));

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(listOffsetsResponse(Collections.singletonMap(tp0, 50L)));
        client.prepareResponse(fetchResponse(tp0, 50L, 5));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
        assertEquals(5, records.count());
        assertEquals(55L, consumer.position(tp0));
        consumer.close(Duration.ofMillis(0));
    }

    @Test
    public void testFetchProgressWithMissingPartitionPosition() {
        // Verifies that we can make progress on one partition while we are awaiting
        // a reset on another partition.

        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 2));

        KafkaConsumer<String, String> consumer = newConsumerNoAutoCommit(time, client, metadata);
        consumer.assign(Arrays.asList(tp0, tp1));
        consumer.seekToEnd(singleton(tp0));
        consumer.seekToBeginning(singleton(tp1));

        client.prepareResponse(
                new MockClient.RequestMatcher() {
                    @Override
                    public boolean matches(AbstractRequest body) {
                        ListOffsetRequest request = (ListOffsetRequest) body;
                        Map<TopicPartition, ListOffsetRequest.PartitionData> timestamps = request.partitionTimestamps();
                        return timestamps.get(tp0).timestamp == ListOffsetRequest.LATEST_TIMESTAMP &&
                                timestamps.get(tp1).timestamp == ListOffsetRequest.EARLIEST_TIMESTAMP;
                    }
                }, listOffsetsResponse(Collections.singletonMap(tp0, 50L),
                        Collections.singletonMap(tp1, Errors.NOT_LEADER_FOR_PARTITION)));
        client.prepareResponse(
                new MockClient.RequestMatcher() {
                    @Override
                    public boolean matches(AbstractRequest body) {
                        FetchRequest request = (FetchRequest) body;
                        return request.fetchData().keySet().equals(singleton(tp0)) &&
                                request.fetchData().get(tp0).fetchOffset == 50L;

                    }
                }, fetchResponse(tp0, 50L, 5));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
        assertEquals(5, records.count());
        assertEquals(singleton(tp0), records.partitions());
    }

    private void initMetadata(MockClient mockClient, Map<String, Integer> partitionCounts) {
        MetadataResponse initialMetadata = TestUtils.metadataUpdateWith(1, partitionCounts);
        mockClient.updateMetadata(initialMetadata);
    }

    @Test(expected = NoOffsetForPartitionException.class)
    public void testMissingOffsetNoResetPolicy() {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                OffsetResetStrategy.NONE, true, groupId);
        consumer.assign(singletonList(tp0));

        client.prepareResponseFrom(new FindCoordinatorResponse(Errors.NONE, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // lookup committed offset and find nothing
        client.prepareResponseFrom(offsetResponse(Collections.singletonMap(tp0, -1L), Errors.NONE), coordinator);
        consumer.poll(Duration.ZERO);
    }

    @Test
    public void testResetToCommittedOffset() {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                OffsetResetStrategy.NONE, true, groupId);
        consumer.assign(singletonList(tp0));

        client.prepareResponseFrom(new FindCoordinatorResponse(Errors.NONE, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        client.prepareResponseFrom(offsetResponse(Collections.singletonMap(tp0, 539L), Errors.NONE), coordinator);
        consumer.poll(Duration.ZERO);

        assertEquals(539L, consumer.position(tp0));
    }

    @Test
    public void testResetUsingAutoResetPolicy() {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                OffsetResetStrategy.LATEST, true, groupId);
        consumer.assign(singletonList(tp0));

        client.prepareResponseFrom(new FindCoordinatorResponse(Errors.NONE, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        client.prepareResponseFrom(offsetResponse(Collections.singletonMap(tp0, -1L), Errors.NONE), coordinator);
        client.prepareResponse(listOffsetsResponse(Collections.singletonMap(tp0, 50L)));

        consumer.poll(Duration.ZERO);

        assertEquals(50L, consumer.position(tp0));
    }

    @Test
    public void testCommitsFetchedDuringAssign() {
        long offset1 = 10000;
        long offset2 = 20000;

        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 2));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);
        consumer.assign(singletonList(tp0));

        // lookup coordinator
        client.prepareResponseFrom(new FindCoordinatorResponse(Errors.NONE, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // fetch offset for one topic
        client.prepareResponseFrom(offsetResponse(Collections.singletonMap(tp0, offset1), Errors.NONE), coordinator);
        assertEquals(offset1, consumer.committed(tp0).offset());

        consumer.assign(Arrays.asList(tp0, tp1));

        // fetch offset for two topics
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(tp0, offset1);
        client.prepareResponseFrom(offsetResponse(offsets, Errors.NONE), coordinator);
        assertEquals(offset1, consumer.committed(tp0).offset());

        offsets.remove(tp0);
        offsets.put(tp1, offset2);
        client.prepareResponseFrom(offsetResponse(offsets, Errors.NONE), coordinator);
        assertEquals(offset2, consumer.committed(tp1).offset());
        consumer.close(Duration.ofMillis(0));
    }

    @Test
    public void testAutoCommitSentBeforePositionUpdate() {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        Node coordinator = prepareRebalance(client, node, assignor, singletonList(tp0), null);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(fetchResponse(tp0, 0, 5), node);
        client.poll(0, time.milliseconds());

        time.sleep(autoCommitIntervalMs);

        client.prepareResponseFrom(fetchResponse(tp0, 5, 0), node);

        // no data has been returned to the user yet, so the committed offset should be 0
        AtomicBoolean commitReceived = prepareOffsetCommitResponse(client, coordinator, tp0, 0);

        consumer.poll(Duration.ZERO);

        assertTrue(commitReceived.get());
        consumer.close(Duration.ofMillis(0));
    }

    @Test
    public void testRegexSubscription() {
        String unmatchedTopic = "unmatched";
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(topic, 1);
        partitionCounts.put(unmatchedTopic, 1);
        initMetadata(client, partitionCounts);
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);
        prepareRebalance(client, node, singleton(topic), assignor, singletonList(tp0), null);

        consumer.subscribe(Pattern.compile(topic), getConsumerRebalanceListener(consumer));

        client.prepareMetadataUpdate(TestUtils.metadataUpdateWith(1, partitionCounts));

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(singleton(topic), consumer.subscription());
        assertEquals(singleton(tp0), consumer.assignment());
        consumer.close(Duration.ofMillis(0));
    }

    @Test
    public void testChangingRegexSubscription() {
        PartitionAssignor assignor = new RoundRobinAssignor();

        String otherTopic = "other";
        TopicPartition otherTopicPartition = new TopicPartition(otherTopic, 0);

        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(topic, 1);
        partitionCounts.put(otherTopic, 1);
        initMetadata(client, partitionCounts);
        Node node = metadata.fetch().nodes().get(0);

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, false);

        Node coordinator = prepareRebalance(client, node, singleton(topic), assignor, singletonList(tp0), null);
        consumer.subscribe(Pattern.compile(topic), getConsumerRebalanceListener(consumer));

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        assertEquals(singleton(topic), consumer.subscription());

        consumer.subscribe(Pattern.compile(otherTopic), getConsumerRebalanceListener(consumer));

        client.prepareMetadataUpdate(TestUtils.metadataUpdateWith(1, partitionCounts));
        prepareRebalance(client, node, singleton(otherTopic), assignor, singletonList(otherTopicPartition), coordinator);
        consumer.poll(Duration.ZERO);

        assertEquals(singleton(otherTopic), consumer.subscription());
        consumer.close(Duration.ofMillis(0));
    }

    @Test
    public void testWakeupWithFetchDataAvailable() throws Exception {
        final Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(fetchResponse(tp0, 0, 5), node);
        client.poll(0, time.milliseconds());

        consumer.wakeup();

        try {
            consumer.poll(Duration.ZERO);
            fail();
        } catch (WakeupException e) {
        }

        // make sure the position hasn't been updated
        assertEquals(0, consumer.position(tp0));

        // the next poll should return the completed fetch
        ConsumerRecords<String, String> records = consumer.poll(Duration.ZERO);
        assertEquals(5, records.count());
        // Increment time asynchronously to clear timeouts in closing the consumer
        final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                time.sleep(sessionTimeoutMs);
            }
        }, 0L, 10L, TimeUnit.MILLISECONDS);
        consumer.close();
        exec.shutdownNow();
        exec.awaitTermination(5L, TimeUnit.SECONDS);
    }

    @Test
    public void testPollThrowsInterruptExceptionIfInterrupted() {
        final Time time = new MockTime();
        final Metadata metadata = createMetadata();
        final MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        final PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, false);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        // interrupt the thread and call poll
        try {
            Thread.currentThread().interrupt();
            expectedException.expect(InterruptException.class);
            consumer.poll(Duration.ZERO);
        } finally {
            // clear interrupted state again since this thread may be reused by JUnit
            Thread.interrupted();
        }
        consumer.close(Duration.ofMillis(0));
    }

    @Test
    public void fetchResponseWithUnexpectedPartitionIsIgnored() {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RangeAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);
        consumer.subscribe(singletonList(topic), getConsumerRebalanceListener(consumer));

        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        Map<TopicPartition, FetchInfo> fetches1 = new HashMap<>();
        fetches1.put(tp0, new FetchInfo(0, 1));
        fetches1.put(t2p0, new FetchInfo(0, 10)); // not assigned and not fetched
        client.prepareResponseFrom(fetchResponse(fetches1), node);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ZERO);
        assertEquals(0, records.count());
        consumer.close(Duration.ofMillis(0));
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
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        tpCounts.put(topic3, 1);
        initMetadata(client, tpCounts);
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RangeAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);

        // initial subscription
        consumer.subscribe(Arrays.asList(topic, topic2), getConsumerRebalanceListener(consumer));

        // verify that subscription has changed but assignment is still unchanged
        assertTrue(consumer.subscription().size() == 2);
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic2));
        assertTrue(consumer.assignment().isEmpty());

        // mock rebalance responses
        Node coordinator = prepareRebalance(client, node, assignor, Arrays.asList(tp0, t2p0), null);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        // verify that subscription is still the same, and now assignment has caught up
        assertEquals(2, consumer.subscription().size());
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic2));
        assertEquals(2, consumer.assignment().size());
        assertTrue(consumer.assignment().contains(tp0) && consumer.assignment().contains(t2p0));

        // mock a response to the outstanding fetch so that we have data available on the next poll
        Map<TopicPartition, FetchInfo> fetches1 = new HashMap<>();
        fetches1.put(tp0, new FetchInfo(0, 1));
        fetches1.put(t2p0, new FetchInfo(0, 10));
        client.respondFrom(fetchResponse(fetches1), node);
        client.poll(0, time.milliseconds());

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));

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

        records = consumer.poll(Duration.ofMillis(1));

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

        client.requests().clear();
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
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        initMetadata(client, tpCounts);
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RangeAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, false);

        // initial subscription
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));

        // verify that subscription has changed but assignment is still unchanged
        assertEquals(singleton(topic), consumer.subscription());
        assertEquals(Collections.emptySet(), consumer.assignment());

        // mock rebalance responses
        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        // verify that subscription is still the same, and now assignment has caught up
        assertEquals(singleton(topic), consumer.subscription());
        assertEquals(singleton(tp0), consumer.assignment());

        consumer.poll(Duration.ZERO);

        // subscription change
        consumer.subscribe(singleton(topic2), getConsumerRebalanceListener(consumer));

        // verify that subscription has changed but assignment is still unchanged
        assertEquals(singleton(topic2), consumer.subscription());
        assertEquals(singleton(tp0), consumer.assignment());

        // the auto commit is disabled, so no offset commit request should be sent
        for (ClientRequest req: client.requests())
            assertNotSame(ApiKeys.OFFSET_COMMIT, req.requestBuilder().apiKey());

        // subscription change
        consumer.unsubscribe();

        // verify that subscription and assignment are both updated
        assertEquals(Collections.emptySet(), consumer.subscription());
        assertEquals(Collections.emptySet(), consumer.assignment());

        // the auto commit is disabled, so no offset commit request should be sent
        for (ClientRequest req: client.requests())
            assertNotSame(ApiKeys.OFFSET_COMMIT, req.requestBuilder().apiKey());

        client.requests().clear();
        consumer.close();
    }

    @Test
    public void testManualAssignmentChangeWithAutoCommitEnabled() {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        initMetadata(client, tpCounts);
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RangeAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);

        // lookup coordinator
        client.prepareResponseFrom(new FindCoordinatorResponse(Errors.NONE, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // manual assignment
        consumer.assign(singleton(tp0));
        consumer.seekToBeginning(singleton(tp0));

        // fetch offset for one topic
        client.prepareResponseFrom(offsetResponse(Collections.singletonMap(tp0, 0L), Errors.NONE), coordinator);
        assertEquals(0, consumer.committed(tp0).offset());

        // verify that assignment immediately changes
        assertTrue(consumer.assignment().equals(singleton(tp0)));

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(listOffsetsResponse(Collections.singletonMap(tp0, 10L)));
        client.prepareResponse(fetchResponse(tp0, 10L, 1));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
        assertEquals(1, records.count());
        assertEquals(11L, consumer.position(tp0));

        // mock the offset commit response for to be revoked partitions
        AtomicBoolean commitReceived = prepareOffsetCommitResponse(client, coordinator, tp0, 11);

        // new manual assignment
        consumer.assign(singleton(t2p0));

        // verify that assignment immediately changes
        assertTrue(consumer.assignment().equals(singleton(t2p0)));
        // verify that the offset commits occurred as expected
        assertTrue(commitReceived.get());

        client.requests().clear();
        consumer.close();
    }

    @Test
    public void testManualAssignmentChangeWithAutoCommitDisabled() {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        initMetadata(client, tpCounts);
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RangeAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, false);

        // lookup coordinator
        client.prepareResponseFrom(new FindCoordinatorResponse(Errors.NONE, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // manual assignment
        consumer.assign(singleton(tp0));
        consumer.seekToBeginning(singleton(tp0));

        // fetch offset for one topic
        client.prepareResponseFrom(
                offsetResponse(Collections.singletonMap(tp0, 0L), Errors.NONE),
                coordinator);
        assertEquals(0, consumer.committed(tp0).offset());

        // verify that assignment immediately changes
        assertTrue(consumer.assignment().equals(singleton(tp0)));

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(listOffsetsResponse(Collections.singletonMap(tp0, 10L)));
        client.prepareResponse(fetchResponse(tp0, 10L, 1));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
        assertEquals(1, records.count());
        assertEquals(11L, consumer.position(tp0));

        // new manual assignment
        consumer.assign(singleton(t2p0));

        // verify that assignment immediately changes
        assertTrue(consumer.assignment().equals(singleton(t2p0)));

        // the auto commit is disabled, so no offset commit request should be sent
        for (ClientRequest req : client.requests())
            assertTrue(req.requestBuilder().apiKey() != ApiKeys.OFFSET_COMMIT);

        client.requests().clear();
        consumer.close();
    }

    @Test
    public void testOffsetOfPausedPartitions() {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 2));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RangeAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);

        // lookup coordinator
        client.prepareResponseFrom(new FindCoordinatorResponse(Errors.NONE, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // manual assignment
        Set<TopicPartition> partitions = Utils.mkSet(tp0, tp1);
        consumer.assign(partitions);
        // verify consumer's assignment
        assertEquals(partitions, consumer.assignment());

        consumer.pause(partitions);
        consumer.seekToEnd(partitions);

        // fetch and verify committed offset of two partitions
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(tp0, 0L);
        offsets.put(tp1, 0L);

        client.prepareResponseFrom(offsetResponse(offsets, Errors.NONE), coordinator);
        assertEquals(0, consumer.committed(tp0).offset());

        offsets.remove(tp0);
        offsets.put(tp1, 0L);
        client.prepareResponseFrom(offsetResponse(offsets, Errors.NONE), coordinator);
        assertEquals(0, consumer.committed(tp1).offset());

        // fetch and verify consumer's position in the two partitions
        final Map<TopicPartition, Long> offsetResponse = new HashMap<>();
        offsetResponse.put(tp0, 3L);
        offsetResponse.put(tp1, 3L);
        client.prepareResponse(listOffsetsResponse(offsetResponse));
        assertEquals(3L, consumer.position(tp0));
        assertEquals(3L, consumer.position(tp1));

        client.requests().clear();
        consumer.unsubscribe();
        consumer.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testPollWithNoSubscription() {
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer((String) null)) {
            consumer.poll(Duration.ZERO);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testPollWithEmptySubscription() {
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer(groupId)) {
            consumer.subscribe(Collections.<String>emptyList());
            consumer.poll(Duration.ZERO);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testPollWithEmptyUserAssignment() {
        try (KafkaConsumer<byte[], byte[]> consumer = newConsumer(groupId)) {
            consumer.assign(Collections.<TopicPartition>emptySet());
            consumer.poll(Duration.ZERO);
        }
    }

    @Test
    public void testGracefulClose() throws Exception {
        Map<TopicPartition, Errors> response = new HashMap<>();
        response.put(tp0, Errors.NONE);
        OffsetCommitResponse commitResponse = offsetCommitResponse(response);
        LeaveGroupResponse leaveGroupResponse = new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()));
        consumerCloseTest(5000, Arrays.asList(commitResponse, leaveGroupResponse), 0, false);
    }

    @Test
    public void testCloseTimeout() throws Exception {
        consumerCloseTest(5000, Collections.<AbstractResponse>emptyList(), 5000, false);
    }

    @Test
    public void testLeaveGroupTimeout() throws Exception {
        Map<TopicPartition, Errors> response = new HashMap<>();
        response.put(tp0, Errors.NONE);
        OffsetCommitResponse commitResponse = offsetCommitResponse(response);
        consumerCloseTest(5000, singletonList(commitResponse), 5000, false);
    }

    @Test
    public void testCloseNoWait() throws Exception {
        consumerCloseTest(0, Collections.<AbstractResponse>emptyList(), 0, false);
    }

    @Test
    public void testCloseInterrupt() throws Exception {
        consumerCloseTest(Long.MAX_VALUE, Collections.emptyList(), 0, true);
    }

    @Test
    public void closeShouldBeIdempotent() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer((String) null);
        consumer.close();
        consumer.close();
        consumer.close();
    }

    @Test
    public void testOperationsBySubscribingConsumerWithDefaultGroupId() {
        try {
            newConsumer(null, Optional.of(Boolean.TRUE));
            fail("Expected an InvalidConfigurationException");
        } catch (KafkaException e) {
            assertEquals(InvalidConfigurationException.class, e.getCause().getClass());
        }

        try {
            newConsumer((String) null).subscribe(Collections.singleton(topic));
            fail("Expected an InvalidGroupIdException");
        } catch (InvalidGroupIdException e) {
            // OK, expected
        }

        try {
            newConsumer((String) null).committed(tp0);
            fail("Expected an InvalidGroupIdException");
        } catch (InvalidGroupIdException e) {
            // OK, expected
        }

        try {
            newConsumer((String) null).commitAsync();
            fail("Expected an InvalidGroupIdException");
        } catch (InvalidGroupIdException e) {
            // OK, expected
        }

        try {
            newConsumer((String) null).commitSync();
            fail("Expected an InvalidGroupIdException");
        } catch (InvalidGroupIdException e) {
            // OK, expected
        }
    }

    @Test
    public void testOperationsByAssigningConsumerWithDefaultGroupId() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer((String) null);
        consumer.assign(singleton(tp0));

        try {
            consumer.committed(tp0);
            fail("Expected an InvalidGroupIdException");
        } catch (InvalidGroupIdException e) {
            // OK, expected
        }

        try {
            consumer.commitAsync();
            fail("Expected an InvalidGroupIdException");
        } catch (InvalidGroupIdException e) {
            // OK, expected
        }

        try {
            consumer.commitSync();
            fail("Expected an InvalidGroupIdException");
        } catch (InvalidGroupIdException e) {
            // OK, expected
        }
    }

    @Test
    public void testMetricConfigRecordingLevel() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        try (KafkaConsumer consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            assertEquals(Sensor.RecordingLevel.INFO, consumer.metrics.config().recordLevel());
        }

        props.put(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        try (KafkaConsumer consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            assertEquals(Sensor.RecordingLevel.DEBUG, consumer.metrics.config().recordLevel());
        }
    }

    @Test
    public void shouldAttemptToRejoinGroupAfterSyncGroupFailed() throws Exception {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RoundRobinAssignor();

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, false);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        client.prepareResponseFrom(new FindCoordinatorResponse(Errors.NONE, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());


        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, "memberId", "leaderId", Errors.NONE), coordinator);
        client.prepareResponseFrom(syncGroupResponse(singletonList(tp0), Errors.NONE), coordinator);

        client.prepareResponseFrom(fetchResponse(tp0, 0, 1), node);
        client.prepareResponseFrom(fetchResponse(tp0, 1, 0), node);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        // heartbeat fails due to rebalance in progress
        client.prepareResponseFrom(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return true;
            }
        }, new HeartbeatResponse(Errors.REBALANCE_IN_PROGRESS), coordinator);

        // join group
        final ByteBuffer byteBuffer = ConsumerProtocol.serializeSubscription(new PartitionAssignor.Subscription(singletonList(topic)));

        // This member becomes the leader
        final JoinGroupResponse leaderResponse = new JoinGroupResponse(Errors.NONE, 1, assignor.name(), "memberId", "memberId",
                Collections.singletonMap("memberId", byteBuffer));
        client.prepareResponseFrom(leaderResponse, coordinator);

        // sync group fails due to disconnect
        client.prepareResponseFrom(syncGroupResponse(singletonList(tp0), Errors.NONE), coordinator, true);

        // should try and find the new coordinator
        client.prepareResponseFrom(new FindCoordinatorResponse(Errors.NONE, node), node);

        // rejoin group
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, "memberId", "leaderId", Errors.NONE), coordinator);
        client.prepareResponseFrom(syncGroupResponse(singletonList(tp0), Errors.NONE), coordinator);

        client.prepareResponseFrom(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(final AbstractRequest body) {
                return body instanceof FetchRequest && ((FetchRequest) body).fetchData().containsKey(tp0);
            }
        }, fetchResponse(tp0, 1, 1), node);
        time.sleep(heartbeatIntervalMs);
        Thread.sleep(heartbeatIntervalMs);
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ZERO);
        assertFalse(records.isEmpty());
        consumer.close(Duration.ofMillis(0));
    }

    private void consumerCloseTest(final long closeTimeoutMs,
                                   List<? extends AbstractResponse> responses,
                                   long waitMs,
                                   boolean interrupt) throws Exception {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, false);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        Node coordinator = prepareRebalance(client, node, assignor, singletonList(tp0), null);

        client.prepareMetadataUpdate(TestUtils.metadataUpdateWith(1, Collections.singletonMap(topic, 1)));

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));

        // Poll with responses
        client.prepareResponseFrom(fetchResponse(tp0, 0, 1), node);
        client.prepareResponseFrom(fetchResponse(tp0, 1, 0), node);
        consumer.poll(Duration.ZERO);

        // Initiate close() after a commit request on another thread.
        // Kafka consumer is single-threaded, but the implementation allows calls on a
        // different thread as long as the calls are not executed concurrently. So this is safe.
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicReference<Exception> closeException = new AtomicReference<>();
        try {
            Future<?> future = executor.submit(new Runnable() {
                @Override
                public void run() {
                    consumer.commitAsync();
                    try {
                        consumer.close(Duration.ofMillis(closeTimeoutMs));
                    } catch (Exception e) {
                        closeException.set(e);
                    }
                }
            });

            // Close task should not complete until commit succeeds or close times out
            // if close timeout is not zero.
            try {
                future.get(100, TimeUnit.MILLISECONDS);
                if (closeTimeoutMs != 0)
                    fail("Close completed without waiting for commit or leave response");
            } catch (TimeoutException e) {
                // Expected exception
            }

            // Ensure close has started and queued at least one more request after commitAsync
            client.waitForRequests(2, 1000);

            // In graceful mode, commit response results in close() completing immediately without a timeout
            // In non-graceful mode, close() times out without an exception even though commit response is pending
            for (int i = 0; i < responses.size(); i++) {
                client.waitForRequests(1, 1000);
                client.respondFrom(responses.get(i), coordinator);
                if (i != responses.size() - 1) {
                    try {
                        future.get(100, TimeUnit.MILLISECONDS);
                        fail("Close completed without waiting for response");
                    } catch (TimeoutException e) {
                        // Expected exception
                    }
                }
            }

            if (waitMs > 0)
                time.sleep(waitMs);
            if (interrupt) {
                assertTrue("Close terminated prematurely", future.cancel(true));

                TestUtils.waitForCondition(new TestCondition() {
                    @Override
                    public boolean conditionMet() {
                        return closeException.get() != null;
                    }
                }, "InterruptException did not occur within timeout.");

                assertTrue("Expected exception not thrown " + closeException, closeException.get() instanceof InterruptException);
            } else {
                future.get(500, TimeUnit.MILLISECONDS); // Should succeed without TimeoutException or ExecutionException
                assertNull("Unexpected exception during close", closeException.get());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(expected = AuthenticationException.class)
    public void testPartitionsForAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthentication();
        consumer.partitionsFor("some other topic");
    }

    @Test(expected = AuthenticationException.class)
    public void testBeginningOffsetsAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthentication();
        consumer.beginningOffsets(Collections.singleton(tp0));
    }

    @Test(expected = AuthenticationException.class)
    public void testEndOffsetsAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthentication();
        consumer.endOffsets(Collections.singleton(tp0));
    }

    @Test(expected = AuthenticationException.class)
    public void testPollAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthentication();
        consumer.subscribe(singleton(topic));
        consumer.poll(Duration.ZERO);
    }

    @Test(expected = AuthenticationException.class)
    public void testOffsetsForTimesAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthentication();
        consumer.offsetsForTimes(singletonMap(tp0, 0L));
    }

    @Test(expected = AuthenticationException.class)
    public void testCommitSyncAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthentication();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp0, new OffsetAndMetadata(10L));
        consumer.commitSync(offsets);
    }

    @Test(expected = AuthenticationException.class)
    public void testCommittedAuthenticationFaiure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthentication();
        consumer.committed(tp0);
    }

    private KafkaConsumer<String, String> consumerWithPendingAuthentication() {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        PartitionAssignor assignor = new RangeAssignor();

        client.createPendingAuthenticationError(node, 0);
        return newConsumer(time, client, metadata, assignor, false);
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

    private Metadata createMetadata() {
        return new Metadata(0, Long.MAX_VALUE, true);
    }

    private Node prepareRebalance(MockClient client, Node node, final Set<String> subscribedTopics, PartitionAssignor assignor, List<TopicPartition> partitions, Node coordinator) {
        if (coordinator == null) {
            // lookup coordinator
            client.prepareResponseFrom(new FindCoordinatorResponse(Errors.NONE, node), node);
            coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        }

        // join group
        client.prepareResponseFrom(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                JoinGroupRequest joinGroupRequest = (JoinGroupRequest) body;
                PartitionAssignor.Subscription subscription = ConsumerProtocol.deserializeSubscription(joinGroupRequest.groupProtocols().get(0).metadata());
                return subscribedTopics.equals(new HashSet<>(subscription.topics()));
            }
        }, joinGroupFollowerResponse(assignor, 1, "memberId", "leaderId", Errors.NONE), coordinator);

        // sync group
        client.prepareResponseFrom(syncGroupResponse(partitions, Errors.NONE), coordinator);

        return coordinator;
    }

    private Node prepareRebalance(MockClient client, Node node, PartitionAssignor assignor, List<TopicPartition> partitions, Node coordinator) {
        if (coordinator == null) {
            // lookup coordinator
            client.prepareResponseFrom(new FindCoordinatorResponse(Errors.NONE, node), node);
            coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        }

        // join group
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, "memberId", "leaderId", Errors.NONE), coordinator);

        // sync group
        client.prepareResponseFrom(syncGroupResponse(partitions, Errors.NONE), coordinator);

        return coordinator;
    }

    private AtomicBoolean prepareHeartbeatResponse(MockClient client, Node coordinator) {
        final AtomicBoolean heartbeatReceived = new AtomicBoolean(false);
        client.prepareResponseFrom(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                heartbeatReceived.set(true);
                return true;
            }
        }, new HeartbeatResponse(Errors.NONE), coordinator);
        return heartbeatReceived;
    }

    private AtomicBoolean prepareOffsetCommitResponse(MockClient client, Node coordinator, final Map<TopicPartition, Long> partitionOffsets) {
        final AtomicBoolean commitReceived = new AtomicBoolean(true);
        Map<TopicPartition, Errors> response = new HashMap<>();
        for (TopicPartition partition : partitionOffsets.keySet())
            response.put(partition, Errors.NONE);

        client.prepareResponseFrom(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                OffsetCommitRequest commitRequest = (OffsetCommitRequest) body;
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

    private OffsetCommitResponse offsetCommitResponse(Map<TopicPartition, Errors> responseData) {
        return new OffsetCommitResponse(responseData);
    }

    private JoinGroupResponse joinGroupFollowerResponse(PartitionAssignor assignor, int generationId, String memberId, String leaderId, Errors error) {
        return new JoinGroupResponse(error, generationId, assignor.name(), memberId, leaderId,
                Collections.emptyMap());
    }

    private SyncGroupResponse syncGroupResponse(List<TopicPartition> partitions, Errors error) {
        ByteBuffer buf = ConsumerProtocol.serializeAssignment(new PartitionAssignor.Assignment(partitions));
        return new SyncGroupResponse(error, buf);
    }

    private OffsetFetchResponse offsetResponse(Map<TopicPartition, Long> offsets, Errors error) {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> partitionData = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            partitionData.put(entry.getKey(), new OffsetFetchResponse.PartitionData(entry.getValue(),
                    Optional.empty(), "", error));
        }
        return new OffsetFetchResponse(Errors.NONE, partitionData);
    }

    private ListOffsetResponse listOffsetsResponse(Map<TopicPartition, Long> offsets) {
        return listOffsetsResponse(offsets, Collections.emptyMap());
    }

    private ListOffsetResponse listOffsetsResponse(Map<TopicPartition, Long> partitionOffsets,
                                                   Map<TopicPartition, Errors> partitionErrors) {
        Map<TopicPartition, ListOffsetResponse.PartitionData> partitionData = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> partitionOffset : partitionOffsets.entrySet()) {
            partitionData.put(partitionOffset.getKey(), new ListOffsetResponse.PartitionData(Errors.NONE,
                    ListOffsetResponse.UNKNOWN_TIMESTAMP, partitionOffset.getValue(),
                    Optional.empty()));
        }

        for (Map.Entry<TopicPartition, Errors> partitionError : partitionErrors.entrySet()) {
            partitionData.put(partitionError.getKey(), new ListOffsetResponse.PartitionData(
                    partitionError.getValue(), ListOffsetResponse.UNKNOWN_TIMESTAMP,
                    ListOffsetResponse.UNKNOWN_OFFSET, Optional.empty()));
        }

        return new ListOffsetResponse(partitionData);
    }


    private FetchResponse<MemoryRecords> fetchResponse(Map<TopicPartition, FetchInfo> fetches) {
        LinkedHashMap<TopicPartition, FetchResponse.PartitionData<MemoryRecords>> tpResponses = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, FetchInfo> fetchEntry : fetches.entrySet()) {
            TopicPartition partition = fetchEntry.getKey();
            long fetchOffset = fetchEntry.getValue().offset;
            int fetchCount = fetchEntry.getValue().count;
            final MemoryRecords records;
            if (fetchCount == 0) {
                records = MemoryRecords.EMPTY;
            } else {
                MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), CompressionType.NONE,
                        TimestampType.CREATE_TIME, fetchOffset);
                for (int i = 0; i < fetchCount; i++)
                    builder.append(0L, ("key-" + i).getBytes(), ("value-" + i).getBytes());
                records = builder.build();
            }
            tpResponses.put(partition, new FetchResponse.PartitionData<>(
                    Errors.NONE, 0, FetchResponse.INVALID_LAST_STABLE_OFFSET,
                    0L, null, records));
        }
        return new FetchResponse<>(Errors.NONE, tpResponses, 0, INVALID_SESSION_ID);
    }

    private FetchResponse fetchResponse(TopicPartition partition, long fetchOffset, int count) {
        FetchInfo fetchInfo = new FetchInfo(fetchOffset, count);
        return fetchResponse(Collections.singletonMap(partition, fetchInfo));
    }

    private KafkaConsumer<String, String> newConsumer(Time time,
                                                      KafkaClient client,
                                                      Metadata metadata,
                                                      PartitionAssignor assignor,
                                                      boolean autoCommitEnabled) {
        return newConsumer(time, client, metadata, assignor, OffsetResetStrategy.EARLIEST, autoCommitEnabled, groupId);
    }

    private KafkaConsumer<String, String> newConsumerNoAutoCommit(Time time,
                                                                  KafkaClient client,
                                                                  Metadata metadata) {
        return newConsumer(time, client, metadata, new RangeAssignor(), OffsetResetStrategy.EARLIEST, false, groupId);
    }

    private KafkaConsumer<String, String> newConsumer(Time time,
                                                      KafkaClient client,
                                                      Metadata metadata,
                                                      String groupId) {
        return newConsumer(time, client, metadata, new RangeAssignor(), OffsetResetStrategy.LATEST, true, groupId);
    }

    private KafkaConsumer<String, String> newConsumer(Time time,
                                                      KafkaClient client,
                                                      Metadata metadata,
                                                      PartitionAssignor assignor,
                                                      OffsetResetStrategy resetStrategy,
                                                      boolean autoCommitEnabled,
                                                      String groupId) {
        String clientId = "mock-consumer";
        String metricGroupPrefix = "consumer";
        long retryBackoffMs = 100;
        int requestTimeoutMs = 30000;
        int defaultApiTimeoutMs = 30000;
        boolean excludeInternalTopics = true;
        int minBytes = 1;
        int maxBytes = Integer.MAX_VALUE;
        int maxWaitMs = 500;
        int fetchSize = 1024 * 1024;
        int maxPollRecords = Integer.MAX_VALUE;
        boolean checkCrcs = true;
        int rebalanceTimeoutMs = 60000;

        Deserializer<String> keyDeserializer = new StringDeserializer();
        Deserializer<String> valueDeserializer = new StringDeserializer();

        List<PartitionAssignor> assignors = singletonList(assignor);
        ConsumerInterceptors<String, String> interceptors = new ConsumerInterceptors<>(Collections.emptyList());

        Metrics metrics = new Metrics();
        ConsumerMetrics metricsRegistry = new ConsumerMetrics(metricGroupPrefix);

        SubscriptionState subscriptions = new SubscriptionState(resetStrategy);
        LogContext loggerFactory = new LogContext();
        ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(loggerFactory, client, metadata, time,
                retryBackoffMs, requestTimeoutMs, heartbeatIntervalMs);

        Heartbeat heartbeat = new Heartbeat(time, sessionTimeoutMs, heartbeatIntervalMs, rebalanceTimeoutMs, retryBackoffMs);
        ConsumerCoordinator consumerCoordinator = new ConsumerCoordinator(
                loggerFactory,
                consumerClient,
                groupId,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                heartbeat,
                assignors,
                metadata,
                subscriptions,
                metrics,
                metricGroupPrefix,
                time,
                retryBackoffMs,
                autoCommitEnabled,
                autoCommitIntervalMs,
                interceptors,
                excludeInternalTopics,
                true);

        Fetcher<String, String> fetcher = new Fetcher<>(
                loggerFactory,
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
                metricsRegistry.fetcherMetrics,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                IsolationLevel.READ_UNCOMMITTED);

        return new KafkaConsumer<>(
                loggerFactory,
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
                requestTimeoutMs,
                defaultApiTimeoutMs,
                assignors,
                groupId);
    }

    private static class FetchInfo {
        long offset;
        int count;

        FetchInfo(long offset, int count) {
            this.offset = offset;
            this.count = count;
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testCloseWithTimeUnit() {
        KafkaConsumer consumer = mock(KafkaConsumer.class);
        doCallRealMethod().when(consumer).close(anyLong(), any());
        consumer.close(1, TimeUnit.SECONDS);
        verify(consumer).close(Duration.ofSeconds(1));
    }

    @Test(expected = InvalidTopicException.class)
    public void testSubscriptionOnInvalidTopic() {
        Time time = new MockTime();
        Metadata metadata = createMetadata();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Cluster cluster = metadata.fetch();

        PartitionAssignor assignor = new RoundRobinAssignor();

        String invalidTopicName = "topic abc";  // Invalid topic name due to space

        List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
        topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.INVALID_TOPIC_EXCEPTION,
                invalidTopicName, false, Collections.emptyList()));
        MetadataResponse updateResponse = new MetadataResponse(cluster.nodes(),
                cluster.clusterResource().clusterId(),
                cluster.controller().id(),
                topicMetadata);
        client.prepareMetadataUpdate(updateResponse);

        KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor, true);
        consumer.subscribe(singleton(invalidTopicName), getConsumerRebalanceListener(consumer));

        consumer.poll(Duration.ZERO);
    }
}
