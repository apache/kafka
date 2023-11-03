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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.clients.consumer.internals.Deserializers;
import org.apache.kafka.clients.consumer.internals.FetchConfig;
import org.apache.kafka.clients.consumer.internals.FetchMetricsManager;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.MockRebalanceListener;
import org.apache.kafka.clients.consumer.internals.OffsetFetcher;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.TopicMetadataFetcher;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
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
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
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
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.consumer.KafkaConsumer.DEFAULT_REASON;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Note to future authors in this class. If you close the consumer, close with DURATION.ZERO to reduce the duration of
 * the test.
 */
public class KafkaConsumerTest {

    private final String topic = "test";
    private final Uuid topicId = Uuid.randomUuid();
    private final TopicPartition tp0 = new TopicPartition(topic, 0);
    private final TopicPartition tp1 = new TopicPartition(topic, 1);

    private final String topic2 = "test2";
    private final Uuid topicId2 = Uuid.randomUuid();
    private final TopicPartition t2p0 = new TopicPartition(topic2, 0);

    private final String topic3 = "test3";
    private final Uuid topicId3 = Uuid.randomUuid();
    private final TopicPartition t3p0 = new TopicPartition(topic3, 0);

    private final int sessionTimeoutMs = 10000;
    private final int defaultApiTimeoutMs = 60000;
    private final int requestTimeoutMs = defaultApiTimeoutMs / 2;
    private final int heartbeatIntervalMs = 1000;

    // Set auto commit interval lower than heartbeat so we don't need to deal with
    // a concurrent heartbeat request
    private final int autoCommitIntervalMs = 500;
    private final int throttleMs = 10;

    private final String groupId = "mock-group";
    private final String memberId = "memberId";
    private final String leaderId = "leaderId";
    private final Optional<String> groupInstanceId = Optional.of("mock-instance");
    private Map<String, Uuid> topicIds = Stream.of(
            new AbstractMap.SimpleEntry<>(topic, topicId),
            new AbstractMap.SimpleEntry<>(topic2, topicId2),
            new AbstractMap.SimpleEntry<>(topic3, topicId3))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    private Map<Uuid, String> topicNames = Stream.of(
            new AbstractMap.SimpleEntry<>(topicId, topic),
            new AbstractMap.SimpleEntry<>(topicId2, topic2),
            new AbstractMap.SimpleEntry<>(topicId3, topic3))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    private final String partitionRevoked = "Hit partition revoke ";
    private final String partitionAssigned = "Hit partition assign ";
    private final String partitionLost = "Hit partition lost ";

    private final Collection<TopicPartition> singleTopicPartition = Collections.singleton(new TopicPartition(topic, 0));
    private final Time time = new MockTime();
    private final SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
    private final ConsumerPartitionAssignor assignor = new RoundRobinAssignor();

    private KafkaConsumer<?, ?> consumer;

    @AfterEach
    public void cleanup() {
        if (consumer != null) {
            consumer.close(Duration.ZERO);
        }
    }

    @Test
    public void testMetricsReporterAutoGeneratedClientId() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());

        MockMetricsReporter mockMetricsReporter = (MockMetricsReporter) consumer.metrics.reporters().get(0);

        assertEquals(consumer.getClientId(), mockMetricsReporter.clientId);
        assertEquals(2, consumer.metrics.reporters().size());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDisableJmxReporter() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.AUTO_INCLUDE_JMX_REPORTER_CONFIG, "false");
        consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        assertTrue(consumer.metrics.reporters().isEmpty());
    }

    @Test
    public void testExplicitlyEnableJmxReporter() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "org.apache.kafka.common.metrics.JmxReporter");
        consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        assertEquals(1, consumer.metrics.reporters().size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPollReturnsRecords() {
        consumer = setUpConsumerWithRecordsToPoll(tp0, 5);

        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ZERO);

        assertEquals(records.count(), 5);
        assertEquals(records.partitions(), Collections.singleton(tp0));
        assertEquals(records.records(tp0).size(), 5);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSecondPollWithDeserializationErrorThrowsRecordDeserializationException() {
        int invalidRecordNumber = 4;
        int invalidRecordOffset = 3;
        StringDeserializer deserializer = mockErrorDeserializer(invalidRecordNumber);

        consumer = setUpConsumerWithRecordsToPoll(tp0, 5, deserializer);
        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ZERO);

        assertEquals(invalidRecordNumber - 1, records.count());
        assertEquals(Collections.singleton(tp0), records.partitions());
        assertEquals(invalidRecordNumber - 1, records.records(tp0).size());
        long lastOffset = records.records(tp0).get(records.records(tp0).size() - 1).offset();
        assertEquals(invalidRecordNumber - 2, lastOffset);

        RecordDeserializationException rde = assertThrows(RecordDeserializationException.class, () -> consumer.poll(Duration.ZERO));
        assertEquals(invalidRecordOffset, rde.offset());
        assertEquals(tp0, rde.topicPartition());
        assertEquals(rde.offset(), consumer.position(tp0));
    }

    /*
        Create a mock deserializer which throws a SerializationException on the Nth record's value deserialization
     */
    private StringDeserializer mockErrorDeserializer(int recordNumber) {
        int recordIndex = recordNumber - 1;
        return new StringDeserializer() {
            int i = 0;
            @Override
            public String deserialize(String topic, byte[] data) {
                if (i == recordIndex) {
                    throw new SerializationException();
                } else {
                    i++;
                    return super.deserialize(topic, data);
                }
            }

            @Override
            public String deserialize(String topic, Headers headers, ByteBuffer data) {
                if (i == recordIndex) {
                    throw new SerializationException();
                } else {
                    i++;
                    return super.deserialize(topic, headers, data);
                }
            }
        };
    }

    private KafkaConsumer<?, ?> setUpConsumerWithRecordsToPoll(TopicPartition tp, int recordCount) {
        return setUpConsumerWithRecordsToPoll(tp, recordCount, new StringDeserializer());
    }

    private KafkaConsumer<?, ?> setUpConsumerWithRecordsToPoll(TopicPartition tp, int recordCount, Deserializer<String> deserializer) {
        Cluster cluster = TestUtils.singletonCluster(tp.topic(), 1);
        Node node = cluster.nodes().get(0);

        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 1));
        consumer = newConsumer(time, client, subscription, metadata, assignor,
                true, groupId, groupInstanceId, Optional.of(deserializer), false);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        prepareRebalance(client, node, assignor, singletonList(tp), null);
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        client.prepareResponseFrom(fetchResponse(tp, 0, recordCount), node);
        return consumer;
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
            fail("should have caught an exception and returned");
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
        consumer = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test
    public void testInvalidSocketSendBufferSize() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, -2);
        assertThrows(KafkaException.class,
            () -> new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()));
    }

    @Test
    public void testInvalidSocketReceiveBufferSize() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, -2);
        assertThrows(KafkaException.class,
            () -> new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()));
    }

    @Test
    public void shouldIgnoreGroupInstanceIdForEmptyGroupId() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "instance_id");
        consumer = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test
    public void testSubscription() {
        consumer = newConsumer(groupId);

        consumer.subscribe(singletonList(topic));
        assertEquals(singleton(topic), consumer.subscription());
        assertTrue(consumer.assignment().isEmpty());

        consumer.subscribe(Collections.emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());

        consumer.assign(singletonList(tp0));
        assertTrue(consumer.subscription().isEmpty());
        assertEquals(singleton(tp0), consumer.assignment());

        consumer.unsubscribe();
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
    }

    @Test
    public void testSubscriptionOnNullTopicCollection() {
        consumer = newConsumer(groupId);
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe((List<String>) null));
    }

    @Test
    public void testSubscriptionOnNullTopic() {
        consumer = newConsumer(groupId);
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(singletonList(null)));
    }

    @Test
    public void testSubscriptionOnEmptyTopic() {
        consumer = newConsumer(groupId);
        String emptyTopic = "  ";
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(singletonList(emptyTopic)));
    }

    @Test
    public void testSubscriptionOnNullPattern() {
        consumer = newConsumer(groupId);
        assertThrows(IllegalArgumentException.class,
            () -> consumer.subscribe((Pattern) null));
    }

    @Test
    public void testSubscriptionOnEmptyPattern() {
        consumer = newConsumer(groupId);
        assertThrows(IllegalArgumentException.class,
            () -> consumer.subscribe(Pattern.compile("")));
    }

    @Test
    public void testSubscriptionWithEmptyPartitionAssignment() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumer = newConsumer(props);
        assertThrows(IllegalStateException.class,
            () -> consumer.subscribe(singletonList(topic)));
    }

    @Test
    public void testSeekNegative() {
        consumer = newConsumer((String) null);
        consumer.assign(singleton(new TopicPartition("nonExistTopic", 0)));
        assertThrows(IllegalArgumentException.class,
            () -> consumer.seek(new TopicPartition("nonExistTopic", 0), -1));
    }

    @Test
    public void testAssignOnNullTopicPartition() {
        consumer = newConsumer((String) null);
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(null));
    }

    @Test
    public void testAssignOnEmptyTopicPartition() {
        consumer = newConsumer(groupId);
        consumer.assign(Collections.emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
    }

    @Test
    public void testAssignOnNullTopicInPartition() {
        consumer = newConsumer((String) null);
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(singleton(new TopicPartition(null, 0))));
    }

    @Test
    public void testAssignOnEmptyTopicInPartition() {
        consumer = newConsumer((String) null);
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(singleton(new TopicPartition("  ", 0))));
    }

    @Test
    public void testInterceptorConstructorClose() {
        try {
            Properties props = new Properties();
            // test with client ID assigned by KafkaConsumer
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            props.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName());

            consumer = new KafkaConsumer<>(
                    props, new StringDeserializer(), new StringDeserializer());
            assertEquals(1, MockConsumerInterceptor.INIT_COUNT.get());
            assertEquals(0, MockConsumerInterceptor.CLOSE_COUNT.get());

            consumer.close(Duration.ZERO);
            assertEquals(1, MockConsumerInterceptor.INIT_COUNT.get());
            assertEquals(1, MockConsumerInterceptor.CLOSE_COUNT.get());
            // Cluster metadata will only be updated on calling poll.
            assertNull(MockConsumerInterceptor.CLUSTER_META.get());

        } finally {
            // cleanup since we are using mutable static variables in MockConsumerInterceptor
            MockConsumerInterceptor.resetCounters();
        }
    }

    @Test
    public void testInterceptorConstructorConfigurationWithExceptionShouldCloseRemainingInstances() {
        final int targetInterceptor = 3;

        try {
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            props.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,  MockConsumerInterceptor.class.getName() + ", "
                    + MockConsumerInterceptor.class.getName() + ", "
                    + MockConsumerInterceptor.class.getName());

            MockConsumerInterceptor.setThrowOnConfigExceptionThreshold(targetInterceptor);

            assertThrows(KafkaException.class, () -> {
                new KafkaConsumer<>(
                        props, new StringDeserializer(), new StringDeserializer());
            });

            assertEquals(3, MockConsumerInterceptor.CONFIG_COUNT.get());
            assertEquals(3, MockConsumerInterceptor.CLOSE_COUNT.get());

        } finally {
            MockConsumerInterceptor.resetCounters();
        }
    }

    @Test
    public void testPause() {
        consumer = newConsumer(groupId);

        consumer.assign(singletonList(tp0));
        assertEquals(singleton(tp0), consumer.assignment());
        assertTrue(consumer.paused().isEmpty());

        consumer.pause(singleton(tp0));
        assertEquals(singleton(tp0), consumer.paused());

        consumer.resume(singleton(tp0));
        assertTrue(consumer.paused().isEmpty());

        consumer.unsubscribe();
        assertTrue(consumer.paused().isEmpty());
    }

    @Test
    public void testConsumerJmxPrefix() throws  Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put("client.id", "client-1");
        consumer = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        MetricName testMetricName = consumer.metrics.metricName("test-metric",
                "grp1", "test metric");
        consumer.metrics.addMetric(testMetricName, new Avg());
        assertNotNull(server.getObjectInstance(new ObjectName("kafka.consumer:type=grp1,client-id=client-1")));
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
        enableAutoCommit.ifPresent(
            autoCommit -> props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit.toString()));
        return newConsumer(props);
    }

    private KafkaConsumer<byte[], byte[]> newConsumer(Properties props) {
        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test
    public void verifyHeartbeatSent() throws Exception {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);

        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        Node coordinator = prepareRebalance(client, node, assignor, singletonList(tp0), null);

        // initial fetch
        client.prepareResponseFrom(fetchResponse(tp0, 0, 0), node);
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(singleton(tp0), consumer.assignment());

        AtomicBoolean heartbeatReceived = prepareHeartbeatResponse(client, coordinator, Errors.NONE);

        // heartbeat interval is 2 seconds
        time.sleep(heartbeatIntervalMs);
        Thread.sleep(heartbeatIntervalMs);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));

        assertTrue(heartbeatReceived.get());
    }

    @Test
    public void verifyHeartbeatSentWhenFetchedDataReady() throws Exception {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        Node coordinator = prepareRebalance(client, node, assignor, singletonList(tp0), null);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(fetchResponse(tp0, 0, 5), node);
        client.poll(0, time.milliseconds());

        client.prepareResponseFrom(fetchResponse(tp0, 5, 0), node);
        AtomicBoolean heartbeatReceived = prepareHeartbeatResponse(client, coordinator, Errors.NONE);

        time.sleep(heartbeatIntervalMs);
        Thread.sleep(heartbeatIntervalMs);

        consumer.poll(Duration.ZERO);

        assertTrue(heartbeatReceived.get());
    }

    @Test
    public void verifyPollTimesOutDuringMetadataUpdate() {
        final ConsumerMetadata metadata = createMetadata(subscription);
        final MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        // Since we would enable the heartbeat thread after received join-response which could
        // send the sync-group on behalf of the consumer if it is enqueued, we may still complete
        // the rebalance and send out the fetch; in order to avoid it we do not prepare sync response here.
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, memberId, leaderId, Errors.NONE), coordinator);

        consumer.poll(Duration.ZERO);

        final Queue<ClientRequest> requests = client.requests();
        assertEquals(0, requests.stream().filter(request -> request.apiKey().equals(ApiKeys.FETCH)).count());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void verifyDeprecatedPollDoesNotTimeOutDuringMetadataUpdate() {
        final ConsumerMetadata metadata = createMetadata(subscription);
        final MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        consumer.poll(0L);

        // The underlying client SHOULD get a fetch request
        final Queue<ClientRequest> requests = client.requests();
        assertEquals(1, requests.size());
        final Class<? extends AbstractRequest.Builder> aClass = requests.peek().requestBuilder().getClass();
        assertEquals(FetchRequest.Builder.class, aClass);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void verifyNoCoordinatorLookupForManualAssignmentWithSeek() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, null, groupInstanceId, false);
        consumer.assign(singleton(tp0));
        consumer.seekToBeginning(singleton(tp0));

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(listOffsetsResponse(Collections.singletonMap(tp0, 50L)));
        client.prepareResponse(fetchResponse(tp0, 50L, 5));

        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ofMillis(1));
        assertEquals(5, records.count());
        assertEquals(55L, consumer.position(tp0));
    }

    @Test
    public void verifyNoCoordinatorLookupForManualAssignmentWithOffsetCommit() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        // create a consumer with groupID with manual assignment
        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        consumer.assign(singleton(tp0));

        // 1st coordinator error should cause coordinator unknown
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.COORDINATOR_NOT_AVAILABLE, groupId, node), node);
        consumer.poll(Duration.ofMillis(0));

        // 2nd coordinator error should find the correct coordinator and clear the findCoordinatorFuture
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);

        client.prepareResponse(offsetResponse(Collections.singletonMap(tp0, 50L), Errors.NONE));
        client.prepareResponse(fetchResponse(tp0, 50L, 5));

        @SuppressWarnings("unchecked")
        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ofMillis(0));
        assertEquals(5, records.count());
        assertEquals(55L, consumer.position(tp0));

        // after coordinator found, consumer should be able to commit the offset successfully
        client.prepareResponse(offsetCommitResponse(Collections.singletonMap(tp0, Errors.NONE)));
        consumer.commitSync(Collections.singletonMap(tp0, new OffsetAndMetadata(55L)));

        // verify the offset is committed
        client.prepareResponse(offsetResponse(Collections.singletonMap(tp0, 55L), Errors.NONE));
        assertEquals(55, consumer.committed(Collections.singleton(tp0), Duration.ZERO).get(tp0).offset());
    }

    @Test
    public void testFetchProgressWithMissingPartitionPosition() {
        // Verifies that we can make progress on one partition while we are awaiting
        // a reset on another partition.

        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 2));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumerNoAutoCommit(time, client, subscription, metadata);
        consumer.assign(Arrays.asList(tp0, tp1));
        consumer.seekToEnd(singleton(tp0));
        consumer.seekToBeginning(singleton(tp1));

        client.prepareResponse(body -> {
            ListOffsetsRequest request = (ListOffsetsRequest) body;
            List<ListOffsetsPartition> partitions = request.topics().stream().flatMap(t -> {
                if (t.name().equals(topic))
                    return Stream.of(t.partitions());
                else
                    return Stream.empty();
            }).flatMap(List::stream).collect(Collectors.toList());
            ListOffsetsPartition expectedTp0 = new ListOffsetsPartition()
                    .setPartitionIndex(tp0.partition())
                    .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP);
            ListOffsetsPartition expectedTp1 = new ListOffsetsPartition()
                    .setPartitionIndex(tp1.partition())
                    .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP);
            return partitions.contains(expectedTp0) && partitions.contains(expectedTp1);
        }, listOffsetsResponse(Collections.singletonMap(tp0, 50L), Collections.singletonMap(tp1, Errors.NOT_LEADER_OR_FOLLOWER)));
        client.prepareResponse(
            body -> {
                FetchRequest request = (FetchRequest) body;
                Map<TopicIdPartition, FetchRequest.PartitionData> fetchData = request.fetchData(topicNames);
                TopicIdPartition tidp0 = new TopicIdPartition(topicIds.get(tp0.topic()), tp0);
                return fetchData.keySet().equals(singleton(tidp0)) &&
                        fetchData.get(tidp0).fetchOffset == 50L;

            }, fetchResponse(tp0, 50L, 5));

        @SuppressWarnings("unchecked")
        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ofMillis(1));
        assertEquals(5, records.count());
        assertEquals(singleton(tp0), records.partitions());
    }

    private void initMetadata(MockClient mockClient, Map<String, Integer> partitionCounts) {
        Map<String, Uuid> metadataIds = new HashMap<>();
        for (String name : partitionCounts.keySet()) {
            metadataIds.put(name, topicIds.get(name));
        }
        MetadataResponse initialMetadata = RequestTestUtils.metadataUpdateWithIds(1, partitionCounts, metadataIds);

        mockClient.updateMetadata(initialMetadata);
    }

    @Test
    public void testMissingOffsetNoResetPolicy() {
        SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor,
                true, groupId, groupInstanceId, false);
        consumer.assign(singletonList(tp0));

        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // lookup committed offset and find nothing
        client.prepareResponseFrom(offsetResponse(Collections.singletonMap(tp0, -1L), Errors.NONE), coordinator);
        assertThrows(NoOffsetForPartitionException.class, () -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testResetToCommittedOffset() {
        SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata, assignor,
                true, groupId, groupInstanceId, false);
        consumer.assign(singletonList(tp0));

        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        client.prepareResponseFrom(offsetResponse(Collections.singletonMap(tp0, 539L), Errors.NONE), coordinator);
        consumer.poll(Duration.ZERO);

        assertEquals(539L, consumer.position(tp0));
    }

    @Test
    public void testResetUsingAutoResetPolicy() {
        SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.LATEST);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor,
                true, groupId, groupInstanceId, false);
        consumer.assign(singletonList(tp0));

        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        client.prepareResponseFrom(offsetResponse(Collections.singletonMap(tp0, -1L), Errors.NONE), coordinator);
        client.prepareResponse(listOffsetsResponse(Collections.singletonMap(tp0, 50L)));

        consumer.poll(Duration.ZERO);

        assertEquals(50L, consumer.position(tp0));
    }

    @Test
    public void testOffsetIsValidAfterSeek() {
        SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.LATEST);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));

        consumer = newConsumer(time, client, subscription, metadata, assignor,
                true, groupId, Optional.empty(), false);
        consumer.assign(singletonList(tp0));
        consumer.seek(tp0, 20L);
        consumer.poll(Duration.ZERO);
        assertEquals(subscription.validPosition(tp0).offset, 20L);
    }

    @Test
    public void testCommitsFetchedDuringAssign() {
        long offset1 = 10000;
        long offset2 = 20000;

        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 2));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        consumer.assign(singletonList(tp0));

        // lookup coordinator
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // fetch offset for one topic
        client.prepareResponseFrom(offsetResponse(Collections.singletonMap(tp0, offset1), Errors.NONE), coordinator);
        assertEquals(offset1, consumer.committed(Collections.singleton(tp0)).get(tp0).offset());

        consumer.assign(Arrays.asList(tp0, tp1));

        // fetch offset for two topics
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(tp0, offset1);
        client.prepareResponseFrom(offsetResponse(offsets, Errors.NONE), coordinator);
        assertEquals(offset1, consumer.committed(Collections.singleton(tp0)).get(tp0).offset());

        offsets.remove(tp0);
        offsets.put(tp1, offset2);
        client.prepareResponseFrom(offsetResponse(offsets, Errors.NONE), coordinator);
        assertEquals(offset2, consumer.committed(Collections.singleton(tp1)).get(tp1).offset());
    }

    @Test
    public void testFetchStableOffsetThrowInCommitted() {
        assertThrows(UnsupportedVersionException.class, () -> setupThrowableConsumer().committed(Collections.singleton(tp0)));
    }

    @Test
    public void testFetchStableOffsetThrowInPoll() {
        assertThrows(UnsupportedVersionException.class, () -> setupThrowableConsumer().poll(Duration.ZERO));
    }

    @Test
    public void testFetchStableOffsetThrowInPosition() {
        assertThrows(UnsupportedVersionException.class, () -> setupThrowableConsumer().position(tp0));
    }

    private KafkaConsumer<?, ?> setupThrowableConsumer() {
        long offset1 = 10000;

        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 2));
        client.setNodeApiVersions(NodeApiVersions.create(ApiKeys.OFFSET_FETCH.id, (short) 0, (short) 6));

        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(
            time, client, subscription, metadata, assignor, true, groupId, groupInstanceId, true);
        consumer.assign(singletonList(tp0));

        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        client.prepareResponseFrom(offsetResponse(
            Collections.singletonMap(tp0, offset1), Errors.NONE), coordinator);
        return consumer;
    }

    @Test
    public void testNoCommittedOffsets() {
        long offset1 = 10000;

        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 2));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        consumer.assign(Arrays.asList(tp0, tp1));

        // lookup coordinator
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // fetch offset for one topic
        client.prepareResponseFrom(offsetResponse(Utils.mkMap(Utils.mkEntry(tp0, offset1), Utils.mkEntry(tp1, -1L)), Errors.NONE), coordinator);
        final Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Utils.mkSet(tp0, tp1));
        assertEquals(2, committed.size());
        assertEquals(offset1, committed.get(tp0).offset());
        assertNull(committed.get(tp1));
    }

    @Test
    public void testAutoCommitSentBeforePositionUpdate() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
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
    }

    @Test
    public void testRegexSubscription() {
        String unmatchedTopic = "unmatched";
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(topic, 1);
        partitionCounts.put(unmatchedTopic, 1);
        topicIds.put(unmatchedTopic, Uuid.randomUuid());
        initMetadata(client, partitionCounts);
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        prepareRebalance(client, node, singleton(topic), assignor, singletonList(tp0), null);

        consumer.subscribe(Pattern.compile(topic), getConsumerRebalanceListener(consumer));

        client.prepareMetadataUpdate(RequestTestUtils.metadataUpdateWithIds(1, partitionCounts, topicIds));

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));

        assertEquals(singleton(topic), consumer.subscription());
        assertEquals(singleton(tp0), consumer.assignment());
    }

    @Test
    public void testChangingRegexSubscription() {
        String otherTopic = "other";
        TopicPartition otherTopicPartition = new TopicPartition(otherTopic, 0);

        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(topic, 1);
        partitionCounts.put(otherTopic, 1);
        topicIds.put(otherTopic, Uuid.randomUuid());
        initMetadata(client, partitionCounts);
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, false, groupInstanceId);

        Node coordinator = prepareRebalance(client, node, singleton(topic), assignor, singletonList(tp0), null);
        consumer.subscribe(Pattern.compile(topic), getConsumerRebalanceListener(consumer));

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        assertEquals(singleton(topic), consumer.subscription());

        consumer.subscribe(Pattern.compile(otherTopic), getConsumerRebalanceListener(consumer));

        client.prepareMetadataUpdate(RequestTestUtils.metadataUpdateWithIds(1, partitionCounts, topicIds));
        prepareRebalance(client, node, singleton(otherTopic), assignor, singletonList(otherTopicPartition), coordinator);
        consumer.poll(Duration.ZERO);

        assertEquals(singleton(otherTopic), consumer.subscription());
    }

    @Test
    public void testWakeupWithFetchDataAvailable() throws Exception {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(fetchResponse(tp0, 0, 5), node);
        client.poll(0, time.milliseconds());

        consumer.wakeup();

        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ZERO));

        // make sure the position hasn't been updated
        assertEquals(0, consumer.position(tp0));

        // the next poll should return the completed fetch
        @SuppressWarnings("unchecked")
        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ZERO);
        assertEquals(5, records.count());
        // Increment time asynchronously to clear timeouts in closing the consumer
        final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(() -> time.sleep(sessionTimeoutMs), 0L, 10L, TimeUnit.MILLISECONDS);
        consumer.close();
        exec.shutdownNow();
        exec.awaitTermination(5L, TimeUnit.SECONDS);
    }

    @Test
    public void testPollThrowsInterruptExceptionIfInterrupted() {
        final ConsumerMetadata metadata = createMetadata(subscription);
        final MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, false, groupInstanceId);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        // interrupt the thread and call poll
        try {
            Thread.currentThread().interrupt();
            assertThrows(InterruptException.class, () -> consumer.poll(Duration.ZERO));
        } finally {
            // clear interrupted state again since this thread may be reused by JUnit
            Thread.interrupted();
        }
    }

    @Test
    public void fetchResponseWithUnexpectedPartitionIsIgnored() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        consumer.subscribe(singletonList(topic), getConsumerRebalanceListener(consumer));

        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        Map<TopicPartition, FetchInfo> fetches1 = new HashMap<>();
        fetches1.put(tp0, new FetchInfo(0, 1));
        fetches1.put(t2p0, new FetchInfo(0, 10)); // not assigned and not fetched
        client.prepareResponseFrom(fetchResponse(fetches1), node);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));

        @SuppressWarnings("unchecked")
        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ZERO);
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
    @SuppressWarnings("unchecked")
    public void testSubscriptionChangesWithAutoCommitEnabled() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        tpCounts.put(topic3, 1);
        initMetadata(client, tpCounts);
        Node node = metadata.fetch().nodes().get(0);

        ConsumerPartitionAssignor assignor = new RangeAssignor();

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);

        // initial subscription
        consumer.subscribe(Arrays.asList(topic, topic2), getConsumerRebalanceListener(consumer));

        // verify that subscription has changed but assignment is still unchanged
        assertEquals(2, consumer.subscription().size());
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

        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ofMillis(1));

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
        assertEquals(2, consumer.subscription().size());
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic3));
        assertEquals(2, consumer.assignment().size());
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

        records = (ConsumerRecords<String, String>) consumer.poll(Duration.ofMillis(1));

        // verify that the fetch occurred as expected
        assertEquals(101, records.count());
        assertEquals(2L, consumer.position(tp0));
        assertEquals(100L, consumer.position(t3p0));

        // verify that the offset commits occurred as expected
        assertTrue(commitReceived.get());

        // verify that subscription is still the same, and now assignment has caught up
        assertEquals(2, consumer.subscription().size());
        assertTrue(consumer.subscription().contains(topic) && consumer.subscription().contains(topic3));
        assertEquals(2, consumer.assignment().size());
        assertTrue(consumer.assignment().contains(tp0) && consumer.assignment().contains(t3p0));

        consumer.unsubscribe();

        // verify that subscription and assignment are both cleared
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());

        client.requests().clear();
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
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        initMetadata(client, tpCounts);
        Node node = metadata.fetch().nodes().get(0);

        ConsumerPartitionAssignor assignor = new RangeAssignor();

        consumer = newConsumer(time, client, subscription, metadata, assignor, false, groupInstanceId);

        initializeSubscriptionWithSingleTopic(consumer, getConsumerRebalanceListener(consumer));

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
    }

    @Test
    public void testUnsubscribeShouldTriggerPartitionsRevokedWithValidGeneration() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        CooperativeStickyAssignor assignor = new CooperativeStickyAssignor();
        consumer = newConsumer(time, client, subscription, metadata, assignor, false, groupInstanceId);

        initializeSubscriptionWithSingleTopic(consumer, getExceptionConsumerRebalanceListener());

        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        RuntimeException assignmentException = assertThrows(RuntimeException.class,
            () -> consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE)));
        assertEquals(partitionAssigned + singleTopicPartition, assignmentException.getCause().getMessage());

        RuntimeException unsubscribeException = assertThrows(RuntimeException.class, consumer::unsubscribe);
        assertEquals(partitionRevoked + singleTopicPartition, unsubscribeException.getCause().getMessage());
    }

    @Test
    public void testUnsubscribeShouldTriggerPartitionsLostWithNoGeneration() throws Exception {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        CooperativeStickyAssignor assignor = new CooperativeStickyAssignor();
        consumer = newConsumer(time, client, subscription, metadata, assignor, false, groupInstanceId);

        initializeSubscriptionWithSingleTopic(consumer, getExceptionConsumerRebalanceListener());
        Node coordinator = prepareRebalance(client, node, assignor, singletonList(tp0), null);

        RuntimeException assignException = assertThrows(RuntimeException.class,
            () -> consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE)));
        assertEquals(partitionAssigned + singleTopicPartition, assignException.getCause().getMessage());

        AtomicBoolean heartbeatReceived = prepareHeartbeatResponse(client, coordinator, Errors.UNKNOWN_MEMBER_ID);

        time.sleep(heartbeatIntervalMs);
        TestUtils.waitForCondition(heartbeatReceived::get, "Heartbeat response did not occur within timeout.");

        RuntimeException unsubscribeException = assertThrows(RuntimeException.class, consumer::unsubscribe);
        assertEquals(partitionLost + singleTopicPartition, unsubscribeException.getCause().getMessage());
    }

    private void initializeSubscriptionWithSingleTopic(KafkaConsumer<?, ?> consumer,
                                                       ConsumerRebalanceListener consumerRebalanceListener) {
        consumer.subscribe(singleton(topic), consumerRebalanceListener);
        // verify that subscription has changed but assignment is still unchanged
        assertEquals(singleton(topic), consumer.subscription());
        assertEquals(Collections.emptySet(), consumer.assignment());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testManualAssignmentChangeWithAutoCommitEnabled() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        initMetadata(client, tpCounts);
        Node node = metadata.fetch().nodes().get(0);

        ConsumerPartitionAssignor assignor = new RangeAssignor();

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);

        // lookup coordinator
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // manual assignment
        consumer.assign(singleton(tp0));
        consumer.seekToBeginning(singleton(tp0));

        // fetch offset for one topic
        client.prepareResponseFrom(offsetResponse(Collections.singletonMap(tp0, 0L), Errors.NONE), coordinator);
        assertEquals(0, consumer.committed(Collections.singleton(tp0)).get(tp0).offset());

        // verify that assignment immediately changes
        assertEquals(consumer.assignment(), singleton(tp0));

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(listOffsetsResponse(Collections.singletonMap(tp0, 10L)));
        client.prepareResponse(fetchResponse(tp0, 10L, 1));

        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ofMillis(100));

        assertEquals(1, records.count());
        assertEquals(11L, consumer.position(tp0));

        // mock the offset commit response for to be revoked partitions
        AtomicBoolean commitReceived = prepareOffsetCommitResponse(client, coordinator, tp0, 11);

        // new manual assignment
        consumer.assign(singleton(t2p0));

        // verify that assignment immediately changes
        assertEquals(consumer.assignment(), singleton(t2p0));
        // verify that the offset commits occurred as expected
        assertTrue(commitReceived.get());

        client.requests().clear();
    }

    @Test
    public void testManualAssignmentChangeWithAutoCommitDisabled() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        Map<String, Integer> tpCounts = new HashMap<>();
        tpCounts.put(topic, 1);
        tpCounts.put(topic2, 1);
        initMetadata(client, tpCounts);
        Node node = metadata.fetch().nodes().get(0);

        ConsumerPartitionAssignor assignor = new RangeAssignor();

        consumer = newConsumer(time, client, subscription, metadata, assignor, false, groupInstanceId);

        // lookup coordinator
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // manual assignment
        consumer.assign(singleton(tp0));
        consumer.seekToBeginning(singleton(tp0));

        // fetch offset for one topic
        client.prepareResponseFrom(
                offsetResponse(Collections.singletonMap(tp0, 0L), Errors.NONE),
                coordinator);
        assertEquals(0, consumer.committed(Collections.singleton(tp0)).get(tp0).offset());

        // verify that assignment immediately changes
        assertEquals(consumer.assignment(), singleton(tp0));

        // there shouldn't be any need to lookup the coordinator or fetch committed offsets.
        // we just lookup the starting position and send the record fetch.
        client.prepareResponse(listOffsetsResponse(Collections.singletonMap(tp0, 10L)));
        client.prepareResponse(fetchResponse(tp0, 10L, 1));

        @SuppressWarnings("unchecked")
        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ofMillis(1));
        assertEquals(1, records.count());
        assertEquals(11L, consumer.position(tp0));

        // new manual assignment
        consumer.assign(singleton(t2p0));

        // verify that assignment immediately changes
        assertEquals(consumer.assignment(), singleton(t2p0));

        // the auto commit is disabled, so no offset commit request should be sent
        for (ClientRequest req : client.requests())
            assertNotSame(req.requestBuilder().apiKey(), ApiKeys.OFFSET_COMMIT);

        client.requests().clear();
    }

    @Test
    public void testOffsetOfPausedPartitions() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 2));
        Node node = metadata.fetch().nodes().get(0);

        ConsumerPartitionAssignor assignor = new RangeAssignor();

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);

        // lookup coordinator
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
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
        assertEquals(0, consumer.committed(Collections.singleton(tp0)).get(tp0).offset());

        offsets.remove(tp0);
        offsets.put(tp1, 0L);
        client.prepareResponseFrom(offsetResponse(offsets, Errors.NONE), coordinator);
        assertEquals(0, consumer.committed(Collections.singleton(tp1)).get(tp1).offset());

        // fetch and verify consumer's position in the two partitions
        final Map<TopicPartition, Long> offsetResponse = new HashMap<>();
        offsetResponse.put(tp0, 3L);
        offsetResponse.put(tp1, 3L);
        client.prepareResponse(listOffsetsResponse(offsetResponse));
        assertEquals(3L, consumer.position(tp0));
        assertEquals(3L, consumer.position(tp1));

        client.requests().clear();
        consumer.unsubscribe();
    }

    @Test
    public void testPollWithNoSubscription() {
        consumer = newConsumer((String) null);
        assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testPollWithEmptySubscription() {
        consumer = newConsumer(groupId);
        consumer.subscribe(Collections.emptyList());
        assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testPollWithEmptyUserAssignment() {
        consumer = newConsumer(groupId);
        consumer.assign(Collections.emptySet());
        assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testGracefulClose() throws Exception {
        Map<TopicPartition, Errors> response = new HashMap<>();
        response.put(tp0, Errors.NONE);
        OffsetCommitResponse commitResponse = offsetCommitResponse(response);
        LeaveGroupResponse leaveGroupResponse = new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()));
        FetchResponse closeResponse = FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, new LinkedHashMap<>());
        consumerCloseTest(5000, Arrays.asList(commitResponse, leaveGroupResponse, closeResponse), 0, false);
    }

    @Test
    public void testCloseTimeoutDueToNoResponseForCloseFetchRequest() throws Exception {
        Map<TopicPartition, Errors> response = new HashMap<>();
        response.put(tp0, Errors.NONE);
        OffsetCommitResponse commitResponse = offsetCommitResponse(response);
        LeaveGroupResponse leaveGroupResponse = new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()));
        final List<AbstractResponse> serverResponsesWithoutCloseResponse = Arrays.asList(commitResponse, leaveGroupResponse);

        // to ensure timeout due to no response for fetcher close request, we will ensure that we have successful
        // response from server for first two requests and the test is configured to wait for duration which is greater
        // than configured timeout.
        final int closeTimeoutMs = 5000;
        final int waitForCloseCompletionMs = closeTimeoutMs + 1000;
        consumerCloseTest(closeTimeoutMs, serverResponsesWithoutCloseResponse, waitForCloseCompletionMs, false);
    }

    @Test
    public void testCloseTimeout() throws Exception {
        consumerCloseTest(5000, Collections.emptyList(), 5000, false);
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
        consumerCloseTest(0, Collections.emptyList(), 0, false);
    }

    @Test
    public void testCloseInterrupt() throws Exception {
        consumerCloseTest(Long.MAX_VALUE, Collections.emptyList(), 0, true);
    }

    @Test
    public void testCloseShouldBeIdempotent() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = spy(new MockClient(time, metadata));
        initMetadata(client, singletonMap(topic, 1));

        consumer = newConsumer(time, client, subscription, metadata, assignor, false, groupInstanceId);

        consumer.close(Duration.ZERO);
        consumer.close(Duration.ZERO);

        // verify that the call is idempotent by checking that the network client is only closed once.
        verify(client).close();
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
            newConsumer((String) null).committed(Collections.singleton(tp0)).get(tp0);
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
            consumer.committed(Collections.singleton(tp0)).get(tp0);
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
    public void testMetricConfigRecordingLevelInfo() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertEquals(Sensor.RecordingLevel.INFO, consumer.metrics.config().recordLevel());
        consumer.close(Duration.ZERO);

        props.put(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        KafkaConsumer<byte[], byte[]> consumer2 = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertEquals(Sensor.RecordingLevel.DEBUG, consumer2.metrics.config().recordLevel());
        consumer2.close(Duration.ZERO);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testShouldAttemptToRejoinGroupAfterSyncGroupFailed() throws Exception {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(time, client, subscription, metadata, assignor, false, groupInstanceId);
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());


        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, memberId, leaderId, Errors.NONE), coordinator);
        client.prepareResponseFrom(syncGroupResponse(singletonList(tp0), Errors.NONE), coordinator);

        client.prepareResponseFrom(fetchResponse(tp0, 0, 1), node);
        client.prepareResponseFrom(fetchResponse(tp0, 1, 0), node);

        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        consumer.poll(Duration.ZERO);

        // heartbeat fails due to rebalance in progress
        client.prepareResponseFrom(body -> true, new HeartbeatResponse(
            new HeartbeatResponseData().setErrorCode(Errors.REBALANCE_IN_PROGRESS.code())), coordinator);

        // join group
        final ByteBuffer byteBuffer = ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(singletonList(topic)));

        // This member becomes the leader
        final JoinGroupResponse leaderResponse = new JoinGroupResponse(
                new JoinGroupResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGenerationId(1).setProtocolName(assignor.name())
                        .setLeader(memberId).setMemberId(memberId)
                        .setMembers(Collections.singletonList(
                                new JoinGroupResponseData.JoinGroupResponseMember()
                                        .setMemberId(memberId)
                                        .setMetadata(byteBuffer.array())
                                )
                        ),
                ApiKeys.JOIN_GROUP.latestVersion()
        );

        client.prepareResponseFrom(leaderResponse, coordinator);

        // sync group fails due to disconnect
        client.prepareResponseFrom(syncGroupResponse(singletonList(tp0), Errors.NONE), coordinator, true);

        // should try and find the new coordinator
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);

        // rejoin group
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, memberId, leaderId, Errors.NONE), coordinator);
        client.prepareResponseFrom(syncGroupResponse(singletonList(tp0), Errors.NONE), coordinator);

        client.prepareResponseFrom(body -> body instanceof FetchRequest 
            && ((FetchRequest) body).fetchData(topicNames).containsKey(new TopicIdPartition(topicId, tp0)), fetchResponse(tp0, 1, 1), node);
        time.sleep(heartbeatIntervalMs);
        Thread.sleep(heartbeatIntervalMs);
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
        final ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ZERO);
        assertFalse(records.isEmpty());
    }

    private void consumerCloseTest(final long closeTimeoutMs,
                                   List<? extends AbstractResponse> responses,
                                   long waitMs,
                                   boolean interrupt) throws Exception {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata, assignor, false, Optional.empty());
        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        Node coordinator = prepareRebalance(client, node, assignor, singletonList(tp0), null);

        client.prepareMetadataUpdate(RequestTestUtils.metadataUpdateWithIds(1, Collections.singletonMap(topic, 1), topicIds));

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
            Future<?> future = executor.submit(() -> {
                consumer.commitAsync();
                try {
                    consumer.close(Duration.ofMillis(closeTimeoutMs));
                } catch (Exception e) {
                    closeException.set(e);
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

            // Ensure close has started and queued at least one more request after commitAsync.
            //
            // Close enqueues two requests, but second is enqueued only after first has succeeded. First is
            // LEAVE_GROUP as part of coordinator close and second is FETCH with epoch=FINAL_EPOCH. At this stage
            // we expect only the first one to have been requested. Hence, waiting for total 2 requests, one for
            // commit and another for LEAVE_GROUP.
            client.waitForRequests(2, 1000);

            // In graceful mode, commit response results in close() completing immediately without a timeout
            // In non-graceful mode, close() times out without an exception even though commit response is pending
            int nonCloseRequests = 1;
            for (int i = 0; i < responses.size(); i++) {
                client.waitForRequests(1, 1000);
                if (i == responses.size() - 1 && responses.get(i) instanceof FetchResponse) {
                    // last request is the close session request which is sent to the leader of the partition.
                    client.respondFrom(responses.get(i), node);
                } else {
                    client.respondFrom(responses.get(i), coordinator);
                }
                if (i < nonCloseRequests) {
                    // the close request should not complete until non-close requests (commit requests) have completed.
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
                assertTrue(future.cancel(true), "Close terminated prematurely");

                TestUtils.waitForCondition(
                    () -> closeException.get() != null, "InterruptException did not occur within timeout.");

                assertTrue(closeException.get() instanceof InterruptException, "Expected exception not thrown " + closeException);
            } else {
                future.get(closeTimeoutMs, TimeUnit.MILLISECONDS); // Should succeed without TimeoutException or ExecutionException
                assertNull(closeException.get(), "Unexpected exception during close");
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testPartitionsForNonExistingTopic() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Cluster cluster = metadata.fetch();

        MetadataResponse updateResponse = RequestTestUtils.metadataResponse(cluster.nodes(),
            cluster.clusterResource().clusterId(),
            cluster.controller().id(),
            Collections.emptyList());
        client.prepareResponse(updateResponse);

        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        assertEquals(Collections.emptyList(), consumer.partitionsFor("non-exist-topic"));
    }

    @Test
    public void testPartitionsForAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthenticationError();
        assertThrows(AuthenticationException.class, () -> consumer.partitionsFor("some other topic"));
    }

    @Test
    public void testBeginningOffsetsAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthenticationError();
        assertThrows(AuthenticationException.class, () -> consumer.beginningOffsets(Collections.singleton(tp0)));
    }

    @Test
    public void testEndOffsetsAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthenticationError();
        assertThrows(AuthenticationException.class, () -> consumer.endOffsets(Collections.singleton(tp0)));
    }

    @Test
    public void testPollAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthenticationError();
        consumer.subscribe(singleton(topic));
        assertThrows(AuthenticationException.class, () -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testOffsetsForTimesAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthenticationError();
        assertThrows(AuthenticationException.class, () -> consumer.offsetsForTimes(singletonMap(tp0, 0L)));
    }

    @Test
    public void testCommitSyncAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthenticationError();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp0, new OffsetAndMetadata(10L));
        assertThrows(AuthenticationException.class, () -> consumer.commitSync(offsets));
    }

    @Test
    public void testCommittedAuthenticationFailure() {
        final KafkaConsumer<String, String> consumer = consumerWithPendingAuthenticationError();
        assertThrows(AuthenticationException.class, () -> consumer.committed(Collections.singleton(tp0)).get(tp0));
    }

    @Test
    public void testMeasureCommitSyncDurationOnFailure() {
        final KafkaConsumer<String, String> consumer
            = consumerWithPendingError(new MockTime(Duration.ofSeconds(1).toMillis()));

        try {
            consumer.commitSync(Collections.singletonMap(tp0, new OffsetAndMetadata(10L)));
        } catch (final RuntimeException e) {
        }

        final Metric metric = consumer.metrics()
            .get(consumer.metrics.metricName("commit-sync-time-ns-total", "consumer-metrics"));
        assertTrue((Double) metric.metricValue() >= Duration.ofMillis(999).toNanos());
    }

    @Test
    public void testMeasureCommitSyncDuration() {
        Time time = new MockTime(Duration.ofSeconds(1).toMillis());
        SubscriptionState subscription = new SubscriptionState(new LogContext(),
            OffsetResetStrategy.EARLIEST);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 2));
        Node node = metadata.fetch().nodes().get(0);
        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata,
            assignor, true, groupInstanceId);
        consumer.assign(singletonList(tp0));

        client.prepareResponseFrom(
            FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        client.prepareResponseFrom(
            offsetCommitResponse(Collections.singletonMap(tp0, Errors.NONE)),
            coordinator
        );

        consumer.commitSync(Collections.singletonMap(tp0, new OffsetAndMetadata(10L)));

        final Metric metric = consumer.metrics()
            .get(consumer.metrics.metricName("commit-sync-time-ns-total", "consumer-metrics"));
        assertTrue((Double) metric.metricValue() >= Duration.ofMillis(999).toNanos());
    }

    @Test
    public void testMeasureCommittedDurationOnFailure() {
        final KafkaConsumer<String, String> consumer
            = consumerWithPendingError(new MockTime(Duration.ofSeconds(1).toMillis()));

        try {
            consumer.committed(Collections.singleton(tp0));
        } catch (final RuntimeException e) {
        }

        final Metric metric = consumer.metrics()
            .get(consumer.metrics.metricName("committed-time-ns-total", "consumer-metrics"));
        assertTrue((Double) metric.metricValue() >= Duration.ofMillis(999).toNanos());
    }

    @Test
    public void testMeasureCommittedDuration() {
        long offset1 = 10000;
        Time time = new MockTime(Duration.ofSeconds(1).toMillis());
        SubscriptionState subscription = new SubscriptionState(new LogContext(),
            OffsetResetStrategy.EARLIEST);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 2));
        Node node = metadata.fetch().nodes().get(0);
        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata,
            assignor, true, groupInstanceId);
        consumer.assign(singletonList(tp0));

        // lookup coordinator
        client.prepareResponseFrom(
            FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // fetch offset for one topic
        client.prepareResponseFrom(
            offsetResponse(Collections.singletonMap(tp0, offset1), Errors.NONE), coordinator);

        consumer.committed(Collections.singleton(tp0)).get(tp0).offset();

        final Metric metric = consumer.metrics()
            .get(consumer.metrics.metricName("committed-time-ns-total", "consumer-metrics"));
        assertTrue((Double) metric.metricValue() >= Duration.ofMillis(999).toNanos());
    }

    @Test
    public void testRebalanceException() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);

        consumer.subscribe(singleton(topic), getExceptionConsumerRebalanceListener());
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, memberId, leaderId, Errors.NONE), coordinator);
        client.prepareResponseFrom(syncGroupResponse(singletonList(tp0), Errors.NONE), coordinator);

        // assign throws
        try {
            consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));
            fail("Should throw exception");
        } catch (Throwable e) {
            assertEquals(partitionAssigned + singleTopicPartition, e.getCause().getMessage());
        }

        // the assignment is still updated regardless of the exception
        assertEquals(singleton(tp0), subscription.assignedPartitions());

        // close's revoke throws
        try {
            consumer.close(Duration.ofMillis(0));
            fail("Should throw exception");
        } catch (Throwable e) {
            assertEquals(partitionRevoked + singleTopicPartition, e.getCause().getCause().getMessage());
        }

        consumer.close(Duration.ofMillis(0));

        // the assignment is still updated regardless of the exception
        assertTrue(subscription.assignedPartitions().isEmpty());
    }

    @Test
    public void testReturnRecordsDuringRebalance() throws InterruptedException {
        Time time = new MockTime(1L);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        ConsumerPartitionAssignor assignor = new CooperativeStickyAssignor();
        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);

        initMetadata(client, Utils.mkMap(Utils.mkEntry(topic, 1), Utils.mkEntry(topic2, 1), Utils.mkEntry(topic3, 1)));

        consumer.subscribe(Arrays.asList(topic, topic2), getConsumerRebalanceListener(consumer));

        Node node = metadata.fetch().nodes().get(0);
        Node coordinator = prepareRebalance(client, node, assignor, Arrays.asList(tp0, t2p0), null);

        // a poll with non-zero milliseconds would complete three round-trips (discover, join, sync)
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100L));
            return consumer.assignment().equals(Utils.mkSet(tp0, t2p0));
        }, "Does not complete rebalance in time");

        assertEquals(Utils.mkSet(topic, topic2), consumer.subscription());
        assertEquals(Utils.mkSet(tp0, t2p0), consumer.assignment());

        // prepare a response of the outstanding fetch so that we have data available on the next poll
        Map<TopicPartition, FetchInfo> fetches1 = new HashMap<>();
        fetches1.put(tp0, new FetchInfo(0, 1));
        fetches1.put(t2p0, new FetchInfo(0, 10));
        client.respondFrom(fetchResponse(fetches1), node);

        ConsumerRecords<String, String> records = consumer.poll(Duration.ZERO);

        // verify that the fetch occurred as expected
        assertEquals(11, records.count());
        assertEquals(1L, consumer.position(tp0));
        assertEquals(10L, consumer.position(t2p0));

        // prepare the next response of the prefetch
        fetches1.clear();
        fetches1.put(tp0, new FetchInfo(1, 1));
        fetches1.put(t2p0, new FetchInfo(10, 20));
        client.respondFrom(fetchResponse(fetches1), node);

        // subscription change
        consumer.subscribe(Arrays.asList(topic, topic3), getConsumerRebalanceListener(consumer));

        // verify that subscription has changed but assignment is still unchanged
        assertEquals(Utils.mkSet(topic, topic3), consumer.subscription());
        assertEquals(Utils.mkSet(tp0, t2p0), consumer.assignment());

        // mock the offset commit response for to be revoked partitions
        Map<TopicPartition, Long> partitionOffsets1 = new HashMap<>();
        partitionOffsets1.put(t2p0, 10L);
        AtomicBoolean commitReceived = prepareOffsetCommitResponse(client, coordinator, partitionOffsets1);

        // poll once which would not complete the rebalance
        records = consumer.poll(Duration.ZERO);

        // clear out the prefetch so it doesn't interfere with the rest of the test
        fetches1.clear();
        fetches1.put(tp0, new FetchInfo(2, 1));
        client.respondFrom(fetchResponse(fetches1), node);

        // verify that the fetch still occurred as expected
        assertEquals(Utils.mkSet(topic, topic3), consumer.subscription());
        assertEquals(Collections.singleton(tp0), consumer.assignment());
        assertEquals(1, records.count());
        assertEquals(2L, consumer.position(tp0));

        // verify that the offset commits occurred as expected
        assertTrue(commitReceived.get());

        // mock rebalance responses
        client.respondFrom(joinGroupFollowerResponse(assignor, 2, "memberId", "leaderId", Errors.NONE), coordinator);

        // we need to poll 1) for getting the join response, and then send the sync request;
        //                 2) for getting the sync response
        records = consumer.poll(Duration.ZERO);

        // should not finish the response yet
        assertEquals(Utils.mkSet(topic, topic3), consumer.subscription());
        assertEquals(Collections.singleton(tp0), consumer.assignment());
        assertEquals(1, records.count());
        assertEquals(3L, consumer.position(tp0));

        fetches1.clear();
        fetches1.put(tp0, new FetchInfo(3, 1));
        client.respondFrom(fetchResponse(fetches1), node);

        // now complete the rebalance
        client.respondFrom(syncGroupResponse(Arrays.asList(tp0, t3p0), Errors.NONE), coordinator);

        AtomicInteger count = new AtomicInteger(0);
        TestUtils.waitForCondition(() -> {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(100L));
            return consumer.assignment().equals(Utils.mkSet(tp0, t3p0)) && count.addAndGet(recs.count()) == 1;

        }, "Does not complete rebalance in time");

        // should have t3 but not sent yet the t3 records
        assertEquals(Utils.mkSet(topic, topic3), consumer.subscription());
        assertEquals(Utils.mkSet(tp0, t3p0), consumer.assignment());
        assertEquals(4L, consumer.position(tp0));
        assertEquals(0L, consumer.position(t3p0));

        fetches1.clear();
        fetches1.put(tp0, new FetchInfo(4, 1));
        fetches1.put(t3p0, new FetchInfo(0, 100));
        client.respondFrom(fetchResponse(fetches1), node);

        count.set(0);
        TestUtils.waitForCondition(() -> {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(100L));
            return count.addAndGet(recs.count()) == 101;

        }, "Does not complete rebalance in time");

        assertEquals(5L, consumer.position(tp0));
        assertEquals(100L, consumer.position(t3p0));

        client.requests().clear();
        consumer.unsubscribe();
        consumer.close(Duration.ZERO);
    }

    @Test
    public void testGetGroupMetadata() {
        final ConsumerMetadata metadata = createMetadata(subscription);
        final MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        final Node node = metadata.fetch().nodes().get(0);

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);

        final ConsumerGroupMetadata groupMetadataOnStart = consumer.groupMetadata();
        assertEquals(groupId, groupMetadataOnStart.groupId());
        assertEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, groupMetadataOnStart.memberId());
        assertEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, groupMetadataOnStart.generationId());
        assertEquals(groupInstanceId, groupMetadataOnStart.groupInstanceId());

        consumer.subscribe(singleton(topic), getConsumerRebalanceListener(consumer));
        prepareRebalance(client, node, assignor, singletonList(tp0), null);

        // initial fetch
        client.prepareResponseFrom(fetchResponse(tp0, 0, 0), node);
        consumer.updateAssignmentMetadataIfNeeded(time.timer(Long.MAX_VALUE));

        final ConsumerGroupMetadata groupMetadataAfterPoll = consumer.groupMetadata();
        assertEquals(groupId, groupMetadataAfterPoll.groupId());
        assertEquals(memberId, groupMetadataAfterPoll.memberId());
        assertEquals(1, groupMetadataAfterPoll.generationId());
        assertEquals(groupInstanceId, groupMetadataAfterPoll.groupInstanceId());
    }

    @Test
    public void testInvalidGroupMetadata() throws InterruptedException {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 1));
        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata,
                new RoundRobinAssignor(), true, groupInstanceId);
        consumer.subscribe(singletonList(topic));
        // concurrent access is illegal
        client.enableBlockingUntilWakeup(1);
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.execute(() -> consumer.poll(Duration.ofSeconds(5)));
        try {
            TimeUnit.SECONDS.sleep(1);
            assertThrows(ConcurrentModificationException.class, consumer::groupMetadata);
            client.wakeup();
            consumer.wakeup();
        } finally {
            service.shutdown();
            assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
        }

        // accessing closed consumer is illegal
        consumer.close(Duration.ZERO);
        assertThrows(IllegalStateException.class, consumer::groupMetadata);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCurrentLag() {
        final ConsumerMetadata metadata = createMetadata(subscription);
        final MockClient client = new MockClient(time, metadata);

        initMetadata(client, singletonMap(topic, 1));

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);

        // throws for unassigned partition
        assertThrows(IllegalStateException.class, () -> consumer.currentLag(tp0));

        consumer.assign(singleton(tp0));

        // poll once to update with the current metadata
        consumer.poll(Duration.ofMillis(0));
        client.respond(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, metadata.fetch().nodes().get(0)));

        // no error for no current position
        assertEquals(OptionalLong.empty(), consumer.currentLag(tp0));
        assertEquals(0, client.inFlightRequestCount());

        // poll once again, which should send the list-offset request
        consumer.seek(tp0, 50L);
        consumer.poll(Duration.ofMillis(0));
        // requests: list-offset, fetch
        assertEquals(2, client.inFlightRequestCount());

        // no error for no end offset (so unknown lag)
        assertEquals(OptionalLong.empty(), consumer.currentLag(tp0));

        // poll once again, which should return the list-offset response
        // and hence next call would return correct lag result
        client.respond(listOffsetsResponse(singletonMap(tp0, 90L)));
        consumer.poll(Duration.ofMillis(0));

        assertEquals(OptionalLong.of(40L), consumer.currentLag(tp0));
        // requests: fetch
        assertEquals(1, client.inFlightRequestCount());

        // one successful fetch should update the log end offset and the position
        final FetchInfo fetchInfo = new FetchInfo(1L, 99L, 50L, 5);
        client.respond(fetchResponse(singletonMap(tp0, fetchInfo)));

        final ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ofMillis(1));
        assertEquals(5, records.count());
        assertEquals(55L, consumer.position(tp0));

        // correct lag result
        assertEquals(OptionalLong.of(45L), consumer.currentLag(tp0));
    }

    @Test
    public void testListOffsetShouldUpdateSubscriptions() {
        final ConsumerMetadata metadata = createMetadata(subscription);
        final MockClient client = new MockClient(time, metadata);

        initMetadata(client, singletonMap(topic, 1));

        consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);

        consumer.assign(singleton(tp0));

        // poll once to update with the current metadata
        consumer.poll(Duration.ofMillis(0));
        client.respond(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, metadata.fetch().nodes().get(0)));

        consumer.seek(tp0, 50L);
        client.prepareResponse(listOffsetsResponse(singletonMap(tp0, 90L)));

        assertEquals(singletonMap(tp0, 90L), consumer.endOffsets(Collections.singleton(tp0)));
        // correct lag result should be returned as well
        assertEquals(OptionalLong.of(40L), consumer.currentLag(tp0));
    }

    private KafkaConsumer<String, String> consumerWithPendingAuthenticationError(final Time time) {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        ConsumerPartitionAssignor assignor = new RangeAssignor();

        client.createPendingAuthenticationError(node, 0);
        return newConsumer(time, client, subscription, metadata, assignor, false, groupInstanceId);
    }

    private KafkaConsumer<String, String> consumerWithPendingAuthenticationError() {
        return consumerWithPendingAuthenticationError(new MockTime());
    }

    private KafkaConsumer<String, String> consumerWithPendingError(final Time time) {
        return consumerWithPendingAuthenticationError(time);
    }

    private ConsumerRebalanceListener getConsumerRebalanceListener(final KafkaConsumer<?, ?> consumer) {
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

    private ConsumerRebalanceListener getExceptionConsumerRebalanceListener() {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                throw new RuntimeException(partitionRevoked + partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                throw new RuntimeException(partitionAssigned + partitions);
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                throw new RuntimeException(partitionLost + partitions);
            }
        };
    }

    private ConsumerMetadata createMetadata(SubscriptionState subscription) {
        return new ConsumerMetadata(0, 0, Long.MAX_VALUE, false, false,
                                    subscription, new LogContext(), new ClusterResourceListeners());
    }

    private Node prepareRebalance(MockClient client, Node node, final Set<String> subscribedTopics, ConsumerPartitionAssignor assignor, List<TopicPartition> partitions, Node coordinator) {
        if (coordinator == null) {
            // lookup coordinator
            client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
            coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        }

        // join group
        client.prepareResponseFrom(body -> {
            JoinGroupRequest joinGroupRequest = (JoinGroupRequest) body;
            Iterator<JoinGroupRequestData.JoinGroupRequestProtocol> protocolIterator =
                    joinGroupRequest.data().protocols().iterator();
            assertTrue(protocolIterator.hasNext());

            ByteBuffer protocolMetadata = ByteBuffer.wrap(protocolIterator.next().metadata());
            ConsumerPartitionAssignor.Subscription subscription = ConsumerProtocol.deserializeSubscription(protocolMetadata);
            return subscribedTopics.equals(new HashSet<>(subscription.topics()));
        }, joinGroupFollowerResponse(assignor, 1, memberId, leaderId, Errors.NONE), coordinator);

        // sync group
        client.prepareResponseFrom(syncGroupResponse(partitions, Errors.NONE), coordinator);

        return coordinator;
    }

    private Node prepareRebalance(MockClient client, Node node, ConsumerPartitionAssignor assignor, List<TopicPartition> partitions, Node coordinator) {
        if (coordinator == null) {
            // lookup coordinator
            client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
            coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        }

        // join group
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, memberId, leaderId, Errors.NONE), coordinator);

        // sync group
        client.prepareResponseFrom(syncGroupResponse(partitions, Errors.NONE), coordinator);

        return coordinator;
    }

    private AtomicBoolean prepareHeartbeatResponse(MockClient client, Node coordinator, Errors error) {
        final AtomicBoolean heartbeatReceived = new AtomicBoolean(false);
        client.prepareResponseFrom(body -> {
            heartbeatReceived.set(true);
            return true;
        }, new HeartbeatResponse(new HeartbeatResponseData().setErrorCode(error.code())), coordinator);
        return heartbeatReceived;
    }

    private AtomicBoolean prepareOffsetCommitResponse(MockClient client, Node coordinator, final Map<TopicPartition, Long> partitionOffsets) {
        final AtomicBoolean commitReceived = new AtomicBoolean(true);
        Map<TopicPartition, Errors> response = new HashMap<>();
        for (TopicPartition partition : partitionOffsets.keySet())
            response.put(partition, Errors.NONE);

        client.prepareResponseFrom(body -> {
            OffsetCommitRequest commitRequest = (OffsetCommitRequest) body;
            Map<TopicPartition, Long> commitErrors = commitRequest.offsets();

            for (Map.Entry<TopicPartition, Long> partitionOffset : partitionOffsets.entrySet()) {
                // verify that the expected offset has been committed
                if (!commitErrors.get(partitionOffset.getKey()).equals(partitionOffset.getValue())) {
                    commitReceived.set(false);
                    return false;
                }
            }
            return true;
        }, offsetCommitResponse(response), coordinator);
        return commitReceived;
    }

    private AtomicBoolean prepareOffsetCommitResponse(MockClient client, Node coordinator, final TopicPartition partition, final long offset) {
        return prepareOffsetCommitResponse(client, coordinator, Collections.singletonMap(partition, offset));
    }

    private OffsetCommitResponse offsetCommitResponse(Map<TopicPartition, Errors> responseData) {
        return new OffsetCommitResponse(responseData);
    }

    private JoinGroupResponse joinGroupFollowerResponse(ConsumerPartitionAssignor assignor, int generationId, String memberId, String leaderId, Errors error) {
        return new JoinGroupResponse(
                new JoinGroupResponseData()
                        .setErrorCode(error.code())
                        .setGenerationId(generationId)
                        .setProtocolName(assignor.name())
                        .setLeader(leaderId)
                        .setMemberId(memberId)
                        .setMembers(Collections.emptyList()),
                ApiKeys.JOIN_GROUP.latestVersion()
        );
    }

    private SyncGroupResponse syncGroupResponse(List<TopicPartition> partitions, Errors error) {
        ByteBuffer buf = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(partitions));
        return new SyncGroupResponse(
                new SyncGroupResponseData()
                        .setErrorCode(error.code())
                        .setAssignment(Utils.toArray(buf))
        );
    }

    private OffsetFetchResponse offsetResponse(Map<TopicPartition, Long> offsets, Errors error) {
        Map<TopicPartition, OffsetFetchResponse.PartitionData> partitionData = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            partitionData.put(entry.getKey(), new OffsetFetchResponse.PartitionData(entry.getValue(),
                    Optional.empty(), "", error));
        }
        return new OffsetFetchResponse(
            throttleMs,
            Collections.singletonMap(groupId, Errors.NONE),
            Collections.singletonMap(groupId, partitionData));
    }

    private ListOffsetsResponse listOffsetsResponse(Map<TopicPartition, Long> offsets) {
        return listOffsetsResponse(offsets, Collections.emptyMap());
    }

    private ListOffsetsResponse listOffsetsResponse(Map<TopicPartition, Long> partitionOffsets,
                                                    Map<TopicPartition, Errors> partitionErrors) {
        Map<String, ListOffsetsTopicResponse> responses = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> partitionOffset : partitionOffsets.entrySet()) {
            TopicPartition tp = partitionOffset.getKey();
            ListOffsetsTopicResponse topic = responses.computeIfAbsent(tp.topic(), k -> new ListOffsetsTopicResponse().setName(tp.topic()));
            topic.partitions().add(new ListOffsetsPartitionResponse()
                    .setPartitionIndex(tp.partition())
                    .setErrorCode(Errors.NONE.code())
                    .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                    .setOffset(partitionOffset.getValue()));
        }

        for (Map.Entry<TopicPartition, Errors> partitionError : partitionErrors.entrySet()) {
            TopicPartition tp = partitionError.getKey();
            ListOffsetsTopicResponse topic = responses.computeIfAbsent(tp.topic(), k -> new ListOffsetsTopicResponse().setName(tp.topic()));
            topic.partitions().add(new ListOffsetsPartitionResponse()
                    .setPartitionIndex(tp.partition())
                    .setErrorCode(partitionError.getValue().code())
                    .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                    .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET));
        }
        ListOffsetsResponseData data = new ListOffsetsResponseData()
                .setTopics(new ArrayList<>(responses.values()));
        return new ListOffsetsResponse(data);
    }

    private FetchResponse fetchResponse(Map<TopicPartition, FetchInfo> fetches) {
        LinkedHashMap<TopicIdPartition, FetchResponseData.PartitionData> tpResponses = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, FetchInfo> fetchEntry : fetches.entrySet()) {
            TopicPartition partition = fetchEntry.getKey();
            long fetchOffset = fetchEntry.getValue().offset;
            int fetchCount = fetchEntry.getValue().count;
            final long highWatermark = fetchEntry.getValue().logLastOffset + 1;
            final long logStartOffset = fetchEntry.getValue().logFirstOffset;
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
            tpResponses.put(new TopicIdPartition(topicIds.get(partition.topic()), partition),
                new FetchResponseData.PartitionData()
                    .setPartitionIndex(partition.partition())
                    .setHighWatermark(highWatermark)
                    .setLogStartOffset(logStartOffset)
                    .setRecords(records));
        }
        return FetchResponse.of(Errors.NONE, 0, INVALID_SESSION_ID, tpResponses);
    }

    private FetchResponse fetchResponse(TopicPartition partition, long fetchOffset, int count) {
        FetchInfo fetchInfo = new FetchInfo(fetchOffset, count);
        return fetchResponse(Collections.singletonMap(partition, fetchInfo));
    }

    private KafkaConsumer<String, String> newConsumer(Time time,
                                                      KafkaClient client,
                                                      SubscriptionState subscription,
                                                      ConsumerMetadata metadata,
                                                      ConsumerPartitionAssignor assignor,
                                                      boolean autoCommitEnabled,
                                                      Optional<String> groupInstanceId) {
        return newConsumer(time, client, subscription, metadata, assignor, autoCommitEnabled, groupId, groupInstanceId, false);
    }

    private KafkaConsumer<String, String> newConsumerNoAutoCommit(Time time,
                                                                  KafkaClient client,
                                                                  SubscriptionState subscription,
                                                                  ConsumerMetadata metadata) {
        return newConsumer(time, client, subscription, metadata, new RangeAssignor(), false, groupId, groupInstanceId, false);
    }

    private KafkaConsumer<String, String> newConsumer(Time time,
                                                      KafkaClient client,
                                                      SubscriptionState subscription,
                                                      ConsumerMetadata metadata,
                                                      ConsumerPartitionAssignor assignor,
                                                      boolean autoCommitEnabled,
                                                      String groupId,
                                                      Optional<String> groupInstanceId,
                                                      boolean throwOnStableOffsetNotSupported) {
        return newConsumer(time, client, subscription, metadata, assignor, autoCommitEnabled, groupId, groupInstanceId,
        Optional.of(new StringDeserializer()), throwOnStableOffsetNotSupported);
    }

    private KafkaConsumer<String, String> newConsumer(Time time,
                                                      KafkaClient client,
                                                      SubscriptionState subscription,
                                                      ConsumerMetadata metadata,
                                                      ConsumerPartitionAssignor assignor,
                                                      boolean autoCommitEnabled,
                                                      String groupId,
                                                      Optional<String> groupInstanceId,
                                                      Optional<Deserializer<String>> valueDeserializer,
                                                      boolean throwOnStableOffsetNotSupported) {
        String clientId = "mock-consumer";
        String metricGroupPrefix = "consumer";
        long retryBackoffMs = 100;
        long retryBackoffMaxMs = 1000;
        int minBytes = 1;
        int maxBytes = Integer.MAX_VALUE;
        int maxWaitMs = 500;
        int fetchSize = 1024 * 1024;
        int maxPollRecords = Integer.MAX_VALUE;
        boolean checkCrcs = true;
        int rebalanceTimeoutMs = 60000;

        Deserializer<String> keyDeserializer = new StringDeserializer();
        Deserializer<String> deserializer = valueDeserializer.orElse(new StringDeserializer());

        List<ConsumerPartitionAssignor> assignors = singletonList(assignor);
        ConsumerInterceptors<String, String> interceptors = new ConsumerInterceptors<>(Collections.emptyList());

        Metrics metrics = new Metrics(time);
        ConsumerMetrics metricsRegistry = new ConsumerMetrics(metricGroupPrefix);

        LogContext loggerFactory = new LogContext();
        ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(loggerFactory, client, metadata, time,
                retryBackoffMs, requestTimeoutMs, heartbeatIntervalMs);

        ConsumerCoordinator consumerCoordinator = null;
        if (groupId != null) {
            GroupRebalanceConfig rebalanceConfig = new GroupRebalanceConfig(sessionTimeoutMs,
                rebalanceTimeoutMs,
                heartbeatIntervalMs,
                groupId,
                groupInstanceId,
                retryBackoffMs,
                retryBackoffMaxMs,
                true);
            consumerCoordinator = new ConsumerCoordinator(rebalanceConfig,
                loggerFactory,
                consumerClient,
                assignors,
                metadata,
                subscription,
                metrics,
                metricGroupPrefix,
                time,
                autoCommitEnabled,
                autoCommitIntervalMs,
                interceptors,
                throwOnStableOffsetNotSupported,
                null);
        }
        IsolationLevel isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        FetchMetricsManager metricsManager = new FetchMetricsManager(metrics, metricsRegistry.fetcherMetrics);
        FetchConfig fetchConfig = new FetchConfig(
                minBytes,
                maxBytes,
                maxWaitMs,
                fetchSize,
                maxPollRecords,
                checkCrcs,
                CommonClientConfigs.DEFAULT_CLIENT_RACK,
                isolationLevel);
        Fetcher<String, String> fetcher = new Fetcher<>(
                loggerFactory,
                consumerClient,
                metadata,
                subscription,
                fetchConfig,
                new Deserializers<>(keyDeserializer, deserializer),
                metricsManager,
                time,
                new ApiVersions());
        OffsetFetcher offsetFetcher = new OffsetFetcher(loggerFactory,
                consumerClient,
                metadata,
                subscription,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                isolationLevel,
                new ApiVersions());
        TopicMetadataFetcher topicMetadataFetcher = new TopicMetadataFetcher(loggerFactory,
                consumerClient,
                retryBackoffMs,
                retryBackoffMaxMs);

        return new KafkaConsumer<>(
                loggerFactory,
                clientId,
                consumerCoordinator,
                keyDeserializer,
                deserializer,
                fetcher,
                offsetFetcher,
                topicMetadataFetcher,
                interceptors,
                time,
                consumerClient,
                metrics,
                subscription,
                metadata,
                retryBackoffMs,
                retryBackoffMaxMs,
                requestTimeoutMs,
                defaultApiTimeoutMs,
                assignors,
                groupId);
    }

    private static class FetchInfo {
        long logFirstOffset;
        long logLastOffset;
        long offset;
        int count;

        FetchInfo(long offset, int count) {
            this(0L, offset + count, offset, count);
        }

        FetchInfo(long logFirstOffset, long logLastOffset, long offset, int count) {
            this.logFirstOffset = logFirstOffset;
            this.logLastOffset = logLastOffset;
            this.offset = offset;
            this.count = count;
        }
    }

    @Test
    public void testSubscriptionOnInvalidTopic() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Cluster cluster = metadata.fetch();

        String invalidTopicName = "topic abc";  // Invalid topic name due to space

        List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
        topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.INVALID_TOPIC_EXCEPTION,
                invalidTopicName, false, Collections.emptyList()));
        MetadataResponse updateResponse = RequestTestUtils.metadataResponse(cluster.nodes(),
                cluster.clusterResource().clusterId(),
                cluster.controller().id(),
                topicMetadata);
        client.prepareMetadataUpdate(updateResponse);

        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        consumer.subscribe(singleton(invalidTopicName), getConsumerRebalanceListener(consumer));

        assertThrows(InvalidTopicException.class, () -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testPollTimeMetrics() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 1));

        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        consumer.subscribe(singletonList(topic));
        // MetricName objects to check
        Metrics metrics = consumer.metrics;
        MetricName lastPollSecondsAgoName = metrics.metricName("last-poll-seconds-ago", "consumer-metrics");
        MetricName timeBetweenPollAvgName = metrics.metricName("time-between-poll-avg", "consumer-metrics");
        MetricName timeBetweenPollMaxName = metrics.metricName("time-between-poll-max", "consumer-metrics");
        // Test default values
        assertEquals(-1.0d, consumer.metrics().get(lastPollSecondsAgoName).metricValue());
        assertEquals(Double.NaN, consumer.metrics().get(timeBetweenPollAvgName).metricValue());
        assertEquals(Double.NaN, consumer.metrics().get(timeBetweenPollMaxName).metricValue());
        // Call first poll
        consumer.poll(Duration.ZERO);
        assertEquals(0.0d, consumer.metrics().get(lastPollSecondsAgoName).metricValue());
        assertEquals(0.0d, consumer.metrics().get(timeBetweenPollAvgName).metricValue());
        assertEquals(0.0d, consumer.metrics().get(timeBetweenPollMaxName).metricValue());
        // Advance time by 5,000 (total time = 5,000)
        time.sleep(5 * 1000L);
        assertEquals(5.0d, consumer.metrics().get(lastPollSecondsAgoName).metricValue());
        // Call second poll
        consumer.poll(Duration.ZERO);
        assertEquals(2.5 * 1000d, consumer.metrics().get(timeBetweenPollAvgName).metricValue());
        assertEquals(5 * 1000d, consumer.metrics().get(timeBetweenPollMaxName).metricValue());
        // Advance time by 10,000 (total time = 15,000)
        time.sleep(10 * 1000L);
        assertEquals(10.0d, consumer.metrics().get(lastPollSecondsAgoName).metricValue());
        // Call third poll
        consumer.poll(Duration.ZERO);
        assertEquals(5 * 1000d, consumer.metrics().get(timeBetweenPollAvgName).metricValue());
        assertEquals(10 * 1000d, consumer.metrics().get(timeBetweenPollMaxName).metricValue());
        // Advance time by 5,000 (total time = 20,000)
        time.sleep(5 * 1000L);
        assertEquals(5.0d, consumer.metrics().get(lastPollSecondsAgoName).metricValue());
        // Call fourth poll
        consumer.poll(Duration.ZERO);
        assertEquals(5 * 1000d, consumer.metrics().get(timeBetweenPollAvgName).metricValue());
        assertEquals(10 * 1000d, consumer.metrics().get(timeBetweenPollMaxName).metricValue());
    }

    @Test
    public void testPollIdleRatio() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 1));

        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        // MetricName object to check
        Metrics metrics = consumer.metrics;
        MetricName pollIdleRatio = metrics.metricName("poll-idle-ratio-avg", "consumer-metrics");
        // Test default value
        assertEquals(Double.NaN, consumer.metrics().get(pollIdleRatio).metricValue());

        // 1st poll
        // Spend 50ms in poll so value = 1.0
        consumer.kafkaConsumerMetrics.recordPollStart(time.milliseconds());
        time.sleep(50);
        consumer.kafkaConsumerMetrics.recordPollEnd(time.milliseconds());

        assertEquals(1.0d, consumer.metrics().get(pollIdleRatio).metricValue());

        // 2nd poll
        // Spend 50m outside poll and 0ms in poll so value = 0.0
        time.sleep(50);
        consumer.kafkaConsumerMetrics.recordPollStart(time.milliseconds());
        consumer.kafkaConsumerMetrics.recordPollEnd(time.milliseconds());

        // Avg of first two data points
        assertEquals((1.0d + 0.0d) / 2, consumer.metrics().get(pollIdleRatio).metricValue());

        // 3rd poll
        // Spend 25ms outside poll and 25ms in poll so value = 0.5
        time.sleep(25);
        consumer.kafkaConsumerMetrics.recordPollStart(time.milliseconds());
        time.sleep(25);
        consumer.kafkaConsumerMetrics.recordPollEnd(time.milliseconds());

        // Avg of three data points
        assertEquals((1.0d + 0.0d + 0.5d) / 3, consumer.metrics().get(pollIdleRatio).metricValue());
    }

    private static boolean consumerMetricPresent(KafkaConsumer<String, String> consumer, String name) {
        MetricName metricName = new MetricName(name, "consumer-metrics", "", Collections.emptyMap());
        return consumer.metrics.metrics().containsKey(metricName);
    }

    @Test
    public void testClosingConsumerUnregistersConsumerMetrics() {
        Time time = new MockTime(1L);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 1));
        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata,
            new RoundRobinAssignor(), true, groupInstanceId);
        consumer.subscribe(singletonList(topic));
        assertTrue(consumerMetricPresent(consumer, "last-poll-seconds-ago"));
        assertTrue(consumerMetricPresent(consumer, "time-between-poll-avg"));
        assertTrue(consumerMetricPresent(consumer, "time-between-poll-max"));
        consumer.close();
        assertFalse(consumerMetricPresent(consumer, "last-poll-seconds-ago"));
        assertFalse(consumerMetricPresent(consumer, "time-between-poll-avg"));
        assertFalse(consumerMetricPresent(consumer, "time-between-poll-max"));
    }

    @Test
    public void testEnforceRebalanceWithManualAssignment() {
        consumer = newConsumer((String) null);
        consumer.assign(singleton(new TopicPartition("topic", 0)));
        assertThrows(IllegalStateException.class, consumer::enforceRebalance);
    }

    @Test
    public void testEnforceRebalanceTriggersRebalanceOnNextPoll() {
        Time time = new MockTime(1L);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata, assignor, true, groupInstanceId);
        MockRebalanceListener countingRebalanceListener = new MockRebalanceListener();
        initMetadata(client, Utils.mkMap(Utils.mkEntry(topic, 1), Utils.mkEntry(topic2, 1), Utils.mkEntry(topic3, 1)));

        consumer.subscribe(Arrays.asList(topic, topic2), countingRebalanceListener);
        Node node = metadata.fetch().nodes().get(0);
        prepareRebalance(client, node, assignor, Arrays.asList(tp0, t2p0), null);

        // a first rebalance to get the assignment, we need two poll calls since we need two round trips to finish join / sync-group
        consumer.poll(Duration.ZERO);
        consumer.poll(Duration.ZERO);

        // onPartitionsRevoked is not invoked when first joining the group
        assertEquals(countingRebalanceListener.revokedCount, 0);
        assertEquals(countingRebalanceListener.assignedCount, 1);

        consumer.enforceRebalance();

        // the next poll should trigger a rebalance
        consumer.poll(Duration.ZERO);

        assertEquals(countingRebalanceListener.revokedCount, 1);
    }

    @Test
    public void testEnforceRebalanceReason() {
        Time time = new MockTime(1L);

        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Utils.mkMap(Utils.mkEntry(topic, 1)));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newConsumer(
            time,
            client,
            subscription,
            metadata,
            assignor,
            true,
            groupInstanceId
        );
        consumer.subscribe(Collections.singletonList(topic));

        // Lookup coordinator.
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        consumer.poll(Duration.ZERO);

        // Initial join sends an empty reason.
        prepareJoinGroupAndVerifyReason(client, node, "");
        consumer.poll(Duration.ZERO);

        // A null reason should be replaced by the default reason.
        consumer.enforceRebalance(null);
        prepareJoinGroupAndVerifyReason(client, node, DEFAULT_REASON);
        consumer.poll(Duration.ZERO);

        // An empty reason should be replaced by the default reason.
        consumer.enforceRebalance("");
        prepareJoinGroupAndVerifyReason(client, node, DEFAULT_REASON);
        consumer.poll(Duration.ZERO);

        // A non-null and non-empty reason is sent as-is.
        String customReason = "user provided reason";
        consumer.enforceRebalance(customReason);
        prepareJoinGroupAndVerifyReason(client, node, customReason);
        consumer.poll(Duration.ZERO);
    }

    private void prepareJoinGroupAndVerifyReason(
        MockClient client,
        Node node,
        String expectedReason
    ) {
        client.prepareResponseFrom(
            body -> {
                JoinGroupRequest joinGroupRequest = (JoinGroupRequest) body;
                return expectedReason.equals(joinGroupRequest.data().reason());
            },
            joinGroupFollowerResponse(assignor, 1, memberId, leaderId, Errors.NONE),
            node
        );
    }

    @Test
    public void configurableObjectsShouldSeeGeneratedClientId() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DeserializerForClientId.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DeserializerForClientId.class.getName());
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptorForClientId.class.getName());

        consumer = new KafkaConsumer<>(props);
        assertNotNull(consumer.getClientId());
        assertNotEquals(0, consumer.getClientId().length());
        assertEquals(3, CLIENT_IDS.size());
        CLIENT_IDS.forEach(id -> assertEquals(id, consumer.getClientId()));
    }

    @Test
    public void testUnusedConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLS");
        ConsumerConfig config = new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(props, new StringDeserializer(), new StringDeserializer()));

        assertTrue(config.unused().contains(SslConfigs.SSL_PROTOCOL_CONFIG));

        consumer = new KafkaConsumer<>(config, null, null);
        assertTrue(config.unused().contains(SslConfigs.SSL_PROTOCOL_CONFIG));
    }

    @Test
    public void testAssignorNameConflict() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            Arrays.asList(RangeAssignor.class.getName(), ConsumerPartitionAssignorTest.TestConsumerPartitionAssignor.class.getName()));

        assertThrows(KafkaException.class,
            () -> new KafkaConsumer<>(configs, new StringDeserializer(), new StringDeserializer()));
    }

    @Test
    public void testOffsetsForTimesTimeout() {
        final KafkaConsumer<String, String> consumer = consumerForCheckingTimeoutException();
        assertEquals(
            "Failed to get offsets by times in 60000ms",
            assertThrows(org.apache.kafka.common.errors.TimeoutException.class, () -> consumer.offsetsForTimes(singletonMap(tp0, 0L))).getMessage()
        );
    }

    @Test
    public void testBeginningOffsetsTimeout() {
        final KafkaConsumer<String, String> consumer = consumerForCheckingTimeoutException();
        assertEquals(
            "Failed to get offsets by times in 60000ms",
            assertThrows(org.apache.kafka.common.errors.TimeoutException.class, () -> consumer.beginningOffsets(singletonList(tp0))).getMessage()
        );
    }

    @Test
    public void testEndOffsetsTimeout() {
        final KafkaConsumer<String, String> consumer = consumerForCheckingTimeoutException();
        assertEquals(
            "Failed to get offsets by times in 60000ms",
            assertThrows(org.apache.kafka.common.errors.TimeoutException.class, () -> consumer.endOffsets(singletonList(tp0))).getMessage()
        );
    }

    private KafkaConsumer<String, String> consumerForCheckingTimeoutException() {
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, singletonMap(topic, 1));

        ConsumerPartitionAssignor assignor = new RangeAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, subscription, metadata, assignor, false, groupInstanceId);

        for (int i = 0; i < 10; i++) {
            client.prepareResponse(
                request -> {
                    time.sleep(defaultApiTimeoutMs / 10);
                    return request instanceof ListOffsetsRequest;
                },
                listOffsetsResponse(
                    Collections.emptyMap(),
                    Collections.singletonMap(tp0, Errors.UNKNOWN_TOPIC_OR_PARTITION)
                ));
        }

        return consumer;
    }

    private static final List<String> CLIENT_IDS = new ArrayList<>();
    public static class DeserializerForClientId implements Deserializer<byte[]> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            CLIENT_IDS.add(configs.get(ConsumerConfig.CLIENT_ID_CONFIG).toString());
        }

        @Override
        public byte[] deserialize(String topic, byte[] data) {
            return data;
        }
    }

    public static class ConsumerInterceptorForClientId implements ConsumerInterceptor<byte[], byte[]> {

        @Override
        public ConsumerRecords<byte[], byte[]> onConsume(ConsumerRecords<byte[], byte[]> records) {
            return records;
        }

        @Override
        public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {
            CLIENT_IDS.add(configs.get(ConsumerConfig.CLIENT_ID_CONFIG).toString());
        }
    }
}
