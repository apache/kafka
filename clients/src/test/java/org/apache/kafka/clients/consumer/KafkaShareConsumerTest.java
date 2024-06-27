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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.requests.ShareGroupHeartbeatResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.apache.kafka.common.utils.Utils.propsToMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Note to future authors in this class. If you close the consumer, close with Duration.ZERO to reduce the duration of
 * the test.
 */
@SuppressWarnings({"ClassFanOutComplexity", "ClassDataAbstractionCoupling"})
public class KafkaShareConsumerTest {

    private final String topic = "test";
    private final Uuid topicId = Uuid.randomUuid();
    private final TopicPartition tp0 = new TopicPartition(topic, 0);

    private final String topic2 = "test2";
    private final Uuid topicId2 = Uuid.randomUuid();
    private final TopicPartition t2p0 = new TopicPartition(topic2, 0);

    private final String topic3 = "test3";
    private final Uuid topicId3 = Uuid.randomUuid();

    private final int heartbeatIntervalMs = 1000;

    private final String groupId = "mock-group";
    private final Map<String, Uuid> topicIds = Stream.of(
                    new AbstractMap.SimpleEntry<>(topic, topicId),
                    new AbstractMap.SimpleEntry<>(topic2, topicId2),
                    new AbstractMap.SimpleEntry<>(topic3, topicId3))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    private KafkaShareConsumer<?, ?> consumer;

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
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        consumer = newShareConsumer(props, new StringDeserializer(), new StringDeserializer());

        assertEquals(3, consumer.metricsRegistry().reporters().size());

        MockMetricsReporter mockMetricsReporter = (MockMetricsReporter) consumer.metricsRegistry().reporters().stream()
                .filter(reporter -> reporter instanceof MockMetricsReporter).findFirst().get();
        assertEquals(consumer.clientId(), mockMetricsReporter.clientId);
    }

    @Test
    public void testExplicitlyOnlyEnableJmxReporter() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "org.apache.kafka.common.metrics.JmxReporter");
        props.setProperty(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG, "false");
        consumer = newShareConsumer(props, new StringDeserializer(), new StringDeserializer());
        assertEquals(1, consumer.metricsRegistry().reporters().size());
        assertInstanceOf(JmxReporter.class, consumer.metricsRegistry().reporters().get(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPollReturnsRecords() {
        consumer = setUpConsumerWithRecordsToPoll(tp0, 5);

        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ofMillis(2000L));

        assertEquals(5, records.count());
        assertEquals(Collections.singleton(tp0), records.partitions());
        assertEquals(5, records.records(tp0).size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSecondPollWithDeserializationErrorThrowsRecordDeserializationException() {
        int invalidRecordNumber = 4;
        int invalidRecordOffset = 3;
        StringDeserializer deserializer = mockErrorDeserializer(invalidRecordNumber);

        consumer = setUpConsumerWithRecordsToPoll(tp0, 5, deserializer);
        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ofMillis(2000L));

        assertEquals(invalidRecordNumber - 1, records.count());
        assertEquals(Collections.singleton(tp0), records.partitions());
        assertEquals(invalidRecordNumber - 1, records.records(tp0).size());
        long lastOffset = records.records(tp0).get(records.records(tp0).size() - 1).offset();
        assertEquals(invalidRecordNumber - 2, lastOffset);

        RecordDeserializationException rde = assertThrows(RecordDeserializationException.class, () -> consumer.poll(Duration.ZERO));
        assertEquals(invalidRecordOffset, rde.offset());
        assertEquals(tp0, rde.topicPartition());
    }

    /* A mock deserializer which throws a SerializationException on the Nth record's value deserialization */
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

    private KafkaShareConsumer<?, ?> setUpConsumerWithRecordsToPoll(TopicPartition tp,
                                                                    int recordCount) {
        return setUpConsumerWithRecordsToPoll(tp, recordCount, new StringDeserializer());
    }

    private KafkaShareConsumer<?, ?> setUpConsumerWithRecordsToPoll(TopicPartition tp,
                                                                    int recordCount,
                                                                    Deserializer<String> deserializer) {
        Cluster cluster = TestUtils.singletonCluster(tp.topic(), 1);
        Node node = cluster.nodes().get(0);

        SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
        ConsumerMetadata metadata = createMetadata(subscription);
        Time time = new MockTime();
        MockClient client = new MockClient(time, metadata);
        initMetadata(client, Collections.singletonMap(topic, 1));
        Uuid memberId = Uuid.randomUuid();
        joinGroupAndGetInitialAssignment(client, node, memberId, 1, Collections.singletonList(tp), null);
        client.prepareResponseFrom(shareFetchResponse(tp, 0L, recordCount), node);

        consumer = newShareConsumer(time, client, subscription, metadata, groupId, Optional.of(deserializer));
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    @Test
    public void testConstructorClose() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "invalid-23-8409-adsfsdj");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
        try {
            newShareConsumer(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            fail("should have caught an exception and returned");
        } catch (KafkaException e) {
            assertEquals(oldInitCount + 1, MockMetricsReporter.INIT_COUNT.get());
            assertEquals(oldCloseCount + 1, MockMetricsReporter.CLOSE_COUNT.get());
            assertEquals("Failed to construct Kafka share consumer", e.getMessage());
        }
    }

    @Test
    public void testOsDefaultSocketBufferSizes() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumer = newShareConsumer(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test
    public void testInvalidSocketSendBufferSize() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, -2);
        assertThrows(KafkaException.class,
                () -> newShareConsumer(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()));
    }

    @Test
    public void testInvalidSocketReceiveBufferSize() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, -2);
        assertThrows(KafkaException.class,
                () -> newShareConsumer(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()));
    }

    @Test
    public void testSubscription() {
        consumer = newShareConsumer(groupId);

        consumer.subscribe(Collections.singletonList(topic));
        assertEquals(Collections.singleton(topic), consumer.subscription());

        consumer.subscribe(Collections.emptyList());
        assertTrue(consumer.subscription().isEmpty());

        consumer.unsubscribe();
        assertTrue(consumer.subscription().isEmpty());
    }

    @Test
    public void testSubscriptionOnNullTopicCollection() {
        consumer = newShareConsumer(groupId);
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(null));
    }

    @Test
    public void testSubscriptionOnNullTopic() {
        consumer = newShareConsumer(groupId);
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(Collections.singletonList(null)));
    }

    @Test
    public void testSubscriptionOnEmptyTopic() {
        consumer = newShareConsumer(groupId);
        String emptyTopic = "  ";
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(Collections.singletonList(emptyTopic)));
    }

    @Test
    public void testConsumerJmxPrefix() throws  Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ConsumerConfig.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-1");
        consumer = newShareConsumer(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        MetricName testMetricName = consumer.metricsRegistry().metricName("test-metric",
                "grp1", "test metric");
        consumer.metricsRegistry().addMetric(testMetricName, new Avg());
        assertNotNull(server.getObjectInstance(new ObjectName("kafka.consumer:type=grp1,client-id=client-1")));
    }

    private KafkaShareConsumer<byte[], byte[]> newShareConsumer(String groupId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my.consumer");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return newShareConsumer(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private <K, V> KafkaShareConsumer<K, V> newShareConsumer(Properties props) {
        return newShareConsumer(props, null, null);
    }

    private <K, V> KafkaShareConsumer<K, V> newShareConsumer(Map<String, Object> configs,
                                                             Deserializer<K> keyDeserializer,
                                                             Deserializer<V> valueDeserializer) {
        return new KafkaShareConsumer<>(
                new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    private <K, V> KafkaShareConsumer<K, V> newShareConsumer(Properties props,
                                                             Deserializer<K> keyDeserializer,
                                                             Deserializer<V> valueDeserializer) {
        return newShareConsumer(propsToMap(props), keyDeserializer, valueDeserializer);
    }

    @Test
    public void testHeartbeatSent() throws Exception {
        SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
        ConsumerMetadata metadata = createMetadata(subscription);
        Time time = new MockTime();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        Uuid memberId = Uuid.randomUuid();
        Node coordinator = joinGroupAndGetInitialAssignment(client, node, memberId, 1, Collections.singletonList(tp0), null);

        consumer = newShareConsumer(time, client, subscription, metadata);
        consumer.subscribe(Collections.singleton(topic));

        // initial share fetch
        client.prepareResponseFrom(shareFetchResponse(tp0, 0L, 0), node);

        AtomicBoolean heartbeatReceived = prepareShareGroupHeartbeatResponse(client, coordinator, memberId, 2, Errors.NONE);

        // heartbeat interval is 1 second
        time.sleep(heartbeatIntervalMs + 500);
        Thread.sleep(heartbeatIntervalMs + 500);

        assertTrue(heartbeatReceived.get());
    }

    @Test
    public void testPollTimesOutDuringMetadataUpdate() {
        SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
        ConsumerMetadata metadata = createMetadata(subscription);
        Time time = new MockTime();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newShareConsumer(time, client, subscription, metadata);
        consumer.subscribe(Collections.singleton(topic));

        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        Uuid memberId = Uuid.randomUuid();
        client.prepareResponseFrom(shareGroupHeartbeatResponse(memberId, 0, Errors.NONE), coordinator);

        consumer.poll(Duration.ZERO);

        Queue<ClientRequest> requests = client.requests();
        assertEquals(0, requests.stream().filter(request -> request.apiKey().equals(ApiKeys.SHARE_FETCH)).count());
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
    public void testFetchResponseWithUnexpectedPartitionIsIgnored() {
        SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
        ConsumerMetadata metadata = createMetadata(subscription);
        Time time = new MockTime();
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        consumer = newShareConsumer(time, client, subscription, metadata);
        consumer.subscribe(Collections.singletonList(topic));

        Uuid memberId = Uuid.randomUuid();
        joinGroupAndGetInitialAssignment(client, node, memberId, 1, Collections.singletonList(tp0), null);

        Map<TopicPartition, ShareFetchInfo> fetches1 = new HashMap<>();
        fetches1.put(tp0, new ShareFetchInfo(0, 40000, 1));
        fetches1.put(t2p0, new ShareFetchInfo(0, 40000, 10)); // not assigned and not fetched
        client.prepareResponseFrom(shareFetchResponse(fetches1), node);

        @SuppressWarnings("unchecked")
        ConsumerRecords<String, String> records = (ConsumerRecords<String, String>) consumer.poll(Duration.ZERO);
        assertEquals(0, records.count());
    }

    @Test
    public void testPollWithNoSubscription() {
        consumer = newShareConsumer(groupId);
        assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ofMillis(2000L)));
    }

    @Test
    public void testPollWithEmptySubscription() {
        consumer = newShareConsumer(groupId);
        consumer.subscribe(Collections.emptyList());
        assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ofMillis(2000L)));
    }

    @Test
    public void testCloseShouldBeIdempotent() {
        SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
        ConsumerMetadata metadata = createMetadata(subscription);
        Time time = new MockTime();
        MockClient client = spy(new MockClient(time, metadata));
        initMetadata(client, Collections.singletonMap(topic, 1));

        consumer = newShareConsumer(time, client, subscription, metadata);

        consumer.close(Duration.ZERO);
        consumer.close(Duration.ZERO);

        // verify that the call is idempotent by checking that the network client is only closed once.
        verify(client).close();
    }

    @Test
    public void testMetricConfigRecordingLevelInfo() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9000");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        KafkaShareConsumer<byte[], byte[]> consumer = newShareConsumer(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertEquals(Sensor.RecordingLevel.INFO, consumer.metricsRegistry().config().recordLevel());
        consumer.close(Duration.ZERO);

        props.put(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        KafkaShareConsumer<byte[], byte[]> consumer2 = newShareConsumer(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        assertEquals(Sensor.RecordingLevel.DEBUG, consumer2.metricsRegistry().config().recordLevel());
        consumer2.close(Duration.ZERO);
    }

    // This test is not reliable and needs more work
    @Disabled
    @Test
    public void testPollAuthenticationFailure() {
        KafkaShareConsumer<String, String> consumer = consumerWithPendingAuthenticationError();
        consumer.subscribe(Collections.singleton(topic));
        assertThrows(AuthenticationException.class, () -> consumer.poll(Duration.ZERO));
    }

    // This test is not reliable and needs more work
    @Disabled
    @Test
    public void testCommitSyncAuthenticationFailure() {
        KafkaShareConsumer<String, String> consumer = consumerWithPendingAuthenticationError();
        assertThrows(AuthenticationException.class, consumer::commitSync);
    }

    private KafkaShareConsumer<String, String> consumerWithPendingAuthenticationError() {
        return consumerWithPendingAuthenticationError(new MockTime());
    }

    private KafkaShareConsumer<String, String> consumerWithPendingAuthenticationError(final Time time) {
        SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
        ConsumerMetadata metadata = createMetadata(subscription);
        MockClient client = new MockClient(time, metadata);

        initMetadata(client, Collections.singletonMap(topic, 1));
        Node node = metadata.fetch().nodes().get(0);

        client.createPendingAuthenticationError(node, 0);
        return newShareConsumer(time, client, subscription, metadata);
    }

    private ConsumerMetadata createMetadata(SubscriptionState subscription) {
        return new ConsumerMetadata(0, 0, Long.MAX_VALUE, false, false,
                subscription, new LogContext(), new ClusterResourceListeners());
    }

    private ShareGroupHeartbeatResponse shareGroupHeartbeatResponse(Uuid memberId, int memberEpoch, List<ShareGroupHeartbeatResponseData.TopicPartitions> topicPartitions, Errors error) {
        return new ShareGroupHeartbeatResponse(
                new ShareGroupHeartbeatResponseData()
                        .setErrorCode(error.code())
                        .setMemberId(memberId.toString())
                        .setMemberEpoch(memberEpoch)
                        .setHeartbeatIntervalMs(30000)
                        .setAssignment(
                                new ShareGroupHeartbeatResponseData.Assignment()
                                        .setTopicPartitions(topicPartitions)));
    }

    private ShareGroupHeartbeatResponse shareGroupHeartbeatResponse(Uuid memberId, int memberEpoch, Errors error) {
        return new ShareGroupHeartbeatResponse(
                new ShareGroupHeartbeatResponseData()
                        .setMemberId(memberId.toString())
                        .setMemberEpoch(memberEpoch)
                        .setHeartbeatIntervalMs(30000)
                        .setErrorCode(error.code()));
    }

    private Node joinGroupAndGetInitialAssignment(MockClient client, Node node, Uuid memberId, int memberEpoch, List<TopicPartition> partitions, Node coordinator) {
        if (coordinator == null) {
            // lookup coordinator
            client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node), node);
            coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        }

        // share group heartbeat
        Map<Uuid, ShareGroupHeartbeatResponseData.TopicPartitions> uuidTopicPartitionsMap = new HashMap<>();
        for (TopicPartition partition : partitions) {
            TopicIdPartition tip = new TopicIdPartition(topicIds.get(partition.topic()), partition);
            ShareGroupHeartbeatResponseData.TopicPartitions responseTopicData =
                    uuidTopicPartitionsMap.computeIfAbsent(tip.topicId(),
                            topicId -> new ShareGroupHeartbeatResponseData.TopicPartitions().setTopicId(topicId));
            responseTopicData.partitions().add(tip.partition());
        }
        List<ShareGroupHeartbeatResponseData.TopicPartitions> topicPartitionsList = new ArrayList<>(uuidTopicPartitionsMap.values());
        client.prepareResponseFrom(shareGroupHeartbeatResponse(memberId, memberEpoch, topicPartitionsList, Errors.NONE), coordinator);

        return coordinator;
    }

    private AtomicBoolean prepareShareGroupHeartbeatResponse(MockClient client, Node coordinator, Uuid memberId, int memberEpoch, Errors error) {
        final AtomicBoolean heartbeatReceived = new AtomicBoolean(false);
        client.prepareResponseFrom(body -> {
            heartbeatReceived.set(true);
            return true;
        }, new ShareGroupHeartbeatResponse(new ShareGroupHeartbeatResponseData()
                .setMemberId(memberId.toString())
                .setMemberEpoch(memberEpoch)
                .setHeartbeatIntervalMs(30000)
                .setErrorCode(error.code())), coordinator);
        return heartbeatReceived;
    }

    private ShareFetchResponse shareFetchResponse(Map<TopicPartition, ShareFetchInfo> fetches) {
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> tpResponses = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, ShareFetchInfo> fetchEntry : fetches.entrySet()) {
            TopicPartition tp = fetchEntry.getKey();
            TopicIdPartition tip = new TopicIdPartition(topicIds.get(tp.topic()), tp);
            long firstOffset = fetchEntry.getValue().firstOffset;
            int count = fetchEntry.getValue().count;
            MemoryRecords records;
            if (count == 0) {
                records = MemoryRecords.EMPTY;
            } else {
                try (MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE,
                        TimestampType.CREATE_TIME, firstOffset)) {
                    for (int i = 0; i < count; i++)
                        builder.append(0L, ("key-" + i).getBytes(), ("value-" + i).getBytes());
                    records = builder.build();
                }
            }
            tpResponses.put(tip,
                    new ShareFetchResponseData.PartitionData()
                            .setPartitionIndex(tip.partition())
                            .setRecords(records)
                            .setAcquiredRecords(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                                    .setFirstOffset(firstOffset)
                                    .setLastOffset(firstOffset + count - 1)
                                    .setDeliveryCount((short) 1))));
        }
        return ShareFetchResponse.of(Errors.NONE, 0, tpResponses, Collections.emptyList());
    }

    private ShareFetchResponse shareFetchResponse(TopicPartition tp, long firstOffset, int count) {
        ShareFetchInfo info = new ShareFetchInfo(firstOffset, 40000, count);
        return shareFetchResponse(Collections.singletonMap(tp, info));
    }

    private ShareFetchResponse shareFetchResponse(List<TopicPartition> partitions, Errors error, Errors acknowledgeError) {
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> tpResponses = new LinkedHashMap<>();
        for (TopicPartition tp : partitions) {
            TopicIdPartition tip = new TopicIdPartition(topicIds.get(tp.topic()), tp);
            final MemoryRecords records = MemoryRecords.EMPTY;
            tpResponses.put(tip,
                    new ShareFetchResponseData.PartitionData()
                            .setPartitionIndex(tip.partition())
                            .setRecords(records)
                            .setErrorCode(error.code())
                            .setAcknowledgeErrorCode(acknowledgeError.code()));
        }
        return ShareFetchResponse.of(Errors.NONE, 0, tpResponses, Collections.emptyList());
    }

    private KafkaShareConsumer<String, String> newShareConsumer(Time time,
                                                                KafkaClient client,
                                                                SubscriptionState subscription,
                                                                ConsumerMetadata metadata) {
        return newShareConsumer(
                time,
                client,
                subscription,
                metadata,
                groupId,
                Optional.of(new StringDeserializer())
        );
    }

    private KafkaShareConsumer<String, String> newShareConsumer(Time time,
                                                                KafkaClient client,
                                                                SubscriptionState subscriptions,
                                                                ConsumerMetadata metadata,
                                                                String groupId,
                                                                Optional<Deserializer<String>> valueDeserializerOpt) {
        String clientId = "mock-consumer";
        Deserializer<String> keyDeserializer = new StringDeserializer();
        Deserializer<String> valueDeserializer = valueDeserializerOpt.orElse(new StringDeserializer());
        LogContext logContext = new LogContext();
        ConsumerConfig config = newConsumerConfig(groupId, valueDeserializer);
        return new KafkaShareConsumer<>(
                logContext,
                clientId,
                groupId,
                config,
                keyDeserializer,
                valueDeserializer,
                time,
                client,
                subscriptions,
                metadata
        );
    }

    private ConsumerConfig newConsumerConfig(String groupId,
                                             Deserializer<String> valueDeserializer) {
        String clientId = "mock-consumer";
        long retryBackoffMs = 100;
        long retryBackoffMaxMs = 1000;
        int minBytes = 1;
        int maxBytes = Integer.MAX_VALUE;
        int maxWaitMs = 500;
        int fetchSize = 1024 * 1024;
        int maxPollRecords = Integer.MAX_VALUE;
        boolean checkCrcs = true;
        int rebalanceTimeoutMs = 60000;
        int defaultApiTimeoutMs = 60000;
        int requestTimeoutMs = defaultApiTimeoutMs / 2;

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.CHECK_CRCS_CONFIG, checkCrcs);
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        configs.put(ConsumerConfig.CLIENT_RACK_CONFIG, CommonClientConfigs.DEFAULT_CLIENT_RACK);
        configs.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutMs);
        configs.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maxBytes);
        configs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, maxWaitMs);
        configs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, minBytes);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, fetchSize);
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, rebalanceTimeoutMs);
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        configs.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        configs.put(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, retryBackoffMaxMs);
        configs.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());

        return new ConsumerConfig(configs);
    }

    private static class ShareFetchInfo {
        long firstOffset;
        int partitionMaxBytes;
        int count;

        ShareFetchInfo(long firstOffset, int partitionMaxBytes, int count) {
            this.firstOffset = firstOffset;
            this.partitionMaxBytes = partitionMaxBytes;
            this.count = count;
        }
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

    @Test
    public void testConfigurableObjectsShouldSeeGeneratedClientId() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DeserializerForClientId.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DeserializerForClientId.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        consumer = newShareConsumer(props);
        assertNotNull(consumer.clientId());
        assertNotEquals(0, consumer.clientId().length());
        assertEquals(2, CLIENT_IDS.size());
        CLIENT_IDS.forEach(id -> assertEquals(id, consumer.clientId()));
    }

    @Test
    public void testUnusedConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLS");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        ConsumerConfig config = new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(props, new StringDeserializer(), new StringDeserializer()));

        assertTrue(config.unused().contains(SslConfigs.SSL_PROTOCOL_CONFIG));

        consumer = new KafkaShareConsumer<>(config, null, null);
        assertTrue(config.unused().contains(SslConfigs.SSL_PROTOCOL_CONFIG));
    }

    @Test
    public void testClientInstanceId() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        ClientTelemetryReporter clientTelemetryReporter = mock(ClientTelemetryReporter.class);
        clientTelemetryReporter.configure(any());

        try (MockedStatic<CommonClientConfigs> mockedCommonClientConfigs = mockStatic(CommonClientConfigs.class, new CallsRealMethods())) {
            mockedCommonClientConfigs.when(() -> CommonClientConfigs.telemetryReporter(anyString(), any())).thenReturn(Optional.of(clientTelemetryReporter));

            ClientTelemetrySender clientTelemetrySender = mock(ClientTelemetrySender.class);
            Uuid expectedUuid = Uuid.randomUuid();
            when(clientTelemetryReporter.telemetrySender()).thenReturn(clientTelemetrySender);
            when(clientTelemetrySender.clientInstanceId(any())).thenReturn(Optional.of(expectedUuid));

            consumer = newShareConsumer(props, new StringDeserializer(), new StringDeserializer());
            Uuid uuid = consumer.clientInstanceId(Duration.ofMillis(0));
            assertEquals(expectedUuid, uuid);
        }
    }

    @Test
    public void testClientInstanceIdInvalidTimeout() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        consumer = newShareConsumer(props, new StringDeserializer(), new StringDeserializer());
        Exception exception = assertThrows(IllegalArgumentException.class, () -> consumer.clientInstanceId(Duration.ofMillis(-1)));
        assertEquals("The timeout cannot be negative.", exception.getMessage());
    }

    @Test
    public void testClientInstanceIdNoTelemetryReporterRegistered() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        props.setProperty(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG, "false");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        consumer = newShareConsumer(props, new StringDeserializer(), new StringDeserializer());
        Exception exception = assertThrows(IllegalStateException.class, () -> consumer.clientInstanceId(Duration.ofMillis(0)));
        assertEquals("Telemetry is not enabled. Set config `enable.metrics.push` to `true`.", exception.getMessage());
    }
}
