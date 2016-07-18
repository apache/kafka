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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaConsumerTest {

    private final String topic = "test";
    private final TopicPartition tp0 = new TopicPartition("test", 0);

    @Test
    public void testConstructorClose() throws Exception {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "some.invalid.hostname.foo.bar:9999");
        props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());

        final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
        final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
        try {
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
                    props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
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

        consumer.subscribe(Collections.singletonList(topic));
        assertEquals(singleton(topic), consumer.subscription());
        assertTrue(consumer.assignment().isEmpty());

        consumer.subscribe(Collections.<String>emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());

        consumer.assign(Collections.singletonList(tp0));
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
            consumer.subscribe(Collections.singletonList(nullTopic));
        } finally {
            consumer.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubscriptionOnEmptyTopic() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        String emptyTopic = "  ";

        try {
            consumer.subscribe(Collections.singletonList(emptyTopic));
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
        } finally {
            // cleanup since we are using mutable static variables in MockConsumerInterceptor
            MockConsumerInterceptor.resetCounters();
        }
    }

    @Test
    public void testPause() {
        KafkaConsumer<byte[], byte[]> consumer = newConsumer();

        consumer.assign(Collections.singletonList(tp0));
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

        return new KafkaConsumer<byte[], byte[]>(
            props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Test
    public void verifyHeartbeatSent() {
        String topic = "topic";
        TopicPartition partition = new TopicPartition(topic, 0);

        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;
        int autoCommitIntervalMs = 10000;

        Time time = new MockTime();
        MockClient client = new MockClient(time);
        Cluster cluster = TestUtils.singletonCluster(topic, 1);
        Node node = cluster.nodes().get(0);
        client.setNode(node);
        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());
        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                sessionTimeoutMs, heartbeatIntervalMs, autoCommitIntervalMs);

        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // set initial position so we don't need a lookup
                for (TopicPartition partition : partitions)
                    consumer.seek(partition, 0);
            }
        });

        // lookup coordinator
        client.prepareResponseFrom(new GroupCoordinatorResponse(Errors.NONE.code(), node).toStruct(), node);

        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // join group
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, "memberId", "leaderId", Errors.NONE.code()), coordinator);

        // sync group
        client.prepareResponseFrom(syncGroupResponse(Arrays.asList(partition), Errors.NONE.code()), coordinator);

        // initial fetch
        client.prepareResponseFrom(fetchResponse(partition, 0, 0), node);

        consumer.poll(0);
        assertEquals(Collections.singleton(partition), consumer.assignment());

        // heartbeat interval is 2 seconds
        time.sleep(heartbeatIntervalMs);

        final AtomicBoolean heartbeatReceived = new AtomicBoolean(false);
        client.prepareResponseFrom(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                heartbeatReceived.set(true);
                return true;
            }
        }, new HeartbeatResponse(Errors.NONE.code()).toStruct(), coordinator);

        consumer.poll(0);

        assertTrue(heartbeatReceived.get());
    }

    @Test
    public void verifyHeartbeatSentWhenFetchedDataReady() {
        String topic = "topic";
        TopicPartition partition = new TopicPartition(topic, 0);

        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;
        int autoCommitIntervalMs = 10000;

        Time time = new MockTime();
        MockClient client = new MockClient(time);
        Cluster cluster = TestUtils.singletonCluster(topic, 1);
        Node node = cluster.nodes().get(0);
        client.setNode(node);
        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());
        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                sessionTimeoutMs, heartbeatIntervalMs, autoCommitIntervalMs);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // set initial position so we don't need a lookup
                for (TopicPartition partition : partitions)
                    consumer.seek(partition, 0);
            }
        });

        // lookup coordinator
        client.prepareResponseFrom(new GroupCoordinatorResponse(Errors.NONE.code(), node).toStruct(), node);

        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // join group
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, "memberId", "leaderId", Errors.NONE.code()), coordinator);

        // sync group
        client.prepareResponseFrom(syncGroupResponse(Arrays.asList(partition), Errors.NONE.code()), coordinator);

        consumer.poll(0);

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(fetchResponse(partition, 0, 5), node);
        client.poll(0, time.milliseconds());

        time.sleep(heartbeatIntervalMs);

        client.prepareResponseFrom(fetchResponse(partition, 5, 0), node);
        final AtomicBoolean heartbeatReceived = new AtomicBoolean(false);
        client.prepareResponseFrom(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                heartbeatReceived.set(true);
                return true;
            }
        }, new HeartbeatResponse(Errors.NONE.code()).toStruct(), coordinator);

        consumer.poll(0);

        assertTrue(heartbeatReceived.get());
    }

    @Test
    public void testAutoCommitSentBeforePositionUpdate() {
        String topic = "topic";
        final TopicPartition partition = new TopicPartition(topic, 0);

        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;

        // adjust auto commit interval lower than heartbeat so we don't need to deal with
        // a concurrent heartbeat request
        int autoCommitIntervalMs = 1000;

        Time time = new MockTime();
        MockClient client = new MockClient(time);
        Cluster cluster = TestUtils.singletonCluster(topic, 1);
        Node node = cluster.nodes().get(0);
        client.setNode(node);
        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());
        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                sessionTimeoutMs, heartbeatIntervalMs, autoCommitIntervalMs);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // set initial position so we don't need a lookup
                for (TopicPartition partition : partitions)
                    consumer.seek(partition, 0);
            }
        });

        // lookup coordinator
        client.prepareResponseFrom(new GroupCoordinatorResponse(Errors.NONE.code(), node).toStruct(), node);

        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // join group
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, "memberId", "leaderId", Errors.NONE.code()), coordinator);

        // sync group
        client.prepareResponseFrom(syncGroupResponse(Arrays.asList(partition), Errors.NONE.code()), coordinator);

        consumer.poll(0);

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(fetchResponse(partition, 0, 5), node);
        client.poll(0, time.milliseconds());

        time.sleep(autoCommitIntervalMs);

        client.prepareResponseFrom(fetchResponse(partition, 5, 0), node);

        // no data has been returned to the user yet, so the committed offset should be 0
        final AtomicBoolean commitReceived = new AtomicBoolean(false);
        client.prepareResponseFrom(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                OffsetCommitRequest commitRequest = new OffsetCommitRequest(request.request().body());
                OffsetCommitRequest.PartitionData partitionData = commitRequest.offsetData().get(partition);
                if (partitionData.offset == 0) {
                    commitReceived.set(true);
                    return true;
                }
                return false;
            }
        }, new OffsetCommitResponse(Collections.singletonMap(partition, Errors.NONE.code())).toStruct(), coordinator);

        consumer.poll(0);

        assertTrue(commitReceived.get());
    }

    @Test
    public void testWakeupWithFetchDataAvailable() {
        String topic = "topic";
        final TopicPartition partition = new TopicPartition(topic, 0);

        int sessionTimeoutMs = 30000;
        int heartbeatIntervalMs = 3000;

        // adjust auto commit interval lower than heartbeat so we don't need to deal with
        // a concurrent heartbeat request
        int autoCommitIntervalMs = 1000;

        Time time = new MockTime();
        MockClient client = new MockClient(time);
        Cluster cluster = TestUtils.singletonCluster(topic, 1);
        Node node = cluster.nodes().get(0);
        client.setNode(node);
        Metadata metadata = new Metadata(0, Long.MAX_VALUE);
        metadata.update(cluster, time.milliseconds());
        PartitionAssignor assignor = new RoundRobinAssignor();

        final KafkaConsumer<String, String> consumer = newConsumer(time, client, metadata, assignor,
                sessionTimeoutMs, heartbeatIntervalMs, autoCommitIntervalMs);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // set initial position so we don't need a lookup
                for (TopicPartition partition : partitions)
                    consumer.seek(partition, 0);
            }
        });

        // lookup coordinator
        client.prepareResponseFrom(new GroupCoordinatorResponse(Errors.NONE.code(), node).toStruct(), node);

        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        // join group
        client.prepareResponseFrom(joinGroupFollowerResponse(assignor, 1, "memberId", "leaderId", Errors.NONE.code()), coordinator);

        // sync group
        client.prepareResponseFrom(syncGroupResponse(Arrays.asList(partition), Errors.NONE.code()), coordinator);

        consumer.poll(0);

        // respond to the outstanding fetch so that we have data available on the next poll
        client.respondFrom(fetchResponse(partition, 0, 5), node);
        client.poll(0, time.milliseconds());

        consumer.wakeup();

        try {
            consumer.poll(0);
            fail();
        } catch (WakeupException e) {
        }

        // make sure the position hasn't been updated
        assertEquals(0, consumer.position(partition));

        // the next poll should return the completed fetch
        ConsumerRecords<String, String> records = consumer.poll(0);
        assertEquals(5, records.count());
    }

    private Struct joinGroupFollowerResponse(PartitionAssignor assignor, int generationId, String memberId, String leaderId, short error) {
        return new JoinGroupResponse(error, generationId, assignor.name(), memberId, leaderId,
                Collections.<String, ByteBuffer>emptyMap()).toStruct();
    }

    private Struct syncGroupResponse(List<TopicPartition> partitions, short error) {
        ByteBuffer buf = ConsumerProtocol.serializeAssignment(new PartitionAssignor.Assignment(partitions));
        return new SyncGroupResponse(error, buf).toStruct();
    }

    private Struct fetchResponse(TopicPartition tp, long fetchOffset, int count) {
        MemoryRecords records = MemoryRecords.emptyRecords(ByteBuffer.allocate(1024), CompressionType.NONE);
        for (int i = 0; i < count; i++)
            records.append(fetchOffset + i, 0L, ("key-" + i).getBytes(), ("value-" + i).getBytes());
        records.close();
        FetchResponse response = new FetchResponse(Collections.singletonMap(
                tp, new FetchResponse.PartitionData(Errors.NONE.code(), 5, records.buffer())), 0);
        return response.toStruct();
    }

    private KafkaConsumer<String, String> newConsumer(Time time,
                                                      KafkaClient client,
                                                      Metadata metadata,
                                                      PartitionAssignor assignor,
                                                      int sessionTimeoutMs,
                                                      int heartbeatIntervalMs,
                                                      int autoCommitIntervalMs) {
        // create a consumer with mocked time and mocked network

        String clientId = "mock-consumer";
        String groupId = "mock-group";
        String metricGroupPrefix = "consumer";
        long retryBackoffMs = 100;
        long requestTimeoutMs = 30000;
        boolean autoCommitEnabled = true;
        boolean excludeInternalTopics = true;
        int minBytes = 1;
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
        ConsumerMetrics metricsRegistry = new ConsumerMetrics(metricGroupPrefix);

        SubscriptionState subscriptions = new SubscriptionState(autoResetStrategy);
        ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(client, metadata, time, retryBackoffMs, requestTimeoutMs);
        ConsumerCoordinator consumerCoordinator = new ConsumerCoordinator(
                consumerClient,
                groupId,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                assignors,
                metadata,
                subscriptions,
                metrics,
                metricsRegistry.consumerCoordinatorMetrics,
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
}
