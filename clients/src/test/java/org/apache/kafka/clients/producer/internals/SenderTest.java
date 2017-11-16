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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.DelayedReceive;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SenderTest {

    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final String CLIENT_ID = "clientId";
    private static final double EPS = 0.0001;
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 1000;

    private TopicPartition tp0 = new TopicPartition("test", 0);
    private TopicPartition tp1 = new TopicPartition("test", 1);
    private MockTime time = new MockTime();
    private MockClient client = new MockClient(time);
    private int batchSize = 16 * 1024;
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE, true, true, new ClusterResourceListeners());
    private ApiVersions apiVersions = new ApiVersions();
    private Cluster cluster = TestUtils.singletonCluster("test", 2);
    private Metrics metrics = null;
    private RecordAccumulator accumulator = null;
    private Sender sender = null;
    private SenderMetricsRegistry senderMetricsRegistry = null;
    private final LogContext logContext = new LogContext();

    @Before
    public void setup() {
        client.setNode(cluster.nodes().get(0));
        setupWithTransactionState(null);
    }

    @After
    public void tearDown() {
        this.metrics.close();
    }

    @Test
    public void testSimple() throws Exception {
        long offset = 0;
        Future<RecordMetadata> future = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds()); // connect
        sender.run(time.milliseconds()); // send produce request
        assertEquals("We should have a single produce request in flight.", 1, client.inFlightRequestCount());
        assertTrue(client.hasInFlightRequests());
        client.respond(produceResponse(tp0, offset, Errors.NONE, 0));
        sender.run(time.milliseconds());
        assertEquals("All requests completed.", 0, client.inFlightRequestCount());
        assertFalse(client.hasInFlightRequests());
        sender.run(time.milliseconds());
        assertTrue("Request should be completed", future.isDone());
        assertEquals(offset, future.get().offset());
    }

    @Test
    public void testMessageFormatDownConversion() throws Exception {
        // this test case verifies the behavior when the version of the produce request supported by the
        // broker changes after the record set is created

        long offset = 0;

        // start off support produce request v3
        apiVersions.update("0", NodeApiVersions.create());

        Future<RecordMetadata> future = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;

        // now the partition leader supports only v2
        apiVersions.update("0", NodeApiVersions.create(Collections.singleton(
                new ApiVersionsResponse.ApiVersion(ApiKeys.PRODUCE.id, (short) 0, (short) 2))));

        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                ProduceRequest request = (ProduceRequest) body;
                if (request.version() != 2)
                    return false;

                MemoryRecords records = request.partitionRecordsOrFail().get(tp0);
                return records != null &&
                        records.sizeInBytes() > 0 &&
                        records.hasMatchingMagic(RecordBatch.MAGIC_VALUE_V1);
            }
        }, produceResponse(tp0, offset, Errors.NONE, 0));

        sender.run(time.milliseconds()); // connect
        sender.run(time.milliseconds()); // send produce request

        assertTrue("Request should be completed", future.isDone());
        assertEquals(offset, future.get().offset());
    }

    @Test
    public void testDownConversionForMismatchedMagicValues() throws Exception {
        // it can happen that we construct a record set with mismatching magic values (perhaps
        // because the partition leader changed after the record set was initially constructed)
        // in this case, we down-convert record sets with newer magic values to match the oldest
        // created record set

        long offset = 0;

        // start off support produce request v3
        apiVersions.update("0", NodeApiVersions.create());

        Future<RecordMetadata> future1 = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;

        // now the partition leader supports only v2
        apiVersions.update("0", NodeApiVersions.create(Collections.singleton(
                new ApiVersionsResponse.ApiVersion(ApiKeys.PRODUCE.id, (short) 0, (short) 2))));

        Future<RecordMetadata> future2 = accumulator.append(tp1, 0L, "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;

        // start off support produce request v3
        apiVersions.update("0", NodeApiVersions.create());

        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(Errors.NONE, offset, RecordBatch.NO_TIMESTAMP, 100);
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = new HashMap<>();
        partResp.put(tp0, resp);
        partResp.put(tp1, resp);
        ProduceResponse produceResponse = new ProduceResponse(partResp, 0);

        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                ProduceRequest request = (ProduceRequest) body;
                if (request.version() != 2)
                    return false;

                Map<TopicPartition, MemoryRecords> recordsMap = request.partitionRecordsOrFail();
                if (recordsMap.size() != 2)
                    return false;

                for (MemoryRecords records : recordsMap.values()) {
                    if (records == null || records.sizeInBytes() == 0 || !records.hasMatchingMagic(RecordBatch.MAGIC_VALUE_V1))
                        return false;
                }
                return true;
            }
        }, produceResponse);

        sender.run(time.milliseconds()); // connect
        sender.run(time.milliseconds()); // send produce request

        assertTrue("Request should be completed", future1.isDone());
        assertTrue("Request should be completed", future2.isDone());
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    @SuppressWarnings("deprecation")
    public void testQuotaMetrics() throws Exception {
        MockSelector selector = new MockSelector(time);
        Sensor throttleTimeSensor = Sender.throttleTimeSensor(this.senderMetricsRegistry);
        Cluster cluster = TestUtils.singletonCluster("test", 1);
        Node node = cluster.nodes().get(0);
        NetworkClient client = new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                1000, 1000, 64 * 1024, 64 * 1024, 1000,
                time, true, new ApiVersions(), throttleTimeSensor, logContext);

        short apiVersionsResponseVersion = ApiKeys.API_VERSIONS.latestVersion();
        ByteBuffer buffer = ApiVersionsResponse.createApiVersionsResponse(400, RecordBatch.CURRENT_MAGIC_VALUE).serialize(apiVersionsResponseVersion, new ResponseHeader(0));
        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
        while (!client.ready(node, time.milliseconds()))
            client.poll(1, time.milliseconds());
        selector.clear();

        for (int i = 1; i <= 3; i++) {
            int throttleTimeMs = 100 * i;
            ProduceRequest.Builder builder = ProduceRequest.Builder.forCurrentMagic((short) 1, 1000,
                            Collections.<TopicPartition, MemoryRecords>emptyMap());
            ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true, null);
            client.send(request, time.milliseconds());
            client.poll(1, time.milliseconds());
            ProduceResponse response = produceResponse(tp0, i, Errors.NONE, throttleTimeMs);
            buffer = response.serialize(ApiKeys.PRODUCE.latestVersion(), new ResponseHeader(request.correlationId()));
            selector.completeReceive(new NetworkReceive(node.idString(), buffer));
            client.poll(1, time.milliseconds());
            selector.clear();
        }
        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric avgMetric = allMetrics.get(this.senderMetricsRegistry.produceThrottleTimeAvg);
        KafkaMetric maxMetric = allMetrics.get(this.senderMetricsRegistry.produceThrottleTimeMax);
        // Throttle times are ApiVersions=400, Produce=(100, 200, 300)
        assertEquals(250, avgMetric.value(), EPS);
        assertEquals(400, maxMetric.value(), EPS);
        client.close();
    }

    @Test
    public void testSenderMetricsTemplates() throws Exception {
        metrics.close();
        Map<String, String> clientTags = Collections.singletonMap("client-id", "clientA");
        metrics = new Metrics(new MetricConfig().tags(clientTags));
        SenderMetricsRegistry metricsRegistry = new SenderMetricsRegistry(metrics);
        Sender sender = new Sender(logContext, client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                1, metricsRegistry, time, REQUEST_TIMEOUT, 50, null, apiVersions);

        // Append a message so that topic metrics are created
        accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT);
        sender.run(time.milliseconds()); // connect
        sender.run(time.milliseconds()); // send produce request
        client.respond(produceResponse(tp0, 0, Errors.NONE, 0));
        sender.run(time.milliseconds());
        // Create throttle time metrics
        Sender.throttleTimeSensor(metricsRegistry);

        // Verify that all metrics except metrics-count have registered templates
        Set<MetricNameTemplate> allMetrics = new HashSet<>();
        for (MetricName n : metrics.metrics().keySet()) {
            if (!n.group().equals("kafka-metrics-count"))
                allMetrics.add(new MetricNameTemplate(n.name(), n.group(), "", n.tags().keySet()));
        }
        TestUtils.checkEquals(allMetrics, new HashSet<>(metricsRegistry.allTemplates()), "metrics", "templates");
    }

    @Test
    public void testRetries() throws Exception {
        // create a sender with retries = 1
        int maxRetries = 1;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        try {
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                    maxRetries, senderMetrics, time, REQUEST_TIMEOUT, 50, null, apiVersions);
            // do a successful retry
            Future<RecordMetadata> future = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
            sender.run(time.milliseconds()); // connect
            sender.run(time.milliseconds()); // send produce request
            String id = client.requests().peek().destination();
            Node node = new Node(Integer.parseInt(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));
            client.disconnect(id);
            assertEquals(0, client.inFlightRequestCount());
            assertFalse(client.hasInFlightRequests());
            assertFalse("Client ready status should be false", client.isReady(node, 0L));
            sender.run(time.milliseconds()); // receive error
            sender.run(time.milliseconds()); // reconnect
            sender.run(time.milliseconds()); // resend
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            long offset = 0;
            client.respond(produceResponse(tp0, offset, Errors.NONE, 0));
            sender.run(time.milliseconds());
            assertTrue("Request should have retried and completed", future.isDone());
            assertEquals(offset, future.get().offset());

            // do an unsuccessful retry
            future = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
            sender.run(time.milliseconds()); // send produce request
            for (int i = 0; i < maxRetries + 1; i++) {
                client.disconnect(client.requests().peek().destination());
                sender.run(time.milliseconds()); // receive error
                sender.run(time.milliseconds()); // reconnect
                sender.run(time.milliseconds()); // resend
            }
            sender.run(time.milliseconds());
            assertFutureFailure(future, NetworkException.class);
        } finally {
            m.close();
        }
    }

    @Test
    public void testSendInOrder() throws Exception {
        int maxRetries = 1;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        try {
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                    senderMetrics, time, REQUEST_TIMEOUT, 50, null, apiVersions);
            // Create a two broker cluster, with partition 0 on broker 0 and partition 1 on broker 1
            Cluster cluster1 = TestUtils.clusterWith(2, "test", 2);
            metadata.update(cluster1, Collections.<String>emptySet(), time.milliseconds());

            // Send the first message.
            TopicPartition tp2 = new TopicPartition("test", 1);
            accumulator.append(tp2, 0L, "key1".getBytes(), "value1".getBytes(), null, null, MAX_BLOCK_TIMEOUT);
            sender.run(time.milliseconds()); // connect
            sender.run(time.milliseconds()); // send produce request
            String id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            Node node = new Node(Integer.parseInt(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));

            time.sleep(900);
            // Now send another message to tp2
            accumulator.append(tp2, 0L, "key2".getBytes(), "value2".getBytes(), null, null, MAX_BLOCK_TIMEOUT);

            // Update metadata before sender receives response from broker 0. Now partition 2 moves to broker 0
            Cluster cluster2 = TestUtils.singletonCluster("test", 2);
            metadata.update(cluster2, Collections.<String>emptySet(), time.milliseconds());
            // Sender should not send the second message to node 0.
            sender.run(time.milliseconds());
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
        } finally {
            m.close();
        }
    }

    @Test
    public void testAppendInExpiryCallback() throws InterruptedException {
        int messagesPerBatch = 10;
        final AtomicInteger expiryCallbackCount = new AtomicInteger(0);
        final AtomicReference<Exception> unexpectedException = new AtomicReference<>();
        final byte[] key = "key".getBytes();
        final byte[] value = "value".getBytes();
        final long maxBlockTimeMs = 1000;
        Callback callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception instanceof TimeoutException) {
                    expiryCallbackCount.incrementAndGet();
                    try {
                        accumulator.append(tp1, 0L, key, value, Record.EMPTY_HEADERS, null, maxBlockTimeMs);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Unexpected interruption", e);
                    }
                } else if (exception != null)
                    unexpectedException.compareAndSet(null, exception);
            }
        };

        for (int i = 0; i < messagesPerBatch; i++)
            accumulator.append(tp1, 0L, key, value, null, callback, maxBlockTimeMs);

        // Advance the clock to expire the first batch.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = this.cluster.nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.blackout(clusterNode, 100);

        sender.run(time.milliseconds());  // We should try to flush the batch, but we expire it instead without sending anything.

        assertEquals("Callbacks not invoked for expiry", messagesPerBatch, expiryCallbackCount.get());
        assertNull("Unexpected exception", unexpectedException.get());
        // Make sure that the reconds were appended back to the batch.
        assertTrue(accumulator.batches().containsKey(tp1));
        assertEquals(1, accumulator.batches().get(tp1).size());
        assertEquals(messagesPerBatch, accumulator.batches().get(tp1).peekFirst().recordCount);
    }

    /**
     * Tests that topics are added to the metadata list when messages are available to send
     * and expired if not used during a metadata refresh interval.
     */
    @Test
    public void testMetadataTopicExpiry() throws Exception {
        long offset = 0;
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time.milliseconds());

        Future<RecordMetadata> future = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertTrue("Topic not added to metadata", metadata.containsTopic(tp0.topic()));
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());
        sender.run(time.milliseconds());  // send produce request
        client.respond(produceResponse(tp0, offset++, Errors.NONE, 0));
        sender.run(time.milliseconds());
        assertEquals("Request completed.", 0, client.inFlightRequestCount());
        assertFalse(client.hasInFlightRequests());
        sender.run(time.milliseconds());
        assertTrue("Request should be completed", future.isDone());

        assertTrue("Topic not retained in metadata list", metadata.containsTopic(tp0.topic()));
        time.sleep(Metadata.TOPIC_EXPIRY_MS);
        metadata.update(Cluster.empty(), Collections.<String>emptySet(), time.milliseconds());
        assertFalse("Unused topic has not been expired", metadata.containsTopic(tp0.topic()));
        future = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertTrue("Topic not added to metadata", metadata.containsTopic(tp0.topic()));
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());
        sender.run(time.milliseconds());  // send produce request
        client.respond(produceResponse(tp0, offset++, Errors.NONE, 0));
        sender.run(time.milliseconds());
        assertEquals("Request completed.", 0, client.inFlightRequestCount());
        assertFalse(client.hasInFlightRequests());
        sender.run(time.milliseconds());
        assertTrue("Request should be completed", future.isDone());
    }

    @Test
    public void testInitProducerIdRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        client.setNode(new Node(1, "localhost", 33343));
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());
        assertEquals(producerId, transactionManager.producerIdAndEpoch().producerId);
        assertEquals((short) 0, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testClusterAuthorizationExceptionInInitProducerIdRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        client.setNode(new Node(1, "localhost", 33343));
        prepareAndReceiveInitProducerId(producerId, Errors.CLUSTER_AUTHORIZATION_FAILED);
        assertFalse(transactionManager.hasProducerId());
        assertTrue(transactionManager.hasError());
        assertTrue(transactionManager.lastError() instanceof ClusterAuthorizationException);

        // cluster authorization is a fatal error for the producer
        assertSendFailure(ClusterAuthorizationException.class);
    }

    @Test
    public void testCanRetryWithoutIdempotence() throws Exception {
        // do a successful retry
        Future<RecordMetadata> future = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds()); // connect
        sender.run(time.milliseconds()); // send produce request
        String id = client.requests().peek().destination();
        Node node = new Node(Integer.parseInt(id), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertTrue(client.hasInFlightRequests());
        assertTrue("Client ready status should be true", client.isReady(node, 0L));
        assertFalse(future.isDone());

        client.respond(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                ProduceRequest request = (ProduceRequest) body;
                assertFalse(request.isIdempotent());
                return true;
            }
        }, produceResponse(tp0, -1L, Errors.TOPIC_AUTHORIZATION_FAILED, 0));
        sender.run(time.milliseconds());
        assertTrue(future.isDone());
        try {
            future.get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof TopicAuthorizationException);
        }
    }

    @Test
    public void testIdempotenceWithMultipleInflights() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);

        sender.run(time.milliseconds()); // receive response 0

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());
        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1L);
        sender.run(time.milliseconds()); // receive response 1
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertFalse(client.hasInFlightRequests());
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());
    }


    @Test
    public void testIdempotenceWithMultipleInflightsRetriedInOrder() throws Exception {
        // Send multiple in flight requests, retry them all one at a time, in the correct order.
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());

         // Send third ProduceRequest
        Future<RecordMetadata> request3 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());

        assertEquals(3, client.inFlightRequestCount());
        assertEquals(3, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertFalse(request3.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        sendIdempotentProducerResponse(0, tp0, Errors.LEADER_NOT_AVAILABLE, -1L);
        sender.run(time.milliseconds()); // receive response 0

        // Queue the fourth request, it shouldn't be sent until the first 3 complete.
        Future<RecordMetadata> request4 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;

        assertEquals(2, client.inFlightRequestCount());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(1, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);
        sender.run(time.milliseconds()); // re send request 1, receive response 2

        sendIdempotentProducerResponse(2, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);
        sender.run(time.milliseconds()); // receive response 3

        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(1, client.inFlightRequestCount());

        sender.run(time.milliseconds()); // Do nothing, we are reduced to one in flight request during retries.

        assertEquals(3, transactionManager.sequenceNumber(tp0).longValue());  // the batch for request 4 shouldn't have been drained, and hence the sequence should not have been incremented.
        assertEquals(1, client.inFlightRequestCount());

        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.run(time.milliseconds());  // receive response 1
        assertEquals(0, transactionManager.lastAckedSequence(tp0));
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());


        assertFalse(client.hasInFlightRequests());
        sender.run(time.milliseconds()); // send request 2;
        assertEquals(1, client.inFlightRequestCount());

        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1L);
        sender.run(time.milliseconds());  // receive response 2
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());

        assertFalse(client.hasInFlightRequests());

        sender.run(time.milliseconds()); // send request 3
        assertEquals(1, client.inFlightRequestCount());

        sendIdempotentProducerResponse(2, tp0, Errors.NONE, 2L);
        sender.run(time.milliseconds());  // receive response 3, send request 4 since we are out of 'retry' mode.
        assertEquals(2, transactionManager.lastAckedSequence(tp0));
        assertTrue(request3.isDone());
        assertEquals(2, request3.get().offset());

        assertEquals(1, client.inFlightRequestCount());

        sendIdempotentProducerResponse(3, tp0, Errors.NONE, 3L);
        sender.run(time.milliseconds());  // receive response 4
        assertEquals(3, transactionManager.lastAckedSequence(tp0));
        assertTrue(request4.isDone());
        assertEquals(3, request4.get().offset());
    }

    @Test
    public void testIdempotenceWithMultipleInflightsWhereFirstFailsFatallyAndSequenceOfFutureBatchesIsAdjusted() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        sendIdempotentProducerResponse(0, tp0, Errors.MESSAGE_TOO_LARGE, -1L);

        sender.run(time.milliseconds()); // receive response 0, should adjust sequences of future batches.
        assertFutureFailure(request1, RecordTooLargeException.class);

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(1, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);

        sender.run(time.milliseconds()); // receive response 1

        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());

        sender.run(time.milliseconds()); // resend request 1

        assertEquals(1, client.inFlightRequestCount());

        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.run(time.milliseconds());  // receive response 1
        assertEquals(0, transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());

        assertTrue(request1.isDone());
        assertEquals(0, request2.get().offset());
    }

    @Test
    public void testMustNotRetryOutOfOrderSequenceForNextBatch() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest with multiple messages.
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT);
        sender.run(time.milliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());

        // make sure the next sequence number accounts for multi-message batches.
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0);

        sender.run(time.milliseconds());

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(3, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        // This OutOfOrderSequence is fatal since it is returned for the batch succeeding the last acknowledged batch.
        sendIdempotentProducerResponse(2, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);

        sender.run(time.milliseconds());
        assertFutureFailure(request2, OutOfOrderSequenceException.class);
    }

    @Test
    public void testCorrectHandlingOfOutOfOrderResponses() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        ClientRequest firstClientRequest = client.requests().peek();
        ClientRequest secondClientRequest = (ClientRequest) client.requests().toArray()[1];

        client.respondToRequest(secondClientRequest, produceResponse(tp0, -1, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1));

        sender.run(time.milliseconds()); // receive response 1
        Deque<ProducerBatch> queuedBatches = accumulator.batches().get(tp0);

        // Make sure that we are queueing the second batch first.
        assertEquals(1, queuedBatches.size());
        assertEquals(1, queuedBatches.peekFirst().baseSequence());
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        client.respondToRequest(firstClientRequest, produceResponse(tp0, -1, Errors.NOT_LEADER_FOR_PARTITION, -1));

        sender.run(time.milliseconds()); // receive response 0

        // Make sure we requeued both batches in the correct order.
        assertEquals(2, queuedBatches.size());
        assertEquals(0, queuedBatches.peekFirst().baseSequence());
        assertEquals(1, queuedBatches.peekLast().baseSequence());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());

        sender.run(time.milliseconds()); // send request 0
        assertEquals(1, client.inFlightRequestCount());
        sender.run(time.milliseconds()); // don't do anything, only one inflight allowed once we are retrying.

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Make sure that the requests are sent in order, even though the previous responses were not in order.
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.run(time.milliseconds());  // receive response 0
        assertEquals(0, transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());

        sender.run(time.milliseconds()); // send request 1
        assertEquals(1, client.inFlightRequestCount());
        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1L);
        sender.run(time.milliseconds());  // receive response 1

        assertFalse(client.hasInFlightRequests());
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());
    }

    @Test
    public void testCorrectHandlingOfOutOfOrderResponsesWhenSecondSucceeds() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertEquals(2, client.inFlightRequestCount());
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        ClientRequest firstClientRequest = client.requests().peek();
        ClientRequest secondClientRequest = (ClientRequest) client.requests().toArray()[1];

        client.respondToRequest(secondClientRequest, produceResponse(tp0, 1, Errors.NONE, 1));

        sender.run(time.milliseconds()); // receive response 1
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());
        assertFalse(request1.isDone());
        Deque<ProducerBatch> queuedBatches = accumulator.batches().get(tp0);

        assertEquals(0, queuedBatches.size());
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.lastAckedSequence(tp0));

        client.respondToRequest(firstClientRequest, produceResponse(tp0, -1, Errors.REQUEST_TIMED_OUT, -1));

        sender.run(time.milliseconds()); // receive response 0

        // Make sure we requeued both batches in the correct order.
        assertEquals(1, queuedBatches.size());
        assertEquals(0, queuedBatches.peekFirst().baseSequence());
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());

        sender.run(time.milliseconds()); // resend request 0
        assertEquals(1, client.inFlightRequestCount());

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.lastAckedSequence(tp0));

        // Make sure we handle the out of order successful responses correctly.
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.run(time.milliseconds());  // receive response 0
        assertEquals(0, queuedBatches.size());
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());

        assertFalse(client.hasInFlightRequests());
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());
    }

    @Test
    public void testExpiryOfUnsentBatchesShouldNotCauseUnresolvedSequences() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        Node node = this.cluster.nodes().get(0);
        time.sleep(10000L);
        client.disconnect(node.idString());
        client.blackout(node, 10);

        sender.run(time.milliseconds());

        assertFutureFailure(request1, TimeoutException.class);
        assertFalse(transactionManager.hasUnresolvedSequence(tp0));
    }

    @Test
    public void testExpiryOfFirstBatchShouldNotCauseUnresolvedSequencesIfFutureBatchesSucceed() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());  // send request

        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());  // send request

        assertEquals(2, client.inFlightRequestCount());

        sendIdempotentProducerResponse(0, tp0, Errors.REQUEST_TIMED_OUT, -1);
        sender.run(time.milliseconds());  // receive first response

        Node node = this.cluster.nodes().get(0);
        time.sleep(10000L);
        client.disconnect(node.idString());
        client.blackout(node, 10);

        sender.run(time.milliseconds()); // now expire the first batch.
        assertFutureFailure(request1, TimeoutException.class);
        assertTrue(transactionManager.hasUnresolvedSequence(tp0));
        // let's enqueue another batch, which should not be dequeued until the unresolved state is clear.
        Future<RecordMetadata> request3 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;

        time.sleep(20);

        assertFalse(request2.isDone());

        sender.run(time.milliseconds());  // send second request
        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1);
        sender.run(time.milliseconds()); // receive second response, the third request shouldn't be sent since we are in an unresolved state.
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());
        Deque<ProducerBatch> batches = accumulator.batches().get(tp0);

        assertEquals(1, batches.size());
        assertFalse(batches.peekFirst().hasSequence());
        assertFalse(client.hasInFlightRequests());
        assertEquals(2L, transactionManager.sequenceNumber(tp0).longValue());
        assertTrue(transactionManager.hasUnresolvedSequence(tp0));

        sender.run(time.milliseconds());  // clear the unresolved state, send the pending request.
        assertFalse(transactionManager.hasUnresolvedSequence(tp0));
        assertTrue(transactionManager.hasProducerId());
        assertEquals(0, batches.size());
        assertEquals(1, client.inFlightRequestCount());
        assertFalse(request3.isDone());
    }

    @Test
    public void testExpiryOfFirstBatchShouldCauseResetIfFutureBatchesFail() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());  // send request

        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());  // send request

        assertEquals(2, client.inFlightRequestCount());

        sendIdempotentProducerResponse(0, tp0, Errors.NOT_LEADER_FOR_PARTITION, -1);
        sender.run(time.milliseconds());  // receive first response

        Node node = this.cluster.nodes().get(0);
        time.sleep(10000L);
        client.disconnect(node.idString());
        client.blackout(node, 10);

        sender.run(time.milliseconds()); // now expire the first batch.
        assertFutureFailure(request1, TimeoutException.class);
        assertTrue(transactionManager.hasUnresolvedSequence(tp0));
        // let's enqueue another batch, which should not be dequeued until the unresolved state is clear.
        Future<RecordMetadata> request3 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;

        time.sleep(20);

        assertFalse(request2.isDone());

        sender.run(time.milliseconds());  // send second request
        sendIdempotentProducerResponse(1, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, 1);
        sender.run(time.milliseconds()); // receive second response, the third request shouldn't be sent since we are in an unresolved state.
        assertFutureFailure(request2, OutOfOrderSequenceException.class);

        Deque<ProducerBatch> batches = accumulator.batches().get(tp0);

        // The second request should not be requeued.
        assertEquals(1, batches.size());
        assertFalse(batches.peekFirst().hasSequence());
        assertFalse(client.hasInFlightRequests());

        // The producer state should be reset.
        assertFalse(transactionManager.hasProducerId());
        assertFalse(transactionManager.hasUnresolvedSequence(tp0));
    }

    @Test
    public void testExpiryOfAllSentBatchesShouldCauseUnresolvedSequences() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());  // send request
        sendIdempotentProducerResponse(0, tp0, Errors.NOT_LEADER_FOR_PARTITION, -1);
        sender.run(time.milliseconds());  // receive response

        assertEquals(1L, transactionManager.sequenceNumber(tp0).longValue());

        Node node = this.cluster.nodes().get(0);
        time.sleep(10000L);
        client.disconnect(node.idString());
        client.blackout(node, 10);

        sender.run(time.milliseconds()); // now expire the batch.

        assertFutureFailure(request1, TimeoutException.class);
        assertTrue(transactionManager.hasUnresolvedSequence(tp0));
        assertFalse(client.hasInFlightRequests());
        Deque<ProducerBatch> batches = accumulator.batches().get(tp0);
        assertEquals(0, batches.size());
        assertTrue(transactionManager.hasProducerId(producerId));
        // We should now clear the old producerId and get a new one in a single run loop.
        prepareAndReceiveInitProducerId(producerId + 1, Errors.NONE);
        assertTrue(transactionManager.hasProducerId(producerId + 1));
    }

    @Test
    public void testResetOfProducerStateShouldAllowQueuedBatchesToDrain() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.setProducerIdAndEpoch(new ProducerIdAndEpoch(producerId, (short) 0));
        setupWithTransactionState(transactionManager);
        client.setNode(new Node(1, "localhost", 33343));

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

        Future<RecordMetadata> failedResponse = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        Future<RecordMetadata> successfulResponse = accumulator.append(tp1, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());  // connect.
        sender.run(time.milliseconds());  // send.

        assertEquals(1, client.inFlightRequestCount());

        Map<TopicPartition, OffsetAndError> responses = new LinkedHashMap<>();
        responses.put(tp1, new OffsetAndError(-1, Errors.NOT_LEADER_FOR_PARTITION));
        responses.put(tp0, new OffsetAndError(-1, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER));
        client.respond(produceResponse(responses));
        sender.run(time.milliseconds());
        assertTrue(failedResponse.isDone());
        assertFalse("Expected transaction state to be reset upon receiving an OutOfOrderSequenceException", transactionManager.hasProducerId());
        prepareAndReceiveInitProducerId(producerId + 1, Errors.NONE);
        assertEquals(producerId + 1, transactionManager.producerIdAndEpoch().producerId);
        sender.run(time.milliseconds());  // send request to tp1

        assertFalse(successfulResponse.isDone());
        client.respond(produceResponse(tp1, 10, Errors.NONE, -1));
        sender.run(time.milliseconds());

        assertTrue(successfulResponse.isDone());
        assertEquals(10, successfulResponse.get().offset());

        // Since the response came back for the old producer id, we shouldn't update the next sequence.
        assertEquals(0, transactionManager.sequenceNumber(tp1).longValue());
    }

    @Test
    public void testBatchesDrainedWithOldProducerIdShouldFailWithOutOfOrderSequenceOnSubsequentRetry() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.setProducerIdAndEpoch(new ProducerIdAndEpoch(producerId, (short) 0));
        setupWithTransactionState(transactionManager);
        client.setNode(new Node(1, "localhost", 33343));

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

        Future<RecordMetadata> failedResponse = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        Future<RecordMetadata> successfulResponse = accumulator.append(tp1, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());  // connect.
        sender.run(time.milliseconds());  // send.

        assertEquals(1, client.inFlightRequestCount());

        Map<TopicPartition, OffsetAndError> responses = new LinkedHashMap<>();
        responses.put(tp1, new OffsetAndError(-1, Errors.NOT_LEADER_FOR_PARTITION));
        responses.put(tp0, new OffsetAndError(-1, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER));
        client.respond(produceResponse(responses));
        sender.run(time.milliseconds());
        assertTrue(failedResponse.isDone());
        assertFalse("Expected transaction state to be reset upon receiving an OutOfOrderSequenceException", transactionManager.hasProducerId());
        prepareAndReceiveInitProducerId(producerId + 1, Errors.NONE);
        assertEquals(producerId + 1, transactionManager.producerIdAndEpoch().producerId);
        sender.run(time.milliseconds());  // send request to tp1 with the old producerId

        assertFalse(successfulResponse.isDone());
        // The response comes back with a retriable error.
        client.respond(produceResponse(tp1, 0, Errors.NOT_LEADER_FOR_PARTITION, -1));
        sender.run(time.milliseconds());

        assertTrue(successfulResponse.isDone());
        // Since the batch has an old producerId, it will not be retried yet again, but will be failed with a Fatal
        // exception.
        try {
            successfulResponse.get();
            fail("Should have raised an OutOfOrderSequenceException");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof OutOfOrderSequenceException);
        }
    }

    @Test
    public void testCorrectHandlingOfDuplicateSequenceError() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        ClientRequest firstClientRequest = client.requests().peek();
        ClientRequest secondClientRequest = (ClientRequest) client.requests().toArray()[1];

        client.respondToRequest(secondClientRequest, produceResponse(tp0, 1000, Errors.NONE, 0));

        sender.run(time.milliseconds()); // receive response 1

        assertEquals(1000, transactionManager.lastAckedOffset(tp0));
        assertEquals(1, transactionManager.lastAckedSequence(tp0));

        client.respondToRequest(firstClientRequest, produceResponse(tp0, ProduceResponse.INVALID_OFFSET, Errors.DUPLICATE_SEQUENCE_NUMBER, 0));

        sender.run(time.milliseconds()); // receive response 0

        // Make sure that the last ack'd sequence doesn't change.
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertEquals(1000, transactionManager.lastAckedOffset(tp0));
        assertFalse(client.hasInFlightRequests());

        RecordMetadata unknownMetadata = request1.get();
        assertFalse(unknownMetadata.hasOffset());
        assertEquals(-1L, unknownMetadata.offset());
    }

    @Test
    public void testUnknownProducerHandlingWhenRetentionLimitReached() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.run(time.milliseconds());  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(0L, transactionManager.lastAckedSequence(tp0));
        assertEquals(1000L, transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest, a single batch with 2 records.
        accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT);
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertEquals(3, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, 1010L);
        sender.run(time.milliseconds()); // receive response 0, should be retried since the logStartOffset > lastAckedOffset.

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertFalse(request2.isDone());
        assertFalse(client.hasInFlightRequests());

        sender.run(time.milliseconds()); // should retry request 1

        // resend the request. Note that the expected sequence is 0, since we have lost producer state on the broker.
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1011L, 1010L);
        sender.run(time.milliseconds()); // receive response 1
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertFalse(client.hasInFlightRequests());
        assertTrue(request2.isDone());
        assertEquals(1012L, request2.get().offset());
        assertEquals(1012L, transactionManager.lastAckedOffset(tp0));
    }

    @Test
    public void testUnknownProducerErrorShouldBeRetriedWhenLogStartOffsetIsUnknown() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.run(time.milliseconds());  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(0L, transactionManager.lastAckedSequence(tp0));
        assertEquals(1000L, transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, -1L);
        sender.run(time.milliseconds()); // receive response 0, should be retried without resetting the sequence numbers since the log start offset is unknown.

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertEquals(0, transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertFalse(request2.isDone());
        assertFalse(client.hasInFlightRequests());

        sender.run(time.milliseconds()); // should retry request 1

        // resend the request. Note that the expected sequence is 1, since we never got the logStartOffset in the previous
        // response and hence we didn't reset the sequence numbers.
        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1011L, 1010L);
        sender.run(time.milliseconds()); // receive response 1
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertFalse(client.hasInFlightRequests());
        assertTrue(request2.isDone());
        assertEquals(1011L, request2.get().offset());
        assertEquals(1011L, transactionManager.lastAckedOffset(tp0));
    }

    @Test
    public void testUnknownProducerErrorShouldBeRetriedForFutureBatchesWhenFirstFails() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.run(time.milliseconds());  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(0L, transactionManager.lastAckedSequence(tp0));
        assertEquals(1000L, transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));

        // Send the third ProduceRequest, in parallel with the second. It should be retried even though the
        // lastAckedOffset > logStartOffset when its UnknownProducerResponse comes back.
        Future<RecordMetadata> request3 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertEquals(3, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());
        assertFalse(request3.isDone());
        assertEquals(2, client.inFlightRequestCount());


        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, 1010L);
        sender.run(time.milliseconds()); // receive response 2, should reset the sequence numbers and be retried.

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertFalse(request2.isDone());
        assertFalse(request3.isDone());
        assertEquals(1, client.inFlightRequestCount());

        sender.run(time.milliseconds()); // resend request 2.

        assertEquals(2, client.inFlightRequestCount());

        // receive the original response 3. note the expected sequence is still the originally assigned sequence.
        sendIdempotentProducerResponse(2, tp0, Errors.UNKNOWN_PRODUCER_ID, -1, 1010L);
        sender.run(time.milliseconds()); // receive response 3

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1011L, 1010L);
        sender.run(time.milliseconds());  // receive response 2, don't send request 3 since we can have at most 1 in flight when retrying

        assertTrue(request2.isDone());
        assertFalse(request3.isDone());
        assertFalse(client.hasInFlightRequests());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));
        assertEquals(1011L, request2.get().offset());
        assertEquals(1011L, transactionManager.lastAckedOffset(tp0));

        sender.run(time.milliseconds());  // resend request 3.
        assertEquals(1, client.inFlightRequestCount());

        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1012L, 1010L);
        sender.run(time.milliseconds());  // receive response 3.

        assertFalse(client.hasInFlightRequests());
        assertTrue(request3.isDone());
        assertEquals(1012L, request3.get().offset());
        assertEquals(1012L, transactionManager.lastAckedOffset(tp0));
    }

    @Test
    public void testShouldRaiseOutOfOrderSequenceExceptionToUserIfLogWasNotTruncated() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.run(time.milliseconds());  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(0L, transactionManager.lastAckedSequence(tp0));
        assertEquals(1000L, transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest,
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, 10L);
        sender.run(time.milliseconds()); // receive response 0, should cause a producerId reset since the logStartOffset < lastAckedOffset
        assertFutureFailure(request2, OutOfOrderSequenceException.class);

    }
    void sendIdempotentProducerResponse(int expectedSequence, TopicPartition tp, Errors responseError, long responseOffset) {
        sendIdempotentProducerResponse(expectedSequence, tp, responseError, responseOffset, -1L);
    }

    void sendIdempotentProducerResponse(final int expectedSequence, TopicPartition tp, Errors responseError, long responseOffset, long logStartOffset) {
        client.respond(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                ProduceRequest produceRequest = (ProduceRequest) body;
                assertTrue(produceRequest.isIdempotent());

                MemoryRecords records = produceRequest.partitionRecordsOrFail().get(tp0);
                Iterator<MutableRecordBatch> batchIterator = records.batches().iterator();
                RecordBatch firstBatch = batchIterator.next();
                assertFalse(batchIterator.hasNext());
                assertEquals(expectedSequence, firstBatch.baseSequence());

                return true;
            }
        }, produceResponse(tp, responseOffset, responseError, 0, logStartOffset));
    }

    @Test
    public void testClusterAuthorizationExceptionInProduceRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);

        client.setNode(new Node(1, "localhost", 33343));
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        // cluster authorization is a fatal error for the producer
        Future<RecordMetadata> future = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return body instanceof ProduceRequest && ((ProduceRequest) body).isIdempotent();
            }
        }, produceResponse(tp0, -1, Errors.CLUSTER_AUTHORIZATION_FAILED, 0));

        sender.run(time.milliseconds());
        assertFutureFailure(future, ClusterAuthorizationException.class);

        // cluster authorization errors are fatal, so we should continue seeing it on future sends
        assertTrue(transactionManager.hasFatalError());
        assertSendFailure(ClusterAuthorizationException.class);
    }

    @Test
    public void testCancelInFlightRequestAfterFatalError() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);

        client.setNode(new Node(1, "localhost", 33343));
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        // cluster authorization is a fatal error for the producer
        Future<RecordMetadata> future1 = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());

        Future<RecordMetadata> future2 = accumulator.append(tp1, time.milliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());

        client.respond(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return body instanceof ProduceRequest && ((ProduceRequest) body).isIdempotent();
            }
        }, produceResponse(tp0, -1, Errors.CLUSTER_AUTHORIZATION_FAILED, 0));

        sender.run(time.milliseconds());
        assertTrue(transactionManager.hasFatalError());
        assertFutureFailure(future1, ClusterAuthorizationException.class);

        sender.run(time.milliseconds());
        assertFutureFailure(future2, ClusterAuthorizationException.class);

        // Should be fine if the second response eventually returns
        client.respond(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return body instanceof ProduceRequest && ((ProduceRequest) body).isIdempotent();
            }
        }, produceResponse(tp1, 0, Errors.NONE, 0));
        sender.run(time.milliseconds());
    }

    @Test
    public void testUnsupportedForMessageFormatInProduceRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);

        client.setNode(new Node(1, "localhost", 33343));
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        Future<RecordMetadata> future = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return body instanceof ProduceRequest && ((ProduceRequest) body).isIdempotent();
            }
        }, produceResponse(tp0, -1, Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, 0));

        sender.run(time.milliseconds());
        assertFutureFailure(future, UnsupportedForMessageFormatException.class);

        // unsupported for message format is not a fatal error
        assertFalse(transactionManager.hasError());
    }

    @Test
    public void testUnsupportedVersionInProduceRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);

        client.setNode(new Node(1, "localhost", 33343));
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        Future<RecordMetadata> future = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        client.prepareUnsupportedVersionResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return body instanceof ProduceRequest && ((ProduceRequest) body).isIdempotent();
            }
        });

        sender.run(time.milliseconds());
        assertFutureFailure(future, UnsupportedVersionException.class);

        // unsupported version errors are fatal, so we should continue seeing it on future sends
        assertTrue(transactionManager.hasFatalError());
        assertSendFailure(UnsupportedVersionException.class);
    }

    @Test
    public void testSequenceNumberIncrement() throws InterruptedException {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.setProducerIdAndEpoch(new ProducerIdAndEpoch(producerId, (short) 0));
        setupWithTransactionState(transactionManager);
        client.setNode(new Node(1, "localhost", 33343));

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        
        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                if (body instanceof ProduceRequest) {
                    ProduceRequest request = (ProduceRequest) body;
                    MemoryRecords records = request.partitionRecordsOrFail().get(tp0);
                    Iterator<MutableRecordBatch> batchIterator = records.batches().iterator();
                    assertTrue(batchIterator.hasNext());
                    RecordBatch batch = batchIterator.next();
                    assertFalse(batchIterator.hasNext());
                    assertEquals(0, batch.baseSequence());
                    assertEquals(producerId, batch.producerId());
                    assertEquals(0, batch.producerEpoch());
                    return true;
                }
                return false;
            }
        }, produceResponse(tp0, 0, Errors.NONE, 0));

        sender.run(time.milliseconds());  // connect.
        sender.run(time.milliseconds());  // send.

        sender.run(time.milliseconds());  // receive response
        assertTrue(responseFuture.isDone());
        assertEquals(0L, (long) transactionManager.lastAckedSequence(tp0));
        assertEquals(1L, (long) transactionManager.sequenceNumber(tp0));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testAbortRetryWhenProducerIdChanges() throws InterruptedException {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.setProducerIdAndEpoch(new ProducerIdAndEpoch(producerId, (short) 0));
        setupWithTransactionState(transactionManager);
        client.setNode(new Node(1, "localhost", 33343));

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());  // connect.
        sender.run(time.milliseconds());  // send.
        String id = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(id), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertTrue("Client ready status should be true", client.isReady(node, 0L));
        client.disconnect(id);
        assertEquals(0, client.inFlightRequestCount());
        assertFalse("Client ready status should be false", client.isReady(node, 0L));

        transactionManager.resetProducerId();
        transactionManager.setProducerIdAndEpoch(new ProducerIdAndEpoch(producerId + 1, (short) 0));
        sender.run(time.milliseconds()); // receive error
        sender.run(time.milliseconds()); // reconnect
        sender.run(time.milliseconds()); // nothing to do, since the pid has changed. We should check the metrics for errors.
        assertEquals("Expected requests to be aborted after pid change", 0, client.inFlightRequestCount());

        KafkaMetric recordErrors = m.metrics().get(senderMetrics.recordErrorRate);
        assertTrue("Expected non-zero value for record send errors", recordErrors.value() > 0);

        assertTrue(responseFuture.isDone());
        assertEquals(0, (long) transactionManager.sequenceNumber(tp0));
    }

    @Test
    public void testResetWhenOutOfOrderSequenceReceived() throws InterruptedException {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.setProducerIdAndEpoch(new ProducerIdAndEpoch(producerId, (short) 0));
        setupWithTransactionState(transactionManager);
        client.setNode(new Node(1, "localhost", 33343));

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        
        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());  // connect.
        sender.run(time.milliseconds());  // send.

        assertEquals(1, client.inFlightRequestCount());

        client.respond(produceResponse(tp0, 0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, 0));

        sender.run(time.milliseconds());
        assertTrue(responseFuture.isDone());
        assertFalse("Expected transaction state to be reset upon receiving an OutOfOrderSequenceException", transactionManager.hasProducerId());
    }

    @Test
    public void testIdempotentSplitBatchAndSend() throws Exception {
        TopicPartition tp = new TopicPartition("testSplitBatchAndSend", 1);
        TransactionManager txnManager = new TransactionManager();
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        txnManager.setProducerIdAndEpoch(producerIdAndEpoch);
        testSplitBatchAndSend(txnManager, producerIdAndEpoch, tp);
    }

    @Test
    public void testTransactionalSplitBatchAndSend() throws Exception {
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        TopicPartition tp = new TopicPartition("testSplitBatchAndSend", 1);
        TransactionManager txnManager = new TransactionManager(logContext, "testSplitBatchAndSend", 60000, 100);

        setupWithTransactionState(txnManager);
        doInitTransactions(txnManager, producerIdAndEpoch);

        txnManager.beginTransaction();
        txnManager.maybeAddPartitionToTransaction(tp);
        client.prepareResponse(new AddPartitionsToTxnResponse(0, Collections.singletonMap(tp, Errors.NONE)));
        sender.run(time.milliseconds());

        testSplitBatchAndSend(txnManager, producerIdAndEpoch, tp);
    }

    @SuppressWarnings("deprecation")
    private void testSplitBatchAndSend(TransactionManager txnManager,
                                       ProducerIdAndEpoch producerIdAndEpoch,
                                       TopicPartition tp) throws Exception {
        int maxRetries = 1;
        String topic = tp.topic();
        // Set a good compression ratio.
        CompressionRatioEstimator.setEstimation(topic, CompressionType.GZIP, 0.2f);
        try (Metrics m = new Metrics()) {
            accumulator = new RecordAccumulator(logContext, batchSize, 1024 * 1024, CompressionType.GZIP, 0L, 0L, m, time,
                    new ApiVersions(), txnManager);
            SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                    senderMetrics, time, REQUEST_TIMEOUT, 1000L, txnManager, new ApiVersions());
            // Create a two broker cluster, with partition 0 on broker 0 and partition 1 on broker 1
            Cluster cluster1 = TestUtils.clusterWith(2, topic, 2);
            metadata.update(cluster1, Collections.<String>emptySet(), time.milliseconds());
            // Send the first message.
            Future<RecordMetadata> f1 =
                    accumulator.append(tp, 0L, "key1".getBytes(), new byte[batchSize / 2], null, null, MAX_BLOCK_TIMEOUT).future;
            Future<RecordMetadata> f2 =
                    accumulator.append(tp, 0L, "key2".getBytes(), new byte[batchSize / 2], null, null, MAX_BLOCK_TIMEOUT).future;
            sender.run(time.milliseconds()); // connect
            sender.run(time.milliseconds()); // send produce request

            assertEquals("The next sequence should be 2", 2, txnManager.sequenceNumber(tp).longValue());
            String id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            Node node = new Node(Integer.valueOf(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));

            Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<>();
            responseMap.put(tp, new ProduceResponse.PartitionResponse(Errors.MESSAGE_TOO_LARGE));
            client.respond(new ProduceResponse(responseMap));
            sender.run(time.milliseconds()); // split and reenqueue
            assertEquals("The next sequence should be 2", 2, txnManager.sequenceNumber(tp).longValue());
            // The compression ratio should have been improved once.
            assertEquals(CompressionType.GZIP.rate - CompressionRatioEstimator.COMPRESSION_RATIO_IMPROVING_STEP,
                    CompressionRatioEstimator.estimation(topic, CompressionType.GZIP), 0.01);
            sender.run(time.milliseconds()); // send the first produce request
            assertEquals("The next sequence number should be 2", 2, txnManager.sequenceNumber(tp).longValue());
            assertFalse("The future shouldn't have been done.", f1.isDone());
            assertFalse("The future shouldn't have been done.", f2.isDone());
            id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            node = new Node(Integer.valueOf(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));

            responseMap.put(tp, new ProduceResponse.PartitionResponse(Errors.NONE, 0L, 0L, 0L));
            client.respond(produceRequestMatcher(tp, producerIdAndEpoch, 0, txnManager.isTransactional()),
                    new ProduceResponse(responseMap));

            sender.run(time.milliseconds()); // receive
            assertTrue("The future should have been done.", f1.isDone());
            assertEquals("The next sequence number should still be 2", 2, txnManager.sequenceNumber(tp).longValue());
            assertEquals("The last ack'd sequence number should be 0", 0, txnManager.lastAckedSequence(tp));
            assertFalse("The future shouldn't have been done.", f2.isDone());
            assertEquals("Offset of the first message should be 0", 0L, f1.get().offset());
            sender.run(time.milliseconds()); // send the seconcd produce request
            id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            node = new Node(Integer.valueOf(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));

            responseMap.put(tp, new ProduceResponse.PartitionResponse(Errors.NONE, 1L, 0L, 0L));
            client.respond(produceRequestMatcher(tp, producerIdAndEpoch, 1, txnManager.isTransactional()),
                    new ProduceResponse(responseMap));

            sender.run(time.milliseconds()); // receive
            assertTrue("The future should have been done.", f2.isDone());
            assertEquals("The next sequence number should be 2", 2, txnManager.sequenceNumber(tp).longValue());
            assertEquals("The last ack'd sequence number should be 1", 1, txnManager.lastAckedSequence(tp));
            assertEquals("Offset of the first message should be 1", 1L, f2.get().offset());
            assertTrue("There should be no batch in the accumulator", accumulator.batches().get(tp).isEmpty());

            assertTrue("There should be a split",
                    m.metrics().get(senderMetrics.batchSplitRate).value() > 0);
        }
    }

    private MockClient.RequestMatcher produceRequestMatcher(final TopicPartition tp,
                                                            final ProducerIdAndEpoch producerIdAndEpoch,
                                                            final int sequence,
                                                            final boolean isTransactional) {
        return new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                if (!(body instanceof ProduceRequest))
                    return false;

                ProduceRequest request = (ProduceRequest) body;
                Map<TopicPartition, MemoryRecords> recordsMap = request.partitionRecordsOrFail();
                MemoryRecords records = recordsMap.get(tp);
                if (records == null)
                    return false;

                List<MutableRecordBatch> batches = TestUtils.toList(records.batches());
                if (batches.isEmpty() || batches.size() > 1)
                    return false;

                MutableRecordBatch batch = batches.get(0);
                return batch.baseOffset() == 0L &&
                        batch.baseSequence() == sequence &&
                        batch.producerId() == producerIdAndEpoch.producerId &&
                        batch.producerEpoch() == producerIdAndEpoch.epoch &&
                        batch.isTransactional() == isTransactional;
            }
        };
    }

    class OffsetAndError {
        long offset;
        Errors error;
        OffsetAndError(long offset, Errors error) {
            this.offset = offset;
            this.error = error;
        }
    }

    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs, long logStartOffset) {
        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(error, offset, RecordBatch.NO_TIMESTAMP, logStartOffset);
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = Collections.singletonMap(tp, resp);
        return new ProduceResponse(partResp, throttleTimeMs);
    }

    private ProduceResponse produceResponse(Map<TopicPartition, OffsetAndError> responses) {
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResponses = new LinkedHashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndError> entry : responses.entrySet()) {
            ProduceResponse.PartitionResponse response = new ProduceResponse.PartitionResponse(entry.getValue().error,
                    entry.getValue().offset, RecordBatch.NO_TIMESTAMP, -1);
            partResponses.put(entry.getKey(), response);
        }
        return new ProduceResponse(partResponses);

    }
    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs) {
        return produceResponse(tp, offset, error, throttleTimeMs, -1L);
    }

    private void setupWithTransactionState(TransactionManager transactionManager) {
        Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("client-id", CLIENT_ID);
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        this.metrics = new Metrics(metricConfig, time);
        this.accumulator = new RecordAccumulator(logContext, batchSize, 1024 * 1024, CompressionType.NONE, 0L, 0L, metrics, time,
                apiVersions, transactionManager);
        this.senderMetricsRegistry = new SenderMetricsRegistry(this.metrics);

        this.sender = new Sender(logContext, this.client, this.metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                Integer.MAX_VALUE, this.senderMetricsRegistry, this.time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);
        this.metadata.update(this.cluster, Collections.<String>emptySet(), time.milliseconds());
    }

    private void assertSendFailure(Class<? extends RuntimeException> expectedError) throws Exception {
        Future<RecordMetadata> future = accumulator.append(tp0, time.milliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertTrue(future.isDone());
        try {
            future.get();
            fail("Future should have raised " + expectedError.getSimpleName());
        } catch (ExecutionException e) {
            assertTrue(expectedError.isAssignableFrom(e.getCause().getClass()));
        }
    }

    private void prepareAndReceiveInitProducerId(long producerId, Errors error) {
        short producerEpoch = 0;
        if (error != Errors.NONE)
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH;

        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return body instanceof InitProducerIdRequest && ((InitProducerIdRequest) body).transactionalId() == null;
            }
        }, new InitProducerIdResponse(0, error, producerId, producerEpoch));
        sender.run(time.milliseconds());
    }

    private void doInitTransactions(TransactionManager transactionManager, ProducerIdAndEpoch producerIdAndEpoch) {
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE);
        sender.run(time.milliseconds());
        sender.run(time.milliseconds());

        prepareInitPidResponse(Errors.NONE, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        sender.run(time.milliseconds());
        assertTrue(transactionManager.hasProducerId());
    }

    private void prepareFindCoordinatorResponse(Errors error) {
        client.prepareResponse(new FindCoordinatorResponse(error, cluster.nodes().get(0)));
    }

    private void prepareInitPidResponse(Errors error, long pid, short epoch) {
        client.prepareResponse(new InitProducerIdResponse(0, error, pid, epoch));
    }

    private void assertFutureFailure(Future<?> future, Class<? extends Exception> expectedExceptionType)
            throws InterruptedException {
        assertTrue(future.isDone());
        try {
            future.get();
            fail("Future should have raised " + expectedExceptionType.getName());
        } catch (ExecutionException e) {
            Class<? extends Throwable> causeType = e.getCause().getClass();
            assertTrue("Unexpected cause " + causeType.getName(), expectedExceptionType.isAssignableFrom(causeType));
        }
    }

}
