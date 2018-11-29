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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
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
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.DelayedReceive;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;

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
    private int batchSize = 16 * 1024;
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE, true, true, new ClusterResourceListeners());
    private MockClient client = new MockClient(time, metadata);
    private ApiVersions apiVersions = new ApiVersions();
    private Metrics metrics = null;
    private RecordAccumulator accumulator = null;
    private Sender sender = null;
    private SenderMetricsRegistry senderMetricsRegistry = null;
    private final LogContext logContext = new LogContext();

    @Before
    public void setup() {
        setupWithTransactionState(null);
    }

    @After
    public void tearDown() {
        this.metrics.close();
    }

    @Test
    public void testSimple() throws Exception {
        long offset = 0;
        Future<RecordMetadata> future = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds()); // connect
        sender.run(time.absoluteMilliseconds()); // send produce request
        assertEquals("We should have a single produce request in flight.", 1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());
        assertTrue(client.hasInFlightRequests());
        client.respond(produceResponse(tp0, offset, Errors.NONE, 0));
        sender.run(time.absoluteMilliseconds());
        assertEquals("All requests completed.", 0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());
        assertFalse(client.hasInFlightRequests());
        sender.run(time.absoluteMilliseconds());
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

        sender.run(time.absoluteMilliseconds()); // connect
        sender.run(time.absoluteMilliseconds()); // send produce request

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

        sender.run(time.absoluteMilliseconds()); // connect
        sender.run(time.absoluteMilliseconds()); // send produce request

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
                1000, 1000, 64 * 1024, 64 * 1024, 1000,  ClientDnsLookup.DEFAULT,
                time, true, new ApiVersions(), throttleTimeSensor, logContext);

        short apiVersionsResponseVersion = ApiKeys.API_VERSIONS.latestVersion();
        ByteBuffer buffer = ApiVersionsResponse.createApiVersionsResponse(400, RecordBatch.CURRENT_MAGIC_VALUE).serialize(apiVersionsResponseVersion, new ResponseHeader(0));
        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
        while (!client.ready(node, time.absoluteMilliseconds())) {
            client.poll(1, time.absoluteMilliseconds());
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.absoluteMilliseconds()));
        }
        selector.clear();

        for (int i = 1; i <= 3; i++) {
            int throttleTimeMs = 100 * i;
            ProduceRequest.Builder builder = ProduceRequest.Builder.forCurrentMagic((short) 1, 1000,
                            Collections.emptyMap());
            ClientRequest request = client.newClientRequest(node.idString(), builder, time.absoluteMilliseconds(), true);
            client.send(request, time.absoluteMilliseconds());
            client.poll(1, time.absoluteMilliseconds());
            ProduceResponse response = produceResponse(tp0, i, Errors.NONE, throttleTimeMs);
            buffer = response.serialize(ApiKeys.PRODUCE.latestVersion(), new ResponseHeader(request.correlationId()));
            selector.completeReceive(new NetworkReceive(node.idString(), buffer));
            client.poll(1, time.absoluteMilliseconds());
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.absoluteMilliseconds()));
            selector.clear();
        }
        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric avgMetric = allMetrics.get(this.senderMetricsRegistry.produceThrottleTimeAvg);
        KafkaMetric maxMetric = allMetrics.get(this.senderMetricsRegistry.produceThrottleTimeMax);
        // Throttle times are ApiVersions=400, Produce=(100, 200, 300)
        assertEquals(250, (Double) avgMetric.metricValue(), EPS);
        assertEquals(400, (Double) maxMetric.metricValue(), EPS);
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
        sender.run(time.absoluteMilliseconds()); // connect
        sender.run(time.absoluteMilliseconds()); // send produce request
        client.respond(produceResponse(tp0, 0, Errors.NONE, 0));
        sender.run(time.absoluteMilliseconds());
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
            sender.run(time.absoluteMilliseconds()); // connect
            sender.run(time.absoluteMilliseconds()); // send produce request
            String id = client.requests().peek().destination();
            Node node = new Node(Integer.parseInt(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertEquals(1, sender.inFlightBatches(tp0).size());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));
            client.disconnect(id);
            assertEquals(0, client.inFlightRequestCount());
            assertFalse(client.hasInFlightRequests());
            assertFalse("Client ready status should be false", client.isReady(node, 0L));
            // the batch is in accumulator.inFlightBatches until it expires
            assertEquals(1, sender.inFlightBatches(tp0).size());
            sender.run(time.absoluteMilliseconds()); // receive error
            sender.run(time.absoluteMilliseconds()); // reconnect
            sender.run(time.absoluteMilliseconds()); // resend
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertEquals(1, sender.inFlightBatches(tp0).size());
            long offset = 0;
            client.respond(produceResponse(tp0, offset, Errors.NONE, 0));
            sender.run(time.absoluteMilliseconds());
            assertTrue("Request should have retried and completed", future.isDone());
            assertEquals(offset, future.get().offset());
            assertEquals(0, sender.inFlightBatches(tp0).size());

            // do an unsuccessful retry
            future = accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
            sender.run(time.absoluteMilliseconds()); // send produce request
            assertEquals(1, sender.inFlightBatches(tp0).size());
            for (int i = 0; i < maxRetries + 1; i++) {
                client.disconnect(client.requests().peek().destination());
                sender.run(time.absoluteMilliseconds()); // receive error
                assertEquals(0, sender.inFlightBatches(tp0).size());
                sender.run(time.absoluteMilliseconds()); // reconnect
                sender.run(time.absoluteMilliseconds()); // resend
                assertEquals(i > 0 ? 0 : 1, sender.inFlightBatches(tp0).size());
            }
            sender.run(time.absoluteMilliseconds());
            assertFutureFailure(future, NetworkException.class);
            assertEquals(0, sender.inFlightBatches(tp0).size());
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
            MetadataResponse metadataUpdate1 = TestUtils.metadataUpdateWith(2, Collections.singletonMap("test", 2));
            client.prepareMetadataUpdate(metadataUpdate1);

            // Send the first message.
            TopicPartition tp2 = new TopicPartition("test", 1);
            accumulator.append(tp2, 0L, "key1".getBytes(), "value1".getBytes(), null, null, MAX_BLOCK_TIMEOUT);
            sender.run(time.absoluteMilliseconds()); // connect
            sender.run(time.absoluteMilliseconds()); // send produce request
            String id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            Node node = new Node(Integer.parseInt(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));
            assertEquals(1, sender.inFlightBatches(tp2).size());

            time.sleep(900);
            // Now send another message to tp2
            accumulator.append(tp2, 0L, "key2".getBytes(), "value2".getBytes(), null, null, MAX_BLOCK_TIMEOUT);

            // Update metadata before sender receives response from broker 0. Now partition 2 moves to broker 0
            MetadataResponse metadataUpdate2 = TestUtils.metadataUpdateWith(1, Collections.singletonMap("test", 2));
            client.prepareMetadataUpdate(metadataUpdate2);
            // Sender should not send the second message to node 0.
            assertEquals(1, sender.inFlightBatches(tp2).size());
            sender.run(time.absoluteMilliseconds());  // receive the response for the previous send, and send the new batch
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertEquals(1, sender.inFlightBatches(tp2).size());
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

        Node clusterNode = metadata.fetch().nodes().get(0);
        Map<Integer, List<ProducerBatch>> drainedBatches =
            accumulator.drain(metadata.fetch(), Collections.singleton(clusterNode), Integer.MAX_VALUE, time.absoluteMilliseconds());
        sender.addToInflightBatches(drainedBatches);

        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        client.disconnect(clusterNode.idString());
        client.blackout(clusterNode, 100);

        sender.run(time.absoluteMilliseconds());  // We should try to flush the batch, but we expire it instead without sending anything.
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
        client.updateMetadata(TestUtils.metadataUpdateWith(1, Collections.emptyMap()));

        Future<RecordMetadata> future = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertTrue("Topic not added to metadata", metadata.containsTopic(tp0.topic()));
        client.updateMetadata(TestUtils.metadataUpdateWith(1, Collections.singletonMap("test", 2)));
        sender.run(time.absoluteMilliseconds());  // send produce request
        client.respond(produceResponse(tp0, offset++, Errors.NONE, 0));
        sender.run(time.absoluteMilliseconds());
        assertEquals("Request completed.", 0, client.inFlightRequestCount());
        assertFalse(client.hasInFlightRequests());
        assertEquals(0, sender.inFlightBatches(tp0).size());
        sender.run(time.absoluteMilliseconds());
        assertTrue("Request should be completed", future.isDone());

        assertTrue("Topic not retained in metadata list", metadata.containsTopic(tp0.topic()));
        time.sleep(Metadata.TOPIC_EXPIRY_MS);
        client.updateMetadata(TestUtils.metadataUpdateWith(1, Collections.emptyMap()));
        assertFalse("Unused topic has not been expired", metadata.containsTopic(tp0.topic()));
        future = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertTrue("Topic not added to metadata", metadata.containsTopic(tp0.topic()));
        client.updateMetadata(TestUtils.metadataUpdateWith(1, Collections.singletonMap("test", 2)));
        sender.run(time.absoluteMilliseconds());  // send produce request
        client.respond(produceResponse(tp0, offset++, Errors.NONE, 0));
        sender.run(time.absoluteMilliseconds());
        assertEquals("Request completed.", 0, client.inFlightRequestCount());
        assertFalse(client.hasInFlightRequests());
        assertEquals(0, sender.inFlightBatches(tp0).size());
        sender.run(time.absoluteMilliseconds());
        assertTrue("Request should be completed", future.isDone());
    }

    @Test
    public void testInitProducerIdRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);
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
        sender.run(time.absoluteMilliseconds()); // connect
        sender.run(time.absoluteMilliseconds()); // send produce request
        String id = client.requests().peek().destination();
        Node node = new Node(Integer.parseInt(id), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertTrue(client.hasInFlightRequests());
        assertEquals(1, sender.inFlightBatches(tp0).size());
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
        sender.run(time.absoluteMilliseconds());
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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.absoluteMilliseconds()));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);

        sender.run(time.absoluteMilliseconds()); // receive response 0

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());
        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1L);
        sender.run(time.absoluteMilliseconds()); // receive response 1
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertFalse(client.hasInFlightRequests());
        assertEquals(0, sender.inFlightBatches(tp0).size());
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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());

         // Send third ProduceRequest
        Future<RecordMetadata> request3 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());

        assertEquals(3, client.inFlightRequestCount());
        assertEquals(3, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertFalse(request3.isDone());
        assertTrue(client.isReady(node, time.absoluteMilliseconds()));

        sendIdempotentProducerResponse(0, tp0, Errors.LEADER_NOT_AVAILABLE, -1L);
        sender.run(time.absoluteMilliseconds()); // receive response 0

        // Queue the fourth request, it shouldn't be sent until the first 3 complete.
        Future<RecordMetadata> request4 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;

        assertEquals(2, client.inFlightRequestCount());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(1, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);
        sender.run(time.absoluteMilliseconds()); // re send request 1, receive response 2

        sendIdempotentProducerResponse(2, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);
        sender.run(time.absoluteMilliseconds()); // receive response 3

        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(1, client.inFlightRequestCount());

        sender.run(time.absoluteMilliseconds()); // Do nothing, we are reduced to one in flight request during retries.

        assertEquals(3, transactionManager.sequenceNumber(tp0).longValue());  // the batch for request 4 shouldn't have been drained, and hence the sequence should not have been incremented.
        assertEquals(1, client.inFlightRequestCount());

        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.run(time.absoluteMilliseconds());  // receive response 1
        assertEquals(0, transactionManager.lastAckedSequence(tp0));
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());
        assertFalse(client.hasInFlightRequests());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        sender.run(time.absoluteMilliseconds()); // send request 2;
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1L);
        sender.run(time.absoluteMilliseconds());  // receive response 2
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());

        assertFalse(client.hasInFlightRequests());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        sender.run(time.absoluteMilliseconds()); // send request 3
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        sendIdempotentProducerResponse(2, tp0, Errors.NONE, 2L);
        sender.run(time.absoluteMilliseconds());  // receive response 3, send request 4 since we are out of 'retry' mode.
        assertEquals(2, transactionManager.lastAckedSequence(tp0));
        assertTrue(request3.isDone());
        assertEquals(2, request3.get().offset());
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        sendIdempotentProducerResponse(3, tp0, Errors.NONE, 3L);
        sender.run(time.absoluteMilliseconds());  // receive response 4
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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.absoluteMilliseconds()));

        sendIdempotentProducerResponse(0, tp0, Errors.MESSAGE_TOO_LARGE, -1L);

        sender.run(time.absoluteMilliseconds()); // receive response 0, should adjust sequences of future batches.
        assertFutureFailure(request1, RecordTooLargeException.class);

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(1, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);

        sender.run(time.absoluteMilliseconds()); // receive response 1

        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());

        sender.run(time.absoluteMilliseconds()); // resend request 1

        assertEquals(1, client.inFlightRequestCount());

        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.run(time.absoluteMilliseconds());  // receive response 1
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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT);
        sender.run(time.absoluteMilliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());

        // make sure the next sequence number accounts for multi-message batches.
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0);

        sender.run(time.absoluteMilliseconds());

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(3, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.absoluteMilliseconds()));

        // This OutOfOrderSequence is fatal since it is returned for the batch succeeding the last acknowledged batch.
        sendIdempotentProducerResponse(2, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);

        sender.run(time.absoluteMilliseconds());
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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.absoluteMilliseconds()));

        ClientRequest firstClientRequest = client.requests().peek();
        ClientRequest secondClientRequest = (ClientRequest) client.requests().toArray()[1];

        client.respondToRequest(secondClientRequest, produceResponse(tp0, -1, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1));

        sender.run(time.absoluteMilliseconds()); // receive response 1
        Deque<ProducerBatch> queuedBatches = accumulator.batches().get(tp0);

        // Make sure that we are queueing the second batch first.
        assertEquals(1, queuedBatches.size());
        assertEquals(1, queuedBatches.peekFirst().baseSequence());
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        client.respondToRequest(firstClientRequest, produceResponse(tp0, -1, Errors.NOT_LEADER_FOR_PARTITION, -1));

        sender.run(time.absoluteMilliseconds()); // receive response 0

        // Make sure we requeued both batches in the correct order.
        assertEquals(2, queuedBatches.size());
        assertEquals(0, queuedBatches.peekFirst().baseSequence());
        assertEquals(1, queuedBatches.peekLast().baseSequence());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());

        sender.run(time.absoluteMilliseconds()); // send request 0
        assertEquals(1, client.inFlightRequestCount());
        sender.run(time.absoluteMilliseconds()); // don't do anything, only one inflight allowed once we are retrying.

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Make sure that the requests are sent in order, even though the previous responses were not in order.
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.run(time.absoluteMilliseconds());  // receive response 0
        assertEquals(0, transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());

        sender.run(time.absoluteMilliseconds()); // send request 1
        assertEquals(1, client.inFlightRequestCount());
        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1L);
        sender.run(time.absoluteMilliseconds());  // receive response 1

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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertEquals(2, client.inFlightRequestCount());
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.absoluteMilliseconds()));

        ClientRequest firstClientRequest = client.requests().peek();
        ClientRequest secondClientRequest = (ClientRequest) client.requests().toArray()[1];

        client.respondToRequest(secondClientRequest, produceResponse(tp0, 1, Errors.NONE, 1));

        sender.run(time.absoluteMilliseconds()); // receive response 1
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());
        assertFalse(request1.isDone());
        Deque<ProducerBatch> queuedBatches = accumulator.batches().get(tp0);

        assertEquals(0, queuedBatches.size());
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.lastAckedSequence(tp0));

        client.respondToRequest(firstClientRequest, produceResponse(tp0, -1, Errors.REQUEST_TIMED_OUT, -1));

        sender.run(time.absoluteMilliseconds()); // receive response 0

        // Make sure we requeued both batches in the correct order.
        assertEquals(1, queuedBatches.size());
        assertEquals(0, queuedBatches.peekFirst().baseSequence());
        assertEquals(1, transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());

        sender.run(time.absoluteMilliseconds()); // resend request 0
        assertEquals(1, client.inFlightRequestCount());

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.lastAckedSequence(tp0));

        // Make sure we handle the out of order successful responses correctly.
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.run(time.absoluteMilliseconds());  // receive response 0
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
        Node node = metadata.fetch().nodes().get(0);
        time.sleep(10000L);
        client.disconnect(node.idString());
        client.blackout(node, 10);

        sender.run(time.absoluteMilliseconds());

        assertFutureFailure(request1, TimeoutException.class);
        assertFalse(transactionManager.hasUnresolvedSequence(tp0));
    }

    @Test
    public void testExpiryOfFirstBatchShouldNotCauseUnresolvedSequencesIfFutureBatchesSucceed() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager, false, null);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());
        assertEquals(0, transactionManager.sequenceNumber(tp0).longValue());

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());  // send request
        // We separate the two appends by 1 second so that the two batches
        // don't expire at the same time.
        time.sleep(1000L);

        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());  // send request
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, sender.inFlightBatches(tp0).size());

        sendIdempotentProducerResponse(0, tp0, Errors.REQUEST_TIMED_OUT, -1);
        sender.run(time.absoluteMilliseconds());  // receive first response
        assertEquals(1, sender.inFlightBatches(tp0).size());

        Node node = metadata.fetch().nodes().get(0);
        // We add 600 millis to expire the first batch but not the second.
        // Note deliveryTimeoutMs is 1500.
        time.sleep(600L);
        client.disconnect(node.idString());
        client.blackout(node, 10);

        sender.run(time.absoluteMilliseconds()); // now expire the first batch.
        assertFutureFailure(request1, TimeoutException.class);
        assertTrue(transactionManager.hasUnresolvedSequence(tp0));
        assertEquals(0, sender.inFlightBatches(tp0).size());

        // let's enqueue another batch, which should not be dequeued until the unresolved state is clear.
        Future<RecordMetadata> request3 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        time.sleep(20);
        assertFalse(request2.isDone());

        sender.run(time.absoluteMilliseconds());  // send second request
        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1);
        assertEquals(1, sender.inFlightBatches(tp0).size());

        sender.run(time.absoluteMilliseconds()); // receive second response, the third request shouldn't be sent since we are in an unresolved state.
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        Deque<ProducerBatch> batches = accumulator.batches().get(tp0);
        assertEquals(1, batches.size());
        assertFalse(batches.peekFirst().hasSequence());
        assertFalse(client.hasInFlightRequests());
        assertEquals(2L, transactionManager.sequenceNumber(tp0).longValue());
        assertTrue(transactionManager.hasUnresolvedSequence(tp0));

        sender.run(time.absoluteMilliseconds());  // clear the unresolved state, send the pending request.
        assertFalse(transactionManager.hasUnresolvedSequence(tp0));
        assertTrue(transactionManager.hasProducerId());
        assertEquals(0, batches.size());
        assertEquals(1, client.inFlightRequestCount());
        assertFalse(request3.isDone());
        assertEquals(1, sender.inFlightBatches(tp0).size());
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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());  // send request

        time.sleep(1000L);
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());  // send request

        assertEquals(2, client.inFlightRequestCount());

        sendIdempotentProducerResponse(0, tp0, Errors.NOT_LEADER_FOR_PARTITION, -1);
        sender.run(time.absoluteMilliseconds());  // receive first response

        Node node = metadata.fetch().nodes().get(0);
        time.sleep(1000L);
        client.disconnect(node.idString());
        client.blackout(node, 10);

        sender.run(time.absoluteMilliseconds()); // now expire the first batch.
        assertFutureFailure(request1, TimeoutException.class);
        assertTrue(transactionManager.hasUnresolvedSequence(tp0));
        // let's enqueue another batch, which should not be dequeued until the unresolved state is clear.
        Future<RecordMetadata> request3 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;

        time.sleep(20);
        assertFalse(request2.isDone());
        sender.run(time.absoluteMilliseconds());  // send second request
        sendIdempotentProducerResponse(1, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, 1);
        sender.run(time.absoluteMilliseconds()); // receive second response, the third request shouldn't be sent since we are in an unresolved state.
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
        sender.run(time.absoluteMilliseconds());  // send request
        sendIdempotentProducerResponse(0, tp0, Errors.NOT_LEADER_FOR_PARTITION, -1);

        sender.run(time.absoluteMilliseconds());  // receive response
        assertEquals(1L, transactionManager.sequenceNumber(tp0).longValue());

        Node node = metadata.fetch().nodes().get(0);
        time.sleep(15000L);
        client.disconnect(node.idString());
        client.blackout(node, 10);

        sender.run(time.absoluteMilliseconds()); // now expire the batch.

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

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

        Future<RecordMetadata> failedResponse = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(),
                "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        Future<RecordMetadata> successfulResponse = accumulator.append(tp1, time.absoluteMilliseconds(), "key".getBytes(),
                "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());  // connect.
        sender.run(time.absoluteMilliseconds());  // send.

        assertEquals(1, client.inFlightRequestCount());

        Map<TopicPartition, OffsetAndError> responses = new LinkedHashMap<>();
        responses.put(tp1, new OffsetAndError(-1, Errors.NOT_LEADER_FOR_PARTITION));
        responses.put(tp0, new OffsetAndError(-1, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER));
        client.respond(produceResponse(responses));
        sender.run(time.absoluteMilliseconds());
        assertTrue(failedResponse.isDone());
        assertFalse("Expected transaction state to be reset upon receiving an OutOfOrderSequenceException", transactionManager.hasProducerId());
        prepareAndReceiveInitProducerId(producerId + 1, Errors.NONE);
        assertEquals(producerId + 1, transactionManager.producerIdAndEpoch().producerId);
        sender.run(time.absoluteMilliseconds());  // send request to tp1

        assertFalse(successfulResponse.isDone());
        client.respond(produceResponse(tp1, 10, Errors.NONE, -1));
        sender.run(time.absoluteMilliseconds());

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

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

        Future<RecordMetadata> failedResponse = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(),
                "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        Future<RecordMetadata> successfulResponse = accumulator.append(tp1, time.absoluteMilliseconds(), "key".getBytes(),
                "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());  // connect.
        sender.run(time.absoluteMilliseconds());  // send.

        assertEquals(1, client.inFlightRequestCount());

        Map<TopicPartition, OffsetAndError> responses = new LinkedHashMap<>();
        responses.put(tp1, new OffsetAndError(-1, Errors.NOT_LEADER_FOR_PARTITION));
        responses.put(tp0, new OffsetAndError(-1, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER));
        client.respond(produceResponse(responses));
        sender.run(time.absoluteMilliseconds());
        assertTrue(failedResponse.isDone());
        assertFalse("Expected transaction state to be reset upon receiving an OutOfOrderSequenceException", transactionManager.hasProducerId());
        prepareAndReceiveInitProducerId(producerId + 1, Errors.NONE);
        assertEquals(producerId + 1, transactionManager.producerIdAndEpoch().producerId);
        sender.run(time.absoluteMilliseconds());  // send request to tp1 with the old producerId

        assertFalse(successfulResponse.isDone());
        // The response comes back with a retriable error.
        client.respond(produceResponse(tp1, 0, Errors.NOT_LEADER_FOR_PARTITION, -1));
        sender.run(time.absoluteMilliseconds());

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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.absoluteMilliseconds()));

        ClientRequest firstClientRequest = client.requests().peek();
        ClientRequest secondClientRequest = (ClientRequest) client.requests().toArray()[1];

        client.respondToRequest(secondClientRequest, produceResponse(tp0, 1000, Errors.NONE, 0));

        sender.run(time.absoluteMilliseconds()); // receive response 1

        assertEquals(1000, transactionManager.lastAckedOffset(tp0));
        assertEquals(1, transactionManager.lastAckedSequence(tp0));

        client.respondToRequest(firstClientRequest, produceResponse(tp0, ProduceResponse.INVALID_OFFSET, Errors.DUPLICATE_SEQUENCE_NUMBER, 0));

        sender.run(time.absoluteMilliseconds()); // receive response 0

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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.run(time.absoluteMilliseconds());  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(0L, transactionManager.lastAckedSequence(tp0));
        assertEquals(1000L, transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest, a single batch with 2 records.
        accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT);
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertEquals(3, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, 1010L);
        sender.run(time.absoluteMilliseconds()); // receive response 0, should be retried since the logStartOffset > lastAckedOffset.

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertFalse(request2.isDone());
        assertFalse(client.hasInFlightRequests());

        sender.run(time.absoluteMilliseconds()); // should retry request 1

        // resend the request. Note that the expected sequence is 0, since we have lost producer state on the broker.
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1011L, 1010L);
        sender.run(time.absoluteMilliseconds()); // receive response 1
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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.run(time.absoluteMilliseconds());  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(0L, transactionManager.lastAckedSequence(tp0));
        assertEquals(1000L, transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, -1L);
        sender.run(time.absoluteMilliseconds()); // receive response 0, should be retried without resetting the sequence numbers since the log start offset is unknown.

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertEquals(0, transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertFalse(request2.isDone());
        assertFalse(client.hasInFlightRequests());

        sender.run(time.absoluteMilliseconds()); // should retry request 1

        // resend the request. Note that the expected sequence is 1, since we never got the logStartOffset in the previous
        // response and hence we didn't reset the sequence numbers.
        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1011L, 1010L);
        sender.run(time.absoluteMilliseconds()); // receive response 1
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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.run(time.absoluteMilliseconds());  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(0L, transactionManager.lastAckedSequence(tp0));
        assertEquals(1000L, transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));

        // Send the third ProduceRequest, in parallel with the second. It should be retried even though the
        // lastAckedOffset > logStartOffset when its UnknownProducerResponse comes back.
        Future<RecordMetadata> request3 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertEquals(3, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());
        assertFalse(request3.isDone());
        assertEquals(2, client.inFlightRequestCount());


        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, 1010L);
        sender.run(time.absoluteMilliseconds()); // receive response 2, should reset the sequence numbers and be retried.

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertFalse(request2.isDone());
        assertFalse(request3.isDone());
        assertEquals(1, client.inFlightRequestCount());

        sender.run(time.absoluteMilliseconds()); // resend request 2.

        assertEquals(2, client.inFlightRequestCount());

        // receive the original response 3. note the expected sequence is still the originally assigned sequence.
        sendIdempotentProducerResponse(2, tp0, Errors.UNKNOWN_PRODUCER_ID, -1, 1010L);
        sender.run(time.absoluteMilliseconds()); // receive response 3

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1011L, 1010L);
        sender.run(time.absoluteMilliseconds());  // receive response 2, don't send request 3 since we can have at most 1 in flight when retrying

        assertTrue(request2.isDone());
        assertFalse(request3.isDone());
        assertFalse(client.hasInFlightRequests());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));
        assertEquals(1011L, request2.get().offset());
        assertEquals(1011L, transactionManager.lastAckedOffset(tp0));

        sender.run(time.absoluteMilliseconds());  // resend request 3.
        assertEquals(1, client.inFlightRequestCount());

        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1012L, 1010L);
        sender.run(time.absoluteMilliseconds());  // receive response 3.

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
        Future<RecordMetadata> request1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(-1, transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.run(time.absoluteMilliseconds());  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(0L, transactionManager.lastAckedSequence(tp0));
        assertEquals(1000L, transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest,
        Future<RecordMetadata> request2 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
        assertEquals(2, transactionManager.sequenceNumber(tp0).longValue());
        assertEquals(0, transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, 10L);
        sender.run(time.absoluteMilliseconds()); // receive response 0, should cause a producerId reset since the logStartOffset < lastAckedOffset
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

        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        // cluster authorization is a fatal error for the producer
        Future<RecordMetadata> future = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return body instanceof ProduceRequest && ((ProduceRequest) body).isIdempotent();
            }
        }, produceResponse(tp0, -1, Errors.CLUSTER_AUTHORIZATION_FAILED, 0));

        sender.run(time.absoluteMilliseconds());
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

        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        // cluster authorization is a fatal error for the producer
        Future<RecordMetadata> future1 = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());

        Future<RecordMetadata> future2 = accumulator.append(tp1, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());

        client.respond(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return body instanceof ProduceRequest && ((ProduceRequest) body).isIdempotent();
            }
        }, produceResponse(tp0, -1, Errors.CLUSTER_AUTHORIZATION_FAILED, 0));

        sender.run(time.absoluteMilliseconds());
        assertTrue(transactionManager.hasFatalError());
        assertFutureFailure(future1, ClusterAuthorizationException.class);

        sender.run(time.absoluteMilliseconds());
        assertFutureFailure(future2, ClusterAuthorizationException.class);

        // Should be fine if the second response eventually returns
        client.respond(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return body instanceof ProduceRequest && ((ProduceRequest) body).isIdempotent();
            }
        }, produceResponse(tp1, 0, Errors.NONE, 0));
        sender.run(time.absoluteMilliseconds());
    }

    @Test
    public void testUnsupportedForMessageFormatInProduceRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);

        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        Future<RecordMetadata> future = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return body instanceof ProduceRequest && ((ProduceRequest) body).isIdempotent();
            }
        }, produceResponse(tp0, -1, Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, 0));

        sender.run(time.absoluteMilliseconds());
        assertFutureFailure(future, UnsupportedForMessageFormatException.class);

        // unsupported for message format is not a fatal error
        assertFalse(transactionManager.hasError());
    }

    @Test
    public void testUnsupportedVersionInProduceRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        setupWithTransactionState(transactionManager);

        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        Future<RecordMetadata> future = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        client.prepareUnsupportedVersionResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                return body instanceof ProduceRequest && ((ProduceRequest) body).isIdempotent();
            }
        });

        sender.run(time.absoluteMilliseconds());
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

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
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

        sender.run(time.absoluteMilliseconds());  // connect.
        sender.run(time.absoluteMilliseconds());  // send.

        sender.run(time.absoluteMilliseconds());  // receive response
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

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());  // connect.
        sender.run(time.absoluteMilliseconds());  // send.
        String id = client.requests().peek().destination();
        Node node = new Node(Integer.valueOf(id), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertTrue("Client ready status should be true", client.isReady(node, 0L));
        client.disconnect(id);
        assertEquals(0, client.inFlightRequestCount());
        assertFalse("Client ready status should be false", client.isReady(node, 0L));

        transactionManager.resetProducerId();
        transactionManager.setProducerIdAndEpoch(new ProducerIdAndEpoch(producerId + 1, (short) 0));
        sender.run(time.absoluteMilliseconds()); // receive error
        sender.run(time.absoluteMilliseconds()); // reconnect
        sender.run(time.absoluteMilliseconds()); // nothing to do, since the pid has changed. We should check the metrics for errors.
        assertEquals("Expected requests to be aborted after pid change", 0, client.inFlightRequestCount());

        KafkaMetric recordErrors = m.metrics().get(senderMetrics.recordErrorRate);
        assertTrue("Expected non-zero value for record send errors", (Double) recordErrors.metricValue() > 0);

        assertTrue(responseFuture.isDone());
        assertEquals(0, (long) transactionManager.sequenceNumber(tp0));
    }

    @Test
    public void testResetWhenOutOfOrderSequenceReceived() throws InterruptedException {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.setProducerIdAndEpoch(new ProducerIdAndEpoch(producerId, (short) 0));
        setupWithTransactionState(transactionManager);

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());  // connect.
        sender.run(time.absoluteMilliseconds());  // send.

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        client.respond(produceResponse(tp0, 0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, 0));

        sender.run(time.absoluteMilliseconds());
        assertTrue(responseFuture.isDone());
        assertEquals(0, sender.inFlightBatches(tp0).size());
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
        sender.run(time.absoluteMilliseconds());

        testSplitBatchAndSend(txnManager, producerIdAndEpoch, tp);
    }

    @SuppressWarnings("deprecation")
    private void testSplitBatchAndSend(TransactionManager txnManager,
                                       ProducerIdAndEpoch producerIdAndEpoch,
                                       TopicPartition tp) throws Exception {
        int maxRetries = 1;
        String topic = tp.topic();
        long deliveryTimeoutMs = 3000L;
        long totalSize = 1024 * 1024;
        String metricGrpName = "producer-metrics";
        // Set a good compression ratio.
        CompressionRatioEstimator.setEstimation(topic, CompressionType.GZIP, 0.2f);
        try (Metrics m = new Metrics()) {
            accumulator = new RecordAccumulator(logContext, batchSize, CompressionType.GZIP,
                0L, 0L, deliveryTimeoutMs, m, metricGrpName, time, new ApiVersions(), txnManager,
                new BufferPool(totalSize, batchSize, metrics, time, "producer-internal-metrics"));
            SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                    senderMetrics, time, REQUEST_TIMEOUT, 1000L, txnManager, new ApiVersions());
            // Create a two broker cluster, with partition 0 on broker 0 and partition 1 on broker 1
            MetadataResponse metadataUpdate1 = TestUtils.metadataUpdateWith(2, Collections.singletonMap(topic, 2));
            client.prepareMetadataUpdate(metadataUpdate1);
            // Send the first message.
            Future<RecordMetadata> f1 =
                    accumulator.append(tp, 0L, "key1".getBytes(), new byte[batchSize / 2], null, null, MAX_BLOCK_TIMEOUT).future;
            Future<RecordMetadata> f2 =
                    accumulator.append(tp, 0L, "key2".getBytes(), new byte[batchSize / 2], null, null, MAX_BLOCK_TIMEOUT).future;
            sender.run(time.absoluteMilliseconds()); // connect
            sender.run(time.absoluteMilliseconds()); // send produce request

            assertEquals("The next sequence should be 2", 2, txnManager.sequenceNumber(tp).longValue());
            String id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            Node node = new Node(Integer.valueOf(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));

            Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<>();
            responseMap.put(tp, new ProduceResponse.PartitionResponse(Errors.MESSAGE_TOO_LARGE));
            client.respond(new ProduceResponse(responseMap));
            sender.run(time.absoluteMilliseconds()); // split and reenqueue
            assertEquals("The next sequence should be 2", 2, txnManager.sequenceNumber(tp).longValue());
            // The compression ratio should have been improved once.
            assertEquals(CompressionType.GZIP.rate - CompressionRatioEstimator.COMPRESSION_RATIO_IMPROVING_STEP,
                    CompressionRatioEstimator.estimation(topic, CompressionType.GZIP), 0.01);
            sender.run(time.absoluteMilliseconds()); // send the first produce request
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

            sender.run(time.absoluteMilliseconds()); // receive
            assertTrue("The future should have been done.", f1.isDone());
            assertEquals("The next sequence number should still be 2", 2, txnManager.sequenceNumber(tp).longValue());
            assertEquals("The last ack'd sequence number should be 0", 0, txnManager.lastAckedSequence(tp));
            assertFalse("The future shouldn't have been done.", f2.isDone());
            assertEquals("Offset of the first message should be 0", 0L, f1.get().offset());
            sender.run(time.absoluteMilliseconds()); // send the seconcd produce request
            id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            node = new Node(Integer.valueOf(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));

            responseMap.put(tp, new ProduceResponse.PartitionResponse(Errors.NONE, 1L, 0L, 0L));
            client.respond(produceRequestMatcher(tp, producerIdAndEpoch, 1, txnManager.isTransactional()),
                    new ProduceResponse(responseMap));

            sender.run(time.absoluteMilliseconds()); // receive
            assertTrue("The future should have been done.", f2.isDone());
            assertEquals("The next sequence number should be 2", 2, txnManager.sequenceNumber(tp).longValue());
            assertEquals("The last ack'd sequence number should be 1", 1, txnManager.lastAckedSequence(tp));
            assertEquals("Offset of the first message should be 1", 1L, f2.get().offset());
            assertTrue("There should be no batch in the accumulator", accumulator.batches().get(tp).isEmpty());
            assertTrue("There should be a split", (Double) (m.metrics().get(senderMetrics.batchSplitRate).metricValue()) > 0);
        }
    }

    @Test
    public void testNoDoubleDeallocation() throws Exception {
        long deliverTimeoutMs = 1500L;
        long totalSize = 1024 * 1024;
        String metricGrpName = "producer-custom-metrics";
        MatchingBufferPool pool = new MatchingBufferPool(totalSize, batchSize, metrics, time, metricGrpName);
        setupWithTransactionState(null, false, pool);

        // Send first ProduceRequest
        Future<RecordMetadata> request1 =
            accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());  // send request
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        time.sleep(deliverTimeoutMs);
        assertFalse(pool.allMatch());

        sender.run(time.absoluteMilliseconds());  // expire the batch
        assertTrue(request1.isDone());
        assertTrue("The batch should have been de-allocated", pool.allMatch());
        assertTrue(pool.allMatch());

        sender.run(time.absoluteMilliseconds());
        assertTrue("The batch should have been de-allocated", pool.allMatch());
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());
    }

    @Test
    public void testInflightBatchesExpireOnDeliveryTimeout() throws InterruptedException {
        long deliveryTimeoutMs = 1500L;
        setupWithTransactionState(null, true, null);

        // Send first ProduceRequest
        Future<RecordMetadata> request = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());  // send request
        assertEquals(1, client.inFlightRequestCount());
        assertEquals("Expect one in-flight batch in accumulator", 1, sender.inFlightBatches(tp0).size());

        Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<>();
        responseMap.put(tp0, new ProduceResponse.PartitionResponse(Errors.NONE, 0L, 0L, 0L));
        client.respond(new ProduceResponse(responseMap));

        time.sleep(deliveryTimeoutMs);
        sender.run(time.absoluteMilliseconds());  // receive first response
        assertEquals("Expect zero in-flight batch in accumulator", 0, sender.inFlightBatches(tp0).size());
        try {
            request.get();
            fail("The expired batch should throw a TimeoutException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testWhenFirstBatchExpireNoSendSecondBatchIfGuaranteeOrder() throws InterruptedException {
        long deliveryTimeoutMs = 1500L;
        setupWithTransactionState(null, true, null);

        // Send first ProduceRequest
        accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT);
        sender.run(time.absoluteMilliseconds());  // send request
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        time.sleep(deliveryTimeoutMs / 2);

        // Send second ProduceRequest
        accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null, MAX_BLOCK_TIMEOUT);
        sender.run(time.absoluteMilliseconds());  // must not send request because the partition is muted
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        time.sleep(deliveryTimeoutMs / 2); // expire the first batch only

        client.respond(produceResponse(tp0, 0L, Errors.NONE, 0, 0L));
        sender.run(time.absoluteMilliseconds());  // receive response (offset=0)
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        sender.run(time.absoluteMilliseconds());  // Drain the second request only this time
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());
    }

    @Test
    public void testExpiredBatchDoesNotRetry() throws Exception {
        long deliverTimeoutMs = 1500L;
        setupWithTransactionState(null, false, null);

        // Send first ProduceRequest
        Future<RecordMetadata> request1 =
            accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(), null, null,
                MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());  // send request
        assertEquals(1, client.inFlightRequestCount());
        time.sleep(deliverTimeoutMs);

        Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<>();
        responseMap.put(tp0, new ProduceResponse.PartitionResponse(Errors.NONE, 0L, 0L, 0L));
        client.respond(produceResponse(tp0, -1, Errors.NOT_LEADER_FOR_PARTITION, -1)); // return a retriable error

        sender.run(time.absoluteMilliseconds());  // expire the batch
        assertTrue(request1.isDone());
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        sender.run(time.absoluteMilliseconds()); // receive first response and do not reenqueue.
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        sender.run(time.absoluteMilliseconds()); // run again and must not send anything.
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());
    }

    @Test
    public void testExpiredBatchDoesNotSplitOnMessageTooLargeError() throws Exception {
        long deliverTimeoutMs = 1500L;
        // create a producer batch with more than one record so it is eligible to split
        Future<RecordMetadata> request1 =
            accumulator.append(tp0, time.absoluteMilliseconds(), "key1".getBytes(), "value1".getBytes(), null, null,
                MAX_BLOCK_TIMEOUT).future;
        Future<RecordMetadata> request2 =
            accumulator.append(tp0, time.absoluteMilliseconds(), "key2".getBytes(), "value2".getBytes(), null, null,
                MAX_BLOCK_TIMEOUT).future;

        sender.run(time.absoluteMilliseconds());  // send request
        assertEquals(1, client.inFlightRequestCount());
        // return a MESSAGE_TOO_LARGE error
        client.respond(produceResponse(tp0, -1, Errors.MESSAGE_TOO_LARGE, -1));

        time.sleep(deliverTimeoutMs);
        // expire the batch and process the response
        sender.run(time.absoluteMilliseconds());
        assertTrue(request1.isDone());
        assertTrue(request2.isDone());
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        // run again and must not split big batch and resend anything.
        sender.run(time.absoluteMilliseconds());
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());
    }

    @Test
    public void testResetNextBatchExpiry() throws Exception {
        client = spy(new MockClient(time, metadata));

        setupWithTransactionState(null);

        accumulator.append(tp0, 0L, "key".getBytes(), "value".getBytes(), null, null,
                MAX_BLOCK_TIMEOUT);

        sender.run(time.absoluteMilliseconds());
        sender.run(time.absoluteMilliseconds());
        time.setCurrentTimeMs(time.absoluteMilliseconds() + accumulator.getDeliveryTimeoutMs() + 1);
        sender.run(time.absoluteMilliseconds());

        InOrder inOrder = inOrder(client);
        inOrder.verify(client, atLeastOnce()).ready(any(), anyLong());
        inOrder.verify(client, atLeastOnce()).newClientRequest(anyString(), any(), anyLong(), anyBoolean(), anyInt(),
                any());
        inOrder.verify(client, atLeastOnce()).send(any(), anyLong());
        inOrder.verify(client).poll(eq(0L), anyLong());
        inOrder.verify(client).poll(eq(accumulator.getDeliveryTimeoutMs()), anyLong());
        inOrder.verify(client).poll(geq(1L), anyLong());

    }

    private class MatchingBufferPool extends BufferPool {
        IdentityHashMap<ByteBuffer, Boolean> allocatedBuffers;

        MatchingBufferPool(long totalSize, int batchSize, Metrics metrics, Time time, String metricGrpName) {
            super(totalSize, batchSize, metrics, time, metricGrpName);
            allocatedBuffers = new IdentityHashMap<>();
        }

        @Override
        public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
            ByteBuffer buffer = super.allocate(size, maxTimeToBlockMs);
            allocatedBuffers.put(buffer, Boolean.TRUE);
            return buffer;
        }

        @Override
        public void deallocate(ByteBuffer buffer, int size) {
            if (!allocatedBuffers.containsKey(buffer)) {
                throw new IllegalStateException("Deallocating a buffer that is not allocated");
            }
            allocatedBuffers.remove(buffer);
            super.deallocate(buffer, size);
        }

        public boolean allMatch() {
            return allocatedBuffers.isEmpty();
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
        setupWithTransactionState(transactionManager, false, null);
    }

    private void setupWithTransactionState(TransactionManager transactionManager, boolean guaranteeOrder, BufferPool customPool) {
        long deliveryTimeoutMs = 1500L;
        long totalSize = 1024 * 1024;
        String metricGrpName = "producer-metrics";
        MetricConfig metricConfig = new MetricConfig().tags(Collections.singletonMap("client-id", CLIENT_ID));
        this.metrics = new Metrics(metricConfig, time);
        BufferPool pool = (customPool == null) ? new BufferPool(totalSize, batchSize, metrics, time, metricGrpName) : customPool;

        this.accumulator = new RecordAccumulator(logContext, batchSize, CompressionType.NONE, 0L, 0L,
                deliveryTimeoutMs, metrics, metricGrpName, time, apiVersions, transactionManager, pool);
        this.senderMetricsRegistry = new SenderMetricsRegistry(this.metrics);
        this.sender = new Sender(logContext, this.client, this.metadata, this.accumulator, guaranteeOrder, MAX_REQUEST_SIZE, ACKS_ALL,
                Integer.MAX_VALUE, this.senderMetricsRegistry, this.time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

        this.client.updateMetadata(TestUtils.metadataUpdateWith(1, Collections.singletonMap("test", 2)));
    }

    private void assertSendFailure(Class<? extends RuntimeException> expectedError) throws Exception {
        Future<RecordMetadata> future = accumulator.append(tp0, time.absoluteMilliseconds(), "key".getBytes(), "value".getBytes(),
                null, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.absoluteMilliseconds());
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
        sender.run(time.absoluteMilliseconds());
    }

    private void doInitTransactions(TransactionManager transactionManager, ProducerIdAndEpoch producerIdAndEpoch) {
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE);
        sender.run(time.absoluteMilliseconds());
        sender.run(time.absoluteMilliseconds());

        prepareInitPidResponse(Errors.NONE, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        sender.run(time.absoluteMilliseconds());
        assertTrue(transactionManager.hasProducerId());
    }

    private void prepareFindCoordinatorResponse(Errors error) {
        client.prepareResponse(new FindCoordinatorResponse(error, metadata.fetch().nodes().get(0)));
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
