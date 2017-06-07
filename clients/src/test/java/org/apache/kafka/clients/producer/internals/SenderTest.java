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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.DelayedReceive;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SenderTest {

    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = 0;
    private static final String CLIENT_ID = "clientId";
    private static final String METRIC_GROUP = "producer-metrics";
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

        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(Errors.NONE, offset, RecordBatch.NO_TIMESTAMP);
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
    public void testQuotaMetrics() throws Exception {
        MockSelector selector = new MockSelector(time);
        Sensor throttleTimeSensor = Sender.throttleTimeSensor(metrics);
        Cluster cluster = TestUtils.singletonCluster("test", 1);
        Node node = cluster.nodes().get(0);
        NetworkClient client = new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                1000, 1000, 64 * 1024, 64 * 1024, 1000,
                time, true, new ApiVersions(), throttleTimeSensor);

        short apiVersionsResponseVersion = ApiKeys.API_VERSIONS.latestVersion();
        ByteBuffer buffer = ApiVersionsResponse.createApiVersionsResponse(400, RecordBatch.CURRENT_MAGIC_VALUE).serialize(apiVersionsResponseVersion, new ResponseHeader(0));
        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
        while (!client.ready(node, time.milliseconds()))
            client.poll(1, time.milliseconds());
        selector.clear();

        for (int i = 1; i <= 3; i++) {
            int throttleTimeMs = 100 * i;
            ProduceRequest.Builder builder = new ProduceRequest.Builder(RecordBatch.CURRENT_MAGIC_VALUE, (short) 1, 1000,
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
        KafkaMetric avgMetric = allMetrics.get(metrics.metricName("produce-throttle-time-avg", METRIC_GROUP, ""));
        KafkaMetric maxMetric = allMetrics.get(metrics.metricName("produce-throttle-time-max", METRIC_GROUP, ""));
        // Throttle times are ApiVersions=400, Produce=(100, 200, 300)
        assertEquals(250, avgMetric.value(), EPS);
        assertEquals(400, maxMetric.value(), EPS);
        client.close();
    }

    @Test
    public void testRetries() throws Exception {
        // create a sender with retries = 1
        int maxRetries = 1;
        Metrics m = new Metrics();
        try {
            Sender sender = new Sender(client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                    maxRetries, m, time, REQUEST_TIMEOUT, 50, null, apiVersions);
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
            completedWithError(future, Errors.NETWORK_EXCEPTION);
        } finally {
            m.close();
        }
    }

    @Test
    public void testSendInOrder() throws Exception {
        int maxRetries = 1;
        Metrics m = new Metrics();
        try {
            Sender sender = new Sender(client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                    m, time, REQUEST_TIMEOUT, 50, null, apiVersions);
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
        assertTrue(future.isDone());
        try {
            future.get();
            fail("Future should have raised ClusterAuthorizationException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ClusterAuthorizationException);
        }

        // cluster authorization is a fatal error for the producer
        assertSendFailure(ClusterAuthorizationException.class);
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
        Sender sender = new Sender(client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                m, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

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
        assertEquals((long) transactionManager.sequenceNumber(tp0), 1L);
    }

    @Test
    public void testAbortRetryWhenProducerIdChanges() throws InterruptedException {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.setProducerIdAndEpoch(new ProducerIdAndEpoch(producerId, (short) 0));
        setupWithTransactionState(transactionManager);
        client.setNode(new Node(1, "localhost", 33343));

        int maxRetries = 10;
        Metrics m = new Metrics();
        Sender sender = new Sender(client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                m, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

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

        transactionManager.setProducerIdAndEpoch(new ProducerIdAndEpoch(producerId + 1, (short) 0));
        sender.run(time.milliseconds()); // receive error
        sender.run(time.milliseconds()); // reconnect
        sender.run(time.milliseconds()); // nothing to do, since the pid has changed. We should check the metrics for errors.
        assertEquals("Expected requests to be aborted after pid change", 0, client.inFlightRequestCount());

        KafkaMetric recordErrors = m.metrics().get(m.metricName("record-error-rate", METRIC_GROUP, ""));
        assertTrue("Expected non-zero value for record send errors", recordErrors.value() > 0);

        assertTrue(responseFuture.isDone());
        assertEquals((long) transactionManager.sequenceNumber(tp0), 0L);
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
        Sender sender = new Sender(client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                m, time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);

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
        TransactionManager txnManager = new TransactionManager("testSplitBatchAndSend", 60000);

        setupWithTransactionState(txnManager);
        doInitTransactions(txnManager, producerIdAndEpoch);

        txnManager.beginTransaction();
        txnManager.maybeAddPartitionToTransaction(tp);
        client.prepareResponse(new AddPartitionsToTxnResponse(0, Collections.singletonMap(tp, Errors.NONE)));
        sender.run(time.milliseconds());

        testSplitBatchAndSend(txnManager, producerIdAndEpoch, tp);
    }

    private void testSplitBatchAndSend(TransactionManager txnManager,
                                       ProducerIdAndEpoch producerIdAndEpoch,
                                       TopicPartition tp) throws Exception {
        int maxRetries = 1;
        String topic = tp.topic();
        // Set a good compression ratio.
        CompressionRatioEstimator.setEstimation(topic, CompressionType.GZIP, 0.2f);
        Metrics m = new Metrics();
        accumulator = new RecordAccumulator(batchSize, 1024 * 1024, CompressionType.GZIP, 0L, 0L, m, time,
                new ApiVersions(), txnManager);
        try {
            Sender sender = new Sender(client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                    m, time, REQUEST_TIMEOUT, 1000L, txnManager, new ApiVersions());
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
            assertEquals("The sequence number should be 0", 0, txnManager.sequenceNumber(tp).longValue());
            String id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            Node node = new Node(Integer.valueOf(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));

            Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<>();
            responseMap.put(tp, new ProduceResponse.PartitionResponse(Errors.MESSAGE_TOO_LARGE));
            client.respond(new ProduceResponse(responseMap));
            sender.run(time.milliseconds()); // split and reenqueue
            // The compression ratio should have been improved once.
            assertEquals(CompressionType.GZIP.rate - CompressionRatioEstimator.COMPRESSION_RATIO_IMPROVING_STEP,
                    CompressionRatioEstimator.estimation(topic, CompressionType.GZIP), 0.01);
            sender.run(time.milliseconds()); // send produce request
            assertEquals("The sequence number should be 0", 0, txnManager.sequenceNumber(tp).longValue());
            assertFalse("The future shouldn't have been done.", f1.isDone());
            assertFalse("The future shouldn't have been done.", f2.isDone());
            id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            node = new Node(Integer.valueOf(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));

            responseMap.put(tp, new ProduceResponse.PartitionResponse(Errors.NONE, 0L, 0L));
            client.respond(produceRequestMatcher(tp, producerIdAndEpoch, 0, txnManager.isTransactional()),
                    new ProduceResponse(responseMap));

            sender.run(time.milliseconds()); // receive
            assertTrue("The future should have been done.", f1.isDone());
            assertEquals("The sequence number should be 1", 1, txnManager.sequenceNumber(tp).longValue());
            assertFalse("The future shouldn't have been done.", f2.isDone());
            assertEquals("Offset of the first message should be 0", 0L, f1.get().offset());
            sender.run(time.milliseconds()); // send produce request
            id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            node = new Node(Integer.valueOf(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));

            responseMap.put(tp, new ProduceResponse.PartitionResponse(Errors.NONE, 1L, 0L));
            client.respond(produceRequestMatcher(tp, producerIdAndEpoch, 1, txnManager.isTransactional()),
                    new ProduceResponse(responseMap));

            sender.run(time.milliseconds()); // receive
            assertTrue("The future should have been done.", f2.isDone());
            assertEquals("The sequence number should be 2", 2, txnManager.sequenceNumber(tp).longValue());
            assertEquals("Offset of the first message should be 1", 1L, f2.get().offset());
            assertTrue("There should be no batch in the accumulator", accumulator.batches().get(tp).isEmpty());

            assertTrue("There should be a split",
                    m.metrics().get(m.metricName("batch-split-rate", "producer-metrics")).value() > 0);
        } finally {
            m.close();
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

    private void completedWithError(Future<RecordMetadata> future, Errors error) throws Exception {
        assertTrue("Request should be completed", future.isDone());
        try {
            future.get();
            fail("Should have thrown an exception.");
        } catch (ExecutionException e) {
            assertEquals(error.exception().getClass(), e.getCause().getClass());
        }
    }

    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs) {
        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(error, offset, RecordBatch.NO_TIMESTAMP);
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = Collections.singletonMap(tp, resp);
        return new ProduceResponse(partResp, throttleTimeMs);
    }

    private void setupWithTransactionState(TransactionManager transactionManager) {
        Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("client-id", CLIENT_ID);
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        this.metrics = new Metrics(metricConfig, time);
        this.accumulator = new RecordAccumulator(batchSize, 1024 * 1024, CompressionType.NONE, 0L, 0L, metrics, time,
                apiVersions, transactionManager);
        this.sender = new Sender(this.client, this.metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL,
                MAX_RETRIES, this.metrics, this.time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);
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

}
