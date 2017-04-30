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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitPidRequest;
import org.apache.kafka.common.requests.InitPidResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransactionManagerTest {
    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = 0;
    private static final String CLIENT_ID = "clientId";
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 1000;
    private final String transactionalId = "foobar";
    private final int transactionTimeoutMs = 1121;

    private TopicPartition tp0 = new TopicPartition("test", 0);
    private TopicPartition tp1 = new TopicPartition("test", 1);
    private MockTime time = new MockTime();
    private MockClient client = new MockClient(time);

    private Metadata metadata = new Metadata(0, Long.MAX_VALUE, true, new ClusterResourceListeners());
    private ApiVersions apiVersions = new ApiVersions();
    private Cluster cluster = TestUtils.singletonCluster("test", 2);
    private RecordAccumulator accumulator = null;
    private Sender sender = null;
    private TransactionManager transactionManager = null;
    private Node brokerNode = null;

    @Before
    public void setup() {
        Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("client-id", CLIENT_ID);
        int batchSize = 16 * 1024;
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        this.brokerNode = new Node(0, "localhost", 2211);
        this.transactionManager = new TransactionManager(transactionalId, transactionTimeoutMs);
        Metrics metrics = new Metrics(metricConfig, time);
        this.accumulator = new RecordAccumulator(batchSize, 1024 * 1024, CompressionType.NONE, 0L, 0L, metrics, time, apiVersions, transactionManager);
        this.sender = new Sender(this.client,
                this.metadata,
                this.accumulator,
                true,
                MAX_REQUEST_SIZE,
                ACKS_ALL,
                MAX_RETRIES,
                metrics,
                this.time,
                REQUEST_TIMEOUT,
                50,
                transactionManager,
                apiVersions);
        this.metadata.update(this.cluster, Collections.<String>emptySet(), time.milliseconds());
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidSequenceIncrement() {
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.incrementSequenceNumber(tp0, 3333);
    }

    @Test
    public void testDefaultSequenceNumber() {
        TransactionManager transactionManager = new TransactionManager();
        assertEquals((int) transactionManager.sequenceNumber(tp0), 0);
        transactionManager.incrementSequenceNumber(tp0, 3);
        assertEquals((int) transactionManager.sequenceNumber(tp0), 3);
    }


    @Test
    public void testProducerIdReset() {
        TransactionManager transactionManager = new TransactionManager();
        assertEquals((int) transactionManager.sequenceNumber(tp0), 0);
        transactionManager.incrementSequenceNumber(tp0, 3);
        assertEquals((int) transactionManager.sequenceNumber(tp0), 3);
        transactionManager.resetProducerId();
        assertEquals((int) transactionManager.sequenceNumber(tp0), 0);
    }

    @Test
    public void testBasicTransaction() throws InterruptedException {
        client.setNode(brokerNode);
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);

        sender.run(time.milliseconds());  // find coordinator
        assertEquals(brokerNode, transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NONE, false, pid, epoch);

        sender.run(time.milliseconds());  // get pid.

        assertTrue(transactionManager.hasPid());
        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                                                                   "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        prepareProduceResponse(Errors.NONE, pid, epoch);
        assertFalse(transactionManager.transactionContainsPartition(tp0));
        sender.run(time.milliseconds());  // send addPartitions.
        // Check that only addPartitions was sent.
        assertTrue(transactionManager.transactionContainsPartition(tp0));
        assertFalse(responseFuture.isDone());

        sender.run(time.milliseconds());  // send produce request.
        assertTrue(responseFuture.isDone());

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));
        final String consumerGroupId = "myconsumergroup";
        FutureTransactionalResult addOffsetsResult = transactionManager.sendOffsetsToTransaction(offsets, consumerGroupId);

        assertFalse(transactionManager.hasPendingOffsetCommits());

        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                AddOffsetsToTxnRequest addOffsetsToTxnRequest = (AddOffsetsToTxnRequest) body;
                assertEquals(consumerGroupId, addOffsetsToTxnRequest.consumerGroupId());
                assertEquals(transactionalId, addOffsetsToTxnRequest.transactionalId());
                assertEquals(pid, addOffsetsToTxnRequest.producerId());
                assertEquals(epoch, addOffsetsToTxnRequest.producerEpoch());
                return true;
            }
        }, new AddOffsetsToTxnResponse(Errors.NONE));

        sender.run(time.milliseconds());  // Send AddOffsetsRequest
        assertTrue(transactionManager.hasPendingOffsetCommits());  // We should now have created and queued the offset commit request.
        assertFalse(addOffsetsResult.isDone());

        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp1, Errors.NONE);

        prepareFindCoordinatorResponse(Errors.NONE, false, FindCoordinatorRequest.CoordinatorType.GROUP, consumerGroupId);

        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) body;
                assertEquals(consumerGroupId, txnOffsetCommitRequest.consumerGroupId());
                assertEquals(pid, txnOffsetCommitRequest.producerId());
                assertEquals(epoch, txnOffsetCommitRequest.producerEpoch());
                return true;
            }
        }, new TxnOffsetCommitResponse(txnOffsetCommitResponse));

        assertEquals(null, transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.GROUP));
        sender.run(time.milliseconds());  // try to send TxnOffsetCommitRequest, but find we don't have a group coordinator.
        sender.run(time.milliseconds());  // send find coordinator for group request
        assertNotNull(transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.GROUP));
        assertTrue(transactionManager.hasPendingOffsetCommits());

        sender.run(time.milliseconds());  // send TxnOffsetCommitRequest commit.

        assertFalse(transactionManager.hasPendingOffsetCommits());
        assertTrue(addOffsetsResult.isDone());  // We should only be done after both RPCs complete.

        transactionManager.beginCommittingTransaction();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        sender.run(time.milliseconds());  // commit.

        assertFalse(transactionManager.isInTransaction());
        assertFalse(transactionManager.isCompletingTransaction());
        assertFalse(transactionManager.transactionContainsPartition(tp0));
    }

    @Test
    public void testDisconnectAndRetry() {
        client.setNode(brokerNode);
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, true, FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());  // find coordinator, connection lost.

        prepareFindCoordinatorResponse(Errors.NONE, false, FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());  // find coordinator
        assertEquals(brokerNode, transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION));
    }

    @Test
    public void testCoordinatorLost() {
        client.setNode(brokerNode);
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        FutureTransactionalResult initPidResult = transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());  // find coordinator
        assertEquals(brokerNode, transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NOT_COORDINATOR, false, pid, epoch);
        sender.run(time.milliseconds());  // send pid, get not coordinator. Should resend the FindCoordinator and InitPid requests

        assertEquals(null, transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isDone());
        assertFalse(transactionManager.hasPid());

        prepareFindCoordinatorResponse(Errors.NONE, false, FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());
        assertEquals(brokerNode, transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isDone());
        prepareInitPidResponse(Errors.NONE, false, pid, epoch);
        sender.run(time.milliseconds());  // get pid and epoch

        assertTrue(initPidResult.isDone()); // The future should only return after the second round of retries succeed.
        assertTrue(transactionManager.hasPid());
        assertEquals(pid, transactionManager.pidAndEpoch().producerId);
        assertEquals(epoch, transactionManager.pidAndEpoch().epoch);
    }

    @Test
    public void testFlushPendingPartitionsOnCommit() throws InterruptedException {
        client.setNode(brokerNode);
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);

        sender.run(time.milliseconds());  // find coordinator
        assertEquals(brokerNode, transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NONE, false, pid, epoch);

        sender.run(time.milliseconds());  // get pid.

        assertTrue(transactionManager.hasPid());

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());

        FutureTransactionalResult commitResult = transactionManager.beginCommittingTransaction();

        // we have an append, an add partitions request, and now also an endtxn.
        // The order should be:
        //  1. Add Partitions
        //  2. Produce
        //  3. EndTxn.
        assertFalse(transactionManager.transactionContainsPartition(tp0));
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        sender.run(time.milliseconds());  // AddPartitions.
        assertTrue(transactionManager.transactionContainsPartition(tp0));
        assertFalse(responseFuture.isDone());
        assertFalse(commitResult.isDone());

        prepareProduceResponse(Errors.NONE, pid, epoch);
        sender.run(time.milliseconds());  // Produce.
        assertTrue(responseFuture.isDone());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        assertFalse(commitResult.isDone());
        assertTrue(transactionManager.isInTransaction());
        assertTrue(transactionManager.isCompletingTransaction());

        sender.run(time.milliseconds());
        assertTrue(commitResult.isDone());
        assertFalse(transactionManager.isInTransaction());
    }

    @Test
    public void testMultipleAddPartitionsPerForOneProduce() throws InterruptedException {
        client.setNode(brokerNode);
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);

        sender.run(time.milliseconds());  // find coordinator
        assertEquals(brokerNode, transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NONE, false, pid, epoch);

        sender.run(time.milliseconds());  // get pid.

        assertTrue(transactionManager.hasPid());
        transactionManager.beginTransaction();
        // User does one producer.sed
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        assertFalse(transactionManager.transactionContainsPartition(tp0));

        // Sender flushes one add partitions. The produce goes next.
        sender.run(time.milliseconds());  // send addPartitions.
        // Check that only addPartitions was sent.
        assertTrue(transactionManager.transactionContainsPartition(tp0));

        // In the mean time, the user does a second produce to a different partition
        transactionManager.maybeAddPartitionToTransaction(tp1);
        Future<RecordMetadata> secondResponseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp1, epoch, pid);
        prepareProduceResponse(Errors.NONE, pid, epoch);

        assertFalse(transactionManager.transactionContainsPartition(tp1));

        assertFalse(responseFuture.isDone());
        assertFalse(secondResponseFuture.isDone());

        // The second add partitionsh should go out here.
        sender.run(time.milliseconds());  // send second add partitions request
        assertTrue(transactionManager.transactionContainsPartition(tp1));

        assertFalse(responseFuture.isDone());
        assertFalse(secondResponseFuture.isDone());

        // Finally we get to the produce.
        sender.run(time.milliseconds());  // send produce request

        assertTrue(responseFuture.isDone());
        assertTrue(secondResponseFuture.isDone());
    }

    @Test(expected = ExecutionException.class)
    public void testProducerFencedException() throws InterruptedException, ExecutionException {
        client.setNode(brokerNode);
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);

        sender.run(time.milliseconds());  // find coordinator
        assertEquals(brokerNode, transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NONE, false, pid, epoch);

        sender.run(time.milliseconds());  // get pid.

        assertTrue(transactionManager.hasPid());
        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.INVALID_PRODUCER_EPOCH, pid, epoch);
        sender.run(time.milliseconds()); // Add partitions.

        sender.run(time.milliseconds());  // send produce.

        responseFuture.get();
    }

    @Test
    public void testDisallowCommitOnProduceFailure() throws InterruptedException {
        client.setNode(brokerNode);
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);

        sender.run(time.milliseconds());  // find coordinator
        assertEquals(brokerNode, transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NONE, false, pid, epoch);

        sender.run(time.milliseconds());  // get pid.

        assertTrue(transactionManager.hasPid());
        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, new MockCallback(transactionManager), MAX_BLOCK_TIMEOUT).future;

        FutureTransactionalResult commitResult = transactionManager.beginCommittingTransaction();
        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, pid, epoch);

        sender.run(time.milliseconds());  // Send AddPartitionsRequest
        assertFalse(commitResult.isDone());

        sender.run(time.milliseconds());  // Send Produce Request, returns OutOfOrderSequenceException.
        sender.run(time.milliseconds());  // try to commit.
        assertTrue(commitResult.isDone());  // commit should be cancelled with exception without being sent.

        try {
            commitResult.get();
            fail();  // the get() must throw an exception.
        } catch (RuntimeException e) {
            assertTrue(e instanceof KafkaException);
        }

        // Commit is not allowed, so let's abort and try again.
        FutureTransactionalResult abortResult = transactionManager.beginAbortingTransaction();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        sender.run(time.milliseconds());  // Send abort request. It is valid to transition from ERROR to ABORT

        assertTrue(abortResult.isDone());
        assertTrue(abortResult.get().isSuccessful());
        assertTrue(transactionManager.isReadyForTransaction());  // make sure we are ready for a transaction now.
    }

    private static class MockCallback implements Callback {
        private final TransactionManager transactionManager;
        public MockCallback(TransactionManager transactionManager) {
            this.transactionManager = transactionManager;
        }
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null && transactionManager != null) {
                transactionManager.maybeSetError(exception);
            }
        }
    }

    private void prepareFindCoordinatorResponse(Errors error, boolean shouldDisconnect,
                                                final FindCoordinatorRequest.CoordinatorType coordinatorType,
                                                final String coordinatorKey) {
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                FindCoordinatorRequest findCoordinatorRequest = (FindCoordinatorRequest) body;
                assertEquals(findCoordinatorRequest.coordinatorType(), coordinatorType);
                assertEquals(findCoordinatorRequest.coordinatorKey(), coordinatorKey);
                return true;
            }
        }, new FindCoordinatorResponse(error, brokerNode), shouldDisconnect);
    }

    private void prepareInitPidResponse(Errors error, boolean shouldDisconnect, long pid, short epoch) {
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                InitPidRequest initPidRequest = (InitPidRequest) body;
                assertEquals(initPidRequest.transactionalId(), transactionalId);
                assertEquals(initPidRequest.transactionTimeoutMs(), transactionTimeoutMs);
                return true;
            }
        }, new InitPidResponse(error, pid, epoch), shouldDisconnect);
    }

    private void prepareProduceResponse(Errors error, final long pid, final short epoch) {
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                ProduceRequest produceRequest = (ProduceRequest) body;
                MemoryRecords records = produceRequest.partitionRecordsOrFail().get(tp0);
                assertNotNull(records);
                Iterator<MutableRecordBatch> batchIterator = records.batches().iterator();
                assertTrue(batchIterator.hasNext());
                MutableRecordBatch batch = batchIterator.next();
                assertFalse(batchIterator.hasNext());
                assertTrue(batch.isTransactional());
                assertEquals(pid, batch.producerId());
                assertEquals(epoch, batch.producerEpoch());
                assertEquals(transactionalId, produceRequest.transactionalId());
                return true;
            }
        }, produceResponse(tp0, 0, error, 0));

    }

    private void prepareAddPartitionsToTxnResponse(Errors error, final TopicPartition topicPartition, final short epoch, final long pid) {
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                AddPartitionsToTxnRequest addPartitionsToTxnRequest = (AddPartitionsToTxnRequest) body;
                assertEquals(pid, addPartitionsToTxnRequest.producerId());
                assertEquals(epoch, addPartitionsToTxnRequest.producerEpoch());
                assertEquals(Arrays.asList(topicPartition), addPartitionsToTxnRequest.partitions());
                assertEquals(transactionalId, addPartitionsToTxnRequest.transactionalId());
                return true;
            }
        }, new AddPartitionsToTxnResponse(error));
    }

    private void prepareEndTxnResponse(Errors error, final TransactionResult result, final long pid, final short epoch) {
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                EndTxnRequest endTxnRequest = (EndTxnRequest) body;
                assertEquals(transactionalId, endTxnRequest.transactionalId());
                assertEquals(pid, endTxnRequest.producerId());
                assertEquals(epoch, endTxnRequest.producerEpoch());
                assertEquals(result, endTxnRequest.command());
                return true;
            }
        }, new EndTxnResponse(error));
    }

    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs) {
        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(error, offset, RecordBatch.NO_TIMESTAMP);
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = Collections.singletonMap(tp, resp);
        return new ProduceResponse(partResp, throttleTimeMs);
    }

}
