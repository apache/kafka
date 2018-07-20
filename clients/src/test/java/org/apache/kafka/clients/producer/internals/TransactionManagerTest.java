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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
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
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransactionManagerTest {
    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = Integer.MAX_VALUE;
    private static final String CLIENT_ID = "clientId";
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 1000;
    private static final long DEFAULT_RETRY_BACKOFF_MS = 100L;
    private final String transactionalId = "foobar";
    private final int transactionTimeoutMs = 1121;

    private final String topic = "test";
    private TopicPartition tp0 = new TopicPartition(topic, 0);
    private TopicPartition tp1 = new TopicPartition(topic, 1);
    private MockTime time = new MockTime();
    private MockClient client = new MockClient(time);

    private Metadata metadata = new Metadata(0, Long.MAX_VALUE, true, true, new ClusterResourceListeners());
    private ApiVersions apiVersions = new ApiVersions();
    private Cluster cluster = TestUtils.singletonCluster("test", 2);
    private RecordAccumulator accumulator = null;
    private Sender sender = null;
    private TransactionManager transactionManager = null;
    private Node brokerNode = null;
    private final LogContext logContext = new LogContext();

    @Before
    public void setup() {
        Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("client-id", CLIENT_ID);
        int batchSize = 16 * 1024;
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        this.brokerNode = new Node(0, "localhost", 2211);
        this.transactionManager = new TransactionManager(logContext, transactionalId, transactionTimeoutMs,
                DEFAULT_RETRY_BACKOFF_MS);
        Metrics metrics = new Metrics(metricConfig, time);
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(metrics);

        this.accumulator = new RecordAccumulator(logContext, batchSize, 1024 * 1024, CompressionType.NONE, 0L, 0L, metrics, time, apiVersions, transactionManager);
        this.sender = new Sender(logContext, this.client, this.metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL,
                MAX_RETRIES, senderMetrics, this.time, REQUEST_TIMEOUT, 50, transactionManager, apiVersions);
        this.metadata.update(this.cluster, Collections.<String>emptySet(), time.milliseconds());
        client.setNode(brokerNode);
    }

    @Test
    public void testSenderShutdownWithPendingAddPartitions() throws Exception {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartitionToTransaction(tp0);
        FutureRecordMetadata sendFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        prepareProduceResponse(Errors.NONE, pid, epoch);

        sender.initiateClose();
        sender.run();

        assertTrue(sendFuture.isDone());
    }

    @Test
    public void testEndTxnNotSentIfIncompleteBatches() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        sender.run(time.milliseconds());
        assertTrue(transactionManager.isPartitionAdded(tp0));

        transactionManager.beginCommit();
        assertNull(transactionManager.nextRequestHandler(true));
        assertTrue(transactionManager.nextRequestHandler(false).isEndTxn());
    }

    @Test(expected = IllegalStateException.class)
    public void testFailIfNotReadyForSendNoProducerId() {
        transactionManager.failIfNotReadyForSend();
    }

    @Test
    public void testFailIfNotReadyForSendIdempotentProducer() {
        TransactionManager idempotentTransactionManager = new TransactionManager();
        idempotentTransactionManager.failIfNotReadyForSend();
    }

    @Test(expected = KafkaException.class)
    public void testFailIfNotReadyForSendIdempotentProducerFatalError() {
        TransactionManager idempotentTransactionManager = new TransactionManager();
        idempotentTransactionManager.transitionToFatalError(new KafkaException());
        idempotentTransactionManager.failIfNotReadyForSend();
    }

    @Test(expected = IllegalStateException.class)
    public void testFailIfNotReadyForSendNoOngoingTransaction() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.failIfNotReadyForSend();
    }

    @Test(expected = KafkaException.class)
    public void testFailIfNotReadyForSendAfterAbortableError() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        transactionManager.transitionToAbortableError(new KafkaException());
        transactionManager.failIfNotReadyForSend();
    }

    @Test(expected = KafkaException.class)
    public void testFailIfNotReadyForSendAfterFatalError() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.transitionToFatalError(new KafkaException());
        transactionManager.failIfNotReadyForSend();
    }

    @Test
    public void testHasOngoingTransactionSuccessfulAbort() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions(pid, epoch);
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        sender.run(time.milliseconds());

        transactionManager.beginAbort();
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        sender.run(time.milliseconds());
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testHasOngoingTransactionSuccessfulCommit() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions(pid, epoch);
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        sender.run(time.milliseconds());

        transactionManager.beginCommit();
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        sender.run(time.milliseconds());
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testHasOngoingTransactionAbortableError() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions(pid, epoch);
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        sender.run(time.milliseconds());

        transactionManager.transitionToAbortableError(new KafkaException());
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.beginAbort();
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        sender.run(time.milliseconds());
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testHasOngoingTransactionFatalError() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions(pid, epoch);
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        sender.run(time.milliseconds());

        transactionManager.transitionToFatalError(new KafkaException());
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testMaybeAddPartitionToTransaction() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.isPartitionAdded(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        sender.run(time.milliseconds());

        assertFalse(transactionManager.hasPartitionsToAdd());
        assertTrue(transactionManager.isPartitionAdded(partition));
        assertFalse(transactionManager.isPartitionPendingAdd(partition));

        // adding the partition again should not have any effect
        transactionManager.maybeAddPartitionToTransaction(partition);
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertTrue(transactionManager.isPartitionAdded(partition));
        assertFalse(transactionManager.isPartitionPendingAdd(partition));
    }

    @Test
    public void testAddPartitionToTransactionOverridesRetryBackoffForConcurrentTransactions() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.isPartitionAdded(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.CONCURRENT_TRANSACTIONS);
        sender.run(time.milliseconds());

        TransactionManager.TxnRequestHandler handler = transactionManager.nextRequestHandler(false);
        assertNotNull(handler);
        assertEquals(20, handler.retryBackoffMs());
    }

    @Test
    public void testAddPartitionToTransactionRetainsRetryBackoffForRegularRetriableError() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.isPartitionAdded(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.COORDINATOR_NOT_AVAILABLE);
        sender.run(time.milliseconds());

        TransactionManager.TxnRequestHandler handler = transactionManager.nextRequestHandler(false);
        assertNotNull(handler);
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, handler.retryBackoffMs());
    }

    @Test
    public void testAddPartitionToTransactionRetainsRetryBackoffWhenPartitionsAlreadyAdded() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.isPartitionAdded(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        sender.run(time.milliseconds());
        assertTrue(transactionManager.isPartitionAdded(partition));

        TopicPartition otherPartition = new TopicPartition("foo", 1);
        transactionManager.maybeAddPartitionToTransaction(otherPartition);

        prepareAddPartitionsToTxn(otherPartition, Errors.CONCURRENT_TRANSACTIONS);
        TransactionManager.TxnRequestHandler handler = transactionManager.nextRequestHandler(false);
        assertNotNull(handler);
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, handler.retryBackoffMs());
    }

    @Test(expected = IllegalStateException.class)
    public void testMaybeAddPartitionToTransactionBeforeInitTransactions() {
        transactionManager.maybeAddPartitionToTransaction(new TopicPartition("foo", 0));
    }

    @Test(expected = IllegalStateException.class)
    public void testMaybeAddPartitionToTransactionBeforeBeginTransaction() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.maybeAddPartitionToTransaction(new TopicPartition("foo", 0));
    }

    @Test(expected = KafkaException.class)
    public void testMaybeAddPartitionToTransactionAfterAbortableError() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        transactionManager.transitionToAbortableError(new KafkaException());
        transactionManager.maybeAddPartitionToTransaction(new TopicPartition("foo", 0));
    }

    @Test(expected = KafkaException.class)
    public void testMaybeAddPartitionToTransactionAfterFatalError() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.transitionToFatalError(new KafkaException());
        transactionManager.maybeAddPartitionToTransaction(new TopicPartition("foo", 0));
    }

    @Test
    public void testIsSendToPartitionAllowedWithPendingPartitionAfterAbortableError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        transactionManager.transitionToAbortableError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithInFlightPartitionAddAfterAbortableError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        // Send the AddPartitionsToTxn request and leave it in-flight
        sender.run(time.milliseconds());
        transactionManager.transitionToAbortableError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithPendingPartitionAfterFatalError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        transactionManager.transitionToFatalError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithInFlightPartitionAddAfterFatalError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        // Send the AddPartitionsToTxn request and leave it in-flight
        sender.run(time.milliseconds());
        transactionManager.transitionToFatalError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithAddedPartitionAfterAbortableError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();

        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        sender.run(time.milliseconds());
        assertFalse(transactionManager.hasPartitionsToAdd());
        transactionManager.transitionToAbortableError(new KafkaException());

        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithAddedPartitionAfterFatalError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        sender.run(time.milliseconds());
        assertFalse(transactionManager.hasPartitionsToAdd());
        transactionManager.transitionToFatalError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithPartitionNotAdded() {
        final long pid = 13131L;
        final short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
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
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        prepareProduceResponse(Errors.NONE, pid, epoch);
        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        sender.run(time.milliseconds());  // send addPartitions.
        // Check that only addPartitions was sent.
        assertTrue(transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(responseFuture.isDone());

        sender.run(time.milliseconds());  // send produce request.
        assertTrue(responseFuture.isDone());

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));
        final String consumerGroupId = "myconsumergroup";
        TransactionalRequestResult addOffsetsResult = transactionManager.sendOffsetsToTransaction(offsets, consumerGroupId);

        assertFalse(transactionManager.hasPendingOffsetCommits());

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);

        sender.run(time.milliseconds());  // Send AddOffsetsRequest
        assertTrue(transactionManager.hasPendingOffsetCommits());  // We should now have created and queued the offset commit request.
        assertFalse(addOffsetsResult.isCompleted()); // the result doesn't complete until TxnOffsetCommit returns

        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp1, Errors.NONE);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, txnOffsetCommitResponse);

        assertEquals(null, transactionManager.coordinator(CoordinatorType.GROUP));
        sender.run(time.milliseconds());  // try to send TxnOffsetCommitRequest, but find we don't have a group coordinator.
        sender.run(time.milliseconds());  // send find coordinator for group request
        assertNotNull(transactionManager.coordinator(CoordinatorType.GROUP));
        assertTrue(transactionManager.hasPendingOffsetCommits());

        sender.run(time.milliseconds());  // send TxnOffsetCommitRequest commit.

        assertFalse(transactionManager.hasPendingOffsetCommits());
        assertTrue(addOffsetsResult.isCompleted());  // We should only be done after both RPCs complete.

        transactionManager.beginCommit();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        sender.run(time.milliseconds());  // commit.

        assertFalse(transactionManager.hasOngoingTransaction());
        assertFalse(transactionManager.isCompleting());
        assertFalse(transactionManager.transactionContainsPartition(tp0));
    }

    @Test
    public void testDisconnectAndRetry() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, true, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());  // find coordinator, connection lost.

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());  // find coordinator
        sender.run(time.milliseconds());
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
    }

    @Test
    public void testUnsupportedFindCoordinator() {
        transactionManager.initializeTransactions();
        client.prepareUnsupportedVersionResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                FindCoordinatorRequest findCoordinatorRequest = (FindCoordinatorRequest) body;
                assertEquals(findCoordinatorRequest.coordinatorType(), CoordinatorType.TRANSACTION);
                assertEquals(findCoordinatorRequest.coordinatorKey(), transactionalId);
                return true;
            }
        });

        sender.run(time.milliseconds()); // InitProducerRequest is queued
        sender.run(time.milliseconds()); // FindCoordinator is queued after peeking InitProducerRequest
        assertTrue(transactionManager.hasFatalError());
        assertTrue(transactionManager.lastError() instanceof UnsupportedVersionException);
    }

    @Test
    public void testUnsupportedInitTransactions() {
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds()); // InitProducerRequest is queued
        sender.run(time.milliseconds()); // FindCoordinator is queued after peeking InitProducerRequest

        assertFalse(transactionManager.hasError());
        assertNotNull(transactionManager.coordinator(CoordinatorType.TRANSACTION));

        client.prepareUnsupportedVersionResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                InitProducerIdRequest initProducerIdRequest = (InitProducerIdRequest) body;
                assertEquals(initProducerIdRequest.transactionalId(), transactionalId);
                assertEquals(initProducerIdRequest.transactionTimeoutMs(), transactionTimeoutMs);
                return true;
            }
        });

        sender.run(time.milliseconds()); // InitProducerRequest is dequeued
        assertTrue(transactionManager.hasFatalError());
        assertTrue(transactionManager.lastError() instanceof UnsupportedVersionException);
    }

    @Test
    public void testUnsupportedForMessageFormatInTxnOffsetCommit() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), consumerGroupId);

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        sender.run(time.milliseconds());  // AddOffsetsToTxn Handled, TxnOffsetCommit Enqueued
        sender.run(time.milliseconds());  // FindCoordinator Enqueued

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        sender.run(time.milliseconds());  // FindCoordinator Returned

        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, singletonMap(tp, Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT));
        sender.run(time.milliseconds());  // TxnOffsetCommit Handled

        assertTrue(transactionManager.hasError());
        assertTrue(transactionManager.lastError() instanceof UnsupportedForMessageFormatException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof UnsupportedForMessageFormatException);
        assertFatalError(UnsupportedForMessageFormatException.class);
    }

    @Test
    public void testLookupCoordinatorOnDisconnectAfterSend() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());  // find coordinator
        sender.run(time.milliseconds());
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        prepareInitPidResponse(Errors.NONE, true, pid, epoch);
        // send pid to coordinator, should get disconnected before receiving the response, and resend the
        // FindCoordinator and InitPid requests.
        sender.run(time.milliseconds());

        assertEquals(null, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        assertFalse(transactionManager.hasProducerId());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        prepareInitPidResponse(Errors.NONE, false, pid, epoch);
        sender.run(time.milliseconds());  // get pid and epoch

        assertTrue(initPidResult.isCompleted()); // The future should only return after the second round of retries succeed.
        assertTrue(transactionManager.hasProducerId());
        assertEquals(pid, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(epoch, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testLookupCoordinatorOnDisconnectBeforeSend() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());  // one loop to realize we need a coordinator.
        sender.run(time.milliseconds());  // next loop to find coordintor.
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        client.disconnect(brokerNode.idString());
        client.blackout(brokerNode, 100);
        // send pid to coordinator. Should get disconnected before the send and resend the FindCoordinator
        // and InitPid requests.
        sender.run(time.milliseconds());
        time.sleep(110);  // waiting for the blackout period for the node to expire.

        assertEquals(null, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        assertFalse(transactionManager.hasProducerId());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        prepareInitPidResponse(Errors.NONE, false, pid, epoch);
        sender.run(time.milliseconds());  // get pid and epoch

        assertTrue(initPidResult.isCompleted()); // The future should only return after the second round of retries succeed.
        assertTrue(transactionManager.hasProducerId());
        assertEquals(pid, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(epoch, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testLookupCoordinatorOnNotCoordinatorError() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());  // find coordinator
        sender.run(time.milliseconds());
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NOT_COORDINATOR, false, pid, epoch);
        sender.run(time.milliseconds());  // send pid, get not coordinator. Should resend the FindCoordinator and InitPid requests

        assertEquals(null, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        assertFalse(transactionManager.hasProducerId());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        prepareInitPidResponse(Errors.NONE, false, pid, epoch);
        sender.run(time.milliseconds());  // get pid and epoch

        assertTrue(initPidResult.isCompleted()); // The future should only return after the second round of retries succeed.
        assertTrue(transactionManager.hasProducerId());
        assertEquals(pid, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(epoch, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInFindCoordinator() {
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, false,
                CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());  // find coordinator
        sender.run(time.milliseconds());

        assertTrue(transactionManager.hasError());
        assertTrue(transactionManager.lastError() instanceof TransactionalIdAuthorizationException);

        sender.run(time.milliseconds()); // one more run to fail the InitProducerId future
        assertTrue(initPidResult.isCompleted());
        assertFalse(initPidResult.isSuccessful());
        assertTrue(initPidResult.error() instanceof TransactionalIdAuthorizationException);

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInInitProducerId() {
        final long pid = 13131L;
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());  // find coordinator
        sender.run(time.milliseconds());
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, false, pid, RecordBatch.NO_PRODUCER_EPOCH);
        sender.run(time.milliseconds());

        assertTrue(transactionManager.hasError());
        assertTrue(initPidResult.isCompleted());
        assertFalse(initPidResult.isSuccessful());
        assertTrue(initPidResult.error() instanceof TransactionalIdAuthorizationException);

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testGroupAuthorizationFailureInFindCoordinator() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(new TopicPartition("foo", 0), new OffsetAndMetadata(39L)), consumerGroupId);

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        sender.run(time.milliseconds());  // AddOffsetsToTxn Handled, TxnOffsetCommit Enqueued
        sender.run(time.milliseconds());  // FindCoordinator Enqueued

        prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED, false, CoordinatorType.GROUP, consumerGroupId);
        sender.run(time.milliseconds());  // FindCoordinator Failed
        sender.run(time.milliseconds());  // TxnOffsetCommit Aborted
        assertTrue(transactionManager.hasError());
        assertTrue(transactionManager.lastError() instanceof GroupAuthorizationException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof GroupAuthorizationException);

        GroupAuthorizationException exception = (GroupAuthorizationException) sendOffsetsResult.error();
        assertEquals(consumerGroupId, exception.groupId());

        assertAbortableError(GroupAuthorizationException.class);
    }

    @Test
    public void testGroupAuthorizationFailureInTxnOffsetCommit() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), consumerGroupId);

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        sender.run(time.milliseconds());  // AddOffsetsToTxn Handled, TxnOffsetCommit Enqueued
        sender.run(time.milliseconds());  // FindCoordinator Enqueued

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        sender.run(time.milliseconds());  // FindCoordinator Returned

        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, singletonMap(tp, Errors.GROUP_AUTHORIZATION_FAILED));
        sender.run(time.milliseconds());  // TxnOffsetCommit Handled

        assertTrue(transactionManager.hasError());
        assertTrue(transactionManager.lastError() instanceof GroupAuthorizationException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof GroupAuthorizationException);

        GroupAuthorizationException exception = (GroupAuthorizationException) sendOffsetsResult.error();
        assertEquals(consumerGroupId, exception.groupId());

        assertAbortableError(GroupAuthorizationException.class);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInAddOffsetsToTxn() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), consumerGroupId);

        prepareAddOffsetsToTxnResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, consumerGroupId, pid, epoch);
        sender.run(time.milliseconds());  // AddOffsetsToTxn Handled

        assertTrue(transactionManager.hasError());
        assertTrue(transactionManager.lastError() instanceof TransactionalIdAuthorizationException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof TransactionalIdAuthorizationException);

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInTxnOffsetCommit() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), consumerGroupId);

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        sender.run(time.milliseconds());  // AddOffsetsToTxn Handled, TxnOffsetCommit Enqueued
        sender.run(time.milliseconds());  // FindCoordinator Enqueued

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        sender.run(time.milliseconds());  // FindCoordinator Returned

        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, singletonMap(tp, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
        sender.run(time.milliseconds());  // TxnOffsetCommit Handled

        assertTrue(transactionManager.hasError());
        assertTrue(transactionManager.lastError() instanceof TransactionalIdAuthorizationException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof TransactionalIdAuthorizationException);

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testTopicAuthorizationFailureInAddPartitions() {
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("bar", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        transactionManager.maybeAddPartitionToTransaction(tp1);
        Map<TopicPartition, Errors> errors = new HashMap<>();
        errors.put(tp0, Errors.TOPIC_AUTHORIZATION_FAILED);
        errors.put(tp1, Errors.OPERATION_NOT_ATTEMPTED);

        prepareAddPartitionsToTxn(errors);
        sender.run(time.milliseconds());

        assertTrue(transactionManager.hasError());
        assertTrue(transactionManager.lastError() instanceof TopicAuthorizationException);
        assertFalse(transactionManager.isPartitionPendingAdd(tp0));
        assertFalse(transactionManager.isPartitionPendingAdd(tp1));
        assertFalse(transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.isPartitionAdded(tp1));
        assertFalse(transactionManager.hasPartitionsToAdd());

        TopicAuthorizationException exception = (TopicAuthorizationException) transactionManager.lastError();
        assertEquals(singleton(tp0.topic()), exception.unauthorizedTopics());

        assertAbortableError(TopicAuthorizationException.class);
    }

    @Test
    public void testRecoveryFromAbortableErrorTransactionNotStarted() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition unauthorizedPartition = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(unauthorizedPartition);

        Future<RecordMetadata> responseFuture = accumulator.append(unauthorizedPartition, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        prepareAddPartitionsToTxn(singletonMap(unauthorizedPartition, Errors.TOPIC_AUTHORIZATION_FAILED));
        sender.run(time.milliseconds());

        assertTrue(transactionManager.hasAbortableError());
        transactionManager.beginAbort();
        sender.run(time.milliseconds());
        assertTrue(responseFuture.isDone());
        assertFutureFailed(responseFuture);

        // No partitions added, so no need to prepare EndTxn response
        sender.run(time.milliseconds());
        assertTrue(transactionManager.isReady());
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertFalse(accumulator.hasIncomplete());

        // ensure we can now start a new transaction

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        prepareAddPartitionsToTxn(singletonMap(tp0, Errors.NONE));
        sender.run(time.milliseconds());
        assertTrue(transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.hasPartitionsToAdd());

        transactionManager.beginCommit();
        prepareProduceResponse(Errors.NONE, pid, epoch);
        sender.run(time.milliseconds());

        assertTrue(responseFuture.isDone());
        assertNotNull(responseFuture.get());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        sender.run(time.milliseconds());

        assertTrue(transactionManager.isReady());
    }

    @Test
    public void testRecoveryFromAbortableErrorTransactionStarted() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition unauthorizedPartition = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.NONE);

        Future<RecordMetadata> authorizedTopicProduceFuture = accumulator.append(unauthorizedPartition, time.milliseconds(),
                "key".getBytes(), "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertTrue(transactionManager.isPartitionAdded(tp0));

        transactionManager.maybeAddPartitionToTransaction(unauthorizedPartition);
        Future<RecordMetadata> unauthorizedTopicProduceFuture = accumulator.append(unauthorizedPartition, time.milliseconds(),
                "key".getBytes(), "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;
        prepareAddPartitionsToTxn(singletonMap(unauthorizedPartition, Errors.TOPIC_AUTHORIZATION_FAILED));
        sender.run(time.milliseconds());
        assertTrue(transactionManager.hasAbortableError());
        assertTrue(transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.isPartitionAdded(unauthorizedPartition));
        assertFalse(authorizedTopicProduceFuture.isDone());
        assertFalse(unauthorizedTopicProduceFuture.isDone());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        transactionManager.beginAbort();
        sender.run(time.milliseconds());
        // neither produce request has been sent, so they should both be failed immediately
        assertFutureFailed(authorizedTopicProduceFuture);
        assertFutureFailed(unauthorizedTopicProduceFuture);
        assertTrue(transactionManager.isReady());
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertFalse(accumulator.hasIncomplete());

        // ensure we can now start a new transaction

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        FutureRecordMetadata nextTransactionFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        prepareAddPartitionsToTxn(singletonMap(tp0, Errors.NONE));
        sender.run(time.milliseconds());
        assertTrue(transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.hasPartitionsToAdd());

        transactionManager.beginCommit();
        prepareProduceResponse(Errors.NONE, pid, epoch);
        sender.run(time.milliseconds());

        assertTrue(nextTransactionFuture.isDone());
        assertNotNull(nextTransactionFuture.get());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        sender.run(time.milliseconds());

        assertTrue(transactionManager.isReady());
    }

    @Test
    public void testRecoveryFromAbortableErrorProduceRequestInRetry() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition unauthorizedPartition = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.NONE);

        Future<RecordMetadata> authorizedTopicProduceFuture = accumulator.append(tp0, time.milliseconds(),
                "key".getBytes(), "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds());
        assertTrue(transactionManager.isPartitionAdded(tp0));

        accumulator.beginFlush();
        prepareProduceResponse(Errors.REQUEST_TIMED_OUT, pid, epoch);
        sender.run(time.milliseconds());
        assertFalse(authorizedTopicProduceFuture.isDone());
        assertTrue(accumulator.hasIncomplete());

        transactionManager.maybeAddPartitionToTransaction(unauthorizedPartition);
        Future<RecordMetadata> unauthorizedTopicProduceFuture = accumulator.append(unauthorizedPartition, time.milliseconds(),
                "key".getBytes(), "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;
        prepareAddPartitionsToTxn(singletonMap(unauthorizedPartition, Errors.TOPIC_AUTHORIZATION_FAILED));
        sender.run(time.milliseconds());
        assertTrue(transactionManager.hasAbortableError());
        assertTrue(transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.isPartitionAdded(unauthorizedPartition));
        assertFalse(authorizedTopicProduceFuture.isDone());

        prepareProduceResponse(Errors.NONE, pid, epoch);
        sender.run(time.milliseconds());
        assertFutureFailed(unauthorizedTopicProduceFuture);
        assertTrue(authorizedTopicProduceFuture.isDone());
        assertNotNull(authorizedTopicProduceFuture.get());
        assertTrue(authorizedTopicProduceFuture.isDone());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        transactionManager.beginAbort();
        sender.run(time.milliseconds());
        // neither produce request has been sent, so they should both be failed immediately
        assertTrue(transactionManager.isReady());
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertFalse(accumulator.hasIncomplete());

        // ensure we can now start a new transaction

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        FutureRecordMetadata nextTransactionFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        prepareAddPartitionsToTxn(singletonMap(tp0, Errors.NONE));
        sender.run(time.milliseconds());
        assertTrue(transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.hasPartitionsToAdd());

        transactionManager.beginCommit();
        prepareProduceResponse(Errors.NONE, pid, epoch);
        sender.run(time.milliseconds());

        assertTrue(nextTransactionFuture.isDone());
        assertNotNull(nextTransactionFuture.get());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        sender.run(time.milliseconds());

        assertTrue(transactionManager.isReady());
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInAddPartitions() {
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp);

        prepareAddPartitionsToTxn(tp, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        sender.run(time.milliseconds());

        assertTrue(transactionManager.hasError());
        assertTrue(transactionManager.lastError() instanceof TransactionalIdAuthorizationException);

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testFlushPendingPartitionsOnCommit() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());

        TransactionalRequestResult commitResult = transactionManager.beginCommit();

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
        assertFalse(commitResult.isCompleted());

        prepareProduceResponse(Errors.NONE, pid, epoch);
        sender.run(time.milliseconds());  // Produce.
        assertTrue(responseFuture.isDone());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        assertFalse(commitResult.isCompleted());
        assertTrue(transactionManager.hasOngoingTransaction());
        assertTrue(transactionManager.isCompleting());

        sender.run(time.milliseconds());
        assertTrue(commitResult.isCompleted());
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testMultipleAddPartitionsPerForOneProduce() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

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

        // The second add partitions should go out here.
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
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.INVALID_PRODUCER_EPOCH, pid, epoch);
        sender.run(time.milliseconds()); // Add partitions.

        sender.run(time.milliseconds());  // send produce.

        assertTrue(responseFuture.isDone());
        assertTrue(transactionManager.hasError());
        responseFuture.get();
    }

    @Test
    public void testDisallowCommitOnProduceFailure() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        TransactionalRequestResult commitResult = transactionManager.beginCommit();
        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, pid, epoch);

        sender.run(time.milliseconds());  // Send AddPartitionsRequest
        assertFalse(commitResult.isCompleted());
        sender.run(time.milliseconds());  // Send Produce Request, returns OutOfOrderSequenceException.

        sender.run(time.milliseconds());  // try to commit.
        assertTrue(commitResult.isCompleted());  // commit should be cancelled with exception without being sent.

        try {
            commitResult.await();
            fail();  // the get() must throw an exception.
        } catch (KafkaException e) {
        }

        try {
            responseFuture.get();
            fail("Expected produce future to raise an exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof OutOfOrderSequenceException);
        }

        // Commit is not allowed, so let's abort and try again.
        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        sender.run(time.milliseconds());  // Send abort request. It is valid to transition from ERROR to ABORT

        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.
    }

    @Test
    public void testAllowAbortOnProduceFailure() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, pid, epoch);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);

        sender.run(time.milliseconds());  // Send AddPartitionsRequest
        sender.run(time.milliseconds());  // Send Produce Request, returns OutOfOrderSequenceException.

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        sender.run(time.milliseconds());  // try to abort
        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.
    }

    @Test
    public void testAbortableErrorWhileAbortInProgress() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        sender.run(time.milliseconds());  // Send AddPartitionsRequest
        sender.run(time.milliseconds());  // Send Produce Request

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        assertTrue(transactionManager.isAborting());
        assertFalse(transactionManager.hasError());

        sendProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, pid, epoch);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        sender.run(time.milliseconds());  // receive the produce response

        // we do not transition to ABORTABLE_ERROR since we were already aborting
        assertTrue(transactionManager.isAborting());
        assertFalse(transactionManager.hasError());

        sender.run(time.milliseconds());  // handle the abort
        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.
    }

    @Test
    public void testCommitTransactionWithUnsentProduceRequest() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        sender.run(time.milliseconds());
        assertTrue(accumulator.hasUndrained());

        // committing the transaction should cause the unsent batch to be flushed
        transactionManager.beginCommit();
        sender.run(time.milliseconds());
        assertFalse(accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightTransactionalRequest());
        assertFalse(responseFuture.isDone());

        // until the produce future returns, we will not send EndTxn
        sender.run(time.milliseconds());
        assertFalse(accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightTransactionalRequest());
        assertFalse(responseFuture.isDone());

        // now the produce response returns
        sendProduceResponse(Errors.NONE, pid, epoch);
        sender.run(time.milliseconds());
        assertTrue(responseFuture.isDone());
        assertFalse(accumulator.hasUndrained());
        assertFalse(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightTransactionalRequest());

        // now we send EndTxn
        sender.run(time.milliseconds());
        assertTrue(transactionManager.hasInFlightTransactionalRequest());
        sendEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        sender.run(time.milliseconds());
        assertFalse(transactionManager.hasInFlightTransactionalRequest());
        assertTrue(transactionManager.isReady());
    }

    @Test
    public void testCommitTransactionWithInFlightProduceRequest() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        sender.run(time.milliseconds());
        assertTrue(accumulator.hasUndrained());

        accumulator.beginFlush();
        sender.run(time.milliseconds());
        assertFalse(accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightTransactionalRequest());

        // now we begin the commit with the produce request still pending
        transactionManager.beginCommit();
        sender.run(time.milliseconds());
        assertFalse(accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightTransactionalRequest());
        assertFalse(responseFuture.isDone());

        // until the produce future returns, we will not send EndTxn
        sender.run(time.milliseconds());
        assertFalse(accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightTransactionalRequest());
        assertFalse(responseFuture.isDone());

        // now the produce response returns
        sendProduceResponse(Errors.NONE, pid, epoch);
        sender.run(time.milliseconds());
        assertTrue(responseFuture.isDone());
        assertFalse(accumulator.hasUndrained());
        assertFalse(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightTransactionalRequest());

        // now we send EndTxn
        sender.run(time.milliseconds());
        assertTrue(transactionManager.hasInFlightTransactionalRequest());
        sendEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        sender.run(time.milliseconds());
        assertFalse(transactionManager.hasInFlightTransactionalRequest());
        assertTrue(transactionManager.isReady());
    }

    @Test
    public void testFindCoordinatorAllowedInAbortableErrorState() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());
        sender.run(time.milliseconds());  // Send AddPartitionsRequest

        transactionManager.transitionToAbortableError(new KafkaException());
        sendAddPartitionsToTxnResponse(Errors.NOT_COORDINATOR, tp0, epoch, pid);
        sender.run(time.milliseconds()); // AddPartitions returns
        assertTrue(transactionManager.hasAbortableError());

        assertNull(transactionManager.coordinator(CoordinatorType.TRANSACTION));
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds()); // FindCoordinator handled
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testCancelUnsentAddPartitionsAndProduceOnAbort() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        // note since no partitions were added to the transaction, no EndTxn will be sent

        sender.run(time.milliseconds());  // try to abort
        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        try {
            responseFuture.get();
            fail("Expected produce future to raise an exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof KafkaException);
        }
    }

    @Test
    public void testAbortResendsAddPartitionErrorIfRetried() throws InterruptedException {
        final long producerId = 13131L;
        final short producerEpoch = 1;

        doInitTransactions(producerId, producerEpoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxnResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, tp0, producerEpoch, producerId);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        sender.run(time.milliseconds());  // Send AddPartitions and let it fail
        assertFalse(responseFuture.isDone());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        // we should resend the AddPartitions
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, producerEpoch, producerId);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, producerEpoch);

        sender.run(time.milliseconds());  // Resend AddPartitions
        sender.run(time.milliseconds());  // Send EndTxn

        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        try {
            responseFuture.get();
            fail("Expected produce future to raise an exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof KafkaException);
        }
    }

    @Test
    public void testAbortResendsProduceRequestIfRetried() throws Exception {
        final long producerId = 13131L;
        final short producerEpoch = 1;

        doInitTransactions(producerId, producerEpoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, producerEpoch, producerId);
        prepareProduceResponse(Errors.REQUEST_TIMED_OUT, producerId, producerEpoch);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        sender.run(time.milliseconds());  // Send AddPartitions
        sender.run(time.milliseconds());  // Send ProduceRequest and let it fail

        assertFalse(responseFuture.isDone());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        // we should resend the ProduceRequest before aborting
        prepareProduceResponse(Errors.NONE, producerId, producerEpoch);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, producerEpoch);

        sender.run(time.milliseconds());  // Resend ProduceRequest
        sender.run(time.milliseconds());  // Send EndTxn

        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        RecordMetadata recordMetadata = responseFuture.get();
        assertEquals(tp0.topic(), recordMetadata.topic());
    }

    @Test
    public void testHandlingOfUnknownTopicPartitionErrorOnAddPartitions() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, tp0, epoch, pid);

        sender.run(time.milliseconds());  // Send AddPartitionsRequest
        assertFalse(transactionManager.transactionContainsPartition(tp0));  // The partition should not yet be added.

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.NONE, pid, epoch);
        sender.run(time.milliseconds());  // Send AddPartitionsRequest successfully.
        assertTrue(transactionManager.transactionContainsPartition(tp0));

        sender.run(time.milliseconds());  // Send ProduceRequest.
        assertTrue(responseFuture.isDone());
    }

    @Test
    public void testHandlingOfUnknownTopicPartitionErrorOnTxnOffsetCommit() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));
        final String consumerGroupId = "myconsumergroup";

        TransactionalRequestResult addOffsetsResult = transactionManager.sendOffsetsToTransaction(offsets, consumerGroupId);
        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);

        sender.run(time.milliseconds());  // send AddOffsetsToTxnResult

        assertFalse(addOffsetsResult.isCompleted());  // The request should complete only after the TxnOffsetCommit completes.

        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp1, Errors.UNKNOWN_TOPIC_OR_PARTITION);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, txnOffsetCommitResponse);

        assertEquals(null, transactionManager.coordinator(CoordinatorType.GROUP));
        sender.run(time.milliseconds());  // try to send TxnOffsetCommitRequest, but find we don't have a group coordinator.
        sender.run(time.milliseconds());  // send find coordinator for group request
        assertNotNull(transactionManager.coordinator(CoordinatorType.GROUP));
        assertTrue(transactionManager.hasPendingOffsetCommits());

        sender.run(time.milliseconds());  // send TxnOffsetCommitRequest request.

        assertTrue(transactionManager.hasPendingOffsetCommits());  // The TxnOffsetCommit failed.
        assertFalse(addOffsetsResult.isCompleted());  // We should only be done after both RPCs complete successfully.

        txnOffsetCommitResponse.put(tp1, Errors.NONE);
        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, txnOffsetCommitResponse);
        sender.run(time.milliseconds());  // Send TxnOffsetCommitRequest again.

        assertTrue(addOffsetsResult.isCompleted());
        assertTrue(addOffsetsResult.isSuccessful());
    }

    @Test
    public void shouldNotAddPartitionsToTransactionWhenTopicAuthorizationFailed() throws Exception {
        verifyAddPartitionsFailsWithPartitionLevelError(Errors.TOPIC_AUTHORIZATION_FAILED);
    }

    @Test
    public void shouldNotSendAbortTxnRequestWhenOnlyAddPartitionsRequestFailed() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        prepareAddPartitionsToTxnResponse(Errors.TOPIC_AUTHORIZATION_FAILED, tp0, epoch, pid);
        sender.run(time.milliseconds());  // Send AddPartitionsRequest

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        assertFalse(abortResult.isCompleted());

        sender.run(time.milliseconds());
        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
    }

    @Test
    public void shouldNotSendAbortTxnRequestWhenOnlyAddOffsetsRequestFailed() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));
        final String consumerGroupId = "myconsumergroup";

        transactionManager.sendOffsetsToTransaction(offsets, consumerGroupId);

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareAddOffsetsToTxnResponse(Errors.GROUP_AUTHORIZATION_FAILED, consumerGroupId, pid, epoch);
        sender.run(time.milliseconds());  // Send AddOffsetsToTxnRequest
        assertFalse(abortResult.isCompleted());

        sender.run(time.milliseconds());
        assertTrue(transactionManager.isReady());
        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
    }

    @Test
    public void shouldFailAbortIfAddOffsetsFailsWithFatalError() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));
        final String consumerGroupId = "myconsumergroup";

        transactionManager.sendOffsetsToTransaction(offsets, consumerGroupId);

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareAddOffsetsToTxnResponse(Errors.UNKNOWN_SERVER_ERROR, consumerGroupId, pid, epoch);
        sender.run(time.milliseconds());  // Send AddOffsetsToTxnRequest
        assertFalse(abortResult.isCompleted());

        sender.run(time.milliseconds());
        assertTrue(abortResult.isCompleted());
        assertFalse(abortResult.isSuccessful());
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testNoDrainWhenPartitionsPending() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT);
        transactionManager.maybeAddPartitionToTransaction(tp1);
        accumulator.append(tp1, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT);

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp1));

        Node node1 = new Node(0, "localhost", 1111);
        Node node2 = new Node(1, "localhost", 1112);
        PartitionInfo part1 = new PartitionInfo(topic, 0, node1, null, null);
        PartitionInfo part2 = new PartitionInfo(topic, 1, node2, null, null);

        Cluster cluster = new Cluster(null, Arrays.asList(node1, node2), Arrays.asList(part1, part2),
                Collections.<String>emptySet(), Collections.<String>emptySet());
        Set<Node> nodes = new HashSet<>();
        nodes.add(node1);
        nodes.add(node2);
        Map<Integer, List<ProducerBatch>> drainedBatches = accumulator.drain(cluster, nodes, Integer.MAX_VALUE,
                time.milliseconds());

        // We shouldn't drain batches which haven't been added to the transaction yet.
        assertTrue(drainedBatches.containsKey(node1.id()));
        assertTrue(drainedBatches.get(node1.id()).isEmpty());
        assertTrue(drainedBatches.containsKey(node2.id()));
        assertTrue(drainedBatches.get(node2.id()).isEmpty());
        assertFalse(transactionManager.hasError());
    }

    @Test
    public void testAllowDrainInAbortableErrorState() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp1);
        prepareAddPartitionsToTxn(tp1, Errors.NONE);
        sender.run(time.milliseconds());  // Send AddPartitions, tp1 should be in the transaction now.

        assertTrue(transactionManager.transactionContainsPartition(tp1));

        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.TOPIC_AUTHORIZATION_FAILED);
        sender.run(time.milliseconds());  // Send AddPartitions, should be in abortable state.

        assertTrue(transactionManager.hasAbortableError());
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1));

        // Try to drain a message destined for tp1, it should get drained.
        Node node1 = new Node(1, "localhost", 1112);
        PartitionInfo part1 = new PartitionInfo(topic, 1, node1, null, null);
        Cluster cluster = new Cluster(null, Arrays.asList(node1), Arrays.asList(part1),
                Collections.<String>emptySet(), Collections.<String>emptySet());
        accumulator.append(tp1, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT);
        Map<Integer, List<ProducerBatch>> drainedBatches = accumulator.drain(cluster, Collections.singleton(node1),
                Integer.MAX_VALUE,
                time.milliseconds());

        // We should drain the appended record since we are in abortable state and the partition has already been
        // added to the transaction.
        assertTrue(drainedBatches.containsKey(node1.id()));
        assertEquals(1, drainedBatches.get(node1.id()).size());
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testRaiseErrorWhenNoPartitionsPendingOnDrain() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        // Don't execute transactionManager.maybeAddPartitionToTransaction(tp0). This should result in an error on drain.
        accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT);
        Node node1 = new Node(0, "localhost", 1111);
        PartitionInfo part1 = new PartitionInfo(topic, 0, node1, null, null);

        Cluster cluster = new Cluster(null, Arrays.asList(node1), Arrays.asList(part1),
                Collections.<String>emptySet(), Collections.<String>emptySet());
        Set<Node> nodes = new HashSet<>();
        nodes.add(node1);
        Map<Integer, List<ProducerBatch>> drainedBatches = accumulator.drain(cluster, nodes, Integer.MAX_VALUE,
                time.milliseconds());

        // We shouldn't drain batches which haven't been added to the transaction yet.
        assertTrue(drainedBatches.containsKey(node1.id()));
        assertTrue(drainedBatches.get(node1.id()).isEmpty());
    }

    @Test
    public void resendFailedProduceRequestAfterAbortableError() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.NOT_LEADER_FOR_PARTITION, pid, epoch);
        sender.run(time.milliseconds()); // Add partitions
        sender.run(time.milliseconds()); // Produce

        assertFalse(responseFuture.isDone());

        transactionManager.transitionToAbortableError(new KafkaException());
        prepareProduceResponse(Errors.NONE, pid, epoch);
        sender.run(time.milliseconds());

        assertTrue(responseFuture.isDone());
        assertNotNull(responseFuture.get()); // should throw the exception which caused the transaction to be aborted.
    }

    @Test
    public void testTransitionToAbortableErrorOnBatchExpiry() throws InterruptedException, ExecutionException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        sender.run(time.milliseconds());  // send addPartitions.
        // Check that only addPartitions was sent.
        assertTrue(transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(responseFuture.isDone());

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = this.cluster.nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.blackout(clusterNode, 100);

        sender.run(time.milliseconds());  // We should try to flush the produce, but expire it instead without sending anything.
        assertTrue(responseFuture.isDone());

        try {
            // make sure the produce was expired.
            responseFuture.get();
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof  TimeoutException);
        }
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testTransitionToAbortableErrorOnMultipleBatchExpiry() throws InterruptedException, ExecutionException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        transactionManager.maybeAddPartitionToTransaction(tp1);

        Future<RecordMetadata> firstBatchResponse = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;
        Future<RecordMetadata> secondBatchResponse = accumulator.append(tp1, time.milliseconds(), "key".getBytes(),
               "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(firstBatchResponse.isDone());
        assertFalse(secondBatchResponse.isDone());

        Map<TopicPartition, Errors> partitionErrors = new HashMap<>();
        partitionErrors.put(tp0, Errors.NONE);
        partitionErrors.put(tp1, Errors.NONE);
        prepareAddPartitionsToTxn(partitionErrors);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        sender.run(time.milliseconds());  // send addPartitions.
        // Check that only addPartitions was sent.
        assertTrue(transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.transactionContainsPartition(tp1));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1));
        assertFalse(firstBatchResponse.isDone());
        assertFalse(secondBatchResponse.isDone());

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = this.cluster.nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.blackout(clusterNode, 100);

        sender.run(time.milliseconds());  // We should try to flush the produce, but expire it instead without sending anything.
        assertTrue(firstBatchResponse.isDone());
        assertTrue(secondBatchResponse.isDone());

        try {
            // make sure the produce was expired.
            firstBatchResponse.get();
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof  TimeoutException);
        }

        try {
            // make sure the produce was expired.
            secondBatchResponse.get();
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof  TimeoutException);
        }
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testDropCommitOnBatchExpiry() throws InterruptedException, ExecutionException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        sender.run(time.milliseconds());  // send addPartitions.
        // Check that only addPartitions was sent.
        assertTrue(transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(responseFuture.isDone());

        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = this.cluster.nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.blackout(clusterNode, 100);

        sender.run(time.milliseconds());  // We should try to flush the produce, but expire it instead without sending anything.
        assertTrue(responseFuture.isDone());

        try {
            // make sure the produce was expired.
            responseFuture.get();
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof  TimeoutException);
        }
        sender.run(time.milliseconds());  // the commit shouldn't be completed without being sent since the produce request failed.

        assertTrue(commitResult.isCompleted());
        assertFalse(commitResult.isSuccessful());  // the commit shouldn't succeed since the produce request failed.

        assertTrue(transactionManager.hasAbortableError());
        assertTrue(transactionManager.hasOngoingTransaction());
        assertFalse(transactionManager.isCompleting());
        assertTrue(transactionManager.transactionContainsPartition(tp0));

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);

        sender.run(time.milliseconds());  // send the abort.

        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
        assertFalse(transactionManager.hasOngoingTransaction());
        assertFalse(transactionManager.transactionContainsPartition(tp0));
    }

    @Test
    public void testTransitionToFatalErrorWhenRetriedBatchIsExpired() throws InterruptedException, ExecutionException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;

        assertFalse(responseFuture.isDone());

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        sender.run(time.milliseconds());  // send addPartitions.
        // Check that only addPartitions was sent.
        assertTrue(transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));

        prepareProduceResponse(Errors.NOT_LEADER_FOR_PARTITION, pid, epoch);
        sender.run(time.milliseconds());  // send the produce request.

        assertFalse(responseFuture.isDone());

        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = this.cluster.nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.blackout(clusterNode, 100);

        sender.run(time.milliseconds());  // We should try to flush the produce, but expire it instead without sending anything.
        assertTrue(responseFuture.isDone());

        try {
            // make sure the produce was expired.
            responseFuture.get();
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof  TimeoutException);
        }
        sender.run(time.milliseconds());  // Transition to fatal error since we have unresolved batches.
        sender.run(time.milliseconds());  // Fail the queued transactional requests

        assertTrue(commitResult.isCompleted());
        assertFalse(commitResult.isSuccessful());  // the commit should have been dropped.

        assertTrue(transactionManager.hasFatalError());
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testShouldResetProducerStateAfterResolvingSequences() throws InterruptedException, ExecutionException {
        // Create a TransactionManager without a transactionalId to test
        // shouldResetProducerStateAfterResolvingSequences.
        TransactionManager manager = new TransactionManager(logContext, null, transactionTimeoutMs,
            DEFAULT_RETRY_BACKOFF_MS);
        assertFalse(manager.shouldResetProducerStateAfterResolvingSequences());
        TopicPartition tp0 = new TopicPartition("foo", 0);
        TopicPartition tp1 = new TopicPartition("foo", 1);
        assertEquals(Integer.valueOf(0), manager.sequenceNumber(tp0));
        assertEquals(Integer.valueOf(0), manager.sequenceNumber(tp1));

        manager.incrementSequenceNumber(tp0, 1);
        manager.incrementSequenceNumber(tp1, 1);
        manager.maybeUpdateLastAckedSequence(tp0, 0);
        manager.maybeUpdateLastAckedSequence(tp1, 0);
        manager.markSequenceUnresolved(tp0);
        manager.markSequenceUnresolved(tp1);
        assertFalse(manager.shouldResetProducerStateAfterResolvingSequences());

        manager.maybeUpdateLastAckedSequence(tp0, 5);
        manager.incrementSequenceNumber(tp0, 1);
        manager.markSequenceUnresolved(tp0);
        manager.markSequenceUnresolved(tp1);
        assertTrue(manager.shouldResetProducerStateAfterResolvingSequences());
    }

    private void verifyAddPartitionsFailsWithPartitionLevelError(final Errors error) throws InterruptedException {
        final long pid = 1L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT).future;
        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxn(tp0, error);
        sender.run(time.milliseconds());  // attempt send addPartitions.
        assertTrue(transactionManager.hasError());
        assertFalse(transactionManager.transactionContainsPartition(tp0));
    }

    private void prepareAddPartitionsToTxn(final Map<TopicPartition, Errors> errors) {
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                AddPartitionsToTxnRequest request = (AddPartitionsToTxnRequest) body;
                assertEquals(new HashSet<>(request.partitions()), new HashSet<>(errors.keySet()));
                return true;
            }
        }, new AddPartitionsToTxnResponse(0, errors));
    }

    private void prepareAddPartitionsToTxn(final TopicPartition tp, final Errors error) {
        prepareAddPartitionsToTxn(Collections.singletonMap(tp, error));
    }

    private void prepareFindCoordinatorResponse(Errors error, boolean shouldDisconnect,
                                                final CoordinatorType coordinatorType,
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
                InitProducerIdRequest initProducerIdRequest = (InitProducerIdRequest) body;
                assertEquals(initProducerIdRequest.transactionalId(), transactionalId);
                assertEquals(initProducerIdRequest.transactionTimeoutMs(), transactionTimeoutMs);
                return true;
            }
        }, new InitProducerIdResponse(0, error, pid, epoch), shouldDisconnect);
    }

    private void sendProduceResponse(Errors error, final long pid, final short epoch) {
        client.respond(produceRequestMatcher(pid, epoch), produceResponse(tp0, 0, error, 0));
    }

    private void prepareProduceResponse(Errors error, final long pid, final short epoch) {
        client.prepareResponse(produceRequestMatcher(pid, epoch), produceResponse(tp0, 0, error, 0));
    }
    private MockClient.RequestMatcher produceRequestMatcher(final long pid, final short epoch) {
        return new MockClient.RequestMatcher() {
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
        };
    }

    private void prepareAddPartitionsToTxnResponse(Errors error, final TopicPartition topicPartition,
                                                   final short epoch, final long pid) {
        client.prepareResponse(addPartitionsRequestMatcher(topicPartition, epoch, pid),
                new AddPartitionsToTxnResponse(0, singletonMap(topicPartition, error)));
    }

    private void sendAddPartitionsToTxnResponse(Errors error, final TopicPartition topicPartition,
                                                final short epoch, final long pid) {
        client.respond(addPartitionsRequestMatcher(topicPartition, epoch, pid),
                new AddPartitionsToTxnResponse(0, singletonMap(topicPartition, error)));
    }

    private MockClient.RequestMatcher addPartitionsRequestMatcher(final TopicPartition topicPartition,
                                                                  final short epoch, final long pid) {
        return new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                AddPartitionsToTxnRequest addPartitionsToTxnRequest = (AddPartitionsToTxnRequest) body;
                assertEquals(pid, addPartitionsToTxnRequest.producerId());
                assertEquals(epoch, addPartitionsToTxnRequest.producerEpoch());
                assertEquals(singletonList(topicPartition), addPartitionsToTxnRequest.partitions());
                assertEquals(transactionalId, addPartitionsToTxnRequest.transactionalId());
                return true;
            }
        };
    }

    private void prepareEndTxnResponse(Errors error, final TransactionResult result, final long pid, final short epoch) {
        client.prepareResponse(endTxnMatcher(result, pid, epoch), new EndTxnResponse(0, error));
    }

    private void sendEndTxnResponse(Errors error, final TransactionResult result, final long pid, final short epoch) {
        client.respond(endTxnMatcher(result, pid, epoch), new EndTxnResponse(0, error));
    }

    private MockClient.RequestMatcher endTxnMatcher(final TransactionResult result, final long pid, final short epoch) {
        return new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                EndTxnRequest endTxnRequest = (EndTxnRequest) body;
                assertEquals(transactionalId, endTxnRequest.transactionalId());
                assertEquals(pid, endTxnRequest.producerId());
                assertEquals(epoch, endTxnRequest.producerEpoch());
                assertEquals(result, endTxnRequest.command());
                return true;
            }
        };
    }

    private void prepareAddOffsetsToTxnResponse(Errors error, final String consumerGroupId, final long producerId,
                                                final short producerEpoch) {
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                AddOffsetsToTxnRequest addOffsetsToTxnRequest = (AddOffsetsToTxnRequest) body;
                assertEquals(consumerGroupId, addOffsetsToTxnRequest.consumerGroupId());
                assertEquals(transactionalId, addOffsetsToTxnRequest.transactionalId());
                assertEquals(producerId, addOffsetsToTxnRequest.producerId());
                assertEquals(producerEpoch, addOffsetsToTxnRequest.producerEpoch());
                return true;
            }
        }, new AddOffsetsToTxnResponse(0, error));
    }

    private void prepareTxnOffsetCommitResponse(final String consumerGroupId, final long producerId,
                                                final short producerEpoch, Map<TopicPartition, Errors> txnOffsetCommitResponse) {
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) body;
                assertEquals(consumerGroupId, txnOffsetCommitRequest.consumerGroupId());
                assertEquals(producerId, txnOffsetCommitRequest.producerId());
                assertEquals(producerEpoch, txnOffsetCommitRequest.producerEpoch());
                return true;
            }
        }, new TxnOffsetCommitResponse(0, txnOffsetCommitResponse));

    }

    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs) {
        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(error, offset, RecordBatch.NO_TIMESTAMP, 10);
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = singletonMap(tp, resp);
        return new ProduceResponse(partResp, throttleTimeMs);
    }

    private void doInitTransactions(long pid, short epoch) {
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        sender.run(time.milliseconds());  // find coordinator
        sender.run(time.milliseconds());
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NONE, false, pid, epoch);
        sender.run(time.milliseconds());  // get pid.
        assertTrue(transactionManager.hasProducerId());
    }

    private void assertAbortableError(Class<? extends RuntimeException> cause) {
        try {
            transactionManager.beginTransaction();
            fail("Should have raised " + cause.getSimpleName());
        } catch (KafkaException e) {
            assertTrue(cause.isAssignableFrom(e.getCause().getClass()));
            assertTrue(transactionManager.hasError());
        }

        assertTrue(transactionManager.hasError());
        transactionManager.beginAbort();
        assertFalse(transactionManager.hasError());
    }

    private void assertFatalError(Class<? extends RuntimeException> cause) {
        assertTrue(transactionManager.hasError());

        try {
            transactionManager.beginAbort();
            fail("Should have raised " + cause.getSimpleName());
        } catch (KafkaException e) {
            assertTrue(cause.isAssignableFrom(e.getCause().getClass()));
            assertTrue(transactionManager.hasError());
        }

        // Transaction abort cannot clear fatal error state
        try {
            transactionManager.beginAbort();
            fail("Should have raised " + cause.getSimpleName());
        } catch (KafkaException e) {
            assertTrue(cause.isAssignableFrom(e.getCause().getClass()));
            assertTrue(transactionManager.hasError());
        }
    }

    private void assertFutureFailed(Future<RecordMetadata> future) throws InterruptedException {
        assertTrue(future.isDone());

        try {
            future.get();
            fail("Expected produce future to throw");
        } catch (ExecutionException e) {
            // expected
        }
    }

}
