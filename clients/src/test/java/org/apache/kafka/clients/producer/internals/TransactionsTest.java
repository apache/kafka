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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.TransactionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TransactionsTest {
    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = 0;
    private static final String CLIENT_ID = "clientId";
    private static final String METRIC_GROUP = "producer-metrics";
    private static final double EPS = 0.0001;
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 1000;
    private final String transactionalId = "foobar";
    private final int transactionTimeoutMs = 1121;

    private TopicPartition tp0 = new TopicPartition("test", 0);
    private TopicPartition tp1 = new TopicPartition("test", 1);
    private MockTime time = new MockTime();
    private MockClient client = new MockClient(time);
    private int batchSize = 16 * 1024;
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE, true, new ClusterResourceListeners());
    private ApiVersions apiVersions = new ApiVersions();
    private Cluster cluster = TestUtils.singletonCluster("test", 2);
    private Metrics metrics = null;
    private RecordAccumulator accumulator = null;
    private Sender sender = null;
    private TransactionState transactionState = null;
    Node brokerNode = null;

    @Before
    public void setup() {
        Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("client-id", CLIENT_ID);
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        this.brokerNode = new Node(0, "localhost", 2211);
        this.transactionState = new TransactionState(time, transactionalId, transactionTimeoutMs);
        this.metrics = new Metrics(metricConfig, time);
        this.accumulator = new RecordAccumulator(batchSize, 1024 * 1024, CompressionType.NONE, 0L, 0L, metrics, time, apiVersions, transactionState);
        this.sender = new Sender(this.client,
                this.metadata,
                this.accumulator,
                true,
                MAX_REQUEST_SIZE,
                ACKS_ALL,
                MAX_RETRIES,
                this.metrics,
                this.time,
                REQUEST_TIMEOUT,
                50,
                transactionState,
                apiVersions);
        this.metadata.update(this.cluster, Collections.<String>emptySet(), time.milliseconds());
    }


    @Test
    public void testBasicTransaction() throws InterruptedException {
        client.setNode(brokerNode);
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        new Thread(new AwaitPidRunnable(transactionState)).start();
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                FindCoordinatorRequest findCoordinatorRequest = (FindCoordinatorRequest) body;
                assertEquals(findCoordinatorRequest.coordinatorType(), FindCoordinatorRequest.CoordinatorType.TRANSACTION);
                assertEquals(findCoordinatorRequest.coordinatorKey(), transactionalId);
                return true;
            }
        }, new FindCoordinatorResponse(Errors.NONE, brokerNode));

        sender.run(time.milliseconds());  // find coordinator

        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                InitPidRequest initPidRequest = (InitPidRequest) body;
                assertEquals(initPidRequest.transactionalId(), transactionalId);
                assertEquals(initPidRequest.transactionTimeoutMs(), transactionTimeoutMs);
                return true;
            }
        }, new InitPidResponse(Errors.NONE, pid, epoch));

        sender.run(time.milliseconds());  // connect
        sender.run(time.milliseconds());  // get pid.
        assertTrue(transactionState.hasPid());
        assertEquals(transactionState.coordinator(), brokerNode);
        transactionState.beginTransaction();
        transactionState.maybeAddPartitionToTransaction(tp0);
        Future<RecordMetadata> responseFuture = accumulator.append(tp0, time.milliseconds(), "key".getBytes(),
                "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;

        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                AddPartitionsToTxnRequest addPartitionsToTxnRequest = (AddPartitionsToTxnRequest) body;
                assertEquals(pid, addPartitionsToTxnRequest.pid());
                assertEquals(epoch, addPartitionsToTxnRequest.epoch());
                assertEquals(Arrays.asList(tp0), addPartitionsToTxnRequest.partitions());
                assertEquals(transactionalId, addPartitionsToTxnRequest.transactionalId());
                return true;
            }
        }, new AddPartitionsToTxnResponse(Errors.NONE));

        sender.run(time.milliseconds());  // send addPartitions.
        assertTrue(transactionState.transactionContainsPartition(tp0));

        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                ProduceRequest produceRequest = (ProduceRequest) body;
                assertEquals(produceRequest.transactionalId(), transactionalId);
                return true;
            }
        }, produceResponse(tp0, 0, Errors.NONE, 0));
        sender.run(time.milliseconds());  // send produce request.

        transactionState.beginCommittingTransaction();
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                EndTxnRequest endTxnRequest = (EndTxnRequest) body;
                assertEquals(transactionalId, endTxnRequest.transactionalId());
                assertEquals(pid, endTxnRequest.pid());
                assertEquals(epoch, endTxnRequest.epoch());
                assertEquals(TransactionResult.COMMIT, endTxnRequest.command());
                return true;
            }
        }, new EndTxnResponse(Errors.NONE));

        sender.run(time.milliseconds());  // flush
        sender.run(time.milliseconds());  // commit.

        assertFalse(transactionState.isInTransaction());
        assertFalse(transactionState.isCompletingTransaction());
        assertFalse(transactionState.transactionContainsPartition(tp0));

    }

    private class AwaitPidRunnable implements Runnable {
        private TransactionState transactionState;

        public AwaitPidRunnable(TransactionState transactionState) {
            this.transactionState = transactionState;
        }

        @Override
        public void run()  {
            try {
                transactionState.awaitPidAndEpoch(1000);
            } catch (InterruptedException e) {

            }
        }
    }

    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs) {
        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(error, offset, RecordBatch.NO_TIMESTAMP);
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = Collections.singletonMap(tp, resp);
        return new ProduceResponse(partResp, throttleTimeMs);
    }

}
