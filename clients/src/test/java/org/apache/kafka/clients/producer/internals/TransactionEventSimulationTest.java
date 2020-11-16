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
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test tries to test out the EOS robustness on the client side. It features a {@link TransactionSimulationCoordinator}
 * which handles the incoming transactional produce/metadata requests and gives basic feedback through {@link MockClient}.
 *
 * Each iteration the transaction manager will append one record through accumulator and commit offset at the same time. The request
 * being transmitted is not guaranteed to be processed or processed correctly, so a state checking loop is enforced to make the client
 * and the coordinator interact with each other and ensure the state could be eventually clean using {@link TransactionManager#isReady}.
 * By the end of the test we will check whether all the committed transactions are successfully materialized on the coordinator side.
 *
 * Features supported:
 * 
 * 1. Randomly abort transaction
 * 2. Fault injection on response
 * 3. Random message drop
 */
public class TransactionEventSimulationTest {

    private TransactionManager transactionManager;
    private TransactionSimulationCoordinator transactionCoordinator;
    private Sender sender;
    private final LogContext logContext = new LogContext();

    private final MockTime time = new MockTime();
    private final int requestTimeoutMs = 100;
    private final int retryBackOffMs = 0;
    private final long apiVersion = 0L;

    private ProducerMetadata metadata = new ProducerMetadata(0, Long.MAX_VALUE, 10,
        new LogContext(), new ClusterResourceListeners(), time);
    private MockClient client = new MockClient(time, metadata);

    @Before
    public void setup() {
        transactionManager = new TransactionManager(logContext, "txn-id",
            requestTimeoutMs, apiVersion, new ApiVersions(), false);
        transactionCoordinator = new TransactionSimulationCoordinator(client);
    }

    @Test
    public void simulateTxnEvents() throws InterruptedException {
        final int batchSize = 100;
        final int lingerMs = 0;
        final int deliveryTimeoutMs = 10;

        RecordAccumulator accumulator = new RecordAccumulator(logContext, batchSize, CompressionType.GZIP,
            lingerMs, retryBackOffMs, deliveryTimeoutMs, new Metrics(), "accumulator", time, new ApiVersions(), transactionManager,
            new BufferPool(1000, 100, new Metrics(), time, "producer-internal-metrics"));

        metadata.add("topic", time.milliseconds());
        metadata.update(metadata.newMetadataRequestAndVersion(time.milliseconds()).requestVersion,
            TestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 2)), true, time.milliseconds());

        sender = new Sender(logContext, client, metadata, accumulator, false, 100, (short) 1,
            Integer.MAX_VALUE, new SenderMetricsRegistry(new Metrics()), time, requestTimeoutMs, 10, transactionManager, new ApiVersions());

        transactionManager.initializeTransactions();
        sender.runOnce();
        resolvePendingRequests();
        final int numTransactions = 100;

        TopicPartition key = new TopicPartition("topic", 0);
        long committedOffsets = 0L;
        Random abortTxn = new Random();
        client.prepareMetadataUpdate(TestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 2)));
        final long timestamp = 0L;
        final int maxBlockTime = 0;

        for (int i = 0; i < numTransactions; i++) {
            transactionManager.beginTransaction();
            transactionManager.maybeAddPartitionToTransaction(key);
            accumulator.append(key, timestamp, new byte[1], new byte[1],
                Record.EMPTY_HEADERS, null, maxBlockTime, false, time.milliseconds());
            transactionManager.sendOffsetsToTransaction(
                Collections.singletonMap(key, new OffsetAndMetadata(committedOffsets)),
                new ConsumerGroupMetadata("group"));

            if (abortTxn.nextBoolean()) {
                transactionManager.beginCommit();
                committedOffsets += 1;
            } else {
                transactionManager.beginAbort();
            }

            resolvePendingRequests();
        }

        assertTrue(transactionCoordinator.persistentPartitionData().containsKey(key));
        assertTrue(transactionCoordinator.committedOffsets().containsKey(key));
        assertEquals(committedOffsets - 1, (long) transactionCoordinator.committedOffsets().get(key));
    }

    private void resolvePendingRequests() {
        Random dropMessageRandom = new Random();

        while (!client.requests().isEmpty() ||
            transactionManager.coordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION) == null ||
            !transactionManager.isReady()) {
            if (dropMessageRandom.nextBoolean()) {
                transactionCoordinator.runOnce(true);
            } else {
                transactionCoordinator.runOnce(false);
            }

            sender.runOnce();
        }
    }
}
