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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeProducersResponse;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.DescribeTransactionsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.ListTransactionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaAdminClientTransactionTest extends KafkaAdminClientTestBase {

    @Test
    public void testDescribeProducers() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            TopicPartition topicPartition = new TopicPartition("foo", 0);

            Node leader = env.cluster().nodes().iterator().next();
            expectMetadataRequest(env, topicPartition, leader);

            List<ProducerState> expected = asList(
                new ProducerState(12345L, 15, 30, env.time().milliseconds(),
                    OptionalInt.of(99), OptionalLong.empty()),
                new ProducerState(12345L, 15, 30, env.time().milliseconds(),
                    OptionalInt.empty(), OptionalLong.of(23423L))
            );

            DescribeProducersResponse response = buildDescribeProducersResponse(
                topicPartition,
                expected
            );

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof DescribeProducersRequest,
                response,
                leader
            );

            DescribeProducersResult result = env.adminClient().describeProducers(singleton(topicPartition));
            KafkaFuture<DescribeProducersResult.PartitionProducerState> partitionFuture =
                result.partitionResult(topicPartition);
            assertEquals(new HashSet<>(expected), new HashSet<>(partitionFuture.get().activeProducers()));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDescribeProducersTimeout(boolean timeoutInMetadataLookup) throws Exception {
        MockTime time = new MockTime();
        try (AdminClientUnitTestEnv env = mockClientEnv(time)) {
            TopicPartition topicPartition = new TopicPartition("foo", 0);
            int requestTimeoutMs = 15000;

            if (!timeoutInMetadataLookup) {
                Node leader = env.cluster().nodes().iterator().next();
                expectMetadataRequest(env, topicPartition, leader);
            }

            DescribeProducersOptions options = new DescribeProducersOptions().timeoutMs(requestTimeoutMs);
            DescribeProducersResult result = env.adminClient().describeProducers(
                singleton(topicPartition), options);
            assertFalse(result.all().isDone());

            time.sleep(requestTimeoutMs);
            TestUtils.waitForCondition(() -> result.all().isDone(),
                "Future failed to timeout after expiration of timeout");

            assertTrue(result.all().isCompletedExceptionally());
            TestUtils.assertFutureThrows(TimeoutException.class, result.all());
            assertFalse(env.kafkaClient().hasInFlightRequests());
        }
    }

    @Test
    public void testDescribeProducersRetryAfterDisconnect() throws Exception {
        MockTime time = new MockTime();
        int retryBackoffMs = 100;
        Cluster cluster = mockCluster(3, 0);
        Map<String, Object> configOverride = newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoffMs);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster, configOverride)) {
            TopicPartition topicPartition = new TopicPartition("foo", 0);
            Iterator<Node> nodeIterator = env.cluster().nodes().iterator();

            Node initialLeader = nodeIterator.next();
            expectMetadataRequest(env, topicPartition, initialLeader);

            List<ProducerState> expected = asList(
                new ProducerState(12345L, 15, 30, env.time().milliseconds(),
                    OptionalInt.of(99), OptionalLong.empty()),
                new ProducerState(12345L, 15, 30, env.time().milliseconds(),
                    OptionalInt.empty(), OptionalLong.of(23423L))
            );

            DescribeProducersResponse response = buildDescribeProducersResponse(
                topicPartition,
                expected
            );

            env.kafkaClient().prepareResponseFrom(
                request -> {
                    // We need a sleep here because the client will attempt to
                    // backoff after the disconnect
                    env.time().sleep(retryBackoffMs);
                    return request instanceof DescribeProducersRequest;
                },
                response,
                initialLeader,
                true
            );

            Node retryLeader = nodeIterator.next();
            expectMetadataRequest(env, topicPartition, retryLeader);

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof DescribeProducersRequest,
                response,
                retryLeader
            );

            DescribeProducersResult result = env.adminClient().describeProducers(singleton(topicPartition));
            KafkaFuture<DescribeProducersResult.PartitionProducerState> partitionFuture =
                result.partitionResult(topicPartition);
            assertEquals(new HashSet<>(expected), new HashSet<>(partitionFuture.get().activeProducers()));
        }
    }

    @Test
    public void testDescribeTransactions() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            String transactionalId = "foo";
            Node coordinator = env.cluster().nodes().iterator().next();
            TransactionDescription expected = new TransactionDescription(
                coordinator.id(), TransactionState.COMPLETE_COMMIT, 12345L,
                15, 10000L, OptionalLong.empty(), emptySet());

            env.kafkaClient().prepareResponse(
                request -> request instanceof FindCoordinatorRequest,
                prepareFindCoordinatorResponse(Errors.NONE, transactionalId, coordinator)
            );

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof DescribeTransactionsRequest,
                new DescribeTransactionsResponse(new DescribeTransactionsResponseData().setTransactionStates(
                    singletonList(new DescribeTransactionsResponseData.TransactionState()
                        .setErrorCode(Errors.NONE.code())
                        .setProducerEpoch((short) expected.producerEpoch())
                        .setProducerId(expected.producerId())
                        .setTransactionalId(transactionalId)
                        .setTransactionTimeoutMs(10000)
                        .setTransactionStartTimeMs(-1)
                        .setTransactionState(expected.state().toString())
                    )
                )),
                coordinator
            );

            DescribeTransactionsResult result = env.adminClient().describeTransactions(singleton(transactionalId));
            KafkaFuture<TransactionDescription> future = result.description(transactionalId);
            assertEquals(expected, future.get());
        }
    }

    @Test
    public void testRetryDescribeTransactionsAfterNotCoordinatorError() throws Exception {
        MockTime time = new MockTime();
        int retryBackoffMs = 100;
        Cluster cluster = mockCluster(3, 0);
        Map<String, Object> configOverride = newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoffMs);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster, configOverride)) {
            String transactionalId = "foo";

            Iterator<Node> nodeIterator = env.cluster().nodes().iterator();
            Node coordinator1 = nodeIterator.next();
            Node coordinator2 = nodeIterator.next();

            env.kafkaClient().prepareResponse(
                request -> request instanceof FindCoordinatorRequest,
                new FindCoordinatorResponse(new FindCoordinatorResponseData()
                        .setCoordinators(singletonList(new FindCoordinatorResponseData.Coordinator()
                                .setKey(transactionalId)
                                .setErrorCode(Errors.NONE.code())
                                .setNodeId(coordinator1.id())
                                .setHost(coordinator1.host())
                                .setPort(coordinator1.port()))))
            );

            env.kafkaClient().prepareResponseFrom(
                request -> {
                    if (!(request instanceof DescribeTransactionsRequest)) {
                        return false;
                    } else {
                        // Backoff needed here for the retry of FindCoordinator
                        time.sleep(retryBackoffMs);
                        return true;
                    }
                },
                new DescribeTransactionsResponse(new DescribeTransactionsResponseData().setTransactionStates(
                    singletonList(new DescribeTransactionsResponseData.TransactionState()
                        .setErrorCode(Errors.NOT_COORDINATOR.code())
                        .setTransactionalId(transactionalId)
                    )
                )),
                coordinator1
            );

            env.kafkaClient().prepareResponse(
                request -> request instanceof FindCoordinatorRequest,
                new FindCoordinatorResponse(new FindCoordinatorResponseData()
                        .setCoordinators(singletonList(new FindCoordinatorResponseData.Coordinator()
                                .setKey(transactionalId)
                                .setErrorCode(Errors.NONE.code())
                                .setNodeId(coordinator2.id())
                                .setHost(coordinator2.host())
                                .setPort(coordinator2.port()))))
            );

            TransactionDescription expected = new TransactionDescription(
                coordinator2.id(), TransactionState.COMPLETE_COMMIT, 12345L,
                15, 10000L, OptionalLong.empty(), emptySet());

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof DescribeTransactionsRequest,
                new DescribeTransactionsResponse(new DescribeTransactionsResponseData().setTransactionStates(
                    singletonList(new DescribeTransactionsResponseData.TransactionState()
                        .setErrorCode(Errors.NONE.code())
                        .setProducerEpoch((short) expected.producerEpoch())
                        .setProducerId(expected.producerId())
                        .setTransactionalId(transactionalId)
                        .setTransactionTimeoutMs(10000)
                        .setTransactionStartTimeMs(-1)
                        .setTransactionState(expected.state().toString())
                    )
                )),
                coordinator2
            );

            DescribeTransactionsResult result = env.adminClient().describeTransactions(singleton(transactionalId));
            KafkaFuture<TransactionDescription> future = result.description(transactionalId);
            assertEquals(expected, future.get());
        }
    }

    @Test
    public void testAbortTransaction() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            TopicPartition topicPartition = new TopicPartition("foo", 13);
            AbortTransactionSpec abortSpec = new AbortTransactionSpec(
                topicPartition, 12345L, (short) 15, 200);
            Node leader = env.cluster().nodes().iterator().next();

            expectMetadataRequest(env, topicPartition, leader);

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof WriteTxnMarkersRequest,
                writeTxnMarkersResponse(abortSpec, Errors.NONE),
                leader
            );

            AbortTransactionResult result = env.adminClient().abortTransaction(abortSpec);
            assertNull(result.all().get());
        }
    }

    @Test
    public void testAbortTransactionFindLeaderAfterDisconnect() throws Exception {
        MockTime time = new MockTime();
        int retryBackoffMs = 100;
        Cluster cluster = mockCluster(3, 0);
        Map<String, Object> configOverride = newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoffMs);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster, configOverride)) {
            TopicPartition topicPartition = new TopicPartition("foo", 13);
            AbortTransactionSpec abortSpec = new AbortTransactionSpec(
                topicPartition, 12345L, (short) 15, 200);
            Iterator<Node> nodeIterator = env.cluster().nodes().iterator();
            Node firstLeader = nodeIterator.next();

            expectMetadataRequest(env, topicPartition, firstLeader);

            WriteTxnMarkersResponse response = writeTxnMarkersResponse(abortSpec, Errors.NONE);
            env.kafkaClient().prepareResponseFrom(
                request -> {
                    // We need a sleep here because the client will attempt to
                    // backoff after the disconnect
                    time.sleep(retryBackoffMs);
                    return request instanceof WriteTxnMarkersRequest;
                },
                response,
                firstLeader,
                true
            );

            Node retryLeader = nodeIterator.next();
            expectMetadataRequest(env, topicPartition, retryLeader);

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof WriteTxnMarkersRequest,
                response,
                retryLeader
            );

            AbortTransactionResult result = env.adminClient().abortTransaction(abortSpec);
            assertNull(result.all().get());
        }
    }

    @Test
    public void testForceTerminateTransaction() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            String transactionalId = "testForceTerminate";
            Node transactionCoordinator = env.cluster().nodes().iterator().next();

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(
                Errors.NONE,
                transactionalId,
                transactionCoordinator
            ));

            // Complete the init PID request successfully
            InitProducerIdResponseData initProducerIdResponseData = new InitProducerIdResponseData()
                .setProducerId(5678)
                .setProducerEpoch((short) 123);

            env.kafkaClient().prepareResponseFrom(request ->
                request instanceof InitProducerIdRequest,
                new InitProducerIdResponse(initProducerIdResponseData),
                transactionCoordinator
            );

            // Call force terminate and verify results
            TerminateTransactionResult result = env.adminClient().forceTerminateTransaction(transactionalId);
            assertNull(result.result().get());
        }
    }

    @Test
    public void testForceTerminateTransactionWithError() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            String transactionalId = "testForceTerminateError";
            Node transactionCoordinator = env.cluster().nodes().iterator().next();

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(
                Errors.NONE,
                transactionalId,
                transactionCoordinator
            ));

            // Return an error from the InitProducerId request
            env.kafkaClient().prepareResponseFrom(request ->
                request instanceof InitProducerIdRequest,
                new InitProducerIdResponse(new InitProducerIdResponseData()
                    .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code())),
                transactionCoordinator
            );

            // Call force terminate and verify error is propagated
            TerminateTransactionResult result = env.adminClient().forceTerminateTransaction(transactionalId);
            ExecutionException exception = assertThrows(ExecutionException.class, () -> result.result().get());
            assertTrue(exception.getCause() instanceof TransactionalIdAuthorizationException);
        }
    }

    @Test
    public void testForceTerminateTransactionWithCustomTimeout() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            String transactionalId = "testForceTerminateTimeout";
            Node transactionCoordinator = env.cluster().nodes().iterator().next();

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(
                Errors.NONE,
                transactionalId,
                transactionCoordinator
            ));

            // Complete the init PID request
            InitProducerIdResponseData initProducerIdResponseData = new InitProducerIdResponseData()
                .setProducerId(9012)
                .setProducerEpoch((short) 456);

            env.kafkaClient().prepareResponseFrom(request ->
                request instanceof InitProducerIdRequest,
                new InitProducerIdResponse(initProducerIdResponseData),
                transactionCoordinator
            );

            // Use custom timeout
            TerminateTransactionOptions options = new TerminateTransactionOptions().timeoutMs(10000);
            TerminateTransactionResult result = env.adminClient().forceTerminateTransaction(transactionalId, options);
            assertNull(result.result().get());
        }
    }

    @Test
    public void testListTransactions() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            MetadataResponseData.MetadataResponseBrokerCollection brokers =
                new MetadataResponseData.MetadataResponseBrokerCollection();

            env.cluster().nodes().forEach(node ->
                brokers.add(new MetadataResponseData.MetadataResponseBroker()
                    .setHost(node.host())
                    .setNodeId(node.id())
                    .setPort(node.port())
                    .setRack(node.rack())
                )
            );

            env.kafkaClient().prepareResponse(
                request -> request instanceof MetadataRequest,
                new MetadataResponse(new MetadataResponseData().setBrokers(brokers),
                    MetadataResponseData.HIGHEST_SUPPORTED_VERSION)
            );

            List<TransactionListing> expected = asList(
                new TransactionListing("foo", 12345L, TransactionState.ONGOING),
                new TransactionListing("bar", 98765L, TransactionState.PREPARE_ABORT),
                new TransactionListing("baz", 13579L, TransactionState.COMPLETE_COMMIT)
            );
            assertEquals(Set.of(0, 1, 2), env.cluster().nodes().stream().map(Node::id)
                .collect(Collectors.toSet()));

            env.cluster().nodes().forEach(node -> {
                ListTransactionsResponseData response = new ListTransactionsResponseData()
                    .setErrorCode(Errors.NONE.code());

                TransactionListing listing = expected.get(node.id());
                response.transactionStates().add(new ListTransactionsResponseData.TransactionState()
                    .setTransactionalId(listing.transactionalId())
                    .setProducerId(listing.producerId())
                    .setTransactionState(listing.state().toString())
                );

                env.kafkaClient().prepareResponseFrom(
                    request -> request instanceof ListTransactionsRequest,
                    new ListTransactionsResponse(response),
                    node
                );
            });

            ListTransactionsResult result = env.adminClient().listTransactions();
            assertEquals(new HashSet<>(expected), new HashSet<>(result.all().get()));
        }
    }

    @Test
    public void testFenceProducers() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            String transactionalId = "copyCat";
            Node transactionCoordinator = env.cluster().nodes().iterator().next();

            // fail to find the coordinator at first with a retriable error
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, transactionalId, transactionCoordinator));
            // and then succeed in the attempt to find the transaction coordinator
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, transactionalId, transactionCoordinator));
            // unfortunately, a coordinator load is in progress and we need to retry our init PID request
            env.kafkaClient().prepareResponseFrom(
                    request -> request instanceof InitProducerIdRequest,
                    new InitProducerIdResponse(new InitProducerIdResponseData().setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())),
                    transactionCoordinator
            );
            // then find out that the coordinator has changed since then
            env.kafkaClient().prepareResponseFrom(
                    request -> request instanceof InitProducerIdRequest,
                    new InitProducerIdResponse(new InitProducerIdResponseData().setErrorCode(Errors.NOT_COORDINATOR.code())),
                    transactionCoordinator
            );
            // and as a result, try once more to locate the coordinator (this time succeeding on the first try)
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, transactionalId, transactionCoordinator));
            // and finally, complete the init PID request
            InitProducerIdResponseData initProducerIdResponseData = new InitProducerIdResponseData()
                    .setProducerId(4761)
                    .setProducerEpoch((short) 489);
            env.kafkaClient().prepareResponseFrom(
                    request -> request instanceof InitProducerIdRequest,
                    new InitProducerIdResponse(initProducerIdResponseData),
                    transactionCoordinator
            );

            FenceProducersResult result = env.adminClient().fenceProducers(Collections.singleton(transactionalId));
            assertNull(result.all().get());
            assertEquals(4761, result.producerId(transactionalId).get());
            assertEquals((short) 489, result.epochId(transactionalId).get());
        }
    }

    private WriteTxnMarkersResponse writeTxnMarkersResponse(
        AbortTransactionSpec abortSpec,
        Errors error
    ) {
        WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult partitionResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                .setPartitionIndex(abortSpec.topicPartition().partition())
                .setErrorCode(error.code());

        WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult topicResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
                .setName(abortSpec.topicPartition().topic());
        topicResponse.partitions().add(partitionResponse);

        WriteTxnMarkersResponseData.WritableTxnMarkerResult markerResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
                .setProducerId(abortSpec.producerId());
        markerResponse.topics().add(topicResponse);

        WriteTxnMarkersResponseData response = new WriteTxnMarkersResponseData();
        response.markers().add(markerResponse);

        return new WriteTxnMarkersResponse(response);
    }

    private DescribeProducersResponse buildDescribeProducersResponse(
        TopicPartition topicPartition,
        List<ProducerState> producerStates
    ) {
        DescribeProducersResponseData response = new DescribeProducersResponseData();

        DescribeProducersResponseData.TopicResponse topicResponse =
            new DescribeProducersResponseData.TopicResponse()
                .setName(topicPartition.topic());
        response.topics().add(topicResponse);

        DescribeProducersResponseData.PartitionResponse partitionResponse =
            new DescribeProducersResponseData.PartitionResponse()
                .setPartitionIndex(topicPartition.partition())
                .setErrorCode(Errors.NONE.code());
        topicResponse.partitions().add(partitionResponse);

        partitionResponse.setActiveProducers(producerStates.stream().map(producerState ->
            new DescribeProducersResponseData.ProducerState()
                .setProducerId(producerState.producerId())
                .setProducerEpoch(producerState.producerEpoch())
                .setCoordinatorEpoch(producerState.coordinatorEpoch().orElse(-1))
                .setLastSequence(producerState.lastSequence())
                .setLastTimestamp(producerState.lastTimestamp())
                .setCurrentTxnStartOffset(producerState.currentTransactionStartOffset().orElse(-1L))
        ).collect(Collectors.toList()));

        return new DescribeProducersResponse(response);
    }

    private void expectMetadataRequest(
        AdminClientUnitTestEnv env,
        TopicPartition topicPartition,
        Node leader
    ) {
        MetadataResponseData.MetadataResponseTopicCollection responseTopics =
            new MetadataResponseData.MetadataResponseTopicCollection();

        MetadataResponseTopic responseTopic = new MetadataResponseTopic()
            .setName(topicPartition.topic())
            .setErrorCode(Errors.NONE.code());
        responseTopics.add(responseTopic);

        MetadataResponsePartition responsePartition = new MetadataResponsePartition()
            .setErrorCode(Errors.NONE.code())
            .setPartitionIndex(topicPartition.partition())
            .setLeaderId(leader.id())
            .setReplicaNodes(singletonList(leader.id()))
            .setIsrNodes(singletonList(leader.id()));
        responseTopic.partitions().add(responsePartition);

        env.kafkaClient().prepareResponse(
            request -> {
                if (!(request instanceof MetadataRequest)) {
                    return false;
                }
                MetadataRequest metadataRequest = (MetadataRequest) request;
                return metadataRequest.topics().equals(singletonList(topicPartition.topic()));
            },
            new MetadataResponse(new MetadataResponseData().setTopics(responseTopics),
                MetadataResponseData.HIGHEST_SUPPORTED_VERSION)
        );
    }
}
