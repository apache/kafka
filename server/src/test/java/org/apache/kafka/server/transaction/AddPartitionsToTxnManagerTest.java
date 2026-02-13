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

package org.apache.kafka.server.transaction;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransaction;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.AddPartitionsToTxnTransactionCollection;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResultCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager.AppendCallback;
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager.TransactionSupportedOperation;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AddPartitionsToTxnManagerTest {
    private final NetworkClient networkClient = mock(NetworkClient.class);
    private final MetadataCache metadataCache = mock(MetadataCache.class);
    @SuppressWarnings("unchecked")
    private final Function<String, Integer> partitionFor = mock(Function.class);
    private final MockTime time = new MockTime();
    private final AbstractKafkaConfig config = new AbstractKafkaConfig(
            AbstractKafkaConfig.CONFIG_DEF,
            Map.of(
                KRaftConfigs.PROCESS_ROLES_CONFIG, "broker", 
                KRaftConfigs.NODE_ID_CONFIG, "1",
                KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER"),
            Map.of(),
            false) {
        @Override
        public void addReconfigurable(org.apache.kafka.common.Reconfigurable reconfigurable) {
            // No-op for test
        }

        @Override
        public void removeReconfigurable(org.apache.kafka.common.Reconfigurable reconfigurable) {
            // No-op for test
        }
    };
    private final AddPartitionsToTxnManager addPartitionsToTxnManager =
            new AddPartitionsToTxnManager(config, networkClient, metadataCache, partitionFor, time);

    private final String topic = "foo";
    private final List<TopicPartition> topicPartitions = List.of(
            new TopicPartition(topic, 1),
            new TopicPartition(topic, 2),
            new TopicPartition(topic, 3));

    private final Node node0 = new Node(0, "host1", 0);
    private final Node node1 = new Node(1, "host2", 1);
    private final Node node2 = new Node(2, "host2", 2);

    private final String transactionalId1 = "txn1";
    private final String transactionalId2 = "txn2";
    private final String transactionalId3 = "txn3";

    private final long producerId1 = 0L;
    private final long producerId2 = 1L;
    private final long producerId3 = 2L;

    private final ClientResponse authenticationErrorResponse =
            clientResponse(null, new SaslAuthenticationException(""), null, false);
    private final ClientResponse versionMismatchResponse =
            clientResponse(null, null, new UnsupportedVersionException(""), false);
    private final ClientResponse disconnectedResponse =
            clientResponse(null, null, null, true);

    @AfterEach
    public void teardown() throws InterruptedException {
        addPartitionsToTxnManager.shutdown();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testAddTxnData(boolean isAddPartition) {
        var operation = isAddPartition ?
                TransactionSupportedOperation.ADD_PARTITION :
                TransactionSupportedOperation.GENERIC_ERROR_SUPPORTED;

        when(partitionFor.apply(transactionalId1)).thenReturn(0);
        when(partitionFor.apply(transactionalId2)).thenReturn(1);
        when(partitionFor.apply(transactionalId3)).thenReturn(0);

        mockTransactionStateMetadata(0, 0, Optional.of(node0));
        mockTransactionStateMetadata(1, 1, Optional.of(node1));

        Map<TopicPartition, Errors> transaction1Errors = new HashMap<>();
        Map<TopicPartition, Errors> transaction2Errors = new HashMap<>();
        Map<TopicPartition, Errors> transaction3Errors = new HashMap<>();

        addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId1, producerId1, (short) 0,
                topicPartitions, setErrors(transaction1Errors), operation);
        addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId2, producerId2, (short) 0,
                topicPartitions, setErrors(transaction2Errors), operation);
        addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId3, producerId3, (short) 0,
                topicPartitions, setErrors(transaction3Errors), operation);

        // We will try to add transaction1 3 more times (retries).
        // One will have the same epoch, one will have a newer epoch,
        // and one will have an older epoch than the new one we just added.
        Map<TopicPartition, Errors> transaction1RetryWithSameEpochErrors = new HashMap<>();
        Map<TopicPartition, Errors> transaction1RetryWithNewerEpochErrors = new HashMap<>();
        Map<TopicPartition, Errors> transaction1RetryWithOldEpochErrors = new HashMap<>();

        // Trying to add more transactional data for the same transactional ID, producer ID,
        // and epoch should simply replace the old data and send a retriable response.
        addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId1, producerId1, (short) 0,
                topicPartitions, setErrors(transaction1RetryWithSameEpochErrors), operation);
        var expectedNetworkErrors = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), e -> Errors.NETWORK_EXCEPTION));
        assertEquals(expectedNetworkErrors, transaction1Errors);

        // Trying to add more transactional data for the same transactional ID and producer ID,
        // but a new epoch should replace the old data and send an error response for it.
        addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId1, producerId1, (short) 1,
                topicPartitions, setErrors(transaction1RetryWithNewerEpochErrors), operation);
        var expectedEpochErrors = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), e -> Errors.INVALID_PRODUCER_EPOCH));
        assertEquals(expectedEpochErrors, transaction1RetryWithSameEpochErrors);

        // Trying to add more transactional data for the same transactional ID and producer ID,
        // but an older epoch should immediately return with error and keep the old data queued to send.
        addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId1, producerId1, (short) 0,
                topicPartitions, setErrors(transaction1RetryWithOldEpochErrors), operation);
        assertEquals(expectedEpochErrors, transaction1RetryWithOldEpochErrors);

        addPartitionsToTxnManager.generateRequests().forEach(requestAndHandler -> {
            if (requestAndHandler.destination.equals(node0)) {
                assertEquals(time.milliseconds(), requestAndHandler.creationTimeMs);
                var transactions = new AddPartitionsToTxnTransactionCollection(
                        List.of(transactionData(transactionalId3, producerId3, (short) 0, !isAddPartition),
                                transactionData(transactionalId1, producerId1, (short) 1, !isAddPartition)).iterator());
                assertEquals(
                        AddPartitionsToTxnRequest.Builder.forBroker(transactions).data,
                        ((AddPartitionsToTxnRequest.Builder) requestAndHandler.request).data);
            } else {
                verifyRequest(node1, transactionalId2, producerId2, !isAddPartition, requestAndHandler);
            }
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGenerateRequests(boolean isAddPartition) {
        when(partitionFor.apply(transactionalId1)).thenReturn(0);
        when(partitionFor.apply(transactionalId2)).thenReturn(1);
        when(partitionFor.apply(transactionalId3)).thenReturn(2);
        mockTransactionStateMetadata(0, 0, Optional.of(node0));
        mockTransactionStateMetadata(1, 1, Optional.of(node1));
        mockTransactionStateMetadata(2, 2, Optional.of(node2));

        var operation = isAddPartition
                ? TransactionSupportedOperation.ADD_PARTITION
                : TransactionSupportedOperation.GENERIC_ERROR_SUPPORTED;

        Map<TopicPartition, Errors> transactionErrors = new HashMap<>();

        addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId1, producerId1, (short) 0,
                topicPartitions, setErrors(transactionErrors), operation);
        addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId2, producerId2, (short) 0,
                topicPartitions, setErrors(transactionErrors), operation);

        var requestsAndHandlers = addPartitionsToTxnManager.generateRequests();
        assertEquals(2, requestsAndHandlers.size());
        // Note: handlers are tested in testAddPartitionsToTxnHandlerErrorHandling
        requestsAndHandlers.forEach(requestAndHandler -> {
            if (requestAndHandler.destination.equals(node0)) {
                verifyRequest(node0, transactionalId1, producerId1, !isAddPartition, requestAndHandler);
            } else {
                verifyRequest(node1, transactionalId2, producerId2, !isAddPartition, requestAndHandler);
            }
        });

        addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId2, producerId2, (short) 0,
                topicPartitions, setErrors(transactionErrors), operation);
        addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId3, producerId3, (short) 0,
                topicPartitions, setErrors(transactionErrors), operation);

        // Test creationTimeMs increases too.
        time.sleep(10);

        var requestsAndHandlers2 = addPartitionsToTxnManager.generateRequests();
        // The request for node1 should not be added because one request is already inflight.
        assertEquals(1, requestsAndHandlers2.size());
        requestsAndHandlers2.forEach(requestAndHandler ->
                verifyRequest(node2, transactionalId3, producerId3, !isAddPartition, requestAndHandler));

        // Complete the request for node1 so the new one can go through.
        requestsAndHandlers.stream()
                .filter(requestAndHandler -> requestAndHandler.destination.equals(node1))
                .findFirst()
                .orElseThrow()
                .handler.onComplete(authenticationErrorResponse);
        var requestsAndHandlers3 = addPartitionsToTxnManager.generateRequests();
        assertEquals(1, requestsAndHandlers3.size());
        requestsAndHandlers3.forEach(requestAndHandler ->
            verifyRequest(node1, transactionalId2, producerId2, !isAddPartition, requestAndHandler));
    }

    @Test
    public void testTransactionCoordinatorResolution() {
        when(partitionFor.apply(transactionalId1)).thenReturn(0);

        Runnable checkError = () -> {
            Map<TopicPartition, Errors> errors = new HashMap<>();

            addPartitionsToTxnManager.addOrVerifyTransaction(
                    transactionalId1,
                    producerId1,
                    (short) 0,
                    topicPartitions,
                    setErrors(errors),
                    TransactionSupportedOperation.GENERIC_ERROR_SUPPORTED);

            var expected = topicPartitions.stream()
                    .collect(Collectors.toMap(Function.identity(), e -> Errors.COORDINATOR_NOT_AVAILABLE));
            assertEquals(expected, errors);
        };

        // The transaction state topic does not exist.
        when(metadataCache.getLeaderAndIsr(Topic.TRANSACTION_STATE_TOPIC_NAME, 0))
                .thenReturn(Optional.empty());
        checkError.run();

        // The partition has no leader
        mockTransactionStateMetadata(0, -1, Optional.empty());
        checkError.run();

        // The leader is not available
        mockTransactionStateMetadata(0, 0, Optional.empty());
        checkError.run();
    }

    @Test
    public void testAddPartitionsToTxnHandlerErrorHandling() {
        when(partitionFor.apply(transactionalId1)).thenReturn(0);
        when(partitionFor.apply(transactionalId2)).thenReturn(0);
        mockTransactionStateMetadata(0, 0, Optional.of(node0));

        Map<TopicPartition, Errors> transaction1Errors = new HashMap<>();
        Map<TopicPartition, Errors> transaction2Errors = new HashMap<>();

        Runnable addTransactionsToVerify = () -> {
            transaction1Errors.clear();
            transaction2Errors.clear();

            addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId1, producerId1, (short) 0,
                    topicPartitions, setErrors(transaction1Errors), TransactionSupportedOperation.GENERIC_ERROR_SUPPORTED);
            addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId2, producerId2, (short) 0,
                    topicPartitions, setErrors(transaction2Errors), TransactionSupportedOperation.GENERIC_ERROR_SUPPORTED);
        };

        Consumer<TransactionSupportedOperation> addTransactionsToVerifyRequestVersion = operationExpected -> {
            transaction1Errors.clear();
            transaction2Errors.clear();

            addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId1, producerId1, (short) 0,
                    topicPartitions, setErrors(transaction1Errors), operationExpected);
            addPartitionsToTxnManager.addOrVerifyTransaction(transactionalId2, producerId2, (short) 0,
                    topicPartitions, setErrors(transaction2Errors), operationExpected);
        };

        var expectedAuthErrors = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), e -> Errors.SASL_AUTHENTICATION_FAILED));
        addTransactionsToVerify.run();
        receiveResponse(authenticationErrorResponse);
        assertEquals(expectedAuthErrors, transaction1Errors);
        assertEquals(expectedAuthErrors, transaction2Errors);

        // On version mismatch we ignore errors and keep handling.
        var expectedVersionMismatchErrors = new HashMap<>();
        addTransactionsToVerify.run();
        receiveResponse(versionMismatchResponse);
        assertEquals(expectedVersionMismatchErrors, transaction1Errors);
        assertEquals(expectedVersionMismatchErrors, transaction2Errors);

        var expectedDisconnectedErrors = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), e -> Errors.NETWORK_EXCEPTION));
        addTransactionsToVerify.run();
        receiveResponse(disconnectedResponse);
        assertEquals(expectedDisconnectedErrors, transaction1Errors);
        assertEquals(expectedDisconnectedErrors, transaction2Errors);

        var expectedTopLevelErrors = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), e -> Errors.INVALID_TXN_STATE));
        var topLevelErrorAddPartitionsResponse = new AddPartitionsToTxnResponse(
                new AddPartitionsToTxnResponseData().setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code()));
        var topLevelErrorResponse = clientResponse(topLevelErrorAddPartitionsResponse, null, null, false);
        addTransactionsToVerify.run();
        receiveResponse(topLevelErrorResponse);
        assertEquals(expectedTopLevelErrors, transaction1Errors);
        assertEquals(expectedTopLevelErrors, transaction2Errors);

        var preConvertedTransaction1Errors = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), e -> Errors.PRODUCER_FENCED));
        var expectedTransaction1Errors = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), e -> Errors.INVALID_PRODUCER_EPOCH));
        var preConvertedTransaction2Errors = Map.of(
                new TopicPartition("foo", 1), Errors.NONE,
                new TopicPartition("foo", 2), Errors.INVALID_TXN_STATE,
                new TopicPartition("foo", 3), Errors.NONE);
        var expectedTransaction2Errors = Map.of(new TopicPartition("foo", 2), Errors.INVALID_TXN_STATE);

        var transaction1ErrorResponse = AddPartitionsToTxnResponse.resultForTransaction(
                transactionalId1, preConvertedTransaction1Errors);
        var transaction2ErrorResponse = AddPartitionsToTxnResponse.resultForTransaction(
                transactionalId2, preConvertedTransaction2Errors);
        var mixedErrorsAddPartitionsResponse = new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData()
                .setResultsByTransaction(new AddPartitionsToTxnResultCollection(
                        List.of(transaction1ErrorResponse, transaction2ErrorResponse).iterator())));
        var mixedErrorsResponse = clientResponse(mixedErrorsAddPartitionsResponse, null, null, false);

        addTransactionsToVerify.run();
        receiveResponse(mixedErrorsResponse);
        assertEquals(expectedTransaction1Errors, transaction1Errors);
        assertEquals(expectedTransaction2Errors, transaction2Errors);

        var preConvertedTransactionAbortableErrorsTxn1 = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), e -> Errors.TRANSACTION_ABORTABLE));
        var preConvertedTransactionAbortableErrorsTxn2 = Map.of(
                new TopicPartition("foo", 1), Errors.NONE,
                new TopicPartition("foo", 2), Errors.TRANSACTION_ABORTABLE,
                new TopicPartition("foo", 3), Errors.NONE);
        var transactionAbortableErrorResponseTxn1 =
                AddPartitionsToTxnResponse.resultForTransaction(transactionalId1, preConvertedTransactionAbortableErrorsTxn1);
        var transactionAbortableErrorResponseTxn2 =
                AddPartitionsToTxnResponse.resultForTransaction(transactionalId2, preConvertedTransactionAbortableErrorsTxn2);
        var mixedErrorsAddPartitionsResponseAbortableError = new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData()
                .setResultsByTransaction(new AddPartitionsToTxnResultCollection(
                        List.of(transactionAbortableErrorResponseTxn1, transactionAbortableErrorResponseTxn2).iterator())));
        var mixedAbortableErrorsResponse =
                clientResponse(mixedErrorsAddPartitionsResponseAbortableError, null, null, false);

        var expectedTransactionAbortableErrorsTxn1LowerVersion = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), e -> Errors.INVALID_TXN_STATE));
        var expectedTransactionAbortableErrorsTxn2LowerVersion =
                Map.of(new TopicPartition("foo", 2), Errors.INVALID_TXN_STATE);

        var expectedTransactionAbortableErrorsTxn1HigherVersion = topicPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), e -> Errors.TRANSACTION_ABORTABLE));
        var expectedTransactionAbortableErrorsTxn2HigherVersion =
                Map.of(new TopicPartition("foo", 2), Errors.TRANSACTION_ABORTABLE);

        addTransactionsToVerifyRequestVersion.accept(TransactionSupportedOperation.DEFAULT_ERROR);
        receiveResponse(mixedAbortableErrorsResponse);
        assertEquals(expectedTransactionAbortableErrorsTxn1LowerVersion, transaction1Errors);
        assertEquals(expectedTransactionAbortableErrorsTxn2LowerVersion, transaction2Errors);

        addTransactionsToVerifyRequestVersion.accept(TransactionSupportedOperation.GENERIC_ERROR_SUPPORTED);
        receiveResponse(mixedAbortableErrorsResponse);
        assertEquals(expectedTransactionAbortableErrorsTxn1HigherVersion, transaction1Errors);
        assertEquals(expectedTransactionAbortableErrorsTxn2HigherVersion, transaction2Errors);
    }

    @Test
    public void testAddPartitionsToTxnManagerMetrics() throws InterruptedException {
        var startTime = time.milliseconds();
        Map<TopicPartition, Errors> transactionErrors = new HashMap<>();

        var maxVerificationTime = new AtomicLong(0);
        var mockVerificationFailureMeter = mock(Meter.class);
        var mockVerificationTime = mock(Histogram.class);

        when(partitionFor.apply(transactionalId1)).thenReturn(0);
        when(partitionFor.apply(transactionalId2)).thenReturn(1);
        mockTransactionStateMetadata(0, 0, Optional.of(node0));
        mockTransactionStateMetadata(1, 1, Optional.of(node1));

        // Update max verification time when we see a higher verification time.
        doAnswer(invocation -> {
            long newTime = invocation.getArgument(0);
            if (newTime > maxVerificationTime.get()) {
                maxVerificationTime.set(newTime);
            }
            return null;
        }).when(mockVerificationTime).update(anyLong());

        var mockMetricsGroupCtor = mockConstruction(KafkaMetricsGroup.class, (mock, context) -> {
            when(mock.newMeter(eq(AddPartitionsToTxnManager.VERIFICATION_FAILURE_RATE_METRIC_NAME), anyString(), any(TimeUnit.class)))
                    .thenReturn(mockVerificationFailureMeter);
            when(mock.newHistogram(eq(AddPartitionsToTxnManager.VERIFICATION_TIME_MS_METRIC_NAME)))
                    .thenReturn(mockVerificationTime);
        });

        var addPartitionsManagerWithMockedMetrics = new AddPartitionsToTxnManager(
                config, networkClient, metadataCache, partitionFor, time);

        try {
            addPartitionsManagerWithMockedMetrics.addOrVerifyTransaction(transactionalId1, producerId1, (short) 0,
                    topicPartitions, setErrors(transactionErrors), TransactionSupportedOperation.GENERIC_ERROR_SUPPORTED);
            addPartitionsManagerWithMockedMetrics.addOrVerifyTransaction(transactionalId2, producerId2, (short) 0,
                    topicPartitions, setErrors(transactionErrors), TransactionSupportedOperation.GENERIC_ERROR_SUPPORTED);

            time.sleep(100);

            var requestsAndHandlers = addPartitionsManagerWithMockedMetrics.generateRequests();
            var requestsHandled = 0;
            for (RequestAndCompletionHandler handler : requestsAndHandlers) {
                time.sleep(100);
                handler.handler.onComplete(authenticationErrorResponse);
                requestsHandled++;
                verify(mockVerificationTime, times(requestsHandled)).update(anyLong());
                assertEquals(maxVerificationTime.get(), time.milliseconds() - startTime);
                verify(mockVerificationFailureMeter, times(requestsHandled)).mark(3);
            }

            // shutdown the manager so that metrics are removed.
            addPartitionsManagerWithMockedMetrics.shutdown();

            var mockMetricsGroup = mockMetricsGroupCtor.constructed().get(0);

            verify(mockMetricsGroup).newMeter(eq(AddPartitionsToTxnManager.VERIFICATION_FAILURE_RATE_METRIC_NAME), anyString(), any(TimeUnit.class));
            verify(mockMetricsGroup).newHistogram(eq(AddPartitionsToTxnManager.VERIFICATION_TIME_MS_METRIC_NAME));
            verify(mockMetricsGroup).removeMetric(AddPartitionsToTxnManager.VERIFICATION_FAILURE_RATE_METRIC_NAME);
            verify(mockMetricsGroup).removeMetric(AddPartitionsToTxnManager.VERIFICATION_TIME_MS_METRIC_NAME);

            // assert that we have verified all invocations on the metrics group.
            verifyNoMoreInteractions(mockMetricsGroup);
        } finally {
            if (mockMetricsGroupCtor != null) {
                mockMetricsGroupCtor.close();
            }
            if (addPartitionsManagerWithMockedMetrics.isRunning()) {
                addPartitionsManagerWithMockedMetrics.shutdown();
            }
        }
    }

    private AppendCallback setErrors(Map<TopicPartition, Errors> errors) {
        return errors::putAll;
    }

    private void mockTransactionStateMetadata(int partitionIndex, int leaderId, Optional<Node> leaderNode) {
        when(metadataCache.getLeaderAndIsr(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionIndex))
                .thenReturn(Optional.of(new LeaderAndIsr(leaderId, List.of(leaderId))));
        if (leaderId != MetadataResponse.NO_LEADER_ID) {
            when(metadataCache.getAliveBrokerNode(leaderId, config.interBrokerListenerName()))
                    .thenReturn(leaderNode);
        }
    }

    private ClientResponse clientResponse(
            AbstractResponse response,
            AuthenticationException authException,
            UnsupportedVersionException mismatchException,
            boolean disconnected) {
        return new ClientResponse(null, null, null, 0, 0,
                disconnected, mismatchException, authException, response);
    }

    private AddPartitionsToTxnTransaction transactionData(
            String transactionalId,
            long producerId,
            short producerEpoch,
            boolean verifyOnly) {
        return new AddPartitionsToTxnTransaction()
                .setTransactionalId(transactionalId)
                .setProducerId(producerId)
                .setProducerEpoch(producerEpoch)
                .setVerifyOnly(verifyOnly)
                .setTopics(new AddPartitionsToTxnTopicCollection(
                        List.of(new AddPartitionsToTxnTopic()
                                .setName(topic)
                                .setPartitions(List.of(1, 2, 3))).iterator()));
    }

    private void receiveResponse(ClientResponse response) {
        addPartitionsToTxnManager.generateRequests().stream().findFirst().orElseThrow().handler.onComplete(response);
    }

    private void verifyRequest(
            Node expectedDestination,
            String transactionalId,
            long producerId,
            boolean verifyOnly,
            RequestAndCompletionHandler requestAndHandler) {
        assertEquals(time.milliseconds(), requestAndHandler.creationTimeMs);
        assertEquals(expectedDestination, requestAndHandler.destination);
        assertEquals(
                AddPartitionsToTxnRequest.Builder.forBroker(
                        new AddPartitionsToTxnTransactionCollection(
                                List.of(transactionData(transactionalId, producerId, (short) 0, verifyOnly)).iterator())
                ).data,
                ((AddPartitionsToTxnRequest.Builder) requestAndHandler.request).data);
    }
}
