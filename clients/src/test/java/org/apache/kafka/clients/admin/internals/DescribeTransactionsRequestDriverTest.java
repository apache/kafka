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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.internals.RequestDriver.RequestSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.DescribeTransactionsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DescribeTransactionsRequestDriverTest {
    private final MockTime time = new MockTime();
    private final long deadlineMs = time.milliseconds() + 10000;
    private final long retryBackoffMs = 100;

    @Test
    public void testDescribeTransactions() throws Exception {
        String transactionalId1 = "foo";
        String transactionalId2 = "bar";
        Set<String> transactionalIds = mkSet(transactionalId1, transactionalId2);

        DescribeTransactionsRequestDriver driver = new DescribeTransactionsRequestDriver(
            transactionalIds,
            deadlineMs,
            retryBackoffMs
        );

        // Send `FindCoordinator` requests
        List<RequestSpec<CoordinatorKey>> lookupRequests = driver.poll();
        assertEquals(2, lookupRequests.size());

        RequestSpec<CoordinatorKey> lookupSpec1 =
            findRequestWithKey(transactionalId1, lookupRequests);
        assertRetryBackoff(0, lookupSpec1);
        assertFindCoordinatorRequest(transactionalId1, lookupSpec1);

        RequestSpec<CoordinatorKey> lookupSpec2 =
            findRequestWithKey(transactionalId2, lookupRequests);
        assertRetryBackoff(0, lookupSpec2);
        assertFindCoordinatorRequest(transactionalId2, lookupSpec2);

        // Receive `FindCoordinator` responses
        int coordinator1 = 1;
        int coordinator2 = 3;

        driver.onResponse(time.milliseconds(), lookupSpec1,
            findCoordinatorResponse(OptionalInt.of(coordinator1)));
        driver.onResponse(time.milliseconds(), lookupSpec2,
            findCoordinatorResponse(OptionalInt.of(coordinator2)));

        // Send `DescribeTransactions` requests
        List<RequestSpec<CoordinatorKey>> requests = driver.poll();
        assertEquals(2, requests.size());

        RequestSpec<CoordinatorKey> requestSpec1 =
            findRequestWithKey(transactionalId1, requests);
        assertRetryBackoff(0, requestSpec1);
        assertDescribeTransactionsRequest(singleton(transactionalId1), coordinator1, requestSpec1);

        RequestSpec<CoordinatorKey> requestSpec2 =
            findRequestWithKey(transactionalId2, requests);
        assertRetryBackoff(0, requestSpec2);
        assertDescribeTransactionsRequest(singleton(transactionalId2), coordinator2, requestSpec2);

        // Receive `DescribeTransactions` responses
        DescribeTransactionsResponseData.TransactionState transactionState1 =
            sampleTransactionState1(transactionalId1);
        DescribeTransactionsResponseData.TransactionState transactionState2 =
            sampleTransactionState2(transactionalId2);

        driver.onResponse(time.milliseconds(), requestSpec1, new DescribeTransactionsResponse(
            new DescribeTransactionsResponseData().setTransactionStates(singletonList(transactionState1))));
        driver.onResponse(time.milliseconds(), requestSpec2, new DescribeTransactionsResponse(
            new DescribeTransactionsResponseData().setTransactionStates(singletonList(transactionState2))));

        // We are all done, so there should be no requests left to be sent
        assertEquals(Collections.emptyList(), driver.poll());

        KafkaFutureImpl<TransactionDescription> future1 = driver.futures()
            .get(DescribeTransactionsRequestDriver.asCoordinatorKey(transactionalId1));
        KafkaFutureImpl<TransactionDescription> future2 = driver.futures()
            .get(DescribeTransactionsRequestDriver.asCoordinatorKey(transactionalId2));

        assertTrue(future1.isDone());
        assertMatchingTransactionState(coordinator1, transactionState1, future1.get());
        assertTrue(future2.isDone());
        assertMatchingTransactionState(coordinator2, transactionState2, future2.get());
    }

    @Test
    public void testDescribeTransactionsBatching() throws Exception {
        String transactionalId1 = "foo";
        String transactionalId2 = "bar";
        Set<String> transactionalIds = mkSet(transactionalId1, transactionalId2);

        DescribeTransactionsRequestDriver driver = new DescribeTransactionsRequestDriver(
            transactionalIds,
            deadlineMs,
            retryBackoffMs
        );

        // Send `FindCoordinator` requests
        List<RequestSpec<CoordinatorKey>> lookupRequests = driver.poll();
        assertEquals(2, lookupRequests.size());

        RequestSpec<CoordinatorKey> lookupSpec1 =
            findRequestWithKey(transactionalId1, lookupRequests);
        assertRetryBackoff(0, lookupSpec1);
        assertFindCoordinatorRequest(transactionalId1, lookupSpec1);

        RequestSpec<CoordinatorKey> loookupSpec2 =
            findRequestWithKey(transactionalId2, lookupRequests);
        assertRetryBackoff(0, loookupSpec2);
        assertFindCoordinatorRequest(transactionalId2, loookupSpec2);

        // Receive `FindCoordinator` responses
        int coordinator = 1;

        // Since both transactionalIds map to the same coordinator, then there
        // should only be a single `DescribeTransactions` request sent
        driver.onResponse(time.milliseconds(), lookupSpec1,
            findCoordinatorResponse(OptionalInt.of(coordinator)));
        driver.onResponse(time.milliseconds(), loookupSpec2,
            findCoordinatorResponse(OptionalInt.of(coordinator)));

        // Send `DescribeTransactions` request
        List<RequestSpec<CoordinatorKey>> requests = driver.poll();
        assertEquals(1, requests.size());

        RequestSpec<CoordinatorKey> requestSpec = requests.get(0);
        assertRetryBackoff(0, requestSpec);
        assertDescribeTransactionsRequest(transactionalIds, coordinator, requestSpec);

        // Receive `DescribeTransactions` response
        DescribeTransactionsResponseData.TransactionState transactionState1 =
            sampleTransactionState1(transactionalId1);
        DescribeTransactionsResponseData.TransactionState transactionState2 =
            sampleTransactionState2(transactionalId2);

        driver.onResponse(time.milliseconds(), requestSpec, new DescribeTransactionsResponse(
            new DescribeTransactionsResponseData()
                .setTransactionStates(asList(transactionState1, transactionState2))));

        // We are all done, so there should be no requests left to be sent
        assertEquals(Collections.emptyList(), driver.poll());

        KafkaFutureImpl<TransactionDescription> future1 = driver.futures()
            .get(DescribeTransactionsRequestDriver.asCoordinatorKey(transactionalId1));
        KafkaFutureImpl<TransactionDescription> future2 = driver.futures()
            .get(DescribeTransactionsRequestDriver.asCoordinatorKey(transactionalId2));

        assertTrue(future1.isDone());
        assertMatchingTransactionState(coordinator, transactionState1, future1.get());
        assertTrue(future2.isDone());
        assertMatchingTransactionState(coordinator, transactionState2, future2.get());
    }

    @Test
    public void testShouldRetryDescribeTransactionsIfCoordinatorLoadingInProgress() {
        String transactionalId = "foo";

        DescribeTransactionsRequestDriver driver = new DescribeTransactionsRequestDriver(
            singleton(transactionalId),
            deadlineMs,
            retryBackoffMs
        );

        // Send first `FindCoordinator` request
        List<RequestSpec<CoordinatorKey>> lookupRequests = driver.poll();
        assertEquals(1, lookupRequests.size());
        RequestSpec<CoordinatorKey> lookupSpec = lookupRequests.get(0);
        assertRetryBackoff(0, lookupSpec);
        assertFindCoordinatorRequest(transactionalId, lookupSpec);

        int coordinator = 5;
        driver.onResponse(time.milliseconds(), lookupSpec,
            findCoordinatorResponse(OptionalInt.of(coordinator)));

        // Send `DescribeTransactions` request
        List<RequestSpec<CoordinatorKey>> requests1 = driver.poll();
        assertEquals(1, requests1.size());
        RequestSpec<CoordinatorKey> requestSpec1 = requests1.get(0);
        assertRetryBackoff(0, requestSpec1);
        assertDescribeTransactionsRequest(singleton(transactionalId), coordinator, requestSpec1);

        // Receive `DescribeTransactions` response
        driver.onResponse(time.milliseconds(), requestSpec1, new DescribeTransactionsResponse(
            new DescribeTransactionsResponseData().setTransactionStates(
                singletonList(new DescribeTransactionsResponseData.TransactionState()
                    .setTransactionalId(transactionalId)
                    .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())))
        ));

        // Send retry `DescribeTransactions` request
        assertFalse(futureFor(driver, transactionalId).isDone());
        List<RequestSpec<CoordinatorKey>> requests2 = driver.poll();
        assertEquals(1, requests2.size());
        RequestSpec<CoordinatorKey> requestSpec2 = requests2.get(0);
        assertRetryBackoff(1, requestSpec2);
        assertDescribeTransactionsRequest(singleton(transactionalId), coordinator, requestSpec2);
    }

    @Test
    public void testShouldRetryFindCoordinatorAfterNotCoordinatorError() {
        String transactionalId = "foo";

        DescribeTransactionsRequestDriver driver = new DescribeTransactionsRequestDriver(
            singleton(transactionalId),
            deadlineMs,
            retryBackoffMs
        );

        // Send first `FindCoordinator` request
        List<RequestSpec<CoordinatorKey>> lookupRequests1 = driver.poll();
        assertEquals(1, lookupRequests1.size());
        RequestSpec<CoordinatorKey> lookupSpec1 = lookupRequests1.get(0);
        assertRetryBackoff(0, lookupSpec1);
        assertFindCoordinatorRequest(transactionalId, lookupSpec1);

        int coordinator = 5;
        driver.onResponse(time.milliseconds(), lookupSpec1,
            findCoordinatorResponse(OptionalInt.of(coordinator)));

        // Send `DescribeTransactions` request
        List<RequestSpec<CoordinatorKey>> requests = driver.poll();
        assertEquals(1, requests.size());
        RequestSpec<CoordinatorKey> requestSpec = requests.get(0);
        assertDescribeTransactionsRequest(singleton(transactionalId), coordinator, requestSpec);

        driver.onResponse(time.milliseconds(), requestSpec, new DescribeTransactionsResponse(
            new DescribeTransactionsResponseData().setTransactionStates(
                singletonList(new DescribeTransactionsResponseData.TransactionState()
                    .setTransactionalId(transactionalId)
                    .setErrorCode(Errors.NOT_COORDINATOR.code())))
        ));

        // Send second `FindCoordinator` request
        assertFalse(futureFor(driver, transactionalId).isDone());
        List<RequestSpec<CoordinatorKey>> lookupRequests2 = driver.poll();
        assertEquals(1, lookupRequests2.size());
        RequestSpec<CoordinatorKey> lookupSpec2 = lookupRequests2.get(0);
        assertRetryBackoff(1, lookupSpec2);
        assertFindCoordinatorRequest(transactionalId, lookupSpec2);
    }

    @Test
    public void testShouldFailTransactionalIdAfterFatalErrorInDescribeTransactions() {
        String transactionalId = "foo";

        DescribeTransactionsRequestDriver driver = new DescribeTransactionsRequestDriver(
            singleton(transactionalId),
            deadlineMs,
            retryBackoffMs
        );

        // Send first `FindCoordinator` request
        List<RequestSpec<CoordinatorKey>> lookupRequests1 = driver.poll();
        assertEquals(1, lookupRequests1.size());
        RequestSpec<CoordinatorKey> lookupSpec1 = lookupRequests1.get(0);
        assertRetryBackoff(0, lookupSpec1);
        assertFindCoordinatorRequest(transactionalId, lookupSpec1);

        int coordinator = 5;
        driver.onResponse(time.milliseconds(), lookupSpec1,
            findCoordinatorResponse(OptionalInt.of(coordinator)));

        // Send `DescribeTransactions` request
        List<RequestSpec<CoordinatorKey>> requests = driver.poll();
        assertEquals(1, requests.size());
        RequestSpec<CoordinatorKey> requestSpec = requests.get(0);
        assertDescribeTransactionsRequest(singleton(transactionalId), coordinator, requestSpec);

        driver.onResponse(time.milliseconds(), requestSpec, new DescribeTransactionsResponse(
            new DescribeTransactionsResponseData().setTransactionStates(
                singletonList(new DescribeTransactionsResponseData.TransactionState()
                    .setTransactionalId(transactionalId)
                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())))
        ));

        KafkaFutureImpl<TransactionDescription> future = futureFor(driver, transactionalId);
        assertTrue(future.isDone());
        assertFutureThrows(future, UnknownServerException.class);
    }

    private KafkaFutureImpl<TransactionDescription> futureFor(
        DescribeTransactionsRequestDriver driver,
        String transactionalId
    ) {
        CoordinatorKey key = DescribeTransactionsRequestDriver.asCoordinatorKey(transactionalId);
        return driver.futures().get(key);
    }

    private RequestSpec<CoordinatorKey> findRequestWithKey(
        String transactionalId,
        List<RequestSpec<CoordinatorKey>> requests
    ) {
        CoordinatorKey key = DescribeTransactionsRequestDriver.asCoordinatorKey(transactionalId);

        Optional<RequestSpec<CoordinatorKey>> firstMatch = requests.stream()
            .filter(spec -> spec.keys.contains(key))
            .findFirst();

        assertTrue(firstMatch.isPresent());

        return firstMatch.get();
    }

    private void assertDescribeTransactionsRequest(
        Set<String> expectedTransactionalIds,
        int expectedCoordinatorId,
        RequestSpec<CoordinatorKey> spec
    ) {
        Set<CoordinatorKey> keys = expectedTransactionalIds.stream()
            .map(DescribeTransactionsRequestDriver::asCoordinatorKey)
            .collect(Collectors.toSet());
        assertEquals(keys, spec.keys);
        assertEquals(OptionalInt.of(expectedCoordinatorId), spec.scope.destinationBrokerId());

        assertTrue(spec.request instanceof DescribeTransactionsRequest.Builder);
        DescribeTransactionsRequest.Builder request = (DescribeTransactionsRequest.Builder) spec.request;
        assertEquals(expectedTransactionalIds, new HashSet<>(request.data.transactionalIds()));
    }

    private void assertFindCoordinatorRequest(
        String expectedTransactionalId,
        RequestSpec<CoordinatorKey> spec
    ) {
        CoordinatorKey key = DescribeTransactionsRequestDriver.asCoordinatorKey(expectedTransactionalId);
        assertEquals(singleton(key), spec.keys);
        assertEquals(OptionalInt.empty(), spec.scope.destinationBrokerId());

        assertTrue(spec.request instanceof FindCoordinatorRequest.Builder);
        FindCoordinatorRequest.Builder request1 = (FindCoordinatorRequest.Builder) spec.request;
        assertEquals(expectedTransactionalId, request1.data().key());
        assertEquals(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id(), request1.data().keyType());
    }

    private void assertRetryBackoff(
        int expectedTries,
        RequestSpec<CoordinatorKey> spec
    ) {
        assertEquals(deadlineMs, spec.deadlineMs);
        assertEquals(expectedTries, spec.tries);
        if (expectedTries == 0) {
            assertEquals(0, spec.nextAllowedTryMs);
        } else {
            assertEquals(time.milliseconds() + (expectedTries * retryBackoffMs), spec.nextAllowedTryMs);
        }
    }

    private FindCoordinatorResponse findCoordinatorResponse(
        OptionalInt coordinatorId
    ) {
        return new FindCoordinatorResponse(
            new FindCoordinatorResponseData()
                .setErrorCode(Errors.NONE.code())
                .setNodeId(coordinatorId.orElse(-1))
                .setHost("localhost")
                .setPort(9092 + coordinatorId.orElse(-1))
        );
    }

    private DescribeTransactionsResponseData.TransactionState sampleTransactionState1(
        String transactionalId
    ) {
        return new DescribeTransactionsResponseData.TransactionState()
            .setErrorCode(Errors.NONE.code())
            .setTransactionState("Ongoing")
            .setTransactionalId(transactionalId)
            .setProducerId(12345L)
            .setProducerEpoch(15)
            .setTransactionStartTimeMs(1599151791L)
            .setTransactionTimeoutMs(10000)
            .setTopicPartitions(asList(
                new DescribeTransactionsResponseData.TopicData()
                    .setName("foo")
                    .setPartitionIndexes(asList(1, 3, 5)),
                new DescribeTransactionsResponseData.TopicData()
                    .setName("bar")
                    .setPartitionIndexes(asList(1, 3, 5))
            ));
    }

    private DescribeTransactionsResponseData.TransactionState sampleTransactionState2(
        String transactionalId
    ) {
        return new DescribeTransactionsResponseData.TransactionState()
            .setErrorCode(Errors.NONE.code())
            .setTransactionState("Empty")
            .setTransactionalId(transactionalId)
            .setProducerId(98765L)
            .setProducerEpoch(30)
            .setTransactionStartTimeMs(-1);
    }

    private void assertMatchingTransactionState(
        int expectedCoordinatorId,
        DescribeTransactionsResponseData.TransactionState expected,
        TransactionDescription actual
    ) {
        assertEquals(expectedCoordinatorId, actual.coordinatorId());
        assertEquals(expected.producerId(), actual.producerId());
        assertEquals(expected.producerEpoch(), actual.producerEpoch());
        assertEquals(expected.transactionTimeoutMs(), actual.transactionTimeoutMs());
        assertEquals(expected.transactionStartTimeMs(), actual.transactionStartTimeMs().orElse(-1));
        assertEquals(collectTransactionPartitions(expected), actual.topicPartitions());
    }

    private Set<TopicPartition> collectTransactionPartitions(
        DescribeTransactionsResponseData.TransactionState transactionState
    ) {
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (DescribeTransactionsResponseData.TopicData topicData : transactionState.topicPartitions()) {
            for (Integer partitionId : topicData.partitionIndexes()) {
                topicPartitions.add(new TopicPartition(topicData.name(), partitionId));
            }
        }
        return topicPartitions;
    }

}
