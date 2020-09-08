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

import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.admin.internals.AllBrokerRequestDriver.BrokerKey;
import org.apache.kafka.clients.admin.internals.RequestDriver.RequestSpec;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.ListTransactionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ListTransactionsRequestDriverTest {
    private final MockTime time = new MockTime();
    private final long deadlineMs = time.milliseconds() + 10000;
    private final long retryBackoffMs = 100;

    @Test
    public void testFailedMetadataRequest() {
        ListTransactionsOptions options = new ListTransactionsOptions();
        ListTransactionsRequestDriver driver = new ListTransactionsRequestDriver(options, deadlineMs, retryBackoffMs);

        KafkaFutureImpl<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> lookupFuture =
            driver.lookupFuture();

        RequestSpec<BrokerKey> lookupRequestSpec = assertLookupRequest(driver);
        driver.onFailure(time.milliseconds(), lookupRequestSpec, new TimeoutException());
        assertFutureThrows(lookupFuture, TimeoutException.class);
    }

    @Test
    public void testMultiBrokerListTransactions() throws Exception {
        ListTransactionsOptions options = new ListTransactionsOptions();
        ListTransactionsRequestDriver driver = new ListTransactionsRequestDriver(options, deadlineMs, retryBackoffMs);

        Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>> brokerFutures =
            assertMetadataLookup(driver, mkSet(0, 1));
        assertEquals(mkSet(0, 1), brokerFutures.keySet());
        assertTrue(brokerFutures.values().stream().noneMatch(KafkaFutureImpl::isDone));

        // Send `ListTransactions` requests
        List<RequestSpec<BrokerKey>> requestSpecs = driver.poll();
        assertEquals(2, requestSpecs.size());
        RequestSpec<BrokerKey> requestBroker0 = findBrokerRequest(requestSpecs, 0);
        assertListTransactionsRequest(options, requestBroker0);

        RequestSpec<BrokerKey> requestBroker1 = findBrokerRequest(requestSpecs, 1);
        assertListTransactionsRequest(options, requestBroker1);

        // Receive `ListTransactions` responses
        KafkaFutureImpl<Collection<TransactionListing>> broker0Future = brokerFutures.get(0);
        ListTransactionsResponse broker0Response = sampleListTransactionsResponse1();
        driver.onResponse(time.milliseconds(), requestBroker0, broker0Response);
        assertTrue(broker0Future.isDone());
        assertExpectedTransactions(broker0Response.data().transactionStates(), broker0Future.get());

        KafkaFutureImpl<Collection<TransactionListing>> broker1Future = brokerFutures.get(1);
        ListTransactionsResponse broker1Response = sampleListTransactionsResponse2();
        driver.onResponse(time.milliseconds(), requestBroker1, broker1Response);
        assertTrue(broker1Future.isDone());
        assertExpectedTransactions(broker1Response.data().transactionStates(), broker1Future.get());
    }

    @Test
    public void testRetryListTransactionsAfterCoordinatorLoading() throws Exception {
        ListTransactionsOptions options = new ListTransactionsOptions();
        ListTransactionsRequestDriver driver = new ListTransactionsRequestDriver(options, deadlineMs, retryBackoffMs);

        int brokerId = 0;
        Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>> brokerFutures =
            assertMetadataLookup(driver, singleton(brokerId));
        assertEquals(singleton(brokerId), brokerFutures.keySet());
        KafkaFutureImpl<Collection<TransactionListing>> brokerFuture = brokerFutures.get(brokerId);
        assertFalse(brokerFuture.isDone());

        // Send `ListTransactions` once and receive loading error
        ListTransactionsResponse errorResponse = listTransactionsResponseWithError(
            Errors.COORDINATOR_LOAD_IN_PROGRESS);
        assertListTransactions(driver, options, errorResponse, brokerId);
        assertFalse(brokerFuture.isDone());

        // Now we expect `ListTransactions` to be retried
        ListTransactionsResponse response = sampleListTransactionsResponse1();
        assertListTransactions(driver, options, response, brokerId);
        assertTrue(brokerFuture.isDone());
        assertExpectedTransactions(response.data().transactionStates(), brokerFuture.get());
    }

    @Test
    public void testFatalListTransactionsError() throws Exception {
        ListTransactionsOptions options = new ListTransactionsOptions();
        ListTransactionsRequestDriver driver = new ListTransactionsRequestDriver(options, deadlineMs, retryBackoffMs);

        int brokerId = 0;
        Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>> brokerFutures =
            assertMetadataLookup(driver, singleton(brokerId));
        assertEquals(singleton(brokerId), brokerFutures.keySet());
        KafkaFutureImpl<Collection<TransactionListing>> brokerFuture = brokerFutures.get(brokerId);
        assertFalse(brokerFuture.isDone());

        ListTransactionsResponse errorResponse = listTransactionsResponseWithError(Errors.UNKNOWN_SERVER_ERROR);
        assertListTransactions(driver, options, errorResponse, brokerId);
        assertTrue(brokerFuture.isDone());
        assertFutureThrows(brokerFuture, UnknownServerException.class);
    }

    private void assertListTransactions(
        ListTransactionsRequestDriver driver,
        ListTransactionsOptions options,
        ListTransactionsResponse response,
        int brokerId
    ) {
        List<RequestSpec<BrokerKey>> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());
        RequestSpec<BrokerKey> request = findBrokerRequest(requestSpecs, brokerId);
        assertListTransactionsRequest(options, request);
        driver.onResponse(time.milliseconds(), request, response);
    }

    private Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>> assertMetadataLookup(
        ListTransactionsRequestDriver driver,
        Set<Integer> brokers
    ) throws Exception {
        KafkaFutureImpl<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> lookupFuture =
            driver.lookupFuture();

        // Send `Metadata` request to find brokerIds
        RequestSpec<BrokerKey> lookupRequestSpec = assertLookupRequest(driver);

        // Receive `Metadata` response
        MetadataResponse metadataResponse = metadataResponse(brokers.stream()
            .map(this::brokerMetadata)
            .collect(Collectors.toList())
        );

        driver.onResponse(time.milliseconds(), lookupRequestSpec, metadataResponse);
        assertTrue(lookupFuture.isDone());

        return lookupFuture.get();
    }

    private ListTransactionsResponse listTransactionsResponseWithError(Errors error) {
        return new ListTransactionsResponse(new ListTransactionsResponseData().setErrorCode(error.code()));
    }

    private ListTransactionsResponse sampleListTransactionsResponse1() {
        return new ListTransactionsResponse(
            new ListTransactionsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setTransactionStates(asList(
                    new ListTransactionsResponseData.TransactionState()
                        .setTransactionalId("foo")
                        .setProducerId(12345L)
                        .setTransactionState("Ongoing"),
                    new ListTransactionsResponseData.TransactionState()
                        .setTransactionalId("bar")
                        .setProducerId(98765L)
                        .setTransactionState("PrepareAbort")
            ))
        );
    }

    private ListTransactionsResponse sampleListTransactionsResponse2() {
        return new ListTransactionsResponse(
            new ListTransactionsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setTransactionStates(singletonList(
                    new ListTransactionsResponseData.TransactionState()
                        .setTransactionalId("baz")
                        .setProducerId(13579L)
                        .setTransactionState("CompleteCommit")
            ))
        );
    }

    private void assertExpectedTransactions(
        List<ListTransactionsResponseData.TransactionState> expected,
        Collection<TransactionListing> actual
    ) {
        assertEquals(expected.size(), actual.size());

        Map<String, ListTransactionsResponseData.TransactionState> expectedMap = expected.stream().collect(Collectors.toMap(
            ListTransactionsResponseData.TransactionState::transactionalId,
            Function.identity()
        ));

        for (TransactionListing actualListing : actual) {
            ListTransactionsResponseData.TransactionState expectedState =
                expectedMap.get(actualListing.transactionalId());
            assertNotNull(expectedState);
            assertExpectedTransactionState(expectedState, actualListing);
        }
    }

    private void assertExpectedTransactionState(
        ListTransactionsResponseData.TransactionState expected,
        TransactionListing actual
    ) {
        assertEquals(expected.transactionalId(), actual.transactionalId());
        assertEquals(expected.producerId(), actual.producerId());
        assertEquals(expected.transactionState(), actual.state().toString());
    }

    private void assertListTransactionsRequest(
        ListTransactionsOptions options,
        RequestSpec<BrokerKey> spec
    ) {
        assertTrue(spec.request instanceof ListTransactionsRequest.Builder);
        ListTransactionsRequest.Builder request = (ListTransactionsRequest.Builder) spec.request;

        assertEquals(options.filteredProducerIds(), new HashSet<>(request.data.producerIdFilter()));

        Set<String> expectedFilteredStates = options.filteredStates().stream()
            .map(TransactionState::toString)
            .collect(Collectors.toSet());
        assertEquals(expectedFilteredStates, new HashSet<>(request.data.statesFilter()));
    }

    private RequestSpec<BrokerKey> findBrokerRequest(
        List<RequestSpec<BrokerKey>> requests,
        Integer brokerId
    ) {
        Optional<RequestSpec<BrokerKey>> requestOpt = requests.stream()
            .filter(spec -> spec.scope.destinationBrokerId().isPresent()
                && spec.scope.destinationBrokerId().getAsInt() == brokerId)
            .findFirst();
        assertTrue(requestOpt.isPresent());
        return requestOpt.get();
    }

    private RequestSpec<BrokerKey> assertLookupRequest(
        ListTransactionsRequestDriver driver
    ) {
        List<RequestSpec<BrokerKey>> requests = driver.poll();
        assertEquals(1, requests.size());

        RequestSpec<BrokerKey> lookupRequestSpec = requests.get(0);
        assertEquals(OptionalInt.empty(), lookupRequestSpec.scope.destinationBrokerId());
        assertTrue(lookupRequestSpec.request instanceof MetadataRequest.Builder);

        MetadataRequest.Builder metadataRequest = (MetadataRequest.Builder) lookupRequestSpec.request;
        assertFalse(metadataRequest.isAllTopics());
        assertEquals(Collections.emptyList(), metadataRequest.data.topics());
        return lookupRequestSpec;
    }

    private MetadataResponse metadataResponse(List<MetadataResponseData.MetadataResponseBroker> brokers) {
        MetadataResponseData response = new MetadataResponseData();
        brokers.forEach(response.brokers()::add);
        return new MetadataResponse(response);
    }

    private MetadataResponseData.MetadataResponseBroker brokerMetadata(int brokerId) {
        return new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(brokerId)
            .setHost("localhost")
            .setPort(9092 + brokerId);
    }

}