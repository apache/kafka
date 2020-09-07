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
    public void testSuccessfulListTransactions() throws Exception {
        ListTransactionsOptions options = new ListTransactionsOptions();
        ListTransactionsRequestDriver driver = new ListTransactionsRequestDriver(options, deadlineMs, retryBackoffMs);

        KafkaFutureImpl<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> lookupFuture =
            driver.lookupFuture();

        // Send `Metadata` request to find brokerIds
        RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec lookupRequestSpec =
            assertLookupRequest(driver);

        // Receive `Metadata` response
        driver.onResponse(time.milliseconds(), lookupRequestSpec, metadataResponse(asList(
            brokerMetadata(0),
            brokerMetadata(1)
        )));

        assertTrue(lookupFuture.isDone());

        Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>> brokerFutures = lookupFuture.get();
        assertEquals(mkSet(0, 1), brokerFutures.keySet());
        assertTrue(brokerFutures.values().stream().noneMatch(KafkaFutureImpl::isDone));

        // Send `ListTransactions` requests
        List<RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec> requestSpecs = driver.poll();
        assertEquals(2, requestSpecs.size());
        RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec requestBroker0 =
            findBrokerRequest(requestSpecs, 0);
        assertListTransactionsRequest(options, requestBroker0);

        RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec requestBroker1 =
            findBrokerRequest(requestSpecs, 1);
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

        KafkaFutureImpl<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> lookupFuture =
            driver.lookupFuture();

        // Send `Metadata` request to find brokerIds
        RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec lookupRequestSpec =
            assertLookupRequest(driver);

        // Receive `Metadata` response
        driver.onResponse(time.milliseconds(), lookupRequestSpec, metadataResponse(singletonList(
            brokerMetadata(0)
        )));

        assertTrue(lookupFuture.isDone());

        Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>> brokerFutures = lookupFuture.get();
        assertEquals(mkSet(0), brokerFutures.keySet());
        KafkaFutureImpl<Collection<TransactionListing>> brokerFuture = brokerFutures.get(0);
        assertFalse(brokerFuture.isDone());

        // Send `ListTransactions` requests
        List<RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());
        RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec request =
            findBrokerRequest(requestSpecs, 0);
        assertListTransactionsRequest(options, request);

        // Receive `ListTransactions` responses
        ListTransactionsResponse broker0Response = new ListTransactionsResponse(
            new ListTransactionsResponseData().setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code()));
        driver.onResponse(time.milliseconds(), request, broker0Response);
        assertFalse(brokerFuture.isDone());

        // Now we expect `ListTransactions` to be retried
        List<RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec> retrySpecs = driver.poll();
        assertEquals(1, retrySpecs.size());
        RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec retryRequest =
            findBrokerRequest(requestSpecs, 0);
        assertListTransactionsRequest(options, retryRequest);

        driver.onResponse(time.milliseconds(), retryRequest, sampleListTransactionsResponse1());
        assertTrue(brokerFuture.isDone());
    }

    @Test
    public void testFatalListTransactionsError() throws Exception {
        ListTransactionsOptions options = new ListTransactionsOptions();
        ListTransactionsRequestDriver driver = new ListTransactionsRequestDriver(options, deadlineMs, retryBackoffMs);

        KafkaFutureImpl<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> lookupFuture =
            driver.lookupFuture();

        // Send `Metadata` request to find brokerIds
        RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec lookupRequestSpec =
            assertLookupRequest(driver);

        // Receive `Metadata` response
        driver.onResponse(time.milliseconds(), lookupRequestSpec, metadataResponse(singletonList(
            brokerMetadata(0)
        )));

        assertTrue(lookupFuture.isDone());

        Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>> brokerFutures = lookupFuture.get();
        assertEquals(mkSet(0), brokerFutures.keySet());
        KafkaFutureImpl<Collection<TransactionListing>> brokerFuture = brokerFutures.get(0);
        assertFalse(brokerFuture.isDone());

        // Send `ListTransactions` requests
        List<RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());
        RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec request =
            findBrokerRequest(requestSpecs, 0);
        assertListTransactionsRequest(options, request);

        // Receive `ListTransactions` responses with an unexpected error
        ListTransactionsResponse broker0Response = new ListTransactionsResponse(
            new ListTransactionsResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code()));
        driver.onResponse(time.milliseconds(), request, broker0Response);
        assertTrue(brokerFuture.isDone());
        assertFutureThrows(brokerFuture, UnknownServerException.class);
    }

    private ListTransactionsResponse sampleListTransactionsResponse1() {
        return new ListTransactionsResponse(
            new ListTransactionsResponseData().setTransactionStates(asList(
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
            new ListTransactionsResponseData().setTransactionStates(singletonList(
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
        assertEquals(expected.transactionState(), actual.transactionState().toString());
    }

    private void assertListTransactionsRequest(
        ListTransactionsOptions options,
        RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec spec
    ) {
        assertTrue(spec.request instanceof ListTransactionsRequest.Builder);
        ListTransactionsRequest.Builder request = (ListTransactionsRequest.Builder) spec.request;

        assertEquals(options.filteredProducerIds(), new HashSet<>(request.data.producerIdFilter()));

        Set<String> expectedFilteredStates = options.filteredStates().stream()
            .map(TransactionState::toString)
            .collect(Collectors.toSet());
        assertEquals(expectedFilteredStates, new HashSet<>(request.data.statesFilter()));
    }

    private RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec findBrokerRequest(
        List<RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec> requests,
        Integer brokerId
    ) {
        Optional<RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec> requestOpt = requests.stream()
            .filter(spec -> spec.scope.destinationBrokerId().isPresent()
                && spec.scope.destinationBrokerId().getAsInt() == brokerId)
            .findFirst();
        assertTrue(requestOpt.isPresent());
        return requestOpt.get();
    }

    private RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec assertLookupRequest(
        ListTransactionsRequestDriver driver
    ) {
        List<RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec> requests = driver.poll();
        assertEquals(1, requests.size());

        RequestDriver<BrokerKey, Collection<TransactionListing>>.RequestSpec lookupRequestSpec = requests.get(0);
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