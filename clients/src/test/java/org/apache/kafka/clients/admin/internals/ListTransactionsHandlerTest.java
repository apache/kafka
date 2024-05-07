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
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult;
import org.apache.kafka.clients.admin.internals.AllBrokersStrategy.BrokerKey;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.ListTransactionsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ListTransactionsHandlerTest {
    private final LogContext logContext = new LogContext();
    private final Node node = new Node(1, "host", 1234);

    @Test
    public void testBuildRequestWithoutFilters() {
        int brokerId = 1;
        BrokerKey brokerKey = new BrokerKey(OptionalInt.of(brokerId));
        ListTransactionsOptions options = new ListTransactionsOptions();
        ListTransactionsHandler handler = new ListTransactionsHandler(options, logContext);
        ListTransactionsRequest request = handler.buildBatchedRequest(brokerId, singleton(brokerKey)).build();
        assertEquals(Collections.emptyList(), request.data().producerIdFilters());
        assertEquals(Collections.emptyList(), request.data().stateFilters());
    }

    @Test
    public void testBuildRequestWithFilteredProducerId() {
        int brokerId = 1;
        BrokerKey brokerKey = new BrokerKey(OptionalInt.of(brokerId));
        long filteredProducerId = 23423L;
        ListTransactionsOptions options = new ListTransactionsOptions()
            .filterProducerIds(singleton(filteredProducerId));
        ListTransactionsHandler handler = new ListTransactionsHandler(options, logContext);
        ListTransactionsRequest request = handler.buildBatchedRequest(brokerId, singleton(brokerKey)).build();
        assertEquals(Collections.singletonList(filteredProducerId), request.data().producerIdFilters());
        assertEquals(Collections.emptyList(), request.data().stateFilters());
    }

    @Test
    public void testBuildRequestWithFilteredState() {
        int brokerId = 1;
        BrokerKey brokerKey = new BrokerKey(OptionalInt.of(brokerId));
        TransactionState filteredState = TransactionState.ONGOING;
        ListTransactionsOptions options = new ListTransactionsOptions()
            .filterStates(singleton(filteredState));
        ListTransactionsHandler handler = new ListTransactionsHandler(options, logContext);
        ListTransactionsRequest request = handler.buildBatchedRequest(brokerId, singleton(brokerKey)).build();
        assertEquals(Collections.singletonList(filteredState.toString()), request.data().stateFilters());
        assertEquals(Collections.emptyList(), request.data().producerIdFilters());
    }

    @Test
    public void testBuildRequestWithDurationFilter() {
        int brokerId = 1;
        BrokerKey brokerKey = new BrokerKey(OptionalInt.of(brokerId));
        ListTransactionsOptions options = new ListTransactionsOptions();
        ListTransactionsHandler handler = new ListTransactionsHandler(options, logContext);
        // case 1: check the default value (-1L) for durationFilter
        ListTransactionsRequest request = handler.buildBatchedRequest(brokerId, singleton(brokerKey)).build((short) 1);
        assertEquals(-1L, request.data().durationFilter());
        request = handler.buildBatchedRequest(brokerId, singleton(brokerKey)).build((short) 0);
        assertEquals(-1L, request.data().durationFilter());
        // case 2: able to set a valid duration filter when using API version 1
        options.filterOnDuration(10L);
        request = handler.buildBatchedRequest(brokerId, singleton(brokerKey)).build((short) 1);
        assertEquals(10L, request.data().durationFilter());
        assertEquals(Collections.emptyList(), request.data().producerIdFilters());
        // case 3: unable to set a valid duration filter when using API version 0
        assertThrows(UnsupportedVersionException.class, () -> handler.buildBatchedRequest(brokerId, singleton(brokerKey)).build((short) 0));
        // case 4: able to set duration filter to -1L when using API version 0
        options.filterOnDuration(-1L);
        ListTransactionsRequest request1 = handler.buildBatchedRequest(brokerId, singleton(brokerKey)).build((short) 0);
        assertEquals(-1L, request1.data().durationFilter());
    }

    @Test
    public void testHandleSuccessfulResponse() {
        int brokerId = 1;
        BrokerKey brokerKey = new BrokerKey(OptionalInt.of(brokerId));
        ListTransactionsOptions options = new ListTransactionsOptions();
        ListTransactionsHandler handler = new ListTransactionsHandler(options, logContext);
        ListTransactionsResponse response = sampleListTransactionsResponse1();
        ApiResult<BrokerKey, Collection<TransactionListing>> result = handler.handleResponse(
            node, singleton(brokerKey), response);
        assertEquals(singleton(brokerKey), result.completedKeys.keySet());
        assertExpectedTransactions(response.data().transactionStates(), result.completedKeys.get(brokerKey));
    }

    @Test
    public void testCoordinatorLoadingErrorIsRetriable() {
        int brokerId = 1;
        ApiResult<BrokerKey, Collection<TransactionListing>> result =
            handleResponseWithError(brokerId, Errors.COORDINATOR_LOAD_IN_PROGRESS);
        assertEquals(Collections.emptyMap(), result.completedKeys);
        assertEquals(Collections.emptyMap(), result.failedKeys);
        assertEquals(Collections.emptyList(), result.unmappedKeys);
    }

    @Test
    public void testHandleResponseWithFatalErrors() {
        assertFatalError(Errors.COORDINATOR_NOT_AVAILABLE);
        assertFatalError(Errors.UNKNOWN_SERVER_ERROR);
    }

    private void assertFatalError(
        Errors error
    ) {
        int brokerId = 1;
        BrokerKey brokerKey = new BrokerKey(OptionalInt.of(brokerId));
        ApiResult<BrokerKey, Collection<TransactionListing>> result = handleResponseWithError(brokerId, error);
        assertEquals(Collections.emptyMap(), result.completedKeys);
        assertEquals(Collections.emptyList(), result.unmappedKeys);
        assertEquals(Collections.singleton(brokerKey), result.failedKeys.keySet());
        Throwable throwable = result.failedKeys.get(brokerKey);
        assertEquals(error, Errors.forException(throwable));
    }

    private ApiResult<BrokerKey, Collection<TransactionListing>> handleResponseWithError(
        int brokerId,
        Errors error
    ) {
        BrokerKey brokerKey = new BrokerKey(OptionalInt.of(brokerId));
        ListTransactionsOptions options = new ListTransactionsOptions();
        ListTransactionsHandler handler = new ListTransactionsHandler(options, logContext);

        ListTransactionsResponse response = new ListTransactionsResponse(
            new ListTransactionsResponseData().setErrorCode(error.code())
        );
        return handler.handleResponse(node, singleton(brokerKey), response);
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

}
