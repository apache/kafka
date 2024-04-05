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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.ListTransactionsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ListTransactionsHandler extends AdminApiHandler.Batched<AllBrokersStrategy.BrokerKey, Collection<TransactionListing>> {
    private final Logger log;
    private final ListTransactionsOptions options;
    private final AllBrokersStrategy lookupStrategy;

    public ListTransactionsHandler(
        ListTransactionsOptions options,
        LogContext logContext
    ) {
        this.options = options;
        this.log = logContext.logger(ListTransactionsHandler.class);
        this.lookupStrategy = new AllBrokersStrategy(logContext);
    }

    public static AllBrokersStrategy.AllBrokersFuture<Collection<TransactionListing>> newFuture() {
        return new AllBrokersStrategy.AllBrokersFuture<>();
    }

    @Override
    public String apiName() {
        return "listTransactions";
    }

    @Override
    public AdminApiLookupStrategy<AllBrokersStrategy.BrokerKey> lookupStrategy() {
        return lookupStrategy;
    }

    @Override
    public ListTransactionsRequest.Builder buildBatchedRequest(
        int brokerId,
        Set<AllBrokersStrategy.BrokerKey> keys
    ) {
        ListTransactionsRequestData request = new ListTransactionsRequestData();
        request.setProducerIdFilters(new ArrayList<>(options.filteredProducerIds()));
        request.setStateFilters(options.filteredStates().stream()
            .map(TransactionState::toString)
            .collect(Collectors.toList()));
        request.setDurationFilter(options.filteredDuration());
        return new ListTransactionsRequest.Builder(request);
    }

    @Override
    public ApiResult<AllBrokersStrategy.BrokerKey, Collection<TransactionListing>> handleResponse(
        Node broker,
        Set<AllBrokersStrategy.BrokerKey> keys,
        AbstractResponse abstractResponse
    ) {
        int brokerId = broker.id();
        AllBrokersStrategy.BrokerKey key = requireSingleton(keys, brokerId);

        ListTransactionsResponse response = (ListTransactionsResponse) abstractResponse;
        Errors error = Errors.forCode(response.data().errorCode());

        if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
            log.debug("The `ListTransactions` request sent to broker {} failed because the " +
                "coordinator is still loading state. Will try again after backing off", brokerId);
            return ApiResult.empty();
        } else if (error == Errors.COORDINATOR_NOT_AVAILABLE) {
            log.debug("The `ListTransactions` request sent to broker {} failed because the " +
                "coordinator is shutting down", brokerId);
            return ApiResult.failed(key, new CoordinatorNotAvailableException("ListTransactions " +
                "request sent to broker " + brokerId + " failed because the coordinator is shutting down"));
        } else if (error != Errors.NONE) {
            log.error("The `ListTransactions` request sent to broker {} failed because of an " +
                "unexpected error {}", brokerId, error);
            return ApiResult.failed(key, error.exception("ListTransactions request " +
                "sent to broker " + brokerId + " failed with an unexpected exception"));
        } else {
            List<TransactionListing> listings = response.data().transactionStates().stream()
                .map(transactionState -> new TransactionListing(
                    transactionState.transactionalId(),
                    transactionState.producerId(),
                    TransactionState.parse(transactionState.transactionState())))
                .collect(Collectors.toList());
            return ApiResult.completed(key, listings);
        }
    }

    private AllBrokersStrategy.BrokerKey requireSingleton(
        Set<AllBrokersStrategy.BrokerKey> keys,
        int brokerId
    ) {
        if (keys.size() != 1) {
            throw new IllegalArgumentException("Unexpected key set: " + keys);
        }

        AllBrokersStrategy.BrokerKey key = keys.iterator().next();
        if (!key.brokerId.isPresent() || key.brokerId.getAsInt() != brokerId) {
            throw new IllegalArgumentException("Unexpected broker key: " + key);
        }

        return key;
    }

}
