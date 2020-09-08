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
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.ListTransactionsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ListTransactionsDriver extends AllBrokerApiDriver<Collection<TransactionListing>> {
    private final Logger log;
    private final ListTransactionsOptions options;

    public ListTransactionsDriver(
        ListTransactionsOptions options,
        long deadlineMs,
        long retryBackoffMs,
        LogContext logContext
    ) {
        super(deadlineMs, retryBackoffMs, logContext);
        this.options = options;
        this.log = logContext.logger(ListTransactionsDriver.class);
    }

    @Override
    String apiName() {
        return "listTransactions";
    }

    @Override
    AbstractRequest.Builder<?> buildFulfillmentRequest(Integer brokerId) {
        ListTransactionsRequestData request = new ListTransactionsRequestData();
        request.setProducerIdFilter(new ArrayList<>(options.filteredProducerIds()));
        request.setStatesFilter(options.filteredStates().stream()
            .map(TransactionState::toString)
            .collect(Collectors.toList()));
        return new ListTransactionsRequest.Builder(request);
    }

    @Override
    void handleFulfillmentResponse(Integer brokerId, AbstractResponse abstractResponse) {
        ListTransactionsResponse response = (ListTransactionsResponse) abstractResponse;
        Errors error = Errors.forCode(response.data().errorCode());

        if (error != Errors.NONE) {
            handleError(brokerId, error);
            return;
        }

        List<TransactionListing> listings = response.data().transactionStates().stream()
            .map(transactionState -> new TransactionListing(
                transactionState.transactionalId(),
                transactionState.producerId(),
                TransactionState.parse(transactionState.transactionState())))
            .collect(Collectors.toList());

        super.complete(brokerId, listings);
    }

    private void handleError(int brokerId, Errors error) {
        switch (error) {
            case COORDINATOR_LOAD_IN_PROGRESS:
                log.debug("ListTransactions request sent to broker {} failed because the " +
                    "coordinator is still loading state. Will try again after backing off", brokerId);
                break;

            case COORDINATOR_NOT_AVAILABLE:
                super.completeExceptionally(brokerId, new CoordinatorNotAvailableException("ListTransactions " +
                    "request sent to broker " + brokerId + " failed because the coordinator is shutting down"));
                break;

            default:
                super.completeExceptionally(brokerId, error.exception("ListTransactions request " +
                    "sent to broker " + brokerId + " failed with an unexpected exception"));
        }
    }

}
