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
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdNotFoundException;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.DescribeTransactionsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public class DescribeTransactionsDriver extends CoordinatorApiDriver<TransactionDescription> {
    private final Logger log;

    public DescribeTransactionsDriver(
        Collection<String> transactionalIds,
        long deadlineMs,
        long retryBackoffMs,
        LogContext logContext
    ) {
        super(buildKeySet(transactionalIds), deadlineMs, retryBackoffMs, logContext);
        this.log = logContext.logger(DescribeTransactionsDriver.class);
    }

    private static Set<CoordinatorKey> buildKeySet(Collection<String> transactionalIds) {
        return transactionalIds.stream()
            .map(DescribeTransactionsDriver::asCoordinatorKey)
            .collect(Collectors.toSet());
    }

    @Override
    String apiName() {
        return "describeTransactions";
    }

    @Override
    AbstractRequest.Builder<?> buildFulfillmentRequest(Integer brokerId, Set<CoordinatorKey> keys) {
        DescribeTransactionsRequestData request = new DescribeTransactionsRequestData();
        List<String> transactionalIds = keys.stream().map(key -> key.idValue).collect(Collectors.toList());
        request.setTransactionalIds(transactionalIds);
        return new DescribeTransactionsRequest.Builder(request);
    }

    @Override
    void handleFulfillmentResponse(Integer brokerId, Set<CoordinatorKey> keys, AbstractResponse abstractResponse) {
        DescribeTransactionsResponse response = (DescribeTransactionsResponse) abstractResponse;
        for (DescribeTransactionsResponseData.TransactionState transactionState : response.data().transactionStates()) {
            CoordinatorKey transactionalIdKey = asCoordinatorKey(transactionState.transactionalId());
            Errors error = Errors.forCode(transactionState.errorCode());

            if (error != Errors.NONE) {
                handleError(transactionalIdKey, error);
                continue;
            }

            OptionalLong transactionStartTimeMs = transactionState.transactionStartTimeMs() < 0 ?
                OptionalLong.empty() :
                OptionalLong.of(transactionState.transactionStartTimeMs());

            complete(transactionalIdKey, new TransactionDescription(
                brokerId,
                TransactionState.parse(transactionState.transactionState()),
                transactionState.producerId(),
                transactionState.producerEpoch(),
                transactionState.transactionTimeoutMs(),
                transactionStartTimeMs,
                collectTopicPartitions(transactionState)
            ));
        }
    }

    private Set<TopicPartition> collectTopicPartitions(
        DescribeTransactionsResponseData.TransactionState transactionState
    ) {
        Set<TopicPartition> res = new HashSet<>();
        for (DescribeTransactionsResponseData.TopicData topicData : transactionState.topicPartitions()) {
            String topic = topicData.name();
            for (Integer partitionId : topicData.partitionIndexes()) {
                res.add(new TopicPartition(topic, partitionId));
            }
        }
        return res;
    }

    public static CoordinatorKey asCoordinatorKey(String transactionalId) {
        return new CoordinatorKey(transactionalId, FindCoordinatorRequest.CoordinatorType.TRANSACTION);
    }

    private void handleError(CoordinatorKey transactionalIdKey, Errors error) {
        switch (error) {
            case TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                completeExceptionally(transactionalIdKey, new TransactionalIdAuthorizationException(
                    "DescribeTransactions request for transactionalId `" + transactionalIdKey.idValue + "` " +
                        "failed due to authorization failure"));
                break;

            case TRANSACTIONAL_ID_NOT_FOUND:
                completeExceptionally(transactionalIdKey, new TransactionalIdNotFoundException(
                    "DescribeTransactions request for transactionalId `" + transactionalIdKey.idValue + "` " +
                        "failed because the ID could not be found"));
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("Desc>ribeTransactions request for transactionalId `{}` failed because the " +
                        " coordinator is still in the process of loading state. Will retry",
                    transactionalIdKey.idValue);
                break;

            case NOT_COORDINATOR:
            case COORDINATOR_NOT_AVAILABLE:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                super.unmap(transactionalIdKey);
                log.debug("DescribeTransactions request for transactionalId `{}` returned error {}. Will retry",
                    transactionalIdKey.idValue, error);
                break;

            default:
                super.completeExceptionally(transactionalIdKey, error.exception("DescribeTransactions request for " +
                    "transactionalId `" + transactionalIdKey.idValue + "` failed due to unexpected error"));
        }
    }

}
