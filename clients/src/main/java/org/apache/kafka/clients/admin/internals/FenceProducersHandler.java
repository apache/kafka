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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FenceProducersHandler extends AdminApiHandler.Unbatched<CoordinatorKey, ProducerIdAndEpoch> {
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public FenceProducersHandler(
        LogContext logContext
    ) {
        this.log = logContext.logger(FenceProducersHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(FindCoordinatorRequest.CoordinatorType.TRANSACTION, logContext);
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, ProducerIdAndEpoch> newFuture(
            Collection<String> transactionalIds
    ) {
        return AdminApiFuture.forKeys(buildKeySet(transactionalIds));
    }

    private static Set<CoordinatorKey> buildKeySet(Collection<String> transactionalIds) {
        return transactionalIds.stream()
                .map(CoordinatorKey::byTransactionalId)
                .collect(Collectors.toSet());
    }

    @Override
    public String apiName() {
        return "fenceProducer";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    @Override
    InitProducerIdRequest.Builder buildSingleRequest(int brokerId, CoordinatorKey key) {
        if (key.type != FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
            throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `InitProducerId` request");
        }
        InitProducerIdRequestData data = new InitProducerIdRequestData()
                // Because we never include a producer epoch or ID in this request, we expect that some errors
                // (such as PRODUCER_FENCED) will never be returned in the corresponding broker response.
                // If we ever modify this logic to include an epoch or producer ID, we will need to update the
                // error handling logic for this handler to accommodate these new errors.
                .setProducerEpoch(ProducerIdAndEpoch.NONE.epoch)
                .setProducerId(ProducerIdAndEpoch.NONE.producerId)
                .setTransactionalId(key.idValue)
                // Set transaction timeout to 1 since it's only being initialized to fence out older producers with the same transactional ID,
                // and shouldn't be used for any actual record writes
                .setTransactionTimeoutMs(1);
        return new InitProducerIdRequest.Builder(data);
    }

    @Override
    public ApiResult<CoordinatorKey, ProducerIdAndEpoch> handleSingleResponse(
            Node broker,
            CoordinatorKey key,
            AbstractResponse abstractResponse
    ) {
        InitProducerIdResponse response = (InitProducerIdResponse) abstractResponse;

        Errors error = Errors.forCode(response.data().errorCode());
        if (error != Errors.NONE) {
            return handleError(key, error);
        }

        Map<CoordinatorKey, ProducerIdAndEpoch> completed = Collections.singletonMap(key, new ProducerIdAndEpoch(
                response.data().producerId(),
                response.data().producerEpoch()
        ));

        return new ApiResult<>(completed, Collections.emptyMap(), Collections.emptyList());
    }

    private ApiResult<CoordinatorKey, ProducerIdAndEpoch> handleError(CoordinatorKey transactionalIdKey, Errors error) {
        switch (error) {
            case CLUSTER_AUTHORIZATION_FAILED:
                return ApiResult.failed(transactionalIdKey, new ClusterAuthorizationException(
                        "InitProducerId request for transactionalId `" + transactionalIdKey.idValue + "` " +
                                "failed due to cluster authorization failure"));

            case TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                return ApiResult.failed(transactionalIdKey, new TransactionalIdAuthorizationException(
                        "InitProducerId request for transactionalId `" + transactionalIdKey.idValue + "` " +
                                "failed due to transactional ID authorization failure"));

            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("InitProducerId request for transactionalId `{}` failed because the " +
                                "coordinator is still in the process of loading state. Will retry",
                        transactionalIdKey.idValue);
                return ApiResult.empty();

            case NOT_COORDINATOR:
            case COORDINATOR_NOT_AVAILABLE:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("InitProducerId request for transactionalId `{}` returned error {}. Will attempt " +
                        "to find the coordinator again and retry", transactionalIdKey.idValue, error);
                return ApiResult.unmapped(Collections.singletonList(transactionalIdKey));

            // We intentionally omit cases for PRODUCER_FENCED, TRANSACTIONAL_ID_NOT_FOUND, and INVALID_PRODUCER_EPOCH
            // since those errors should never happen when our InitProducerIdRequest doesn't include a producer epoch or ID
            // and should therefore fall under the "unexpected error" catch-all case below

            default:
                return ApiResult.failed(transactionalIdKey, error.exception("InitProducerId request for " +
                        "transactionalId `" + transactionalIdKey.idValue + "` failed due to unexpected error"));
        }
    }
}
