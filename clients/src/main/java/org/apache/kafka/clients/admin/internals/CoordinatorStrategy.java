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

import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CoordinatorStrategy implements AdminApiLookupStrategy<CoordinatorKey> {

    private static final ApiRequestScope BATCH_REQUEST_SCOPE = new ApiRequestScope() { };

    private final Logger log;
    private final FindCoordinatorRequest.CoordinatorType type;
    private Set<CoordinatorKey> unrepresentableKeys = Collections.emptySet();

    boolean batch = true;

    public CoordinatorStrategy(
        FindCoordinatorRequest.CoordinatorType type,
        LogContext logContext
    ) {
        this.type = type;
        this.log = logContext.logger(CoordinatorStrategy.class);
    }

    @Override
    public ApiRequestScope lookupScope(CoordinatorKey key) {
        if (batch) {
            return BATCH_REQUEST_SCOPE;
        } else {
            // If the `FindCoordinator` API does not support batched lookups, we use a
            // separate lookup context for each coordinator key we need to lookup
            return new LookupRequestScope(key);
        }
    }

    @Override
    public FindCoordinatorRequest.Builder buildRequest(Set<CoordinatorKey> keys) {
        unrepresentableKeys = keys.stream().filter(k -> k == null || !isRepresentableKey(k.idValue)).collect(Collectors.toSet());
        Set<CoordinatorKey> representableKeys = keys.stream().filter(k -> k != null && isRepresentableKey(k.idValue)).collect(Collectors.toSet());
        if (batch) {
            ensureSameType(representableKeys);
            FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                    .setKeyType(type.id())
                    .setCoordinatorKeys(representableKeys.stream().map(k -> k.idValue).collect(Collectors.toList()));
            return new FindCoordinatorRequest.Builder(data);
        } else {
            CoordinatorKey key = requireSingletonAndType(representableKeys);
            return new FindCoordinatorRequest.Builder(
                new FindCoordinatorRequestData()
                    .setKey(key.idValue)
                    .setKeyType(key.type.id())
            );
        }
    }

    @Override
    public LookupResult<CoordinatorKey> handleResponse(
        Set<CoordinatorKey> keys,
        AbstractResponse abstractResponse
    ) {
        Map<CoordinatorKey, Integer> mappedKeys = new HashMap<>();
        Map<CoordinatorKey, Throwable> failedKeys = new HashMap<>();

        for (CoordinatorKey key : unrepresentableKeys) {
            failedKeys.put(key, new InvalidGroupIdException("The given group id '" +
                key.idValue + "' cannot be represented in a request."));
        }

        for (Coordinator coordinator : ((FindCoordinatorResponse) abstractResponse).coordinators()) {
            CoordinatorKey key;
            if (coordinator.key() == null) // old version without batching
                key = requireSingletonAndType(keys);
            else {
                key = (type == CoordinatorType.GROUP)
                        ? CoordinatorKey.byGroupId(coordinator.key())
                        : CoordinatorKey.byTransactionalId(coordinator.key());
            }
            handleError(Errors.forCode(coordinator.errorCode()),
                        key,
                        coordinator.nodeId(),
                        mappedKeys,
                        failedKeys);
        }
        return new LookupResult<>(failedKeys, mappedKeys);
    }

    public void disableBatch() {
        batch = false;
    }

    public boolean batch() {
        return batch;
    }

    private CoordinatorKey requireSingletonAndType(Set<CoordinatorKey> keys) {
        if (keys.size() != 1) {
            throw new IllegalArgumentException("Unexpected size of key set: expected 1, but got " + keys.size());
        }
        CoordinatorKey key = keys.iterator().next();
        if (key.type != type) {
            throw new IllegalArgumentException("Unexpected key type: expected key to be of type " + type + ", but got " + key.type);
        }
        return key;
    }

    private void ensureSameType(Set<CoordinatorKey> keys) {
        if (keys.size() < 1) {
            throw new IllegalArgumentException("Unexpected size of key set: expected >= 1, but got " + keys.size());
        }
        if (keys.stream().filter(k -> k.type == type).collect(Collectors.toSet()).size() != keys.size()) {
            throw new IllegalArgumentException("Unexpected key set: expected all key to be of type " + type + ", but some key were not");
        }
    }

    private static boolean isRepresentableKey(String groupId) {
        return groupId != null;
    }

    private void handleError(Errors error, CoordinatorKey key, int nodeId, Map<CoordinatorKey, Integer> mappedKeys, Map<CoordinatorKey, Throwable> failedKeys) {
        switch (error) {
            case NONE:
                mappedKeys.put(key, nodeId);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case COORDINATOR_LOAD_IN_PROGRESS:
                log.debug("FindCoordinator request for key {} returned topic-level error {}. Will retry",
                    key, error);
                break;
            case GROUP_AUTHORIZATION_FAILED:
                failedKeys.put(key, new GroupAuthorizationException("FindCoordinator request for groupId " +
                    "`" + key + "` failed due to authorization failure", key.idValue));
                break;
            case TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                failedKeys.put(key, new TransactionalIdAuthorizationException("FindCoordinator request for " +
                    "transactionalId `" + key + "` failed due to authorization failure"));
                break;
            default:
                failedKeys.put(key, error.exception("FindCoordinator request for key " +
                    "`" + key + "` failed due to an unexpected error"));
        }
    }

    private static class LookupRequestScope implements ApiRequestScope {
        final CoordinatorKey key;

        private LookupRequestScope(CoordinatorKey key) {
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LookupRequestScope that = (LookupRequestScope) o;
            return Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }
}
