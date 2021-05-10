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
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.Set;

public class CoordinatorStrategy implements AdminApiLookupStrategy<CoordinatorKey> {
    private final Logger log;

    public CoordinatorStrategy(
        LogContext logContext
    ) {
        this.log = logContext.logger(CoordinatorStrategy.class);
    }

    @Override
    public ApiRequestScope lookupScope(CoordinatorKey key) {
        // The `FindCoordinator` API does not support batched lookups, so we use a
        // separate lookup context for each coordinator key we need to lookup
        return new LookupRequestScope(key);
    }

    @Override
    public FindCoordinatorRequest.Builder buildRequest(Set<CoordinatorKey> keys) {
        CoordinatorKey key = requireSingleton(keys);
        return new FindCoordinatorRequest.Builder(
            new FindCoordinatorRequestData()
                .setKey(key.idValue)
                .setKeyType(key.type.id())
        );
    }

    @Override
    public LookupResult<CoordinatorKey> handleResponse(
        Set<CoordinatorKey> keys,
        AbstractResponse abstractResponse
    ) {
        CoordinatorKey key = requireSingleton(keys);
        FindCoordinatorResponse response = (FindCoordinatorResponse) abstractResponse;
        Errors error = response.error();

        switch (error) {
            case NONE:
                return LookupResult.mapped(key, response.data().nodeId());

            case COORDINATOR_NOT_AVAILABLE:
            case COORDINATOR_LOAD_IN_PROGRESS:
                log.debug("FindCoordinator request for key {} returned topic-level error {}. Will retry",
                    key, error);
                return LookupResult.empty();

            case GROUP_AUTHORIZATION_FAILED:
                return LookupResult.failed(key, new GroupAuthorizationException("FindCoordinator request for groupId " +
                    "`" + key + "` failed due to authorization failure", key.idValue));

            case TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                return LookupResult.failed(key, new TransactionalIdAuthorizationException("FindCoordinator request for " +
                    "transactionalId `" + key + "` failed due to authorization failure"));

            default:
                return LookupResult.failed(key, error.exception("FindCoordinator request for key " +
                    "`" + key + "` failed due to an unexpected error"));
        }
    }

    private static CoordinatorKey requireSingleton(Set<CoordinatorKey> keys) {
        if (keys.size() != 1) {
            throw new IllegalArgumentException("Unexpected lookup key set");
        }
        return keys.iterator().next();
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
