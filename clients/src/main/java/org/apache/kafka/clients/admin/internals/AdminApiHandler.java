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

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public interface AdminApiHandler<K, V> {

    /**
     * Get a user-friendly name for the API this handler is implementing.
     */
    String apiName();

    /**
     * Initialize the set of keys required to handle this API and how the driver
     * should map them to the broker that will handle the request for these keys.
     *
     * Two mapping types are supported:
     *
     * - Static mapping: when the brokerId is known ahead of time
     * - Dynamic mapping: when the brokerId must be discovered dynamically
     *
     * @return the key mappings
     */
    Keys<K> initializeKeys();

    /**
     * Build the request. The set of keys are derived by {@link AdminApiDriver}
     * during the lookup stage as the set of keys which all map to the same
     * destination broker.
     *
     * @param brokerId the target brokerId for the request
     * @param keys the set of keys that should be handled by this request
     *
     * @return a builder for the request containing the given keys
     */
    AbstractRequest.Builder<?> buildRequest(int brokerId, Set<K> keys);

    /**
     * Callback that is invoked when a request returns successfully.
     * The handler should parse the response, check for errors, and return a
     * result which indicates which keys (if any) have either been completed or
     * failed with an unrecoverable error.
     *
     * It is also possible that the response indicates an incorrect target brokerId
     * (e.g. in the case of a NotLeader error when the request is bound for a partition
     * leader). In this case the key will be "unmapped" from the target brokerId
     * and lookup will be retried.
     *
     * Note that keys which received a retriable error should be left out of the
     * result. They will be retried automatically.
     *
     * @param brokerId the brokerId that the associated request was sent to
     * @param keys the set of keys from the associated request
     * @param response the response received from the broker
     *
     * @return result indicating key completion, failure, and unmapping
     */
    ApiResult<K, V> handleResponse(int brokerId, Set<K> keys, AbstractResponse response);

    class Keys<K> {
        public final Map<K, Integer> staticKeys;
        public final Set<K> dynamicKeys;
        public final AdminApiLookupStrategy<K> lookupStrategy;

        public Keys(
            Map<K, Integer> staticKeys,
            Set<K> dynamicKeys,
            AdminApiLookupStrategy<K> lookupStrategy
        ) {
            this.staticKeys = requireNonNull(staticKeys);
            this.dynamicKeys = requireNonNull(dynamicKeys);
            this.lookupStrategy = lookupStrategy;

            Set<K> staticAndDynamicKeys = Utils.intersection(HashSet::new, staticKeys.keySet(), dynamicKeys);
            if (!staticAndDynamicKeys.isEmpty()) {
                throw new IllegalArgumentException("The following keys were configured both as dynamically " +
                    "and statically mapped: " + staticAndDynamicKeys);
            }

            if (!dynamicKeys.isEmpty()) {
                requireNonNull(lookupStrategy);
            }
        }

        public static <K> Keys<K> staticMapped(
            Map<K, Integer> staticKeys
        ) {
            return new Keys<>(staticKeys, Collections.emptySet(), null);
        }

        public static <K> Keys<K> dynamicMapped(
            Set<K> dynamicKeys,
            AdminApiLookupStrategy<K> lookupStrategy
        ) {
            return new Keys<>(Collections.emptyMap(), dynamicKeys, lookupStrategy);
        }
    }

    class ApiResult<K, V> {
        public final Map<K, V> completedKeys;
        public final Map<K, Throwable> failedKeys;
        public final List<K> unmappedKeys;

        public ApiResult(
            Map<K, V> completedKeys,
            Map<K, Throwable> failedKeys,
            List<K> unmappedKeys
        ) {
            this.completedKeys = Collections.unmodifiableMap(completedKeys);
            this.failedKeys = Collections.unmodifiableMap(failedKeys);
            this.unmappedKeys = Collections.unmodifiableList(unmappedKeys);
        }

        public static <K, V> ApiResult<K, V> completed(K key, V value) {
            return new ApiResult<>(
                Collections.singletonMap(key, value),
                Collections.emptyMap(),
                Collections.emptyList()
            );
        }

        public static <K, V> ApiResult<K, V> failed(K key, Throwable t) {
            return new ApiResult<>(
                Collections.emptyMap(),
                Collections.singletonMap(key, t),
                Collections.emptyList()
            );
        }

        public static <K, V> ApiResult<K, V> unmapped(List<K> keys) {
            return new ApiResult<>(
                Collections.emptyMap(),
                Collections.emptyMap(),
                keys
            );
        }
    }

}
