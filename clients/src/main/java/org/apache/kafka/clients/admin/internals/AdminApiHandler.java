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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface AdminApiHandler<K, V> {

    /**
     * Get a user-friendly name for the API this handler is implementing.
     */
    String apiName();

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

    /**
     * Get the lookup strategy that is responsible for finding the brokerId
     * which will handle each respective key.
     *
     * @return non-null lookup strategy
     */
    AdminApiLookupStrategy<K> lookupStrategy();

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

        public static <K, V> ApiResult<K, V> empty() {
            return new ApiResult<>(
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList()
            );
        }
    }

}
