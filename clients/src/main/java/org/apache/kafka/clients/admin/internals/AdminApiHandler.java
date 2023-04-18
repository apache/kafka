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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface AdminApiHandler<K, V> {

    /**
     * Get a user-friendly name for the API this handler is implementing.
     */
    String apiName();

    /**
     * Build the requests necessary for the given keys. The set of keys is derived by
     * {@link AdminApiDriver} during the lookup stage as the set of keys which all map
     * to the same destination broker. Handlers can choose to issue a single request for
     * all of the provided keys (see {@link Batched}, issue one request per key (see
     * {@link Unbatched}, or implement their own custom grouping logic if necessary.
     *
     * @param brokerId the target brokerId for the request
     * @param keys the set of keys that should be handled by this request
     *
     * @return a collection of {@link RequestAndKeys} for the requests containing the given keys
     */
    Collection<RequestAndKeys<K>> buildRequest(int brokerId, Set<K> keys);

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
     * @param broker the broker that the associated request was sent to
     * @param keys the set of keys from the associated request
     * @param response the response received from the broker
     *
     * @return result indicating key completion, failure, and unmapping
     */
    ApiResult<K, V> handleResponse(Node broker, Set<K> keys, AbstractResponse response);

    /**
     * Callback that is invoked when a fulfillment request hits an UnsupportedVersionException.
     * Keys for which the exception cannot be handled and the request shouldn't be retried must be mapped
     * to an error and returned. The request will then be retried for the remainder of the keys.
     *
     * @return The failure mappings for the keys for which the exception cannot be handled and the
     * request shouldn't be retried. If the exception cannot be handled all initial keys will be in
     * the returned map.
     */
    default Map<K, Throwable> handleUnsupportedVersionException(
        int brokerId,
        UnsupportedVersionException exception,
        Set<K> keys
    ) {
        return keys.stream().collect(Collectors.toMap(k -> k, k -> exception));
    }

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

    class RequestAndKeys<K> {
        public final AbstractRequest.Builder<?> request;
        public final Set<K> keys;

        public RequestAndKeys(AbstractRequest.Builder<?> request, Set<K> keys) {
            this.request = request;
            this.keys = keys;
        }
    }

    /**
     * An {@link AdminApiHandler} that will group multiple keys into a single request when possible.
     * Keys will be grouped together whenever they target the same broker. This type of handler
     * should be used when interacting with broker APIs that can act on multiple keys at once, such
     * as describing or listing transactions.
     */
    abstract class Batched<K, V> implements AdminApiHandler<K, V> {
        abstract AbstractRequest.Builder<?> buildBatchedRequest(int brokerId, Set<K> keys);

        @Override
        public final Collection<RequestAndKeys<K>> buildRequest(int brokerId, Set<K> keys) {
            return Collections.singleton(new RequestAndKeys<>(buildBatchedRequest(brokerId, keys), keys));
        }
    }

    /**
     * An {@link AdminApiHandler} that will create one request per key, not performing any grouping based
     * on the targeted broker. This type of handler should only be used for broker APIs that do not accept
     * multiple keys at once, such as initializing a transactional producer.
     */
    abstract class Unbatched<K, V> implements AdminApiHandler<K, V> {
        abstract AbstractRequest.Builder<?> buildSingleRequest(int brokerId, K key);
        abstract ApiResult<K, V> handleSingleResponse(Node broker, K key, AbstractResponse response);

        @Override
        public final Collection<RequestAndKeys<K>> buildRequest(int brokerId, Set<K> keys) {
            return keys.stream()
                .map(key -> new RequestAndKeys<>(buildSingleRequest(brokerId, key), Collections.singleton(key)))
                .collect(Collectors.toSet());
        }

        @Override
        public final ApiResult<K, V> handleResponse(Node broker, Set<K> keys, AbstractResponse response) {
            if (keys.size() != 1) {
                throw new IllegalArgumentException("Unbatched admin handler should only be required to handle responses for a single key at a time");
            }
            K key = keys.iterator().next();
            return handleSingleResponse(broker, key, response);
        }
    }
}
