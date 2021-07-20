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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public interface AdminApiLookupStrategy<T> {

    /**
     * Define the scope of a given key for lookup. Key lookups are complicated
     * by the need to accommodate different batching mechanics. For example,
     * a `Metadata` request supports arbitrary batching of topic partitions in
     * order to discover partitions leaders. This can be supported by returning
     * a single scope object for all keys.
     *
     * On the other hand, `FindCoordinator` requests only support lookup of a
     * single key. This can be supported by returning a different scope object
     * for each lookup key.
     *
     * Note that if the {@link ApiRequestScope#destinationBrokerId()} maps to
     * a specific brokerId, then lookup will be skipped. See the use of
     * {@link StaticBrokerStrategy} in {@link DescribeProducersHandler} for
     * an example of this usage.
     *
     * @param key the lookup key
     *
     * @return request scope indicating how lookup requests can be batched together
     */
    ApiRequestScope lookupScope(T key);

    /**
     * Build the lookup request for a set of keys. The grouping of the keys is controlled
     * through {@link #lookupScope(Object)}. In other words, each set of keys that map
     * to the same request scope object will be sent to this method.
     *
     * @param keys the set of keys that require lookup
     *
     * @return a builder for the lookup request
     */
    AbstractRequest.Builder<?> buildRequest(Set<T> keys);

    /**
     * Callback that is invoked when a lookup request returns successfully. The handler
     * should parse the response, check for errors, and return a result indicating
     * which keys were mapped to a brokerId successfully and which keys received
     * a fatal error (e.g. a topic authorization failure).
     *
     * Note that keys which receive a retriable error should be left out of the
     * result. They will be retried automatically. For example, if the response of
     * `FindCoordinator` request indicates an unavailable coordinator, then the key
     * should be left out of the result so that the request will be retried.
     *
     * @param keys the set of keys from the associated request
     * @param response the response received from the broker
     *
     * @return a result indicating which keys mapped successfully to a brokerId and
     *         which encountered a fatal error
     */
    LookupResult<T> handleResponse(Set<T> keys, AbstractResponse response);

    class LookupResult<K> {
        // This is the set of keys that have been completed by the lookup phase itself.
        // The driver will not attempt lookup or fulfillment for completed keys.
        public final List<K> completedKeys;

        // This is the set of keys that have been mapped to a specific broker for
        // fulfillment of the API request.
        public final Map<K, Integer> mappedKeys;

        // This is the set of keys that have encountered a fatal error during the lookup
        // phase. The driver will not attempt lookup or fulfillment for failed keys.
        public final Map<K, Throwable> failedKeys;

        public LookupResult(
            Map<K, Throwable> failedKeys,
            Map<K, Integer> mappedKeys
        ) {
            this(Collections.emptyList(), failedKeys, mappedKeys);
        }

        public LookupResult(
            List<K> completedKeys,
            Map<K, Throwable> failedKeys,
            Map<K, Integer> mappedKeys
        ) {
            this.completedKeys = Collections.unmodifiableList(completedKeys);
            this.failedKeys = Collections.unmodifiableMap(failedKeys);
            this.mappedKeys = Collections.unmodifiableMap(mappedKeys);
        }

        static <K> LookupResult<K> empty() {
            return new LookupResult<>(emptyMap(), emptyMap());
        }

        static <K> LookupResult<K> failed(K key, Throwable exception) {
            return new LookupResult<>(singletonMap(key, exception), emptyMap());
        }

        static <K> LookupResult<K> mapped(K key, Integer brokerId) {
            return new LookupResult<>(emptyMap(), singletonMap(key, brokerId));
        }

    }

}
