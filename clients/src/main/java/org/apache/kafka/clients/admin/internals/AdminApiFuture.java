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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface AdminApiFuture<K, V> {

    /**
     * The initial set of lookup keys. Although this will usually match the fulfillment
     * keys, it does not necessarily have to. For example, in the case of
     * {@link AllBrokersStrategy.AllBrokersFuture},
     * we use the lookup phase in order to discover the set of keys that will be searched
     * during the fulfillment phase.
     *
     * @return non-empty set of initial lookup keys
     */
    Set<K> lookupKeys();

    /**
     * Complete the futures associated with the given keys.
     *
     * @param values the completed keys with their respective values
     */
    void complete(Map<K, V> values);

    /**
     * Invoked when lookup of a set of keys succeeds.
     *
     * @param brokerIdMapping the discovered mapping from key to the respective brokerId that will
     *                        handle the fulfillment request
     */
    default void completeLookup(Map<K, Integer> brokerIdMapping) {
    }

    /**
     * Invoked when lookup fails with a fatal error on a set of keys.
     *
     * @param lookupErrors the set of keys that failed lookup with their respective errors
     */
    default void completeLookupExceptionally(Map<K, Throwable> lookupErrors) {
        completeExceptionally(lookupErrors);
    }

    /**
     * Complete the futures associated with the given keys exceptionally.
     *
     * @param errors the failed keys with their respective errors
     */
    void completeExceptionally(Map<K, Throwable> errors);

    static <K, V> SimpleAdminApiFuture<K, V> forKeys(Set<K> keys) {
        return new SimpleAdminApiFuture<>(keys);
    }

    /**
     * This class can be used when the set of keys is known ahead of time.
     */
    class SimpleAdminApiFuture<K, V> implements AdminApiFuture<K, V> {
        private final Map<K, KafkaFuture<V>> futures;

        public SimpleAdminApiFuture(Set<K> keys) {
            this.futures = keys.stream().collect(Collectors.toMap(
                Function.identity(),
                k -> new KafkaFutureImpl<>()
            ));
        }

        @Override
        public Set<K> lookupKeys() {
            return futures.keySet();
        }

        @Override
        public void complete(Map<K, V> values) {
            values.forEach(this::complete);
        }

        private void complete(K key, V value) {
            futureOrThrow(key).complete(value);
        }

        @Override
        public void completeExceptionally(Map<K, Throwable> errors) {
            errors.forEach(this::completeExceptionally);
        }

        private void completeExceptionally(K key, Throwable t) {
            futureOrThrow(key).completeExceptionally(t);
        }

        private KafkaFutureImpl<V> futureOrThrow(K key) {
            // The below typecast is safe because we initialise futures using only KafkaFutureImpl.
            KafkaFutureImpl<V> future = (KafkaFutureImpl<V>) futures.get(key);
            if (future == null) {
                throw new IllegalArgumentException("Attempt to complete future for " + key +
                    ", which was not requested");
            } else {
                return future;
            }
        }

        public Map<K, KafkaFuture<V>> all() {
            return futures;
        }

        public KafkaFuture<V> get(K key) {
            return futures.get(key);
        }
    }
}
