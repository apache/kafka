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

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is used for use cases which require requests to be sent to all
 * brokers in the cluster.
 *
 * This is a slightly degenerate case of a lookup strategy in the sense that
 * the broker IDs are used as both the keys and values. Also, unlike
 * {@link CoordinatorStrategy} and {@link PartitionLeaderStrategy}, we do not
 * know the set of keys ahead of time: we require the initial lookup in order
 * to discover what the broker IDs are. This is represented with a more complex
 * type {@code Future<Map<Integer, Future<V>>} in the admin API result type.
 * For example, see {@link org.apache.kafka.clients.admin.ListTransactionsResult}.
 */
public class AllBrokersStrategy implements AdminApiLookupStrategy<AllBrokersStrategy.BrokerKey> {
    public static final BrokerKey ANY_BROKER = new BrokerKey(OptionalInt.empty());
    public static final Set<BrokerKey> LOOKUP_KEYS = Collections.singleton(ANY_BROKER);
    private static final ApiRequestScope SINGLE_REQUEST_SCOPE = new ApiRequestScope() {
    };

    private final Logger log;

    public AllBrokersStrategy(
        LogContext logContext
    ) {
        this.log = logContext.logger(AllBrokersStrategy.class);
    }

    @Override
    public ApiRequestScope lookupScope(BrokerKey key) {
        return SINGLE_REQUEST_SCOPE;
    }

    @Override
    public MetadataRequest.Builder buildRequest(Set<BrokerKey> keys) {
        validateLookupKeys(keys);
        // Send empty `Metadata` request. We are only interested in the brokers from the response
        return new MetadataRequest.Builder(new MetadataRequestData());
    }

    @Override
    public LookupResult<BrokerKey> handleResponse(Set<BrokerKey> keys, AbstractResponse abstractResponse) {
        validateLookupKeys(keys);

        MetadataResponse response = (MetadataResponse) abstractResponse;
        MetadataResponseData.MetadataResponseBrokerCollection brokers = response.data().brokers();

        if (brokers.isEmpty()) {
            log.debug("Metadata response contained no brokers. Will backoff and retry");
            return LookupResult.empty();
        } else {
            log.debug("Discovered all brokers {} to send requests to", brokers);
        }

        Map<BrokerKey, Integer> brokerKeys = brokers.stream().collect(Collectors.toMap(
            broker -> new BrokerKey(OptionalInt.of(broker.nodeId())),
            MetadataResponseData.MetadataResponseBroker::nodeId
        ));

        return new LookupResult<>(
            Collections.singletonList(ANY_BROKER),
            Collections.emptyMap(),
            brokerKeys
        );
    }

    private void validateLookupKeys(Set<BrokerKey> keys) {
        if (keys.size() != 1) {
            throw new IllegalArgumentException("Unexpected key set: " + keys);
        }
        BrokerKey key = keys.iterator().next();
        if (key != ANY_BROKER) {
            throw new IllegalArgumentException("Unexpected key set: " + keys);
        }
    }

    public static class BrokerKey {
        public final OptionalInt brokerId;

        public BrokerKey(OptionalInt brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BrokerKey that = (BrokerKey) o;
            return Objects.equals(brokerId, that.brokerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(brokerId);
        }

        @Override
        public String toString() {
            return "BrokerKey(" +
                "brokerId=" + brokerId +
                ')';
        }
    }

    public static class AllBrokersFuture<V> implements AdminApiFuture<BrokerKey, V> {
        private final KafkaFutureImpl<Map<Integer, KafkaFutureImpl<V>>> future = new KafkaFutureImpl<>();
        private final Map<Integer, KafkaFutureImpl<V>> brokerFutures = new HashMap<>();

        @Override
        public Set<BrokerKey> lookupKeys() {
            return LOOKUP_KEYS;
        }

        @Override
        public void completeLookup(Map<BrokerKey, Integer> brokerMapping) {
            brokerMapping.forEach((brokerKey, brokerId) -> {
                if (brokerId != brokerKey.brokerId.orElse(-1)) {
                    throw new IllegalArgumentException("Invalid lookup mapping " + brokerKey + " -> " + brokerId);
                }
                brokerFutures.put(brokerId, new KafkaFutureImpl<>());
            });
            future.complete(brokerFutures);
        }

        @Override
        public void completeLookupExceptionally(Map<BrokerKey, Throwable> lookupErrors) {
            if (!LOOKUP_KEYS.equals(lookupErrors.keySet())) {
                throw new IllegalArgumentException("Unexpected keys among lookup errors: " + lookupErrors);
            }
            future.completeExceptionally(lookupErrors.get(ANY_BROKER));
        }

        @Override
        public void complete(Map<BrokerKey, V> values) {
            values.forEach(this::complete);
        }

        private void complete(AllBrokersStrategy.BrokerKey key, V value) {
            if (key == ANY_BROKER) {
                throw new IllegalArgumentException("Invalid attempt to complete with lookup key sentinel");
            } else {
                futureOrThrow(key).complete(value);
            }
        }

        @Override
        public void completeExceptionally(Map<BrokerKey, Throwable> errors) {
            errors.forEach(this::completeExceptionally);
        }

        private void completeExceptionally(AllBrokersStrategy.BrokerKey key, Throwable t) {
            if (key == ANY_BROKER) {
                future.completeExceptionally(t);
            } else {
                futureOrThrow(key).completeExceptionally(t);
            }
        }

        public KafkaFutureImpl<Map<Integer, KafkaFutureImpl<V>>> all() {
            return future;
        }

        private KafkaFutureImpl<V> futureOrThrow(BrokerKey key) {
            if (!key.brokerId.isPresent()) {
                throw new IllegalArgumentException("Attempt to complete with invalid key: " + key);
            } else {
                int brokerId = key.brokerId.getAsInt();
                KafkaFutureImpl<V> future = brokerFutures.get(brokerId);
                if (future == null) {
                    throw new IllegalArgumentException("Attempt to complete with unknown broker id: " + brokerId);
                } else {
                    return future;
                }
            }
        }

    }

}
