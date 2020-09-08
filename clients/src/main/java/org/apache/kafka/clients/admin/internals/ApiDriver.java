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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * The `KafkaAdminClient`'s internal `Call` primitive is not a good fit for multi-stage
 * request workflows such as we see with the group coordinator APIs or any request which
 * needs to be sent to a partition leader. Typically these APIs have two concrete stages:
 *
 * 1. Lookup: Find the broker that can fulfill the request (e.g. partition leader or group
 *            coordinator)
 * 2. Fulfillment: Send the request to the broker found in the first step
 *
 * This is complicated by the fact that `Admin` APIs are typically batched, which
 * means the Lookup stage may result in a set of brokers. For example, take a `ListOffsets`
 * request for a set of topic partitions. In the Lookup stage, we will find the partition
 * leaders for this set of partitions; in the Fulfillment stage, we will group together
 * partition according to the IDs of the discovered leaders.
 *
 * Additionally, the flow between these two stages is bi-directional. We may find after
 * sending a `ListOffsets` request to an expected leader that there was a leader change.
 * This would result in a topic partition being sent back to the Lookup stage.
 *
 * Managing this complexity by chaining together `Call` implementations is challenging,
 * so instead we use this class to do the bookkeeping. It handles both the batching
 * aspect as well as the transitions between the Lookup and Fulfillment stages.
 *
 * Note that the interpretation of the AdminClient `retries` configuration becomes
 * ambiguous for this kind of pipeline. We could treat it as an overall limit on the
 * number of requests that can be sent, but that is not very useful because each pipeline
 * has a minimum number of requests that need to be sent in order to satisfy the request.
 * Instead, we treat this number of tries independently at each stage so that each
 * stage has at least one opportunity to complete. So if a user sets `retries=1`, then
 * the full pipeline can still complete as long as there are no request failures.
 *
 * @param <K> The key type, which is also the granularity of the request routing (e.g.
 *            this could be `TopicPartition` in the case of requests intended for a partition
 *            leader or the `GroupId` in the case of consumer group requests intended for
 *            the group coordinator)
 * @param <V> The fulfillment type for each key (e.g. this could be consumer group state
 *            when the key type is a consumer `GroupId`)
 */
public abstract class ApiDriver<K, V> {

    private final Logger log;
    private final long retryBackoffMs;
    private final long deadlineMs;
    private final Map<K, KafkaFutureImpl<V>> futures;

    private final BiMultimap<RequestScope, K> lookupMap = new BiMultimap<>();
    private final BiMultimap<BrokerScope, K> fulfillmentMap = new BiMultimap<>();
    private final Map<RequestScope, RequestState> requestStates = new HashMap<>();

    public ApiDriver(
        Collection<K> keys,
        long deadlineMs,
        long retryBackoffMs,
        LogContext logContext
    ) {
        this.futures = Utils.initializeMap(keys, KafkaFutureImpl::new);
        this.deadlineMs = deadlineMs;
        this.retryBackoffMs = retryBackoffMs;
        this.log = logContext.logger(ApiDriver.class);
        initializeLookupKeys();
    }

    private void initializeLookupKeys() {
        for (K key : futures.keySet()) {
            lookupMap.put(lookupScope(key), key);
        }
    }

    /**
     * Check whether a particular key has been requested. This is useful when a response
     * contains more partitions than are strictly needed. For example, a `Metadata`
     * response always includes all partitions for each requested topic, even if we are
     * only interested in a subset of them.
     */
    boolean contains(K key) {
        return futures.containsKey(key);
    }

    /**
     * Associate a key with a brokerId. This is called after a response in the Lookup
     * stage reveals the mapping (e.g. when the `FindCoordinator` tells us the the
     * group coordinator for a specific consumer group).
     */
    void map(K key, Integer brokerId) {
        lookupMap.remove(key);
        fulfillmentMap.put(new BrokerScope(brokerId), key);

        // To allow for derived keys, we create futures dynamically if they
        // do not already exist in the future map
        futures.computeIfAbsent(key, k -> new KafkaFutureImpl<>());
    }

    /**
     * Disassociate a key from the currently mapped brokerId. This will send the key
     * back to the Lookup stage, which will allow us to attempt lookup again.
     */
    void unmap(K key) {
        fulfillmentMap.remove(key);
        lookupMap.put(lookupScope(key), key);
    }

    private void clear(K key) {
        lookupMap.remove(key);
        fulfillmentMap.remove(key);
    }

    /**
     * Complete the future associated with the given key exceptionally. After is called,
     * the key will be taken out of both the Lookup and Fulfillment stages so that request
     * are not retried.
     */
    void completeExceptionally(K key, Throwable t) {
        KafkaFutureImpl<V> future = futures.get(key);
        if (future == null) {
            log.warn("Attempt to complete future for {}, which was not requested", key);
        } else {
            clear(key);
            future.completeExceptionally(t);
        }
    }

    /**
     * Complete the future associated with the given key. After is called, the key will
     * be taken out of both the Lookup and Fulfillment stages so that request are not retried.
     */
    void complete(K key, V value) {
        KafkaFutureImpl<V> future = futures.get(key);
        if (future == null) {
            log.warn("Attempt to complete future for {}, which was not requested", key);
        } else {
            clear(key);
            future.complete(value);
        }
    }

    /**
     * Check whether any requests need to be sent. This should be called immediately
     * after the driver is constructed and then again after each request returns
     * (i.e. after {@link #onFailure(long, RequestSpec, Throwable)} or
     * {@link #onResponse(long, RequestSpec, AbstractResponse)}).
     *
     * @return A list of requests that need to be sent
     */
    public List<RequestSpec<K>> poll() {
        List<RequestSpec<K>> requests = new ArrayList<>();
        collectLookupRequests(requests);
        collectFulfillmentRequests(requests);
        return requests;
    }

    /**
     * Get a map of the futures that are awaiting completion.
     */
    public Map<K, KafkaFutureImpl<V>> futures() {
        return futures;
    }

    /**
     * Callback that is invoked when a `Call` returns a response successfully.
     */
    public void onResponse(
        long currentTimeMs,
        RequestSpec<K> spec,
        AbstractResponse response
    ) {
        clearInflightRequest(currentTimeMs, spec);
        if (spec.scope instanceof ApiDriver.BrokerScope) {
            int brokerId = ((BrokerScope) spec.scope).destinationBrokerId;
            handleFulfillmentResponse(brokerId, spec.keys, response);
        } else {
            handleLookupResponse(spec.keys, response);
        }
    }

    /**
     * Callback that is invoked when a `Call` is failed.
     */
    public void onFailure(
        long currentTimeMs,
        RequestSpec<K> spec,
        Throwable t
    ) {
        clearInflightRequest(currentTimeMs, spec);
        spec.keys.forEach(key -> completeExceptionally(key, t));
    }

    /**
     * Get a user-friendly name for the API this class is implementing.
     */
    abstract String apiName();

    /**
     * The Lookup stage is complicated by the need to accommodate different batching mechanics.
     * Specifically, a `Metadata` request supports arbitrary batching of topic partitions, but
     * a `FindCoordinator` request only supports lookup of a single key. See the implementations
     * in {@link PartitionLeaderApiDriver#lookupScope(TopicPartition)} and
     * {@link CoordinatorApiDriver#lookupScope(CoordinatorKey)}.
     */
    abstract RequestScope lookupScope(K key);

    /**
     * Build the lookup request for a set of keys. The grouping of the keys is controlled
     * through {@link #lookupScope(Object)}. In other words, each set of keys that map
     * to the same request scope object will be sent to this method.
     */
    abstract AbstractRequest.Builder<?> buildLookupRequest(Set<K> keys);

    /**
     * Callback that is invoked when a Lookup request returns successfully. The handler
     * should parse the response, check for errors, and updating mappings with
     * {@link #map(Object, Integer)} and {@link #unmap(Object)} as needed.
     */
    abstract void handleLookupResponse(Set<K> keys, AbstractResponse response);

    /**
     * Build the fulfillment request. The set of keys are derived during the Lookup stage
     * as the set of keys which all map to the same destination broker.
     */
    abstract AbstractRequest.Builder<?> buildFulfillmentRequest(Integer brokerId, Set<K> keys);

    /**
     * Callback that is invoked when a Fulfillment request returns successfully.
     * The handler should parse the response, check for errors, update mappings as
     * needed, and complete any futures which can be completed.
     */
    abstract void handleFulfillmentResponse(Integer brokerId, Set<K> keys, AbstractResponse response);

    private void clearInflightRequest(long currentTimeMs, RequestSpec<K> spec) {
        RequestState requestState = requestStates.get(spec.scope);
        if (requestState != null) {
            requestState.clearInflight(currentTimeMs);
        }
    }

    private <T extends RequestScope> void collectRequests(
        List<RequestSpec<K>> requests,
        BiMultimap<T, K> multimap,
        BiFunction<Set<K>, T, AbstractRequest.Builder<?>> buildRequest
    ) {
        for (Map.Entry<T, Set<K>> entry : multimap.entrySet()) {
            T scope = entry.getKey();

            Set<K> keys = entry.getValue();
            if (keys.isEmpty()) {
                continue;
            }

            RequestState requestState = requestStates.computeIfAbsent(scope, c -> new RequestState());
            if (requestState.hasInflight()) {
                continue;
            }

            AbstractRequest.Builder<?> request = buildRequest.apply(keys, scope);
            RequestSpec<K> spec = new RequestSpec<>(
                this.apiName() + "(api=" + request.apiKey() + ")",
                scope,
                new HashSet<>(keys), // copy to avoid exposing mutable state
                request,
                requestState.nextAllowedRetryMs,
                deadlineMs,
                requestState.tries
            );

            requestState.setInflight(spec);
            requests.add(spec);
        }
    }

    private void collectLookupRequests(List<RequestSpec<K>> requests) {
        collectRequests(
            requests,
            lookupMap,
            (keys, scope) -> buildLookupRequest(keys)
        );
    }

    private void collectFulfillmentRequests(List<RequestSpec<K>> requests) {
        collectRequests(
            requests,
            fulfillmentMap,
            (keys, scope) -> buildFulfillmentRequest(scope.destinationBrokerId, keys)
        );
    }

    /**
     * This is a helper class which helps us to map requests that need to be sent to
     * the to the internal `Call` implementation that is used internally in
     * {@link org.apache.kafka.clients.admin.KafkaAdminClient}.
     */
    public static class RequestSpec<K> {
        public final String name;
        public final RequestScope scope;
        public final Set<K> keys;
        public final AbstractRequest.Builder<?> request;
        public final long nextAllowedTryMs;
        public final long deadlineMs;
        public final int tries;

        public RequestSpec(
            String name,
            RequestScope scope,
            Set<K> keys,
            AbstractRequest.Builder<?> request,
            long nextAllowedTryMs,
            long deadlineMs,
            int tries
        ) {
            this.name = name;
            this.scope = scope;
            this.keys = keys;
            this.request = request;
            this.nextAllowedTryMs = nextAllowedTryMs;
            this.deadlineMs = deadlineMs;
            this.tries = tries;
        }
    }

    /**
     * Helper class used to track the request state within each request scope.
     * This class enforces a maximum number of inflight request and keeps track
     * of backoff/retry state.
     */
    private class RequestState {
        private Optional<RequestSpec<K>> inflightRequest = Optional.empty();
        private int tries = 0;
        private long nextAllowedRetryMs = 0;

        boolean hasInflight() {
            return inflightRequest.isPresent();
        }

        public void clearInflight(long currentTimeMs) {
            this.inflightRequest = Optional.empty();
            this.nextAllowedRetryMs = currentTimeMs + retryBackoffMs;
        }

        public void setInflight(RequestSpec<K> spec) {
            this.inflightRequest = Optional.of(spec);
            this.tries++;
        }
    }

    /**
     * Interface which is used to identify the scope of a request in either stage
     * of the pipeline. This is primarily used to support backoff/retry mechanics.
     */
    public interface RequestScope {
        default OptionalInt destinationBrokerId() {
            return OptionalInt.empty();
        }
    }

    /**
     * Completion of the Lookup stage results in a destination broker to send the
     * fulfillment request to. Each destination broker in the Fulfillment stage
     * gets its own request scope.
     */
    private static class BrokerScope implements RequestScope {
        public final int destinationBrokerId;

        private BrokerScope(int destinationBrokerId) {
            this.destinationBrokerId = destinationBrokerId;
        }

        @Override
        public OptionalInt destinationBrokerId() {
            return OptionalInt.of(destinationBrokerId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BrokerScope that = (BrokerScope) o;
            return destinationBrokerId == that.destinationBrokerId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(destinationBrokerId);
        }
    }

    /**
     * Helper class which maintains a bi-directional mapping from a key to a set of values.
     * Each value can map to one and only one key, but many values can be associated with
     * a single key.
     *
     * @param <K> The key type
     * @param <V> The value type
     */
    private static class BiMultimap<K, V> {
        private final Map<V, K> reverseMap = new HashMap<>();
        private final Map<K, Set<V>> map = new HashMap<>();

        void put(K key, V value) {
            remove(value);
            reverseMap.put(value, key);
            map.computeIfAbsent(key, k -> new HashSet<>()).add(value);
        }

        void remove(V value) {
            K key = reverseMap.get(value);
            if (key != null) {
                Set<V> set = map.get(key);
                if (set != null) {
                    set.remove(value);
                    if (set.isEmpty()) {
                        map.remove(key);
                    }
                }
            }
        }

        Set<Map.Entry<K, Set<V>>> entrySet() {
            return map.entrySet();
        }
    }

}
