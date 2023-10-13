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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.NoBatchedFindCoordinatorsException;
import org.apache.kafka.common.requests.OffsetFetchRequest.NoBatchedOffsetFetchRequestException;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

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
 * Managing this complexity by chaining together `Call` implementations is challenging
 * and messy, so instead we use this class to do the bookkeeping. It handles both the
 * batching aspect as well as the transitions between the Lookup and Fulfillment stages.
 *
 * Note that the interpretation of the `retries` configuration becomes ambiguous
 * for this kind of pipeline. We could treat it as an overall limit on the number
 * of requests that can be sent, but that is not very useful because each pipeline
 * has a minimum number of requests that need to be sent in order to satisfy the request.
 * Instead, we treat this number of retries independently at each stage so that each
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
public class AdminApiDriver<K, V> {
    private final Logger log;
    private final ExponentialBackoff retryBackoff;
    private final long deadlineMs;
    private final AdminApiHandler<K, V> handler;
    private final AdminApiFuture<K, V> future;

    private final BiMultimap<ApiRequestScope, K> lookupMap = new BiMultimap<>();
    private final BiMultimap<FulfillmentScope, K> fulfillmentMap = new BiMultimap<>();
    private final Map<ApiRequestScope, RequestState> requestStates = new HashMap<>();

    public AdminApiDriver(
        AdminApiHandler<K, V> handler,
        AdminApiFuture<K, V> future,
        long deadlineMs,
        long retryBackoffMs,
        long retryBackoffMaxMs,
        LogContext logContext
    ) {
        this.handler = handler;
        this.future = future;
        this.deadlineMs = deadlineMs;
        this.retryBackoff = new ExponentialBackoff(
            retryBackoffMs,
            CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
            retryBackoffMaxMs,
            CommonClientConfigs.RETRY_BACKOFF_JITTER);
        this.log = logContext.logger(AdminApiDriver.class);
        retryLookup(future.lookupKeys());
    }

    /**
     * Associate a key with a brokerId. This is called after a response in the Lookup
     * stage reveals the mapping (e.g. when the `FindCoordinator` tells us the group
     * coordinator for a specific consumer group).
     */
    private void map(K key, Integer brokerId) {
        lookupMap.remove(key);
        fulfillmentMap.put(new FulfillmentScope(brokerId), key);
    }

    /**
     * Disassociate a key from the currently mapped brokerId. This will send the key
     * back to the Lookup stage, which will allow us to attempt lookup again.
     */
    private void unmap(K key) {
        fulfillmentMap.remove(key);

        ApiRequestScope lookupScope = handler.lookupStrategy().lookupScope(key);
        OptionalInt destinationBrokerId = lookupScope.destinationBrokerId();

        if (destinationBrokerId.isPresent()) {
            fulfillmentMap.put(new FulfillmentScope(destinationBrokerId.getAsInt()), key);
        } else {
            lookupMap.put(handler.lookupStrategy().lookupScope(key), key);
        }
    }

    private void clear(Collection<K> keys) {
        keys.forEach(key -> {
            lookupMap.remove(key);
            fulfillmentMap.remove(key);
        });
    }

    OptionalInt keyToBrokerId(K key) {
        Optional<FulfillmentScope> scope = fulfillmentMap.getKey(key);
        return scope
            .map(fulfillmentScope -> OptionalInt.of(fulfillmentScope.destinationBrokerId))
            .orElseGet(OptionalInt::empty);
    }

    /**
     * Complete the future associated with the given key exceptionally. After is called,
     * the key will be taken out of both the Lookup and Fulfillment stages so that request
     * are not retried.
     */
    private void completeExceptionally(Map<K, Throwable> errors) {
        if (!errors.isEmpty()) {
            future.completeExceptionally(errors);
            clear(errors.keySet());
        }
    }

    private void completeLookupExceptionally(Map<K, Throwable> errors) {
        if (!errors.isEmpty()) {
            future.completeLookupExceptionally(errors);
            clear(errors.keySet());
        }
    }

    private void retryLookup(Collection<K> keys) {
        keys.forEach(this::unmap);
    }

    /**
     * Complete the future associated with the given key. After this is called, all keys will
     * be taken out of both the Lookup and Fulfillment stages so that request are not retried.
     */
    private void complete(Map<K, V> values) {
        if (!values.isEmpty()) {
            future.complete(values);
            clear(values.keySet());
        }
    }

    private void completeLookup(Map<K, Integer> brokerIdMapping) {
        if (!brokerIdMapping.isEmpty()) {
            future.completeLookup(brokerIdMapping);
            brokerIdMapping.forEach(this::map);
        }
    }

    /**
     * Check whether any requests need to be sent. This should be called immediately
     * after the driver is constructed and then again after each request returns
     * (i.e. after {@link #onFailure(long, RequestSpec, Throwable)} or
     * {@link #onResponse(long, RequestSpec, AbstractResponse, Node)}).
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
     * Callback that is invoked when a `Call` returns a response successfully.
     */
    public void onResponse(
        long currentTimeMs,
        RequestSpec<K> spec,
        AbstractResponse response,
        Node node
    ) {
        clearInflightRequest(currentTimeMs, spec);

        if (spec.scope instanceof FulfillmentScope) {
            AdminApiHandler.ApiResult<K, V> result = handler.handleResponse(
                node,
                spec.keys,
                response
            );
            complete(result.completedKeys);
            completeExceptionally(result.failedKeys);
            retryLookup(result.unmappedKeys);
        } else {
            AdminApiLookupStrategy.LookupResult<K> result = handler.lookupStrategy().handleResponse(
                spec.keys,
                response
            );

            result.completedKeys.forEach(lookupMap::remove);
            completeLookup(result.mappedKeys);
            completeLookupExceptionally(result.failedKeys);
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
        if (t instanceof DisconnectException) {
            log.debug("Node disconnected before response could be received for request {}. " +
                "Will attempt retry", spec.request);

            // After a disconnect, we want the driver to attempt to lookup the key
            // again. This gives us a chance to find a new coordinator or partition
            // leader for example.
            Set<K> keysToUnmap = spec.keys.stream()
                .filter(future.lookupKeys()::contains)
                .collect(Collectors.toSet());
            retryLookup(keysToUnmap);

        } else if (t instanceof NoBatchedFindCoordinatorsException || t instanceof NoBatchedOffsetFetchRequestException) {
            ((CoordinatorStrategy) handler.lookupStrategy()).disableBatch();
            Set<K> keysToUnmap = spec.keys.stream()
                .filter(future.lookupKeys()::contains)
                .collect(Collectors.toSet());
            retryLookup(keysToUnmap);
        } else if (t instanceof UnsupportedVersionException) {
            if (spec.scope instanceof FulfillmentScope) {
                int brokerId = ((FulfillmentScope) spec.scope).destinationBrokerId;
                Map<K, Throwable> unrecoverableFailures =
                    handler.handleUnsupportedVersionException(
                        brokerId,
                        (UnsupportedVersionException) t,
                        spec.keys);
                completeExceptionally(unrecoverableFailures);
            } else {
                Map<K, Throwable> unrecoverableLookupFailures =
                    handler.lookupStrategy().handleUnsupportedVersionException(
                        (UnsupportedVersionException) t,
                        spec.keys);
                completeLookupExceptionally(unrecoverableLookupFailures);
                Set<K> keysToUnmap = spec.keys.stream()
                    .filter(k -> !unrecoverableLookupFailures.containsKey(k))
                    .collect(Collectors.toSet());
                retryLookup(keysToUnmap);
            }
        } else {
            Map<K, Throwable> errors = spec.keys.stream().collect(Collectors.toMap(
                Function.identity(),
                key -> t
            ));
            if (spec.scope instanceof FulfillmentScope) {
                completeExceptionally(errors);
            } else {
                completeLookupExceptionally(errors);
            }
        }
    }

    private void clearInflightRequest(long currentTimeMs, RequestSpec<K> spec) {
        RequestState requestState = requestStates.get(spec.scope);
        if (requestState != null) {
            // Only apply backoff if it's not a retry of a lookup request
            if (spec.scope instanceof FulfillmentScope) {
                requestState.clearInflightAndBackoff(currentTimeMs);
            } else {
                requestState.clearInflight(currentTimeMs);
            }
        }
    }

    private <T extends ApiRequestScope> void collectRequests(
        List<RequestSpec<K>> requests,
        BiMultimap<T, K> multimap,
        BiFunction<Set<K>, T, Collection<AdminApiHandler.RequestAndKeys<K>>> buildRequest
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

            // Copy the keys to avoid exposing the underlying mutable set
            Set<K> copyKeys = Collections.unmodifiableSet(new HashSet<>(keys));

            Collection<AdminApiHandler.RequestAndKeys<K>> newRequests = buildRequest.apply(copyKeys, scope);
            if (newRequests.isEmpty()) {
                return;
            }

            // Only process the first request; all the remaining requests will be targeted at the same broker
            // and we don't want to issue more than one fulfillment request per broker at a time
            AdminApiHandler.RequestAndKeys<K> newRequest = newRequests.iterator().next();
            RequestSpec<K> spec = new RequestSpec<>(
                handler.apiName() + "(api=" + newRequest.request.apiKey() + ")",
                scope,
                newRequest.keys,
                newRequest.request,
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
            (keys, scope) -> Collections.singletonList(new AdminApiHandler.RequestAndKeys<>(handler.lookupStrategy().buildRequest(keys), keys))
        );
    }

    private void collectFulfillmentRequests(List<RequestSpec<K>> requests) {
        collectRequests(
            requests,
            fulfillmentMap,
            (keys, scope) -> handler.buildRequest(scope.destinationBrokerId, keys)
        );
    }

    /**
     * This is a helper class which helps us to map requests that need to be sent
     * to the internal `Call` implementation that is used internally in
     * {@link org.apache.kafka.clients.admin.KafkaAdminClient}.
     */
    public static class RequestSpec<K> {
        public final String name;
        public final ApiRequestScope scope;
        public final Set<K> keys;
        public final AbstractRequest.Builder<?> request;
        public final long nextAllowedTryMs;
        public final long deadlineMs;
        public final int tries;

        public RequestSpec(
            String name,
            ApiRequestScope scope,
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

        @Override
        public String toString() {
            return "RequestSpec(" +
                "name=" + name +
                ", scope=" + scope +
                ", keys=" + keys +
                ", request=" + request +
                ", nextAllowedTryMs=" + nextAllowedTryMs +
                ", deadlineMs=" + deadlineMs +
                ", tries=" + tries +
                ')';
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
            this.nextAllowedRetryMs = currentTimeMs;
        }

        public void clearInflightAndBackoff(long currentTimeMs) {
            clearInflight(currentTimeMs + retryBackoff.backoff(tries >= 1 ? tries - 1 : 0));
        }

        public void setInflight(RequestSpec<K> spec) {
            this.inflightRequest = Optional.of(spec);
            this.tries++;
        }
    }

    /**
     * Completion of the Lookup stage results in a destination broker to send the
     * fulfillment request to. Each destination broker in the Fulfillment stage
     * gets its own request scope.
     */
    private static class FulfillmentScope implements ApiRequestScope {
        public final int destinationBrokerId;

        private FulfillmentScope(int destinationBrokerId) {
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
            FulfillmentScope that = (FulfillmentScope) o;
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
            K key = reverseMap.remove(value);
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

        Optional<K> getKey(V value) {
            return Optional.ofNullable(reverseMap.get(value));
        }

        Set<Map.Entry<K, Set<V>>> entrySet() {
            return map.entrySet();
        }
    }

}
