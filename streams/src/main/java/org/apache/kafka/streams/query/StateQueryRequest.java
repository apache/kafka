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
package org.apache.kafka.streams.query;

import org.apache.kafka.common.annotation.InterfaceStability.Evolving;

import java.util.Optional;
import java.util.Set;

/**
 * The request object for Interactive Queries. This is an immutable builder class for passing all
 * required and optional arguments for querying a state store in Kafka Streams.
 * <p>
 *
 * @param <R> The type of the query result.
 */
@Evolving
public class StateQueryRequest<R> {

    private final String storeName;
    private final PositionBound position;
    private final Optional<Set<Integer>> partitions;
    private final Query<R> query;
    private final boolean executionInfoEnabled;
    private final boolean requireActive;

    private StateQueryRequest(
        final String storeName,
        final PositionBound position,
        final Optional<Set<Integer>> partitions,
        final Query<R> query,
        final boolean executionInfoEnabled,
        final boolean requireActive) {

        this.storeName = storeName;
        this.position = position;
        this.partitions = partitions;
        this.query = query;
        this.executionInfoEnabled = executionInfoEnabled;
        this.requireActive = requireActive;
    }

    /**
     * Specifies the name of the store to query.
     */
    public static InStore inStore(final String name) {
        return new InStore(name);
    }

    /**
     * Bounds the position of the state store against its input topics.
     */
    public StateQueryRequest<R> withPositionBound(final PositionBound positionBound) {
        return new StateQueryRequest<>(
            storeName,
            positionBound,
            partitions,
            query,
            executionInfoEnabled,
            requireActive
        );
    }


    /**
     * Specifies that the query will run against all locally available partitions.
     */
    public StateQueryRequest<R> withAllPartitions() {
        return new StateQueryRequest<>(
            storeName,
            position,
            Optional.empty(),
            query,
            executionInfoEnabled,
            requireActive
        );
    }

    /**
     * Specifies a set of partitions to run against. If some partitions are not locally available,
     * the response will contain a {@link FailureReason#NOT_PRESENT} for those partitions. If some
     * partitions in this set are not valid partitions for the store, the response will contain a
     * {@link FailureReason#DOES_NOT_EXIST} for those partitions.
     */
    public StateQueryRequest<R> withPartitions(final Set<Integer> partitions) {
        return new StateQueryRequest<>(
            storeName,
            position,
            Optional.of(Set.copyOf(partitions)),
            query,
            executionInfoEnabled,
            requireActive
        );
    }

    /**
     * Requests for stores and the Streams runtime to record any useful details about how the query
     * was executed.
     */
    public StateQueryRequest<R> enableExecutionInfo() {
        return new StateQueryRequest<>(
            storeName,
            position,
            partitions,
            query,
            true,
            requireActive
        );
    }

    /**
     * Specifies that this query should only run on partitions for which this instance is the leader
     * (aka "active"). Partitions for which this instance is not the active replica will return
     * {@link FailureReason#NOT_ACTIVE}.
     */
    public StateQueryRequest<R> requireActive() {
        return new StateQueryRequest<>(
            storeName,
            position,
            partitions,
            query,
            executionInfoEnabled,
            true
        );
    }

    /**
     * The name of the store this request is for.
     */
    public String getStoreName() {
        return storeName;
    }

    /**
     * The bound that this request places on its query, in terms of the partitions' positions
     * against its inputs.
     */
    public PositionBound getPositionBound() {
        return position;
    }

    /**
     * The query this request is meant to run.
     */
    public Query<R> getQuery() {
        return query;
    }

    /**
     * Whether this request should fetch from all locally available partitions.
     */
    public boolean isAllPartitions() {
        return !partitions.isPresent();
    }

    /**
     * If the request is for specific partitions, return the set of partitions to query.
     *
     * @throws IllegalStateException if this is a request for all partitions
     */
    public Set<Integer> getPartitions() {
        if (!partitions.isPresent()) {
            throw new IllegalStateException(
                "Cannot list partitions of an 'all partitions' request");
        } else {
            return partitions.get();
        }
    }

    /**
     * Whether the request includes detailed execution information.
     */
    public boolean executionInfoEnabled() {
        return executionInfoEnabled;
    }

    /**
     * Whether this request requires the query to execute only on active partitions.
     */
    public boolean isRequireActive() {
        return requireActive;
    }

    /**
     * A progressive builder interface for creating {@code StoreQueryRequest}s.
     */
    public static class InStore {

        private final String name;

        private InStore(final String name) {
            this.name = name;
        }

        /**
         * Specifies the query to run on the specified store.
         */
        public <R> StateQueryRequest<R> withQuery(final Query<R> query) {
            return new StateQueryRequest<>(
                name, // name is already specified
                PositionBound.unbounded(), // default: unbounded
                Optional.empty(), // default: all partitions
                query, // the query is specified
                false, // default: no execution info
                false // default: don't require active
            );
        }
    }
}
