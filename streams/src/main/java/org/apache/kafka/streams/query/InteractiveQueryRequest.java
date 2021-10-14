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

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * @param <R>
 */
public class InteractiveQueryRequest<R> {

    private final String storeName;
    private final PositionBound position;
    private final Optional<Set<Integer>> partitions;
    private final Query<R> query;
    private boolean executionInfoEnabled;
    private boolean requireActive;

    private InteractiveQueryRequest(
        final String storeName,
        final PositionBound position,
        final Optional<Set<Integer>> partitions,
        final Query<R> query,
        final boolean executionInfoEnabled, final boolean requireActive) {

        this.storeName = storeName;
        this.position = position;
        this.partitions = partitions;
        this.query = query;
        this.executionInfoEnabled = executionInfoEnabled;
        this.requireActive = requireActive;
    }

    public static InStore inStore(final String name) {
        return new InStore(name);
    }

    public InteractiveQueryRequest<R> withPositionBound(final PositionBound positionBound) {
        return new InteractiveQueryRequest<>(
            storeName,
            positionBound,
            partitions,
            query,
            executionInfoEnabled,
            false);
    }


    public InteractiveQueryRequest<R> withNoPartitions() {
        return new InteractiveQueryRequest<>(storeName,
            position,
            Optional.of(Collections.emptySet()),
            query,
            executionInfoEnabled,
            requireActive);
    }

    public InteractiveQueryRequest<R> withAllPartitions() {
        return new InteractiveQueryRequest<>(storeName,
            position,
            Optional.empty(),
            query,
            executionInfoEnabled,
            requireActive);
    }

    public InteractiveQueryRequest<R> withPartitions(final Set<Integer> partitions) {
        return new InteractiveQueryRequest<>(storeName,
            position,
            Optional.of(Collections.unmodifiableSet(new HashSet<>(partitions))),
            query,
            executionInfoEnabled,
            requireActive);
    }

    public String getStoreName() {
        return storeName;
    }

    public PositionBound getPositionBound() {
        if (requireActive) {
            throw new IllegalArgumentException();
        }
        return Objects.requireNonNull(position);
    }

    public Query<R> getQuery() {
        return query;
    }

    public boolean isAllPartitions() {
        return !partitions.isPresent();
    }

    public Set<Integer> getPartitions() {
        if (!partitions.isPresent()) {
            throw new UnsupportedOperationException(
                "Cannot list partitions of an 'all partitions' request");
        } else {
            return partitions.get();
        }
    }

    public InteractiveQueryRequest<R> enableExecutionInfo() {
        return new InteractiveQueryRequest<>(storeName,
            position,
            partitions,
            query,
            true,
            requireActive);
    }

    public boolean executionInfoEnabled() {
        return executionInfoEnabled;
    }

    public boolean isRequireActive() {
        return requireActive;
    }

    public static class InStore {

        private final String name;

        private InStore(final String name) {
            this.name = name;
        }

        public <R> InteractiveQueryRequest<R> withQuery(final Query<R> query) {
            return new InteractiveQueryRequest<>(
                name,
                null,
                Optional.empty(),
                query,
                false,
                true);
        }
    }
}
