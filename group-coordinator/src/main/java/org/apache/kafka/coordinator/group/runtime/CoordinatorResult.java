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
package org.apache.kafka.coordinator.group.runtime;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * The result of an operation applied to a state machine. The result
 * contains a list of records and a response.
 *
 * @param <T> The type of the response.
 * @param <U> The type of the record.
 */
public class CoordinatorResult<T, U> {
    /**
     * The records.
     */
    private final List<U> records;

    /**
     * The response.
     */
    private final T response;

    /**
     * The future to complete once the records are committed.
     */
    private final CompletableFuture<Void> appendFuture;

    /**
     * Constructs a Result with records and a response.
     *
     * @param records   A non-null list of records.
     * @param response  A response or null.
     */
    public CoordinatorResult(
        List<U> records,
        T response
    ) {
        this(records, response, null);
    }

    /**
     * Constructs a Result with records and an append-future.
     *
     * @param records       A non-null list of records.
     * @param appendFuture  The future to complete once the records are committed.
     */
    public CoordinatorResult(
        List<U> records,
        CompletableFuture<Void> appendFuture
    ) {
        this(records, null, appendFuture);
    }

    /**
     * Constructs a Result with records, a response, and an append-future.
     *
     * @param records       A non-null list of records.
     * @param response      A response.
     * @param appendFuture  The future to complete once the records are committed.
     */
    public CoordinatorResult(
        List<U> records,
        T response,
        CompletableFuture<Void> appendFuture
    ) {
        this.records = Objects.requireNonNull(records);
        this.response = response;
        this.appendFuture = appendFuture;
    }

    /**
     * Constructs a Result with records and a response.
     *
     * @param records   A non-null list of records.
     */
    public CoordinatorResult(
        List<U> records
    ) {
        this(records, null, null);
    }

    /**
     * @return The list of records.
     */
    public List<U> records() {
        return records;
    }

    /**
     * @return The response.
     */
    public T response() {
        return response;
    }

    /**
     * @return The append-future.
     */
    public CompletableFuture<Void> appendFuture() {
        return appendFuture;
    }

    /**
     * If the append-future exists, this means
     * that the in-memory state was already updated.
     *
     * @return Whether to replay the records.
     */
    public boolean replayRecords() {
        return appendFuture == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CoordinatorResult<?, ?> that = (CoordinatorResult<?, ?>) o;

        if (!Objects.equals(records, that.records)) return false;
        if (!Objects.equals(response, that.response)) return false;
        return Objects.equals(appendFuture, that.appendFuture);
    }

    @Override
    public int hashCode() {
        int result = records != null ? records.hashCode() : 0;
        result = 31 * result + (response != null ? response.hashCode() : 0);
        result = 31 * result + (appendFuture != null ? appendFuture.hashCode() : 0);
        return result;
    }
    @Override
    public String toString() {
        return "CoordinatorResult(records=" + records +
            ", response=" + response +
            ", appendFuture=" + appendFuture +
            ")";
    }
}
