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


import org.apache.kafka.streams.processor.StateStore;

import java.util.LinkedList;
import java.util.List;

/**
 * Container for a single partition's result when executing a {@link StateQueryRequest}.
 *
 * @param <R> The result type of the query.
 */
public final class QueryResult<R> {

    private final List<String> executionInfo = new LinkedList<>();
    private final FailureReason failureReason;
    private final String failure;
    private final R result;
    private Position position;

    private QueryResult(final R result) {
        this.result = result;
        this.failureReason = null;
        this.failure = null;
    }

    private QueryResult(final FailureReason failureReason, final String failure) {
        this.result = null;
        this.failureReason = failureReason;
        this.failure = failure;
    }

    /**
     * Static factory method to create a result object for a successful query. Used by StateStores
     * to respond to a {@link StateStore#query(Query, PositionBound, boolean)}.
     */
    public static <R> QueryResult<R> forResult(final R result) {
        return new QueryResult<>(result);
    }

    /**
     * Static factory method to create a result object for a failed query. Used by StateStores to
     * respond to a {@link StateStore#query(Query, PositionBound, boolean)}.
     */
    public static <R> QueryResult<R> forFailure(
        final FailureReason failureReason,
        final String failureMessage) {

        return new QueryResult<>(failureReason, failureMessage);
    }

    /**
     * Static factory method to create a failed query result object to indicate that the store does
     * not know how to handle the query.
     * <p>
     * Used by StateStores to respond to a {@link StateStore#query(Query, PositionBound, boolean)}.
     */
    public static <R> QueryResult<R> forUnknownQueryType(
        final Query<R> query,
        final StateStore store) {

        return new QueryResult<>(
            FailureReason.UNKNOWN_QUERY_TYPE,
            "This store (" + store.getClass() + ") doesn't know how to execute "
                + "the given query (" + query + ")." +
                " Contact the store maintainer if you need support for a new query type.");
    }

    /**
     * Static factory method to create a failed query result object to indicate that the store has
     * not yet caught up to the requested position bound.
     * <p>
     * Used by StateStores to respond to a {@link StateStore#query(Query, PositionBound, boolean)}.
     */
    public static <R> QueryResult<R> notUpToBound(
        final Position currentPosition,
        final PositionBound positionBound,
        final int partition) {

        return new QueryResult<>(
            FailureReason.NOT_UP_TO_BOUND,
            "For store partition " + partition + ", the current position "
                + currentPosition + " is not yet up to the bound "
                + positionBound
        );
    }

    /**
     * Used by stores to add detailed execution information (if requested) during query execution.
     */
    public void addExecutionInfo(final String message) {
        executionInfo.add(message);
    }

    /**
     * Used by stores to report what exact position in the store's history it was at when it
     * executed the query.
     */
    public void setPosition(final Position position) {
        this.position = position;
    }

    /**
     * True iff the query was successfully executed. The response is available in {@link
     * this#getResult()}.
     */
    public boolean isSuccess() {
        return failureReason == null;
    }


    /**
     * True iff the query execution failed. More information about the failure is available in
     * {@link this#getFailureReason()} and {@link this#getFailureMessage()}.
     */
    public boolean isFailure() {
        return failureReason != null;
    }

    /**
     * If detailed execution information was requested in {@link StateQueryRequest#enableExecutionInfo()},
     * this method returned the execution details for this partition's result.
     */
    public List<String> getExecutionInfo() {
        return executionInfo;
    }

    /**
     * This state partition's exact position in its history when this query was executed. Can be
     * used in conjunction with subsequent queries via {@link StateQueryRequest#withPositionBound(PositionBound)}.
     * <p>
     * Note: stores are encouraged, but not required to set this property.
     */
    public Position getPosition() {
        return position;
    }

    /**
     * If this partition failed to execute the query, returns the reason.
     *
     * @throws IllegalArgumentException if this is not a failed result.
     */
    public FailureReason getFailureReason() {
        if (!isFailure()) {
            throw new IllegalArgumentException(
                "Cannot get failure reason because this query did not fail."
            );
        }
        return failureReason;
    }

    /**
     * If this partition failed to execute the query, returns the failure message.
     *
     * @throws IllegalArgumentException if this is not a failed result.
     */
    public String getFailureMessage() {
        if (!isFailure()) {
            throw new IllegalArgumentException(
                "Cannot get failure message because this query did not fail."
            );
        }
        return failure;
    }

    /**
     * Returns the result of executing the query on one partition. The result type is determined by
     * the query. Note: queries may choose to return {@code null} for a successful query, so {@link
     * this#isSuccess()} and {@link this#isFailure()} must be used to determine whether the query
     * was successful of failed on this partition.
     *
     * @throws IllegalArgumentException if this is not a successful query.
     */
    public R getResult() {
        if (!isSuccess()) {
            throw new IllegalArgumentException(
                "Cannot get result for failed query. Failure is " + failureReason.name() + ": "
                    + failure);
        }
        return result;
    }

    @Override
    public String toString() {
        return "QueryResult{" +
            "executionInfo=" + executionInfo +
            ", failureReason=" + failureReason +
            ", failure='" + failure + '\'' +
            ", result=" + result +
            ", position=" + position +
            '}';
    }
}
