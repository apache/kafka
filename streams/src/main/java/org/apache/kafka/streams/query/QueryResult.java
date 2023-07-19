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
import org.apache.kafka.streams.query.internals.FailedQueryResult;
import org.apache.kafka.streams.query.internals.SucceededQueryResult;

import java.util.List;

/**
 * Container for a single partition's result when executing a {@link StateQueryRequest}.
 *
 * @param <R> The result type of the query.
 */
public interface QueryResult<R> {
    /**
     * Static factory method to create a result object for a successful query. Used by StateStores
     * to respond to a {@link StateStore#query(Query, PositionBound, QueryConfig)}.
     */
    static <R> QueryResult<R> forResult(final R result) {
        return new SucceededQueryResult<>(result);
    }

    /**
     * Static factory method to create a result object for a failed query. Used by StateStores to
     * respond to a {@link StateStore#query(Query, PositionBound, QueryConfig)}.
     */
    static <R> QueryResult<R> forFailure(
        final FailureReason failureReason,
        final String failureMessage) {

        return new FailedQueryResult<>(failureReason, failureMessage);
    }

    /**
     * Static factory method to create a failed query result object to indicate that the store does
     * not know how to handle the query.
     * <p>
     * Used by StateStores to respond to a {@link StateStore#query(Query, PositionBound, QueryConfig)}.
     */
    static <R> QueryResult<R> forUnknownQueryType(
        final Query<R> query,
        final StateStore store) {

        return forFailure(
            FailureReason.UNKNOWN_QUERY_TYPE,
            "This store (" + store.getClass() + ") doesn't know how to execute "
                + "the given query (" + query + ")." +
                " Contact the store maintainer if you need support for a new query type.");
    }

    /**
     * Static factory method to create a failed query result object to indicate that the store has
     * not yet caught up to the requested position bound.
     * <p>
     * Used by StateStores to respond to a {@link StateStore#query(Query, PositionBound, QueryConfig)}.
     */
    static <R> QueryResult<R> notUpToBound(
        final Position currentPosition,
        final PositionBound positionBound,
        final Integer partition) {

        if (partition == null) {
            return new FailedQueryResult<>(
                FailureReason.NOT_UP_TO_BOUND,
                "The store is not initialized yet, so it is not yet up to the bound "
                    + positionBound
            );
        } else {
            return new FailedQueryResult<>(
                FailureReason.NOT_UP_TO_BOUND,
                "For store partition " + partition + ", the current position "
                    + currentPosition + " is not yet up to the bound "
                    + positionBound
            );
        }
    }

    /**
     * Used by stores to add detailed execution information (if requested) during query execution.
     */
    void addExecutionInfo(final String message);

    /**
     * Used by stores to report what exact position in the store's history it was at when it
     * executed the query.
     */
    void setPosition(final Position position);

    /**
     * True iff the query was successfully executed. The response is available in {@link
     * #getResult()}.
     */
    boolean isSuccess();


    /**
     * True iff the query execution failed. More information about the failure is available in
     * {@link #getFailureReason()} and {@link #getFailureMessage()}.
     */
    boolean isFailure();

    /**
     * If detailed execution information was requested in {@link StateQueryRequest#enableExecutionInfo()},
     * this method returned the execution details for this partition's result.
     */
    List<String> getExecutionInfo();

    /**
     * This state partition's exact position in its history when this query was executed. Can be
     * used in conjunction with subsequent queries via {@link StateQueryRequest#withPositionBound(PositionBound)}.
     * <p>
     * Note: stores are encouraged, but not required to set this property.
     */
    Position getPosition();

    /**
     * If this partition failed to execute the query, returns the reason.
     *
     * @throws IllegalArgumentException if this is not a failed result.
     */
    FailureReason getFailureReason();

    /**
     * If this partition failed to execute the query, returns the failure message.
     *
     * @throws IllegalArgumentException if this is not a failed result.
     */
    String getFailureMessage();

    /**
     * Returns the result of executing the query on one partition. The result type is determined by
     * the query. Note: queries may choose to return {@code null} for a successful query, so {@link
     * #isSuccess()} and {@link #isFailure()} must be used to determine whether the query
     * was successful of failed on this partition.
     *
     * @throws IllegalArgumentException if this is not a successful query.
     */
    R getResult();
}
