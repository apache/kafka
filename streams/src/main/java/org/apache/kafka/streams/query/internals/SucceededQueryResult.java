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
package org.apache.kafka.streams.query.internals;


import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.internals.InternalQueryResultUtil;

import java.util.LinkedList;
import java.util.List;

/**
 * Container for a single partition's result when executing a {@link StateQueryRequest}.
 *
 * @param <R> The result type of the query.
 */
public final class SucceededQueryResult<R> implements QueryResult<R> {

    private final R result;
    private List<String> executionInfo = new LinkedList<>();
    private Position position;

    public SucceededQueryResult(final R result) {
        this.result = result;
    }

    /**
     * Special constructor used in {@link InternalQueryResultUtil}.
     */
    SucceededQueryResult(final R result,
                         final List<String> executionInfo,
                         final Position position) {
        this.result = result;
        this.executionInfo = executionInfo;
        this.position = position;
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
        return true;
    }


    /**
     * True iff the query execution failed. More information about the failure is available in
     * {@link this#getFailureReason()} and {@link this#getFailureMessage()}.
     */
    public boolean isFailure() {
        return false;
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
        throw new IllegalArgumentException(
            "Cannot get failure reason because this query did not fail."
        );
    }

    /**
     * If this partition failed to execute the query, returns the failure message.
     *
     * @throws IllegalArgumentException if this is not a failed result.
     */
    public String getFailureMessage() {
        throw new IllegalArgumentException(
            "Cannot get failure message because this query did not fail."
        );
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
        return result;
    }

    @Override
    public String toString() {
        return "QueryResult{" +
            "executionInfo=" + executionInfo +
            ", result=" + result +
            ", position=" + position +
            '}';
    }
}
