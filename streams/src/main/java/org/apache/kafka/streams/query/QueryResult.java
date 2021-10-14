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
import org.apache.kafka.streams.processor.internals.StreamThread.State;

import java.util.LinkedList;
import java.util.List;

public final class QueryResult<R> {

    private final List<String> executionInfo = new LinkedList<>();
    private final FailureReason failureReason;
    private final String failure;
    private final R result;
    private Position boundUpdate;

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

    public static <R> QueryResult<R> forResult(final R result) {
        return new QueryResult<>(result);
    }

    public static <R> QueryResult<R> forUnknownQueryType(
        final Query<R> query,
        final StateStore store) {

        return new QueryResult<>(
            FailureReason.UNKNOWN_QUERY_TYPE,
            "This store (" + store.getClass() + ") doesn't know how to execute "
                + "the given query (" + query + ")." +
                " Contact the store maintainer if you need support for a new query type.");
    }

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

    public static <R> QueryResult<R> notActive(
        final State state,
        final boolean active,
        final int partition) {
        return new QueryResult<>(
            FailureReason.NOT_UP_TO_BOUND,
            "Query requires a running active task,"
                + " but partition " + partition + " was in state " + state + " and was "
                + (active ? "active" : "not active") + "."
        );
    }


    public <NewR> QueryResult<NewR> swapResult(final NewR typedResult) {
        final QueryResult<NewR> queryResult = new QueryResult<>(typedResult);
        queryResult.executionInfo.addAll(executionInfo);
        queryResult.boundUpdate = boundUpdate;
        return queryResult;
    }

    public void addExecutionInfo(final String s) {
        executionInfo.add(s);
    }

    public void throwIfFailure() {
        if (isFailure()) {
            throw new RuntimeException(failureReason.name() + ": " + failure);
        }
    }

    public boolean isSuccess() {
        return failureReason == null;
    }

    public boolean isFailure() {
        return failureReason != null;
    }

    public List<String> getExecutionInfo() {
        return executionInfo;
    }

    public FailureReason getFailureReason() {
        return failureReason;
    }

    public String getFailure() {
        return failure;
    }

    public R getResult() {
        if (result == null) {
            throwIfFailure();
        }
        // will return `null` if there's not a failure recorded.
        return result;
    }

    public void setPosition(final Position boundUpdate) {
        this.boundUpdate = boundUpdate;
    }

    public Position getPosition() {
        return boundUpdate;
    }

    @Override
    public String toString() {
        return "QueryResult{" +
            "executionInfo=" + executionInfo +
            ", failureReason=" + failureReason +
            ", failure='" + failure + '\'' +
            ", result=" + result +
            ", boundUpdate=" + boundUpdate +
            '}';
    }
}
