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


import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;

import java.util.LinkedList;
import java.util.List;

/**
 * Container for a single partition's result when executing a {@link StateQueryRequest}.
 *
 * @param <R> The result type of the query.
 */
public abstract class AbstractQueryResult<R> implements QueryResult<R> {

    private List<String> executionInfo = new LinkedList<>();
    private Position position;

    public AbstractQueryResult() {

    }

    public AbstractQueryResult(final List<String> executionInfo, final Position position) {
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

}
