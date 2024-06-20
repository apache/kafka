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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The response object for interactive queries. This wraps the individual partition results, as well
 * as metadata relating to the result as a whole.
 * <p>
 *
 * @param <R> The type of the query result.
 */
@Evolving
public class StateQueryResult<R> {

    private final Map<Integer, QueryResult<R>> partitionResults = new HashMap<>();
    private QueryResult<R> globalResult = null;

    /**
     * Set the result for a global store query. Used by Kafka Streams and available for tests.
     */
    public void setGlobalResult(final QueryResult<R> r) {
        this.globalResult = r;
    }

    /**
     * Set the result for a partitioned store query. Used by Kafka Streams and available for tests.
     */
    public void addResult(final int partition, final QueryResult<R> r) {
        partitionResults.put(partition, r);
    }


    /**
     * The query's result for each partition that executed the query. Empty for global store
     * queries.
     */
    public Map<Integer, QueryResult<R>> getPartitionResults() {
        return partitionResults;
    }

    /**
     * For queries that are expected to match records in only one partition, returns the result.
     *
     * @throws IllegalArgumentException if the results are not for exactly one partition.
     */
    public QueryResult<R> getOnlyPartitionResult() {
        final List<QueryResult<R>> nonempty =
            partitionResults
                .values()
                .stream()
                .filter(QueryResult::isSuccess)
                .filter(r -> r.getResult() != null)
                .collect(Collectors.toList());

        if (nonempty.size() > 1) {
            throw new IllegalArgumentException(
                "The query did not return exactly one partition result: " + partitionResults
            );
        } else {
            return nonempty.isEmpty() ? null : nonempty.get(0);
        }
    }

    /**
     * The query's result for global store queries. Is {@code null} for non-global (partitioned)
     * store queries.
     */
    public QueryResult<R> getGlobalResult() {
        return globalResult;
    }

    /**
     * The position of the state store at the moment it executed the query. In conjunction with
     * {@link StateQueryRequest#withPositionBound}, this can be used to achieve a good balance
     * between consistency and availability in which repeated queries are guaranteed to advance in
     * time while allowing reads to be served from any replica that is caught up to that caller's
     * prior observations.
     */
    public Position getPosition() {
        if (globalResult != null) {
            return globalResult.getPosition();
        } else {
            final Position position = Position.emptyPosition();
            for (final QueryResult<R> r : partitionResults.values()) {
                position.merge(r.getPosition());
            }
            return position;
        }
    }

    @Override
    public String toString() {
        return "StateQueryResult{" +
            "partitionResults=" + partitionResults +
            ", globalResult=" + globalResult +
            '}';
    }
}
