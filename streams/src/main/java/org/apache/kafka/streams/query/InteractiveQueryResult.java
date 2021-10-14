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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InteractiveQueryResult<R> {

    private final Map<Integer, QueryResult<R>> partitionResults;

    public InteractiveQueryResult(final Map<Integer, QueryResult<R>> resultMap) {
        partitionResults = resultMap;
    }

    public void setGlobalResult(final QueryResult<R> r) {

    }

    public void addResult(final int partition, final QueryResult<R> r) {
        partitionResults.put(partition, r);
    }

    public Map<Integer, QueryResult<R>> getPartitionResults() {
        return partitionResults;
    }

    public QueryResult<R> getOnlyPartitionResult() {
        final List<QueryResult<R>> nonempty =
            partitionResults
                .values()
                .stream()
                .filter(r -> r.getResult() != null)
                .collect(Collectors.toList());

        if (nonempty.size() != 1) {
            throw new IllegalStateException();
        } else {
            return nonempty.get(0);
        }
    }

    public Position getPosition() {
        Position position = Position.emptyPosition();
        for (final QueryResult<R> r : partitionResults.values()) {
            position = position.merge(r.getPosition());
        }
        return position;
    }

    @Override
    public String toString() {
        return "InteractiveQueryResult{" +
            "partitionResults=" + partitionResults +
            '}';
    }
}
