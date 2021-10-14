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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RawKeyQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;

public final class StoreQueryUtils {

    // make this class uninstantiable
    private StoreQueryUtils() {
    }

    public static <R> QueryResult<R> requireKVQuery(
        final Query<R> query,
        final KeyValueStore<Bytes, byte[]> kvStore,
        final boolean enableExecutionInfo) {
        final QueryResult<R> r = StoreQueryUtils.handleKVQuery(query, kvStore, enableExecutionInfo);
        r.throwIfFailure();
        return r;
    }

    public static <R> QueryResult<R> handleKVQuery(
        final Query<R> query,
        final KeyValueStore<Bytes, byte[]> kvStore,
        final boolean enableExecutionInfo) {

        final long start = System.nanoTime();
        final String name = query.getClass().getCanonicalName();
        switch (name) {
            case "org.apache.kafka.streams.query.RawKeyQuery": {
                final RawKeyQuery keyQuery = (RawKeyQuery) query;
                return handleRawKeyQuery(kvStore, enableExecutionInfo, start, keyQuery);
            }
            case "org.apache.kafka.streams.query.RawScanQuery": {
                final KeyValueIterator<Bytes, byte[]> iterator = kvStore.all();
                @SuppressWarnings("unchecked") final R result = (R) iterator;
                final long end = System.nanoTime();
                final QueryResult<R> queryResult = QueryResult.forResult(result);
                if (enableExecutionInfo) {
                    queryResult.addExecutionInfo("Handled on " + kvStore.getClass().getName()
                        + "#all via StoreQueryAdapters" + " in " + (end - start) + "ns");
                }
                return queryResult;
            }
            default:
                return QueryResult.forUnknownQueryType(query, kvStore);
        }
    }

    public static <R> QueryResult<R> handleRawKeyQuery(
        final KeyValueStore<Bytes, byte[]> kvStore,
        final boolean enableExecutionInfo,
        final long start,
        final RawKeyQuery keyQuery) {

        final Bytes key = keyQuery.getKey();
        final byte[] value = kvStore.get(key);
        @SuppressWarnings("unchecked") final R result = (R) value;
        final long end = System.nanoTime();

        final QueryResult<R> queryResult = QueryResult.forResult(result);
        if (enableExecutionInfo) {
            queryResult.addExecutionInfo("Handled on " + kvStore.getClass().getName()
                + "#get via StoreQueryAdapters" + " in " + (end - start) + "ns");
        }
        return queryResult;
    }

    public static boolean isPermitted(
        final Map<String, Map<Integer, Long>> seenOffsets,
        final PositionBound positionBound,
        final int partition) {
        if (positionBound.isUnbounded()) {
            return true;
        } else {
            final Position position = positionBound.position();
            for (final String topic : position.getTopics()) {
                final Map<Integer, Long> partitionBounds = position.getBound(topic);
                final Map<Integer, Long> seenPartitionBounds = seenOffsets.get(topic);
                if (!partitionBounds.containsKey(partition)) {
                    // this topic isn't bounded for our partition, so just skip over it.
                } else {
                    if (seenPartitionBounds == null) {
                        // we haven't seen a topic that is bounded for our partition
                        return false;
                    } else if (!seenPartitionBounds.containsKey(partition)) {
                        // we haven't seen a partition that we have a bound for
                        return false;
                    } else if (seenPartitionBounds.get(partition) < partitionBounds.get(
                        partition)) {
                        // our current position is behind the bound
                        return false;
                    }
                }
            }
            return true;
        }
    }
}
