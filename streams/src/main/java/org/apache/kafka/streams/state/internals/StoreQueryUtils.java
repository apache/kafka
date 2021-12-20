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
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public final class StoreQueryUtils {

    /**
     * a utility interface to facilitate stores' query dispatch logic,
     * allowing them to generically store query execution logic as the values
     * in a map.
     */
    @FunctionalInterface
    public interface QueryHandler {
        QueryResult<?> apply(
            final Query<?> query,
            final PositionBound positionBound,
            final boolean collectExecutionInfo,
            final StateStore store
        );
    }

    @SuppressWarnings("rawtypes")
    private static final Map<Class, QueryHandler> QUERY_HANDLER_MAP =
        mkMap(
            mkEntry(
                RangeQuery.class,
                StoreQueryUtils::runRangeQuery
            ),
            mkEntry(
                KeyQuery.class,
                StoreQueryUtils::runKeyQuery
            )
        );

    // make this class uninstantiable

    private StoreQueryUtils() {
    }
    @SuppressWarnings("unchecked")
    public static <R> QueryResult<R> handleBasicQueries(
        final Query<R> query,
        final PositionBound positionBound,
        final boolean collectExecutionInfo,
        final StateStore store,
        final Position position,
        final int partition
    ) {

        final long start = collectExecutionInfo ? System.nanoTime() : -1L;
        final QueryResult<R> result;

        final QueryHandler handler = QUERY_HANDLER_MAP.get(query.getClass());
        if (handler == null) {
            result = QueryResult.forUnknownQueryType(query, store);
        } else if (!isPermitted(position, positionBound, partition)) {
            result = QueryResult.notUpToBound(position, positionBound, partition);
        } else {
            result = (QueryResult<R>) handler.apply(
                query,
                positionBound,
                collectExecutionInfo,
                store
            );
        }
        if (collectExecutionInfo) {
            result.addExecutionInfo(
                "Handled in " + store.getClass() + " in " + (System.nanoTime() - start) + "ns"
            );
        }
        result.setPosition(position);
        return result;
    }

    public static void updatePosition(
        final Position position,
        final StateStoreContext stateStoreContext) {

        if (stateStoreContext != null && stateStoreContext.recordMetadata().isPresent()) {
            final RecordMetadata meta = stateStoreContext.recordMetadata().get();
            position.withComponent(meta.topic(), meta.partition(), meta.offset());
        }
    }

    public static boolean isPermitted(
        final Position position,
        final PositionBound positionBound,
        final int partition
    ) {
        final Position bound = positionBound.position();
        for (final String topic : bound.getTopics()) {
            final Map<Integer, Long> partitionBounds = bound.getPartitionPositions(topic);
            final Map<Integer, Long> seenPartitionPositions = position.getPartitionPositions(topic);
            if (!partitionBounds.containsKey(partition)) {
                // this topic isn't bounded for our partition, so just skip over it.
            } else {
                if (!seenPartitionPositions.containsKey(partition)) {
                    // we haven't seen a partition that we have a bound for
                    return false;
                } else if (seenPartitionPositions.get(partition) < partitionBounds.get(partition)) {
                    // our current position is behind the bound
                    return false;
                }
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static <R> QueryResult<R> runRangeQuery(
        final Query<R> query,
        final PositionBound positionBound,
        final boolean collectExecutionInfo,
        final StateStore store
    ) {
        if (!(store instanceof KeyValueStore)) {
            return QueryResult.forUnknownQueryType(query, store);
        }
        final KeyValueStore<Bytes, byte[]> kvStore = (KeyValueStore<Bytes, byte[]>) store;
        final RangeQuery<Bytes, byte[]> rangeQuery = (RangeQuery<Bytes, byte[]>) query;
        final Optional<Bytes> lowerRange = rangeQuery.getLowerBound();
        final Optional<Bytes> upperRange = rangeQuery.getUpperBound();
        final KeyValueIterator<Bytes, byte[]> iterator;
        try {
            if (!lowerRange.isPresent() && !upperRange.isPresent()) {
                iterator = kvStore.all();
            } else {
                iterator = kvStore.range(lowerRange.orElse(null), upperRange.orElse(null));
            }
            final R result = (R) iterator;
            return QueryResult.forResult(result);
        } catch (final Exception e) {
            final String message = parseStoreException(e, store, query);
            return QueryResult.forFailure(
                FailureReason.STORE_EXCEPTION,
                message
            );
        }
    }

    @SuppressWarnings("unchecked")
    private static <R> QueryResult<R> runKeyQuery(final Query<R> query,
                                                  final PositionBound positionBound,
                                                  final boolean collectExecutionInfo,
                                                  final StateStore store) {
        if (store instanceof KeyValueStore) {
            final KeyQuery<Bytes, byte[]> rawKeyQuery = (KeyQuery<Bytes, byte[]>) query;
            final KeyValueStore<Bytes, byte[]> keyValueStore =
                (KeyValueStore<Bytes, byte[]>) store;
            try {
                final byte[] bytes = keyValueStore.get(rawKeyQuery.getKey());
                return (QueryResult<R>) QueryResult.forResult(bytes);
            } catch (final Exception e) {
                final String message = parseStoreException(e, store, query);
                return QueryResult.forFailure(
                    FailureReason.STORE_EXCEPTION,
                    message
                );
            }
        } else {
            return QueryResult.forUnknownQueryType(query, store);
        }
    }

    private static <R> String parseStoreException(final Exception e, final StateStore store, final Query<R> query) {
        final StringWriter stringWriter = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(stringWriter);
        printWriter.println(
            store.getClass() + " failed to handle query " + query + ":");
        e.printStackTrace(printWriter);
        printWriter.flush();
        return stringWriter.toString();
    }
}