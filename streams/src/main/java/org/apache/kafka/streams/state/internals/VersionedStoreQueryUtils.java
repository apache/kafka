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

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.temporal.ChronoField;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;

public final class VersionedStoreQueryUtils {

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
            final QueryConfig config,
            final StateStore store
        );
    }

    @SuppressWarnings("rawtypes")
    private static final Map<Class, QueryHandler> QUERY_HANDLER_MAP =
        mkMap(
            mkEntry(
                VersionedKeyQuery.class,
                VersionedStoreQueryUtils::runKeyQuery
            )
        );

    // make this class uninstantiable

    private VersionedStoreQueryUtils() {
    }
    @SuppressWarnings("unchecked")
    public static <R> QueryResult<R> handleBasicQueries(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config,
        final StateStore store,
        final Position position,
        final StateStoreContext context
    ) {

        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result;

        final QueryHandler handler = QUERY_HANDLER_MAP.get(query.getClass());
        if (handler == null) {
            result = QueryResult.forUnknownQueryType(query, store);
        } else if (context == null || !isPermitted(position, positionBound, context.taskId().partition())) {
            result = QueryResult.notUpToBound(
                position,
                positionBound,
                context == null ? null : context.taskId().partition()
            );
        } else {
            result = (QueryResult<R>) handler.apply(
                query,
                positionBound,
                config,
                store
            );
        }
        if (config.isCollectExecutionInfo()) {
            result.addExecutionInfo(
                "Handled in " + store.getClass() + " in " + (System.nanoTime() - start) + "ns"
            );
        }
        result.setPosition(position);
        return result;
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
    private static <R> QueryResult<R> runKeyQuery(final Query<R> query,
                                                  final PositionBound positionBound,
                                                  final QueryConfig config,
                                                  final StateStore store) {
        if (store instanceof VersionedKeyValueStore) {
            final VersionedKeyValueStore<Bytes, byte[]> versionedKeyValueStore =
                (VersionedKeyValueStore<Bytes, byte[]>) store;
            if (query instanceof VersionedKeyQuery) {
                final VersionedKeyQuery<Bytes, byte[]> rawKeyQuery =
                    (VersionedKeyQuery<Bytes, byte[]>) query;
                try {
                    final VersionedRecord<byte[]> bytes;
                    if (((VersionedKeyQuery<?, ?>) query).asOfTimestamp().isPresent()) {
                        bytes = versionedKeyValueStore.get(rawKeyQuery.key(),
                            ((VersionedKeyQuery<?, ?>) query).asOfTimestamp().get()
                                .getLong(ChronoField.INSTANT_SECONDS));
                    } else {
                        bytes = versionedKeyValueStore.get(rawKeyQuery.key());
                    }
                    return (QueryResult<R>) QueryResult.forResult(bytes);
                } catch (final Exception e) {
                    final String message = parseStoreException(e, store, query);
                    return QueryResult.forFailure(
                        FailureReason.STORE_EXCEPTION,
                        message
                    );
                }
            } else {
                // Here is preserved for VersionedMultiTimestampedKeyQuery
                return QueryResult.forUnknownQueryType(query, store);
            }
        } else {
            return QueryResult.forUnknownQueryType(query, store);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <V> Function<byte[], ValueAndTimestamp<V>> getDeserializeValue(
        final StateSerdes<?, V> serdes) {

        final ValueAndTimestampSerde<V> valueSerde = new ValueAndTimestampSerde<>(serdes.valueSerde());
        final ValueAndTimestampDeserializer<V> deserializer =
            (ValueAndTimestampDeserializer<V>) valueSerde.deserializer();
        return byteArray -> deserializer.deserialize(serdes.topic(), byteArray);
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