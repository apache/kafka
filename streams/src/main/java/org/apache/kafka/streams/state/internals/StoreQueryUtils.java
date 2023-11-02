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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

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
            final QueryConfig config,
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
            ),
            mkEntry(
                WindowKeyQuery.class,
                StoreQueryUtils::runWindowKeyQuery
            ),
            mkEntry(
                WindowRangeQuery.class,
                StoreQueryUtils::runWindowRangeQuery
            )
        );

    // make this class uninstantiable

    private StoreQueryUtils() {
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

    public static void updatePosition(
        final Position position,
        final StateStoreContext stateStoreContext) {

        if (stateStoreContext != null && stateStoreContext.recordMetadata().isPresent()) {
            final RecordMetadata meta = stateStoreContext.recordMetadata().get();
            if (meta.topic() != null) {
                position.withComponent(meta.topic(), meta.partition(), meta.offset());
            }
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
        final QueryConfig config,
        final StateStore store
    ) {
        if (!(store instanceof KeyValueStore)) {
            return QueryResult.forUnknownQueryType(query, store);
        }
        final KeyValueStore<Bytes, byte[]> kvStore = (KeyValueStore<Bytes, byte[]>) store;
        final RangeQuery<Bytes, byte[]> rangeQuery = (RangeQuery<Bytes, byte[]>) query;
        final Optional<Bytes> lowerRange = rangeQuery.getLowerBound();
        final Optional<Bytes> upperRange = rangeQuery.getUpperBound();
        final boolean isKeyAscending = rangeQuery.isKeyAscending();
        final KeyValueIterator<Bytes, byte[]> iterator;
        try {
            if (!lowerRange.isPresent() && !upperRange.isPresent() && isKeyAscending) {
                iterator = kvStore.all();
            } else if (isKeyAscending) {
                iterator = kvStore.range(lowerRange.orElse(null), upperRange.orElse(null));
            } else if (!lowerRange.isPresent() && !upperRange.isPresent()) {
                iterator = kvStore.reverseAll();
            } else {
                iterator = kvStore.reverseRange(lowerRange.orElse(null), upperRange.orElse(null));
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
                                                  final QueryConfig config,
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

    @SuppressWarnings("unchecked")
    private static <R> QueryResult<R> runWindowKeyQuery(final Query<R> query,
                                                        final PositionBound positionBound,
                                                        final QueryConfig config,
                                                        final StateStore store) {
        if (store instanceof WindowStore) {
            final WindowKeyQuery<Bytes, byte[]> windowKeyQuery =
                (WindowKeyQuery<Bytes, byte[]>) query;
            final WindowStore<Bytes, byte[]> windowStore = (WindowStore<Bytes, byte[]>) store;
            try {
                if (windowKeyQuery.getTimeFrom().isPresent() && windowKeyQuery.getTimeTo().isPresent()) {
                    final WindowStoreIterator<byte[]> iterator = windowStore.fetch(
                        windowKeyQuery.getKey(),
                        windowKeyQuery.getTimeFrom().get(),
                        windowKeyQuery.getTimeTo().get()
                    );
                    return (QueryResult<R>) QueryResult.forResult(iterator);
                } else {
                    return QueryResult.forFailure(
                        FailureReason.UNKNOWN_QUERY_TYPE,
                        "This store (" + store.getClass() + ") doesn't know how to"
                            + " execute the given query (" + query + ") because it only supports"
                            + " closed-range queries."
                            + " Contact the store maintainer if you need support"
                            + " for a new query type."
                    );
                }
            } catch (final Exception e) {
                final String message = parseStoreException(e, store, query);
                return QueryResult.forFailure(FailureReason.STORE_EXCEPTION, message);
            }
        } else {
            return QueryResult.forUnknownQueryType(query, store);
        }
    }

    @SuppressWarnings("unchecked")
    private static <R> QueryResult<R> runWindowRangeQuery(final Query<R> query,
                                                          final PositionBound positionBound,
                                                          final QueryConfig config,
                                                          final StateStore store) {
        if (store instanceof WindowStore) {
            final WindowRangeQuery<Bytes, byte[]> windowRangeQuery =
                (WindowRangeQuery<Bytes, byte[]>) query;
            final WindowStore<Bytes, byte[]> windowStore = (WindowStore<Bytes, byte[]>) store;
            try {
                // There's no store API for open time ranges
                if (windowRangeQuery.getTimeFrom().isPresent() && windowRangeQuery.getTimeTo().isPresent()) {
                    final KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
                        windowStore.fetchAll(
                            windowRangeQuery.getTimeFrom().get(),
                            windowRangeQuery.getTimeTo().get()
                        );
                    return (QueryResult<R>) QueryResult.forResult(iterator);
                } else {
                    return QueryResult.forFailure(
                        FailureReason.UNKNOWN_QUERY_TYPE,
                        "This store (" + store.getClass() + ") doesn't know how to"
                            + " execute the given query (" + query + ") because"
                            + " WindowStores only supports WindowRangeQuery.withWindowStartRange."
                            + " Contact the store maintainer if you need support"
                            + " for a new query type."
                    );
                }
            } catch (final Exception e) {
                final String message = parseStoreException(e, store, query);
                return QueryResult.forFailure(
                    FailureReason.STORE_EXCEPTION,
                    message
                );
            }
        } else if (store instanceof SessionStore) {
            final WindowRangeQuery<Bytes, byte[]> windowRangeQuery =
                (WindowRangeQuery<Bytes, byte[]>) query;
            final SessionStore<Bytes, byte[]> sessionStore = (SessionStore<Bytes, byte[]>) store;
            try {
                if (windowRangeQuery.getKey().isPresent()) {
                    final KeyValueIterator<Windowed<Bytes>, byte[]> iterator = sessionStore.fetch(
                        windowRangeQuery.getKey().get());
                    return (QueryResult<R>) QueryResult.forResult(iterator);
                } else {
                    return QueryResult.forFailure(
                        FailureReason.UNKNOWN_QUERY_TYPE,
                        "This store (" + store.getClass() + ") doesn't know how to"
                            + " execute the given query (" + query + ") because"
                            + " SessionStores only support WindowRangeQuery.withKey."
                            + " Contact the store maintainer if you need support"
                            + " for a new query type."
                    );
                }
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <V> Function<byte[], V> getDeserializeValue(final StateSerdes<?, V> serdes,
                                                              final StateStore wrapped) {
        final Serde<V> valueSerde = serdes.valueSerde();
        final boolean timestamped = WrappedStateStore.isTimestamped(wrapped);
        final Deserializer<V> deserializer;
        if (!timestamped && valueSerde instanceof ValueAndTimestampSerde) {
            final ValueAndTimestampDeserializer valueAndTimestampDeserializer =
                (ValueAndTimestampDeserializer) ((ValueAndTimestampSerde) valueSerde).deserializer();
            deserializer = (Deserializer<V>) valueAndTimestampDeserializer.valueDeserializer;
        } else {
            deserializer = valueSerde.deserializer();
        }
        return byteArray -> deserializer.deserialize(serdes.topic(), byteArray);
    }

    public static void checkpointPosition(final OffsetCheckpoint checkpointFile,
                                          final Position position) {
        try {
            checkpointFile.write(positionToTopicPartitionMap(position));
        } catch (final IOException e) {
            throw new ProcessorStateException("Error writing checkpoint file", e);
        }
    }

    public static Position readPositionFromCheckpoint(final OffsetCheckpoint checkpointFile) {
        try {
            return topicPartitionMapToPosition(checkpointFile.read());
        } catch (final IOException e) {
            throw new ProcessorStateException("Error reading checkpoint file", e);
        }
    }

    private static Map<TopicPartition, Long> positionToTopicPartitionMap(final Position position) {
        final Map<TopicPartition, Long> topicPartitions = new HashMap<>();
        final Set<String> topics = position.getTopics();
        for (final String t : topics) {
            final Map<Integer, Long> partitions = position.getPartitionPositions(t);
            for (final Entry<Integer, Long> e : partitions.entrySet()) {
                final TopicPartition tp = new TopicPartition(t, e.getKey());
                topicPartitions.put(tp, e.getValue());
            }
        }
        return topicPartitions;
    }

    private static Position topicPartitionMapToPosition(final Map<TopicPartition, Long> topicPartitions) {
        final Map<String, Map<Integer, Long>> pos = new HashMap<>();
        for (final Entry<TopicPartition, Long> e : topicPartitions.entrySet()) {
            pos
                .computeIfAbsent(e.getKey().topic(), t -> new HashMap<>())
                .put(e.getKey().partition(), e.getValue());
        }
        return Position.fromMap(pos);
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