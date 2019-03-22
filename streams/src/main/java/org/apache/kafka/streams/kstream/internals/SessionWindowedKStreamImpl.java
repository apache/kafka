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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.streams.kstream.internals.KGroupedStreamImpl.AGGREGATE_NAME;
import static org.apache.kafka.streams.kstream.internals.KGroupedStreamImpl.REDUCE_NAME;

public class SessionWindowedKStreamImpl<K, V> extends AbstractStream<K, V> implements SessionWindowedKStream<K, V> {
    private final SessionWindows windows;
    private final GroupedStreamAggregateBuilder<K, V> aggregateBuilder;
    private final Merger<K, Long> countMerger = (aggKey, aggOne, aggTwo) -> aggOne + aggTwo;

    SessionWindowedKStreamImpl(final SessionWindows windows,
                               final InternalStreamsBuilder builder,
                               final Set<String> sourceNodes,
                               final String name,
                               final Serde<K> keySerde,
                               final Serde<V> valSerde,
                               final GroupedStreamAggregateBuilder<K, V> aggregateBuilder,
                               final StreamsGraphNode streamsGraphNode) {
        super(name, keySerde, valSerde, sourceNodes, streamsGraphNode, builder);
        Objects.requireNonNull(windows, "windows can't be null");
        this.windows = windows;
        this.aggregateBuilder = aggregateBuilder;
    }

    @Override
    public KTable<Windowed<K>, Long> count() {
        return doCount(Materialized.with(keySerde, Serdes.Long()));
    }

    @Override
    public KTable<Windowed<K>, Long> count(final Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(materialized, "materialized can't be null");

        // TODO: remove this when we do a topology-incompatible release
        // we used to burn a topology name here, so we have to keep doing it for compatibility
        if (new MaterializedInternal<>(materialized).storeName() == null) {
            builder.newStoreName(AGGREGATE_NAME);
        }

        return doCount(materialized);
    }

    private KTable<Windowed<K>, Long> doCount(final Materialized<K, Long, SessionStore<Bytes, byte[]>> materialized) {
        final MaterializedInternal<K, Long, SessionStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);
        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null) {
            materializedInternal.withValueSerde(Serdes.Long());
        }

        return aggregateBuilder.build(
            AGGREGATE_NAME,
            materialize(materializedInternal),
            new KStreamSessionWindowAggregate<>(
                windows,
                materializedInternal.storeName(),
                aggregateBuilder.countInitializer,
                aggregateBuilder.countAggregator,
                countMerger),
            materializedInternal.queryableStoreName(),
            materializedInternal.keySerde() != null ? new WindowedSerdes.SessionWindowedSerde<>(materializedInternal.keySerde()) : null,
            materializedInternal.valueSerde());
    }

    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer) {
        return reduce(reducer, Materialized.with(keySerde, valSerde));
    }

    @Override
    public KTable<Windowed<K>, V> reduce(final Reducer<V> reducer,
                                         final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(reducer, "reducer can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final Aggregator<K, V, V> reduceAggregator = aggregatorForReducer(reducer);
        final MaterializedInternal<K, V, SessionStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, REDUCE_NAME);
        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }
        if (materializedInternal.valueSerde() == null) {
            materializedInternal.withValueSerde(valSerde);
        }

        return aggregateBuilder.build(
            REDUCE_NAME,
            materialize(materializedInternal),
            new KStreamSessionWindowAggregate<>(
                windows,
                materializedInternal.storeName(),
                aggregateBuilder.reduceInitializer,
                reduceAggregator,
                mergerForAggregator(reduceAggregator)
            ),
            materializedInternal.queryableStoreName(),
            materializedInternal.keySerde() != null ? new WindowedSerdes.SessionWindowedSerde<>(materializedInternal.keySerde()) : null,
            materializedInternal.valueSerde());
    }

    @Override
    public <T> KTable<Windowed<K>, T> aggregate(final Initializer<T> initializer,
                                                final Aggregator<? super K, ? super V, T> aggregator,
                                                final Merger<? super K, T> sessionMerger) {
        return aggregate(initializer, aggregator, sessionMerger, Materialized.with(keySerde, null));
    }

    @Override
    public <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                                  final Aggregator<? super K, ? super V, VR> aggregator,
                                                  final Merger<? super K, VR> sessionMerger,
                                                  final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        Objects.requireNonNull(sessionMerger, "sessionMerger can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, SessionStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(materialized, builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }

        return aggregateBuilder.build(
            AGGREGATE_NAME,
            materialize(materializedInternal),
            new KStreamSessionWindowAggregate<>(
                windows,
                materializedInternal.storeName(),
                initializer,
                aggregator,
                sessionMerger),
            materializedInternal.queryableStoreName(),
            materializedInternal.keySerde() != null ? new WindowedSerdes.SessionWindowedSerde<>(materializedInternal.keySerde()) : null,
            materializedInternal.valueSerde());
    }

    @SuppressWarnings("deprecation") // continuing to support SessionWindows#maintainMs in fallback mode
    private <VR> StoreBuilder<SessionStore<K, VR>> materialize(final MaterializedInternal<K, VR, SessionStore<Bytes, byte[]>> materialized) {
        SessionBytesStoreSupplier supplier = (SessionBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null) {
            // NOTE: in the future, when we remove Windows#maintainMs(), we should set the default retention
            // to be (windows.inactivityGap() + windows.grace()). This will yield the same default behavior.
            final long retentionPeriod = materialized.retention() != null ? materialized.retention().toMillis() : windows.maintainMs();

            if ((windows.inactivityGap() + windows.gracePeriodMs()) > retentionPeriod) {
                throw new IllegalArgumentException("The retention period of the session store "
                                                       + materialized.storeName()
                                                       + " must be no smaller than the session inactivity gap plus the"
                                                       + " grace period."
                                                       + " Got gap=[" + windows.inactivityGap() + "],"
                                                       + " grace=[" + windows.gracePeriodMs() + "],"
                                                       + " retention=[" + retentionPeriod + "]");
            }
            supplier = Stores.persistentSessionStore(
                materialized.storeName(),
                retentionPeriod
            );
        }
        final StoreBuilder<SessionStore<K, VR>> builder = Stores.sessionStoreBuilder(
            supplier,
            materialized.keySerde(),
            materialized.valueSerde()
        );

        if (materialized.loggingEnabled()) {
            builder.withLoggingEnabled(materialized.logConfig());
        } else {
            builder.withLoggingDisabled();
        }

        if (materialized.cachingEnabled()) {
            builder.withCachingEnabled();
        }
        return builder;
    }

    private Merger<K, V> mergerForAggregator(final Aggregator<K, V, V> aggregator) {
        return (aggKey, aggOne, aggTwo) -> aggregator.apply(aggKey, aggTwo, aggOne);
    }

    private Aggregator<K, V, V> aggregatorForReducer(final Reducer<V> reducer) {
        return (aggKey, value, aggregate) -> aggregate == null ? value : reducer.apply(aggregate, value);
    }
}
