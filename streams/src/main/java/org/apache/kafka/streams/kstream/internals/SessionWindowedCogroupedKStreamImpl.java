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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SessionWindowedCogroupedKStreamImpl<K, V> extends
    AbstractStream<K, V> implements SessionWindowedCogroupedKStream<K, V> {

    private final SessionWindows sessionWindows;
    private final CogroupedStreamAggregateBuilder<K, V> aggregateBuilder;
    private final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, V>> groupPatterns;

    SessionWindowedCogroupedKStreamImpl(final SessionWindows sessionWindows,
                                        final InternalStreamsBuilder builder,
                                        final Set<String> subTopologySourceNodes,
                                        final String name,
                                        final CogroupedStreamAggregateBuilder<K, V> aggregateBuilder,
                                        final GraphNode graphNode,
                                        final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, V>> groupPatterns) {
        super(name, null, null, subTopologySourceNodes, graphNode, builder);
        //keySerde and valueSerde are null because there are many different groupStreams that they could be from
        this.sessionWindows = sessionWindows;
        this.aggregateBuilder = aggregateBuilder;
        this.groupPatterns = groupPatterns;
    }

    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Merger<? super K, V> sessionMerger) {
        return aggregate(initializer, sessionMerger, Materialized.with(null, null));
    }

    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Merger<? super K, V> sessionMerger,
                                            final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        return aggregate(initializer, sessionMerger, NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Merger<? super K, V> sessionMerger, final Named named) {
        return aggregate(initializer, sessionMerger, named, Materialized.with(null, null));
    }

    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Merger<? super K, V> sessionMerger, final Named named,
                                            final Materialized<K, V, SessionStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(sessionMerger, "sessionMerger can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        Objects.requireNonNull(named, "named can't be null");
        final MaterializedInternal<K, V, SessionStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(
            materialized,
            builder,
            CogroupedKStreamImpl.AGGREGATE_NAME);
        return aggregateBuilder.build(
            groupPatterns,
            initializer,
            new NamedInternal(named),
            materialize(materializedInternal),
            materializedInternal.keySerde() != null ?
                new WindowedSerdes.SessionWindowedSerde<>(
                    materializedInternal.keySerde()) :
                null,
            materializedInternal.valueSerde(),
            materializedInternal.queryableStoreName(),
            sessionWindows,
            sessionMerger);
    }

    private  StoreBuilder<SessionStore<K, V>> materialize(final MaterializedInternal<K, V, SessionStore<Bytes, byte[]>> materialized) {
        SessionBytesStoreSupplier supplier = (SessionBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null) {
            final long retentionPeriod = materialized.retention() != null ?
                materialized.retention().toMillis() : sessionWindows.inactivityGap() + sessionWindows.gracePeriodMs();

            if ((sessionWindows.inactivityGap() + sessionWindows.gracePeriodMs()) > retentionPeriod) {
                throw new IllegalArgumentException("The retention period of the session store "
                    + materialized.storeName()
                    + " must be no smaller than the session inactivity gap plus the"
                    + " grace period."
                    + " Got gap=[" + sessionWindows.inactivityGap() + "],"
                    + " grace=[" + sessionWindows.gracePeriodMs() + "],"
                    + " retention=[" + retentionPeriod + "]");
            }

            switch (materialized.storeType()) {
                case IN_MEMORY:
                    supplier = Stores.inMemorySessionStore(
                        materialized.storeName(),
                        Duration.ofMillis(retentionPeriod)
                    );
                    break;
                case ROCKS_DB:
                    supplier = Stores.persistentSessionStore(
                        materialized.storeName(),
                        Duration.ofMillis(retentionPeriod)
                    );
                    break;
                default:
                    throw new IllegalStateException("Unknown store type: " + materialized.storeType());
            }
        }

        final StoreBuilder<SessionStore<K, V>> builder = Stores.sessionStoreBuilder(
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

}
