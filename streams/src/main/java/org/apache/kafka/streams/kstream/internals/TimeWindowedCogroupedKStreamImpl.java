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
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.EnumerableWindowDefinition;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.streams.kstream.internals.DeprecatedWindowsUtils.isDeprecatedWindows;
import static org.apache.kafka.streams.kstream.internals.DeprecatedWindowsUtils.supplierFromDeprecatedWindows;

public class TimeWindowedCogroupedKStreamImpl<K, V, W extends Window> extends AbstractStream<K, V>
        implements TimeWindowedCogroupedKStream<K, V> {

    private final EnumerableWindowDefinition windows;
    private final CogroupedStreamAggregateBuilder<K, V> aggregateBuilder;
    private final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, V>> groupPatterns;

    TimeWindowedCogroupedKStreamImpl(final EnumerableWindowDefinition windows,
                                     final InternalStreamsBuilder builder,
                                     final Set<String> subTopologySourceNodes,
                                     final String name,
                                     final CogroupedStreamAggregateBuilder<K, V> aggregateBuilder,
                                     final StreamsGraphNode streamsGraphNode,
                                     final Map<KGroupedStreamImpl<K, ?>, Aggregator<? super K, ? super Object, V>> groupPatterns) {
        super(name, null, null, subTopologySourceNodes, streamsGraphNode, builder);
        //keySerde and valueSerde are null because there are many different groupStreams that they could be from
        this.windows = windows;
        this.aggregateBuilder = aggregateBuilder;
        this.groupPatterns = groupPatterns;
    }


    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer) {
        return aggregate(initializer, Materialized.with(null, null));
    }

    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized) {
        return aggregate(initializer, NamedInternal.empty(), materialized);
    }

    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Named named) {
        return aggregate(initializer, named, Materialized.with(null, null));
    }

    @Override
    public KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                            final Named named,
                                            final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(named, "named can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, WindowStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(
            materialized,
            builder,
            CogroupedKStreamImpl.AGGREGATE_NAME);
        return aggregateBuilder.build(
            groupPatterns,
            initializer,
            new NamedInternal(named),
            materialize(materializedInternal),
            materializedInternal.keySerde() != null ?
                    new FullTimeWindowedSerde<>(materializedInternal.keySerde(), start -> windows.windowsFor(start).get(start))
                    : null,
            materializedInternal.valueSerde(),
            materializedInternal.queryableStoreName(),
            windows,
            null,
            null);
    }

    @SuppressWarnings("deprecation")
    // continuing to support Windows#maintainMs/segmentInterval in fallback mode
    private StoreBuilder<TimestampedWindowStore<K, V>> materialize(
            final MaterializedInternal<K, V, WindowStore<Bytes, byte[]>> materialized) {
        WindowBytesStoreSupplier supplier = (WindowBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null) {
            if (materialized.retention() != null) {
                // new style retention: use Materialized retention and default segmentInterval
                final long retentionPeriod = materialized.retention().toMillis();

                if ((windows.maxSize() + windows.gracePeriodMs()) > retentionPeriod) {
                    throw new IllegalArgumentException("The retention period of the window store "
                                                       + name
                                                       + " must be no smaller than its window size plus the grace period."
                                                       + " Got size=[" + windows.maxSize() + "],"
                                                       + " grace=[" + windows.gracePeriodMs()
                                                       + "],"
                                                       + " retention=[" + retentionPeriod
                                                       + "]");
                }

                supplier = Stores.persistentTimestampedWindowStore(
                    materialized.storeName(),
                    Duration.ofMillis(retentionPeriod),
                    Duration.ofMillis(windows.maxSize()),
                    false
                );

            } else if (isDeprecatedWindows(windows)) {
                supplier = supplierFromDeprecatedWindows(name, windows, materialized);
            } else {
                // no retention on store or window definition, so set it to the minimum
                final long retentionPeriod = windows.maxSize() + windows.gracePeriodMs();
                supplier = Stores.persistentTimestampedWindowStore(
                    materialized.storeName(),
                    Duration.ofMillis(retentionPeriod),
                    Duration.ofMillis(windows.maxSize()),
                    false
                );
            }
        }
        final StoreBuilder<TimestampedWindowStore<K, V>> builder = Stores
            .timestampedWindowStoreBuilder(
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
