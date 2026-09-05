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
import org.apache.kafka.streams.DslStoreFormat;
import org.apache.kafka.streams.kstream.Range;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

/**
 * Materializes the buffer {@link WindowStore} for a {@link org.apache.kafka.streams.kstream.RangedKStream}.
 * Always uses {@code retainDuplicates=true} so that multiple records at the same timestamp per key
 * are preserved in the store.
 */
public class RangeWindowStoreMaterializer<K, V> extends MaterializedStoreFactory<K, V, WindowStore<Bytes, byte[]>> {

    private final long retentionPeriodMs;

    public RangeWindowStoreMaterializer(
        final MaterializedInternal<K, V, WindowStore<Bytes, byte[]>> materialized,
        final Range<?, ?> range
    ) {
        super(materialized, DslStoreFormat.PLAIN);

        if (materialized.storeSupplier() instanceof WindowBytesStoreSupplier) {
            final WindowBytesStoreSupplier userSupplier = (WindowBytesStoreSupplier) materialized.storeSupplier();
            if (!userSupplier.retainDuplicates()) {
                throw new IllegalArgumentException(
                    "A custom WindowBytesStoreSupplier used with rangeOver() must have retainDuplicates=true");
            }
        }

        this.retentionPeriodMs = retentionPeriod(materialized, range);
    }

    private static long retentionPeriod(
        final MaterializedInternal<?, ?, ?> materialized,
        final Range<?, ?> range
    ) {
        final long configured = materialized.retention() != null
            ? materialized.retention().toMillis()
            : range.retentionMs();

        if (configured < range.retentionMs()) {
            throw new IllegalArgumentException(
                "The retention period of the buffer store must be at least Range.retentionMs()="
                    + range.retentionMs() + "ms, but got " + configured + "ms");
        }

        return configured;
    }

    @Override
    public StoreBuilder<?> builder() {
        final WindowBytesStoreSupplier supplier;
        if (materialized.storeSupplier() != null) {
            supplier = (WindowBytesStoreSupplier) materialized.storeSupplier();
        } else {
            supplier = Stores.persistentWindowStore(
                materialized.storeName(),
                Duration.ofMillis(retentionPeriodMs),
                Duration.ofMillis(1L),
                true
            );
        }

        final StoreBuilder<WindowStore<K, V>> builder = Stores.windowStoreBuilder(
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

    @Override
    public long retentionPeriod() {
        return retentionPeriodMs;
    }

    @Override
    public long historyRetention() {
        throw new IllegalStateException("historyRetention is not supported for range stores");
    }

    @Override
    public boolean isWindowStore() {
        return true;
    }

    @Override
    public boolean isVersionedStore() {
        return false;
    }
}
