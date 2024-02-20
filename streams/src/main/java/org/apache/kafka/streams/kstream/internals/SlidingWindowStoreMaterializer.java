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

import java.time.Duration;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.DslWindowParams;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class SlidingWindowStoreMaterializer<K, V> extends MaterializedStoreFactory<K, V, WindowStore<Bytes, byte[]>> {

    private final SlidingWindows windows;
    private final EmitStrategy emitStrategy;
    private final long retentionPeriod;

    public SlidingWindowStoreMaterializer(
            final MaterializedInternal<K, V, WindowStore<Bytes, byte[]>> materialized,
            final SlidingWindows windows,
            final EmitStrategy emitStrategy
    ) {
        super(materialized);
        this.windows = windows;
        this.emitStrategy = emitStrategy;

        retentionPeriod = retentionPeriod();
        // large retention time to ensure that all existing windows needed to create new sliding windows can be accessed
        // earliest window start time we could need to create corresponding right window would be recordTime - 2 * timeDifference
        if ((windows.timeDifferenceMs() * 2 + windows.gracePeriodMs()) > retentionPeriod) {
            throw new IllegalArgumentException("The retention period of the window store "
                    + materialized.storeName()
                    + " must be no smaller than 2 * time difference plus the grace period."
                    + " Got time difference=[" + windows.timeDifferenceMs() + "],"
                    + " grace=[" + windows.gracePeriodMs() + "],"
                    + " retention=[" + retentionPeriod + "]");
        }
    }

    @Override
    public StateStore build() {
        final WindowBytesStoreSupplier supplier = materialized.storeSupplier() == null
                ? dslStoreSuppliers().windowStore(new DslWindowParams(
                        materialized.storeName(),
                        Duration.ofMillis(retentionPeriod),
                        Duration.ofMillis(windows.timeDifferenceMs()),
                        false,
                        emitStrategy,
                        true,
                        true
                ))
                : (WindowBytesStoreSupplier) materialized.storeSupplier();

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

        // do not enable cache if the emit final strategy is used
        if (materialized.cachingEnabled() && emitStrategy.type() != EmitStrategy.StrategyType.ON_WINDOW_CLOSE) {
            builder.withCachingEnabled();
        } else {
            builder.withCachingDisabled();
        }

        return builder.build();
    }

    @Override
    public final long retentionPeriod() {
        return  materialized.retention() != null
                ? materialized.retention().toMillis()
                : windows.gracePeriodMs() + 2 * windows.timeDifferenceMs();
    }

    @Override
    public long historyRetention() {
        throw new IllegalStateException(
                "historyRetention is not supported when not a versioned store");
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
