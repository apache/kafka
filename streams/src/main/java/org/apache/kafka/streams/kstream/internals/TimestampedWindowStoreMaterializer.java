package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;

public class TimestampedWindowStoreMaterializer<K, V> {
    private final MaterializedInternal<K, V, ?> materialized;
    private final Windows<?> windows;

    public TimestampedWindowStoreMaterializer(final MaterializedInternal<K, V, ?> materialized, final Windows<?> windows) {
        this.materialized = materialized;
        this.windows = windows;
    }

    /**
     * @return  window StoreBuilder
     */
    public StoreBuilder<?> materialize() {
        WindowBytesStoreSupplier supplier = (WindowBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null) {
            final String name = materialized.storeName();

            supplier = Stores.persistentTimestampedWindowStore(name,
                    Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                    Duration.ofMillis(windows.size()),
                    true);
        }
        final StoreBuilder<TimestampedWindowStore<K, V>> builder =
                Stores.timestampedWindowStoreBuilder(supplier,
                                                     materialized.keySerde(),
                                                     materialized.valueSerde());

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
