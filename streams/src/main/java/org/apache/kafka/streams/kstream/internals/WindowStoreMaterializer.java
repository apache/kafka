package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

public class WindowStoreMaterializer<K, V> {
    private final MaterializedInternal<K, V, ?> materialized;
    private final Windows<?> windows;

    public WindowStoreMaterializer(final MaterializedInternal<K, V, ?> materialized, final Windows<?> windows) {
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

            supplier = Stores.persistentWindowStore(name,
                    Duration.ofMillis(windows.size() + windows.gracePeriodMs()),
                    Duration.ofMillis(windows.size()),
                    true);
        }
        final StoreBuilder<WindowStore<K, V>> builder = Stores.windowStoreBuilder(
                supplier,
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
