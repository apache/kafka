package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

public class SessionStoreMaterializer<K, V> {
    private final MaterializedInternal<K, V, ?> materialized;
    private final SessionWindows sessionWindow;

    public SessionStoreMaterializer(final MaterializedInternal<K, V, ?> materialized, final SessionWindows sessionWindow) {
        this.materialized = materialized;
        this.sessionWindow = sessionWindow;
    }

    /**
     * @return  window StoreBuilder
     */
    @SuppressWarnings("deprecation")
    public StoreBuilder<?> materialize() {
        SessionBytesStoreSupplier supplier = (SessionBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null) {
            final long retentionPeriod = materialized.retention() != null ? materialized.retention().toMillis() : sessionWindow.maintainMs();
            final String name = materialized.storeName();
            supplier = Stores.persistentSessionStore(name, Duration.ofMillis(retentionPeriod));
        }
        final StoreBuilder<SessionStore<K, V>> builder = Stores.sessionStoreBuilder(
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
