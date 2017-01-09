package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.state.WindowStore;

/**
 * A {@link WindowStore} storage engine wrapper for utilities like logging, caching, and metering.
 */
interface WrapperWindowStore<K, V> extends WrapperStateStore, WindowStore<K, V> {

    abstract class AbstractWindowStore<K, V> extends WrapperStateStore.AbstractStateStore implements WrapperWindowStore<K, V> {
        AbstractWindowStore(WindowStore<?, ?> inner) {
            super(inner);
        }
    }
}
