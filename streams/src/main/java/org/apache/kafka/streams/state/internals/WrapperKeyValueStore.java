package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.state.KeyValueStore;

/**
 * A {@link KeyValueStore} storage engine wrapper for utilities like logging, caching, and metering.
 */
interface WrapperKeyValueStore<K, V> extends WrapperStateStore, KeyValueStore<K, V> {

    abstract class AbstractKeyValueStore<K, V> extends WrapperStateStore.AbstractStateStore implements WrapperKeyValueStore<K, V> {
        private final KeyValueStore<?, ?> inner;

        AbstractKeyValueStore(KeyValueStore<?, ?> inner) {
            super(inner);

            this.inner = inner;
        }

        @Override
        public long approximateNumEntries() {
            return this.inner.approximateNumEntries();
        }
    }
}
