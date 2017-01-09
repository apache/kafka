package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;

abstract class AbstractKeyValueIterator<K, V> implements KeyValueIterator<K, V> {
    private final String name;
    private volatile boolean open;

    AbstractKeyValueIterator(final String name) {
        this.name = name;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported.");
    }

    @Override
    public synchronized void close() {
        open = false;
    }

    void validateIsOpen() {
        if (!open) {
            throw new InvalidStateStoreException(String.format("Iterator %s has closed", name));
        }
    }
}
