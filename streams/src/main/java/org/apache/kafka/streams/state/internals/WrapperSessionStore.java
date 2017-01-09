package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.state.SessionStore;

/**
 * A {@link SessionStore} storage engine wrapper for utilities like logging, caching, and metering.
 */
interface WrapperSessionStore<K, V> extends WrapperStateStore, SessionStore<K, V> {

    abstract class AbstractSessionStore<K, V> extends WrapperStateStore.AbstractStateStore implements WrapperSessionStore<K, V> {
        AbstractSessionStore(SessionStore<?, ?> inner) {
            super(inner);
        }
    }
}

