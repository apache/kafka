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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import java.util.NoSuchElementException;

/**
 * A wrapper for session stores that abstracts whether the underlying store supports headers.
 * When the store is headers-aware, it delegates to a {@code SessionStore<K, AggregationWithHeaders<VAgg>>};
 * otherwise it delegates to a plain {@code SessionStore<K, VAgg>}.
 *
 * @param <K>    The key type
 * @param <VAgg> The aggregated value type
 */
@SuppressWarnings("unchecked")
public class SessionStoreWrapper<K, VAgg> {

    private final SessionStore<K, ?> store;
    private final boolean supportsHeaders;

    public SessionStoreWrapper(final ProcessorContext<?, ?> context, final String storeName) {
        this.store = context.getStateStore(storeName);
        this.supportsHeaders = WrappedStateStore.isHeadersAware(store);
    }

    public boolean supportsHeaders() {
        return supportsHeaders;
    }

    public StateStore store() {
        return store;
    }

    /**
     * Find sessions for the given key within the time range, returning unwrapped values.
     */
    public KeyValueIterator<Windowed<K>, VAgg> findSessions(final K key,
                                                             final long earliestSessionEndTime,
                                                             final long latestSessionStartTime) {
        if (supportsHeaders) {
            return unwrapIterator(headersStore().findSessions(key, earliestSessionEndTime, latestSessionStartTime));
        }
        return plainStore().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
    }

    /**
     * Find sessions within the time range (no key filter), returning unwrapped values.
     */
    public KeyValueIterator<Windowed<K>, VAgg> findSessions(final long earliestSessionEndTime,
                                                             final long latestSessionEndTime) {
        if (supportsHeaders) {
            return unwrapIterator(headersStore().findSessions(earliestSessionEndTime, latestSessionEndTime));
        }
        return plainStore().findSessions(earliestSessionEndTime, latestSessionEndTime);
    }

    /**
     * Put a value without headers.
     */
    public void put(final Windowed<K> sessionKey, final VAgg aggregate) {
        if (supportsHeaders) {
            headersStore().put(sessionKey, AggregationWithHeaders.make(aggregate, null));
        } else {
            plainStore().put(sessionKey, aggregate);
        }
    }

    /**
     * Put a value with headers. If the store does not support headers, the headers are ignored.
     */
    public void put(final Windowed<K> sessionKey, final VAgg aggregate, final Headers headers) {
        if (supportsHeaders) {
            headersStore().put(sessionKey, AggregationWithHeaders.make(aggregate, headers));
        } else {
            plainStore().put(sessionKey, aggregate);
        }
    }

    /**
     * Remove the session for the given key.
     */
    public void remove(final Windowed<K> sessionKey) {
        if (supportsHeaders) {
            headersStore().remove(sessionKey);
        } else {
            plainStore().remove(sessionKey);
        }
    }

    private SessionStore<K, VAgg> plainStore() {
        return (SessionStore<K, VAgg>) store;
    }

    private SessionStore<K, AggregationWithHeaders<VAgg>> headersStore() {
        return (SessionStore<K, AggregationWithHeaders<VAgg>>) store;
    }

    /**
     * Wraps an iterator of {@code AggregationWithHeaders<VAgg>} to return unwrapped {@code VAgg} values.
     */
    private KeyValueIterator<Windowed<K>, VAgg> unwrapIterator(
            final KeyValueIterator<Windowed<K>, AggregationWithHeaders<VAgg>> inner) {
        return new KeyValueIterator<>() {
            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public KeyValue<Windowed<K>, VAgg> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final KeyValue<Windowed<K>, AggregationWithHeaders<VAgg>> next = inner.next();
                return new KeyValue<>(next.key, AggregationWithHeaders.getAggregationOrNull(next.value));
            }

            @Override
            public void close() {
                inner.close();
            }

            @Override
            public Windowed<K> peekNextKey() {
                return inner.peekNextKey();
            }
        };
    }
}
