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
package org.apache.kafka.streams.internals;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.SessionWithTimestampStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.KeyValueIteratorFacade;
import org.apache.kafka.streams.state.internals.ReadOnlySessionStoreFacade;

public class SessionStoreFacade<K, V> extends ReadOnlySessionStoreFacade<K, V> implements SessionStore<K, V> {

    public SessionStoreFacade(final SessionWithTimestampStore<K, V> store) {
        super(store);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final K key,
                                                         final long earliestSessionEndTime,
                                                         final long latestSessionStartTime) {
        final KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator
            = inner.findSessions(key, earliestSessionEndTime, latestSessionStartTime);
        return new KeyValueIteratorFacade<>(innerIterator);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final K keyFrom,
                                                         final K keyTo,
                                                         final long earliestSessionEndTime,
                                                         final long latestSessionStartTime) {
        final KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator
            = inner.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        return new KeyValueIteratorFacade<>(innerIterator);
    }

    @Override
    public void remove(final Windowed<K> sessionKey) {
        inner.remove(sessionKey);
    }

    @Override
    public void put(final Windowed<K> sessionKey,
                    final V aggregate) {
        inner.put(sessionKey, aggregate, -1L);
    }

    @Override
    public String name() {
        return inner.name();
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        inner.init(context, root);
    }

    @Override
    public void flush() {
        inner.flush();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public boolean persistent() {
        return inner.persistent();
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }
}
