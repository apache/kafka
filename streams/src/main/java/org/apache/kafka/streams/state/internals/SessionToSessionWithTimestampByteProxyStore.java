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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import static org.apache.kafka.streams.state.internals.StoreProxyUtils.getValue;

class SessionToSessionWithTimestampByteProxyStore implements SessionStore<Bytes, byte[]> {
    final SessionStore<Bytes, byte[]> store;

    SessionToSessionWithTimestampByteProxyStore(final SessionStore<Bytes, byte[]> store) {
        this.store = store;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        return new StoreProxyUtils.KeyValueIteratorProxy<>(
            store.findSessions(key, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                  final Bytes keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        return new StoreProxyUtils.KeyValueIteratorProxy<>(
            store.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime));
    }

    @Override
    public void remove(final Windowed<Bytes> sessionKey) {
        store.remove(sessionKey);
    }

    @Override
    public void put(final Windowed<Bytes> sessionKey,
                    final byte[] aggregateWithTimestamp) {
        store.put(sessionKey, getValue(aggregateWithTimestamp));
    }

    @Override
    public String name() {
        return store.name();
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        store.init(context, root);
    }

    @Override
    public void flush() {
        store.flush();
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return store.isOpen();
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
        return new StoreProxyUtils.KeyValueIteratorProxy<>(store.fetch(key));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from, final Bytes to) {
        return new StoreProxyUtils.KeyValueIteratorProxy<>(store.fetch(from, to));
    }
}
