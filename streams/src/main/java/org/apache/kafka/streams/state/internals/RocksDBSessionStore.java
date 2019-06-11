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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

import java.util.List;


public class RocksDBSessionStore
    extends WrappedStateStore<SegmentedBytesStore, Object, Object>
    implements SessionStore<Bytes, byte[]> {

    RocksDBSessionStore(final SegmentedBytesStore bytesStore) {
        super(bytesStore);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(
            key,
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator(bytesIterator);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                  final Bytes keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(
            keyFrom,
            keyTo,
            earliestSessionEndTime,
            latestSessionStartTime
        );
        return new WrappedSessionStoreIterator(bytesIterator);
    }

    @Override
    public byte[] fetchSession(final Bytes key, final long startTime, final long endTime) {
        return wrapped().get(SessionKeySchema.toBinary(key, startTime, endTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from, final Bytes to) {
        return findSessions(from, to, 0, Long.MAX_VALUE);
    }

    @Override
    public void remove(final Windowed<Bytes> key) {
        wrapped().remove(SessionKeySchema.toBinary(key));
    }

    @Override
    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        wrapped().put(SessionKeySchema.toBinary(sessionKey), aggregate);
    }

    @Override
    public byte[] putIfAbsent(Windowed<Bytes> key, byte[] value) {
        return new byte[0];
    }

    @Override
    public void putAll(List<KeyValue<Windowed<Bytes>, byte[]>> entries) {

    }

    @Override
    public byte[] delete(Windowed<Bytes> key) {
        return new byte[0];
    }

    @Override
    public byte[] get(Windowed<Bytes> key) {
        return new byte[0];
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> range(Windowed<Bytes> from, Windowed<Bytes> to) {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return null;
    }

    @Override
    public long approximateNumEntries() {
        return 0;
    }
}
