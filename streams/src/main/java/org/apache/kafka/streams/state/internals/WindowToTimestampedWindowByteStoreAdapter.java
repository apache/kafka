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
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;

import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;
import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.rawValue;

class WindowToTimestampedWindowByteStoreAdapter implements WindowStore<Bytes, byte[]> {
    final WindowStore<Bytes, byte[]> store;

    WindowToTimestampedWindowByteStoreAdapter(final WindowStore<Bytes, byte[]> store) {
        if (!store.persistent()) {
            throw new IllegalArgumentException("Provided store must be a persistent store, but it is not.");
        }
        this.store = store;
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueWithTimestamp) {
        store.put(key, valueWithTimestamp == null ? null : rawValue(valueWithTimestamp));
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueWithTimestamp,
                    final long windowStartTimestamp) {
        store.put(key, valueWithTimestamp == null ? null : rawValue(valueWithTimestamp), windowStartTimestamp);
    }

    @Override
    public byte[] fetch(final Bytes key,
                        final long time) {
        return convertToTimestampedFormat(store.fetch(key, time));
    }

    @Override
    @SuppressWarnings("deprecation")
    public WindowStoreIterator<byte[]> fetch(final Bytes key,
                                             final long timeFrom,
                                             final long timeTo) {
        return new WindowToTimestampedWindowIteratorAdapter(store.fetch(key, timeFrom, timeTo));
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key,
                                             final Instant from,
                                             final Instant to) {
        return new WindowToTimestampedWindowIteratorAdapter(store.fetch(key, from, to));
    }

    @Override
    @SuppressWarnings("deprecation")
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from,
                                                           final Bytes to,
                                                           final long timeFrom,
                                                           final long timeTo) {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetch(from, to, timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from,
                                                           final Bytes to,
                                                           final Instant fromTime,
                                                           final Instant toTime) {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetch(from, to, fromTime, toTime));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.all());
    }

    @Override
    @SuppressWarnings("deprecation")
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom,
                                                              final long timeTo) {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetchAll(timeFrom, timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final Instant from,
                                                              final Instant to) {
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.fetchAll(from, to));
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


    private static class WindowToTimestampedWindowIteratorAdapter
        extends KeyValueToTimestampedKeyValueIteratorAdapter<Long>
        implements WindowStoreIterator<byte[]> {

        WindowToTimestampedWindowIteratorAdapter(final KeyValueIterator<Long, byte[]> innerIterator) {
            super(innerIterator);
        }
    }

}