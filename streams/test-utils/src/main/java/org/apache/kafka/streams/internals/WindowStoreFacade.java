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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.ReadOnlyWindowStoreFacade;

import java.time.Instant;

public class WindowStoreFacade<K, V> extends ReadOnlyWindowStoreFacade<K, V> implements WindowStore<K, V> {

    public WindowStoreFacade(final TimestampedWindowStore<K, V> store) {
        super(store);
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        inner.init(context, root);
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        inner.init(context, root);
    }

    @Deprecated
    @Override
    public void put(final K key,
                    final V value) {
        inner.put(key, ValueAndTimestamp.make(value, ConsumerRecord.NO_TIMESTAMP));
    }

    @Override
    public void put(final K key,
                    final V value,
                    final long windowStartTimestamp) {
        inner.put(key, ValueAndTimestamp.make(value, ConsumerRecord.NO_TIMESTAMP), windowStartTimestamp);
    }

    @Override
    public WindowStoreIterator<V> backwardFetch(final K key,
                                                final long timeFrom,
                                                final long timeTo) {
        return backwardFetch(key, Instant.ofEpochMilli(timeFrom), Instant.ofEpochMilli(timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K keyFrom,
                                                          final K keyTo,
                                                          final long timeFrom,
                                                          final long timeTo) {
        return backwardFetch(keyFrom, keyTo, Instant.ofEpochMilli(timeFrom), Instant.ofEpochMilli(timeTo));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetchAll(final long timeFrom, final long timeTo) {
        return backwardFetchAll(Instant.ofEpochMilli(timeFrom), Instant.ofEpochMilli(timeTo));
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
    public String name() {
        return inner.name();
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