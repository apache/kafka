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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

import static org.apache.kafka.streams.state.internals.StoreProxyUtils.getValue;
import static org.apache.kafka.streams.state.internals.StoreProxyUtils.getValueWithUnknownTimestamp;

public class KeyValueToKeyValueWithTimestampByteProxyStore implements KeyValueStore<Bytes, byte[]> {
    final KeyValueStore<Bytes, byte[]> store;

    KeyValueToKeyValueWithTimestampByteProxyStore(final KeyValueStore<Bytes, byte[]> store) {
        this.store = store;
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueWithTimestamp) {
        store.put(key, getValue(valueWithTimestamp));
    }

    @Override
    public byte[] putIfAbsent(final Bytes key,
                              final byte[] valueWithTimestamp) {
        return getValueWithUnknownTimestamp(store.putIfAbsent(key, getValue(valueWithTimestamp)));
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            final byte[] valueWithTimestamp = entry.value;
            store.put(entry.key, getValue(valueWithTimestamp));
        }
    }

    @Override
    public byte[] delete(final Bytes key) {
        return getValueWithUnknownTimestamp(store.delete(key));
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
    public byte[] get(final Bytes key) {
        return getValueWithUnknownTimestamp(store.get(key));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                 final Bytes to) {
        return new StoreProxyUtils.KeyValueIteratorProxy<>(store.range(from, to));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return new StoreProxyUtils.KeyValueIteratorProxy<>(store.all());
    }

    @Override
    public long approximateNumEntries() {
        return store.approximateNumEntries();
    }

}
