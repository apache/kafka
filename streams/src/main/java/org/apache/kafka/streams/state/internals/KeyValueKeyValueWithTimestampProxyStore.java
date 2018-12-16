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

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

public class KeyValueKeyValueWithTimestampProxyStore implements KeyValueStore<Bytes, byte[]> {
    private static final byte[] UNKNOWN_TIMESTAMP_BYTE_ARRAY = new LongSerializer().serialize(null, -1L);

    final KeyValueStore<Bytes, byte[]> store;

    public KeyValueKeyValueWithTimestampProxyStore(final KeyValueStore<Bytes, byte[]> store) {
        this.store = store;
    }

    @Override
    public void put(final Bytes key, final byte[] valueWithTimestamp) {
        store.put(key, valueWithTimestamp == null ? null : getValue(valueWithTimestamp));
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] valueWithTimestamp) {
        return getValueWithUnknownTimestamp(store.putIfAbsent(key, valueWithTimestamp == null ? null : getValue(valueWithTimestamp)));
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            final byte[] valueWithTimestamp = entry.value;
            store.put(entry.key, valueWithTimestamp == null ? null : getValue(valueWithTimestamp));
        }
    }

    @Override
    public byte[] delete(final Bytes key) {
        final byte[] oldValue = store.delete(key);
        return oldValue == null ? null : getValueWithUnknownTimestamp(oldValue);
    }

    @Override
    public String name() {
        return store.name();
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
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
        final byte[] oldValue = store.get(key);
        return oldValue == null ? null : getValueWithUnknownTimestamp(oldValue);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        return new KeyValueIteratorProxy(store.range(from, to));
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return new KeyValueIteratorProxy(store.all());
    }

    @Override
    public long approximateNumEntries() {
        return store.approximateNumEntries();
    }

    private static byte[] getValue(final byte[] rawValueWithTimestamp) {
        final byte[] rawValue = new byte[rawValueWithTimestamp.length - 8];
        // TODO: should we use `ByteBuffer` instead of `System.arraycopy` ?
        System.arraycopy(rawValueWithTimestamp, 8, rawValue, 0, rawValue.length);
        return rawValue;
    }

    static byte[] getValueWithUnknownTimestamp(final byte[] rawValue) {
        final byte[] rawValueWithUnknownTimestamp = new byte[8 + rawValue.length];
        // TODO: should we use `ByteBuffer` instead of `System.arraycopy` ?
        System.arraycopy(UNKNOWN_TIMESTAMP_BYTE_ARRAY, 0, rawValueWithUnknownTimestamp, 0, 8);
        System.arraycopy(rawValue, 0, rawValueWithUnknownTimestamp, 8, rawValue.length);
        return rawValueWithUnknownTimestamp;
    }

    private static class KeyValueIteratorProxy implements KeyValueIterator<Bytes, byte[]> {
        private final KeyValueIterator<Bytes, byte[]> innerIterator;

        private KeyValueIteratorProxy(final KeyValueIterator<Bytes, byte[]> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public Bytes peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            final KeyValue<Bytes, byte[]> plainKeyValue = innerIterator.next();
            final byte[] rawValue = plainKeyValue.value;
            return KeyValue.pair(plainKeyValue.key, rawValue == null ? rawValue : getValueWithUnknownTimestamp(rawValue));
        }
    }
}
