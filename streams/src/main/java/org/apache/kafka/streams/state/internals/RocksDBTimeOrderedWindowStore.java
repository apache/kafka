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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;

/**
 * A persistent (time-key)-value store based on RocksDB.
 *
 * The store uses the {@link TimeOrderedKeySchema} to serialize the record key bytes to generate the
 * combined (time-key) store key. This key schema is efficient when doing time range queries in
 * the store (i.e. fetchAll(from, to) ).
 *
 * For key range queries, like fetch(key, fromTime, toTime), use the {@link RocksDBWindowStore}
 * which uses the {@link WindowKeySchema} to serialize the record bytes for efficient key queries.
 */
@SuppressWarnings("unchecked")
public class RocksDBTimeOrderedWindowStore
    extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, Bytes, byte[]>
    implements KeyValueStore<Bytes, byte[]> {

    static private final Serde<List<byte[]>> LIST_SERDE = Serdes.ListSerde(ArrayList.class, Serdes.ByteArray());

    RocksDBTimeOrderedWindowStore(final KeyValueStore<Bytes, byte[]> bytesStore) {
        super(bytesStore);
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        // if the value is null we can skip the get and blind delete
        if (value == null) {
            wrapped().put(key, null);
        } else {
            final byte[] oldValue = wrapped().get(key);

            if (oldValue == null) {
                wrapped().put(key, LIST_SERDE.serializer().serialize(null, Collections.singletonList(value)));
            } else {
                final List<byte[]> list = LIST_SERDE.deserializer().deserialize(null, oldValue);
                list.add(value);

                wrapped().put(key, LIST_SERDE.serializer().serialize(null, list));
            }
        }
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] value) {
        throw new UnsupportedOperationException("putIfAbsent not supported");
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        throw new UnsupportedOperationException("putAll not supported");
    }

    @Override
    public byte[] delete(final Bytes key) {
        final byte[] oldValue = wrapped().get(key);
        put(key, null);
        return oldValue;
    }

    @Override
    public byte[] get(final Bytes key) {
        return wrapped().get(key);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        throw new UnsupportedOperationException("range not supported");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return new WrappedStoreIterator(wrapped().all());
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    private static class WrappedStoreIterator extends AbstractIterator<KeyValue<Bytes, byte[]>>
        implements KeyValueIterator<Bytes, byte[]> {

        private final KeyValueIterator<Bytes, byte[]> bytesIterator;
        private final List<byte[]> currList = new ArrayList<>();
        private KeyValue<Bytes, byte[]> next;
        private Bytes nextKey;

        WrappedStoreIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator) {
            this.bytesIterator = bytesIterator;
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return next.key;
        }

        @Override
        public KeyValue<Bytes, byte[]> makeNext() {
            while (currList.isEmpty() && bytesIterator.hasNext()) {
                final KeyValue<Bytes, byte[]> next = bytesIterator.next();
                nextKey = next.key;
                currList.addAll(LIST_SERDE.deserializer().deserialize(null, next.value));
            }

            if (currList.isEmpty()) {
                return allDone();
            } else {
                next = KeyValue.pair(nextKey, currList.remove(0));
                return next;
            }
        }

        @Override
        public void close() {
            bytesIterator.close();
        }
    }
}