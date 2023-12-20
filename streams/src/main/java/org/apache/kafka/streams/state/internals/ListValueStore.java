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
 * A wrapper key-value store that serializes the record values bytes as a list.
 * As a result put calls would be interpreted as a get-append-put to the underlying RocksDB store.
 * A put(k,null) will still delete the key, ie, the full list of all values of this key.
 * Range iterators would also flatten the value lists and return the values one-by-one.
 *
 * This store is used for cases where we do not want to de-duplicate values of the same keys but want to retain all such values.
 */
@SuppressWarnings("unchecked")
public class ListValueStore
    extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, Bytes, byte[]>
    implements KeyValueStore<Bytes, byte[]> {

    static private final Serde<List<byte[]>> LIST_SERDE = Serdes.ListSerde(ArrayList.class, Serdes.ByteArray());

    ListValueStore(final KeyValueStore<Bytes, byte[]> bytesStore) {
        super(bytesStore);
    }

    @Override
    public void put(final Bytes key, final byte[] addedValue) {
        // if the value is null we can skip the get and blind delete
        if (addedValue == null) {
            wrapped().put(key, null);
        } else {
            final byte[] oldValue = wrapped().get(key);
            putInternal(key, addedValue, oldValue);
        }
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] addedValue) {
        final byte[] oldValue = wrapped().get(key);

        if (oldValue != null) {
            // if the value is null we can skip the get and blind delete
            if (addedValue == null) {
                wrapped().put(key, null);
            } else {
                putInternal(key, addedValue, oldValue);
            }
        }

        // TODO: here we always return null so that deser would not fail.
        //       we only do this since we know the only caller (stream-stream join processor)
        //       would not need the actual value at all; the changelogging wrapper would not call this function
        return null;
    }

    // this function assumes the addedValue is not null; callers should check null themselves
    private void putInternal(final Bytes key, final byte[] addedValue, final byte[] oldValue) {
        if (oldValue == null) {
            wrapped().put(key, LIST_SERDE.serializer().serialize(null, Collections.singletonList(addedValue)));
        } else {
            final List<byte[]> list = LIST_SERDE.deserializer().deserialize(null, oldValue);
            list.add(addedValue);

            wrapped().put(key, LIST_SERDE.serializer().serialize(null, list));
        }
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        throw new UnsupportedOperationException("putAll not supported");
    }

    @Override
    public byte[] delete(final Bytes key) {
        // we intentionally disable delete calls since the returned bytes would
        // represent a list, not a single value; we need to have a new API for delete if we do need it
        throw new UnsupportedOperationException("delete not supported");
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
        return new ValueListIterator(wrapped().all());
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    private static class ValueListIterator extends AbstractIterator<KeyValue<Bytes, byte[]>>
        implements KeyValueIterator<Bytes, byte[]> {

        private final KeyValueIterator<Bytes, byte[]> bytesIterator;
        private final List<byte[]> currList = new ArrayList<>();
        private KeyValue<Bytes, byte[]> next;
        private Bytes nextKey;

        ValueListIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator) {
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
        protected KeyValue<Bytes, byte[]> makeNext() {
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
            // also need to clear the current list buffer since
            // otherwise even after close the iter can still return data
            currList.clear();
        }
    }
}