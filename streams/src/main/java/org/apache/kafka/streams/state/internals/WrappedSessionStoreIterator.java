/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

class WrappedSessionStoreIterator<K, V> implements KeyValueIterator<Windowed<K>, V> {
    final KeyValueIterator<Bytes, byte[]> bytesIterator;
    private final StateSerdes<K, V> serdes;

    // this is optimizing the case when underlying is already a bytes store iterator, in which we can avoid Bytes.wrap() costs
    private static class WrappedSessionStoreBytesIterator extends WrappedSessionStoreIterator<Bytes, byte[]> {
        WrappedSessionStoreBytesIterator(final KeyValueIterator<Bytes, byte[]> underlying,
                                         final StateSerdes<Bytes, byte[]> serdes) {
            super(underlying, serdes);
        }

        @Override
        public Windowed<Bytes> peekNextKey() {
            final Bytes key = bytesIterator.peekNextKey();

            return SessionKeySerde.fromBytes(key);
        }

        @Override
        public KeyValue<Windowed<Bytes>, byte[]> next() {
            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            return KeyValue.pair(SessionKeySerde.fromBytes(next.key), next.value);
        }
    }

    static WrappedSessionStoreIterator<Bytes, byte[]> bytesIterator(final KeyValueIterator<Bytes, byte[]> underlying,
                                                                    final StateSerdes<Bytes, byte[]> serdes) {
        return new WrappedSessionStoreBytesIterator(underlying, serdes);
    }

    WrappedSessionStoreIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator, final StateSerdes<K, V> serdes) {
        this.bytesIterator = bytesIterator;
        this.serdes = serdes;
    }

    @Override
    public void close() {
        bytesIterator.close();
    }

    @Override
    public Windowed<K> peekNextKey() {
        final Bytes bytes = bytesIterator.peekNextKey();
        return SessionKeySerde.from(bytes.get(), serdes.keyDeserializer());
    }

    @Override
    public boolean hasNext() {
        return bytesIterator.hasNext();
    }

    @Override
    public KeyValue<Windowed<K>, V> next() {
        final KeyValue<Bytes, byte[]> next = bytesIterator.next();
        return KeyValue.pair(SessionKeySerde.from(next.key.get(), serdes.keyDeserializer()), serdes.valueFrom(next.value));
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported in " + getClass().getName());
    }
}