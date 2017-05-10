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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.NoSuchElementException;

class WrappedWindowStoreIterator<K, V> implements WindowStoreIterator<KeyValue<K, V>> {
    final KeyValueIterator<Bytes, byte[]> bytesIterator;
    private final StateSerdes<K, V> serdes;

    // this is optimizing the case when underlying is already a bytes store iterator, in which we can avoid Bytes.wrap() costs
    private static class WrappedWindowStoreBytesIterator extends WrappedWindowStoreIterator<Bytes, byte[]> {
        WrappedWindowStoreBytesIterator(final KeyValueIterator<Bytes, byte[]> underlying,
                                        final StateSerdes<Bytes, byte[]> serdes) {
            super(underlying, serdes);
        }

        @Override
        public KeyValue<Long, KeyValue<Bytes, byte[]>> next() {
            if (!bytesIterator.hasNext()) {
                throw new NoSuchElementException();
            }

            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
            return KeyValue.pair(timestamp, KeyValue.pair(next.key, next.value));
        }

        @Override
        public WindowStoreIterator<byte[]> valuesIterator() {
            final WrappedWindowStoreBytesIterator delegate = this;
            return new WindowStoreIterator<byte[]>() {
                @Override
                public void close() {
                    delegate.close();
                }

                @Override
                public Long peekNextKey() {
                    return delegate.peekNextKey();
                }

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public KeyValue<Long, byte[]> next() {
                    final KeyValue<Bytes, byte[]> next = bytesIterator.next();
                    final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
                    return KeyValue.pair(timestamp, next.value);
                }

                @Override
                public void remove() {
                    delegate.remove();
                }
            };
        }
    }

    static WrappedWindowStoreIterator<Bytes, byte[]> bytesIterator(final KeyValueIterator<Bytes, byte[]> underlying,
                                                            final StateSerdes<Bytes, byte[]> serdes) {
        return new WrappedWindowStoreBytesIterator(underlying, serdes);
    }

    WrappedWindowStoreIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator, final StateSerdes<K, V> serdes) {
        this.bytesIterator = bytesIterator;
        this.serdes = serdes;
    }

    public WindowStoreIterator<V> valuesIterator() {
        final WrappedWindowStoreIterator<K, V> delegate = this;
        return new WindowStoreIterator<V>() {
            @Override
            public void close() {
                delegate.close();
            }

            @Override
            public Long peekNextKey() {
                return delegate.peekNextKey();
            }

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public KeyValue<Long, V> next() {
                final KeyValue<Bytes, byte[]> next = bytesIterator.next();
                final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
                final V value = serdes.valueFrom(next.value);
                return KeyValue.pair(timestamp, value);
            }

            @Override
            public void remove() {
                delegate.remove();
            }
        };
    }

    @Override
    public boolean hasNext() {
        return bytesIterator.hasNext();
    }

    /**
     * @throws NoSuchElementException if no next element exists
     */
    @Override
    public KeyValue<Long, KeyValue<K, V>> next() {
        final KeyValue<Bytes, byte[]> next = bytesIterator.next();
        final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
        final K key = WindowStoreUtils.keyFromBinaryKey(next.key.get(), serdes);
        final V value = serdes.valueFrom(next.value);
        return KeyValue.pair(timestamp, KeyValue.pair(key, value));
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported in " + getClass().getName());
    }

    @Override
    public void close() {
        bytesIterator.close();
    }

    @Override
    public Long peekNextKey() {
        return WindowStoreUtils.timestampFromBinaryKey(bytesIterator.peekNextKey().get());
    }
}
