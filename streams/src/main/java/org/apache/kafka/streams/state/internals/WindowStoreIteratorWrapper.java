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
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.NoSuchElementException;

class WindowStoreIteratorWrapper<K, V> {

    // this is optimizing the case when underlying is already a bytes store iterator, in which we can avoid Bytes.wrap() costs
    private static class WrappedWindowStoreBytesIterator extends WindowStoreIteratorWrapper<Bytes, byte[]> {
        WrappedWindowStoreBytesIterator(final KeyValueIterator<Bytes, byte[]> underlying,
                                        final StateSerdes<Bytes, byte[]> serdes,
                                        final long windowSize) {
            super(underlying, serdes, windowSize);
        }

        @Override
        public WindowStoreIterator<byte[]> valuesIterator() {
            return new WrappedWindowStoreIterator<byte[]>(bytesIterator, serdes) {
                @Override
                public KeyValue<Long, byte[]> next() {
                    final KeyValue<Bytes, byte[]> next = bytesIterator.next();
                    final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
                    return KeyValue.pair(timestamp, next.value);
                }
            };
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> keyValueIterator() {
            return new WrappedKeyValueIterator<Bytes, byte[]>(bytesIterator, serdes, windowSize) {
                @Override
                public Windowed<Bytes> peekNextKey() {
                    final Bytes next = bytesIterator.peekNextKey();
                    final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.get());
                    final Bytes key = WindowStoreUtils.bytesKeyFromBinaryKey(next.get());
                    return new Windowed<>(key, WindowStoreUtils.timeWindowForSize(timestamp, windowSize));
                }

                @Override
                public KeyValue<Windowed<Bytes>, byte[]> next() {
                    if (!bytesIterator.hasNext()) {
                        throw new NoSuchElementException();
                    }

                    final KeyValue<Bytes, byte[]> next = bytesIterator.next();
                    final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
                    final Bytes key = WindowStoreUtils.bytesKeyFromBinaryKey(next.key.get());
                    return KeyValue.pair(
                        new Windowed<>(key, WindowStoreUtils.timeWindowForSize(timestamp, windowSize)),
                        next.value
                    );
                }
            };
        }
    }

    static WindowStoreIteratorWrapper<Bytes, byte[]> bytesIterator(final KeyValueIterator<Bytes, byte[]> underlying,
                                                                   final StateSerdes<Bytes, byte[]> serdes,
                                                                   final long windowSize) {
        return new WrappedWindowStoreBytesIterator(underlying, serdes, windowSize);
    }


    protected final KeyValueIterator<Bytes, byte[]> bytesIterator;
    protected final StateSerdes<K, V> serdes;
    protected final long windowSize;

    WindowStoreIteratorWrapper(
        final KeyValueIterator<Bytes, byte[]> bytesIterator,
        final StateSerdes<K, V> serdes,
        final long windowSize
    ) {
        this.bytesIterator = bytesIterator;
        this.serdes = serdes;
        this.windowSize = windowSize;
    }

    public WindowStoreIterator<V> valuesIterator() {
        return new WrappedWindowStoreIterator<>(bytesIterator, serdes);
    }

    public KeyValueIterator<Windowed<K>, V> keyValueIterator() {
        return new WrappedKeyValueIterator<>(bytesIterator, serdes, windowSize);
    }

    private static class WrappedWindowStoreIterator<V> implements WindowStoreIterator<V> {
        final KeyValueIterator<Bytes, byte[]> bytesIterator;
        final StateSerdes<?, V> serdes;

        WrappedWindowStoreIterator(
            KeyValueIterator<Bytes, byte[]> bytesIterator, StateSerdes<?, V> serdes) {
            this.bytesIterator = bytesIterator;
            this.serdes = serdes;
        }

        @Override
        public Long peekNextKey() {
            return WindowStoreUtils.timestampFromBinaryKey(bytesIterator.peekNextKey().get());
        }

        @Override
        public boolean hasNext() {
            return bytesIterator.hasNext();
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
            throw new UnsupportedOperationException("remove() is not supported in " + getClass().getName());
        }

        @Override
        public void close() {
            bytesIterator.close();
        }
    }

    private static class WrappedKeyValueIterator<K, V> implements KeyValueIterator<Windowed<K>, V> {
        final KeyValueIterator<Bytes, byte[]> bytesIterator;
        final StateSerdes<K, V> serdes;
        final long windowSize;

        WrappedKeyValueIterator(
            KeyValueIterator<Bytes, byte[]> bytesIterator, StateSerdes<K, V> serdes, long windowSize) {
            this.bytesIterator = bytesIterator;
            this.serdes = serdes;
            this.windowSize = windowSize;
        }

        @Override
        public Windowed<K> peekNextKey() {
            final byte[] nextKey = bytesIterator.peekNextKey().get();
            final long timestamp = WindowStoreUtils.timestampFromBinaryKey(nextKey);
            final K key = WindowStoreUtils.keyFromBinaryKey(nextKey, serdes);
            return new Windowed<>(key, WindowStoreUtils.timeWindowForSize(timestamp, windowSize));
        }

        @Override
        public boolean hasNext() {
            return bytesIterator.hasNext();
        }

        @Override
        public KeyValue<Windowed<K>, V> next() {
            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            final long timestamp = WindowStoreUtils.timestampFromBinaryKey(next.key.get());
            final K key = WindowStoreUtils.keyFromBinaryKey(next.key.get(), serdes);
            final V value = serdes.valueFrom(next.value);
            return KeyValue.pair(
                new Windowed<>(key, WindowStoreUtils.timeWindowForSize(timestamp, windowSize)),
                value
            );

        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() is not supported in " + getClass().getName());
        }

        @Override
        public void close() {
            bytesIterator.close();
        }
    }
}
