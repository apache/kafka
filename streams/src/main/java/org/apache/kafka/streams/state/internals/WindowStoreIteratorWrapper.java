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

class WindowStoreIteratorWrapper<K, V> {

    private final KeyValueIterator<Bytes, byte[]> bytesIterator;
    private final StateSerdes<K, V> serdes;
    private final long windowSize;

    WindowStoreIteratorWrapper(final KeyValueIterator<Bytes, byte[]> bytesIterator,
                               final StateSerdes<K, V> serdes,
                               final long windowSize) {
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
            final KeyValueIterator<Bytes, byte[]> bytesIterator, final StateSerdes<?, V> serdes) {
            this.bytesIterator = bytesIterator;
            this.serdes = serdes;
        }

        @Override
        public Long peekNextKey() {
            return WindowKeySchema.extractStoreTimestamp(bytesIterator.peekNextKey().get());
        }

        @Override
        public boolean hasNext() {
            return bytesIterator.hasNext();
        }

        @Override
        public KeyValue<Long, V> next() {
            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            final long timestamp = WindowKeySchema.extractStoreTimestamp(next.key.get());
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

        WrappedKeyValueIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator,
                                final StateSerdes<K, V> serdes,
                                final long windowSize) {
            this.bytesIterator = bytesIterator;
            this.serdes = serdes;
            this.windowSize = windowSize;
        }

        @Override
        public Windowed<K> peekNextKey() {
            final byte[] nextKey = bytesIterator.peekNextKey().get();
            final long timestamp = WindowKeySchema.extractStoreTimestamp(nextKey);
            final K key = WindowKeySchema.extractStoreKey(nextKey, serdes);
            return new Windowed<>(key, WindowKeySchema.timeWindowForSize(timestamp, windowSize));
        }

        @Override
        public boolean hasNext() {
            return bytesIterator.hasNext();
        }

        @Override
        public KeyValue<Windowed<K>, V> next() {
            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            final long timestamp = WindowKeySchema.extractStoreTimestamp(next.key.get());
            final K key = WindowKeySchema.extractStoreKey(next.key.get(), serdes);
            final V value = serdes.valueFrom(next.value);
            return KeyValue.pair(
                new Windowed<>(key, WindowKeySchema.timeWindowForSize(timestamp, windowSize)),
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
