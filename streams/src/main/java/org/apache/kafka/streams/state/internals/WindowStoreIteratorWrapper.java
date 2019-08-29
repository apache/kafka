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
import org.apache.kafka.streams.state.WindowStoreIterator;

class WindowStoreIteratorWrapper {

    private final KeyValueIterator<Bytes, byte[]> bytesIterator;
    private final long windowSize;

    WindowStoreIteratorWrapper(final KeyValueIterator<Bytes, byte[]> bytesIterator,
                               final long windowSize) {
        this.bytesIterator = bytesIterator;
        this.windowSize = windowSize;
    }

    public WindowStoreIterator<byte[]> valuesIterator() {
        return new WrappedWindowStoreIterator(bytesIterator);
    }

    public KeyValueIterator<Windowed<Bytes>, byte[]> keyValueIterator() {
        return new WrappedKeyValueIterator(bytesIterator, windowSize);
    }

    private static class WrappedWindowStoreIterator implements WindowStoreIterator<byte[]> {
        final KeyValueIterator<Bytes, byte[]> bytesIterator;

        WrappedWindowStoreIterator(
            final KeyValueIterator<Bytes, byte[]> bytesIterator) {
            this.bytesIterator = bytesIterator;
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
        public KeyValue<Long, byte[]> next() {
            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            final long timestamp = WindowKeySchema.extractStoreTimestamp(next.key.get());
            return KeyValue.pair(timestamp, next.value);
        }

        @Override
        public void close() {
            bytesIterator.close();
        }
    }

    private static class WrappedKeyValueIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {
        final KeyValueIterator<Bytes, byte[]> bytesIterator;
        final long windowSize;

        WrappedKeyValueIterator(final KeyValueIterator<Bytes, byte[]> bytesIterator,
                                final long windowSize) {
            this.bytesIterator = bytesIterator;
            this.windowSize = windowSize;
        }

        @Override
        public Windowed<Bytes> peekNextKey() {
            final byte[] nextKey = bytesIterator.peekNextKey().get();
            return WindowKeySchema.fromStoreBytesKey(nextKey, windowSize);
        }

        @Override
        public boolean hasNext() {
            return bytesIterator.hasNext();
        }

        @Override
        public KeyValue<Windowed<Bytes>, byte[]> next() {
            final KeyValue<Bytes, byte[]> next = bytesIterator.next();
            return KeyValue.pair(WindowKeySchema.fromStoreBytesKey(next.key.get(), windowSize), next.value);
        }

        @Override
        public void close() {
            bytesIterator.close();
        }
    }
}
