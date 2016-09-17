/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MergedSortedCacheWindowStoreIteratorTest {

    @Test
    public void shouldIterateOverValueFromBothIterators() throws Exception {
        final List<KeyValue<Long, byte[]>> storeValues = new ArrayList<>();
        final ThreadCache cache = new ThreadCache(1000000L);
        final String namespace = "one";
        final StateSerdes<String, String> stateSerdes = new StateSerdes<>("foo", Serdes.String(), Serdes.String());
        final List<KeyValue<Long, byte[]>> expectedKvPairs = new ArrayList<>();

        for (long t = 0; t < 100; t += 20) {
            final KeyValue<Long, byte[]> v1 = KeyValue.pair(t, String.valueOf(t).getBytes());
            storeValues.add(v1);
            expectedKvPairs.add(v1);
            final byte[] keyBytes = WindowStoreUtils.toBinaryKey("a", t + 10, 0, stateSerdes);
            final byte[] valBytes = String.valueOf(t + 10).getBytes();
            expectedKvPairs.add(KeyValue.pair(t + 10, valBytes));
            cache.put(namespace, keyBytes, new LRUCacheEntry(valBytes));
        }

        byte[] binaryFrom = WindowStoreUtils.toBinaryKey("a", 0, 0, stateSerdes);
        byte[] binaryTo = WindowStoreUtils.toBinaryKey("a", 100, 0, stateSerdes);
        final PeekingWindowIterator<byte[]> storeIterator = new DelegatingPeekingWindowIterator<>(new WindowStoreIteratorStub(storeValues.iterator()));

        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, binaryFrom, binaryTo);

        final MergedSortedCachedWindowStoreIterator<byte[], byte[]> iterator = new MergedSortedCachedWindowStoreIterator<>(cacheIterator, storeIterator, new StateSerdes<>("name", Serdes.ByteArray(), Serdes.ByteArray()));
        int index = 0;
        while (iterator.hasNext()) {
            final KeyValue<Long, byte[]> next = iterator.next();
            final KeyValue<Long, byte[]> expected = expectedKvPairs.get(index++);
            assertArrayEquals(expected.value, next.value);
            assertEquals(expected.key, next.key);
        }
    }

    private static class WindowStoreIteratorStub implements WindowStoreIterator<byte[]> {

        private final Iterator<KeyValue<Long, byte[]>> iterator;

        public WindowStoreIteratorStub(final Iterator<KeyValue<Long, byte[]>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public void close() {
            //no-op
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public KeyValue<Long, byte[]> next() {
            return iterator.next();
        }

        @Override
        public void remove() {

        }
    }
}