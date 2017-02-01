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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MergedSortedCacheWrappedWindowStoreIteratorTest {

    private final List<KeyValue<Long, byte[]>> windowStoreKvPairs = new ArrayList<>();
    private final ThreadCache cache = new ThreadCache("testCache", 1000000L,  new MockStreamsMetrics(new Metrics()));
    private final String namespace = "one";
    private final StateSerdes<String, String> stateSerdes = new StateSerdes<>("foo", Serdes.String(), Serdes.String());

    @Test
    public void shouldIterateOverValueFromBothIterators() throws Exception {
        final List<KeyValue<Long, byte[]>> expectedKvPairs = new ArrayList<>();
        for (long t = 0; t < 100; t += 20) {
            final byte[] v1Bytes = String.valueOf(t).getBytes();
            final KeyValue<Long, byte[]> v1 = KeyValue.pair(t, v1Bytes);
            windowStoreKvPairs.add(v1);
            expectedKvPairs.add(KeyValue.pair(t, v1Bytes));
            final Bytes keyBytes = WindowStoreUtils.toBinaryKey("a", t + 10, 0, stateSerdes);
            final byte[] valBytes = String.valueOf(t + 10).getBytes();
            expectedKvPairs.add(KeyValue.pair(t + 10, valBytes));
            cache.put(namespace, keyBytes, new LRUCacheEntry(valBytes));
        }

        Bytes fromBytes = WindowStoreUtils.toBinaryKey("a", 0, 0, stateSerdes);
        Bytes toBytes = WindowStoreUtils.toBinaryKey("a", 100, 0, stateSerdes);
        final KeyValueIterator<Long, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(windowStoreKvPairs.iterator()));

        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, fromBytes, toBytes);

        final MergedSortedCacheWindowStoreIterator<byte[]> iterator = new MergedSortedCacheWindowStoreIterator<>(cacheIterator, storeIterator, new StateSerdes<>("name", Serdes.Long(), Serdes.ByteArray()));
        int index = 0;
        while (iterator.hasNext()) {
            final KeyValue<Long, byte[]> next = iterator.next();
            final KeyValue<Long, byte[]> expected = expectedKvPairs.get(index++);
            assertArrayEquals(expected.value, next.value);
            assertEquals(expected.key, next.key);
        }
    }

    @Test
    public void shouldPeekNextKey() throws Exception {
        windowStoreKvPairs.add(KeyValue.pair(10L, "a".getBytes()));
        cache.put(namespace, WindowStoreUtils.toBinaryKey("a", 0, 0, stateSerdes), new LRUCacheEntry("b".getBytes()));
        Bytes fromBytes = WindowStoreUtils.toBinaryKey("a", 0, 0, stateSerdes);
        Bytes toBytes = WindowStoreUtils.toBinaryKey("a", 100, 0, stateSerdes);
        final KeyValueIterator<Long, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(windowStoreKvPairs.iterator()));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, fromBytes, toBytes);
        final MergedSortedCacheWindowStoreIterator<byte[]> iterator = new MergedSortedCacheWindowStoreIterator<>(cacheIterator, storeIterator, new StateSerdes<>("name", Serdes.Long(), Serdes.ByteArray()));
        assertThat(iterator.peekNextKey(), equalTo(0L));
        iterator.next();
        assertThat(iterator.peekNextKey(), equalTo(10L));
    }

}