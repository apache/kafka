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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class MergedSortedCacheKeyValueStoreIteratorTest {

    @Test
    public void shouldIterateOverRange() throws Exception {
        KeyValueStore<Bytes, byte[]> kv = new InMemoryKeyValueStore<>("one");
        final ThreadCache cache = new ThreadCache(1000000L);
        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}};
        final String namespace = "one";
        for (int i = 0; i < bytes.length - 1; i += 2) {
            kv.put(Bytes.wrap(bytes[i]), bytes[i]);
            cache.put(namespace, bytes[i + 1], new LRUCacheEntry(bytes[i + 1]));
        }

        final Bytes from = Bytes.wrap(new byte[]{2});
        final Bytes to = Bytes.wrap(new byte[]{9});
        final PeekingKeyValueIterator<Bytes, byte[]> storeIterator = new DelegatingPeekingKeyValueIterator<>(kv.range(from, to));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(namespace, from.get(), to.get());

        final MergedSortedCacheKeyValueStoreIterator<byte[], byte[]> iterator = new MergedSortedCacheKeyValueStoreIterator<>(cacheIterator, storeIterator, new StateSerdes<>("name", Serdes.ByteArray(), Serdes.ByteArray()));
        byte[][] values = new byte[8][];
        int index = 0;
        int bytesIndex = 2;
        while (iterator.hasNext()) {
            final byte[] value = iterator.next().value;
            values[index++] = value;
            assertArrayEquals(bytes[bytesIndex++], value);
        }
    }

}