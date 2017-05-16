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

class MergedSortedCacheWindowStoreKeyValueIterator<K, V>
    extends AbstractMergedSortedCacheStoreIterator<Windowed<K>, Windowed<Bytes>, V, byte[]> {

    private final StateSerdes<K, V> serdes;
    private final long windowSize;
    private final SegmentedCacheFunction cacheFunction;

    MergedSortedCacheWindowStoreKeyValueIterator(
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator,
        final KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator,
        final StateSerdes<K, V> serdes,
        final long windowSize,
        final SegmentedCacheFunction cacheFunction
    ) {
        super(filteredCacheIterator, underlyingIterator);
        this.serdes = serdes;
        this.windowSize = windowSize;
        this.cacheFunction = cacheFunction;
    }

    @Override
    Windowed<K> deserializeStoreKey(final Windowed<Bytes> key) {
        return new Windowed<>(serdes.keyFrom(key.key().get()), key.window());
    }

    @Override
    KeyValue<Windowed<K>, V> deserializeStorePair(final KeyValue<Windowed<Bytes>, byte[]> pair) {
        return KeyValue.pair(deserializeStoreKey(pair.key), serdes.valueFrom(pair.value));
    }

    @Override
    Windowed<K> deserializeCacheKey(final Bytes cacheKey) {
        byte[] binaryKey = cacheFunction.key(cacheKey).get();

        final long timestamp = WindowStoreUtils.timestampFromBinaryKey(binaryKey);
        final K key = WindowStoreUtils.keyFromBinaryKey(binaryKey, serdes);
        return new Windowed<>(key, WindowStoreUtils.timeWindowForSize(timestamp, windowSize));
    }

    @Override
    V deserializeCacheValue(final LRUCacheEntry cacheEntry) {
        return serdes.valueFrom(cacheEntry.value);
    }

    @Override
    int compare(final Bytes cacheKey, final Windowed<Bytes> storeKey) {
        Bytes storeKeyBytes = WindowStoreUtils.toBinaryKey(storeKey.key().get(), storeKey.window().start(), 0);
        return cacheFunction.compareSegmentedKeys(cacheKey, storeKeyBytes);
    }
}
