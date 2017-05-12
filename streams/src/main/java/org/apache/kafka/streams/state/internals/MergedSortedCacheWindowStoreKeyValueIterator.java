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
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

class MergedSortedCacheWindowStoreKeyValueIterator<K, V>
    extends AbstractMergedSortedCacheStoreIterator<Windowed<K>, Windowed<Bytes>, V, byte[]> {

    private final StateSerdes<K, V> serdes;
    private final long windowSize;

    public MergedSortedCacheWindowStoreKeyValueIterator(
        PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator,
        KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator,
        StateSerdes<K, V> serdes,
        final long windowSize
    ) {
        super(filteredCacheIterator, underlyingIterator);
        this.serdes = serdes;
        this.windowSize = windowSize;
    }

    @Override
    Windowed<K> deserializeStoreKey(Windowed<Bytes> key) {
        return new Windowed<>(serdes.keyFrom(key.key().get()), key.window());
    }

    @Override
    KeyValue<Windowed<K>, V> deserializeStorePair(KeyValue<Windowed<Bytes>, byte[]> pair) {
        return KeyValue.pair(deserializeStoreKey(pair.key), serdes.valueFrom(pair.value));
    }

    @Override
    Windowed<K> deserializeCacheKey(Bytes cacheKey) {
        final long timestamp = WindowStoreUtils.timestampFromBinaryKey(cacheKey.get());
        final K key = WindowStoreUtils.keyFromBinaryKey(cacheKey.get(), serdes);
        return new Windowed<>(key, new TimeWindow(timestamp, timestamp + windowSize));
    }

    @Override
    V deserializeCacheValue(LRUCacheEntry cacheEntry) {
        return serdes.valueFrom(cacheEntry.value);
    }

    @Override
    int compare(Bytes cacheKey, Windowed<Bytes> storeKey) {
        Bytes storeKeyBytes = SessionKeySerde.bytesToBinary(storeKey);
        return cacheKey.compareTo(storeKeyBytes);
    }
}
