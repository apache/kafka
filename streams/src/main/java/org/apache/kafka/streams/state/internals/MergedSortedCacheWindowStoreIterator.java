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

import static org.apache.kafka.streams.state.internals.SegmentedCacheFunction.bytesFromCacheKey;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 * @param <V>
 */
class MergedSortedCacheWindowStoreIterator<V> extends AbstractMergedSortedCacheStoreIterator<Long, Long, V, byte[]> implements WindowStoreIterator<V> {

    private final StateSerdes<Long, V> serdes;

    MergedSortedCacheWindowStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                         final KeyValueIterator<Long, byte[]> storeIterator,
                                         final StateSerdes<Long, V> serdes) {
        super(cacheIterator, storeIterator);
        this.serdes = serdes;
    }

    @Override
    public KeyValue<Long, V> deserializeStorePair(final KeyValue<Long, byte[]> pair) {
        return KeyValue.pair(pair.key, serdes.valueFrom(pair.value));
    }

    @Override
    Long deserializeCacheKey(final Bytes cacheKey) {
        byte[] binaryKey = bytesFromCacheKey(cacheKey);

        return WindowStoreUtils.timestampFromBinaryKey(binaryKey);
    }

    @Override
    V deserializeCacheValue(final LRUCacheEntry cacheEntry) {
        return serdes.valueFrom(cacheEntry.value);
    }

    @Override
    public Long deserializeStoreKey(final Long key) {
        return key;
    }

    @Override
    public int compare(final Bytes cacheKey, final Long storeKey) {
        byte[] binaryKey = bytesFromCacheKey(cacheKey);

        final Long cacheTimestamp = WindowStoreUtils.timestampFromBinaryKey(binaryKey);
        return cacheTimestamp.compareTo(storeKey);
    }
}
