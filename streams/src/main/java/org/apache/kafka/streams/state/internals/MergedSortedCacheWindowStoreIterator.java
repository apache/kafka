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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 * @param <V>
 */
class MergedSortedCacheWindowStoreIterator<V> extends AbstractMergedSortedCacheStoreIterator<Long, Long, V> implements WindowStoreIterator<V> {

    MergedSortedCacheWindowStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                         final KeyValueIterator<Long, byte[]> storeIterator,
                                         final StateSerdes<Long, V> serdes) {
        super(cacheIterator, storeIterator, serdes);
    }

    @Override
    public KeyValue<Long, V> deserializeStorePair(final KeyValue<Long, byte[]> pair) {
        return KeyValue.pair(pair.key, serdes.valueFrom(pair.value));
    }

    @Override
    Long deserializeCacheKey(final Bytes cacheKey) {
        return WindowStoreUtils.timestampFromBinaryKey(cacheKey.get());
    }

    @Override
    public Long deserializeStoreKey(final Long key) {
        return key;
    }

    @Override
    public int compare(final Bytes cacheKey, final Long storeKey) {
        final Long cacheTimestamp = WindowStoreUtils.timestampFromBinaryKey(cacheKey.get());
        return cacheTimestamp.compareTo(storeKey);
    }
}
