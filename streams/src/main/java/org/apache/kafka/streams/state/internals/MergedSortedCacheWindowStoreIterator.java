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
import org.apache.kafka.streams.state.WindowStoreIterator;

import static org.apache.kafka.streams.state.internals.SegmentedCacheFunction.bytesFromCacheKey;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 */
class MergedSortedCacheWindowStoreIterator extends AbstractMergedSortedCacheStoreIterator<Long, Long, byte[], byte[]> implements WindowStoreIterator<byte[]> {


    MergedSortedCacheWindowStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                         final KeyValueIterator<Long, byte[]> storeIterator) {
        super(cacheIterator, storeIterator);
    }

    @Override
    public KeyValue<Long, byte[]> deserializeStorePair(final KeyValue<Long, byte[]> pair) {
        return pair;
    }

    @Override
    Long deserializeCacheKey(final Bytes cacheKey) {
        byte[] binaryKey = bytesFromCacheKey(cacheKey);
        return WindowKeySchema.extractStoreTimestamp(binaryKey);
    }

    @Override
    byte[] deserializeCacheValue(final LRUCacheEntry cacheEntry) {
        return cacheEntry.value();
    }

    @Override
    public Long deserializeStoreKey(final Long key) {
        return key;
    }

    @Override
    public int compare(final Bytes cacheKey, final Long storeKey) {
        byte[] binaryKey = bytesFromCacheKey(cacheKey);

        final Long cacheTimestamp = WindowKeySchema.extractStoreTimestamp(binaryKey);
        return cacheTimestamp.compareTo(storeKey);
    }
}
