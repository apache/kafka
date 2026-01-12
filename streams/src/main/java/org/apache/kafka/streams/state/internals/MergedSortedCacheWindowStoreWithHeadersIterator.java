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
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.function.Function;

import static org.apache.kafka.streams.state.internals.SegmentedCacheFunction.bytesFromCacheKey;

/**
 * Merges two iterators for timestamped window store with headers.
 * Assumes each of them is sorted by key.
 */
class MergedSortedCacheWindowStoreWithHeadersIterator
    extends AbstractMergedSortedCacheStoreIterator<Long, Long, ValueTimestampHeaders<byte[]>, ValueTimestampHeaders<byte[]>>
    implements WindowStoreIterator<ValueTimestampHeaders<byte[]>> {

    private final Function<byte[], Long> timestampExtractor;

    MergedSortedCacheWindowStoreWithHeadersIterator(
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
        final KeyValueIterator<Long, ValueTimestampHeaders<byte[]>> storeIterator,
        final boolean forward) {
        this(cacheIterator, storeIterator, forward, WindowKeySchema::extractStoreTimestamp);
    }

    MergedSortedCacheWindowStoreWithHeadersIterator(
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
        final KeyValueIterator<Long, ValueTimestampHeaders<byte[]>> storeIterator,
        final boolean forward,
        final Function<byte[], Long> tsExtractor) {
        super(cacheIterator, storeIterator, forward);
        this.timestampExtractor = tsExtractor;
    }

    @Override
    public KeyValue<Long, ValueTimestampHeaders<byte[]>> deserializeStorePair(
        final KeyValue<Long, ValueTimestampHeaders<byte[]>> pair) {
        return pair;
    }

    @Override
    Long deserializeCacheKey(final Bytes cacheKey) {
        final byte[] binaryKey = bytesFromCacheKey(cacheKey);
        return timestampExtractor.apply(binaryKey);
    }

    @Override
    ValueTimestampHeaders<byte[]> deserializeCacheValue(final LRUCacheEntry cacheEntry) {
        return ValueTimestampHeaders.makeAllowNullable(
            cacheEntry.value(),
            cacheEntry.context().timestamp(),
            cacheEntry.context().headers()
        );
    }

    @Override
    public Long deserializeStoreKey(final Long key) {
        return key;
    }

    @Override
    public int compare(final Bytes cacheKey, final Long storeKey) {
        final byte[] binaryKey = bytesFromCacheKey(cacheKey);
        final Long cacheTimestamp = timestampExtractor.apply(binaryKey);
        return cacheTimestamp.compareTo(storeKey);
    }
}
