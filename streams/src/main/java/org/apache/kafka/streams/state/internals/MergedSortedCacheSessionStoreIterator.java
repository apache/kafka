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
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 */
class MergedSortedCacheSessionStoreIterator extends AbstractMergedSortedCacheStoreIterator<Windowed<Bytes>, Windowed<Bytes>, byte[], byte[]> {

    private final SegmentedCacheFunction cacheFunction;

    MergedSortedCacheSessionStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                          final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator,
                                          final SegmentedCacheFunction cacheFunction) {
        super(cacheIterator, storeIterator);
        this.cacheFunction = cacheFunction;
    }

    @Override
    public KeyValue<Windowed<Bytes>, byte[]> deserializeStorePair(final KeyValue<Windowed<Bytes>, byte[]> pair) {
        return pair;
    }

    @Override
    Windowed<Bytes> deserializeCacheKey(final Bytes cacheKey) {
        final byte[] binaryKey = cacheFunction.key(cacheKey).get();
        final byte[] keyBytes = SessionKeySchema.extractKeyBytes(binaryKey);
        final Window window = SessionKeySchema.extractWindow(binaryKey);
        return new Windowed<>(Bytes.wrap(keyBytes), window);
    }


    @Override
    byte[] deserializeCacheValue(final LRUCacheEntry cacheEntry) {
        return cacheEntry.value();
    }

    @Override
    public Windowed<Bytes> deserializeStoreKey(final Windowed<Bytes> key) {
        return key;
    }

    @Override
    public int compare(final Bytes cacheKey, final Windowed<Bytes> storeKey) {
        final Bytes storeKeyBytes = Bytes.wrap(SessionKeySchema.toBinary(storeKey));
        return cacheFunction.compareSegmentedKeys(cacheKey, storeKeyBytes);
    }
}
