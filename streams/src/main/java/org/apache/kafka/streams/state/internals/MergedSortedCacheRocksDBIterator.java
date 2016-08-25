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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 * @param <K>
 * @param <V>
 */
class MergedSortedCacheRocksDBIterator<K, V> implements KeyValueIterator<K, V> {
    private final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator cacheIter;
    private KeyValue<byte[], MemoryLRUCacheBytesEntry> lastEntryCache = null;
    private KeyValue<byte[], MemoryLRUCacheBytesEntry> cacheNext = null;

    private final KeyValueIterator<Bytes, byte[]> storeIterator;
    private KeyValue<Bytes, byte[]> lastEntryRocksDb = null;
    private final KeyValueStore<Bytes, byte[]> store;
    private final StateSerdes<K, V> serdes;
    private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
    private KeyValue<K, V> returnValue = null;

    public MergedSortedCacheRocksDBIterator(final KeyValueStore<Bytes, byte[]> store,
                                            final MemoryLRUCacheBytes.MemoryLRUCacheBytesIterator cacheIter,
                                            final KeyValueIterator<Bytes, byte[]> storeIterator,
                                            final StateSerdes<K, V> serdes) {
        this.cacheIter = cacheIter;
        this.storeIterator = storeIterator;
        this.store = store;
        this.serdes = serdes;
    }

    /**
     * like hasNext(), but for elements of this store only
     *
     * @return
     */
    private boolean cacheHasNextThisStore() {
        cacheGetNextThisStore(false);
        return cacheNext != null;
    }

    @Override
    public boolean hasNext() {
        return storeIterator.hasNext() || cacheHasNextThisStore();
    }

    /**
     * Get in advance the next element that belongs to this store
     */
    private void updateCacheNext() {
        boolean found = false;
        KeyValue<byte[], MemoryLRUCacheBytesEntry> entry = null;
        // skip cache entries that do not belong to our store
        try {
            entry = cacheIter.next();
            while (entry != null) {
                byte[] storeNameBytes = storeNameBytes(entry);

                if (Arrays.equals(store.name().getBytes(), storeNameBytes)) {
                    found = true;
                    break;
                } else {
                    entry = cacheIter.next();
                }
            }
        } catch (java.util.NoSuchElementException e) {
            found = false;
        }
        if (!found) {
            entry = null;
        }

        cacheNext = entry;
    }

    private byte[] storeNameBytes(final KeyValue<byte[], MemoryLRUCacheBytesEntry> entry) {
        byte[] cacheKey = entry.key;
        byte[] originalKey = entry.value.key;
        byte[] storeName = new byte[cacheKey.length - originalKey.length];
        System.arraycopy(cacheKey, 0, storeName, 0, storeName.length);
        return storeName;
    }

    /**
     * Like next(), but for elements of this store only
     *
     * @param advance
     * @return
     */
    private KeyValue<byte[], MemoryLRUCacheBytesEntry> cacheGetNextThisStore(boolean advance) {
        KeyValue<byte[], MemoryLRUCacheBytesEntry> entry = null;

        if (cacheNext == null) {
            updateCacheNext();
        }
        entry = cacheNext;

        if (advance) {
            updateCacheNext();
        }

        return entry;
    }


    @Override
    public KeyValue<K, V> next() {
        if (!cacheHasNextThisStore()) {
            final KeyValue<Bytes, byte[]> next = storeIterator.next();
            return KeyValue.pair(serdes.keyFrom(next.key.get()), serdes.valueFrom(next.value));
        }

        if (lastEntryRocksDb == null && !storeIterator.hasNext()) {

            lastEntryCache = cacheGetNextThisStore(true);
            if (lastEntryCache.value == null) {
                byte[] cacheKey = lastEntryCache.key;
                byte[] storeName = store.name().getBytes();
                byte[] originalKeyBytes = new byte[cacheKey.length - storeName.length];
                System.arraycopy(cacheKey, 0, originalKeyBytes, 0, originalKeyBytes.length);
                final byte[] value = store.get(Bytes.wrap(originalKeyBytes));
                return new KeyValue<>(serdes.keyFrom(originalKeyBytes), serdes.valueFrom(value));
            }
            K originalKey = serdes.keyFromRawMergedStoreNameKey(lastEntryCache.key, store.name());
            return new KeyValue<>(originalKey, serdes.valueFrom(lastEntryCache.value.value));
        }

        // convert the RocksDb key to a key recognized by the cache
        // TODO: serdes back and forth is inneficient
        KeyValue<Bytes, byte[]> rocksDbEntry = null;
        if (lastEntryRocksDb == null) {
            rocksDbEntry = storeIterator.next();
        }



        if (lastEntryCache == null) {
            lastEntryCache = cacheGetNextThisStore(true);

            if (lastEntryCache == null) {
                return this.next();
            }
        }
        if (lastEntryRocksDb == null)
            lastEntryRocksDb = rocksDbEntry;

        byte[] storeKeyToCacheKey = serdes.rawMergeStoreNameKey(serdes.keyFrom(lastEntryRocksDb.key.get()), store.name());
        // element is in the cache but not in RocksDB. This can be if an item is not flushed yet
        if (comparator.compare(lastEntryCache.key, storeKeyToCacheKey) <= 0) {
            K originalKey = serdes.keyFromRawMergedStoreNameKey(lastEntryCache.key, store.name());
            returnValue = new KeyValue<>(originalKey, serdes.valueFrom(lastEntryCache.value.value));
        } else {
            // element is in rocksDb, return it but do not advance the cache element
            returnValue = KeyValue.pair(serdes.keyFrom(lastEntryRocksDb.key.get()), serdes.valueFrom(lastEntryRocksDb.value));
            lastEntryRocksDb = null;
        }

        // iterator bookeepeing
        if (comparator.compare(lastEntryCache.key, storeKeyToCacheKey) < 0) {
            // advance cache iterator, but don't advance RocksDb iterator
            lastEntryCache = null;
        } else if (comparator.compare(lastEntryCache.key, storeKeyToCacheKey) == 0) {
            // advance both iterators since the RocksDb balue is superceded by the cache value
            lastEntryCache = null;
            lastEntryRocksDb = null;
        }

        return returnValue;
    }

    @Override
    public void remove() {
        // do nothing
    }

    @Override
    public void close() {
        cacheIter.close();
        storeIterator.close();
    }
}
