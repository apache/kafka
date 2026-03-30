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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * An in-memory implementation of {@link RocksDBStore.DBAccessor} intended for use in tests,
 * eliminating the need for mocks. Data is kept in a {@link NavigableMap} per
 * {@link ColumnFamilyHandle} (identified by object identity), providing full key-ordering semantics
 * equivalent to RocksDB's default comparator.
 */
public class InMemoryRocksDBAccessor implements RocksDBStore.DBAccessor {

    /**
     * Unsigned lexicographic byte-array comparator — matches RocksDB's default comparator.
     */
    private static final Comparator<byte[]> BYTES_COMPARATOR = Arrays::compare;

    /**
     * Per-CF stores, keyed by {@link ColumnFamilyHandle} object identity.
     */
    private final Map<ColumnFamilyHandle, NavigableMap<byte[], byte[]>> stores = new IdentityHashMap<>();
    private final RocksDB rocksDB;

    public InMemoryRocksDBAccessor(final RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    private NavigableMap<byte[], byte[]> storeFor(final ColumnFamilyHandle columnFamily) {
        return stores.computeIfAbsent(columnFamily, cf -> new TreeMap<>(BYTES_COMPARATOR));
    }

    @Override
    public byte[] get(final ColumnFamilyHandle columnFamily, final byte[] key) {
        return storeFor(columnFamily).get(key);
    }

    @Override
    public byte[] get(final ColumnFamilyHandle columnFamily, final ReadOptions readOptions, final byte[] key) {
        // ReadOptions (snapshots, fill-cache, etc.) are not meaningful in-memory; delegate to plain get.
        return get(columnFamily, key);
    }

    @Override
    public RocksIterator newIterator(final ColumnFamilyHandle columnFamily) {
        return new InMemoryRocksIterator(rocksDB, storeFor(columnFamily));
    }

    @Override
    public void put(final ColumnFamilyHandle columnFamily, final byte[] key, final byte[] value) {
        if (value == null) {
            delete(columnFamily, key);
        } else {
            storeFor(columnFamily).put(key, value);
        }
    }

    @Override
    public void delete(final ColumnFamilyHandle columnFamily, final byte[] key) {
        storeFor(columnFamily).remove(key);
    }

    @Override
    public void deleteRange(final ColumnFamilyHandle columnFamily, final byte[] from, final byte[] to) {
        throw new UnsupportedOperationException("deleteRange not supported in-memory");
    }

    @Override
    public long approximateNumEntries(final ColumnFamilyHandle columnFamily) {
        return storeFor(columnFamily).size();
    }

    @Override
    public void flush(final ColumnFamilyHandle... columnFamilies) {
        // No-op: in-memory writes are immediately durable.
    }

    @Override
    public void reset() {
        stores.clear();
    }

    @Override
    public void close() {
        // No native resources to release.
    }

    /**
     * In-memory iterator backed by a navigable map.
     */
    private static class InMemoryRocksIterator extends RocksIterator {
        private final NavigableMap<byte[], byte[]> data;
        private byte[] currentKey;
        private boolean valid;

        InMemoryRocksIterator(final RocksDB rocksDB, final NavigableMap<byte[], byte[]> data) {
            super(rocksDB, 0L);
            this.data = data;
            this.currentKey = null;
            this.valid = false;
        }

        @Override
        protected void disposeInternal() {
            // No native resources to release.
        }

        @Override
        public boolean isValid() {
            return valid && currentKey != null && data.containsKey(currentKey);
        }

        @Override
        public void seekToFirst() {
            if (data.isEmpty()) {
                invalidate();
            } else {
                currentKey = data.firstKey();
                valid = true;
            }
        }

        @Override
        public void seekToLast() {
            if (data.isEmpty()) {
                invalidate();
            } else {
                currentKey = data.lastKey();
                valid = true;
            }
        }

        @Override
        public void seek(final byte[] target) {
            final Map.Entry<byte[], byte[]> entry = data.ceilingEntry(target);
            if (entry == null) {
                invalidate();
            } else {
                currentKey = entry.getKey();
                valid = true;
            }
        }

        @Override
        public void seekForPrev(final byte[] target) {
            final Map.Entry<byte[], byte[]> entry = data.floorEntry(target);
            if (entry == null) {
                invalidate();
            } else {
                currentKey = entry.getKey();
                valid = true;
            }
        }

        @Override
        public void seek(final ByteBuffer target) {
            final ByteBuffer duplicate = target.duplicate();
            final byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            seek(bytes);
        }

        @Override
        public void seekForPrev(final ByteBuffer target) {
            final ByteBuffer duplicate = target.duplicate();
            final byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            seekForPrev(bytes);
        }

        @Override
        public void next() {
            final Map.Entry<byte[], byte[]> entry = data.higherEntry(currentKey);
            if (entry == null) {
                invalidate();
            } else {
                currentKey = entry.getKey();
            }
        }

        @Override
        public void prev() {
            if (!isValid()) {
                return;
            }
            final Map.Entry<byte[], byte[]> entry = data.lowerEntry(currentKey);
            if (entry == null) {
                invalidate();
            } else {
                currentKey = entry.getKey();
            }
        }

        @Override
        public byte[] key() {
            return currentKey;
        }

        @Override
        public byte[] value() {
            return isValid() ? data.get(currentKey) : null;
        }

        @Override
        public void status() throws RocksDBException {
            // In-memory iterator never enters an error state.
        }

        @Override
        public void close() {
            invalidate();
        }

        private void invalidate() {
            valid = false;
            currentKey = null;
        }
    }
}
