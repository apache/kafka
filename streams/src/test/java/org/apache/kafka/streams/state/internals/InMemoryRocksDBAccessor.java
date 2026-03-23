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
import org.rocksdb.RocksIterator;

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
        throw new UnsupportedOperationException("newIterator not supported in-memory");
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
}
