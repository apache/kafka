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
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Delegating {@link RocksDBStore.DBAccessor} that throws {@link RocksDBException} on any
 * {@code put} against the offsets column family. Used to simulate the close-time
 * {@code put(closedState)} failure observed in production (e.g. EOSv2 fencing cascades or
 * unclean shutdowns) and verify that column family handles are still released on the
 * exception path via the try/finally ordering in {@code AbstractColumnFamilyAccessor.close()},
 * {@code SingleColumnFamilyAccessor.close()}, and {@code DualColumnFamilyAccessor.close()}.
 */
final class ThrowingOnOffsetsPutDBAccessor implements RocksDBStore.DBAccessor {
    private final RocksDBStore.DBAccessor delegate;
    private final ColumnFamilyHandle offsetsHandle;
    final AtomicInteger thrownPutCount = new AtomicInteger();

    ThrowingOnOffsetsPutDBAccessor(final RocksDBStore.DBAccessor delegate,
                                   final ColumnFamilyHandle offsetsHandle) {
        this.delegate = delegate;
        this.offsetsHandle = offsetsHandle;
    }

    @Override
    public byte[] get(final ColumnFamilyHandle columnFamily, final byte[] key) throws RocksDBException {
        return delegate.get(columnFamily, key);
    }

    @Override
    public byte[] get(final ColumnFamilyHandle columnFamily, final ReadOptions readOptions, final byte[] key) throws RocksDBException {
        return delegate.get(columnFamily, readOptions, key);
    }

    @Override
    public RocksIterator newIterator(final ColumnFamilyHandle columnFamily) {
        return delegate.newIterator(columnFamily);
    }

    @Override
    public void put(final ColumnFamilyHandle columnFamily, final byte[] key, final byte[] value) throws RocksDBException {
        if (columnFamily == offsetsHandle) {
            thrownPutCount.incrementAndGet();
            throw new RocksDBException("simulated put failure on offsets CF");
        }
        delegate.put(columnFamily, key, value);
    }

    @Override
    public void delete(final ColumnFamilyHandle columnFamily, final byte[] key) throws RocksDBException {
        delegate.delete(columnFamily, key);
    }

    @Override
    public void deleteRange(final ColumnFamilyHandle columnFamily, final byte[] from, final byte[] to) throws RocksDBException {
        delegate.deleteRange(columnFamily, from, to);
    }

    @Override
    public long approximateNumEntries(final ColumnFamilyHandle columnFamily) throws RocksDBException {
        return delegate.approximateNumEntries(columnFamily);
    }

    @Override
    public void flush(final ColumnFamilyHandle... columnFamilies) throws RocksDBException {
        delegate.flush(columnFamilies);
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
