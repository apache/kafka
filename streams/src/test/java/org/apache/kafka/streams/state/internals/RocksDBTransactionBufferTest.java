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
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RocksDBTransactionBufferTest {

    static {
        RocksDB.loadLibrary();
    }

    private File dbDir;
    private RocksDB db;
    private ColumnFamilyHandle cfHandle;
    private ColumnFamilyHandle otherCfHandle;
    private WriteOptions wOptions;
    private RocksDBTransactionBuffer buffer;

    private static Bytes key(final String k) {
        return Bytes.wrap(k.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] val(final String v) {
        return v.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(final byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    @BeforeEach
    public void setUp() throws RocksDBException {
        dbDir = TestUtils.tempDirectory();

        final DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true);
        dbOptions.setCreateMissingColumnFamilies(true);

        final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
            new ColumnFamilyDescriptor("other-cf".getBytes(StandardCharsets.UTF_8), cfOptions)
        );
        final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles);
        cfHandle = cfHandles.get(0);
        otherCfHandle = cfHandles.get(1);

        wOptions = new WriteOptions();
        wOptions.setDisableWAL(true);

        // Pre-populate base data
        db.put(cfHandle, wOptions, key("a").get(), val("base-a"));
        db.put(cfHandle, wOptions, key("b").get(), val("base-b"));
        db.put(cfHandle, wOptions, key("c").get(), val("base-c"));

        buffer = new RocksDBTransactionBuffer(db, cfHandle, wOptions, "test-store");
    }

    @AfterEach
    public void tearDown() {
        if (buffer != null) buffer.close();
        if (cfHandle != null) cfHandle.close();
        if (otherCfHandle != null) otherCfHandle.close();
        if (db != null) db.close();
        if (wOptions != null) wOptions.close();
    }

    @Test
    public void shouldReturnNullForUnstagedKey() {
        assertNull(buffer.get(key("a")));
        assertNull(buffer.get(key("z")));
    }

    @Test
    public void shouldReturnStagedValue() {
        buffer.stage(key("a"), val("staged-a"));
        final Optional<byte[]> staged = buffer.get(key("a"));
        assertTrue(staged.isPresent());
        assertArrayEquals(val("staged-a"), staged.get());
    }

    @Test
    public void shouldReturnEmptyOptionalForStagedTombstone() {
        buffer.stage(key("a"), null);
        final Optional<byte[]> staged = buffer.get(key("a"));
        assertEquals(Optional.empty(), staged);
    }

    @Test
    public void shouldReturnStagedValueForNewKey() {
        buffer.stage(key("z"), val("staged-z"));
        final Optional<byte[]> staged = buffer.get(key("z"));
        assertTrue(staged.isPresent());
        assertArrayEquals(val("staged-z"), staged.get());
    }

    @Test
    public void shouldReportIsEmpty() {
        assertTrue(buffer.isEmpty());
        buffer.stage(key("x"), val("v"));
        assertFalse(buffer.isEmpty());
    }

    @Test
    public void shouldCommitStagedWritesToRocksDB() throws RocksDBException {
        buffer.stage(key("a"), val("new-a"));
        buffer.stage(key("d"), val("new-d"));
        buffer.stage(key("b"), null); // delete b

        buffer.commit();

        assertEquals("new-a", str(db.get(cfHandle, key("a").get())));
        assertNull(db.get(cfHandle, key("b").get()));
        assertEquals("base-c", str(db.get(cfHandle, key("c").get())));
        assertEquals("new-d", str(db.get(cfHandle, key("d").get())));
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void shouldRollbackWithoutAffectingBase() throws RocksDBException {
        buffer.stage(key("a"), val("new-a"));
        buffer.stage(key("d"), val("new-d"));

        buffer.rollback();

        assertEquals("base-a", str(db.get(cfHandle, key("a").get())));
        assertNull(db.get(cfHandle, key("d").get()));
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void shouldMergeStagedWritesInAllScan() {
        buffer.stage(key("a"), val("staged-a"));
        buffer.stage(key("d"), val("staged-d"));
        buffer.stage(key("b"), null); // tombstone

        try (KeyValueIterator<Bytes, byte[]> iter = buffer.all(true)) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("a", "c", "d"), keys);
        }
    }

    @Test
    public void shouldMergeStagedWritesInOwnerAllScan() {
        buffer.stage(key("a"), val("staged-a"));
        buffer.stage(key("d"), val("staged-d"));

        try (KeyValueIterator<Bytes, byte[]> iter = buffer.all(true)) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("a", "b", "c", "d"), keys);
        }
    }

    @Test
    public void shouldMergeStagedWritesInRangeScan() {
        buffer.stage(key("b"), val("staged-b"));

        try (KeyValueIterator<Bytes, byte[]> iter = buffer.range(key("a"), key("c"), true, true)) {
            final List<String> keys = new ArrayList<>();
            final List<String> values = new ArrayList<>();
            while (iter.hasNext()) {
                final KeyValue<Bytes, byte[]> entry = iter.next();
                keys.add(entry.key.toString());
                values.add(str(entry.value));
            }
            assertEquals(List.of("a", "b", "c"), keys);
            assertEquals(List.of("base-a", "staged-b", "base-c"), values);
        }
    }

    @Test
    public void shouldSeeStagedWritesFromAnotherThread() throws Exception {
        buffer.stage(key("x"), val("staged-x"));

        final AtomicReference<Optional<byte[]>> result = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        final Thread reader = new Thread(() -> {
            result.set(buffer.get(key("x")));
            latch.countDown();
        });
        reader.start();
        latch.await();

        assertTrue(result.get().isPresent());
        assertArrayEquals(val("staged-x"), result.get().get());
    }

    @Test
    public void shouldScanFromAnotherThread() throws Exception {
        buffer.stage(key("d"), val("staged-d"));

        final AtomicReference<List<String>> result = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        final Thread reader = new Thread(() -> {
            try (KeyValueIterator<Bytes, byte[]> iter = buffer.all(true)) {
                final List<String> keys = new ArrayList<>();
                while (iter.hasNext()) {
                    keys.add(iter.next().key.toString());
                }
                result.set(keys);
            }
            latch.countDown();
        });
        reader.start();
        latch.await();

        assertEquals(List.of("a", "b", "c", "d"), result.get());
    }

    @Test
    public void shouldHandleConcurrentStageDeleteRangeAndNonOwnerReads() throws Exception {
        final int readerCount = 4;
        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final List<Thread> readers = new ArrayList<>();
        for (int r = 0; r < readerCount; r++) {
            final Thread reader = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        buffer.get(key("k50"));
                        // Non-owner scan → snapshotScan: must never throw and must be sorted, even
                        // while the owner concurrently runs stageDeleteRange (subMap().clear()).
                        try (KeyValueIterator<Bytes, byte[]> iter = buffer.all(true)) {
                            Bytes prev = null;
                            while (iter.hasNext()) {
                                final Bytes k = iter.next().key;
                                if (prev != null && prev.compareTo(k) >= 0) {
                                    throw new AssertionError("non-owner scan returned unsorted keys: " + prev + " >= " + k);
                                }
                                prev = k;
                            }
                        }
                    }
                } catch (final Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            readers.add(reader);
            reader.start();
        }
        try {
            for (int i = 0; i < 5_000; i++) {
                buffer.stage(key(String.format("k%02d", i % 80)), val("v" + i));
                if (i % 50 == 0) {
                    buffer.stageDeleteRange(cfHandle, key("k10"), key("k30"));
                }
                if (i % 200 == 0) {
                    buffer.commit();
                }
            }
        } finally {
            stop.set(true);
            for (final Thread reader : readers) {
                reader.join();
            }
        }
        if (failure.get() != null) {
            throw new AssertionError("concurrent non-owner reader failed", failure.get());
        }
    }

    @Test
    public void shouldNotShowStagedWritesInBaseAfterRollback() throws RocksDBException {
        buffer.stage(key("x"), val("staged-x"));
        buffer.rollback();

        assertNull(db.get(cfHandle, key("x").get()));
        assertNull(buffer.get(key("x")));
    }

    @Test
    public void shouldHideDeletedRangeFromPointReads() {
        buffer.stageDeleteRange(cfHandle, key("a"), key("c"));

        assertEquals(Optional.empty(), buffer.get(key("a")));
        assertEquals(Optional.empty(), buffer.get(key("b")));
        assertNull(buffer.get(key("c"))); // exclusive upper bound
    }

    @Test
    public void shouldHideDeletedRangeFromScans() {
        buffer.stageDeleteRange(cfHandle, key("a"), key("c"));

        try (KeyValueIterator<Bytes, byte[]> iter = buffer.all(true)) {
            final List<String> keys = new ArrayList<>();
            while (iter.hasNext()) {
                keys.add(iter.next().key.toString());
            }
            assertEquals(List.of("c"), keys);
        }
    }

    @Test
    public void shouldAllowStageAfterDeleteRangeToOverride() {
        buffer.stageDeleteRange(cfHandle, key("a"), key("d"));
        buffer.stage(key("b"), val("new-b"));

        final Optional<byte[]> result = buffer.get(key("b"));
        assertTrue(result.isPresent());
        assertArrayEquals(val("new-b"), result.get());
        assertEquals(Optional.empty(), buffer.get(key("a")));
    }

    @Test
    public void shouldCommitRangeDeleteToRocksDB() throws RocksDBException {
        buffer.stageDeleteRange(cfHandle, key("a"), key("c"));
        buffer.commit();

        assertNull(db.get(cfHandle, key("a").get()));
        assertNull(db.get(cfHandle, key("b").get()));
        assertEquals("base-c", str(db.get(cfHandle, key("c").get())));
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void shouldCommitRangeDeleteWithOverridingPut() throws RocksDBException {
        buffer.stageDeleteRange(cfHandle, key("a"), key("d"));
        buffer.stage(key("b"), val("new-b"));
        buffer.commit();

        assertNull(db.get(cfHandle, key("a").get()));
        assertEquals("new-b", str(db.get(cfHandle, key("b").get())));
        assertNull(db.get(cfHandle, key("c").get()));
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void shouldRollbackRangeDelete() throws RocksDBException {
        buffer.stageDeleteRange(cfHandle, key("a"), key("c"));
        buffer.rollback();

        assertEquals("base-a", str(db.get(cfHandle, key("a").get())));
        assertEquals("base-b", str(db.get(cfHandle, key("b").get())));
        assertEquals("base-c", str(db.get(cfHandle, key("c").get())));
        assertTrue(buffer.isEmpty());
    }

    @Test
    public void shouldReportNotEmptyWithRangeTombstones() {
        assertTrue(buffer.isEmpty());
        buffer.stageDeleteRange(cfHandle, key("a"), key("c"));
        assertFalse(buffer.isEmpty());
    }

    @Test
    public void shouldSupportMultipleCommitRollbackCycles() throws RocksDBException {
        buffer.stage(key("x"), val("v1"));
        buffer.commit();
        assertEquals("v1", str(db.get(cfHandle, key("x").get())));

        buffer.stage(key("x"), val("v2"));
        buffer.rollback();
        // After rollback, base still has v1
        assertEquals("v1", str(db.get(cfHandle, key("x").get())));
        // get() returns null (not staged), caller should check base
        assertNull(buffer.get(key("x")));

        buffer.stage(key("x"), val("v3"));
        buffer.commit();
        assertEquals("v3", str(db.get(cfHandle, key("x").get())));
    }

    // --- CF-aware overload tests ---

    @Test
    public void shouldStageWriteToOtherCF() throws RocksDBException {
        buffer.stage(otherCfHandle, key("x"), val("other-x"));

        // Non-primary CF writes are not visible in the staging read buffer
        assertNull(buffer.get(key("x")));
        // Not yet flushed to RocksDB
        assertNull(db.get(otherCfHandle, key("x").get()));

        buffer.commit();
        assertEquals("other-x", str(db.get(otherCfHandle, key("x").get())));
        assertNull(buffer.get(key("x")));
    }

    @Test
    public void shouldStageDeleteToOtherCF() throws RocksDBException {
        db.put(otherCfHandle, wOptions, key("x").get(), val("other-x"));

        buffer.stage(otherCfHandle, key("x"), null);

        // Non-primary CF deletes are not visible in the staging read buffer
        assertNull(buffer.get(key("x")));
        // Not yet flushed
        assertArrayEquals(val("other-x"), db.get(otherCfHandle, key("x").get()));

        buffer.commit();
        assertNull(db.get(otherCfHandle, key("x").get()));
    }

    @Test
    public void shouldStageDeleteRangeToOtherCF() throws RocksDBException {
        db.put(otherCfHandle, wOptions, key("a").get(), val("other-a"));
        db.put(otherCfHandle, wOptions, key("b").get(), val("other-b"));

        buffer.stageDeleteRange(otherCfHandle, key("a"), key("c"));

        // Shared tombstone hides keys in range from buffer reads
        assertEquals(Optional.empty(), buffer.get(key("a")));
        assertEquals(Optional.empty(), buffer.get(key("b")));
        assertNull(buffer.get(key("c"))); // exclusive upper bound: not covered

        buffer.commit();
        assertNull(db.get(otherCfHandle, key("a").get()));
        assertNull(db.get(otherCfHandle, key("b").get()));
    }

    @Test
    public void shouldCommitWritesAcrossCFsAtomically() throws RocksDBException {
        buffer.stage(cfHandle, key("d"), val("data-d"));
        buffer.stage(otherCfHandle, key("o"), val("other-o"));

        // Neither visible in RocksDB before commit
        assertNull(db.get(cfHandle, key("d").get()));
        assertNull(db.get(otherCfHandle, key("o").get()));

        buffer.commit();

        assertEquals("data-d", str(db.get(cfHandle, key("d").get())));
        assertEquals("other-o", str(db.get(otherCfHandle, key("o").get())));
        assertTrue(buffer.isEmpty());
    }
}
