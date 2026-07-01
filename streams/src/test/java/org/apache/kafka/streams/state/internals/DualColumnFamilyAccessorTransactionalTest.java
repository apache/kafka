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
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

/**
 * Regression tests ensuring that {@link DualColumnFamilyAccessor} scans honour writes staged in a
 * {@link RocksDBStore.TransactionalDBAccessor} transaction buffer before they are committed to
 * RocksDB.
 *
 * Prior to the fix on KIP-892/txn-rocksdb-accessor, DualColumnFamilyAccessor built its inner
 * iterators via {@code accessor.newIterator()}, which TransactionalDBAccessor passes straight
 * through to RocksDB without merging the buffer. This caused range/all/prefixScan to miss staged
 * writes and deletions, breaking read-your-writes under EOS.
 */
public class DualColumnFamilyAccessorTransactionalTest {

    static {
        RocksDB.loadLibrary();
    }

    private static final String STORE_NAME = "test-store";

    private File dbDir;
    private RocksDB db;
    private ColumnFamilyHandle offsetsCFHandle;
    private ColumnFamilyHandle oldCFHandle;
    private ColumnFamilyHandle newCFHandle;
    private WriteOptions wOptions;
    private FlushOptions flushOptions;

    private RocksDBStore.DBAccessor txnAccessor;
    private DualColumnFamilyAccessor accessor;

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
            new ColumnFamilyDescriptor("old-cf".getBytes(StandardCharsets.UTF_8), cfOptions),
            new ColumnFamilyDescriptor("new-cf".getBytes(StandardCharsets.UTF_8), cfOptions)
        );
        final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles);
        offsetsCFHandle = cfHandles.get(0);
        oldCFHandle = cfHandles.get(1);
        newCFHandle = cfHandles.get(2);

        wOptions = new WriteOptions();
        wOptions.setDisableWAL(true);
        flushOptions = new FlushOptions();

        final RocksDBStore.DBAccessor directAccessor =
                new RocksDBStore.DirectDBAccessor(db, flushOptions, wOptions);
        txnAccessor = new RocksDBStore.TransactionalDBAccessor(
                directAccessor, db, newCFHandle, offsetsCFHandle, wOptions, STORE_NAME);

        final RocksDBStore store = mock(RocksDBStore.class);
        store.position = Position.emptyPosition();
        store.context = mock(StateStoreContext.class);
        lenient().when(store.name()).thenReturn(STORE_NAME);

        accessor = new DualColumnFamilyAccessor(
                offsetsCFHandle, oldCFHandle, newCFHandle,
                v -> ("converted:" + new String(v, StandardCharsets.UTF_8))
                        .getBytes(StandardCharsets.UTF_8),
                store,
                new AtomicBoolean(true));
    }

    @AfterEach
    public void tearDown() throws RocksDBException {
        if (txnAccessor != null) txnAccessor.close();
        if (offsetsCFHandle != null) offsetsCFHandle.close();
        if (oldCFHandle != null) oldCFHandle.close();
        if (newCFHandle != null) newCFHandle.close();
        if (db != null) db.close();
        if (wOptions != null) wOptions.close();
        if (flushOptions != null) flushOptions.close();
    }

    private List<KeyValue<Bytes, byte[]>> drain(final ManagedKeyValueIterator<Bytes, byte[]> it) {
        it.onClose(() -> { });
        final List<KeyValue<Bytes, byte[]>> results = new ArrayList<>();
        while (it.hasNext()) {
            results.add(it.next());
        }
        it.close();
        return results;
    }

    @Test
    public void shouldSeeStagedPutInAll() throws RocksDBException {
        // Stage a write on the new CF via the dual-CF accessor (no commit).
        accessor.put(txnAccessor, key("b").get(), val("new-b"));

        final List<KeyValue<Bytes, byte[]>> results = drain(accessor.all(txnAccessor, true));

        assertEquals(1, results.size());
        assertArrayEquals(key("b").get(), results.get(0).key.get());
        assertArrayEquals(val("new-b"), results.get(0).value);
    }

    @Test
    public void shouldSeeStagedPutInRange() throws RocksDBException {
        accessor.put(txnAccessor, key("b").get(), val("new-b"));
        accessor.put(txnAccessor, key("d").get(), val("new-d"));

        final List<KeyValue<Bytes, byte[]>> results =
                drain(accessor.range(txnAccessor, key("a"), key("c"), true));

        assertEquals(1, results.size());
        assertArrayEquals(key("b").get(), results.get(0).key.get());
    }

    @Test
    public void shouldSeeStagedPutInPrefixScan() throws RocksDBException {
        accessor.put(txnAccessor, key("foo:1").get(), val("new-foo1"));
        accessor.put(txnAccessor, key("bar:1").get(), val("new-bar"));

        final List<KeyValue<Bytes, byte[]>> results =
                drain(accessor.prefixScan(txnAccessor, Bytes.wrap("foo:".getBytes(StandardCharsets.UTF_8))));

        assertEquals(1, results.size());
        assertArrayEquals(key("foo:1").get(), results.get(0).key.get());
    }

    @Test
    public void shouldSeeStagedDeleteInRange() throws RocksDBException {
        // Commit a base value to new CF via a direct write, then stage a delete.
        db.put(newCFHandle, wOptions, key("b").get(), val("committed-b"));

        accessor.put(txnAccessor, key("b").get(), null);

        final List<KeyValue<Bytes, byte[]>> results = drain(accessor.all(txnAccessor, true));

        assertTrue(results.isEmpty(), "staged delete should suppress the committed value");
    }

    @Test
    public void shouldSeeStagedDeleteRangeInRange() throws RocksDBException {
        // Commit base values to new CF, then stage a deleteRange.
        db.put(newCFHandle, wOptions, key("a").get(), val("a"));
        db.put(newCFHandle, wOptions, key("b").get(), val("b"));
        db.put(newCFHandle, wOptions, key("c").get(), val("c"));

        accessor.deleteRange(txnAccessor, key("a").get(), key("c").get());

        final List<KeyValue<Bytes, byte[]>> results = drain(accessor.all(txnAccessor, true));

        assertEquals(1, results.size(), "keys a and b should be hidden by the staged range tombstone");
        assertArrayEquals(key("c").get(), results.get(0).key.get());
    }

    @Test
    public void shouldPreferStagedNewValueOverCommittedOldValue() throws RocksDBException {
        // Populate old CF only with a committed entry.
        db.put(oldCFHandle, wOptions, key("x").get(), val("old-x"));

        // Migrate: put() stages delete on old CF and put on new CF.
        accessor.put(txnAccessor, key("x").get(), val("new-x"));

        final List<KeyValue<Bytes, byte[]>> results = drain(accessor.all(txnAccessor, true));

        assertEquals(1, results.size(), "same key should appear exactly once");
        assertArrayEquals(key("x").get(), results.get(0).key.get());
        assertArrayEquals(val("new-x"), results.get(0).value,
                "staged new-format value should win over committed old-format value");
    }

    @Test
    public void shouldHideStagedDeleteOfUnmigratedKey() throws RocksDBException {
        // Key lives only in the old CF (unmigrated). Stage put(key, null), which issues
        // delete(oldCF, key) + delete(newCF, key). Both go into the shared read buffer, so
        // scans on either CF see the staged tombstone and suppress the pre-delete base value.
        db.put(oldCFHandle, wOptions, key("x").get(), val("old-x"));

        accessor.put(txnAccessor, key("x").get(), null);

        final List<KeyValue<Bytes, byte[]>> results = drain(accessor.all(txnAccessor, true));

        assertTrue(results.isEmpty(),
                "staged old-CF tombstone should hide the unmigrated key from scans");
    }

    @Test
    public void shouldSeeCommittedValuesAfterCommit() throws RocksDBException {
        accessor.put(txnAccessor, key("a").get(), val("a"));
        accessor.put(txnAccessor, key("b").get(), val("b"));
        txnAccessor.commitStagedWrites();

        // After commit the values are in RocksDB; a fresh scan should return both.
        final List<KeyValue<Bytes, byte[]>> results = drain(accessor.all(txnAccessor, true));

        assertEquals(2, results.size());
        assertArrayEquals(key("a").get(), results.get(0).key.get());
        assertArrayEquals(key("b").get(), results.get(1).key.get());
        assertFalse(results.isEmpty());
    }

    // --- Point get() read-your-writes tests ---
    //
    // These tests specifically exercise the get() code path through
    // TransactionalDBAccessor.get(), which checks the staged-write buffer
    // for any CF except the offsets CF. The buffer is keyed by key only
    // (no CF), so it must be consulted for both oldCF and newCF reads.

    @Test
    public void shouldSeeStagedPutViaGet() throws RocksDBException {
        accessor.put(txnAccessor, key("a").get(), val("new-a"));

        assertArrayEquals(val("new-a"), accessor.get(txnAccessor, key("a").get()),
                "staged put should be visible through get() before commit");
    }

    @Test
    public void shouldSeeStagedDeleteViaGet() throws RocksDBException {
        // Commit a base value to the new CF, then stage a delete.
        db.put(newCFHandle, wOptions, key("a").get(), val("committed-a"));

        accessor.put(txnAccessor, key("a").get(), null);

        assertNull(accessor.get(txnAccessor, key("a").get()),
                "staged delete should be visible through get() before commit");
    }

    @Test
    public void shouldPreferStagedNewValueOverCommittedOldValueViaGet() throws RocksDBException {
        // Key exists only in old CF (unmigrated), then we stage a put to migrate it.
        db.put(oldCFHandle, wOptions, key("x").get(), val("old-x"));

        accessor.put(txnAccessor, key("x").get(), val("new-x"));

        assertArrayEquals(val("new-x"), accessor.get(txnAccessor, key("x").get()),
                "staged new-format value should win over committed old-format value via get()");
    }

    @Test
    public void shouldHideStagedDeleteOfUnmigratedKeyViaGet() throws RocksDBException {
        // Key lives only in old CF. Stage put(key, null) which tombstones both CFs.
        db.put(oldCFHandle, wOptions, key("x").get(), val("old-x"));

        accessor.put(txnAccessor, key("x").get(), null);

        assertNull(accessor.get(txnAccessor, key("x").get()),
                "staged old-CF tombstone should hide the unmigrated key via get()");
    }

    @Test
    public void shouldReturnNullForAbsentKeyViaGet() throws RocksDBException {
        assertNull(accessor.get(txnAccessor, key("absent").get()),
                "get() for a key with no staged write and no committed value should return null");
    }
}
