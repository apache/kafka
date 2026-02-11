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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RocksDBTimestampedStoreWithHeadersTest extends RocksDBStoreTest {

    private final Serializer<String> stringSerializer = new StringSerializer();

    RocksDBStore getRocksDBStore() {
        return new RocksDBTimestampedStoreWithHeaders(DB_NAME, METRICS_SCOPE);
    }

    @Test
    public void shouldOpenNewStoreInRegularMode() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in regular headers-aware mode"));
        }

        try (final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all()) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldOpenExistingStoreInRegularMode() throws Exception {
        // prepare store
        rocksDBStore.init(context, rocksDBStore);
        rocksDBStore.put(new Bytes("key".getBytes()), "timestampedWithHeaders".getBytes());
        rocksDBStore.close();

        // re-open store
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in regular headers-aware mode"));
        } finally {
            rocksDBStore.close();
        }

        // verify store
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());

        RocksDB db = null;
        ColumnFamilyHandle defaultColumnFamily = null, headersColumnFamily = null;
        try {
            db = RocksDB.open(
                    dbOptions,
                    new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                    columnFamilyDescriptors,
                    columnFamilies);

            defaultColumnFamily = columnFamilies.get(0);
            headersColumnFamily = columnFamilies.get(1);

            assertNull(db.get(defaultColumnFamily, "key".getBytes()));
            assertEquals(0L, db.getLongProperty(defaultColumnFamily, "rocksdb.estimate-num-keys"));
            assertEquals(22, db.get(headersColumnFamily, "key".getBytes()).length);
            assertEquals(1L, db.getLongProperty(headersColumnFamily, "rocksdb.estimate-num-keys"));
        } finally {
            // Order of closing must follow: ColumnFamilyHandle > RocksDB > DBOptions > ColumnFamilyOptions
            if (defaultColumnFamily != null) {
                defaultColumnFamily.close();
            }
            if (headersColumnFamily != null) {
                headersColumnFamily.close();
            }
            if (db != null) {
                db.close();
            }
            dbOptions.close();
            columnFamilyOptions.close();
        }
    }

    @Test
    public void shouldMigrateFromTimestampedToHeadersAwareColumnFamily() throws Exception {
        prepareOldStore();

        // Open with RocksDBTimestampedStoreWithHeaders - should detect legacy CF and enter upgrade mode
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in upgrade mode"));
        }

        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 7 entries in legacy CF and 0 in headers-aware CF before migration");

        // get() - tests lazy migration on read

        assertNull(rocksDBStore.get(new Bytes("unknown".getBytes())), "Expected null for unknown key");
        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 7 entries on legacy CF, 0 in headers-aware CF");

        assertEquals(1 + 0 + 8 + 1, rocksDBStore.get(new Bytes("key1".getBytes())).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(1) = 10 bytes");
        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 6 entries on legacy CF, 1 in headers-aware CF after migrating key1");

        // put() - tests migration on write

        rocksDBStore.put(new Bytes("key2".getBytes()), "timestamp+22".getBytes());
        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 5 entries on legacy CF, 2 in headers-aware CF after migrating key2 with put()");

        rocksDBStore.put(new Bytes("key3".getBytes()), null);
        // count is off by one, due to two delete operations (even if one does not delete anything)
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 4 entries on legacy CF, 1 in headers-aware CF after deleting key3 with put()");

        rocksDBStore.put(new Bytes("key8".getBytes()), "headers+timestamp+88888888".getBytes());
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 3 entries on legacy CF, 2 in headers-aware CF after adding new key8 with put()");

        // putIfAbsent() - tests migration on conditional write

        assertEquals(1 + 0 + 8 + 4,
            rocksDBStore.putIfAbsent(new Bytes("key4".getBytes()), "headers+timestamp+4444".getBytes()).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(4) = 13 bytes");
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 2 entries on legacy CF, 3 in headers-aware CF after migrating key4 with putIfAbsent()");

        assertNull(rocksDBStore.putIfAbsent(new Bytes("key11".getBytes()), "headers+timestamp+11111111111".getBytes()),
            "Expected null return value for putIfAbsent on non-existing key11, and new key should be added to headers-aware CF");
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 1 entry on legacy CF, 4 in headers-aware CF after adding new key11 with putIfAbsent()");

        assertEquals(1 + 0 + 8 + 5, rocksDBStore.putIfAbsent(new Bytes("key5".getBytes()), null).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for putIfAbsent with null on existing key5");
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 0 entries on legacy CF, 5 in headers-aware CF after migrating key5 with putIfAbsent(null)");

        assertNull(rocksDBStore.putIfAbsent(new Bytes("key12".getBytes()), null));
        assertEquals(4L, rocksDBStore.approximateNumEntries(), "Expected 0 entries on legacy CF, 4 in headers-aware CF after putIfAbsent with null on non-existing key12");

        // delete() - tests migration on delete

        assertEquals(1 + 0 + 8 + 6, rocksDBStore.delete(new Bytes("key6".getBytes())).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(6) = 15 bytes for delete() on existing key6");
        assertEquals(3L, rocksDBStore.approximateNumEntries(), "Expected 0 entries on legacy CF, 3 in headers-aware CF after deleting key6 with delete()");

        // iterators should not trigger migration (read-only)
        iteratorsShouldNotMigrateData();
        assertEquals(3L, rocksDBStore.approximateNumEntries());

        rocksDBStore.close();

        // Verify the final state of both column families
        verifyOldAndNewColumnFamily();
    }

    private void iteratorsShouldNotMigrateData() {
        // iterating should not migrate any data, but return all keys over both CFs
        // Values from legacy CF are converted to header-aware format: [varint][headers][timestamp][value]
        try (final KeyValueIterator<Bytes, byte[]> itAll = rocksDBStore.all()) {
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                assertEquals(10, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(1) = 10 bytes for key1 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key11".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'t', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertEquals(13, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(4) = 13 bytes for key4 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertEquals(14, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for key5 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get());
                assertEquals(16, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(7) = 16 bytes for key7 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '8', '8', '8', '8', '8', '8', '8', '8'}, keyValue.value);
            }
            assertFalse(itAll.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it =
                          rocksDBStore.range(new Bytes("key2".getBytes()), new Bytes("key5".getBytes()))) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'t', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertEquals(13, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(4) = 13 bytes for key4 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertEquals(14, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for key5 from legacy CF");
            }
            assertFalse(it.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> itAll = rocksDBStore.reverseAll()) {
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '8', '8', '8', '8', '8', '8', '8', '8'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get());
                assertEquals(16, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(7) = 16 bytes for key7 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertEquals(14, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for key5 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertEquals(13, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(4) = 13 bytes for key4 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'t', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key11".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                assertEquals(10, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(1) = 10 bytes for key1 from legacy CF");
            }
            assertFalse(itAll.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it =
                          rocksDBStore.reverseRange(new Bytes("key2".getBytes()), new Bytes("key5".getBytes()))) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertEquals(14, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for key5 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertEquals(13, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(4) = 13 bytes for key4 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'t', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2'}, keyValue.value);
            }
            assertFalse(it.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it = rocksDBStore.prefixScan("key1", stringSerializer)) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                assertEquals(10, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(1) = 10 bytes for key1 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key11".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1'}, keyValue.value);
            }
            assertFalse(it.hasNext());
        }
    }

    private void verifyOldAndNewColumnFamily() throws Exception {
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        // In upgrade scenario from RocksDBTimestampedStore,
        // we expect 3 CFs: DEFAULT (closed on open), keyValueWithTimestamp (legacy), keyValueWithTimestampAndHeaders (new)
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8), columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));

        verifyColumnFamilyContents(dbOptions, columnFamilyDescriptors, columnFamilyOptions);
        verifyStillInUpgradeMode();
        clearLegacyColumnFamily(dbOptions, columnFamilyDescriptors, columnFamilyOptions);
        verifyInHeadersAwareMode();
    }

    private void verifyColumnFamilyContents(
            final DBOptions dbOptions,
            final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
            final ColumnFamilyOptions columnFamilyOptions) throws Exception {
        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());
        RocksDB db = null;
        ColumnFamilyHandle defaultColumnFamily = null;
        ColumnFamilyHandle legacyTimestampedColumnFamily = null;
        ColumnFamilyHandle headersColumnFamily = null;
        boolean errorOccurred = false;
        try {
            db = RocksDB.open(
                    dbOptions,
                    new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                    columnFamilyDescriptors,
                    columnFamilies);

            defaultColumnFamily = columnFamilies.get(0);
            legacyTimestampedColumnFamily = columnFamilies.get(1);
            headersColumnFamily = columnFamilies.get(2);

            verifyDefaultColumnFamily(db, defaultColumnFamily);
            verifyLegacyTimestampedColumnFamily(db, legacyTimestampedColumnFamily);
            verifyHeadersColumnFamily(db, headersColumnFamily);
        } catch (final RuntimeException fatal) {
            errorOccurred = true;
        } finally {
            closeColumnFamilies(db, defaultColumnFamily, legacyTimestampedColumnFamily, headersColumnFamily,
                    dbOptions, columnFamilyOptions, errorOccurred);
        }
    }

    private void verifyDefaultColumnFamily(final RocksDB db, final ColumnFamilyHandle defaultColumnFamily) throws Exception {
        // DEFAULT CF should be empty (closed on open)
        assertNull(db.get(defaultColumnFamily, "unknown".getBytes()));
        assertNull(db.get(defaultColumnFamily, "key1".getBytes()));
    }

    private void verifyLegacyTimestampedColumnFamily(final RocksDB db, final ColumnFamilyHandle legacyTimestampedColumnFamily) throws Exception {
        // Legacy timestamped CF should have migrated keys as null, un-migrated as timestamped values
        assertNull(db.get(legacyTimestampedColumnFamily, "unknown".getBytes()));
        assertNull(db.get(legacyTimestampedColumnFamily, "key1".getBytes())); // migrated
        assertNull(db.get(legacyTimestampedColumnFamily, "key2".getBytes())); // migrated
        assertNull(db.get(legacyTimestampedColumnFamily, "key3".getBytes())); // deleted
        assertNull(db.get(legacyTimestampedColumnFamily, "key4".getBytes())); // migrated
        assertNull(db.get(legacyTimestampedColumnFamily, "key5".getBytes())); // migrated
        assertNull(db.get(legacyTimestampedColumnFamily, "key6".getBytes())); // migrated
        assertEquals(8 + 7, db.get(legacyTimestampedColumnFamily, "key7".getBytes()).length); // not migrated
        assertNull(db.get(legacyTimestampedColumnFamily, "key8".getBytes()));
    }

    private void verifyHeadersColumnFamily(final RocksDB db, final ColumnFamilyHandle headersColumnFamily) throws Exception {
        // Headers CF should have all migrated/new keys with header-aware format
        assertNull(db.get(headersColumnFamily, "unknown".getBytes()));
        assertEquals(1 + 0 + 8 + 1, db.get(headersColumnFamily, "key1".getBytes()).length); // varint + headers + ts + value
        assertEquals(12, db.get(headersColumnFamily, "key2".getBytes()).length);
        assertNull(db.get(headersColumnFamily, "key3".getBytes()));
        assertEquals(1 + 0 + 8 + 4, db.get(headersColumnFamily, "key4".getBytes()).length);
        assertEquals(1 + 0 + 8 + 5, db.get(headersColumnFamily, "key5".getBytes()).length);
        assertNull(db.get(headersColumnFamily, "key6".getBytes()));
        assertNull(db.get(headersColumnFamily, "key7".getBytes()));
        assertEquals(26, db.get(headersColumnFamily, "key8".getBytes()).length);
        assertEquals(29, db.get(headersColumnFamily, "key11".getBytes()).length);
        assertNull(db.get(headersColumnFamily, "key12".getBytes()));
    }

    private void closeColumnFamilies(
            final RocksDB db,
            final ColumnFamilyHandle defaultColumnFamily,
            final ColumnFamilyHandle legacyTimestampedColumnFamily,
            final ColumnFamilyHandle headersColumnFamily,
            final DBOptions dbOptions,
            final ColumnFamilyOptions columnFamilyOptions,
            final boolean errorOccurred) {
        // Order of closing must follow: ColumnFamilyHandle > RocksDB > DBOptions > ColumnFamilyOptions
        if (defaultColumnFamily != null) {
            defaultColumnFamily.close();
        }
        if (legacyTimestampedColumnFamily != null) {
            legacyTimestampedColumnFamily.close();
        }
        if (headersColumnFamily != null) {
            headersColumnFamily.close();
        }
        if (db != null) {
            db.close();
        }
        if (errorOccurred) {
            dbOptions.close();
            columnFamilyOptions.close();
        }
    }

    private void verifyStillInUpgradeMode() {
        // check that still in upgrade mode
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in upgrade mode"));
        } finally {
            rocksDBStore.close();
        }
    }

    private void clearLegacyColumnFamily(
            final DBOptions dbOptions,
            final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
            final ColumnFamilyOptions columnFamilyOptions) throws Exception {
        // clear legacy timestamped CF by deleting remaining key
        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());
        RocksDB db = null;
        ColumnFamilyHandle defaultCF2 = null;
        ColumnFamilyHandle legacyCF2 = null;
        ColumnFamilyHandle headersCF2 = null;
        try {
            db = RocksDB.open(
                    dbOptions,
                    new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                    columnFamilyDescriptors,
                    columnFamilies);

            defaultCF2 = columnFamilies.get(0);
            legacyCF2 = columnFamilies.get(1);
            headersCF2 = columnFamilies.get(2);
            db.delete(legacyCF2, "key7".getBytes());
        } finally {
            // Order of closing must follow: ColumnFamilyHandle > RocksDB > DBOptions > ColumnFamilyOptions
            if (defaultCF2 != null) {
                defaultCF2.close();
            }
            if (legacyCF2 != null) {
                legacyCF2.close();
            }
            if (headersCF2 != null) {
                headersCF2.close();
            }
            if (db != null) {
                db.close();
            }
            dbOptions.close();
            columnFamilyOptions.close();
        }
    }

    private void verifyInHeadersAwareMode() {
        // check that now in regular header-aware mode (all legacy data migrated)
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in regular headers-aware mode"));
        }
    }

    private void prepareOldStore() {
        // Create a legacy RocksDBTimestampedStore to test upgrade scenario
        final RocksDBTimestampedStore timestampedStore = new RocksDBTimestampedStore(DB_NAME, METRICS_SCOPE);
        try {
            timestampedStore.init(context, timestampedStore);

            // Write timestamped values (timestamp = -1 for unknown timestamp)
            timestampedStore.put(new Bytes("key1".getBytes()), wrapTimestampedValue("1".getBytes()));
            timestampedStore.put(new Bytes("key2".getBytes()), wrapTimestampedValue("22".getBytes()));
            timestampedStore.put(new Bytes("key3".getBytes()), wrapTimestampedValue("333".getBytes()));
            timestampedStore.put(new Bytes("key4".getBytes()), wrapTimestampedValue("4444".getBytes()));
            timestampedStore.put(new Bytes("key5".getBytes()), wrapTimestampedValue("55555".getBytes()));
            timestampedStore.put(new Bytes("key6".getBytes()), wrapTimestampedValue("666666".getBytes()));
            timestampedStore.put(new Bytes("key7".getBytes()), wrapTimestampedValue("7777777".getBytes()));
        } finally {
            timestampedStore.close();
        }
    }

    private byte[] wrapTimestampedValue(final byte[] value) {
        // Format: [timestamp(8 bytes)][value]
        // Use the numeric value as timestamp
        final long timestamp = Long.parseLong(new String(value));
        final byte[] result = new byte[8 + value.length];

        // Convert timestamp to big-endian 8-byte array
        result[0] = (byte) (timestamp >> 56);
        result[1] = (byte) (timestamp >> 48);
        result[2] = (byte) (timestamp >> 40);
        result[3] = (byte) (timestamp >> 32);
        result[4] = (byte) (timestamp >> 24);
        result[5] = (byte) (timestamp >> 16);
        result[6] = (byte) (timestamp >> 8);
        result[7] = (byte) timestamp;

        System.arraycopy(value, 0, result, 8, value.length);
        return result;
    }
}
