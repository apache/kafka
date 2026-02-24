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
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.state.KeyValueIterator;

import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        final String key = "key";
        final String value = "timestampedWithHeaders";
        // prepare store
        rocksDBStore.init(context, rocksDBStore);
        rocksDBStore.put(new Bytes(key.getBytes()), value.getBytes());
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
            assertEquals(value.getBytes().length, db.get(headersColumnFamily, "key".getBytes()).length);
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
    public void shouldFailToUpgradeDirectlyFromKeyValueStore() {
        // Prepare a plain key-value store (with data in default column family)
        prepareKeyValueStore();

        // Try to open with RocksDBTimestampedStoreWithHeaders - should throw exception
        final ProcessorStateException exception = assertThrows(ProcessorStateException.class,
            () -> rocksDBStore.init(context, rocksDBStore));

        assertTrue(exception.getMessage().contains("Cannot upgrade directly from key-value store to headers-aware store"));
        assertTrue(exception.getMessage().contains("Please first upgrade to RocksDBTimestampedStore"));
    }

    @Test
    public void shouldMigrateFromTimestampedToHeadersAwareColumnFamily() throws Exception {
        prepareTimestampedStore();

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

        rocksDBStore.put(new Bytes("key2".getBytes()), "headers+timestamp+22".getBytes());
        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 5 entries on legacy CF, 2 in headers-aware CF after migrating key2 with put()");

        rocksDBStore.put(new Bytes("key3".getBytes()), null);
        // count is off by one, due to two delete operations (even if one does not delete anything)
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 4 entries on legacy CF, 1 in headers-aware CF after deleting key3 with put()");

        rocksDBStore.put(new Bytes("key8new".getBytes()), "headers+timestamp+88888888".getBytes());
        // one delete on old CF, one put on new CF, but count is off by one due to delete on old CF not deleting anything
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 3 entries on legacy CF, 2 in headers-aware CF after adding new key8new with put()");

        // putIfAbsent() - tests migration on conditional write

        assertNull(rocksDBStore.putIfAbsent(new Bytes("key11new".getBytes()), "headers+timestamp+11111111111".getBytes()),
            "Expected null return value for putIfAbsent on non-existing key11new, and new key should be added to headers-aware CF");
        // one delete on old CF, one put on new CF, but count is off by one due to delete on old CF not deleting anything
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 2 entries on legacy CF, 3 in headers-aware CF after adding new key11new with putIfAbsent()");

        assertEquals(1 + 0 + 8 + 5, rocksDBStore.putIfAbsent(new Bytes("key5".getBytes()), null).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for putIfAbsent with null on existing key5");
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 1 entry on legacy CF, 4 in headers-aware CF after migrating key5 with putIfAbsent(null)");

        assertNull(rocksDBStore.putIfAbsent(new Bytes("key12new".getBytes()), null));
        // two delete operation, however, only one is counted because old CF count can not be less than 0
        assertEquals(3L, rocksDBStore.approximateNumEntries(), "Expected 0 entries on legacy CF, 3 in headers-aware CF after putIfAbsent with null on non-existing key12new");

        // delete() - tests migration on delete

        assertEquals(1 + 0 + 8 + 6, rocksDBStore.delete(new Bytes("key6".getBytes())).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(6) = 15 bytes for delete() on existing key6");
        // two delete operation, however, only one is counted because old CF count was zero before already
        assertEquals(2L, rocksDBStore.approximateNumEntries(), "Expected 0 entries on legacy CF, 2 in headers-aware CF after deleting key6 with delete()");

        // iterators should not trigger migration (read-only)
        iteratorsShouldNotMigrateData();
        assertEquals(2L, rocksDBStore.approximateNumEntries());

        rocksDBStore.close();

        // Verify the final state of both column families
        verifyOldAndNewColumnFamily();
    }

    private void iteratorsShouldNotMigrateData() {
        // iterating should not migrate any data, but return all keys over both CFs
        // Values from legacy CF are converted to header-aware format on the fly:  1 byte + [timestamp][value]
        try (final KeyValueIterator<Bytes, byte[]> itAll = rocksDBStore.all()) {
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get()); // migrated by get()
                assertEquals(10, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(1) = 10 bytes for key1 from headers-aware CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key11new".getBytes(), keyValue.key.get()); // inserted by putIfAbsent()
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get()); // migrated by put()
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get()); // not migrated since not accessed, should still be in legacy format: [timestamp(8)][value], with 1 byte of varint, but without headers
                assertEquals(13, keyValue.value.length,
                    "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(4) = 13 bytes for key4 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get()); // migrated by putIfAbsent with null value, should be in header-aware format but with former value: varint(1) + empty headers(0) + timestamp(8) + value(5) = 14 bytes
                assertEquals(14, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for key5 from headers-aware CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get()); // not migrated since not accessed, should still be in legacy format: [timestamp(8)][value], with 1 byte of varint, but without headers
                assertEquals(16, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(7) = 16 bytes for key7 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8new".getBytes(), keyValue.key.get()); // inserted by put()
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '8', '8', '8', '8', '8', '8', '8', '8'}, keyValue.value);
            }
            assertFalse(itAll.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it =
                          rocksDBStore.range(new Bytes("key2".getBytes()), new Bytes("key5".getBytes()))) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertEquals(13, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(4) = 13 bytes for key4 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertEquals(14, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for key5 from headers-aware CF");
            }
            assertFalse(it.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> itAll = rocksDBStore.reverseAll()) {
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8new".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '8', '8', '8', '8', '8', '8', '8', '8'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get());
                assertEquals(16, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(7) = 16 bytes for key7 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertEquals(14, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for key5 from headers-aware CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertEquals(13, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(4) = 13 bytes for key4 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key11new".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                assertEquals(10, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(1) = 10 bytes for key1 from headers-aware CF");
            }
            assertFalse(itAll.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it =
                          rocksDBStore.reverseRange(new Bytes("key2".getBytes()), new Bytes("key5".getBytes()))) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertEquals(14, keyValue.value.length, "Expected header-aware format: varint(0) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for key5 from headers-aware CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertEquals(13, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(4) = 13 bytes for key4 from legacy CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2'}, keyValue.value);
            }
            assertFalse(it.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it = rocksDBStore.prefixScan("key1", stringSerializer)) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                assertEquals(10, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(1) = 10 bytes for key1 from headers-aware CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key11new".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1'}, keyValue.value);
            }
            assertFalse(it.hasNext());
        }
    }

    private void verifyOldAndNewColumnFamily() throws Exception {
        // In upgrade scenario from RocksDBTimestampedStore,
        // we expect 3 CFs: DEFAULT (closed on open), keyValueWithTimestamp (legacy), keyValueWithTimestampAndHeaders (new)
        verifyColumnFamilyContents();
        verifyStillInUpgradeMode();
        clearLegacyColumnFamily();
        verifyLegacyColumnFamilyDropped();
        verifyInHeadersAwareMode();
    }

    private void verifyColumnFamilyContents() throws Exception {
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8), columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));

        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());
        RocksDB db = null;
        ColumnFamilyHandle defaultColumnFamily = null;
        ColumnFamilyHandle legacyTimestampedColumnFamily = null;
        ColumnFamilyHandle headersColumnFamily = null;
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
        } finally {
            closeColumnFamilies(db, defaultColumnFamily, legacyTimestampedColumnFamily, headersColumnFamily);
            dbOptions.close();
            columnFamilyOptions.close();
        }
    }

    private void verifyDefaultColumnFamily(final RocksDB db, final ColumnFamilyHandle defaultColumnFamily) {
        // DEFAULT CF should be empty (closed on open)
        try (final RocksIterator iterator = db.newIterator(defaultColumnFamily)) {
            iterator.seekToFirst();
            assertFalse(iterator.isValid(), "Expected no keys in default CF");
        }
    }

    private void verifyLegacyTimestampedColumnFamily(final RocksDB db, final ColumnFamilyHandle legacyTimestampedColumnFamily) throws Exception {
        // Legacy timestamped CF should have migrated keys as null, un-migrated as timestamped values
        assertNull(db.get(legacyTimestampedColumnFamily, "unknown".getBytes()));
        assertNull(db.get(legacyTimestampedColumnFamily, "key1".getBytes())); // migrated
        assertNull(db.get(legacyTimestampedColumnFamily, "key2".getBytes())); // migrated
        assertNull(db.get(legacyTimestampedColumnFamily, "key3".getBytes())); // deleted
        assertEquals(8 + 4, db.get(legacyTimestampedColumnFamily, "key4".getBytes()).length); // not migrated
        assertNull(db.get(legacyTimestampedColumnFamily, "key5".getBytes())); // migrated
        assertNull(db.get(legacyTimestampedColumnFamily, "key6".getBytes())); // migrated
        assertEquals(8 + 7, db.get(legacyTimestampedColumnFamily, "key7".getBytes()).length); // not migrated
        assertNull(db.get(legacyTimestampedColumnFamily, "key8new".getBytes()));
        assertNull(db.get(legacyTimestampedColumnFamily, "key11new".getBytes()));
    }

    private void verifyHeadersColumnFamily(final RocksDB db, final ColumnFamilyHandle headersColumnFamily) throws Exception {
        // Headers CF should have all migrated/new keys with header-aware format
        assertNull(db.get(headersColumnFamily, "unknown".getBytes()));
        assertEquals(1 + 0 + 8 + 1, db.get(headersColumnFamily, "key1".getBytes()).length); // migrated by get()
        assertEquals("headers+timestamp+22".getBytes().length, db.get(headersColumnFamily, "key2".getBytes()).length); // migrated by put() => value is inserted without any conversion
        assertNull(db.get(headersColumnFamily, "key3".getBytes())); // migrated by put() with null value => deleted
        assertNull(db.get(headersColumnFamily, "key4".getBytes())); // not migrated, should still be in legacy column family
        assertEquals(1 + 0 + 8 + 5, db.get(headersColumnFamily, "key5".getBytes()).length); // migrated by putIfAbsent with null value, should be in header-aware format but with former value
        assertNull(db.get(headersColumnFamily, "key6".getBytes())); // migrated by delete() => deleted
        assertNull(db.get(headersColumnFamily, "key7".getBytes())); // not migrated, should still be in legacy column family
        assertEquals("headers+timestamp+88888888".getBytes().length, db.get(headersColumnFamily, "key8new".getBytes()).length); // added by put() => value is inserted without any conversion
        assertEquals("headers+timestamp+11111111111".getBytes().length, db.get(headersColumnFamily, "key11new".getBytes()).length); // inserted (newly added) by putIfAbsent() => value is inserted without any conversion
        assertNull(db.get(headersColumnFamily, "key12new".getBytes())); // putIfAbsent with null value on non-existing key should not create any entry
    }

    private void closeColumnFamilies(
            final RocksDB db,
            final ColumnFamilyHandle defaultColumnFamily,
            final ColumnFamilyHandle legacyTimestampedColumnFamily,
            final ColumnFamilyHandle headersColumnFamily) {
        // Order of closing must follow: ColumnFamilyHandle > RocksDB
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

    private void clearLegacyColumnFamily() throws Exception {
        // clear legacy timestamped CF by deleting remaining keys
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8), columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));

        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());
        RocksDB db = null;
        ColumnFamilyHandle defaultCF = null;
        ColumnFamilyHandle legacyCF = null;
        ColumnFamilyHandle headersCF = null;
        try {
            db = RocksDB.open(
                    dbOptions,
                    new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                    columnFamilyDescriptors,
                    columnFamilies);

            defaultCF = columnFamilies.get(0);
            legacyCF = columnFamilies.get(1);
            headersCF = columnFamilies.get(2);
            db.delete(legacyCF, "key4".getBytes());
            db.delete(legacyCF, "key7".getBytes());
        } finally {
            // Order of closing must follow: ColumnFamilyHandle > RocksDB > DBOptions > ColumnFamilyOptions
            if (defaultCF != null) {
                defaultCF.close();
            }
            if (legacyCF != null) {
                legacyCF.close();
            }
            if (headersCF != null) {
                headersCF.close();
            }
            if (db != null) {
                db.close();
            }
            dbOptions.close();
            columnFamilyOptions.close();
        }
    }

    private void verifyLegacyColumnFamilyDropped() throws Exception {
        // Open and close the store to trigger the legacy CF drop (when it detects empty legacy CF)
        rocksDBStore.init(context, rocksDBStore);
        rocksDBStore.close();

        // Verify that the legacy column family no longer exists
        try (DBOptions dbOptions = new DBOptions(); final Options options = new Options(dbOptions, new ColumnFamilyOptions())) {
            final List<byte[]> existingCFs = RocksDB.listColumnFamilies(
                options,
                new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath()
            );

            // Should only have DEFAULT and keyValueWithTimestampAndHeaders, not the legacy keyValueWithTimestamp
            assertEquals(2, existingCFs.size(), "Expected only 2 column families after legacy CF is dropped");

            boolean hasDefault = false;
            boolean hasHeadersAware = false;
            boolean hasLegacy = false;

            for (final byte[] cf : existingCFs) {
                if (java.util.Arrays.equals(cf, RocksDB.DEFAULT_COLUMN_FAMILY)) {
                    hasDefault = true;
                } else if (java.util.Arrays.equals(cf, "keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8))) {
                    hasHeadersAware = true;
                } else if (java.util.Arrays.equals(cf, "keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8))) {
                    hasLegacy = true;
                }
            }

            assertTrue(hasDefault, "Expected default column family to exist");
            assertTrue(hasHeadersAware, "Expected headers-aware column family to exist");
            assertFalse(hasLegacy, "Expected legacy column family to be dropped");
        }
    }

    private void verifyInHeadersAwareMode() {
        // check that now in regular header-aware mode (all legacy data migrated)
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in regular headers-aware mode"));
        }
    }

    private void prepareKeyValueStore() {
        // Create a plain RocksDBStore (key-value, not timestamped) with data in default column family
        final RocksDBStore kvStore = new RocksDBStore(DB_NAME, METRICS_SCOPE);
        try {
            kvStore.init(context, kvStore);

            // Write plain key-value pairs to default column family
            kvStore.put(new Bytes("key1".getBytes()), "value1".getBytes());
            kvStore.put(new Bytes("key2".getBytes()), "value2".getBytes());
        } finally {
            kvStore.close();
        }
    }

    private void prepareTimestampedStore() {
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

    @Test
    public void shouldThrowUnsupportedOperationExceptionOnQuery() {
        rocksDBStore.init(context, rocksDBStore);

        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(new Bytes("test".getBytes()));

        final UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                () -> rocksDBStore.query(query, PositionBound.unbounded(), new QueryConfig(false))
        );

        assertTrue(exception.getMessage().contains("Queries (IQv2) are not supported for timestamped key-value stores with headers yet."));
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
