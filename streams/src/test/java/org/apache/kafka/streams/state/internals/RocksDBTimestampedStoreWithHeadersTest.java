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
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
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
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.kafka.streams.state.internals.RocksDBStore.OFFSETS_COLUMN_FAMILY_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
                new ColumnFamilyDescriptor("keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions),
                new ColumnFamilyDescriptor(OFFSETS_COLUMN_FAMILY_NAME, columnFamilyOptions));
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
    public void shouldMigrateFromPlainToHeadersAwareColumnFamily() throws Exception {
        prepareKeyValueStoreWithMultipleKeys();

        // Open with RocksDBTimestampedStoreWithHeaders - should detect DEFAULT CF and enter upgrade mode
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in upgrade mode from plain key value store"));
        }

        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 7 entries in DEFAULT CF and 0 in headers-aware CF before migration");

        // get() - tests lazy migration on read

        assertNull(rocksDBStore.get(new Bytes("unknown".getBytes())), "Expected null for unknown key");
        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 7 entries on DEFAULT CF, 0 in headers-aware CF");

        assertEquals(1 + 0 + 8 + 1, rocksDBStore.get(new Bytes("key1".getBytes())).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(1) = 10 bytes");
        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 6 entries on DEFAULT CF, 1 in headers-aware CF after migrating key1");

        // put() - tests migration on write

        rocksDBStore.put(new Bytes("key2".getBytes()), "headers+timestamp+22".getBytes());
        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 5 entries on DEFAULT CF, 2 in headers-aware CF after migrating key2 with put()");

        rocksDBStore.put(new Bytes("key3".getBytes()), null);
        // count is off by one, due to two delete operations (even if one does not delete anything)
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 4 entries on DEFAULT CF, 1 in headers-aware CF after deleting key3 with put()");

        rocksDBStore.put(new Bytes("key8new".getBytes()), "headers+timestamp+88888888".getBytes());
        // one delete on old CF, one put on new CF, but count is off by one due to delete on old CF not deleting anything
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 4 entries on DEFAULT CF, 2 in headers-aware CF after adding new key8new with put()");

        rocksDBStore.put(new Bytes("key9new".getBytes()), null);
        // one delete on old CF, one put on new CF, but count is off by two due to deletes not deleting anything
        assertEquals(3L, rocksDBStore.approximateNumEntries(), "Expected 4 entries on DEFAULT CF, 1 in headers-aware CF after adding new key9new with put()");

        // putIfAbsent() - tests migration on conditional write

        assertNull(rocksDBStore.putIfAbsent(new Bytes("key11new".getBytes()), "headers+timestamp+11111111111".getBytes()),
            "Expected null return value for putIfAbsent on non-existing key11new, and new key should be added to headers-aware CF");
        // one delete on old CF, one put on new CF, but count is off by one due to delete on old CF not deleting anything
        assertEquals(3L, rocksDBStore.approximateNumEntries(), "Expected 4 entries on DEFAULT CF, 2 in headers-aware CF after adding new key11new with putIfAbsent()");

        assertEquals(1 + 0 + 8 + 5, rocksDBStore.putIfAbsent(new Bytes("key5".getBytes()), null).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for putIfAbsent with null on existing key5");
        // one delete on old CF, one put on new CF, due to `get()` migration
        assertEquals(3L, rocksDBStore.approximateNumEntries(), "Expected 3 entries on DEFAULT CF, 3 in headers-aware CF after migrating key5 with putIfAbsent(null)");

        assertNull(rocksDBStore.putIfAbsent(new Bytes("key12new".getBytes()), null));
        // no delete operation, because key12new is unknown
        assertEquals(3L, rocksDBStore.approximateNumEntries(), "Expected 3 entries on DEFAULT CF, 3 in headers-aware CF after putIfAbsent with null on non-existing key12new");

        // delete() - tests migration on delete

        assertEquals(1 + 0 + 8 + 6, rocksDBStore.delete(new Bytes("key6".getBytes())).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(6) = 15 bytes for delete() on existing key6");
        // two delete operation, however, only one is counted because old CF count was zero before already
        assertEquals(2L, rocksDBStore.approximateNumEntries(), "Expected 2 entries on DEFAULT CF, 2 in headers-aware CF after deleting key6 with delete()");

        // iterators should not trigger migration (read-only)
        iteratorsShouldNotMigrateDataFromPlain();
        assertEquals(2L, rocksDBStore.approximateNumEntries());

        rocksDBStore.close();

        // Verify the final state of both column families
        verifyPlainUpgradeColumnFamilies();
    }

    @Test
    public void shouldUpgradeFromPlainKeyValueStore() throws Exception {
        // Prepare a plain key-value store (with data in default column family)
        prepareKeyValueStore();

        // Open with RocksDBTimestampedStoreWithHeaders - should detect DEFAULT CF and enter upgrade mode
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in upgrade mode from plain key value store"));
        }

        // Verify we can read the migrated data
        assertEquals(1 + 0 + 8 + "value1".getBytes().length, rocksDBStore.get(new Bytes("key1".getBytes())).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value");
        assertEquals(1 + 0 + 8 + "value2".getBytes().length, rocksDBStore.get(new Bytes("key2".getBytes())).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value");

        rocksDBStore.close();

        // Verify column family structure
        verifyPlainStoreUpgrade();
    }

    private void verifyPlainStoreUpgrade() throws Exception {
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor("keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions),
            new ColumnFamilyDescriptor(OFFSETS_COLUMN_FAMILY_NAME, columnFamilyOptions));

        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());
        RocksDB db = null;
        ColumnFamilyHandle defaultCF = null;
        ColumnFamilyHandle headersCF = null;
        try {
            db = RocksDB.open(
                dbOptions,
                new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                columnFamilyDescriptors,
                columnFamilies);

            defaultCF = columnFamilies.get(0);
            headersCF = columnFamilies.get(1);

            // After get() is called, data is migrated and deleted from DEFAULT CF
            // DEFAULT CF should be empty for migrated keys
            assertNull(db.get(defaultCF, "key1".getBytes()), "Expected key1 to be deleted from DEFAULT CF after migration");
            assertNull(db.get(defaultCF, "key2".getBytes()), "Expected key2 to be deleted from DEFAULT CF after migration");

            // Headers CF should have the migrated data
            assertEquals(1 + 0 + 8 + "value1".getBytes().length, db.get(headersCF, "key1".getBytes()).length);
            assertEquals(1 + 0 + 8 + "value2".getBytes().length, db.get(headersCF, "key2".getBytes()).length);
        } finally {
            if (defaultCF != null) {
                defaultCF.close();
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

    @Test
    public void shouldFailWhenBothPlainAndTimestampedDataExist() {
        // This is an invalid state - we can't have both DEFAULT CF with data AND LEGACY_TIMESTAMPED CF
        // Step 1: Create a plain store with two keys
        final RocksDBStore kvStore = new RocksDBStore(DB_NAME, METRICS_SCOPE);
        kvStore.init(context, kvStore);
        kvStore.put(new Bytes("plainKey1".getBytes()), "value1".getBytes());
        kvStore.put(new Bytes("plainKey2".getBytes()), "value2".getBytes());
        kvStore.close();

        // Step 2: Open as timestamped store and migrate one key (simulating partial migration)
        final RocksDBTimestampedStore timestampedStore = new RocksDBTimestampedStore(DB_NAME, METRICS_SCOPE);
        timestampedStore.init(context, timestampedStore);
        timestampedStore.get(new Bytes("plainKey1".getBytes())); // Triggers migration of plainKey1 to LEGACY_TIMESTAMPED CF
        timestampedStore.close();
        // Now we have: plainKey2 in DEFAULT CF, plainKey1 in LEGACY_TIMESTAMPED CF

        // Step 3: Try to open with headers store - should fail
        final ProcessorStateException exception = assertThrows(
            ProcessorStateException.class,
            () -> rocksDBStore.init(context, rocksDBStore)
        );

        assertTrue(exception.getMessage().contains("Inconsistent store state"));
        assertTrue(exception.getMessage().contains("Cannot have both plain (DEFAULT) and timestamped data simultaneously"));
        assertTrue(exception.getMessage().contains("Headers store can upgrade from either plain or timestamped format, but not both."));
    }

    @Test
    public void shouldMigrateFromTimestampedToHeadersAwareColumnFamily() throws Exception {
        prepareTimestampedStore();

        // Open with RocksDBTimestampedStoreWithHeaders - should detect legacy CF and enter upgrade mode
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in upgrade mode from timestamped store"));
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

        rocksDBStore.put(new Bytes("key9new".getBytes()), null);
        // one delete on old CF, one put on new CF, but count is off by two due to deletes not deleting anything
        assertEquals(3L, rocksDBStore.approximateNumEntries(), "Expected 2 entries on legacy CF, 1 in headers-aware CF after adding new key8new with put()");

        // putIfAbsent() - tests migration on conditional write

        assertNull(rocksDBStore.putIfAbsent(new Bytes("key11new".getBytes()), "headers+timestamp+11111111111".getBytes()),
            "Expected null return value for putIfAbsent on non-existing key11new, and new key should be added to headers-aware CF");
        // one delete on old CF, one put on new CF, but count is off by one due to delete on old CF not deleting anything
        assertEquals(3L, rocksDBStore.approximateNumEntries(), "Expected 1 entries on legacy CF, 2 in headers-aware CF after adding new key11new with putIfAbsent()");

        assertEquals(1 + 0 + 8 + 5, rocksDBStore.putIfAbsent(new Bytes("key5".getBytes()), null).length,
            "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for putIfAbsent with null on existing key5");
        // one delete on old CF, one put on new CF, due to `get()` migration
        assertEquals(3L, rocksDBStore.approximateNumEntries(), "Expected 0 entry on legacy CF, 3 in headers-aware CF after migrating key5 with putIfAbsent(null)");

        assertNull(rocksDBStore.putIfAbsent(new Bytes("key12new".getBytes()), null));
        // no delete operation, because key12new is unknown
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
                new ColumnFamilyDescriptor("keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions),
                new ColumnFamilyDescriptor(OFFSETS_COLUMN_FAMILY_NAME, columnFamilyOptions));

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
        assertNull(db.get(legacyTimestampedColumnFamily, "key9new".getBytes()));
        assertNull(db.get(legacyTimestampedColumnFamily, "key11new".getBytes()));
        assertNull(db.get(legacyTimestampedColumnFamily, "key12new".getBytes()));

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
        assertNull(db.get(headersColumnFamily, "key9new".getBytes()));
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
        // check that still in upgrade mode from timestamped store
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in upgrade mode from timestamped store"));
        } finally {
            rocksDBStore.close();
        }
    }

    private void iteratorsShouldNotMigrateDataFromPlain() {
        // iterating should not migrate any data, but return all keys over both CFs
        // Values from DEFAULT CF are converted to header-aware format on the fly: [0x00][timestamp=-1][value]
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
                assertArrayEquals("key4".getBytes(), keyValue.key.get()); // not migrated since not accessed, should be converted on-the-fly from DEFAULT CF
                assertEquals(13, keyValue.value.length,
                    "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(4) = 13 bytes for key4 from DEFAULT CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get()); // migrated by putIfAbsent with null value
                assertEquals(14, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(5) = 14 bytes for key5 from headers-aware CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get()); // not migrated since not accessed
                assertEquals(16, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(7) = 16 bytes for key7 from DEFAULT CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8new".getBytes(), keyValue.key.get()); // inserted by put()
                assertArrayEquals(new byte[]{'h', 'e', 'a', 'd', 'e', 'r', 's', '+', 't', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '8', '8', '8', '8', '8', '8', '8', '8'}, keyValue.value);
            }
            assertFalse(itAll.hasNext());
        }
    }

    private void verifyPlainUpgradeColumnFamilies() throws Exception {
        // In upgrade scenario from plain RocksDBStore,
        // we expect 2 CFs: DEFAULT (legacy), keyValueWithTimestampAndHeaders (new)
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor("keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions),
            new ColumnFamilyDescriptor(OFFSETS_COLUMN_FAMILY_NAME, columnFamilyOptions));

        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());
        RocksDB db = null;
        ColumnFamilyHandle defaultCF = null;
        ColumnFamilyHandle headersCF = null;
        try {
            db = RocksDB.open(
                dbOptions,
                new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                columnFamilyDescriptors,
                columnFamilies);

            defaultCF = columnFamilies.get(0);
            headersCF = columnFamilies.get(1);

            // DEFAULT CF should have migrated keys as null, un-migrated as plain values
            assertNull(db.get(defaultCF, "unknown".getBytes()));
            assertNull(db.get(defaultCF, "key1".getBytes())); // migrated
            assertNull(db.get(defaultCF, "key2".getBytes())); // migrated
            assertNull(db.get(defaultCF, "key3".getBytes())); // deleted
            assertEquals(4, db.get(defaultCF, "key4".getBytes()).length); // not migrated
            assertNull(db.get(defaultCF, "key5".getBytes())); // migrated
            assertNull(db.get(defaultCF, "key6".getBytes())); // migrated
            assertEquals(7, db.get(defaultCF, "key7".getBytes()).length); // not migrated
            assertNull(db.get(defaultCF, "key8new".getBytes()));
            assertNull(db.get(defaultCF, "key9new".getBytes()));
            assertNull(db.get(defaultCF, "key11new".getBytes()));
            assertNull(db.get(defaultCF, "key12new".getBytes()));

            // Headers CF should have all migrated/new keys with header-aware format
            assertNull(db.get(headersCF, "unknown".getBytes()));
            assertEquals(1 + 0 + 8 + 1, db.get(headersCF, "key1".getBytes()).length); // migrated by get()
            assertEquals("headers+timestamp+22".getBytes().length, db.get(headersCF, "key2".getBytes()).length); // migrated by put() => value is inserted without any conversion
            assertNull(db.get(headersCF, "key3".getBytes())); // deleted
            assertNull(db.get(headersCF, "key4".getBytes())); // not migrated
            assertEquals(1 + 0 + 8 + 5, db.get(headersCF, "key5".getBytes()).length); // migrated by putIfAbsent
            assertNull(db.get(headersCF, "key6".getBytes())); // migrated by delete() => deleted
            assertNull(db.get(headersCF, "key7".getBytes())); // not migrated
            assertEquals("headers+timestamp+88888888".getBytes().length, db.get(headersCF, "key8new".getBytes()).length); // added by put() => value is inserted without any conversion
            assertNull(db.get(headersCF, "key9new".getBytes()));
            assertEquals("headers+timestamp+11111111111".getBytes().length, db.get(headersCF, "key11new".getBytes()).length); // inserted (newly added) by putIfAbsent() => value is inserted without any conversion
            assertNull(db.get(headersCF, "key12new".getBytes()));
        } finally {
            if (defaultCF != null) {
                defaultCF.close();
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

    private void clearLegacyColumnFamily() throws Exception {
        // clear legacy timestamped CF by deleting remaining keys
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8), columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions),
                new ColumnFamilyDescriptor(OFFSETS_COLUMN_FAMILY_NAME, columnFamilyOptions));

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

            // Should only have DEFAULT, OFFSETS and keyValueWithTimestampAndHeaders, not the legacy keyValueWithTimestamp
            assertEquals(3, existingCFs.size(), "Expected only 3 column families after legacy CF is dropped");

            boolean hasDefault = false;
            boolean hasHeadersAware = false;
            boolean hasLegacy = false;
            boolean hasOffsets = false;

            for (final byte[] cf : existingCFs) {
                if (Arrays.equals(cf, RocksDB.DEFAULT_COLUMN_FAMILY)) {
                    hasDefault = true;
                } else if (Arrays.equals(cf, "keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8))) {
                    hasHeadersAware = true;
                } else if (Arrays.equals(cf, "keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8))) {
                    hasLegacy = true;
                } else if (Arrays.equals(cf, OFFSETS_COLUMN_FAMILY_NAME)) {
                    hasOffsets = true;
                }
            }

            assertTrue(hasOffsets, "Expected offsets column family to exist");
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

    @Test
    public void shouldTransitionFromPlainUpgradeModeToRegularMode() {
        // Prepare plain store
        prepareKeyValueStore();

        // Open in upgrade mode
        rocksDBStore.init(context, rocksDBStore);

        // Migrate all data by reading it (lazy migration deletes from DEFAULT CF)
        rocksDBStore.get(new Bytes("key1".getBytes()));
        rocksDBStore.get(new Bytes("key2".getBytes()));

        rocksDBStore.close();

        // Reopen - should now be in regular mode since DEFAULT CF is empty after migration
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in regular headers-aware mode"));
        } finally {
            rocksDBStore.close();
        }
    }

    @Test
    public void shouldNotSupportDowngradeFromHeadersAwareToRegularStore() {
        // prepare headers-aware store with data
        rocksDBStore.init(context, rocksDBStore);
        rocksDBStore.put(new Bytes("key1".getBytes()), "headers-aware-value1".getBytes());
        rocksDBStore.put(new Bytes("key2".getBytes()), "headers-aware-value2".getBytes());
        rocksDBStore.close();

        final RocksDBStore regularStore = new RocksDBStore(DB_NAME, METRICS_SCOPE);
        try {
            final ProcessorStateException exception = assertThrows(
                ProcessorStateException.class,
                () -> regularStore.init(context, regularStore)
            );

            assertTrue(exception.getMessage().contains("Store " + DB_NAME + " is a headers-aware store"));
            assertTrue(exception.getMessage().contains("cannot be opened as a regular key-value store"));
            assertTrue(exception.getMessage().contains("Downgrade from headers-aware to regular store is not supported"));
        } finally {
            regularStore.close();
        }
    }

    @Test
    public void shouldNotSupportDowngradeFromHeadersAwareToTimestampedStore() {
        rocksDBStore.init(context, rocksDBStore);
        rocksDBStore.put(new Bytes("key1".getBytes()), "headers-aware-value1".getBytes());
        rocksDBStore.put(new Bytes("key2".getBytes()), "headers-aware-value2".getBytes());
        rocksDBStore.close();

        final RocksDBTimestampedStore timestampedStore = new RocksDBTimestampedStore(DB_NAME, METRICS_SCOPE);
        try {
            final ProcessorStateException exception = assertThrows(
                ProcessorStateException.class,
                () -> timestampedStore.init(context, timestampedStore)
            );

            assertTrue(exception.getMessage().contains("Store " + DB_NAME + " is a headers-aware store"));
            assertTrue(exception.getMessage().contains("cannot be opened as a timestamped store"));
            assertTrue(exception.getMessage().contains("Downgrade from headers-aware to timestamped store is not supported"));
            assertTrue(exception.getMessage().contains("To downgrade, you can delete the local state in the state directory, and rebuild the store as timestamped store from the changelog"));
        } finally {
            timestampedStore.close();
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

    private void prepareKeyValueStoreWithMultipleKeys() {
        // Create a plain RocksDBStore with multiple keys for comprehensive migration testing
        final RocksDBStore kvStore = new RocksDBStore(DB_NAME, METRICS_SCOPE);
        try {
            kvStore.init(context, kvStore);

            // Write plain key-value pairs to default column family
            kvStore.put(new Bytes("key1".getBytes()), "1".getBytes());
            kvStore.put(new Bytes("key2".getBytes()), "22".getBytes());
            kvStore.put(new Bytes("key3".getBytes()), "333".getBytes());
            kvStore.put(new Bytes("key4".getBytes()), "4444".getBytes());
            kvStore.put(new Bytes("key5".getBytes()), "55555".getBytes());
            kvStore.put(new Bytes("key6".getBytes()), "666666".getBytes());
            kvStore.put(new Bytes("key7".getBytes()), "7777777".getBytes());
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

    @Test
    public void shouldReturnUnknownQueryTypeForQuery() {
        // Initialize the store
        rocksDBStore.init(context, rocksDBStore);

        // Create a query
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(new Bytes("test-key".getBytes()));
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false);

        // Execute query
        final QueryResult<byte[]> result = rocksDBStore.query(query, positionBound, config);

        // Verify result indicates unknown query type
        assertFalse(result.isSuccess(), "Expected query to fail with unknown query type");
        assertEquals(
            FailureReason.UNKNOWN_QUERY_TYPE,
            result.getFailureReason(),
            "Expected UNKNOWN_QUERY_TYPE failure reason"
        );

        // Verify position is set
        assertNotNull(result.getPosition(), "Expected position to be set");
    }

    @Test
    public void shouldCollectExecutionInfoWhenRequested() {
        // Initialize the store
        rocksDBStore.init(context, rocksDBStore);

        // Create a query with execution info collection enabled
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(new Bytes("test-key".getBytes()));
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(true); // Enable execution info

        // Execute query
        final QueryResult<byte[]> result = rocksDBStore.query(query, positionBound, config);

        // Verify execution info was collected
        assertFalse(result.getExecutionInfo().isEmpty(), "Expected execution info to be collected");
        assertTrue(
            result.getExecutionInfo().get(0).contains("Handled in"),
            "Expected execution info to contain handling information"
        );
        assertTrue(
            result.getExecutionInfo().get(0).contains(RocksDBTimestampedStoreWithHeaders.class.getName()),
            "Expected execution info to mention the class name"
        );
    }

    @Test
    public void shouldNotCollectExecutionInfoWhenNotRequested() {
        // Initialize the store
        rocksDBStore.init(context, rocksDBStore);

        // Create a query with execution info collection disabled
        final KeyQuery<Bytes, byte[]> query = KeyQuery.withKey(new Bytes("test-key".getBytes()));
        final PositionBound positionBound = PositionBound.unbounded();
        final QueryConfig config = new QueryConfig(false); // Disable execution info

        // Execute query
        final QueryResult<byte[]> result = rocksDBStore.query(query, positionBound, config);

        // Verify no execution info was collected
        assertTrue(result.getExecutionInfo().isEmpty(), "Expected no execution info to be collected");
    }
}
