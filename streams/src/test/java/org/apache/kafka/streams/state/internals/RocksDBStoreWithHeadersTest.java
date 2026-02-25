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

public class RocksDBStoreWithHeadersTest extends RocksDBStoreTest {

    private final Serializer<String> stringSerializer = new StringSerializer();

    RocksDBStore getRocksDBStore() {
        return new RocksDBStoreWithHeaders(DB_NAME, METRICS_SCOPE);
    }

    @Test
    public void shouldOpenNewStoreInRegularMode() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in regular mode"));
        }

        try (final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all()) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldOpenExistingStoreInRegularMode() throws Exception {
        final String key = "key";
        final String value = "withHeaders";
        // prepare store
        rocksDBStore.init(context, rocksDBStore);
        rocksDBStore.put(new Bytes(key.getBytes()), value.getBytes());
        rocksDBStore.close();

        // re-open store
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in regular mode"));
        } finally {
            rocksDBStore.close();
        }

        // verify store
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
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
    public void shouldMigrateFromDefaultToHeadersAwareColumnFamily() throws Exception {
        prepareDefaultStore();

        // Open with RocksDBStoreWithHeaders - should detect legacy data in DEFAULT CF and enter upgrade mode
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in upgrade mode"));
        }

        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 7 entries in DEFAULT CF and 0 in headers-aware CF before migration");

        // get() - tests lazy migration on read

        assertNull(rocksDBStore.get(new Bytes("unknown".getBytes())), "Expected null for unknown key");
        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 7 entries on DEFAULT CF, 0 in headers-aware CF");

        assertEquals(1 + 0 + 1, rocksDBStore.get(new Bytes("key1".getBytes())).length,
            "Expected header-aware format: varint(1) + empty headers(0) + value(1) = 2 bytes");
        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 6 entries on DEFAULT CF, 1 in headers-aware CF after migrating key1");

        // put() - tests migration on write

        rocksDBStore.put(new Bytes("key2".getBytes()), "headers+22".getBytes());
        assertEquals(7L, rocksDBStore.approximateNumEntries(), "Expected 5 entries on DEFAULT CF, 2 in headers-aware CF after migrating key2 with put()");

        rocksDBStore.put(new Bytes("key3".getBytes()), null);
        // count is off by one, due to two delete operations (even if one does not delete anything)
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 4 entries on DEFAULT CF, 1 in headers-aware CF after deleting key3 with put()");

        rocksDBStore.put(new Bytes("key8new".getBytes()), "headers+88888888".getBytes());
        // one delete on old CF, one put on new CF, but count is off by one due to delete on old CF not deleting anything
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 3 entries on DEFAULT CF, 2 in headers-aware CF after adding new key8new with put()");

        // putIfAbsent() - tests migration on conditional write

        assertNull(rocksDBStore.putIfAbsent(new Bytes("key11new".getBytes()), "headers+11111111111".getBytes()),
            "Expected null return value for putIfAbsent on non-existing key11new, and new key should be added to headers-aware CF");
        // one delete on old CF, one put on new CF, but count is off by one due to delete on old CF not deleting anything
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 2 entries on DEFAULT CF, 3 in headers-aware CF after adding new key11new with putIfAbsent()");

        assertEquals(1 + 0 + 5, rocksDBStore.putIfAbsent(new Bytes("key5".getBytes()), null).length,
            "Expected header-aware format: varint(1) + empty headers(0) + value(5) = 6 bytes for putIfAbsent with null on existing key5");
        assertEquals(5L, rocksDBStore.approximateNumEntries(), "Expected 1 entry on DEFAULT CF, 4 in headers-aware CF after migrating key5 with putIfAbsent(null)");

        assertNull(rocksDBStore.putIfAbsent(new Bytes("key12new".getBytes()), null));
        // two delete operation, however, only one is counted because old CF count can not be less than 0
        assertEquals(3L, rocksDBStore.approximateNumEntries(), "Expected 0 entries on DEFAULT CF, 3 in headers-aware CF after putIfAbsent with null on non-existing key12new");

        // delete() - tests migration on delete

        assertEquals(1 + 0 + 6, rocksDBStore.delete(new Bytes("key6".getBytes())).length,
            "Expected header-aware format: varint(1) + empty headers(0) + value(6) = 7 bytes for delete() on existing key6");
        // two delete operation, however, only one is counted because old CF count was zero before already
        assertEquals(2L, rocksDBStore.approximateNumEntries(), "Expected 0 entries on DEFAULT CF, 2 in headers-aware CF after deleting key6 with delete()");

        // iterators should not trigger migration (read-only)
        iteratorsShouldNotMigrateData();
        assertEquals(2L, rocksDBStore.approximateNumEntries());

        rocksDBStore.close();

        // Verify the final state of both column families
        verifyOldAndNewColumnFamily();
    }

    private void iteratorsShouldNotMigrateData() {
        // iterating should not migrate any data, but return all keys over both CFs
        // Values from DEFAULT CF are converted to header-aware format on the fly: 1 byte varint + [value]
        try (final KeyValueIterator<Bytes, byte[]> itAll = rocksDBStore.all()) {
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get()); // migrated by get()
                assertEquals(2, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(1) = 2 bytes for key1 from headers-aware CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key11new".getBytes(), keyValue.key.get()); // inserted by putIfAbsent()
                assertArrayEquals("headers+11111111111".getBytes(), keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get()); // migrated by put()
                assertArrayEquals("headers+22".getBytes(), keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get()); // not migrated, on-the-fly conversion
                assertEquals(5, keyValue.value.length,
                    "Expected header-aware format: varint(1) + empty headers(0) + value(4) = 5 bytes for key4 from DEFAULT CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get()); // migrated by putIfAbsent with null value
                assertEquals(6, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(5) = 6 bytes for key5 from headers-aware CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get()); // not migrated, on-the-fly conversion
                assertEquals(8, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(7) = 8 bytes for key7 from DEFAULT CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8new".getBytes(), keyValue.key.get()); // inserted by put()
                assertArrayEquals("headers+88888888".getBytes(), keyValue.value);
            }
            assertFalse(itAll.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it =
                          rocksDBStore.range(new Bytes("key2".getBytes()), new Bytes("key5".getBytes()))) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals("headers+22".getBytes(), keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertEquals(5, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(4) = 5 bytes for key4 from DEFAULT CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertEquals(6, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(5) = 6 bytes for key5 from headers-aware CF");
            }
            assertFalse(it.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> itAll = rocksDBStore.reverseAll()) {
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8new".getBytes(), keyValue.key.get());
                assertArrayEquals("headers+88888888".getBytes(), keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get());
                assertEquals(8, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(7) = 8 bytes for key7 from DEFAULT CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertEquals(6, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(5) = 6 bytes for key5 from headers-aware CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertEquals(5, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(4) = 5 bytes for key4 from DEFAULT CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals("headers+22".getBytes(), keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key11new".getBytes(), keyValue.key.get());
                assertArrayEquals("headers+11111111111".getBytes(), keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                assertEquals(2, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(1) = 2 bytes for key1 from headers-aware CF");
            }
            assertFalse(itAll.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it =
                          rocksDBStore.reverseRange(new Bytes("key2".getBytes()), new Bytes("key5".getBytes()))) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertEquals(6, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(5) = 6 bytes for key5 from headers-aware CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertEquals(5, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(4) = 5 bytes for key4 from DEFAULT CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals("headers+22".getBytes(), keyValue.value);
            }
            assertFalse(it.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it = rocksDBStore.prefixScan("key1", stringSerializer)) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                assertEquals(2, keyValue.value.length, "Expected header-aware format: varint(1) + empty headers(0) + value(1) = 2 bytes for key1 from headers-aware CF");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key11new".getBytes(), keyValue.key.get());
                assertArrayEquals("headers+11111111111".getBytes(), keyValue.value);
            }
            assertFalse(it.hasNext());
        }
    }

    private void verifyOldAndNewColumnFamily() throws Exception {
        verifyColumnFamilyContents();
        verifyStillInUpgradeMode();
        clearDefaultColumnFamily();
        verifyInRegularMode();
    }

    private void verifyColumnFamilyContents() throws Exception {
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));

        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());
        RocksDB db = null;
        ColumnFamilyHandle defaultColumnFamily = null;
        ColumnFamilyHandle headersColumnFamily = null;
        try {
            db = RocksDB.open(
                    dbOptions,
                    new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                    columnFamilyDescriptors,
                    columnFamilies);

            defaultColumnFamily = columnFamilies.get(0);
            headersColumnFamily = columnFamilies.get(1);

            verifyDefaultColumnFamily(db, defaultColumnFamily);
            verifyHeadersColumnFamily(db, headersColumnFamily);
        } finally {
            closeColumnFamilies(db, defaultColumnFamily, headersColumnFamily);
            dbOptions.close();
            columnFamilyOptions.close();
        }
    }

    private void verifyDefaultColumnFamily(final RocksDB db, final ColumnFamilyHandle defaultColumnFamily) throws Exception {
        // DEFAULT CF should have un-migrated keys, migrated keys should be deleted
        assertNull(db.get(defaultColumnFamily, "unknown".getBytes()));
        assertNull(db.get(defaultColumnFamily, "key1".getBytes())); // migrated
        assertNull(db.get(defaultColumnFamily, "key2".getBytes())); // migrated
        assertNull(db.get(defaultColumnFamily, "key3".getBytes())); // deleted
        assertEquals(4, db.get(defaultColumnFamily, "key4".getBytes()).length); // not migrated
        assertNull(db.get(defaultColumnFamily, "key5".getBytes())); // migrated
        assertNull(db.get(defaultColumnFamily, "key6".getBytes())); // migrated
        assertEquals(7, db.get(defaultColumnFamily, "key7".getBytes()).length); // not migrated
        assertNull(db.get(defaultColumnFamily, "key8new".getBytes()));
        assertNull(db.get(defaultColumnFamily, "key11new".getBytes()));
    }

    private void verifyHeadersColumnFamily(final RocksDB db, final ColumnFamilyHandle headersColumnFamily) throws Exception {
        // Headers CF should have all migrated/new keys
        assertNull(db.get(headersColumnFamily, "unknown".getBytes()));
        assertEquals(1 + 0 + 1, db.get(headersColumnFamily, "key1".getBytes()).length); // migrated by get()
        assertEquals("headers+22".getBytes().length, db.get(headersColumnFamily, "key2".getBytes()).length); // migrated by put() => value is inserted without any conversion
        assertNull(db.get(headersColumnFamily, "key3".getBytes())); // migrated by put() with null value => deleted
        assertNull(db.get(headersColumnFamily, "key4".getBytes())); // not migrated, should still be in DEFAULT column family
        assertEquals(1 + 0 + 5, db.get(headersColumnFamily, "key5".getBytes()).length); // migrated by putIfAbsent with null value
        assertNull(db.get(headersColumnFamily, "key6".getBytes())); // migrated by delete() => deleted
        assertNull(db.get(headersColumnFamily, "key7".getBytes())); // not migrated, should still be in DEFAULT column family
        assertEquals("headers+88888888".getBytes().length, db.get(headersColumnFamily, "key8new".getBytes()).length); // added by put()
        assertEquals("headers+11111111111".getBytes().length, db.get(headersColumnFamily, "key11new".getBytes()).length); // inserted by putIfAbsent()
        assertNull(db.get(headersColumnFamily, "key12new".getBytes())); // putIfAbsent with null value on non-existing key
    }

    private void closeColumnFamilies(
            final RocksDB db,
            final ColumnFamilyHandle defaultColumnFamily,
            final ColumnFamilyHandle headersColumnFamily) {
        // Order of closing must follow: ColumnFamilyHandle > RocksDB
        if (defaultColumnFamily != null) {
            defaultColumnFamily.close();
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
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in upgrade mode"));
        } finally {
            rocksDBStore.close();
        }
    }

    private void clearDefaultColumnFamily() throws Exception {
        // clear DEFAULT CF by deleting remaining keys
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor("keyValueWithHeaders".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));

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
            db.delete(defaultCF, "key4".getBytes());
            db.delete(defaultCF, "key7".getBytes());
        } finally {
            // Order of closing must follow: ColumnFamilyHandle > RocksDB > DBOptions > ColumnFamilyOptions
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

    private void verifyInRegularMode() {
        // check that now in regular mode (all legacy data migrated)
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in regular mode"));
        }
    }

    private void prepareDefaultStore() {
        // Create a plain RocksDBStore with data in default column family
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
}
