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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.AggregationWithHeaders;
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

public class RocksDBMigratingSessionStoreWithHeadersTest extends RocksDBStoreTest {

    private final Serializer<String> stringSerializer = new StringSerializer();
    private final AggregationWithHeadersSerializer<String> aggSerializer =
        new AggregationWithHeadersSerializer<>(new StringSerializer());
    private final AggregationWithHeadersDeserializer<String> aggDeserializer =
        new AggregationWithHeadersDeserializer<>(new StringDeserializer());
    private final byte[] sessionStoreHeaderColumnFamilyName = RocksDBMigratingSessionStoreWithHeaders.SESSION_STORE_HEADERS_VALUES_COLUMN_FAMILY_NAME;

    RocksDBStore getRocksDBStore() {
        return new RocksDBMigratingSessionStoreWithHeaders(DB_NAME, METRICS_SCOPE);
    }

    @Test
    public void shouldOpenNewStoreInRegularMode() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBMigratingSessionStoreWithHeaders.class)) {
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
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBMigratingSessionStoreWithHeaders.class)) {
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
                new ColumnFamilyDescriptor(sessionStoreHeaderColumnFamilyName, columnFamilyOptions));
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

        // Open with RocksDBMigratingSessionStoreWithHeaders - should detect legacy data in DEFAULT CF and enter upgrade mode
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBMigratingSessionStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in upgrade mode"));
        }

        // get() - tests lazy migration on read: legacy value converted via convertToHeaderFormat
        assertNull(rocksDBStore.get(new Bytes("unknown".getBytes())), "Expected null for unknown key");

        final byte[] key1Result = rocksDBStore.get(new Bytes("key1".getBytes()));
        assertMigratedValue(key1Result, "1");

        // put() - tests migration on write using properly serialized AggregationWithHeaders
        final byte[] key2Value = serializeAggWithHeaders("22", testHeaders());
        rocksDBStore.put(new Bytes("key2".getBytes()), key2Value);

        rocksDBStore.put(new Bytes("key3".getBytes()), null);

        final byte[] key8Value = serializeAggWithHeaders("88888888", new RecordHeaders());
        rocksDBStore.put(new Bytes("key8new".getBytes()), key8Value);

        // putIfAbsent() - tests migration on conditional write
        final byte[] key11Value = serializeAggWithHeaders("11111111111", testHeaders());
        assertNull(rocksDBStore.putIfAbsent(new Bytes("key11new".getBytes()), key11Value),
            "Expected null return value for putIfAbsent on non-existing key11new");

        final byte[] key5Result = rocksDBStore.putIfAbsent(new Bytes("key5".getBytes()), null);
        assertMigratedValue(key5Result, "55555");

        assertNull(rocksDBStore.putIfAbsent(new Bytes("key12new".getBytes()), null));

        // delete() - tests migration on delete
        final byte[] key6Result = rocksDBStore.delete(new Bytes("key6".getBytes()));
        assertMigratedValue(key6Result, "666666");

        // iterators should not trigger migration (read-only)
        iteratorsShouldNotMigrateData();

        rocksDBStore.close();

        // Verify the final state of both column families
        verifyOldAndNewColumnFamily();
    }

    private void iteratorsShouldNotMigrateData() {
        // iterating should not migrate any data, but return all keys over both CFs
        // Values from DEFAULT CF are converted to header-aware format on the fly via convertToHeaderFormat
        try (final KeyValueIterator<Bytes, byte[]> itAll = rocksDBStore.all()) {
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get()); // migrated by get()
                assertMigratedValue(keyValue.value, "1");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key11new".getBytes(), keyValue.key.get()); // inserted by putIfAbsent()
                assertValueWithHeaders(keyValue.value, "11111111111", testHeaders());
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get()); // replaced by put()
                assertValueWithHeaders(keyValue.value, "22", testHeaders());
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get()); // not migrated, on-the-fly conversion
                assertMigratedValue(keyValue.value, "4444");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get()); // migrated by putIfAbsent with null value
                assertMigratedValue(keyValue.value, "55555");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get()); // not migrated, on-the-fly conversion
                assertMigratedValue(keyValue.value, "7777777");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8new".getBytes(), keyValue.key.get()); // inserted by put()
                assertMigratedValue(keyValue.value, "88888888");
            }
            assertFalse(itAll.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it =
                          rocksDBStore.range(new Bytes("key2".getBytes()), new Bytes("key5".getBytes()))) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertValueWithHeaders(keyValue.value, "22", testHeaders());
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertMigratedValue(keyValue.value, "4444");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertMigratedValue(keyValue.value, "55555");
            }
            assertFalse(it.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> itAll = rocksDBStore.reverseAll()) {
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8new".getBytes(), keyValue.key.get());
                assertMigratedValue(keyValue.value, "88888888");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get());
                assertMigratedValue(keyValue.value, "7777777");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertMigratedValue(keyValue.value, "55555");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertMigratedValue(keyValue.value, "4444");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertValueWithHeaders(keyValue.value, "22", testHeaders());
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key11new".getBytes(), keyValue.key.get());
                assertValueWithHeaders(keyValue.value, "11111111111", testHeaders());
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                assertMigratedValue(keyValue.value, "1");
            }
            assertFalse(itAll.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it =
                          rocksDBStore.reverseRange(new Bytes("key2".getBytes()), new Bytes("key5".getBytes()))) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                assertMigratedValue(keyValue.value, "55555");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                assertMigratedValue(keyValue.value, "4444");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertValueWithHeaders(keyValue.value, "22", testHeaders());
            }
            assertFalse(it.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it = rocksDBStore.prefixScan("key1", stringSerializer)) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                assertMigratedValue(keyValue.value, "1");
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key11new".getBytes(), keyValue.key.get());
                assertValueWithHeaders(keyValue.value, "11111111111", testHeaders());
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
                new ColumnFamilyDescriptor(sessionStoreHeaderColumnFamilyName, columnFamilyOptions));

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
        // DEFAULT CF should have un-migrated keys with plain aggregation bytes; migrated keys should be deleted
        assertNull(db.get(defaultColumnFamily, "unknown".getBytes()));
        assertNull(db.get(defaultColumnFamily, "key1".getBytes())); // migrated by get()
        assertNull(db.get(defaultColumnFamily, "key2".getBytes())); // migrated by put()
        assertNull(db.get(defaultColumnFamily, "key3".getBytes())); // deleted
        assertArrayEquals("4444".getBytes(), db.get(defaultColumnFamily, "key4".getBytes())); // not migrated, plain aggregation
        assertNull(db.get(defaultColumnFamily, "key5".getBytes())); // migrated by putIfAbsent()
        assertNull(db.get(defaultColumnFamily, "key6".getBytes())); // migrated by delete()
        assertArrayEquals("7777777".getBytes(), db.get(defaultColumnFamily, "key7".getBytes())); // not migrated, plain aggregation
        assertNull(db.get(defaultColumnFamily, "key8new".getBytes()));
        assertNull(db.get(defaultColumnFamily, "key11new".getBytes()));
    }

    private void verifyHeadersColumnFamily(final RocksDB db, final ColumnFamilyHandle headersColumnFamily) throws Exception {
        // Headers CF should have all migrated/new keys in headers-aware format
        assertNull(db.get(headersColumnFamily, "unknown".getBytes()));
        assertMigratedValue(db.get(headersColumnFamily, "key1".getBytes()), "1"); // migrated by get()
        assertValueWithHeaders(db.get(headersColumnFamily, "key2".getBytes()), "22", testHeaders()); // put with headers
        assertNull(db.get(headersColumnFamily, "key3".getBytes())); // put with null value => deleted
        assertNull(db.get(headersColumnFamily, "key4".getBytes())); // not migrated, still in DEFAULT CF
        assertMigratedValue(db.get(headersColumnFamily, "key5".getBytes()), "55555"); // migrated by putIfAbsent
        assertNull(db.get(headersColumnFamily, "key6".getBytes())); // migrated by delete() => deleted
        assertNull(db.get(headersColumnFamily, "key7".getBytes())); // not migrated, still in DEFAULT CF
        assertMigratedValue(db.get(headersColumnFamily, "key8new".getBytes()), "88888888"); // put with empty headers
        assertValueWithHeaders(db.get(headersColumnFamily, "key11new".getBytes()), "11111111111", testHeaders()); // putIfAbsent with headers
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
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBMigratingSessionStoreWithHeaders.class)) {
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
                new ColumnFamilyDescriptor(sessionStoreHeaderColumnFamilyName, columnFamilyOptions));

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
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBMigratingSessionStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().contains("Opening store " + DB_NAME + " in regular mode"));
        }
    }

    private void prepareDefaultStore() {
        // Create a plain RocksDBStore with data in default column family
        final RocksDBStore kvStore = new RocksDBStore(DB_NAME, METRICS_SCOPE);
        try {
            kvStore.init(context, kvStore);

            // Write plain aggregation bytes to default column family (simulating pre-headers store data)
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

    private Headers testHeaders() {
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("source", "test".getBytes(StandardCharsets.UTF_8)));
        return headers;
    }

    private byte[] serializeAggWithHeaders(final String aggregation, final Headers headers) {
        return aggSerializer.serialize(null, AggregationWithHeaders.make(aggregation, headers));
    }

    private void assertMigratedValue(final byte[] value, final String expectedAggregation) {
        final Headers headers = AggregationWithHeadersDeserializer.headers(value);
        assertFalse(headers.iterator().hasNext(), "Migrated value should have empty headers");
        assertArrayEquals(
            expectedAggregation.getBytes(StandardCharsets.UTF_8),
            AggregationWithHeadersDeserializer.rawAggregation(value),
            "Migrated value should preserve original aggregation: " + expectedAggregation);
    }

    private void assertValueWithHeaders(final byte[] value, final String expectedAggregation, final Headers expectedHeaders) {
        final AggregationWithHeaders<String> deserialized = aggDeserializer.deserialize(null, value);
        assertEquals(expectedAggregation, deserialized.aggregation());
        for (final Header header : expectedHeaders) {
            assertArrayEquals(
                header.value(),
                deserialized.headers().lastHeader(header.key()).value(),
                "Expected header '" + header.key() + "' to match");
        }
    }
}
