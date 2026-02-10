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

import org.hamcrest.core.IsNull;
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
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RocksDBTimestampedStoreWithHeadersTest extends RocksDBStoreTest {

    private final Serializer<String> stringSerializer = new StringSerializer();

    RocksDBStore getRocksDBStore() {
        return new RocksDBTimestampedStoreWithHeaders(DB_NAME, METRICS_SCOPE);
    }

    @Test
    public void shouldOpenNewStoreInRegularMode() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertThat(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in regular headers-aware mode"));
        }

        try (final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all()) {
            assertThat(iterator.hasNext(), is(false));
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

            assertThat(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in regular headers-aware mode"));
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

            assertThat(db.get(defaultColumnFamily, "key".getBytes()), new IsNull<>());
            assertThat(db.getLongProperty(defaultColumnFamily, "rocksdb.estimate-num-keys"), is(0L));
            assertThat(db.get(headersColumnFamily, "key".getBytes()).length, is(22));
            assertThat(db.getLongProperty(headersColumnFamily, "rocksdb.estimate-num-keys"), is(1L));
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
        // Prepare legacy RocksDBTimestampedStore with timestamped values
        prepareOldStore();

        // Open with RocksDBTimestampedStoreWithHeaders - should detect legacy CF and enter upgrade mode
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertThat(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in upgrade mode"));
        }

        // approx: 7 entries on legacy timestamped CF, 0 in new headers-aware CF
        assertThat(rocksDBStore.approximateNumEntries(), is(7L));

        // get() - tests lazy migration on read

        // should be no-op on both CFs
        assertThat(rocksDBStore.get(new Bytes("unknown".getBytes())), new IsNull<>());
        // approx: 7 entries on legacy CF, 0 in headers-aware CF
        assertThat(rocksDBStore.approximateNumEntries(), is(7L));

        // should migrate key1 from legacy timestamped CF to headers-aware CF
        // returns header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(1) = 10 bytes
        assertThat(rocksDBStore.get(new Bytes("key1".getBytes())).length, is(1 + 0 + 8 + 1));
        // one delete on legacy CF, one put on headers-aware CF
        // approx: 6 entries on legacy CF, 1 in headers-aware CF
        assertThat(rocksDBStore.approximateNumEntries(), is(7L));

        // put() - tests migration on write

        // should migrate key2 from legacy CF to headers-aware CF with new value
        rocksDBStore.put(new Bytes("key2".getBytes()), "timestamp+22".getBytes());
        // one delete on legacy CF, one put on headers-aware CF
        // approx: 5 entries on legacy CF, 2 in headers-aware CF
        assertThat(rocksDBStore.approximateNumEntries(), is(7L));

        // should delete key3 from both legacy and headers-aware CF
        rocksDBStore.put(new Bytes("key3".getBytes()), null);
        // count is off by one, due to two delete operations (even if one does not delete anything)
        // approx: 4 entries on legacy CF, 1 in headers-aware CF
        assertThat(rocksDBStore.approximateNumEntries(), is(5L));

        // should add new key8 to headers-aware CF only
        rocksDBStore.put(new Bytes("key8".getBytes()), "headers+timestamp+88888888".getBytes());
        // one delete on legacy CF (no-op), one put on headers-aware CF
        // approx: 3 entries on legacy CF, 2 in headers-aware CF
        assertThat(rocksDBStore.approximateNumEntries(), is(5L));

        // putIfAbsent() - tests migration on conditional write

        // should migrate key4 from legacy CF to headers-aware CF with old value (not new value)
        // returns header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(4) = 13 bytes
        assertThat(rocksDBStore.putIfAbsent(new Bytes("key4".getBytes()), "headers+timestamp+4444".getBytes()).length, is(1 + 0 + 8 + 4));
        // one delete on legacy CF, one put on headers-aware CF
        // approx: 2 entries on legacy CF, 3 in headers-aware CF
        assertThat(rocksDBStore.approximateNumEntries(), is(5L));

        // should add new key11 to headers-aware CF only (returns null because key doesn't exist)
        assertThat(rocksDBStore.putIfAbsent(new Bytes("key11".getBytes()), "headers+timestamp+11111111111".getBytes()), new IsNull<>());
        // one delete on legacy CF (no-op), one put on headers-aware CF
        // approx: 1 entries on legacy CF, 4 in headers-aware CF
        assertThat(rocksDBStore.approximateNumEntries(), is(5L));

        // should not delete key5 but migrate to headers-aware CF (putIfAbsent with null doesn't delete)
        // returns header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(5) = 14 bytes
        assertThat(rocksDBStore.putIfAbsent(new Bytes("key5".getBytes()), null).length, is(1 + 0 + 8 + 5));
        // one delete on legacy CF, one put on headers-aware CF
        // approx: 0 entries on legacy CF, 5 in headers-aware CF
        assertThat(rocksDBStore.approximateNumEntries(), is(5L));

        // should be no-op on both CFs (key doesn't exist)
        assertThat(rocksDBStore.putIfAbsent(new Bytes("key12".getBytes()), null), new IsNull<>());
        // two delete operations, however, only one is counted because legacy CF count was zero before already
        // approx: 0 entries on legacy CF, 4 in headers-aware CF
        assertThat(rocksDBStore.approximateNumEntries(), is(4L));

        // delete() - tests migration on delete

        // should delete key6 from both legacy and headers-aware CF
        // returns header-aware format: varint(1) + empty headers(0) + timestamp(8) + value(6) = 15 bytes
        assertThat(rocksDBStore.delete(new Bytes("key6".getBytes())).length, is(1 + 0 + 8 + 6));
        // two delete operations, however, only one is counted because legacy CF count was zero before already
        // approx: 0 entries on legacy CF, 3 in headers-aware CF
        assertThat(rocksDBStore.approximateNumEntries(), is(3L));

        // iterators should not trigger migration (read-only)
        iteratorsShouldNotMigrateData();
        assertThat(rocksDBStore.approximateNumEntries(), is(3L));

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
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('1')
                // Total: 1 + 0 + 8 + 1 = 10 bytes
                assertThat(keyValue.value.length, is(10));
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
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('4444')
                // Total: 1 + 0 + 8 + 4 = 13 bytes
                assertThat(keyValue.value.length, is(13));
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('55555')
                // Total: 1 + 0 + 8 + 5 = 14 bytes
                assertThat(keyValue.value.length, is(14));
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get());
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('7777777')
                // Total: 1 + 0 + 8 + 7 = 16 bytes
                assertThat(keyValue.value.length, is(16));
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
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('4444')
                // Total: 1 + 0 + 8 + 4 = 13 bytes
                assertThat(keyValue.value.length, is(13));
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('55555')
                // Total: 1 + 0 + 8 + 5 = 14 bytes
                assertThat(keyValue.value.length, is(14));
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
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('7777777')
                // Total: 1 + 0 + 8 + 7 = 16 bytes
                assertThat(keyValue.value.length, is(16));
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('55555')
                // Total: 1 + 0 + 8 + 5 = 14 bytes
                assertThat(keyValue.value.length, is(14));
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('4444')
                // Total: 1 + 0 + 8 + 4 = 13 bytes
                assertThat(keyValue.value.length, is(13));
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
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('1')
                // Total: 1 + 0 + 8 + 1 = 10 bytes
                assertThat(keyValue.value.length, is(10));
            }
            assertFalse(itAll.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it =
                          rocksDBStore.reverseRange(new Bytes("key2".getBytes()), new Bytes("key5".getBytes()))) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('55555')
                // Total: 1 + 0 + 8 + 5 = 14 bytes
                assertThat(keyValue.value.length, is(14));
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('4444')
                // Total: 1 + 0 + 8 + 4 = 13 bytes
                assertThat(keyValue.value.length, is(13));
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
                // header-aware format: varint(0) + empty headers + timestamp(8 bytes) + value('1')
                // Total: 1 + 0 + 8 + 1 = 10 bytes
                assertThat(keyValue.value.length, is(10));
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
        assertThat(db.get(defaultColumnFamily, "unknown".getBytes()), new IsNull<>());
        assertThat(db.get(defaultColumnFamily, "key1".getBytes()), new IsNull<>());
    }

    private void verifyLegacyTimestampedColumnFamily(final RocksDB db, final ColumnFamilyHandle legacyTimestampedColumnFamily) throws Exception {
        // Legacy timestamped CF should have migrated keys as null, un-migrated as timestamped values
        assertThat(db.get(legacyTimestampedColumnFamily, "unknown".getBytes()), new IsNull<>());
        assertThat(db.get(legacyTimestampedColumnFamily, "key1".getBytes()), new IsNull<>()); // migrated
        assertThat(db.get(legacyTimestampedColumnFamily, "key2".getBytes()), new IsNull<>()); // migrated
        assertThat(db.get(legacyTimestampedColumnFamily, "key3".getBytes()), new IsNull<>()); // deleted
        assertThat(db.get(legacyTimestampedColumnFamily, "key4".getBytes()), new IsNull<>()); // migrated
        assertThat(db.get(legacyTimestampedColumnFamily, "key5".getBytes()), new IsNull<>()); // migrated
        assertThat(db.get(legacyTimestampedColumnFamily, "key6".getBytes()), new IsNull<>()); // migrated
        assertThat(db.get(legacyTimestampedColumnFamily, "key7".getBytes()).length, is(8 + 7)); // not migrated
        assertThat(db.get(legacyTimestampedColumnFamily, "key8".getBytes()), new IsNull<>());
    }

    private void verifyHeadersColumnFamily(final RocksDB db, final ColumnFamilyHandle headersColumnFamily) throws Exception {
        // Headers CF should have all migrated/new keys with header-aware format
        assertThat(db.get(headersColumnFamily, "unknown".getBytes()), new IsNull<>());
        assertThat(db.get(headersColumnFamily, "key1".getBytes()).length, is(1 + 0 + 8 + 1)); // varint + headers + ts + value
        assertThat(db.get(headersColumnFamily, "key2".getBytes()).length, is(12));
        assertThat(db.get(headersColumnFamily, "key3".getBytes()), new IsNull<>());
        assertThat(db.get(headersColumnFamily, "key4".getBytes()).length, is(1 + 0 + 8 + 4));
        assertThat(db.get(headersColumnFamily, "key5".getBytes()).length, is(1 + 0 + 8 + 5));
        assertThat(db.get(headersColumnFamily, "key6".getBytes()), new IsNull<>());
        assertThat(db.get(headersColumnFamily, "key7".getBytes()), new IsNull<>());
        assertThat(db.get(headersColumnFamily, "key8".getBytes()).length, is(26));
        assertThat(db.get(headersColumnFamily, "key11".getBytes()).length, is(29));
        assertThat(db.get(headersColumnFamily, "key12".getBytes()), new IsNull<>());
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

            assertThat(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in upgrade mode"));
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

            assertThat(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in regular headers-aware mode"));
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
