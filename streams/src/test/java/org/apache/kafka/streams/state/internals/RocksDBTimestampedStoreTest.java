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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.hamcrest.core.IsNull;
import org.junit.Test;
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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

public class RocksDBTimestampedStoreTest extends RocksDBStoreTest {

    private final Serializer<String> stringSerializer = new StringSerializer();

    RocksDBStore getRocksDBStore() {
        return new RocksDBTimestampedStore(DB_NAME, METRICS_SCOPE);
    }

    @Test
    public void shouldOpenNewStoreInRegularMode() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStore.class)) {
            rocksDBStore.init((StateStoreContext) context, rocksDBStore);

            assertThat(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in regular mode"));
        }

        try (final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all()) {
            assertThat(iterator.hasNext(), is(false));
        }
    }

    @Test
    public void shouldOpenExistingStoreInRegularMode() throws Exception {
        // prepare store
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        rocksDBStore.put(new Bytes("key".getBytes()), "timestamped".getBytes());
        rocksDBStore.close();

        // re-open store
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStore.class)) {
            rocksDBStore.init((StateStoreContext) context, rocksDBStore);

            assertThat(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in regular mode"));
        } finally {
            rocksDBStore.close();
        }

        // verify store
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor("keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());

        RocksDB db = null;
        ColumnFamilyHandle noTimestampColumnFamily = null, withTimestampColumnFamily = null;
        try {
            db = RocksDB.open(
                dbOptions,
                new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                columnFamilyDescriptors,
                columnFamilies);

            noTimestampColumnFamily = columnFamilies.get(0);
            withTimestampColumnFamily = columnFamilies.get(1);

            assertThat(db.get(noTimestampColumnFamily, "key".getBytes()), new IsNull<>());
            assertThat(db.getLongProperty(noTimestampColumnFamily, "rocksdb.estimate-num-keys"), is(0L));
            assertThat(db.get(withTimestampColumnFamily, "key".getBytes()).length, is(11));
            assertThat(db.getLongProperty(withTimestampColumnFamily, "rocksdb.estimate-num-keys"), is(1L));
        } finally {
            // Order of closing must follow: ColumnFamilyHandle > RocksDB > DBOptions > ColumnFamilyOptions
            if (noTimestampColumnFamily != null) {
                noTimestampColumnFamily.close();
            }
            if (withTimestampColumnFamily != null) {
                withTimestampColumnFamily.close();
            }
            if (db != null) {
                db.close();
            }
            dbOptions.close();
            columnFamilyOptions.close();
        }
    }

    @Test
    public void shouldMigrateDataFromDefaultToTimestampColumnFamily() throws Exception {
        prepareOldStore();

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStore.class)) {
            rocksDBStore.init((StateStoreContext) context, rocksDBStore);

            assertThat(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in upgrade mode"));
        }

        // approx: 7 entries on old CF, 0 in new CF
        assertThat(rocksDBStore.approximateNumEntries(), is(7L));

        // get()

        // should be no-op on both CF
        assertThat(rocksDBStore.get(new Bytes("unknown".getBytes())), new IsNull<>());
        // approx: 7 entries on old CF, 0 in new CF
        assertThat(rocksDBStore.approximateNumEntries(), is(7L));

        // should migrate key1 from old to new CF
        // must return timestamp plus value, ie, it's not 1 byte but 9 bytes
        assertThat(rocksDBStore.get(new Bytes("key1".getBytes())).length, is(8 + 1));
        // one delete on old CF, one put on new CF
        // approx: 6 entries on old CF, 1 in new CF
        assertThat(rocksDBStore.approximateNumEntries(), is(7L));

        // put()

        // should migrate key2 from old to new CF with new value
        rocksDBStore.put(new Bytes("key2".getBytes()), "timestamp+22".getBytes());
        // one delete on old CF, one put on new CF
        // approx: 5 entries on old CF, 2 in new CF
        assertThat(rocksDBStore.approximateNumEntries(), is(7L));

        // should delete key3 from old and new CF
        rocksDBStore.put(new Bytes("key3".getBytes()), null);
        // count is off by one, due to two delete operations (even if one does not delete anything)
        // approx: 4 entries on old CF, 1 in new CF
        assertThat(rocksDBStore.approximateNumEntries(), is(5L));

        // should add new key8 to new CF
        rocksDBStore.put(new Bytes("key8".getBytes()), "timestamp+88888888".getBytes());
        // one delete on old CF, one put on new CF
        // approx: 3 entries on old CF, 2 in new CF
        assertThat(rocksDBStore.approximateNumEntries(), is(5L));

        // putIfAbsent()

        // should migrate key4 from old to new CF with old value
        assertThat(rocksDBStore.putIfAbsent(new Bytes("key4".getBytes()), "timestamp+4444".getBytes()).length, is(8 + 4));
        // one delete on old CF, one put on new CF
        // approx: 2 entries on old CF, 3 in new CF
        assertThat(rocksDBStore.approximateNumEntries(), is(5L));

        // should add new key11 to new CF
        assertThat(rocksDBStore.putIfAbsent(new Bytes("key11".getBytes()), "timestamp+11111111111".getBytes()), new IsNull<>());
        // one delete on old CF, one put on new CF
        // approx: 1 entries on old CF, 4 in new CF
        assertThat(rocksDBStore.approximateNumEntries(), is(5L));

        // should not delete key5 but migrate to new CF
        assertThat(rocksDBStore.putIfAbsent(new Bytes("key5".getBytes()), null).length, is(8 + 5));
        // one delete on old CF, one put on new CF
        // approx: 0 entries on old CF, 5 in new CF
        assertThat(rocksDBStore.approximateNumEntries(), is(5L));

        // should be no-op on both CF
        assertThat(rocksDBStore.putIfAbsent(new Bytes("key12".getBytes()), null), new IsNull<>());
        // two delete operation, however, only one is counted because old CF count was zero before already
        // approx: 0 entries on old CF, 4 in new CF
        assertThat(rocksDBStore.approximateNumEntries(), is(4L));

        // delete()

        // should delete key6 from old and new CF
        assertThat(rocksDBStore.delete(new Bytes("key6".getBytes())).length, is(8 + 6));
        // two delete operation, however, only one is counted because old CF count was zero before already
        // approx: 0 entries on old CF, 3 in new CF
        assertThat(rocksDBStore.approximateNumEntries(), is(3L));

        iteratorsShouldNotMigrateData();
        assertThat(rocksDBStore.approximateNumEntries(), is(3L));

        rocksDBStore.close();

        verifyOldAndNewColumnFamily();
    }

    private void iteratorsShouldNotMigrateData() {
        // iterating should not migrate any data, but return all key over both CF (plus surrogate timestamps for old CF)
        try (final KeyValueIterator<Bytes, byte[]> itAll = rocksDBStore.all()) {
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 1
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '1'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key11".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'t', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'t', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 4444
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '4', '4', '4', '4'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 55555
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '5', '5', '5', '5', '5'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 7777777
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '7', '7', '7', '7', '7', '7', '7'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'t', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '8', '8', '8', '8', '8', '8', '8', '8'}, keyValue.value);
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
                // unknown timestamp == -1 plus value == 4444
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '4', '4', '4', '4'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 55555
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '5', '5', '5', '5', '5'}, keyValue.value);
            }
            assertFalse(it.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> itAll = rocksDBStore.reverseAll()) {
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key8".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'t', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '8', '8', '8', '8', '8', '8', '8', '8'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key7".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 7777777
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '7', '7', '7', '7', '7', '7', '7'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 55555
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '5', '5', '5', '5', '5'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 4444
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '4', '4', '4', '4'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key2".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'t', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '2', '2'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key11".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'t', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = itAll.next();
                assertArrayEquals("key1".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 1
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '1'}, keyValue.value);
            }
            assertFalse(itAll.hasNext());
        }

        try (final KeyValueIterator<Bytes, byte[]> it =
                 rocksDBStore.reverseRange(new Bytes("key2".getBytes()), new Bytes("key5".getBytes()))) {
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key5".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 55555
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '5', '5', '5', '5', '5'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key4".getBytes(), keyValue.key.get());
                // unknown timestamp == -1 plus value == 4444
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '4', '4', '4', '4'}, keyValue.value);
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
                // unknown timestamp == -1 plus value == 1
                assertArrayEquals(new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, '1'}, keyValue.value);
            }
            {
                final KeyValue<Bytes, byte[]> keyValue = it.next();
                assertArrayEquals("key11".getBytes(), keyValue.key.get());
                assertArrayEquals(new byte[]{'t', 'i', 'm', 'e', 's', 't', 'a', 'm', 'p', '+', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1'}, keyValue.value);
            }
            assertFalse(it.hasNext());
        }
    }

    private void verifyOldAndNewColumnFamily() throws Exception {
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor("keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8), columnFamilyOptions));
        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());

        RocksDB db = null;
        ColumnFamilyHandle noTimestampColumnFamily = null, withTimestampColumnFamily = null;
        boolean errorOccurred = false;
        try {
            db = RocksDB.open(
                dbOptions,
                new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                columnFamilyDescriptors,
                columnFamilies);

            noTimestampColumnFamily = columnFamilies.get(0);
            withTimestampColumnFamily = columnFamilies.get(1);

            assertThat(db.get(noTimestampColumnFamily, "unknown".getBytes()), new IsNull<>());
            assertThat(db.get(noTimestampColumnFamily, "key1".getBytes()), new IsNull<>());
            assertThat(db.get(noTimestampColumnFamily, "key2".getBytes()), new IsNull<>());
            assertThat(db.get(noTimestampColumnFamily, "key3".getBytes()), new IsNull<>());
            assertThat(db.get(noTimestampColumnFamily, "key4".getBytes()), new IsNull<>());
            assertThat(db.get(noTimestampColumnFamily, "key5".getBytes()), new IsNull<>());
            assertThat(db.get(noTimestampColumnFamily, "key6".getBytes()), new IsNull<>());
            assertThat(db.get(noTimestampColumnFamily, "key7".getBytes()).length, is(7));
            assertThat(db.get(noTimestampColumnFamily, "key8".getBytes()), new IsNull<>());
            assertThat(db.get(noTimestampColumnFamily, "key11".getBytes()), new IsNull<>());
            assertThat(db.get(noTimestampColumnFamily, "key12".getBytes()), new IsNull<>());

            assertThat(db.get(withTimestampColumnFamily, "unknown".getBytes()), new IsNull<>());
            assertThat(db.get(withTimestampColumnFamily, "key1".getBytes()).length, is(8 + 1));
            assertThat(db.get(withTimestampColumnFamily, "key2".getBytes()).length, is(12));
            assertThat(db.get(withTimestampColumnFamily, "key3".getBytes()), new IsNull<>());
            assertThat(db.get(withTimestampColumnFamily, "key4".getBytes()).length, is(8 + 4));
            assertThat(db.get(withTimestampColumnFamily, "key5".getBytes()).length, is(8 + 5));
            assertThat(db.get(withTimestampColumnFamily, "key6".getBytes()), new IsNull<>());
            assertThat(db.get(withTimestampColumnFamily, "key7".getBytes()), new IsNull<>());
            assertThat(db.get(withTimestampColumnFamily, "key8".getBytes()).length, is(18));
            assertThat(db.get(withTimestampColumnFamily, "key11".getBytes()).length, is(21));
            assertThat(db.get(withTimestampColumnFamily, "key12".getBytes()), new IsNull<>());
        } catch (final RuntimeException fatal) {
            errorOccurred = true;
        } finally {
            // Order of closing must follow: ColumnFamilyHandle > RocksDB > DBOptions > ColumnFamilyOptions
            if (noTimestampColumnFamily != null) {
                noTimestampColumnFamily.close();
            }
            if (withTimestampColumnFamily != null) {
                withTimestampColumnFamily.close();
            }
            if (db != null) {
                db.close();
            }
            if (errorOccurred) {
                dbOptions.close();
                columnFamilyOptions.close();
            }
        }

        // check that still in upgrade mode
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStore.class)) {
            rocksDBStore.init((StateStoreContext) context, rocksDBStore);

            assertThat(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in upgrade mode"));
        } finally {
            rocksDBStore.close();
        }

        // clear old CF
        columnFamilies.clear();
        db = null;
        noTimestampColumnFamily = null;
        try {
            db = RocksDB.open(
                dbOptions,
                new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                columnFamilyDescriptors,
                columnFamilies);

            noTimestampColumnFamily = columnFamilies.get(0);
            db.delete(noTimestampColumnFamily, "key7".getBytes());
        } finally {
            // Order of closing must follow: ColumnFamilyHandle > RocksDB > DBOptions > ColumnFamilyOptions
            if (noTimestampColumnFamily != null) {
                noTimestampColumnFamily.close();
            }
            if (db != null) {
                db.close();
            }
            dbOptions.close();
            columnFamilyOptions.close();
        }

        // check that still in regular mode
        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBTimestampedStore.class)) {
            rocksDBStore.init((StateStoreContext) context, rocksDBStore);

            assertThat(appender.getMessages(), hasItem("Opening store " + DB_NAME + " in regular mode"));
        }
    }

    private void prepareOldStore() {
        final RocksDBStore keyValueStore = new RocksDBStore(DB_NAME, METRICS_SCOPE);
        try {
            keyValueStore.init((StateStoreContext) context, keyValueStore);

            keyValueStore.put(new Bytes("key1".getBytes()), "1".getBytes());
            keyValueStore.put(new Bytes("key2".getBytes()), "22".getBytes());
            keyValueStore.put(new Bytes("key3".getBytes()), "333".getBytes());
            keyValueStore.put(new Bytes("key4".getBytes()), "4444".getBytes());
            keyValueStore.put(new Bytes("key5".getBytes()), "55555".getBytes());
            keyValueStore.put(new Bytes("key6".getBytes()), "666666".getBytes());
            keyValueStore.put(new Bytes("key7".getBytes()), "7777777".getBytes());
        } finally {
            keyValueStore.close();
        }
    }
}
