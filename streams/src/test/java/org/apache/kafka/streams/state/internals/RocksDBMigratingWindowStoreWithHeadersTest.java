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
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RocksDBMigratingWindowStoreWithHeaders}.
 */
public class RocksDBMigratingWindowStoreWithHeadersTest extends RocksDBStoreTest {

    private final byte[] windowStoreHeadersColumnFamilyName =
        RocksDBMigratingWindowStoreWithHeaders.WINDOW_STORE_HEADERS_VALUES_COLUMN_FAMILY_NAME;

    RocksDBStore getRocksDBStore() {
        return new RocksDBMigratingWindowStoreWithHeaders(
            DB_NAME,
            "rocksdb",
            new RocksDBMetricsRecorder(METRICS_SCOPE, DB_NAME));
    }

    @Test
    public void shouldOpenNewStoreInRegularMode() {
        try (final LogCaptureAppender appender =
                 LogCaptureAppender.createAndRegister(RocksDBMigratingWindowStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().stream().anyMatch(m -> m.contains("in regular headers-aware mode")),
                "Expected log message about regular headers-aware mode, got: " + appender.getMessages());
        }

        try (final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all()) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void shouldOpenExistingHeadersAwareStoreInRegularMode() throws Exception {
        final Bytes key = new Bytes("win-key".getBytes());
        final byte[] value = new byte[] {0x00, 'v', 'a', 'l'};

        rocksDBStore.init(context, rocksDBStore);
        rocksDBStore.put(key, value);
        rocksDBStore.close();

        try (final LogCaptureAppender appender =
                 LogCaptureAppender.createAndRegister(RocksDBMigratingWindowStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().stream().anyMatch(m -> m.contains("in regular headers-aware mode")),
                "Expected regular mode on re-open, got: " + appender.getMessages());
        } finally {
            rocksDBStore.close();
        }

        verifyValueLandedInHeadersColumnFamily(key, value.length);
    }

    @Test
    public void shouldMigrateFromDefaultColumnFamilyWhenLegacyDataExists() throws Exception {
        seedDefaultColumnFamilyWithLegacyData();

        try (final LogCaptureAppender appender =
                 LogCaptureAppender.createAndRegister(RocksDBMigratingWindowStoreWithHeaders.class)) {
            rocksDBStore.init(context, rocksDBStore);

            assertTrue(appender.getMessages().stream().anyMatch(m -> m.contains("in upgrade mode from plain value format")),
                "Expected upgrade-mode log, got: " + appender.getMessages());
        }

        final byte[] legacyKey = "legacy".getBytes();
        final byte[] migrated = rocksDBStore.get(new Bytes(legacyKey));
        assertEquals(0x00, migrated[0], "Migrated value must begin with empty-headers prefix");
        final byte[] payload = new byte[migrated.length - 1];
        System.arraycopy(migrated, 1, payload, 0, payload.length);
        assertArrayEquals("v1".getBytes(), payload);

        assertNull(rocksDBStore.get(new Bytes("unknown".getBytes())));

        rocksDBStore.close();
    }

    private void seedDefaultColumnFamilyWithLegacyData() {
        final RocksDBStore plainStore = new RocksDBStore(DB_NAME, METRICS_SCOPE);
        try {
            plainStore.init(context, plainStore);
            plainStore.put(new Bytes("legacy".getBytes()), "v1".getBytes());
        } finally {
            plainStore.close();
        }
    }

    private void verifyValueLandedInHeadersColumnFamily(final Bytes key, final int expectedValueLength) throws Exception {
        final DBOptions dbOptions = new DBOptions();
        final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor(windowStoreHeadersColumnFamilyName, columnFamilyOptions),
            new ColumnFamilyDescriptor(RocksDBStore.OFFSETS_COLUMN_FAMILY_NAME, columnFamilyOptions));
        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());

        RocksDB db = null;
        ColumnFamilyHandle defaultCf = null;
        ColumnFamilyHandle headersCf = null;
        ColumnFamilyHandle offsetsCf = null;
        try {
            db = RocksDB.open(
                dbOptions,
                new File(new File(context.stateDir(), "rocksdb"), DB_NAME).getAbsolutePath(),
                columnFamilyDescriptors,
                columnFamilies);

            defaultCf = columnFamilies.get(0);
            headersCf = columnFamilies.get(1);
            offsetsCf = columnFamilies.get(2);

            assertNull(db.get(defaultCf, key.get()), "DEFAULT CF should not contain the key");
            final byte[] inHeadersCf = db.get(headersCf, key.get());
            assertEquals(expectedValueLength, inHeadersCf.length,
                "Value should be stored in headers CF with the original length");
        } finally {
            if (offsetsCf != null) {
                offsetsCf.close();
            }
            if (defaultCf != null) {
                defaultCf.close();
            }
            if (headersCf != null) {
                headersCf.close();
            }
            if (db != null) {
                db.close();
            }
            dbOptions.close();
            columnFamilyOptions.close();
        }
    }

    @Test
    public void shouldIterateOverBothColumnFamiliesInUpgradeMode() {
        seedDefaultColumnFamilyWithLegacyData();
        rocksDBStore.init(context, rocksDBStore);

        try (final KeyValueIterator<Bytes, byte[]> it = rocksDBStore.all()) {
            assertTrue(it.hasNext(), "Iterator should find the legacy key via on-the-fly conversion");
            final KeyValue<Bytes, byte[]> kv = it.next();
            assertArrayEquals("legacy".getBytes(), kv.key.get());
            assertEquals(0x00, kv.value[0], "Legacy value must be returned with empty-headers prefix");
        }

        rocksDBStore.close();
    }
}
