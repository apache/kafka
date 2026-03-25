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

import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A persistent key-value store with headers support based on RocksDB for window stores.
 * <p>
 * This store provides a migration path from plain {@link RocksDBStore} (DEFAULT column family)
 * to a headers-aware column family ({@code windowKeyValueWithHeaders}). It uses
 * {@link DualColumnFamilyAccessor} for lazy migration when legacy data exists in the DEFAULT CF.
 * <p>
 * Value format:
 * <ul>
 *   <li>Old format (DEFAULT CF): {@code [value]}</li>
 *   <li>New format (HEADERS CF): {@code [headersSize(varint)][headersBytes][value]}</li>
 * </ul>
 * <p>
 * This is similar to {@link RocksDBMigratingSessionStoreWithHeaders} but for window stores.
 * Unlike timestamped stores, window stores don't include timestamp in the value (it's in the key).
 */
public class RocksDBMigratingWindowStoreWithHeaders extends RocksDBStore implements HeadersBytesStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBMigratingWindowStoreWithHeaders.class);

    static final byte[] WINDOW_STORE_HEADERS_VALUES_COLUMN_FAMILY_NAME =
        "windowKeyValueWithHeaders".getBytes(StandardCharsets.UTF_8);

    RocksDBMigratingWindowStoreWithHeaders(final String name,
                                            final String parentDir,
                                            final RocksDBMetricsRecorder metricsRecorder) {
        super(name, parentDir, metricsRecorder);
    }

    @Override
    void openRocksDB(final DBOptions dbOptions,
                     final ColumnFamilyOptions columnFamilyOptions) {
        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
            dbOptions,
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor(WINDOW_STORE_HEADERS_VALUES_COLUMN_FAMILY_NAME, columnFamilyOptions),
            new ColumnFamilyDescriptor(OFFSETS_COLUMN_FAMILY_NAME, columnFamilyOptions)
        );
        final ColumnFamilyHandle noHeadersColumnFamily = columnFamilies.get(0);
        final ColumnFamilyHandle withHeadersColumnFamily = columnFamilies.get(1);
        final ColumnFamilyHandle offsetsCf = columnFamilies.get(2);

        // Check if DEFAULT CF has data (upgrade from old format without headers)
        try (final RocksIterator noHeadersIter = db.newIterator(noHeadersColumnFamily)) {
            noHeadersIter.seekToFirst();
            if (noHeadersIter.isValid()) {
                log.info("Opening window store {} in upgrade mode from plain value format", name);
                // Migrate from [value] to [headers][value]
                // Add empty headers prefix [0x00] to plain value
                cfAccessor = new DualColumnFamilyAccessor(
                    offsetsCf,
                    noHeadersColumnFamily,
                    withHeadersColumnFamily,
                    HeadersBytesStore::convertToHeaderFormat,
                    this,
                    open
                );
            } else {
                log.info("Opening window store {} in regular headers-aware mode", name);
                cfAccessor = new SingleColumnFamilyAccessor(offsetsCf, withHeadersColumnFamily);
                noHeadersColumnFamily.close();
            }
        }
    }
}
