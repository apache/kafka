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

import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.state.HeadersBytesStore;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * A persistent key-(value-timestamp-headers) store based on RocksDB.
 */
public class RocksDBTimestampedStoreWithHeaders extends RocksDBStore implements HeadersBytesStore {

    private static final Logger log = LoggerFactory.getLogger(RocksDBTimestampedStoreWithHeaders.class);

    /**
     * Legacy column family name - must match {@code RocksDBTimestampedStore#TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME} 
     */

    private static final byte[] LEGACY_TIMESTAMPED_CF_NAME =
        "keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8);

    // New column family for header-aware timestamped values.
    private static final byte[] TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME =
        "keyValueWithTimestampAndHeaders".getBytes(StandardCharsets.UTF_8);

    public RocksDBTimestampedStoreWithHeaders(final String name,
                                              final String metricsScope) {
        super(name, metricsScope);
    }

    RocksDBTimestampedStoreWithHeaders(final String name,
                                       final String parentDir,
                                       final RocksDBMetricsRecorder metricsRecorder) {
        super(name, parentDir, metricsRecorder);
    }

    @Override
    void openRocksDB(final DBOptions dbOptions,
                     final ColumnFamilyOptions columnFamilyOptions) {
        // Check if we're upgrading from RocksDBTimestampedStore (which uses keyValueWithTimestamp CF)
        final List<byte[]> existingCFs;
        try (final Options options = new Options(dbOptions, new ColumnFamilyOptions())) {
            existingCFs = RocksDB.listColumnFamilies(options, dbDir.getAbsolutePath());
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error listing column families for store " + name, e);
        }


        final boolean upgradingFromTimestampedStore = existingCFs.stream()
            .anyMatch(cf -> Arrays.equals(cf, LEGACY_TIMESTAMPED_CF_NAME));

        if (upgradingFromTimestampedStore) {
            openWithLegacyTimestampedCF(dbOptions, columnFamilyOptions);
        } else {
            openWith2CFs(dbOptions, columnFamilyOptions);
        }
    }

    /**
     * Opens store when upgrading from RocksDBTimestampedStore (3 CFs).
     * CFs: DEFAULT (closed), keyValueWithTimestamp (legacy), headers (new)
     */
    private void openWithLegacyTimestampedCF(final DBOptions dbOptions,
                                              final ColumnFamilyOptions columnFamilyOptions) {
        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
            dbOptions,
            // we have to open the default CF to be able to open the legacy CF, but we won't use it
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor(LEGACY_TIMESTAMPED_CF_NAME, columnFamilyOptions),
            new ColumnFamilyDescriptor(TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME, columnFamilyOptions)
        );
        // Close unused default CF
        columnFamilies.get(0).close();

        final ColumnFamilyHandle legacyCf = columnFamilies.get(1);
        final ColumnFamilyHandle headersCf = columnFamilies.get(2);

        // Check if legacy CF has data
        final RocksIterator legacyIter = db.newIterator(legacyCf);
        legacyIter.seekToFirst();
        try {
            if (legacyIter.isValid()) {
                log.info("Opening store {} in upgrade mode", name);
                cfAccessor = new DualColumnFamilyAccessor(legacyCf, headersCf,
                    HeadersBytesStore::convertToHeaderFormat, this);
            } else {
                log.info("Opening store {} in regular headers-aware mode", name);
                cfAccessor = new SingleColumnFamilyAccessor(headersCf);
                legacyCf.close();
            }
        } finally {
            legacyIter.close();
        }
    }

    /**
     * Opens store with 2-CF design.
     * CFs: DEFAULT (legacy timestamped without headers), headers (new)
     */
    private void openWith2CFs(final DBOptions dbOptions,
                              final ColumnFamilyOptions columnFamilyOptions) {
        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
            dbOptions,
            // we have to open the default CF to be able to open the legacy CF, but we won't use it
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor(TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME, columnFamilyOptions)
        );

        // Close unused default CF
        columnFamilies.get(0).close();
        final ColumnFamilyHandle headersCf = columnFamilies.get(1);
        log.info("Opening store {} in regular headers-aware mode", name);
        cfAccessor = new SingleColumnFamilyAccessor(headersCf);
    }

}