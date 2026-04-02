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
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
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
        RocksDBTimestampedStore.TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME;

    static final byte[] TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME =
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
        // Check if we're upgrading from RocksDBTimestampedStore or from plain RocksDBStore
        final List<byte[]> existingCFs;
        try (final Options options = new Options(dbOptions, new ColumnFamilyOptions())) {
            existingCFs = RocksDB.listColumnFamilies(options, dbDir.getAbsolutePath());
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error listing column families for store " + name, e);
        }

        final boolean hasTimestampedCF = existingCFs.stream()
            .anyMatch(cf -> Arrays.equals(cf, LEGACY_TIMESTAMPED_CF_NAME));

        if (hasTimestampedCF) {
            // Upgrading from timestamped store - use 2 CFs: LEGACY_TIMESTAMPED + HEADERS
            openFromTimestampedStore(dbOptions, columnFamilyOptions); // needs to check that default-CF has no data
        } else {
            openFromDefaultStore(dbOptions, columnFamilyOptions);
        }

    }

    private void openFromDefaultStore(final DBOptions dbOptions,
                                      final ColumnFamilyOptions columnFamilyOptions) {

        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
            dbOptions,
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor(TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME, columnFamilyOptions),
            new ColumnFamilyDescriptor(OFFSETS_COLUMN_FAMILY_NAME, columnFamilyOptions)
        );

        final ColumnFamilyHandle defaultCf = columnFamilies.get(0);
        final ColumnFamilyHandle headersCf = columnFamilies.get(1);
        final ColumnFamilyHandle offsetsCf = columnFamilies.get(2);

        // Check if default CF has data (plain store upgrade)
        try (final RocksIterator defaultIter = db.newIterator(defaultCf)) {
            defaultIter.seekToFirst();
            if (defaultIter.isValid()) {
                log.info("Opening store {} in upgrade mode from plain key value store", name);
                cfAccessor = new DualColumnFamilyAccessor(
                    offsetsCf,
                    defaultCf,
                    headersCf,
                    HeadersBytesStore::convertFromPlainToHeaderFormat,
                    this,
                    open
                );
            } else {
                log.info("Opening store {} in regular headers-aware mode", name);
                cfAccessor = new SingleColumnFamilyAccessor(offsetsCf, headersCf);
                defaultCf.close();
            }
        } catch (final RuntimeException e) {
            for (final ColumnFamilyHandle handle : columnFamilies) {
                handle.close();
            }
            throw e;
        }
    }

    private void openFromTimestampedStore(final DBOptions dbOptions,
                                          final ColumnFamilyOptions columnFamilyOptions) {
        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
            dbOptions,
            // we have to open the default CF to be able to open the legacy CF, but we won't use it
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
            new ColumnFamilyDescriptor(LEGACY_TIMESTAMPED_CF_NAME, columnFamilyOptions),
            new ColumnFamilyDescriptor(TIMESTAMPED_VALUES_WITH_HEADERS_CF_NAME, columnFamilyOptions),
            new ColumnFamilyDescriptor(OFFSETS_COLUMN_FAMILY_NAME, columnFamilyOptions)
        );

        try {
            // verify and close empty Default ColumnFamily
            try (final RocksIterator defaultIter = db.newIterator(columnFamilies.get(0))) {
                defaultIter.seekToFirst();
                if (defaultIter.isValid()) {
                    throw new ProcessorStateException(
                        "Inconsistent store state for " + name + ". " +
                            "Cannot have both plain (DEFAULT) and timestamped data simultaneously. " +
                            "Headers store can upgrade from either plain or timestamped format, but not both."
                    );
                }
            }
            // close default column family handle
            columnFamilies.get(0).close();

            final ColumnFamilyHandle legacyTimestampedCf = columnFamilies.get(1);
            final ColumnFamilyHandle headersCf = columnFamilies.get(2);
            final ColumnFamilyHandle offsetsCf = columnFamilies.get(3);

            // Check if legacy timestamped CF has data
            try (final RocksIterator legacyIter = db.newIterator(legacyTimestampedCf)) {
                legacyIter.seekToFirst();
                if (legacyIter.isValid()) {
                    log.info("Opening store {} in upgrade mode from timestamped store", name);
                    cfAccessor = new DualColumnFamilyAccessor(
                        offsetsCf,
                        legacyTimestampedCf,
                        headersCf,
                        HeadersBytesStore::convertToHeaderFormat,
                        this,
                            open
                    );
                } else {
                    log.info("Opening store {} in regular headers-aware mode", name);
                    cfAccessor = new SingleColumnFamilyAccessor(offsetsCf, headersCf);
                    try {
                        db.dropColumnFamily(legacyTimestampedCf);
                    } catch (final RocksDBException e) {
                        throw new RuntimeException(e);
                    } finally {
                        legacyTimestampedCf.close();
                    }
                }
            }
        } catch (final RuntimeException e) {
            for (final ColumnFamilyHandle handle : columnFamilies) {
                handle.close();
            }
            throw e;
        }
    }

    @SuppressWarnings("SynchronizeOnNonFinalField")
    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        final long start = config.isCollectExecutionInfo() ? System.nanoTime() : -1L;
        final QueryResult<R> result;

        synchronized (position) {
            result = QueryResult.forUnknownQueryType(query, this);

            if (config.isCollectExecutionInfo()) {
                result.addExecutionInfo(
                    "Handled in " + this.getClass() + " in " + (System.nanoTime() - start) + "ns"
                );
            }
            result.setPosition(position.copy());
        }
        return result;
    }

}