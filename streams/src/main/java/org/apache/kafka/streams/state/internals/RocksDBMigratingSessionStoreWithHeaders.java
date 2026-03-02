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
 * A persistent key-value store with headers support based on RocksDB.
 * <p>
 * This store provides a migration path from plain {@link RocksDBStore} (DEFAULT column family)
 * to a headers-aware column family ({@code sessionKeyValueWithHeaders}). It uses
 * {@link DualColumnFamilyAccessor} for lazy migration when legacy data exists in the DEFAULT CF.
 */
public class RocksDBMigratingSessionStoreWithHeaders extends RocksDBStore implements HeadersBytesStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBMigratingSessionStoreWithHeaders.class);

    static final byte[] SESSION_STORE_HEADERS_VALUES_COLUMN_FAMILY_NAME = "sessionKeyValueWithHeaders".getBytes(StandardCharsets.UTF_8);

    public RocksDBMigratingSessionStoreWithHeaders(final String name,
                                                   final String metricsScope) {
        super(name, metricsScope);
    }

    RocksDBMigratingSessionStoreWithHeaders(final String name,
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
            new ColumnFamilyDescriptor(SESSION_STORE_HEADERS_VALUES_COLUMN_FAMILY_NAME, columnFamilyOptions)
        );
        final ColumnFamilyHandle noHeadersColumnFamily = columnFamilies.get(0);
        final ColumnFamilyHandle withHeadersColumnFamily = columnFamilies.get(1);

        final RocksIterator noHeadersIter = db.newIterator(noHeadersColumnFamily);
        noHeadersIter.seekToFirst();
        if (noHeadersIter.isValid()) {
            log.info("Opening store {} in upgrade mode", name);
            cfAccessor = new DualColumnFamilyAccessor(
                noHeadersColumnFamily,
                withHeadersColumnFamily,
                HeadersBytesStore::convertToHeaderFormat,
                this
            );
        } else {
            log.info("Opening store {} in regular mode", name);
            cfAccessor = new SingleColumnFamilyAccessor(withHeadersColumnFamily);
            noHeadersColumnFamily.close();
        }
        noHeadersIter.close();
    }

}
