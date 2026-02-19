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

import org.apache.kafka.streams.state.TimestampedBytesStore;
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
 * A persistent key-(value-timestamp) store based on RocksDB.
 */
public class RocksDBTimestampedStore extends RocksDBStore implements TimestampedBytesStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBTimestampedStore.class);

    static final byte[] TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME = "keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8);

    public RocksDBTimestampedStore(final String name,
                                   final String metricsScope) {
        super(name, metricsScope);
    }

    RocksDBTimestampedStore(final String name,
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
            new ColumnFamilyDescriptor(TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME, columnFamilyOptions)
        );
        final ColumnFamilyHandle noTimestampColumnFamily = columnFamilies.get(0);
        final ColumnFamilyHandle withTimestampColumnFamily = columnFamilies.get(1);

        final RocksIterator noTimestampsIter = db.newIterator(noTimestampColumnFamily);
        noTimestampsIter.seekToFirst();
        if (noTimestampsIter.isValid()) {
            log.info("Opening store {} in upgrade mode", name);
            cfAccessor = new DualColumnFamilyAccessor(
                noTimestampColumnFamily,
                withTimestampColumnFamily,
                TimestampedBytesStore::convertToTimestampedFormat,
                this
            );
        } else {
            log.info("Opening store {} in regular mode", name);
            cfAccessor = new SingleColumnFamilyAccessor(withTimestampColumnFamily);
            noTimestampColumnFamily.close();
        }
        noTimestampsIter.close();
    }

}
