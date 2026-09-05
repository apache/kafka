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
 * A persistent bytes key-value store for the outer-join {@link ListValueStore} in HEADERS mode.
 * <p>
 * The store keeps two column families so it can be upgraded in place from a pre-headers (PLAIN) store
 * without corrupting existing data (KIP-1271 dual-column-family pattern, mirroring
 * {@link RocksDBTimestampedStoreWithHeaders}):
 * <ul>
 *   <li>DEFAULT: legacy PLAIN list blobs written by the pre-headers version;</li>
 *   <li>{@code listValueWithHeaders}: list blobs whose elements carry inline headers.</li>
 * </ul>
 * When the DEFAULT column family holds data at open time, a {@link DualColumnFamilyAccessor} lifts each
 * legacy blob to the headers format on read/write via
 * {@link ListValueStoreUpgradeUtils#convertPlainListBlobToHeadersListBlob} and migrates it forward.
 * Otherwise a {@link RocksDBStore.SingleColumnFamilyAccessor} over the headers column family is used.
 * <p>
 * Like its siblings it is a {@link HeadersBytesStore}, and additionally a
 * {@link HeadersAwareListValueStore} so that restore picks the list-aware converter rather than the
 * generic whole-value one; see {@link HeadersAwareListValueStore}.
 */
public class RocksDBListValueStoreWithHeaders extends RocksDBStore
    implements HeadersBytesStore, HeadersAwareListValueStore {

    private static final Logger log = LoggerFactory.getLogger(RocksDBListValueStoreWithHeaders.class);

    static final byte[] LIST_VALUE_WITH_HEADERS_CF_NAME =
        "listValueWithHeaders".getBytes(StandardCharsets.UTF_8);

    RocksDBListValueStoreWithHeaders(final String name,
                                     final String metricsScope) {
        super(name, metricsScope);
    }

    RocksDBListValueStoreWithHeaders(final String name,
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
            new ColumnFamilyDescriptor(LIST_VALUE_WITH_HEADERS_CF_NAME, columnFamilyOptions),
            new ColumnFamilyDescriptor(OFFSETS_COLUMN_FAMILY_NAME, offsetsCFOptions())
        );

        final ColumnFamilyHandle defaultCf = columnFamilies.get(0);
        final ColumnFamilyHandle listValueWithHeadersCf = columnFamilies.get(1);
        final ColumnFamilyHandle offsetsCf = columnFamilies.get(2);

        // If the DEFAULT column family holds data, we are upgrading from a plain list-value store.
        try (final RocksIterator defaultIter = db.newIterator(defaultCf)) {
            defaultIter.seekToFirst();
            if (defaultIter.isValid()) {
                log.info("Opening store {} in upgrade mode from plain list-value store", name);
                cfAccessor = new DualColumnFamilyAccessor(
                    offsetsCf,
                    defaultCf,
                    listValueWithHeadersCf,
                    ListValueStoreUpgradeUtils::convertPlainListBlobToHeadersListBlob,
                    this,
                    open
                );
            } else {
                log.info("Opening store {} in regular headers-aware mode", name);
                cfAccessor = new SingleColumnFamilyAccessor(offsetsCf, listValueWithHeadersCf);
                defaultCf.close();
            }
        } catch (final RuntimeException e) {
            for (final ColumnFamilyHandle handle : columnFamilies) {
                handle.close();
            }
            throw e;
        }
    }
}
