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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test utility for directly manipulating RocksDB column family state to simulate
 * store corruption scenarios (e.g., unclean shutdown).
 */
public final class RocksDBStoreCorruptionUtils {

    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final byte[] OFFSETS_COLUMN_FAMILY_NAME = "offsets".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STATUS_KEY = STRING_SERIALIZER.serialize(null, "status");
    private static final byte[] OPEN_STATE = Serdes.Long().serializer().serialize(null, 1L);

    private RocksDBStoreCorruptionUtils() {
    }

    /**
     * Overwrites the store status key to 1L (open), simulating an unclean shutdown.
     *
     * @param dbDir the RocksDB store directory
     */
    public static void setStoreStatusToOpen(final File dbDir) throws RocksDBException {
        try (final DBOptions dbOptions = new DBOptions();
             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = listCfDescriptors(dbDir, cfOptions);
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            try (final RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles)) {
                final ColumnFamilyHandle offsetsCf = findOffsetsCf(cfHandles, cfDescriptors);
                db.put(offsetsCf, STATUS_KEY, OPEN_STATE);
            } finally {
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
    }

    /**
     * Deletes all offset entries from the offsets column family, keeping only the status key.
     *
     * @param dbDir the RocksDB store directory
     */
    public static void deleteOffsets(final File dbDir) throws RocksDBException {
        try (final DBOptions dbOptions = new DBOptions();
             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = listCfDescriptors(dbDir, cfOptions);
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            try (final RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles)) {
                final ColumnFamilyHandle offsetsCf = findOffsetsCf(cfHandles, cfDescriptors);

                try (final org.rocksdb.RocksIterator iter = db.newIterator(offsetsCf)) {
                    iter.seekToFirst();
                    while (iter.isValid()) {
                        final byte[] key = iter.key();
                        if (!Arrays.equals(key, STATUS_KEY)) {
                            db.delete(offsetsCf, key);
                        }
                        iter.next();
                    }
                }
            } finally {
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
    }

    private static List<ColumnFamilyDescriptor> listCfDescriptors(final File dbDir,
                                                                   final ColumnFamilyOptions cfOptions) throws RocksDBException {
        return RocksDB.listColumnFamilies(new Options(), dbDir.getAbsolutePath())
            .stream()
            .map(name -> new ColumnFamilyDescriptor(name, cfOptions))
            .collect(Collectors.toList());
    }

    private static ColumnFamilyHandle findOffsetsCf(final List<ColumnFamilyHandle> handles,
                                                     final List<ColumnFamilyDescriptor> descriptors) {
        for (int i = 0; i < descriptors.size(); i++) {
            if (Arrays.equals(descriptors.get(i).getName(), OFFSETS_COLUMN_FAMILY_NAME)) {
                return handles.get(i);
            }
        }
        throw new IllegalStateException("Offsets column family not found in RocksDB store");
    }
}
