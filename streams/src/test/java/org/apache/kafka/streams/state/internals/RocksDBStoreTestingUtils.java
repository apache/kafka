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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test utility for working with RocksDB including changing column family state to simulate
 * store corruption scenarios (e.g., unclean shutdown).
 */
public final class RocksDBStoreTestingUtils {

    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final byte[] OFFSETS_COLUMN_FAMILY_NAME = "offsets".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STATUS_KEY = STRING_SERIALIZER.serialize(null, "status");
    private static final byte[] OPEN_STATE = Serdes.Long().serializer().serialize(null, 1L);
    private static final byte[] POSITION_KEY = STRING_SERIALIZER.serialize(null, "position");

    private RocksDBStoreTestingUtils() {
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

    /**
     * Reads the store status from the offsets column family.
     *
     * @param dbDir the RocksDB store directory
     * @return the status value (0L = closed, 1L = open), or null if no status key exists
     */
    public static Long readStoreStatus(final File dbDir) throws RocksDBException {
        try (final DBOptions dbOptions = new DBOptions();
             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = listCfDescriptors(dbDir, cfOptions);
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            try (final RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles)) {
                final ColumnFamilyHandle offsetsCf = findOffsetsCf(cfHandles, cfDescriptors);
                final byte[] valueBytes = db.get(offsetsCf, STATUS_KEY);
                if (valueBytes != null) {
                    return Serdes.Long().deserializer().deserialize(null, valueBytes);
                }
                return null;
            } finally {
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
    }

    /**
     * Reads all offset entries from the offsets column family, excluding the status and position keys.
     * Keys are TopicPartition.toString() values, values are committed offsets.
     *
     * @param dbDir the RocksDB store directory
     * @return a map of partition string to committed offset
     */
    public static Map<String, Long> readOffsets(final File dbDir) throws RocksDBException {
        try (final DBOptions dbOptions = new DBOptions();
             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = listCfDescriptors(dbDir, cfOptions);
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            try (final RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles)) {
                final ColumnFamilyHandle offsetsCf = findOffsetsCf(cfHandles, cfDescriptors);
                final Map<String, Long> offsets = new HashMap<>();

                try (final org.rocksdb.RocksIterator iter = db.newIterator(offsetsCf)) {
                    iter.seekToFirst();
                    while (iter.isValid()) {
                        final byte[] key = iter.key();
                        if (!Arrays.equals(key, STATUS_KEY) && !Arrays.equals(key, POSITION_KEY)) {
                            final String partition = new String(key, StandardCharsets.UTF_8);
                            final Long offset = Serdes.Long().deserializer().deserialize(null, iter.value());
                            offsets.put(partition, offset);
                        }
                        iter.next();
                    }
                }
                return offsets;
            } finally {
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
    }

    /**
     * Finds all RocksDB store directories for the given store name across all task directories.
     * Returns an empty list if no task directories exist.
     *
     * @param stateDir the root state directory
     * @param appId    the application ID
     * @param storeName the store name
     * @return list of store directories
     */
    public static List<File> findAllStoreDirs(final File stateDir, final String appId, final String storeName) {
        final File appDir = new File(stateDir, appId);
        final File[] taskDirs = appDir.listFiles(file ->
            file.isDirectory() && !file.getName().startsWith("."));

        if (taskDirs == null || taskDirs.length == 0) {
            return Collections.emptyList();
        }

        final List<File> storeDirs = new ArrayList<>();
        for (final File taskDir : taskDirs) {
            final File storeDir2 = Paths.get(taskDir.getAbsolutePath(), "rocksdb", storeName).toFile();
            if (storeDir2.exists()) {
                storeDirs.add(storeDir2);
            }
        }

        if (storeDirs.isEmpty()) {
            throw new IllegalStateException("No store directories for '" + storeName + "' found under " + appDir);
        }
        return storeDirs;
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
