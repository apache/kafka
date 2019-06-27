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
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.TtlDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A persistent key-value store based on RocksDB with a TTL for entries
 */
public class RocksDBTTLStore extends RocksDBStore {

    private int ttlInSeconds;

    RocksDBTTLStore(final String name, final int ttlInSeconds) {
        this(name, DB_FILE_DIR, ttlInSeconds);
    }

    RocksDBTTLStore(final String name,
                    final String parentDir,
                    final int ttlInSeconds) {
        super(name, parentDir);
        this.ttlInSeconds = ttlInSeconds;
    }

    void openRocksDB(final DBOptions dbOptions,
                     final ColumnFamilyOptions columnFamilyOptions) {
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors
            = Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
        final List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());
        final List<Integer> ttlValues = Collections.singletonList(ttlInSeconds);
        try {
            db = TtlDB.open(dbOptions, dbDir.getAbsolutePath(), columnFamilyDescriptors, columnFamilies, ttlValues, false);
            dbAccessor = new SingleColumnFamilyAccessor(columnFamilies.get(0));
        } catch (final RocksDBException e) {
            throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.toString(), e);
        }
    }

}
