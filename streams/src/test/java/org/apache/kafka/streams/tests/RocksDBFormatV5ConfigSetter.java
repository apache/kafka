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
package org.apache.kafka.streams.tests;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;

import java.util.Map;

/**
 * Forces RocksDB to use format version 5 for system testing.
 * Used in upgrade tests to enable safe downgrade paths.
 * 
 * Background:
 * - Kafka 2.4-4.1: RocksDB 7.9.2 (format v5)
 * - Kafka 4.2+: RocksDB 9.7.3+ (format v6)
 * - Format v6 cannot be read by RocksDB 7.9.2
 * 
 * Related: KAFKA-20096
 */
public class RocksDBFormatV5ConfigSetter implements RocksDBConfigSetter {
    private static final int ROCKSDB_FORMAT_VERSION_5 = 5;
    
    @Override
    public void setConfig(final String storeName, 
                         final Options options, 
                         final Map<String, Object> configs) {
        final BlockBasedTableConfig tableConfig = 
            (BlockBasedTableConfig) options.tableFormatConfig();
        
        tableConfig.setFormatVersion(ROCKSDB_FORMAT_VERSION_5);
        options.setTableFormatConfig(tableConfig);
        
        System.out.println("[RocksDBFormatV5ConfigSetter] Store '" + storeName 
                          + "' configured to use RocksDB format version " 
                          + ROCKSDB_FORMAT_VERSION_5);
    }
    
    @Override
    public void close(final String storeName, final Options options) {
    }
}