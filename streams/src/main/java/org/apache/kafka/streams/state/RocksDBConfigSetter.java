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
package org.apache.kafka.streams.state;

import org.rocksdb.Options;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An interface to that allows developers to customize the RocksDB settings for a given Store.
 * Please read the <a href="https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide">RocksDB Tuning Guide</a>.
 *
 * Note: if you choose to set options.setTableFormatConfig(tableConfig) with a new BlockBasedTableConfig you should
 * probably also set the filter for that tableConfig, most likely with tableConfig.setFilter(new BloomFilter());
 */
public interface RocksDBConfigSetter {

    Logger LOG = LoggerFactory.getLogger(RocksDBConfigSetter.class);

    /**
     * Set the rocks db options for the provided storeName.
     * 
     * @param storeName     the name of the store being configured
     * @param options       the RocksDB options
     * @param configs       the configuration supplied to {@link org.apache.kafka.streams.StreamsConfig}
     */
    void setConfig(final String storeName, final Options options, final Map<String, Object> configs);

    /**
     * Close any user-constructed objects that inherit from {@code org.rocksdb.RocksObject}.
     * <p>
     * Any object created with {@code new} in {@link RocksDBConfigSetter#setConfig setConfig()} and that inherits
     * from {@code org.rocksdb.RocksObject} should have {@code org.rocksdb.RocksObject#close()}
     * called on it here to avoid leaking off-heap memory. Objects to be closed can be saved by the user or retrieved
     * back from {@code options} using its getter methods.
     * <p>
     * Example objects needing to be closed include {@code org.rocksdb.Filter} and {@code org.rocksdb.Cache}.
     *
     * @param storeName     the name of the store being configured
     * @param options       the RocksDB options
     */
    default void close(final String storeName, final Options options) {
        LOG.warn("The default close will be removed in 3.0.0 -- you should overwrite it if you have implemented RocksDBConfigSetter");
    }
}
