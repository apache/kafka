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

/**
 * An interface to that allows developers to customize the RocksDB settings for a given Store.
 * Please read the <a href="https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide">RocksDB Tuning Guide</a>.
 */
public interface RocksDBConfigSetter {

    /**
     * Set the rocks db options for the provided storeName.
     * 
     * @param storeName     the name of the store being configured
     * @param options       the Rocks DB options
     * @param configs       the configuration supplied to {@link org.apache.kafka.streams.StreamsConfig}
     */
    void setConfig(final String storeName, final Options options, final Map<String, Object> configs);
}
