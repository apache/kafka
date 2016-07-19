/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;

/**
 * An interface to that allows developers to customize the RocksDB settings
 * for a given Store. Please read the <a href="https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide">RocksDB Tuning Guide</a>.
 */
public interface RocksDBConfigSetter {

    /**
     * Set the rocks db options for the provided storeName.
     * <p>
     *  Note: Some options may be taken as a hint, i.e, {@link BlockBasedTableConfig#setBlockCacheSize(long)} might be
     *  reduced due to memory constraints.
     * </p>
     * @param storeName     the name of the store being configured
     * @param options       the Rocks DB optionse
     */
    void setConfig(final String storeName, final Options options);
}
