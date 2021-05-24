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

import org.junit.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class BlockBasedTableConfigWithAccessibleCacheTest {

    static {
        RocksDB.loadLibrary();
    }

    @Test
    public void shouldReturnNoBlockCacheIfNoneIsSet() {
        final BlockBasedTableConfigWithAccessibleCache configWithAccessibleCache =
            new BlockBasedTableConfigWithAccessibleCache();

        assertThat(configWithAccessibleCache.blockCache(), nullValue());
    }

    @Test
    public void shouldSetBlockCacheAndMakeItAccessible() {
        final BlockBasedTableConfigWithAccessibleCache configWithAccessibleCache =
            new BlockBasedTableConfigWithAccessibleCache();
        final Cache blockCache = new LRUCache(1024);

        final BlockBasedTableConfig updatedConfig = configWithAccessibleCache.setBlockCache(blockCache);

        assertThat(updatedConfig, sameInstance(configWithAccessibleCache));
        assertThat(configWithAccessibleCache.blockCache(), sameInstance(blockCache));
    }
}